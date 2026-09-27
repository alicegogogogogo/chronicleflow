import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


class SchedulePreviewHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-preview.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, tenant=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        if tenant is not None:
            headers["X-Tenant-Id"] = tenant
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def create_workflow(self, workflow_id, schedule=None, tenant=None):
        body = {"id": workflow_id, "nodes": [{"id": "a", "kind": "task", "depends_on": []}]}
        if schedule is not None:
            body["schedule"] = schedule
        status, data = self.call("POST", "/workflows", body, key=f"create-{workflow_id}", tenant=tenant)
        self.assertEqual(201, status, data)

    def test_interval_preview_projects_from_the_next_unsettled_period(self):
        self.create_workflow(
            "wf-p1",
            {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
        )
        status, data = self.call("GET", "/workflows/wf-p1/schedule/preview?limit=3")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        payload = json.loads(data)
        self.assertEqual(
            {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
            payload["schedule"],
        )
        previews = payload["previews"]
        self.assertEqual(3, len(previews))
        for entry in previews:
            self.assertEqual({"mode": "nightly"}, entry["input"])
            self.assertTrue(entry["trigger_at"].endswith("Z"), entry)
        times = [entry["trigger_at"] for entry in previews]
        self.assertEqual(sorted(times), times)
        self.assertEqual(len(set(times)), 3)

    def test_preview_limit_caps_the_entries(self):
        self.create_workflow("wf-p2", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        status, data = self.call("GET", "/workflows/wf-p2/schedule/preview?limit=1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(json.loads(data)["previews"]))

    def test_cron_preview_matches_the_field_rules(self):
        self.create_workflow(
            "wf-p3", {"cron": "30 9 * * 1-5", "input": {"n": 1}, "missed_policy": "skip"}
        )
        status, data = self.call("GET", "/workflows/wf-p3/schedule/preview?limit=4")
        self.assertEqual(200, status)
        previews = json.loads(data)["previews"]
        self.assertEqual(4, len(previews))
        for entry in previews:
            self.assertTrue(entry["trigger_at"].endswith("T09:30:00Z"), entry)
            self.assertEqual({"n": 1}, entry["input"])
        times = [entry["trigger_at"] for entry in previews]
        self.assertEqual(sorted(times), times)
        self.assertEqual(len(set(times)), 4)

    def test_preview_is_read_only_and_repeatable(self):
        self.create_workflow("wf-p4", {"interval_seconds": 3600, "input": {}, "missed_policy": "catch_up"})
        status, first = self.call("GET", "/workflows/wf-p4/schedule/preview?limit=2")
        self.assertEqual(200, status)
        status, second = self.call("GET", "/workflows/wf-p4/schedule/preview?limit=2")
        self.assertEqual(200, status)
        # No cursor movement: the same query returns the same projection.
        self.assertEqual(first, second)
        # Nothing fired and nothing was created.
        status, data = self.call("GET", "/workflows/wf-p4/schedule")
        payload = json.loads(data)
        self.assertIsNone(payload["last_triggered_at"])
        self.assertIsNone(payload["last_execution_id"])
        status, data = self.call("GET", "/executions/wf-p4-scheduled-i:1")
        self.assertEqual(404, status)

    def test_paused_schedule_still_previews(self):
        self.create_workflow("wf-p5", {"interval_seconds": 3600, "input": {}, "missed_policy": "skip"})
        status, data = self.call("POST", "/workflows/wf-p5/schedule/pause", {}, key="pause-p5")
        self.assertEqual(200, status)
        status, data = self.call("GET", "/workflows/wf-p5/schedule/preview?limit=2")
        self.assertEqual(200, status)
        self.assertEqual(2, len(json.loads(data)["previews"]))
        # The preview created nothing while paused.
        status, data = self.call("GET", "/workflows/wf-p5/schedule")
        self.assertIsNone(json.loads(data)["last_execution_id"])

    def test_preview_without_schedule_is_definite_empty_result(self):
        self.create_workflow("wf-p6")
        status, data = self.call("GET", "/workflows/wf-p6/schedule/preview?limit=3")
        self.assertEqual(200, status)
        self.assertEqual({"schedule": None}, json.loads(data))

    def test_preview_on_missing_or_cross_tenant_workflow_is_not_found(self):
        self.create_workflow("wf-p7", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"}, tenant="alpha")
        status, data = self.call("GET", "/workflows/wf-p7/schedule/preview?limit=2", tenant="alpha")
        self.assertEqual(200, status)
        self.assertEqual(2, len(json.loads(data)["previews"]))
        # Another tenant (and the legacy namespace) cannot see it.
        status, data = self.call("GET", "/workflows/wf-p7/schedule/preview?limit=2", tenant="beta")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/workflows/wf-p7/schedule/preview?limit=2")
        self.assertEqual(404, status)
        status, data = self.call("GET", "/workflows/nope/schedule/preview?limit=2")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_preview_rejects_an_empty_tenant_header(self):
        status, data = self.call("GET", "/workflows/wf-p2/schedule/preview?limit=1", tenant="")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_preview_limit_validation(self):
        self.create_workflow("wf-p8", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        bad_paths = [
            "/workflows/wf-p8/schedule/preview",
            "/workflows/wf-p8/schedule/preview?limit=",
            "/workflows/wf-p8/schedule/preview?limit=0",
            "/workflows/wf-p8/schedule/preview?limit=-2",
            "/workflows/wf-p8/schedule/preview?limit=1.5",
            "/workflows/wf-p8/schedule/preview?limit=abc",
            "/workflows/wf-p8/schedule/preview?limit=NaN",
            "/workflows/wf-p8/schedule/preview?limit=inf",
            "/workflows/wf-p8/schedule/preview?limit=1&limit=2",
            "/workflows/wf-p8/schedule/preview?limit=1&extra=2",
            "/workflows/wf-p8/schedule/preview?count=2",
        ]
        for path in bad_paths:
            with self.subTest(path=path):
                status, data = self.call("GET", path)
                self.assertEqual(400, status, path)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # The rejections wrote nothing: the plan and its status are untouched.
        status, data = self.call("GET", "/workflows/wf-p8/schedule")
        payload = json.loads(data)
        self.assertEqual({"interval_seconds": 60, "input": {}, "missed_policy": "skip"}, payload["schedule"])
        self.assertIsNone(payload["last_execution_id"])

    def test_preview_input_keeps_float_precision_and_negative_zero(self):
        self.create_workflow(
            "wf-p9",
            {"interval_seconds": 3600, "input": {"v": 0.30000000000000004, "z": -0.0}, "missed_policy": "skip"},
        )
        status, data = self.call("GET", "/workflows/wf-p9/schedule/preview?limit=1")
        self.assertEqual(200, status)
        entry = json.loads(data)["previews"][0]
        self.assertEqual(0.30000000000000004, entry["input"]["v"])
        self.assertEqual(-0.0, entry["input"]["z"])
        self.assertIn(b"0.30000000000000004", data)
        self.assertIn(b"-0.0", data)


if __name__ == "__main__":
    unittest.main()
