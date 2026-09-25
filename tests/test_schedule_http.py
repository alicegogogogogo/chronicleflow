import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


class ScheduleHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.service = ChronicleFlow(str(Path(cls.directory.name) / "http-schedules.db"))
        Handler.service = cls.service
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.service.close()
        cls.directory.cleanup()

    def request(self, method, path, body=None, key=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        data = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, data, headers)
        response = connection.getresponse()
        payload = response.read()
        connection.close()
        return response.status, (json.loads(payload) if payload else None)

    def create_workflow(self, workflow_id, schedule=None, key=None):
        body = {"id": workflow_id, "nodes": [{"id": "a", "kind": "task", "depends_on": []}]}
        if schedule is not None:
            body["schedule"] = schedule
        return self.request("POST", "/workflows", body, key or workflow_id)

    def activation(self, workflow_id):
        return self.service.store.connection.execute(
            "SELECT activated_at FROM schedules WHERE workflow_id = ?", (workflow_id,)
        ).fetchone()["activated_at"]

    def test_workflow_created_with_schedule_returns_it(self):
        status, workflow = self.create_workflow(
            "wf-sched",
            {"interval_seconds": 60, "input": {"k": "v"}, "misfire_policy": "catch_up"},
            "wf-sched",
        )
        self.assertEqual(201, status)
        self.assertEqual(60, workflow["schedule"]["interval_seconds"])
        self.assertEqual("catch_up", workflow["schedule"]["misfire_policy"])

    def test_invalid_schedule_on_create_is_validation_error(self):
        status, payload = self.create_workflow(
            "wf-bad-sched",
            {"interval_seconds": -1, "input": {}, "misfire_policy": "catch_up"},
            "wf-bad-sched",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"]["code"])

    def test_schedule_query_empty_result_and_missing_workflow(self):
        self.create_workflow("wf-plain", None, "wf-plain")
        status, payload = self.request("GET", "/workflows/wf-plain/schedule")
        self.assertEqual(200, status)
        self.assertEqual({"schedule": None}, payload)
        status, payload = self.request("GET", "/workflows/wf-missing/schedule")
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"]["code"])

    def test_declare_pause_resume_and_query_lifecycle(self):
        self.create_workflow(
            "wf-life",
            {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            "wf-life",
        )
        status, payload = self.request(
            "POST",
            "/workflows/wf-life/schedule",
            {"cron": "0 12 * * *", "input": {"later": True}, "misfire_policy": "skip"},
            "decl-life",
        )
        self.assertEqual(200, status)
        self.assertEqual("0 12 * * *", payload["schedule"]["cron"])
        self.assertFalse(payload["schedule"]["paused"])

        status, payload = self.request("POST", "/workflows/wf-life/schedule/pause", key="pause-life")
        self.assertEqual(200, status)
        self.assertTrue(payload["schedule"]["paused"])

        # A paused schedule creates nothing when a period is due.
        self.service.pump_schedules(time.time() + 100000)
        status, payload = self.request("GET", "/workflows/wf-life/schedule")
        self.assertTrue(payload["schedule"]["paused"])
        self.assertIsNone(payload["schedule"]["last_execution_id"])

        status, payload = self.request("POST", "/workflows/wf-life/schedule/resume", key="resume-life")
        self.assertEqual(200, status)
        self.assertFalse(payload["schedule"]["paused"])

    def test_declare_on_missing_workflow_is_not_found(self):
        status, payload = self.request(
            "POST",
            "/workflows/wf-ghost/schedule",
            {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            "decl-ghost",
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"]["code"])

    def test_pause_and_resume_without_schedule_are_not_found(self):
        self.create_workflow("wf-no-sched", None, "wf-no-sched")
        status, payload = self.request("POST", "/workflows/wf-no-sched/schedule/pause", key="pause-none")
        self.assertEqual(404, status)
        status, payload = self.request("POST", "/workflows/wf-no-sched/schedule/resume", key="resume-none")
        self.assertEqual(404, status)

    def test_invalid_declare_bodies_are_validation_errors(self):
        self.create_workflow("wf-validate", None, "wf-validate")
        cases = (
            b"{}",
            b'{"interval_seconds":-1,"input":{},"misfire_policy":"catch_up"}',
            b'{"interval_seconds":1.5,"input":{},"misfire_policy":"catch_up"}',
            b'{"interval_seconds":60,"cron":"* * * * *","input":{},"misfire_policy":"catch_up"}',
            b'{"cron":"* * * *","input":{},"misfire_policy":"catch_up"}',
            b'{"cron":"60 * * * *","input":{},"misfire_policy":"catch_up"}',
            b'{"interval_seconds":60,"input":{},"misfire_policy":"later"}',
            b'{"interval_seconds":60,"input":{},"misfire_policy":"catch_up","extra":1}',
        )
        for index, raw in enumerate(cases):
            status, payload = self.request(
                "POST", "/workflows/wf-validate/schedule", raw=raw, key=f"decl-bad-{index}"
            )
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", payload["error"]["code"])

    def test_non_finite_declare_body_is_rejected(self):
        status, payload = self.request(
            "POST",
            "/workflows/wf-validate/schedule",
            raw=b'{"interval_seconds":60,"input":{"v":NaN},"misfire_policy":"catch_up"}',
            key="decl-nan",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"]["code"])

    def test_pause_body_with_fields_is_validation_error(self):
        self.create_workflow(
            "wf-body",
            {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            "wf-body",
        )
        status, payload = self.request(
            "POST", "/workflows/wf-body/schedule/pause", raw=b'{"unexpected":true}', key="pause-bad"
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"]["code"])

    def test_key_reused_across_schedule_operations_conflicts(self):
        self.create_workflow(
            "wf-keys",
            {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            "wf-keys",
        )
        self.request("POST", "/workflows/wf-keys/schedule/pause", key="shared-sched-key")
        status, payload = self.request("POST", "/workflows/wf-keys/schedule/resume", key="shared-sched-key")
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"]["code"])

    def test_due_period_creates_execution_visible_over_http(self):
        self.create_workflow(
            "wf-fire",
            {"interval_seconds": 60, "input": {"order": 42}, "misfire_policy": "catch_up"},
            "wf-fire",
        )
        activation = self.activation("wf-fire")
        fired = [r for r in self.service.pump_schedules(activation + 90) if r["workflow_id"] == "wf-fire"]
        self.assertEqual(1, len(fired))
        execution_id = fired[0]["execution_id"]
        status, schedule = self.request("GET", "/workflows/wf-fire/schedule")
        self.assertEqual(execution_id, schedule["schedule"]["last_execution_id"])
        self.assertIsNotNone(schedule["schedule"]["last_fired_at"])
        status, history = self.request("GET", "/workflows/wf-fire/schedule/events")
        self.assertEqual(200, status)
        self.assertEqual(1, len(history["events"]))
        self.assertEqual(execution_id, history["events"][0]["execution_id"])
        self.create_workflow("wf-plain-hist", None, "wf-plain-hist")
        status, history = self.request("GET", "/workflows/wf-plain-hist/schedule/events")
        self.assertEqual(200, status)
        self.assertEqual([], history["events"])
        status, history = self.request("GET", "/workflows/wf-ghost/schedule/events")
        self.assertEqual(404, status)
        status, execution = self.request("GET", f"/executions/{execution_id}")
        self.assertEqual(200, status)
        self.assertEqual("running", execution["status"])
        self.assertEqual({"order": 42}, execution["input"])
        # The scheduled trigger is not part of the execution event stream.
        status, events = self.request("GET", f"/executions/{execution_id}/events")
        self.assertEqual(["execution_started"], [event["type"] for event in events["events"]])

    def test_repeated_period_request_is_idempotent_over_http(self):
        self.create_workflow(
            "wf-idem",
            {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            "wf-idem",
        )
        activation = self.activation("wf-idem")
        first = [r for r in self.service.pump_schedules(activation + 90) if r["workflow_id"] == "wf-idem"]
        second = [r for r in self.service.pump_schedules(activation + 95) if r["workflow_id"] == "wf-idem"]
        self.assertEqual(1, len(first))
        self.assertEqual([], second)

    def test_background_scheduler_fires_without_a_request(self):
        self.create_workflow(
            "wf-live",
            {"interval_seconds": 1, "input": {"live": True}, "misfire_policy": "catch_up"},
            "wf-live",
        )
        deadline = time.time() + 4
        execution_id = None
        while time.time() < deadline:
            status, schedule = self.request("GET", "/workflows/wf-live/schedule")
            execution_id = schedule["schedule"]["last_execution_id"]
            if execution_id:
                break
            time.sleep(0.05)
        self.assertIsNotNone(execution_id)
        status, execution = self.request("GET", f"/executions/{execution_id}")
        self.assertEqual({"live": True}, execution["input"])


if __name__ == "__main__":
    unittest.main()
