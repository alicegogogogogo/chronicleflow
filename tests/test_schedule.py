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
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-schedule.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def create_workflow(self, workflow_id, schedule=None):
        body = {"id": workflow_id, "nodes": [{"id": "a", "kind": "task", "depends_on": []}]}
        if schedule is not None:
            body["schedule"] = schedule
        status, data = self.call("POST", "/workflows", body, key=f"create-{workflow_id}")
        self.assertEqual(201, status, data)

    def test_interval_schedule_fires_and_status_reports(self):
        self.create_workflow(
            "wf-s1",
            {"interval_seconds": 1, "input": {"n": 1}, "missed_policy": "catch_up"},
        )
        status, data = self.call("GET", "/workflows/wf-s1/schedule")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        payload = json.loads(data)
        self.assertEqual(
            {
                "interval_seconds": 1,
                "input": {"n": 1},
                "missed_policy": "catch_up",
                "paused": False,
                "last_triggered_at": None,
                "last_execution_id": None,
            },
            payload["schedule"],
        )

        time.sleep(1.3)
        status, data = self.call("GET", "/workflows/wf-s1/schedule")
        self.assertEqual(200, status)
        plan = json.loads(data)["schedule"]
        self.assertEqual("wf-s1-scheduled-i:1", plan["last_execution_id"])
        self.assertIsNotNone(plan["last_triggered_at"])

        # The created execution looks exactly like a manually created one.
        status, data = self.call("GET", "/executions/wf-s1-scheduled-i:1")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("running", state["status"])
        self.assertEqual({"n": 1}, state["input"])
        self.assertEqual("wf-s1", state["workflow_id"])
        status, data = self.call("GET", "/executions/wf-s1-scheduled-i:1/events")
        self.assertEqual(["execution_started"], [event["type"] for event in json.loads(data)["events"]])

        # It advances with the usual semantics.
        status, data = self.call(
            "POST", "/executions/wf-s1-scheduled-i:1/advance", {"output": {"done": True}}, key="adv-s1"
        )
        self.assertEqual(200, status)
        self.assertEqual("completed", json.loads(data)["status"])

    def test_scheduled_execution_has_manual_state_shape(self):
        self.create_workflow("wf-shape", {"interval_seconds": 1, "input": {}, "missed_policy": "skip"})
        self.call(
            "POST",
            "/executions",
            {"id": "run-shape-manual", "workflow_id": "wf-shape", "input": {}},
            key="ex-shape-manual",
        )
        status, data = self.call("GET", "/executions/run-shape-manual")
        manual_keys = set(json.loads(data))
        time.sleep(1.3)
        status, data = self.call("GET", "/workflows/wf-shape/schedule")
        scheduled_id = json.loads(data)["schedule"]["last_execution_id"]
        self.assertIsNotNone(scheduled_id)
        status, data = self.call("GET", f"/executions/{scheduled_id}")
        self.assertEqual(200, status)
        self.assertEqual(manual_keys, set(json.loads(data)))

    def test_query_without_schedule_is_definite_empty_result(self):
        self.create_workflow("wf-nosched")
        status, data = self.call("GET", "/workflows/wf-nosched/schedule")
        self.assertEqual(200, status)
        self.assertEqual({"schedule": None}, json.loads(data))

    def test_schedule_operations_on_missing_workflow_are_not_found(self):
        status, data = self.call("GET", "/workflows/nope/schedule")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/workflows/nope/schedule/pause", {}, key="p-nope")
        self.assertEqual(404, status)
        status, data = self.call("POST", "/workflows/nope/schedule/resume", {}, key="r-nope")
        self.assertEqual(404, status)
        status, data = self.call(
            "PUT",
            "/workflows/nope/schedule",
            {"interval_seconds": 5, "input": {}, "missed_policy": "skip"},
            key="u-nope",
        )
        self.assertEqual(404, status)

    def test_pause_resume_without_schedule_are_not_found(self):
        status, data = self.call("POST", "/workflows/wf-nosched/schedule/pause", {}, key="p-nosched")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/workflows/wf-nosched/schedule/resume", {}, key="r-nosched")
        self.assertEqual(404, status)

    def test_invalid_schedule_declarations_are_rejected(self):
        bad_schedules = [
            {"interval_seconds": 0, "input": {}, "missed_policy": "skip"},
            {"interval_seconds": -3, "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1.5, "input": {}, "missed_policy": "skip"},
            {"interval_seconds": "1", "input": {}, "missed_policy": "skip"},
            {"interval_seconds": True, "input": {}, "missed_policy": "skip"},
            {"cron": "* * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "* * * * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "60 * * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "* 24 * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "* * 0 * *", "input": {}, "missed_policy": "skip"},
            {"cron": "* * * 13 *", "input": {}, "missed_policy": "skip"},
            {"cron": "* * * * 8", "input": {}, "missed_policy": "skip"},
            {"cron": "abc * * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "*/0 * * * *", "input": {}, "missed_policy": "skip"},
            {"cron": "5-1 * * * *", "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "cron": "* * * * *", "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "missed_policy": "skip"},
            {"interval_seconds": 1, "input": {}},
            {"interval_seconds": 1, "input": {}, "missed_policy": "wait"},
            {"interval_seconds": 1, "input": {}, "missed_policy": "skip", "extra": 1},
            {"interval_seconds": 1, "input": [], "missed_policy": "skip"},
        ]
        for index, schedule in enumerate(bad_schedules):
            with self.subTest(schedule=schedule):
                status, data = self.call(
                    "POST",
                    "/workflows",
                    {
                        "id": f"wf-bad-{index}",
                        "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
                        "schedule": schedule,
                    },
                    key=f"wf-bad-{index}",
                )
                self.assertEqual(400, status, schedule)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])
                # Nothing was partially written.
                status, _ = self.call("GET", f"/workflows/wf-bad-{index}/schedule")
                self.assertEqual(404, status)

    def test_cron_schedule_is_accepted_and_reported(self):
        self.create_workflow(
            "wf-cron",
            {"cron": "*/5 9-17 1,15 * 1-5", "input": {"mode": "cron"}, "missed_policy": "catch_up"},
        )
        status, data = self.call("GET", "/workflows/wf-cron/schedule")
        self.assertEqual(200, status)
        payload = json.loads(data)
        self.assertEqual("*/5 9-17 1,15 * 1-5", payload["schedule"]["cron"])
        self.assertEqual("catch_up", payload["schedule"]["missed_policy"])
        self.assertIsNone(payload["schedule"]["last_execution_id"])

    def test_pause_halts_and_catch_up_fires_only_latest_missed(self):
        self.create_workflow("wf-s2", {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"})
        status, data = self.call("POST", "/workflows/wf-s2/schedule/pause", {}, key="pause-s2")
        self.assertEqual(200, status)
        self.assertTrue(json.loads(data)["schedule"]["paused"])
        time.sleep(2.4)
        status, data = self.call("GET", "/workflows/wf-s2/schedule")
        self.assertIsNone(json.loads(data)["schedule"]["last_execution_id"])
        status, data = self.call("POST", "/workflows/wf-s2/schedule/resume", {}, key="resume-s2")
        self.assertEqual(200, status)
        plan = json.loads(data)["schedule"]
        self.assertFalse(plan["paused"])
        # Only the most recent missed period is made up, exactly once.
        self.assertEqual("wf-s2-scheduled-i:2", plan["last_execution_id"])
        status, _ = self.call("GET", "/executions/wf-s2-scheduled-i:1")
        self.assertEqual(404, status)
        status, _ = self.call("GET", "/executions/wf-s2-scheduled-i:2")
        self.assertEqual(200, status)

    def test_pause_and_skip_policy_never_makes_up_missed_periods(self):
        self.create_workflow("wf-s3", {"interval_seconds": 1, "input": {}, "missed_policy": "skip"})
        status, _ = self.call("POST", "/workflows/wf-s3/schedule/pause", {}, key="pause-s3")
        self.assertEqual(200, status)
        time.sleep(2.4)
        status, data = self.call("POST", "/workflows/wf-s3/schedule/resume", {}, key="resume-s3")
        self.assertEqual(200, status)
        plan = json.loads(data)["schedule"]
        self.assertIsNone(plan["last_execution_id"])
        self.assertIsNone(plan["last_triggered_at"])
        # New periods still fire after the resume.
        time.sleep(1.3)
        status, data = self.call("GET", "/workflows/wf-s3/schedule")
        plan = json.loads(data)["schedule"]
        self.assertTrue(plan["last_execution_id"].startswith("wf-s3-scheduled-i:"))

    def test_update_schedule_on_existing_workflow(self):
        self.create_workflow("wf-s4")
        status, data = self.call(
            "PUT",
            "/workflows/wf-s4/schedule",
            {"interval_seconds": 5, "input": {"x": 1}, "missed_policy": "skip"},
            key="upd-s4",
        )
        self.assertEqual(200, status)
        plan = json.loads(data)["schedule"]
        self.assertEqual(
            {
                "interval_seconds": 5,
                "input": {"x": 1},
                "missed_policy": "skip",
                "paused": False,
                "last_triggered_at": None,
                "last_execution_id": None,
            },
            plan,
        )
        # Replacing the plan works the same way.
        status, data = self.call(
            "PUT",
            "/workflows/wf-s4/schedule",
            {"cron": "0 9 * * 1", "input": {}, "missed_policy": "catch_up"},
            key="upd-s4b",
        )
        self.assertEqual(200, status)
        self.assertEqual("0 9 * * 1", json.loads(data)["schedule"]["cron"])
        # Invalid replacement plans are rejected and the old plan survives.
        status, data = self.call(
            "PUT",
            "/workflows/wf-s4/schedule",
            {"interval_seconds": 1, "cron": "* * * * *", "input": {}, "missed_policy": "skip"},
            key="upd-s4c",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/workflows/wf-s4/schedule")
        self.assertEqual("0 9 * * 1", json.loads(data)["schedule"]["cron"])

    def test_schedule_operation_body_validation(self):
        self.create_workflow("wf-s5", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        for index, raw in enumerate((b'{"note":"x"}', b"[1]", b'"pause"', b'{"v":NaN}')):
            status, data = self.call("POST", "/workflows/wf-s5/schedule/pause", raw=raw, key=f"pb-{index}")
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
            status, data = self.call("POST", "/workflows/wf-s5/schedule/resume", raw=raw, key=f"rb-{index}")
            self.assertEqual(400, status, raw)
        status, data = self.call("POST", "/workflows/wf-s5/schedule/pause", {})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_idempotency_key_reuse_across_schedule_operations_conflicts(self):
        self.create_workflow("wf-s6", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        status, data = self.call("POST", "/workflows/wf-s6/schedule/pause", {}, key="sched-shared")
        self.assertEqual(200, status)
        # Repeating the same operation with the same key replays the result.
        status, data = self.call("POST", "/workflows/wf-s6/schedule/pause", {}, key="sched-shared")
        self.assertEqual(200, status)
        self.assertTrue(json.loads(data)["schedule"]["paused"])
        status, data = self.call("POST", "/workflows/wf-s6/schedule/resume", {}, key="sched-shared")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call(
            "PUT",
            "/workflows/wf-s6/schedule",
            {"interval_seconds": 30, "input": {}, "missed_policy": "skip"},
            key="sched-shared",
        )
        self.assertEqual(409, status)


if __name__ == "__main__":
    unittest.main()
