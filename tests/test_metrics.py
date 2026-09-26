import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import TERMINATION_REASONS, ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


def empty_metrics():
    return {
        "status_distribution": {
            "completed": 0,
            "running": 0,
            "terminated": {reason: 0 for reason in TERMINATION_REASONS},
        },
        "nodes_completed": {},
        "nodes_failed": {},
        "retries_consumed": {},
        "deliveries_succeeded": {},
        "deliveries_failed": {},
        "schedule_triggered": {},
    }


class _Receiver(BaseHTTPRequestHandler):
    fail_once = False
    calls = 0

    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        _Receiver.calls += 1
        code = 500 if (_Receiver.fail_once and _Receiver.calls == 1) else 200
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()


class MetricsServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "metrics.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_metrics_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.metrics("")

    def test_tenant_without_facts_gets_definite_zero_result(self):
        self.assertEqual(empty_metrics(), self.service.metrics("acme"))

    def test_status_distribution_counts_each_bucket(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "run-running", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "run-done", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self.service.advance("run-done", {"output": {"v": 1}}, "adv-done", "acme")
        self.service.create_execution({"id": "run-cancelled", "workflow_id": "wf", "input": {}}, "r3", "acme")
        self.service.cancel("run-cancelled", "cancel", "acme")
        distribution = self.service.metrics("acme")["status_distribution"]
        self.assertEqual(1, distribution["running"])
        self.assertEqual(1, distribution["completed"])
        self.assertEqual({"cancelled": 1, "rejected": 0, "retries_exhausted": 0, "timeout": 0}, distribution["terminated"])

    def test_terminated_distribution_breaks_down_by_reason(self):
        self.service.create_workflow(
            {"id": "wf-retry", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": 1}]},
            "wf-retry",
            "acme",
        )
        self.service.create_workflow(
            {
                "id": "wf-approval",
                "nodes": [{"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}}],
            },
            "wf-approval",
            "acme",
        )
        self.service.create_execution({"id": "run-retry", "workflow_id": "wf-retry", "input": {}}, "r1", "acme")
        self.service.advance("run-retry", {"failure": {"reason": "boom"}}, "f1", "acme")
        self.service.advance("run-retry", {"failure": {"reason": "boom"}}, "f2", "acme")
        self.service.create_execution({"id": "run-rejected", "workflow_id": "wf-approval", "input": {}}, "r2", "acme")
        self.service.advance("run-rejected", {"output": {}}, "park", "acme")
        self.service.decision(
            "run-rejected",
            {"approver": "alice", "decision": "rejected", "reason": "no"},
            "decide",
            "acme",
        )
        self.service.create_execution(
            {"id": "run-timeout", "workflow_id": "wf-retry", "input": {}, "timeout_seconds": 0.05},
            "r3",
            "acme",
        )
        time.sleep(0.1)
        # The timeout fact is persisted by the next observation of the execution.
        self.service.get_execution("run-timeout", "acme")
        terminated = self.service.metrics("acme")["status_distribution"]["terminated"]
        self.assertEqual(
            {"cancelled": 0, "rejected": 1, "retries_exhausted": 1, "timeout": 1},
            terminated,
        )

    def test_metrics_query_does_not_settle_a_due_timeout(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution(
            {"id": "run", "workflow_id": "wf", "input": {}, "timeout_seconds": 0.05},
            "r",
            "acme",
        )
        time.sleep(0.1)
        # Only persisted facts are summarized: the due timeout has not been
        # settled by any operation, so the execution still counts as running.
        distribution = self.service.metrics("acme")["status_distribution"]
        self.assertEqual(1, distribution["running"])
        self.assertEqual(0, distribution["terminated"]["timeout"])

    def test_node_completions_count_tasks_not_conditions_or_skips(self):
        nodes = [
            {"id": "a", "kind": "task", "depends_on": []},
            {"id": "c", "kind": "condition", "depends_on": ["a"], "path": "flag", "equals": True},
            {"id": "b", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True}},
            {"id": "z", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": False}},
        ]
        self.service.create_workflow({"id": "wf", "nodes": nodes}, "wf", "acme")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {"flag": True}}, "r", "acme")
        self.service.advance("run", {"output": {"v": 1}}, "adv-1", "acme")
        self.service.advance("run", {"output": {"v": 2}}, "adv-2", "acme")
        metrics = self.service.metrics("acme")
        # The condition evaluation and the run_if skip are not completions.
        self.assertEqual({"a": 1, "b": 1}, metrics["nodes_completed"])
        self.assertEqual({}, metrics["nodes_failed"])
        self.assertEqual({}, metrics["retries_consumed"])

    def test_failures_and_retries_are_counted_per_submission(self):
        nodes = [{"id": "a", "kind": "task", "depends_on": [], "retries": 2}]
        self.service.create_workflow({"id": "wf", "nodes": nodes}, "wf", "acme")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("run", {"failure": {"reason": "x"}}, "f1", "acme")
        self.service.advance("run", {"failure": {"reason": "x"}}, "f2", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"a": 2}, metrics["nodes_failed"])
        self.assertEqual({"a": 2}, metrics["retries_consumed"])
        self.assertEqual(1, metrics["status_distribution"]["running"])
        self.service.advance("run", {"failure": {"reason": "x"}}, "f3", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"a": 3}, metrics["nodes_failed"])
        # The last failure exhausts the retries, so no further re-queue happens.
        self.assertEqual({"a": 2}, metrics["retries_consumed"])
        self.assertEqual(1, metrics["status_distribution"]["terminated"]["retries_exhausted"])

    def test_loop_body_rounds_accumulate_per_iteration(self):
        nodes = [
            {"id": "attempt", "kind": "task", "depends_on": ["keep"], "retries": 1},
            {"id": "keep", "kind": "condition", "depends_on": [], "path": "go", "equals": True},
            {
                "id": "loop",
                "kind": "loop",
                "depends_on": [],
                "entry": "attempt",
                "condition": "keep",
                "max_iterations": 2,
            },
        ]
        self.service.create_workflow({"id": "wf", "nodes": nodes}, "wf", "acme")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {"go": True}}, "r", "acme")
        self.service.advance("run", {"failure": {"reason": "x"}}, "f1", "acme")
        self.service.advance("run", {"output": {"v": 1}}, "a1", "acme")
        self.service.advance("run", {"output": {"v": 2}}, "a2", "acme")
        metrics = self.service.metrics("acme")
        # Each iteration of the body task records its own completion fact.
        self.assertEqual({"attempt": 2}, metrics["nodes_completed"])
        self.assertEqual({"attempt": 1}, metrics["nodes_failed"])
        self.assertEqual({"attempt": 1}, metrics["retries_consumed"])

    def test_migration_keeps_facts_under_the_same_execution(self):
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK}, "wf", "acme")
        self.service.create_workflow(
            {
                "id": "wf",
                "version": "v2",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            "wf2",
            "acme",
        )
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("run", {"output": {"v": 1}}, "a1", "acme")
        self.service.migrate("run", {"version": "v2"}, "mig", "acme")
        self.service.advance("run", {"output": {"v": 2}}, "a2", "acme")
        metrics = self.service.metrics("acme")
        # Facts recorded before and after the migration accumulate together.
        self.assertEqual({"a": 1, "b": 1}, metrics["nodes_completed"])
        self.assertEqual(1, metrics["status_distribution"]["completed"])

    def test_delivery_attempts_count_each_try_by_target(self):
        _Receiver.calls = 0
        _Receiver.fail_once = True
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {
                    "id": "wf",
                    "nodes": TASK,
                    "subscriptions": [{"url": url, "events": ["node_completed"], "max_attempts": 2}],
                },
                "wf",
                "acme",
            )
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
            self.service.advance("run", {"output": {"v": 1}}, "adv", "acme")
            deadline = time.time() + 5
            while time.time() < deadline:
                records = self.service.deliveries("run", "acme")["deliveries"]
                if records and records[0]["attempt_count"] >= 2:
                    break
                time.sleep(0.05)
            metrics = self.service.metrics("acme")
            # The first try failed with a 500, the retry succeeded: one of each.
            self.assertEqual({url: 1}, metrics["deliveries_succeeded"])
            self.assertEqual({url: 1}, metrics["deliveries_failed"])
        finally:
            receiver.shutdown()
            receiver.server_close()

    def test_unreachable_target_counts_failed_attempts(self):
        self.service.create_workflow(
            {
                "id": "wf",
                "nodes": TASK,
                "subscriptions": [
                    {"url": "http://127.0.0.1:1/hook", "events": ["node_completed"], "max_attempts": 2, "timeout_seconds": 1}
                ],
            },
            "wf",
            "acme",
        )
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("run", {"output": {"v": 1}}, "adv", "acme")
        deadline = time.time() + 5
        while time.time() < deadline:
            records = self.service.deliveries("run", "acme")["deliveries"]
            if records and records[0]["attempt_count"] >= 2:
                break
            time.sleep(0.05)
        metrics = self.service.metrics("acme")
        self.assertEqual({}, metrics["deliveries_succeeded"])
        self.assertEqual({"http://127.0.0.1:1/hook": 2}, metrics["deliveries_failed"])

    def test_schedule_trigger_counts_each_settled_period_once(self):
        self.service.create_workflow(
            {
                "id": "wf",
                "nodes": TASK,
                "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"},
            },
            "wf",
            "acme",
        )
        time.sleep(1.3)
        self.service.schedule_status("wf", "acme")
        # Settling the same period again does not count it twice.
        self.service.schedule_status("wf", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"wf": 1}, metrics["schedule_triggered"])
        self.assertEqual(1, metrics["status_distribution"]["running"])

    def test_metrics_are_isolated_per_tenant(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "alpha")
        self.assertEqual(empty_metrics(), self.service.metrics("beta"))

    def test_metrics_query_writes_nothing(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("run", {"output": {"v": 1}}, "adv", "acme")
        before = {
            table: self.service.store.connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            for table in ("events", "checkpoints", "usage_records", "deliveries", "schedule_triggers")
        }
        first = self.service.metrics("acme")
        second = self.service.metrics("acme")
        self.assertEqual(first, second)
        after = {
            table: self.service.store.connection.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            for table in ("events", "checkpoints", "usage_records", "deliveries", "schedule_triggers")
        }
        self.assertEqual(before, after)

    def test_group_ordering_is_ascending_by_identifier(self):
        nodes = [
            {"id": "zeta", "kind": "task", "depends_on": []},
            {"id": "alpha", "kind": "task", "depends_on": ["zeta"]},
        ]
        self.service.create_workflow({"id": "wf", "nodes": nodes}, "wf", "acme")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("run", {"output": {}}, "a1", "acme")
        self.service.advance("run", {"output": {}}, "a2", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual(["alpha", "zeta"], list(metrics["nodes_completed"]))


class MetricsHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-metrics.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, json.dumps(body) if body is not None else None, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_metrics_require_a_tenant_header(self):
        status, data = self.call("GET", "/metrics")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/metrics", headers={"X-Tenant-Id": ""})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_metrics_over_http_shape_and_key_order(self):
        headers = {"X-Tenant-Id": "http-acme"}
        status, data = self.call("GET", "/metrics", headers=headers)
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual(empty_metrics(), json.loads(data))
        self.assertEqual(
            [
                "status_distribution",
                "nodes_completed",
                "nodes_failed",
                "retries_consumed",
                "deliveries_succeeded",
                "deliveries_failed",
                "schedule_triggered",
            ],
            list(json.loads(data)),
        )
        self.call("POST", "/workflows", {"id": "wf-http", "nodes": TASK}, key="wf-http", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-http", "workflow_id": "wf-http", "input": {}},
            key="r-http",
            headers=headers,
        )
        self.call(
            "POST",
            "/executions/r-http/advance",
            {"output": {"v": 1}},
            key="adv-http",
            headers=headers,
        )
        status, data = self.call("GET", "/metrics", headers=headers)
        self.assertEqual(200, status)
        metrics = json.loads(data)
        self.assertEqual(1, metrics["status_distribution"]["completed"])
        self.assertEqual({"a": 1}, metrics["nodes_completed"])
        # A metrics query is read-only: it records no usage of its own.
        status, data = self.call("GET", "/usage", headers=headers)
        usage = {entry["type"]: entry["count"] for entry in json.loads(data)["usage"]}
        self.assertEqual({"workflow_created": 1, "execution_started": 1}, usage)

    def test_metrics_do_not_leak_across_tenants(self):
        status, data = self.call("GET", "/metrics", headers={"X-Tenant-Id": "http-other"})
        self.assertEqual(200, status)
        self.assertEqual(empty_metrics(), json.loads(data))


if __name__ == "__main__":
    unittest.main()
