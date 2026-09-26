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
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

EMPTY_METRICS = {
    "status_distribution": {
        "running": 0,
        "completed": 0,
        "terminated": {"cancelled": 0, "rejected": 0, "retries_exhausted": 0, "timeout": 0},
    },
    "node_completions": {},
    "node_failures": {},
    "retry_consumption": {},
    "delivery_succeeded": 0,
    "delivery_failed": 0,
    "schedule_triggers": {},
}

METRICS_KEY_ORDER = [
    "status_distribution",
    "node_completions",
    "node_failures",
    "retry_consumption",
    "delivery_succeeded",
    "delivery_failed",
    "schedule_triggers",
]


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

    def test_fresh_tenant_gets_definite_zero_result(self):
        self.assertEqual(EMPTY_METRICS, self.service.metrics("acme"))

    def test_metrics_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.metrics("")

    def test_top_level_keys_appear_in_documented_order(self):
        self.assertEqual(METRICS_KEY_ORDER, list(self.service.metrics("acme")))

    def test_status_distribution_counts_each_outcome(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_workflow(
            {
                "id": "wf-approval",
                "nodes": [{"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}}],
            },
            "wf-approval",
            "acme",
        )
        self.service.create_workflow(
            {"id": "wf-retry", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": 1}]},
            "wf-retry",
            "acme",
        )
        # running
        self.service.create_execution({"id": "r-run", "workflow_id": "wf", "input": {}}, "r-run", "acme")
        # completed
        self.service.create_execution({"id": "r-done", "workflow_id": "wf", "input": {}}, "r-done", "acme")
        self.service.advance("r-done", {"output": {}}, "r-done-adv", "acme")
        # cancelled
        self.service.create_execution({"id": "r-cancel", "workflow_id": "wf", "input": {}}, "r-cancel", "acme")
        self.service.cancel("r-cancel", "r-cancel-c", "acme")
        # rejected
        self.service.create_execution({"id": "r-reject", "workflow_id": "wf-approval", "input": {}}, "r-reject", "acme")
        self.service.advance("r-reject", {"output": {}}, "r-reject-adv", "acme")
        self.service.decision(
            "r-reject", {"approver": "alice", "decision": "rejected", "reason": "no"}, "r-reject-dec", "acme"
        )
        # retries_exhausted
        self.service.create_execution({"id": "r-fail", "workflow_id": "wf-retry", "input": {}}, "r-fail", "acme")
        self.service.advance("r-fail", {"failure": {"reason": "boom"}}, "r-fail-1", "acme")
        self.service.advance("r-fail", {"failure": {"reason": "boom"}}, "r-fail-2", "acme")
        # timeout
        self.service.create_execution(
            {"id": "r-slow", "workflow_id": "wf", "input": {}, "timeout_seconds": 0.05}, "r-slow", "acme"
        )
        time.sleep(0.1)
        self.service.get_execution("r-slow", "acme")
        distribution = self.service.metrics("acme")["status_distribution"]
        self.assertEqual(1, distribution["running"])
        self.assertEqual(1, distribution["completed"])
        self.assertEqual(
            {"cancelled": 1, "rejected": 1, "retries_exhausted": 1, "timeout": 1},
            distribution["terminated"],
        )

    def test_completions_exclude_conditions_and_skips(self):
        self.service.create_workflow(
            {
                "id": "wf",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "vip", "equals": True},
                    {
                        "id": "a",
                        "kind": "task",
                        "depends_on": ["c"],
                        "run_if": {"condition_id": "c", "expected": True},
                    },
                    {
                        "id": "b",
                        "kind": "task",
                        "depends_on": ["c"],
                        "run_if": {"condition_id": "c", "expected": False},
                    },
                ],
            },
            "wf",
            "acme",
        )
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {"vip": True}}, "r", "acme")
        state = self.service.advance("r", {"output": {"v": 1}}, "adv", "acme")
        self.assertEqual("completed", state["status"])
        metrics = self.service.metrics("acme")
        # Only the completed task counts: the evaluated condition and the
        # skipped task are not completion facts.
        self.assertEqual({"a": 1}, metrics["node_completions"])
        self.assertEqual({}, metrics["node_failures"])
        self.assertEqual({}, metrics["retry_consumption"])

    def test_failures_and_retry_consumption_count_each_submission(self):
        self.service.create_workflow(
            {"id": "wf", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": 2}]},
            "wf",
            "acme",
        )
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"failure": {"reason": "one"}}, "f1", "acme")
        self.service.advance("r", {"failure": {"reason": "two"}}, "f2", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"a": 2}, metrics["node_failures"])
        self.assertEqual({"a": 2}, metrics["retry_consumption"])
        # The final failure exhausts the retries: it counts as a failure but
        # re-queues nothing.
        self.service.advance("r", {"failure": {"reason": "three"}}, "f3", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"a": 3}, metrics["node_failures"])
        self.assertEqual({"a": 2}, metrics["retry_consumption"])
        self.assertEqual(1, metrics["status_distribution"]["terminated"]["retries_exhausted"])

    def test_loop_body_nodes_accumulate_per_iteration(self):
        self.service.create_workflow(
            {
                "id": "wf",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                    {"id": "attempt", "kind": "task", "depends_on": ["check"], "retries": 1},
                    {
                        "id": "retry_loop",
                        "kind": "loop",
                        "depends_on": ["prepare"],
                        "entry": "attempt",
                        "condition": "check",
                        "max_iterations": 2,
                    },
                ],
            },
            "wf",
            "acme",
        )
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {"again": True}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        self.service.advance("r", {"failure": {"reason": "flaky"}}, "a2", "acme")
        self.service.advance("r", {"output": {"try": 1}}, "a3", "acme")
        self.service.advance("r", {"output": {"try": 2}}, "a4", "acme")
        metrics = self.service.metrics("acme")
        # The body task completed once per iteration; its single failure and
        # re-queue count alongside, and the loop's condition never does.
        self.assertEqual({"attempt": 2, "prepare": 1}, metrics["node_completions"])
        self.assertEqual({"attempt": 1}, metrics["node_failures"])
        self.assertEqual({"attempt": 1}, metrics["retry_consumption"])

    def test_delivery_outcomes_count_every_attempt(self):
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
            self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
            self.service.advance("r", {"output": {"v": 1}}, "adv", "acme")
            deadline = time.time() + 5
            while time.time() < deadline:
                records = self.service.deliveries("r", "acme")["deliveries"]
                if records and records[0]["attempt_count"] >= 2:
                    break
                time.sleep(0.05)
            metrics = self.service.metrics("acme")
            # The first attempt failed with a non-2xx status, the retry
            # succeeded: one of each, per attempt.
            self.assertEqual(1, metrics["delivery_succeeded"])
            self.assertEqual(1, metrics["delivery_failed"])
        finally:
            receiver.shutdown()
            receiver.server_close()

    def test_schedule_trigger_counts_once_per_period(self):
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
        # Settling the same period again counts nothing more.
        self.service.schedule_status("wf", "acme")
        self.assertEqual({"wf": 1}, self.service.metrics("acme")["schedule_triggers"])

    def test_migration_keeps_facts_under_the_same_names(self):
        nodes_v1 = [
            {"id": "a", "kind": "task", "depends_on": []},
            {"id": "b", "kind": "task", "depends_on": ["a"]},
        ]
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": nodes_v1}, "wf-1", "acme")
        self.service.create_workflow(
            {"id": "wf", "version": "v2", "nodes": nodes_v1 + [{"id": "c", "kind": "task", "depends_on": ["b"]}]},
            "wf-2",
            "acme",
        )
        self.service.create_execution(
            {"id": "r", "workflow_id": "wf", "version": "v1", "input": {}}, "r", "acme"
        )
        self.service.advance("r", {"output": {}}, "a1", "acme")
        self.service.migrate("r", {"version": "v2"}, "mig", "acme")
        self.service.advance("r", {"output": {}}, "a2", "acme")
        self.service.advance("r", {"output": {}}, "a3", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"a": 1, "b": 1, "c": 1}, metrics["node_completions"])
        self.assertEqual(1, metrics["status_distribution"]["completed"])

    def test_metrics_are_isolated_per_tenant(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "alpha")
        self.service.advance("r", {"output": {}}, "adv", "alpha")
        self.assertEqual(EMPTY_METRICS, self.service.metrics("beta"))

    def test_legacy_namespace_facts_are_not_reported_for_tenants(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r")
        self.service.advance("r", {"output": {}}, "adv")
        self.assertEqual(EMPTY_METRICS, self.service.metrics("acme"))

    def test_metrics_are_read_only(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        usage_before = self.service.usage("acme")
        events_before = self.service.events("r", "acme")
        state_before = self.service.get_execution("r", "acme")
        self.service.metrics("acme")
        self.assertEqual(usage_before, self.service.usage("acme"))
        self.assertEqual(events_before, self.service.events("r", "acme"))
        self.assertEqual(state_before, self.service.get_execution("r", "acme"))


class MetricsFilterServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "metrics-filter.db"))
        self.service.create_workflow(
            {"id": "wf", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf",
            "acme",
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_bounds_are_closed(self):
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        events = {event["type"]: event["occurred_at"] for event in self.service.events("r", "acme")}
        # The fact recorded exactly at each boundary is included.
        metrics = self.service.metrics("acme", since=events["node_completed"], until=events["node_completed"])
        self.assertEqual({"a": 1}, metrics["node_completions"])
        finished_at = events["execution_completed"]
        metrics = self.service.metrics("acme", since=finished_at, until=finished_at)
        self.assertEqual(1, metrics["status_distribution"]["completed"])

    def test_window_selects_only_inside_facts(self):
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        events = self.service.events("r", "acme")
        started_at = events[0]["occurred_at"]
        metrics = self.service.metrics("acme", until=started_at)
        # The execution's current status was established by its completion,
        # which falls outside the window, so it contributes nothing here.
        self.assertEqual(0, metrics["status_distribution"]["running"])
        self.assertEqual(0, metrics["status_distribution"]["completed"])
        self.assertEqual({}, metrics["node_completions"])
        # A window covering the completion sees it.
        completed_at = events[-1]["occurred_at"]
        metrics = self.service.metrics("acme", since=started_at, until=completed_at)
        self.assertEqual(1, metrics["status_distribution"]["completed"])
        self.assertEqual({"a": 1}, metrics["node_completions"])

    def test_since_after_until_is_definite_empty_result(self):
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        metrics = self.service.metrics(
            "acme", since="2026-09-27T00:00:00.000Z", until="2026-09-26T00:00:00.000Z"
        )
        self.assertEqual(EMPTY_METRICS, metrics)

    def test_malformed_bounds_are_validation_errors(self):
        for value in ("yesterday", "2026-13-01T00:00:00Z", "2026-01-01T00:00:00+01:00", "2026-01-01", ""):
            with self.assertRaises(ValidationError):
                self.service.metrics("acme", since=value)
            with self.assertRaises(ValidationError):
                self.service.metrics("acme", until=value)

    def test_filtered_query_is_isolated_and_read_only(self):
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        usage_before = self.service.usage("acme")
        self.assertEqual(
            EMPTY_METRICS,
            self.service.metrics("beta", since="2026-01-01T00:00:00.000Z"),
        )
        self.assertEqual(usage_before, self.service.usage("acme"))

    def test_schedule_triggers_are_filtered_by_trigger_time(self):
        self.service.create_workflow(
            {
                "id": "sched",
                "nodes": TASK,
                "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"},
            },
            "sched",
            "acme",
        )
        time.sleep(1.2)
        self.service.schedule_status("sched", "acme")
        self.assertEqual({}, self.service.metrics("acme", until="2000-01-01T00:00:00.000Z")["schedule_triggers"])
        self.assertEqual(
            {"sched": 1}, self.service.metrics("acme", since="2000-01-01T00:00:00.000Z")["schedule_triggers"]
        )


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

    def test_metrics_over_http(self):
        headers = {"X-Tenant-Id": "http-acme"}
        status, data = self.call("GET", "/metrics", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(EMPTY_METRICS, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        # One compact line: the body is exactly the compact encoding plus a
        # single newline, with top-level keys in the documented order.
        parsed = json.loads(data)
        self.assertEqual(METRICS_KEY_ORDER, list(parsed))
        self.assertEqual(
            json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode() + b"\n",
            data,
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
        self.assertEqual({"a": 1}, metrics["node_completions"])

    def test_metrics_do_not_leak_across_tenants(self):
        headers = {"X-Tenant-Id": "http-other"}
        status, data = self.call("GET", "/metrics", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(EMPTY_METRICS, json.loads(data))


class MetricsExportHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-export.db"))
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

    def test_export_requires_a_tenant_header(self):
        for headers in ({}, {"X-Tenant-Id": ""}):
            status, data = self.call("GET", "/metrics/export", headers=headers)
            self.assertEqual(400, status)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_export_rejects_bad_filters_and_unknown_parameters(self):
        headers = {"X-Tenant-Id": "export-acme"}
        for path in (
            "/metrics/export?since=nope",
            "/metrics/export?until=2026-02-30T00:00:00Z",
            "/metrics/export?unknown=1",
            "/metrics?unknown=1",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_export_renders_prometheus_text(self):
        headers = {"X-Tenant-Id": "export-acme"}
        self.call("POST", "/workflows", {"id": "wf-exp", "nodes": TASK}, key="wf-exp", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-exp", "workflow_id": "wf-exp", "input": {}},
            key="r-exp",
            headers=headers,
        )
        self.call("POST", "/executions/r-exp/advance", {"output": {}}, key="adv-exp", headers=headers)
        status, data = self.call("GET", "/metrics/export", headers=headers)
        self.assertEqual(200, status)
        text = data.decode()
        self.assertTrue(text.endswith("\n"))
        self.assertFalse(text.endswith("\n\n"))
        lines = text.splitlines()
        self.assertEqual(
            [
                "chronicleflow_status_distribution{status=\"completed\"} 1",
                "chronicleflow_status_distribution{status=\"running\"} 0",
                "chronicleflow_status_distribution{reason=\"cancelled\",status=\"terminated\"} 0",
                "chronicleflow_status_distribution{reason=\"rejected\",status=\"terminated\"} 0",
                "chronicleflow_status_distribution{reason=\"retries_exhausted\",status=\"terminated\"} 0",
                "chronicleflow_status_distribution{reason=\"timeout\",status=\"terminated\"} 0",
                "chronicleflow_node_completions{node=\"a\"} 1",
                "chronicleflow_node_failures 0",
                "chronicleflow_retry_consumption 0",
                "chronicleflow_delivery_succeeded 0",
                "chronicleflow_delivery_failed 0",
                "chronicleflow_schedule_triggers 0",
            ],
            lines,
        )

    def test_export_honors_the_time_window(self):
        headers = {"X-Tenant-Id": "export-acme"}
        status, data = self.call(
            "GET", "/metrics/export?until=2000-01-01T00:00:00.000Z", headers=headers
        )
        self.assertEqual(200, status)
        self.assertIn("chronicleflow_node_completions 0\n", data.decode())
        status, data = self.call(
            "GET",
            "/metrics/export?since=2026-09-27T00:00:00.000Z&until=2026-09-26T00:00:00.000Z",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertIn("chronicleflow_status_distribution{status=\"completed\"} 0\n", data.decode())

    def test_export_does_not_leak_across_tenants(self):
        status, data = self.call("GET", "/metrics/export", headers={"X-Tenant-Id": "export-other"})
        self.assertEqual(200, status)
        self.assertIn("chronicleflow_status_distribution{status=\"completed\"} 0\n", data.decode())


if __name__ == "__main__":
    unittest.main()
