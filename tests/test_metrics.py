import http.client
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]
TWO_TASKS = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]

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


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _parse(raw: str):
    from chronicleflow.service import _parse_timestamp

    return _parse_timestamp(raw, "window")


class MetricsFilterServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "filter.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _timed_workflow(self, workflow_id="wf", nodes=None):
        self.service.create_workflow({"id": workflow_id, "nodes": nodes or TWO_TASKS}, workflow_id, "acme")

    def test_invalid_timestamp_raises_validation_error(self):
        for raw in ("not-a-time", "2026-01-01", "2026-01-01T00:00:00", "2026-01-01T00:00:00+00:00", ""):
            with self.assertRaises(ValidationError):
                self.service.metrics("acme", _parse(raw))
            with self.assertRaises(ValidationError):
                self.service.metrics("acme", None, _parse(raw))

    def test_unbounded_window_matches_all_facts(self):
        self._timed_workflow()
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        self.service.advance("r", {"output": {}}, "a2", "acme")
        self.assertEqual(self.service.metrics("acme"), self.service.metrics("acme", None, None))

    def test_window_is_closed_at_both_endpoints(self):
        self._timed_workflow()
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        fact_time = self.service.events("r", "acme")[1]["occurred_at"]
        # A fact whose time equals either endpoint is counted.
        metrics = self.service.metrics("acme", _ts(fact_time), _ts(fact_time))
        self.assertEqual({"a": 1}, metrics["node_completions"])
        self.assertEqual(0, metrics["status_distribution"]["completed"])

    def test_since_after_until_is_a_definite_empty_result(self):
        self._timed_workflow()
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        later = _ts("2099-01-01T00:00:00.000Z")
        earlier = _ts("2000-01-01T00:00:00.000Z")
        self.assertEqual(EMPTY_METRICS, self.service.metrics("acme", later, earlier))
        export = self.service.metrics_export("acme", later, earlier)
        for line in export.splitlines():
            self.assertTrue(line.endswith(" 0"), line)

    def test_status_facts_use_the_status_establishing_event_time(self):
        self._timed_workflow()
        # A completed execution counts when it completed, not when it started.
        self.service.create_execution({"id": "r-done", "workflow_id": "wf", "input": {}}, "r-done", "acme")
        started = self.service.events("r-done", "acme")[0]["occurred_at"]
        self.service.advance("r-done", {"output": {}}, "d1", "acme")
        self.service.advance("r-done", {"output": {}}, "d2", "acme")
        completed_at = self.service.events("r-done", "acme")[-1]["occurred_at"]
        # A running execution counts at its start time.
        self.service.create_execution({"id": "r-run", "workflow_id": "wf", "input": {}}, "r-run", "acme")
        running_started = self.service.events("r-run", "acme")[0]["occurred_at"]
        # A cancelled execution counts at termination.
        self.service.create_execution({"id": "r-stop", "workflow_id": "wf", "input": {}}, "r-stop", "acme")
        self.service.cancel("r-stop", "stop", "acme")
        cancelled_at = self.service.events("r-stop", "acme")[-1]["occurred_at"]

        # A window ending at the start of the soon-to-complete execution sees
        # nothing of it: its status fact is the later completion event.
        at_start = self.service.metrics("acme", None, _ts(started))
        self.assertEqual(0, at_start["status_distribution"]["completed"])
        # The running execution is visible at its own start.
        at_running = self.service.metrics("acme", _ts(running_started), _ts(running_started))
        self.assertEqual(1, at_running["status_distribution"]["running"])
        # The completion lands at its completion time.
        at_done = self.service.metrics("acme", _ts(completed_at), _ts(completed_at))
        self.assertEqual(1, at_done["status_distribution"]["completed"])
        # The cancellation lands at its termination time.
        at_cancel = self.service.metrics("acme", _ts(cancelled_at), _ts(cancelled_at))
        self.assertEqual(
            1, at_cancel["status_distribution"]["terminated"]["cancelled"]
        )

    def test_filter_excludes_facts_outside_window(self):
        self._timed_workflow()
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        boundary = self.service.events("r", "acme")[1]["occurred_at"]
        self.service.advance("r", {"output": {}}, "a2", "acme")
        metrics = self.service.metrics("acme", None, _ts(boundary))
        self.assertEqual({"a": 1}, metrics["node_completions"])
        metrics = self.service.metrics("acme", _ts(boundary), None)
        self.assertEqual({"a": 1, "b": 1}, metrics["node_completions"])

    def test_filter_does_not_change_billing_or_usage(self):
        self._timed_workflow()
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "a1", "acme")
        usage_before = self.service.usage("acme")
        bill_before = self.service.bill("acme")
        self.service.metrics("acme", _ts("2000-01-01T00:00:00Z"), _ts("2001-01-01T00:00:00Z"))
        self.service.metrics_export("acme", _ts("2000-01-01T00:00:00Z"), _ts("2001-01-01T00:00:00Z"))
        self.assertEqual(usage_before, self.service.usage("acme"))
        self.assertEqual(bill_before, self.service.bill("acme"))

    def test_delivery_and_schedule_facts_are_windowed_by_their_own_times(self):
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        _Receiver.calls = 0
        _Receiver.fail_once = False
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            # One workflow settles schedule periods; another records a delivery.
            self.service.create_workflow(
                {"id": "wf-sched", "nodes": TASK,
                 "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"}},
                "wf-sched",
                "acme",
            )
            self.service.create_workflow(
                {"id": "wf-hook", "nodes": TASK,
                 "subscriptions": [{"url": url, "events": ["node_completed"]}]},
                "wf-hook",
                "acme",
            )
            time.sleep(1.3)
            self.service.schedule_status("wf-sched", "acme")
            self.service.create_execution({"id": "r", "workflow_id": "wf-hook", "input": {}}, "r", "acme")
            self.service.advance("r", {"output": {}}, "adv", "acme")
            deadline = time.time() + 5
            occurred_at = None
            while time.time() < deadline:
                deliveries = self.service.deliveries("r", "acme")["deliveries"]
                if deliveries:
                    occurred_at = deliveries[0]["occurred_at"]
                    break
                time.sleep(0.05)
            self.assertIsNotNone(occurred_at)
            trigger_rows = self.service.store.connection.execute(
                "SELECT triggered_at FROM schedule_triggers WHERE tenant = 'acme'"
            ).fetchall()
            self.assertEqual(1, len(trigger_rows))
            triggered_at = trigger_rows[0]["triggered_at"]
            # Each fact is visible at its own occurrence time (closed interval).
            metrics = self.service.metrics("acme", _ts(occurred_at), _ts(occurred_at))
            self.assertEqual(1, metrics["delivery_succeeded"])
            metrics = self.service.metrics("acme", _ts(triggered_at), _ts(triggered_at))
            self.assertEqual({"wf-sched": 1}, metrics["schedule_triggers"])
            # A window ending well before both facts excludes both dimensions.
            before = self.service.metrics("acme", None, _ts("2000-01-01T00:00:00.000Z"))
            self.assertEqual(0, before["delivery_succeeded"])
            self.assertEqual({}, before["schedule_triggers"])
        finally:
            receiver.shutdown()
            receiver.server_close()


class MetricsExportServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "export.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_export_requires_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.metrics_export("")

    def test_empty_export_shape(self):
        text = self.service.metrics_export("acme")
        self.assertEqual(
            [
                'chronicleflow_status_distribution{status="completed"} 0',
                'chronicleflow_status_distribution{status="running"} 0',
                'chronicleflow_status_distribution{status="terminated",reason="cancelled"} 0',
                'chronicleflow_status_distribution{status="terminated",reason="rejected"} 0',
                'chronicleflow_status_distribution{status="terminated",reason="retries_exhausted"} 0',
                'chronicleflow_status_distribution{status="terminated",reason="timeout"} 0',
                "chronicleflow_node_completions 0",
                "chronicleflow_node_failures 0",
                "chronicleflow_retry_consumption 0",
                "chronicleflow_delivery_succeeded 0",
                "chronicleflow_delivery_failed 0",
                "chronicleflow_schedule_triggers 0",
            ],
            text.splitlines(),
        )
        self.assertTrue(text.endswith("\n"))
        self.assertFalse(text.endswith("\n\n"))

    def test_export_families_follow_top_level_order_and_labels_ascend(self):
        self.service.create_workflow(
            {"id": "wf", "nodes": [{"id": "alpha", "kind": "task", "depends_on": [], "retries": 1},
                                   {"id": "zeta", "kind": "task", "depends_on": []}]},
            "wf",
            "acme",
        )
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"failure": {"reason": "x"}}, "f1", "acme")
        self.service.advance("r", {"output": {}}, "o1", "acme")
        self.service.advance("r", {"output": {}}, "o2", "acme")
        text = self.service.metrics_export("acme")
        lines = text.splitlines()
        # Families in top-level key order; samples within a family ascend by
        # label value, so node=alpha precedes node=zeta.
        self.assertEqual(
            'chronicleflow_node_completions{node="alpha"} 1',
            lines[6],
        )
        self.assertEqual(
            'chronicleflow_node_completions{node="zeta"} 1',
            lines[7],
        )
        self.assertEqual('chronicleflow_node_failures{node="alpha"} 1', lines[8])
        self.assertEqual('chronicleflow_retry_consumption{node="alpha"} 1', lines[9])
        self.assertEqual("chronicleflow_delivery_succeeded 0", lines[10])
        self.assertEqual("chronicleflow_delivery_failed 0", lines[11])
        self.assertEqual("chronicleflow_schedule_triggers 0", lines[12])
        self.assertTrue(text.endswith("\n"))

    def test_export_metrics_are_isolated_per_tenant(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "alpha")
        self.service.advance("r", {"output": {}}, "adv", "alpha")
        text = self.service.metrics_export("beta")
        self.assertIn('chronicleflow_status_distribution{status="completed"} 0', text.splitlines())
        self.assertIn("chronicleflow_node_completions 0", text.splitlines())

    def test_export_is_read_only(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.service.advance("r", {"output": {}}, "adv", "acme")
        events_before = self.service.events("r", "acme")
        usage_before = self.service.usage("acme")
        self.service.metrics_export("acme")
        self.assertEqual(events_before, self.service.events("r", "acme"))
        self.assertEqual(usage_before, self.service.usage("acme"))


class MetricsFilterHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-filter.db"))
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
        content_type = response.getheader("Content-Type")
        connection.close()
        return response.status, content_type, data

    def test_invalid_filter_parameters_are_validation_errors(self):
        headers = {"X-Tenant-Id": "http-filter"}
        for path in (
            "/metrics?since=not-a-time",
            "/metrics?until=2026-01-01",
            "/metrics?since=2026-01-01T00:00:00Z&bogus=1",
            "/metrics?since=",
            "/metrics?since=2026-01-01T00:00:00Z&since=2026-02-01T00:00:00Z",
            "/metrics/export?since=not-a-time",
            "/metrics/export?until=2026-01-01",
            "/metrics/export?bogus=1",
        ):
            status, _, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_export_requires_a_tenant_header(self):
        for path in ("/metrics/export", "/metrics/export?since=2026-01-01T00:00:00Z"):
            status, _, data = self.call("GET", path)
            self.assertEqual(400, status)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
            status, _, data = self.call("GET", path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_filtered_metrics_over_http(self):
        headers = {"X-Tenant-Id": "http-filtered"}
        self.call("POST", "/workflows", {"id": "wf", "nodes": TASK}, key="wff", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r", "workflow_id": "wf", "input": {}},
            key="rr",
            headers=headers,
        )
        # A window entirely in the past is a valid query with a zero result.
        status, _, data = self.call(
            "GET",
            "/metrics?since=2000-01-01T00:00:00.000Z&until=2000-02-01T00:00:00.000Z",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(EMPTY_METRICS, json.loads(data))
        # A future-only lower bound includes everything.
        status, _, data = self.call("GET", "/metrics?since=2000-01-01T00:00:00.000Z", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(1, json.loads(data)["status_distribution"]["running"])
        # since later than until is a zero result, not an error.
        status, _, data = self.call(
            "GET",
            "/metrics?since=2099-01-01T00:00:00.000Z&until=2000-01-01T00:00:00.000Z",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(EMPTY_METRICS, json.loads(data))

    def test_export_over_http(self):
        headers = {"X-Tenant-Id": "http-export"}
        status, content_type, data = self.call("GET", "/metrics/export", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual("text/plain; version=0.0.4; charset=utf-8", content_type)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertIn(b"chronicleflow_delivery_succeeded 0\n", data)

    def test_paramless_metrics_body_is_unchanged(self):
        headers = {"X-Tenant-Id": "http-bytes"}
        status, _, data = self.call("GET", "/metrics", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(
            json.dumps(EMPTY_METRICS, ensure_ascii=False, separators=(",", ":")).encode() + b"\n",
            data,
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


if __name__ == "__main__":
    unittest.main()
