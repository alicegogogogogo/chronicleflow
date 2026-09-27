import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import (
    USAGE_TYPES,
    USAGE_UNIT_PRICES,
    ChronicleFlow,
    _parse_timestamp,
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

T0 = "2026-01-01T08:00:00.000Z"
T1 = "2026-01-01T09:00:00.000Z"
T_MID = "2026-01-01T08:30:00.000Z"


def _ts(value):
    return _parse_timestamp(value, "since")


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


class UsageServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "usage.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_empty_usage_and_bill_are_definite_empty_results(self):
        self.assertEqual({"usage": []}, self.service.usage("acme"))
        self.assertEqual({"bill": {"items": [], "total": 0}}, self.service.bill("acme"))

    def test_usage_and_bill_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage("")
        with self.assertRaises(ValidationError):
            self.service.bill("")

    def test_workflow_creation_and_added_version_are_metered(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-1", "acme")
        self.service.create_workflow({"id": "wf", "version": "v2", "nodes": TASK}, "wf-2", "acme")
        self.assertEqual(
            [{"type": "workflow_created", "count": 2}],
            self.service.usage("acme")["usage"],
        )

    def test_rejected_workflow_writes_are_not_metered(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-1", "acme")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-dup", "acme")
        with self.assertRaises(ValidationError):
            self.service.create_workflow(
                {"id": "wf-bad", "nodes": TASK, "extra": 1},
                "wf-bad",
                "acme",
            )
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("acme")["usage"],
        )

    def test_execution_start_is_metered_once(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r-1", "acme")
        # Replaying the same idempotency key returns the first result and is not metered again.
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r-1", "acme")
        # A rejected duplicate write is not metered at all.
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r-2", "acme")
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            self.service.usage("acme")["usage"],
        )

    def test_quota_rejection_writes_no_usage(self):
        self.service.declare_quota({"workflows": 10, "executions": 1}, "q", "acme")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self.assertEqual(
            [{"type": "execution_started", "count": 1}, {"type": "workflow_created", "count": 1}],
            self.service.usage("acme")["usage"],
        )

    def test_usage_is_sorted_by_type_identifier(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        types = [entry["type"] for entry in self.service.usage("acme")["usage"]]
        self.assertEqual(sorted(types), types)

    def test_scheduled_run_meters_start_and_trigger_once_per_period(self):
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
        # Repeated settlement of the same period never meters a second time.
        self.service.schedule_status("wf", "acme")
        counts = {entry["type"]: entry["count"] for entry in self.service.usage("acme")["usage"]}
        self.assertEqual(1, counts["workflow_created"])
        self.assertEqual(1, counts["execution_started"])
        self.assertEqual(1, counts["schedule_triggered"])

    def test_delivery_attempts_are_metered_per_try(self):
        _Receiver.calls = 0
        _Receiver.fail_once = False
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {
                    "id": "wf",
                    "nodes": TASK,
                    "subscriptions": [{"url": url, "events": ["node_completed"]}],
                },
                "wf",
                "acme",
            )
            self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
            self.service.advance("r", {"output": {"v": 1}}, "adv", "acme")
            deadline = time.time() + 3
            while time.time() < deadline:
                if self.service.usage("acme")["usage"]:
                    break
                time.sleep(0.05)
            counts = {entry["type"]: entry["count"] for entry in self.service.usage("acme")["usage"]}
            self.assertEqual(1, counts["delivery_attempted"])
        finally:
            receiver.shutdown()
            receiver.server_close()

    def test_failed_then_retried_delivery_meters_every_attempt(self):
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
                    "subscriptions": [
                        {"url": url, "events": ["node_completed"], "max_attempts": 3}
                    ],
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
            records = self.service.deliveries("r", "acme")["deliveries"]
            self.assertEqual(2, records[0]["attempt_count"])
            counts = {entry["type"]: entry["count"] for entry in self.service.usage("acme")["usage"]}
            self.assertEqual(2, counts["delivery_attempted"])
        finally:
            receiver.shutdown()
            receiver.server_close()

    def test_bill_reports_count_price_subtotal_and_total(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_workflow({"id": "wf", "version": "v2", "nodes": TASK}, "wf2", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        bill = self.service.bill("acme")["bill"]
        items = {item["type"]: item for item in bill["items"]}
        self.assertEqual(2, items["workflow_created"]["count"])
        self.assertEqual(1, items["execution_started"]["count"])
        for item in bill["items"]:
            self.assertIsInstance(item["unit_price"], int)
            self.assertGreater(item["unit_price"], 0)
            self.assertEqual(item["count"] * item["unit_price"], item["subtotal"])
        self.assertEqual(sum(item["subtotal"] for item in bill["items"]), bill["total"])
        self.assertEqual(2 * USAGE_UNIT_PRICES["workflow_created"] + USAGE_UNIT_PRICES["execution_started"], bill["total"])

    def test_unit_prices_exist_for_every_metered_type(self):
        self.assertEqual(sorted(USAGE_TYPES), list(USAGE_TYPES))
        for usage_type in USAGE_TYPES:
            self.assertIsInstance(USAGE_UNIT_PRICES[usage_type], int)
            self.assertGreater(USAGE_UNIT_PRICES[usage_type], 0)

    def test_usage_is_isolated_per_tenant(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.assertEqual([], self.service.usage("beta")["usage"])
        self.assertEqual({"bill": {"items": [], "total": 0}}, self.service.bill("beta"))

    def _metered_records(self):
        """Create one workflow record and two execution records at known times.

        Record sequence 1 is the workflow creation; sequences 2 and 3 are the
        two execution starts. Their occurrence times are stamped explicitly so
        the window behavior is deterministic.
        """
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self._stamp(1, T0)
        self._stamp(2, T_MID)
        self._stamp(3, T1)

    def _stamp(self, sequence, created_at):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (created_at, "acme", sequence),
            )

    def test_no_window_matches_the_cumulative_baseline(self):
        self._metered_records()
        full = {
            "usage": [
                {"type": "execution_started", "count": 2},
                {"type": "workflow_created", "count": 1},
            ]
        }
        self.assertEqual(full, self.service.usage("acme"))
        self.assertEqual(full, self.service.usage("acme", None, None))
        bill = self.service.bill("acme")
        self.assertEqual(bill, self.service.bill("acme", None, None))
        self.assertEqual(
            2 * USAGE_UNIT_PRICES["execution_started"] + USAGE_UNIT_PRICES["workflow_created"],
            bill["bill"]["total"],
        )

    def test_window_counts_only_records_inside_it(self):
        self._metered_records()
        # The open-since window reaches the two records at T_MID and T1.
        self.assertEqual(
            [
                {"type": "execution_started", "count": 2},
            ],
            self.service.usage("acme", _ts(T_MID), None)["usage"],
        )
        # The open-until window reaches T0 and the record at T_MID.
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            self.service.usage("acme", None, _ts(T_MID))["usage"],
        )
        # A window after every record and one before it match nothing.
        self.assertEqual({"usage": []}, self.service.usage("acme", _ts("2027-01-01T00:00:00.000Z"), None))
        self.assertEqual({"usage": []}, self.service.usage("acme", None, _ts("2025-01-01T00:00:00.000Z")))

    def test_window_is_closed_at_both_endpoints(self):
        self._metered_records()
        # A record whose time equals either boundary is counted.
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("acme", _ts(T0), _ts(T0))["usage"],
        )
        point = self.service.bill("acme", _ts(T1), _ts(T1))["bill"]
        self.assertEqual(
            [{"type": "execution_started", "count": 1,
              "unit_price": USAGE_UNIT_PRICES["execution_started"],
              "subtotal": USAGE_UNIT_PRICES["execution_started"]}],
            point["items"],
        )
        self.assertEqual(USAGE_UNIT_PRICES["execution_started"], point["total"])

    def test_since_after_until_is_an_empty_window_not_an_error(self):
        self._metered_records()
        self.assertEqual(
            {"usage": []},
            self.service.usage("acme", _ts(T1), _ts(T0)),
        )
        self.assertEqual(
            {"bill": {"items": [], "total": 0}},
            self.service.bill("acme", _ts(T1), _ts(T0)),
        )

    def test_windowed_bill_keeps_price_subtotal_and_sorting_basis(self):
        self._metered_records()
        bill = self.service.bill("acme", _ts(T0), _ts(T_MID))["bill"]
        types = [item["type"] for item in bill["items"]]
        self.assertEqual(sorted(types), types)
        self.assertEqual(
            ["execution_started", "workflow_created"],
            types,
        )
        for item in bill["items"]:
            self.assertEqual(USAGE_UNIT_PRICES[item["type"]], item["unit_price"])
            self.assertEqual(item["count"] * item["unit_price"], item["subtotal"])
        self.assertEqual(sum(item["subtotal"] for item in bill["items"]), bill["total"])
        self.assertIsInstance(bill["total"], int)

    def test_windowed_queries_are_read_only_and_tenant_isolated(self):
        self._metered_records()
        self.service.create_workflow({"id": "wf-b", "nodes": TASK}, "wf-b", "beta")
        window = (_ts(T0), _ts(T1))
        alpha_before = self.service.usage("acme")
        beta_before = self.service.usage("beta")
        self.service.usage("acme", *window)
        self.service.bill("acme", *window)
        self.assertEqual(alpha_before, self.service.usage("acme"))
        self.assertEqual(beta_before, self.service.usage("beta"))
        # Another tenant's records are never visible, in any window: the window
        # holds all of acme's stamped records but none of beta's, since beta's
        # record carries the wall-clock creation time outside the Jan window.
        self.assertEqual(
            [
                {"type": "execution_started", "count": 2},
                {"type": "workflow_created", "count": 1},
            ],
            self.service.usage("acme", *window)["usage"],
        )
        self.assertEqual({"usage": []}, self.service.usage("beta", *window))
        self.assertEqual(
            {"bill": {"items": [], "total": 0}},
            self.service.bill("beta", *window),
        )

    def test_windowed_usage_and_bill_still_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage("", _ts(T0), _ts(T1))
        with self.assertRaises(ValidationError):
            self.service.bill("", _ts(T0), _ts(T1))

    def test_type_filter_counts_only_named_types(self):
        self._metered_records()
        # A single named type reports just that type's aggregate.
        self.assertEqual(
            {"usage": [{"type": "workflow_created", "count": 1}]},
            self.service.usage("acme", types=("workflow_created",)),
        )
        self.assertEqual(
            {"usage": [{"type": "execution_started", "count": 2}]},
            self.service.usage("acme", types=("execution_started",)),
        )
        # Several named types keep ascending type-identifier order and omit a
        # named type that has no record.
        self.assertEqual(
            {
                "usage": [
                    {"type": "execution_started", "count": 2},
                    {"type": "workflow_created", "count": 1},
                ]
            },
            self.service.usage("acme", types=("workflow_created", "execution_started")),
        )
        self.assertEqual(
            {"usage": [{"type": "workflow_created", "count": 1}]},
            self.service.usage(
                "acme", types=("workflow_created", "schedule_triggered")
            ),
        )

    def test_type_filter_and_window_intersect(self):
        self._metered_records()
        # Workflow_created sits at T0, the two execution starts at T_MID/T1.
        self.assertEqual(
            {"usage": [{"type": "execution_started", "count": 1}]},
            self.service.usage("acme", _ts(T0), _ts(T_MID), ("execution_started",)),
        )
        # The named type has no record inside this window: definite empty list.
        self.assertEqual(
            {"usage": []},
            self.service.usage("acme", _ts(T_MID), _ts(T1), ("workflow_created",)),
        )
        # Both named types, window reaching only T0: the workflow at T0 hits.
        self.assertEqual(
            {"usage": [{"type": "workflow_created", "count": 1}]},
            self.service.usage(
                "acme", _ts(T0), _ts(T0), ("execution_started", "workflow_created")
            ),
        )

    def test_filter_matching_nothing_is_a_definite_empty_result(self):
        self._metered_records()
        for types in (("schedule_triggered",), ("delivery_attempted",)):
            self.assertEqual({"usage": []}, self.service.usage("acme", types=types))
            self.assertEqual(
                {"bill": {"items": [], "total": 0}},
                self.service.bill("acme", types=types),
            )
        # Empty tenant keeps the same definite empty results under a filter.
        self.assertEqual(
            {"usage": []}, self.service.usage("beta", types=("execution_started",))
        )
        self.assertEqual(
            {"bill": {"items": [], "total": 0}},
            self.service.bill("beta", types=("execution_started",)),
        )

    def test_filtered_bill_lists_only_named_items_and_sums_just_them(self):
        self._metered_records()
        bill = self.service.bill("acme", types=("workflow_created",))["bill"]
        self.assertEqual(
            [
                {
                    "type": "workflow_created",
                    "count": 1,
                    "unit_price": USAGE_UNIT_PRICES["workflow_created"],
                    "subtotal": USAGE_UNIT_PRICES["workflow_created"],
                }
            ],
            bill["items"],
        )
        self.assertEqual(USAGE_UNIT_PRICES["workflow_created"], bill["total"])
        # The closed window reaches the execution starts at T_MID and T1.
        windowed = self.service.bill(
            "acme", _ts(T_MID), _ts(T1), ("execution_started",)
        )["bill"]
        self.assertEqual(
            [
                {
                    "type": "execution_started",
                    "count": 2,
                    "unit_price": USAGE_UNIT_PRICES["execution_started"],
                    "subtotal": 2 * USAGE_UNIT_PRICES["execution_started"],
                }
            ],
            windowed["items"],
        )
        self.assertEqual(2 * USAGE_UNIT_PRICES["execution_started"], windowed["total"])

    def test_filtered_usage_and_bill_still_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage("", types=("execution_started",))
        with self.assertRaises(ValidationError):
            self.service.bill("", types=("execution_started",))
        with self.assertRaises(ValidationError):
            self.service.usage("", _ts(T0), _ts(T1), ("execution_started",))
        with self.assertRaises(ValidationError):
            self.service.bill("", _ts(T0), _ts(T1), ("execution_started",))

    def test_type_filter_is_read_only_and_tenant_isolated(self):
        self._metered_records()
        self.service.create_workflow({"id": "wf-b", "nodes": TASK}, "wf-b", "beta")
        before = self.service.usage("acme")
        self.service.usage("acme", types=("execution_started",))
        self.service.bill("acme", _ts(T0), _ts(T1), ("execution_started",))
        self.assertEqual(before, self.service.usage("acme"))
        # Another tenant's records never enter this tenant's filtered result.
        self.assertEqual(
            {"usage": []},
            self.service.usage("beta", _ts(T0), _ts(T1), ("workflow_created",)),
        )

    def test_legacy_namespace_keeps_no_usage_records(self):
        _Receiver.calls = 0
        _Receiver.fail_once = False
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {
                    "id": "wf",
                    "nodes": TASK,
                    "subscriptions": [{"url": url, "events": ["node_completed"]}],
                },
                "wf",
            )
            self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r")
            self.service.advance("r", {"output": {"v": 1}}, "adv")
            deadline = time.time() + 3
            while time.time() < deadline and _Receiver.calls == 0:
                time.sleep(0.05)
            self.assertEqual(1, _Receiver.calls)
        finally:
            receiver.shutdown()
            receiver.server_close()
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = ''"
        ).fetchone()
        self.assertEqual(0, rows["used"])


class UsageHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-usage.db"))
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

    def test_usage_and_bill_require_a_tenant_header(self):
        for path in ("/usage", "/bill"):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)
            status, data = self.call("GET", path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_usage_and_bill_over_http(self):
        headers = {"X-Tenant-Id": "http-acme"}
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))

        self.call(
            "POST",
            "/workflows",
            {"id": "wf-http", "nodes": TASK},
            key="wf-http",
            headers=headers,
        )
        self.call(
            "POST",
            "/executions",
            {"id": "r-http", "workflow_id": "wf-http", "input": {}},
            key="r-http",
            headers=headers,
        )
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            json.loads(data)["usage"],
        )
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        bill = json.loads(data)["bill"]
        self.assertEqual(2, len(bill["items"]))
        for item in bill["items"]:
            self.assertEqual(item["count"] * item["unit_price"], item["subtotal"])
            self.assertIsInstance(item["unit_price"], int)
        self.assertEqual(sum(item["subtotal"] for item in bill["items"]), bill["total"])
        self.assertIsInstance(bill["total"], int)

    def test_usage_does_not_leak_across_tenants(self):
        headers = {"X-Tenant-Id": "http-other"}
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))

    def test_window_parameters_are_validated_on_both_routes(self):
        headers = {"X-Tenant-Id": "http-windowed"}
        bad_paths = (
            "/usage?since=not-a-time",
            "/usage?until=2026-01-01",
            "/bill?since=not-a-time",
            "/bill?since=2026-01-01T00:00:00Z&bogus=1",
            "/usage?unknown=1",
            "/usage?since=2026-01-01T00:00:00Z&since=2026-02-01T00:00:00Z",
            "/bill?until=2026-01-01T00:00:00Z&until=2026-02-01T00:00:00Z",
            "/bill?since=",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_window_validation_failure_writes_no_usage(self):
        headers = {"X-Tenant-Id": "http-window-empty"}
        status, _ = self.call(
            "GET", "/usage?since=not-a-time", headers=headers
        )
        self.assertEqual(400, status)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))

    def test_reverse_window_is_empty_over_http(self):
        headers = {"X-Tenant-Id": "http-window-reverse"}
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-rw", "nodes": TASK},
            key="wf-rw",
            headers=headers,
        )
        query = "?since=2099-01-01T00:00:00.000Z&until=2000-01-01T00:00:00.000Z"
        status, data = self.call("GET", "/usage" + query, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call("GET", "/bill" + query, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_window_filters_windowed_tenant_over_http(self):
        headers = {"X-Tenant-Id": "http-windowed-hit"}
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-wh", "nodes": TASK},
            key="wf-wh",
            headers=headers,
        )
        # A window entirely before the request was made matches nothing and is
        # not an error.
        past = "?since=2000-01-01T00:00:00.000Z&until=2000-02-01T00:00:00.000Z"
        status, data = self.call("GET", "/usage" + past, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        # The same tenant's cumulative result with no parameters is unchanged.
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            json.loads(data)["usage"],
        )
        status, data = self.call("GET", "/bill" + past, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))

    def test_missing_tenant_is_a_400_even_with_a_window(self):
        for path in (
            "/usage?since=2026-01-01T00:00:00Z",
            "/bill?until=2026-01-01T00:00:00Z",
        ):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_type_filter_over_http(self):
        headers = {"X-Tenant-Id": "http-type-filter"}
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-tf", "nodes": TASK},
            key="wf-tf",
            headers=headers,
        )
        self.call(
            "POST",
            "/executions",
            {"id": "r-tf", "workflow_id": "wf-tf", "input": {}},
            key="r-tf",
            headers=headers,
        )
        status, data = self.call("GET", "/usage?type=workflow_created", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            json.loads(data)["usage"],
        )
        status, data = self.call(
            "GET",
            "/usage?type=execution_started,workflow_created",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            json.loads(data)["usage"],
        )
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call("GET", "/bill?type=workflow_created", headers=headers)
        self.assertEqual(200, status)
        bill = json.loads(data)["bill"]
        self.assertEqual(["workflow_created"], [item["type"] for item in bill["items"]])
        self.assertEqual(
            sum(item["subtotal"] for item in bill["items"]), bill["total"]
        )
        # A known type with no records is omitted, giving the definite empties.
        status, data = self.call(
            "GET", "/usage?type=schedule_triggered", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        status, data = self.call(
            "GET", "/bill?type=schedule_triggered", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))

    def test_type_filter_without_type_is_byte_for_byte_the_old_query(self):
        headers = {"X-Tenant-Id": "http-type-absent"}
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-ta", "nodes": TASK},
            key="wf-ta",
            headers=headers,
        )
        for path in ("/usage", "/bill", "/usage?", "/bill?"):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(200, status, path)
            self.assertTrue(data.endswith(b"\n"), path)
            self.assertFalse(data.endswith(b"\n\n"), path)
        status, plain = self.call("GET", "/usage", headers=headers)
        status, marked = self.call("GET", "/usage?", headers=headers)
        self.assertEqual(plain, marked)

    def test_type_filter_is_validated_on_both_routes(self):
        headers = {"X-Tenant-Id": "http-type-bad"}
        bad_paths = (
            "/usage?type=not_a_type",
            "/bill?type=not_a_type",
            "/usage?type=execution_started,execution_started",
            "/bill?type=execution_started,",
            "/usage?type=",
            "/bill?type=,execution_started",
            "/usage?type=execution_started&type=workflow_created",
            "/bill?type=execution_started&bogus=1",
            "/usage?type=execution_started&since=not-a-time",
            "/bill?type=execution_started&until=not-a-time",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_type_filter_validation_failure_writes_nothing_and_reveals_nothing(self):
        headers = {"X-Tenant-Id": "http-type-empty"}
        status, _ = self.call("GET", "/usage?type=", headers=headers)
        self.assertEqual(400, status)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))


if __name__ == "__main__":
    unittest.main()
