import http.client
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import (
    USAGE_TYPES,
    USAGE_UNIT_PRICES,
    ChronicleFlow,
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

T1 = "2026-09-26T08:00:00.000Z"
T2 = "2026-09-26T09:00:00.000Z"
T3 = "2026-09-26T10:00:00.000Z"


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value[:-1] + "+00:00")


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

    def _insert_usage(self, tenant, usage_type, created_at, sequence=None):
        """Append a usage record at a fixed occurrence time, bypassing the clock."""
        connection = self.service.store.connection
        if sequence is None:
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM usage_records WHERE tenant = ?",
                (tenant,),
            ).fetchone()
            sequence = row["sequence"]
        connection.execute(
            "INSERT INTO usage_records(tenant, sequence, type, created_at) VALUES (?, ?, ?, ?)",
            (tenant, sequence, usage_type, created_at),
        )

    def test_unfiltered_window_is_byte_equivalent_to_cumulative_result(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        self.assertEqual(self.service.usage("acme"), self.service.usage("acme", None, None))
        self.assertEqual(self.service.bill("acme"), self.service.bill("acme", None, None))

    def test_window_counts_only_records_in_the_closed_interval(self):
        with self.service.store.transaction():
            self._insert_usage("acme", "workflow_created", T1)
            self._insert_usage("acme", "execution_started", T2)
            self._insert_usage("acme", "workflow_created", T3)
        # A record whose time equals either endpoint is counted.
        usage = self.service.usage("acme", _ts(T1), _ts(T2))["usage"]
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            usage,
        )
        # A single-point window matches only the record at that time.
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("acme", _ts(T3), _ts(T3))["usage"],
        )

    def test_open_bounds_match_everything_on_each_side(self):
        with self.service.store.transaction():
            self._insert_usage("acme", "workflow_created", T1)
            self._insert_usage("acme", "workflow_created", T3)
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("acme", None, _ts(T1))["usage"],
        )
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("acme", _ts(T3), None)["usage"],
        )

    def test_reversed_window_is_a_definite_empty_result_not_an_error(self):
        with self.service.store.transaction():
            self._insert_usage("acme", "workflow_created", T2)
        self.assertEqual({"usage": []}, self.service.usage("acme", _ts(T3), _ts(T1)))
        self.assertEqual(
            {"bill": {"items": [], "total": 0}},
            self.service.bill("acme", _ts(T3), _ts(T1)),
        )

    def test_bill_window_uses_windowed_counts_with_same_prices_and_totals(self):
        with self.service.store.transaction():
            self._insert_usage("acme", "workflow_created", T1)
            self._insert_usage("acme", "workflow_created", T2)
            self._insert_usage("acme", "execution_started", T2)
            self._insert_usage("acme", "workflow_created", T3)
        bill = self.service.bill("acme", _ts(T2), _ts(T2))["bill"]
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1, "unit_price": USAGE_UNIT_PRICES["execution_started"],
                 "subtotal": USAGE_UNIT_PRICES["execution_started"]},
                {"type": "workflow_created", "count": 1, "unit_price": USAGE_UNIT_PRICES["workflow_created"],
                 "subtotal": USAGE_UNIT_PRICES["workflow_created"]},
            ],
            bill["items"],
        )
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"] + USAGE_UNIT_PRICES["workflow_created"],
            bill["total"],
        )
        self.assertIsInstance(bill["total"], int)
        # A type with no record in the window is omitted.
        self.assertNotIn("delivery_attempted", {item["type"] for item in bill["items"]})

    def test_windowed_usage_stays_isolated_per_tenant(self):
        with self.service.store.transaction():
            self._insert_usage("alpha", "workflow_created", T2)
            self._insert_usage("beta", "workflow_created", T2)
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("alpha", _ts(T1), _ts(T3))["usage"],
        )
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            self.service.usage("beta", _ts(T1), _ts(T3))["usage"],
        )
        self.assertEqual([], self.service.usage("gamma", _ts(T1), _ts(T3))["usage"])

    def test_windowed_usage_and_bill_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage("", _ts(T1), _ts(T2))
        with self.assertRaises(ValidationError):
            self.service.bill("", _ts(T1), _ts(T2))

    def test_filtering_changes_no_recorded_usage(self):
        with self.service.store.transaction():
            self._insert_usage("acme", "workflow_created", T2)
        before_usage = self.service.usage("acme")
        before_bill = self.service.bill("acme")
        self.service.usage("acme", _ts(T1), _ts(T2))
        self.service.bill("acme", _ts(T2), _ts(T3))
        self.service.usage("acme", _ts(T3), _ts(T1))
        self.service.bill("acme", _ts(T3), _ts(T1))
        self.assertEqual(before_usage, self.service.usage("acme"))
        self.assertEqual(before_bill, self.service.bill("acme"))


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

    def test_window_parameters_are_rejected_as_validation_errors(self):
        headers = {"X-Tenant-Id": "http-acme"}
        bad_paths = (
            "/usage?since=not-a-time",
            "/usage?until=2026-01-01",
            "/usage?since=2026-01-01T00:00:00Z&bogus=1",
            "/usage?since=",
            "/usage?since=2026-01-01T00:00:00Z&since=2026-02-01T00:00:00Z",
            "/bill?until=not-a-time",
            "/bill?since=2026-01-01T00:00:00Z&bogus=1",
            "/bill?until=2026-01-01T00:00:00Z&until=2026-02-01T00:00:00Z",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_window_validation_still_requires_a_tenant_and_leaks_nothing(self):
        for path in ("/usage?since=2000-01-01T00:00:00.000Z", "/bill?until=2099-01-01T00:00:00.000Z"):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_windowed_queries_over_http(self):
        headers = {"X-Tenant-Id": "http-window"}
        connection = Handler.service.store.connection
        with Handler.service.store.transaction():
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM usage_records WHERE tenant = ?",
                ("http-window",),
            ).fetchone()
            connection.execute(
                "INSERT INTO usage_records(tenant, sequence, type, created_at) VALUES (?, ?, ?, ?)",
                ("http-window", row["sequence"], "workflow_created", T2),
            )
        # The record is counted at both closed endpoints.
        status, data = self.call("GET", f"/usage?since={T2}", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual([{"type": "workflow_created", "count": 1}], json.loads(data)["usage"])
        status, data = self.call("GET", f"/bill?until={T2}", headers=headers)
        self.assertEqual(200, status)
        bill = json.loads(data)["bill"]
        self.assertEqual(1, len(bill["items"]))
        self.assertEqual(USAGE_UNIT_PRICES["workflow_created"], bill["total"])
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        # A window ending before the record matches nothing.
        status, data = self.call("GET", f"/usage?until={T1}", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        # A reversed window is the definite empty result, not an error.
        status, data = self.call(
            "GET",
            f"/usage?since={T3}&until={T1}",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        status, data = self.call(
            "GET",
            f"/bill?since={T3}&until={T1}",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"items": [], "total": 0}}, json.loads(data))
        # No parameters still returns the cumulative answer.
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual([{"type": "workflow_created", "count": 1}], json.loads(data)["usage"])


if __name__ == "__main__":
    unittest.main()
