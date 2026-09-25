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
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


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


if __name__ == "__main__":
    unittest.main()
