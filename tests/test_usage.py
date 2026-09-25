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
from chronicleflow.service import (
    USAGE_DELIVERY_ATTEMPTED,
    USAGE_EXECUTION_STARTED,
    USAGE_SCHEDULE_TRIGGERED,
    USAGE_UNIT_PRICES,
    USAGE_WORKFLOW_CREATED,
    ChronicleFlow,
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


class Receiver(BaseHTTPRequestHandler):
    requests = []
    failures_before_success = 0

    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        Receiver.requests.append(json.loads(body))
        if Receiver.failures_before_success > 0:
            Receiver.failures_before_success -= 1
            self.send_response(500)
        else:
            self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class UsageServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "usage.db"))

    def tearDown(self):
        self.directory.cleanup()

    def usage_map(self, tenant="acme"):
        return {entry["type"]: entry["count"] for entry in self.service.get_usage(tenant)["usage"]}

    def test_empty_usage_and_bill(self):
        self.assertEqual({"usage": []}, self.service.get_usage("acme"))
        bill = self.service.get_bill("acme")
        self.assertEqual({"bill": [], "total": 0}, bill)

    def test_usage_and_bill_require_a_tenant(self):
        for call in (self.service.get_usage, self.service.get_bill):
            with self.assertRaises(ValidationError):
                call("")

    def test_workflow_creation_and_version_append_are_metered(self):
        self.service.create_workflow(
            {"id": "wf", "version": "v1", "nodes": TASK}, "wf-v1", "acme"
        )
        # Appending a version is itself one workflow creation.
        self.service.create_workflow(
            {"id": "wf", "version": "v2", "nodes": TASK}, "wf-v2", "acme"
        )
        self.assertEqual(2, self.usage_map()[USAGE_WORKFLOW_CREATED])

    def test_execution_start_is_metered(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self.assertEqual(2, self.usage_map()[USAGE_EXECUTION_STARTED])

    def test_schedule_trigger_meters_start_and_trigger_once_per_period(self):
        self.service.create_workflow(
            {
                "id": "wf",
                "nodes": TASK,
                "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"},
            },
            "wf",
            "acme",
        )
        # Pause before the first period comes due, then a catch-up resume
        # creates exactly one execution for the single most recent period.
        self.service.pause_schedule("wf", {}, "pause", "acme")
        time.sleep(1.2)
        status = self.service.resume_schedule("wf", {}, "resume", "acme")
        self.assertIsNotNone(status["last_execution_id"])
        usage = self.usage_map()
        self.assertEqual(1, usage[USAGE_EXECUTION_STARTED])
        self.assertEqual(1, usage[USAGE_SCHEDULE_TRIGGERED])
        # Settling the schedule again never meters the same period a second time.
        self.service.schedule_status("wf", "acme")
        usage = self.usage_map()
        self.assertEqual(1, usage[USAGE_EXECUTION_STARTED])
        self.assertEqual(1, usage[USAGE_SCHEDULE_TRIGGERED])

    def test_delivery_is_metered_once_regardless_of_outcome_and_retries(self):
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
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
            Receiver.failures_before_success = 2
            self.service.advance("r", {"output": {"v": 1}}, "adv", "acme")
        finally:
            receiver.shutdown()
            receiver.server_close()
        # Three HTTP tries are one delivery record, metered exactly once.
        self.assertEqual(3, len(Receiver.requests))
        self.assertEqual(1, self.usage_map()[USAGE_DELIVERY_ATTEMPTED])

    def test_failed_delivery_is_still_metered(self):
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        threading.Thread(target=receiver.serve_forever, daemon=True).start()
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {
                    "id": "wf2",
                    "nodes": TASK,
                    "subscriptions": [{"url": url, "events": ["node_completed"]}],
                },
                "wf2",
                "acme",
            )
            self.service.create_execution({"id": "r2", "workflow_id": "wf2", "input": {}}, "r2", "acme")
            Receiver.failures_before_success = 5  # more than the single attempt
            self.service.advance("r2", {"output": {"v": 1}}, "adv2", "acme")
        finally:
            receiver.shutdown()
            receiver.server_close()
        self.assertEqual("failed", self.service.deliveries("r2", "acme")["deliveries"][0]["status"])
        self.assertEqual(1, self.usage_map()[USAGE_DELIVERY_ATTEMPTED])

    def test_rejected_writes_write_no_usage(self):
        # Validation rejection.
        with self.assertRaises(ValidationError):
            self.service.create_workflow({"id": "bad", "nodes": []}, "bad", "acme")
        self.assertEqual({"usage": []}, self.service.get_usage("acme"))
        # Quota rejection.
        self.service.declare_quota({"workflows": 1, "executions": 1}, "q", "acme")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        with self.assertRaises(Exception):
            self.service.create_workflow({"id": "wf2", "nodes": TASK}, "wf2", "acme")
        self.assertEqual(1, self.usage_map()[USAGE_WORKFLOW_CREATED])
        # Identifier conflict (version tag reuse).
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK}, "v1", "acme")
        with self.assertRaises(Exception):
            self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK}, "v1-again", "acme")
        self.assertEqual(2, self.usage_map()[USAGE_WORKFLOW_CREATED])

    def test_idempotent_replay_does_not_meter_again(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        first = self.service.create_execution(
            {"id": "r", "workflow_id": "wf", "input": {}}, "shared", "acme"
        )
        repeated = self.service.create_execution(
            {"id": "r", "workflow_id": "wf", "input": {}}, "shared", "acme"
        )
        self.assertEqual(first, repeated)
        self.assertEqual(1, self.usage_map()[USAGE_EXECUTION_STARTED])
        # Cross-operation key reuse conflicts and meters nothing new.
        with self.assertRaises(Exception):
            self.service.create_workflow({"id": "wf2", "nodes": TASK}, "shared", "acme")
        self.assertEqual(1, self.usage_map()[USAGE_WORKFLOW_CREATED])

    def test_usage_is_isolated_per_tenant_and_sorted(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-a", "alpha")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r-a", "alpha")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-b", "beta")
        alpha = self.service.get_usage("alpha")["usage"]
        beta = self.service.get_usage("beta")["usage"]
        self.assertEqual([USAGE_EXECUTION_STARTED, USAGE_WORKFLOW_CREATED], [e["type"] for e in alpha])
        self.assertEqual([USAGE_WORKFLOW_CREATED], [e["type"] for e in beta])
        for entry in alpha + beta:
            # New fields come out in a stable, sorted key order.
            self.assertEqual(["count", "type"], list(entry))

    def test_legacy_namespace_writes_no_usage_records(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r")
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = ''"
        ).fetchone()
        self.assertEqual(0, rows["used"])

    def test_bill_amounts_are_integer_cents_and_totalled(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_workflow({"id": "wf2", "nodes": TASK}, "wf2", "acme")
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", "acme")
        bill = self.service.get_bill("acme")
        counts = self.usage_map()
        expected_total = 0
        for item in bill["bill"]:
            self.assertEqual(["count", "subtotal", "type", "unit_price"], list(item))
            self.assertEqual(counts[item["type"]], item["count"])
            price = item["unit_price"]
            self.assertIsInstance(price, int)
            self.assertGreater(price, 0)
            self.assertEqual(USAGE_UNIT_PRICES[item["type"]], price)
            self.assertEqual(item["count"] * price, item["subtotal"])
            expected_total += item["subtotal"]
        self.assertEqual(expected_total, bill["total"])
        # Items follow the same ascending type order.
        self.assertEqual(sorted(counts), [item["type"] for item in bill["bill"]])


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

    def test_usage_and_bill_require_tenant(self):
        for path in ("/usage", "/bill"):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
            status, data = self.call("GET", path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_usage_and_bill_end_to_end_over_http(self):
        headers = {"X-Tenant-Id": "http-tenant"}
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": [], "total": 0}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-u", "nodes": TASK},
            key="wf-u",
            headers=headers,
        )
        self.call(
            "POST",
            "/executions",
            {"id": "r-u", "workflow_id": "wf-u", "input": {}},
            key="r-u",
            headers=headers,
        )
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        types = [entry["type"] for entry in json.loads(data)["usage"]]
        self.assertEqual([USAGE_EXECUTION_STARTED, USAGE_WORKFLOW_CREATED], types)
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        payload = json.loads(data)
        self.assertEqual(["bill", "total"], list(payload))
        self.assertEqual(
            USAGE_UNIT_PRICES[USAGE_EXECUTION_STARTED] + USAGE_UNIT_PRICES[USAGE_WORKFLOW_CREATED],
            payload["total"],
        )


if __name__ == "__main__":
    unittest.main()
