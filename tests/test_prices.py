import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
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


class PricingServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "prices.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _usage(self, tenant="acme"):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", tenant)
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r", tenant)

    def test_undeclared_prices_are_the_definite_empty_result(self):
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_prices_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.get_prices("")
        with self.assertRaises(ValidationError):
            self.service.declare_prices({"workflow_created": 1}, "p", "")

    def test_declaring_prices_stores_exactly_the_given_mapping_sorted(self):
        result = self.service.declare_prices(
            {"workflow_created": 2000, "delivery_attempted": 25}, "p1", "acme"
        )
        self.assertEqual(
            {"prices": {"delivery_attempted": 25, "workflow_created": 2000}},
            result,
        )
        # The read returns the declared table verbatim, sorted by type.
        self.assertEqual(
            {"prices": {"delivery_attempted": 25, "workflow_created": 2000}},
            self.service.get_prices("acme"),
        )

    def test_empty_object_is_a_valid_declaration_and_changes_no_bill(self):
        self._usage()
        before = self.service.bill("acme")
        result = self.service.declare_prices({}, "p-empty", "acme")
        self.assertEqual({"prices": {}}, result)
        self.assertEqual({"prices": {}}, self.service.get_prices("acme"))
        # An empty declaration overrides nothing, so the bill is unchanged.
        self.assertEqual(before, self.service.bill("acme"))

    def test_redeclaring_replaces_the_whole_table(self):
        self.service.declare_prices({"workflow_created": 2000}, "p1", "acme")
        self.service.declare_prices({"delivery_attempted": 25}, "p2", "acme")
        # The workflow override vanished with the whole-table replacement.
        self.assertEqual({"prices": {"delivery_attempted": 25}}, self.service.get_prices("acme"))

    def test_bill_uses_declared_prices_with_default_fallback(self):
        self._usage()
        self.service.declare_prices({"execution_started": 7}, "p1", "acme")
        bill = self.service.bill("acme")["bill"]
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1, "unit_price": 7, "subtotal": 7},
                {"type": "workflow_created", "count": 1,
                 "unit_price": USAGE_UNIT_PRICES["workflow_created"],
                 "subtotal": USAGE_UNIT_PRICES["workflow_created"]},
            ],
            bill["items"],
        )
        self.assertEqual(7 + USAGE_UNIT_PRICES["workflow_created"], bill["total"])

    def test_bill_prices_at_query_time_and_repricing_does_not_touch_counts(self):
        self._usage()
        default_bill = self.service.bill("acme")["bill"]
        usage_before = self.service.usage("acme")
        self.service.declare_prices(
            {usage_type: 1000 + index for index, usage_type in enumerate(USAGE_TYPES)},
            "p1",
            "acme",
        )
        repriced = self.service.bill("acme")["bill"]
        # Counts never move; only unit prices and subtotals do.
        self.assertEqual(
            [item["type"] for item in default_bill["items"]],
            [item["type"] for item in repriced["items"]],
        )
        self.assertEqual([1, 1], [item["count"] for item in repriced["items"]])
        self.assertEqual(
            [1000 + USAGE_TYPES.index("execution_started"),
             1000 + USAGE_TYPES.index("workflow_created")],
            [item["unit_price"] for item in repriced["items"]],
        )
        self.assertEqual(sum(item["subtotal"] for item in repriced["items"]), repriced["total"])
        # Usage counts are untouched by the declaration and repricing.
        self.assertEqual(usage_before, self.service.usage("acme"))
        # Declaring again with new prices changes the next query's answer.
        self.service.declare_prices({"execution_started": 3}, "p2", "acme")
        items = {item["type"]: item for item in self.service.bill("acme")["bill"]["items"]}
        self.assertEqual(3, items["execution_started"]["unit_price"])
        self.assertEqual(
            USAGE_UNIT_PRICES["workflow_created"], items["workflow_created"]["unit_price"]
        )

    def test_windowed_and_filtered_bills_use_prices_in_effect_at_query_time(self):
        self._usage()
        self.service.declare_prices({"workflow_created": 4242}, "p1", "acme")
        filtered = self.service.bill("acme", types=("workflow_created",))["bill"]
        self.assertEqual(
            [{"type": "workflow_created", "count": 1, "unit_price": 4242, "subtotal": 4242}],
            filtered["items"],
        )
        self.assertEqual(4242, filtered["total"])
        # A window matching nothing still reports the definite empty result.
        self.assertEqual(
            {"bill": {"items": [], "total": 0}},
            self.service.bill(
                "acme", _parse_timestamp("2030-01-01T00:00:00.000Z", "since")
            ),
        )

    def test_bucketed_bill_uses_the_prices_in_effect_at_query_time(self):
        self._usage()
        self.service.declare_prices({"workflow_created": 333}, "p1", "acme")
        bill = self.service.bill("acme", bucket="hour")["bill"]
        self.assertEqual(1, len(bill["buckets"]))
        bucket = bill["buckets"][0]
        prices = {item["type"]: item["unit_price"] for item in bucket["items"]}
        self.assertEqual(333, prices["workflow_created"])
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"], prices["execution_started"]
        )
        self.assertEqual(sum(item["subtotal"] for item in bucket["items"]), bucket["subtotal"])
        self.assertEqual(bucket["subtotal"], bill["total"])

    def test_invalid_price_declarations_are_validation_errors_that_write_nothing(self):
        bad_bodies = [
            [],
            "prices",
            7,
            None,
            {"workflow_created": 0},
            {"workflow_created": -1},
            {"workflow_created": 1.5},
            {"workflow_created": True},
            {"workflow_created": False},
            {"not_a_type": 1},
            {"delivery_attempted": 1, "bogus": 2},
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.declare_prices(body, f"bad-{index}", "acme")
        # Nothing was stored: the definite empty result and default prices hold.
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_same_idempotency_key_replays_the_first_declaration(self):
        first = self.service.declare_prices({"workflow_created": 111}, "shared", "acme")
        repeated = self.service.declare_prices({"workflow_created": 222}, "shared", "acme")
        self.assertEqual(first, repeated)
        self.assertEqual({"prices": {"workflow_created": 111}}, self.service.get_prices("acme"))

    def test_idempotency_key_reuse_across_operations_conflicts_and_changes_nothing(self):
        self.service.declare_quota({"workflows": 1, "executions": 1}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.declare_prices({"workflow_created": 1}, "shared", "acme")
        # A different key with conflicting reuse also leaves the table alone.
        self.service.declare_prices({"workflow_created": 1}, "prices-key", "acme")
        with self.assertRaises(ConflictError):
            self.service.declare_quota(
                {"workflows": 2, "executions": 2}, "prices-key", "acme"
            )
        self.assertEqual({"prices": {"workflow_created": 1}}, self.service.get_prices("acme"))

    def test_price_tables_are_isolated_per_tenant(self):
        self._usage("alpha")
        self._usage("beta")
        self.service.declare_prices({"workflow_created": 999}, "pa", "alpha")
        # Beta never declared: definite empty result and default bill.
        self.assertEqual({"prices": None}, self.service.get_prices("beta"))
        beta_items = {
            item["type"]: item for item in self.service.bill("beta")["bill"]["items"]
        }
        self.assertEqual(
            USAGE_UNIT_PRICES["workflow_created"], beta_items["workflow_created"]["unit_price"]
        )
        alpha_items = {
            item["type"]: item for item in self.service.bill("alpha")["bill"]["items"]
        }
        self.assertEqual(999, alpha_items["workflow_created"]["unit_price"])

    def test_service_layer_rejects_illegal_bucket_value_without_writing(self):
        self._usage()
        for method in ("usage", "bill"):
            with self.subTest(method=method):
                with self.assertRaises(ValidationError):
                    getattr(self.service, method)("acme", bucket="year")
        # The usage table is unchanged by the rejected queries.
        self.assertEqual(2, len(self.service.usage_records("acme", limit=10)["records"]))


class PricingHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-prices.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, headers=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_prices_routes_over_http(self):
        headers = {"X-Tenant-Id": "http-prices-a"}
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"prices": None}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call(
            "PUT", "/prices", {"workflow_created": 1234, "execution_started": 5},
            key="prices-a", headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(
            {"prices": {"execution_started": 5, "workflow_created": 1234}}, json.loads(data)
        )
        self.assertTrue(data.endswith(b"\n"))
        status, data = self.call("POST", "/prices", {}, key="prices-a-empty", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"prices": {}}, json.loads(data))
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"prices": {}}, json.loads(data))

    def test_prices_routes_without_or_empty_tenant_are_validation_errors(self):
        status, data = self.call("GET", "/prices")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("PUT", "/prices", {"workflow_created": 1}, key="p-no-tenant")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "GET", "/prices", headers={"X-Tenant-Id": ""}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_prices_validation_errors_over_http(self):
        headers = {"X-Tenant-Id": "http-prices-b"}
        for index, raw in enumerate((
            b'{"workflow_created":0}',
            b'{"workflow_created":-5}',
            b'{"workflow_created":1.5}',
            b'{"workflow_created":true}',
            b'{"bogus_type":1}',
            b'[]',
            b'null',
            b'{"workflow_created":1e400}',
        )):
            status, data = self.call(
                "PUT", "/prices", raw=raw, key=f"p-bad-{index}", headers=headers
            )
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # Nothing was written.
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual({"prices": None}, json.loads(data))

    def test_get_prices_rejects_unknown_query_parameters(self):
        headers = {"X-Tenant-Id": "http-prices-c"}
        for path in ("/prices?bogus=1", "/prices?type=workflow_created"):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_idempotent_redeclaration_and_cross_operation_conflict_over_http(self):
        headers = {"X-Tenant-Id": "http-prices-d"}
        status, first = self.call(
            "PUT", "/prices", {"workflow_created": 111}, key="p-shared", headers=headers
        )
        self.assertEqual(200, status)
        status, repeated = self.call(
            "POST", "/prices", {"workflow_created": 222}, key="p-shared", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(first, repeated)
        status, data = self.call("PUT", "/quotas", {"workflows": 1, "executions": 1},
                                 key="p-shared", headers=headers)
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual({"prices": {"workflow_created": 111}}, json.loads(data))

    def test_bill_over_http_reflects_declared_prices_and_tenant_isolation(self):
        alpha = {"X-Tenant-Id": "http-prices-e"}
        beta = {"X-Tenant-Id": "http-prices-f"}
        for tenant_headers in (alpha, beta):
            self.call("POST", "/workflows", {"id": "wf", "nodes": TASK},
                      key=f"wf-{tenant_headers['X-Tenant-Id']}", headers=tenant_headers)
            self.call("POST", "/executions",
                      {"id": "r", "workflow_id": "wf", "input": {}},
                      key=f"r-{tenant_headers['X-Tenant-Id']}", headers=tenant_headers)
        self.call("PUT", "/prices", {"workflow_created": 808}, key="pe", headers=alpha)
        status, data = self.call("GET", "/bill", headers=alpha)
        self.assertEqual(200, status)
        items = {item["type"]: item for item in json.loads(data)["bill"]["items"]}
        self.assertEqual(808, items["workflow_created"]["unit_price"])
        # Beta's answer is unaffected by alpha's declaration.
        status, data = self.call("GET", "/bill", headers=beta)
        items = {item["type"]: item for item in json.loads(data)["bill"]["items"]}
        self.assertEqual(USAGE_UNIT_PRICES["workflow_created"], items["workflow_created"]["unit_price"])
        status, data = self.call("GET", "/bill?bucket=day", headers=alpha)
        self.assertEqual(200, status)
        bucket = json.loads(data)["bill"]["buckets"][0]
        self.assertEqual(808, next(
            item["unit_price"] for item in bucket["items"] if item["type"] == "workflow_created"
        ))

    def test_illegal_bucket_value_is_a_validation_error_over_http(self):
        headers = {"X-Tenant-Id": "http-prices-g"}
        for path in ("/usage?bucket=year", "/bill?bucket=Year", "/usage?bucket="):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # Nothing was written or metered by the rejected queries.
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual({"usage": []}, json.loads(data))


if __name__ == "__main__":
    unittest.main()
