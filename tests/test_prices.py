import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import USAGE_UNIT_PRICES, ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


def _bill_items(service, tenant):
    return {entry["type"]: entry for entry in service.bill(tenant)["bill"]["items"]}


class PriceServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "prices.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _some_usage(self, tenant="acme"):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", tenant)
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", tenant)
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", tenant)

    def test_never_declared_price_table_is_a_definite_empty_result(self):
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_prices_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.declare_prices({"execution_started": 200}, "p-default", "")
        with self.assertRaises(ValidationError):
            self.service.get_prices("")

    def test_prices_require_an_idempotency_key(self):
        with self.assertRaises(ValidationError):
            self.service.declare_prices({"execution_started": 200}, None, "acme")

    def test_declared_prices_are_stored_sorted_and_read_back(self):
        result = self.service.declare_prices(
            {"workflow_created": 1500, "execution_started": 200}, "p1", "acme"
        )
        self.assertEqual(
            {"prices": {"execution_started": 200, "workflow_created": 1500}},
            result,
        )
        self.assertEqual(
            {"prices": {"execution_started": 200, "workflow_created": 1500}},
            self.service.get_prices("acme"),
        )

    def test_redeclaring_replaces_the_table_as_a_whole(self):
        self.service.declare_prices(
            {"execution_started": 200, "workflow_created": 1500}, "p1", "acme"
        )
        self.service.declare_prices({"execution_started": 300}, "p2", "acme")
        # The new table holds exactly this declaration: workflow_created is no
        # longer declared and falls back to the built-in default.
        self.assertEqual({"prices": {"execution_started": 300}}, self.service.get_prices("acme"))

    def test_invalid_price_declarations_are_validation_errors(self):
        bad_bodies = [
            {},
            [],
            None,
            "execution_started",
            42,
            {"execution_started": 0},
            {"execution_started": -5},
            {"execution_started": 1.5},
            {"execution_started": "200"},
            {"execution_started": True},
            {"execution_started": None},
            {"execution_started": float("inf")},
            {"execution_started": float("nan")},
            {"not_a_metered_type": 100},
            {"execution_started": 200, "unknown_type": 100},
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.declare_prices(body, f"bad-{index}", "acme")
        # Every rejected declaration writes nothing.
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_replaying_a_key_returns_the_first_result_without_a_second_replace(self):
        first = self.service.declare_prices({"execution_started": 200}, "p", "acme")
        # The same key carrying a different declaration is the first result
        # again and performs no replacement.
        replay = self.service.declare_prices({"execution_started": 999}, "p", "acme")
        self.assertEqual(first, replay)
        self.assertEqual({"prices": {"execution_started": 200}}, self.service.get_prices("acme"))

    def test_cross_operation_key_reuse_conflicts_and_leaves_the_table(self):
        self.service.declare_quota({"workflows": 10, "executions": 10}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.declare_prices({"execution_started": 200}, "shared", "acme")
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))
        # A price key reused for a quota declaration likewise conflicts.
        self.service.declare_prices({"execution_started": 200}, "price-only", "acme")
        with self.assertRaises(ConflictError):
            self.service.declare_quota({"workflows": 1, "executions": 1}, "price-only", "acme")

    def test_bill_uses_declared_prices_and_defaults_for_unnamed_types(self):
        self._some_usage()
        default_bill = self.service.bill("acme")["bill"]
        self.service.declare_prices({"execution_started": 200}, "p", "acme")
        items = _bill_items(self.service, "acme")
        self.assertEqual(2, items["execution_started"]["count"])
        self.assertEqual(200, items["execution_started"]["unit_price"])
        self.assertEqual(400, items["execution_started"]["subtotal"])
        # workflow_created was left at the built-in default.
        self.assertEqual(
            USAGE_UNIT_PRICES["workflow_created"],
            items["workflow_created"]["unit_price"],
        )
        total = sum(entry["subtotal"] for entry in items.values())
        self.assertEqual(total, self.service.bill("acme")["bill"]["total"])
        # Counts and the pre-declaration answer basis are unchanged.
        self.assertEqual(
            [
                {"type": "execution_started", "count": 2},
                {"type": "workflow_created", "count": 1},
            ],
            self.service.usage("acme")["usage"],
        )
        before = {
            entry["type"]: (entry["unit_price"], entry["subtotal"])
            for entry in default_bill["items"]
        }
        self.assertEqual(
            (USAGE_UNIT_PRICES["execution_started"], 2 * USAGE_UNIT_PRICES["execution_started"]),
            before["execution_started"],
        )

    def test_repricing_changes_only_later_bills_not_recorded_counts(self):
        self._some_usage()
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        self.assertEqual(400, _bill_items(self.service, "acme")["execution_started"]["subtotal"])
        self.service.declare_prices({"execution_started": 350}, "p2", "acme")
        items = _bill_items(self.service, "acme")
        self.assertEqual(2, items["execution_started"]["count"])
        self.assertEqual(350, items["execution_started"]["unit_price"])
        self.assertEqual(700, items["execution_started"]["subtotal"])
        # Recorded usage never moves with a price change.
        self.assertEqual(2, self.service.usage("acme")["usage"][0]["count"])

    def test_bucketed_bill_uses_the_prices_in_effect_at_query_time(self):
        self._some_usage()
        self.service.declare_prices({"execution_started": 250}, "p", "acme")
        bill = self.service.bill("acme", bucket="hour")["bill"]
        for bucket in bill["buckets"]:
            for item in bucket["items"]:
                expected = 250 if item["type"] == "execution_started" else USAGE_UNIT_PRICES[item["type"]]
                self.assertEqual(expected, item["unit_price"])
                self.assertEqual(item["count"] * expected, item["subtotal"])
            self.assertEqual(sum(i["subtotal"] for i in bucket["items"]), bucket["subtotal"])
        self.assertEqual(sum(b["subtotal"] for b in bill["buckets"]), bill["total"])

    def test_declaring_prices_meters_nothing(self):
        self.service.declare_prices({"execution_started": 200}, "p", "acme")
        self.assertEqual({"usage": []}, self.service.usage("acme"))

    def test_price_tables_are_isolated_between_tenants(self):
        self._some_usage("acme")
        self._some_usage("beta")
        self.service.declare_prices({"execution_started": 200}, "p", "acme")
        self.assertEqual({"prices": None}, self.service.get_prices("beta"))
        beta_items = _bill_items(self.service, "beta")
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"],
            beta_items["execution_started"]["unit_price"],
        )
        acme_items = _bill_items(self.service, "acme")
        self.assertEqual(200, acme_items["execution_started"]["unit_price"])

    def test_invalid_bucket_is_rejected_at_the_service_layer(self):
        with self.assertRaises(ValidationError):
            self.service.usage("acme", bucket="year")
        with self.assertRaises(ValidationError):
            self.service.bill("acme", bucket="")
        # The rejected query leaves no table and no recorded state behind.
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))
        self.assertEqual({"usage": []}, self.service.usage("acme"))

    def test_delete_without_a_declaration_is_the_definite_empty_result(self):
        self.assertEqual({"prices": None}, self.service.delete_prices({}, "d1", "acme"))
        # The tenant still reads as never having declared a table.
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_delete_removes_the_declared_table_as_a_whole(self):
        self.service.declare_prices(
            {"execution_started": 200, "workflow_created": 1500}, "p1", "acme"
        )
        self.assertEqual({"prices": None}, self.service.delete_prices({}, "d1", "acme"))
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))
        # A later declaration works exactly as the first one ever did.
        redeclared = self.service.declare_prices({"execution_started": 250}, "p2", "acme")
        self.assertEqual({"prices": {"execution_started": 250}}, redeclared)
        self.assertEqual(
            {"prices": {"execution_started": 250}}, self.service.get_prices("acme")
        )

    def test_delete_requires_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.delete_prices({}, "d-default", "")
        # A delete rejected for a missing tenant writes nowhere.
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        with self.assertRaises(ValidationError):
            self.service.delete_prices({}, "d-default", "")
        self.assertEqual(
            {"prices": {"execution_started": 200}}, self.service.get_prices("acme")
        )

    def test_delete_requires_an_idempotency_key(self):
        with self.assertRaises(ValidationError):
            self.service.delete_prices({}, None, "acme")
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_delete_body_must_be_an_empty_object(self):
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        bad_bodies = [
            {"execution_started": 1},
            {"unknown_type": 1},
            [],
            None,
            "",
            42,
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.delete_prices(body, f"d-bad-{index}", "acme")
        # Every rejected delete leaves the declared table in place.
        self.assertEqual(
            {"prices": {"execution_started": 200}}, self.service.get_prices("acme")
        )

    def test_replaying_a_delete_returns_the_first_result_without_a_second_effect(self):
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        first = self.service.delete_prices({}, "d", "acme")
        self.assertEqual({"prices": None}, first)
        # The same delete with the same key returns the first result and
        # performs no second removal.
        replay = self.service.delete_prices({}, "d", "acme")
        self.assertEqual(first, replay)
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_replaying_a_delete_that_matched_nothing_returns_the_first_result(self):
        first = self.service.delete_prices({}, "d", "acme")
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        # Replaying an earlier no-op delete cannot remove the later table.
        replay = self.service.delete_prices({}, "d", "acme")
        self.assertEqual(first, replay)
        self.assertEqual(
            {"prices": {"execution_started": 200}}, self.service.get_prices("acme")
        )

    def test_distinct_delete_keys_each_return_the_empty_result(self):
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        self.assertEqual({"prices": None}, self.service.delete_prices({}, "d1", "acme"))
        # A second delete against the now-absent table is not an error.
        self.assertEqual({"prices": None}, self.service.delete_prices({}, "d2", "acme"))
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_cross_operation_key_reuse_with_delete_conflicts_and_leaves_the_table(self):
        self.service.declare_prices({"execution_started": 200}, "p1", "acme")
        # A declaration key reused for a delete conflicts and removes nothing.
        with self.assertRaises(ConflictError):
            self.service.delete_prices({}, "p1", "acme")
        self.assertEqual(
            {"prices": {"execution_started": 200}}, self.service.get_prices("acme")
        )
        # A delete key reused for a declaration conflicts and writes nothing.
        self.service.delete_prices({}, "shared", "beta")
        with self.assertRaises(ConflictError):
            self.service.declare_prices({"execution_started": 300}, "shared", "beta")
        self.assertEqual({"prices": None}, self.service.get_prices("beta"))
        # A quota key reused for a delete likewise conflicts.
        self.service.declare_quota({"workflows": 10, "executions": 10}, "quota-key", "acme")
        with self.assertRaises(ConflictError):
            self.service.delete_prices({}, "quota-key", "acme")
        self.assertEqual(
            {"prices": {"execution_started": 200}}, self.service.get_prices("acme")
        )

    def test_bill_returns_to_built_in_defaults_after_delete(self):
        self._some_usage()
        self.service.declare_prices(
            {"execution_started": 200, "workflow_created": 1500}, "p1", "acme"
        )
        self.assertEqual(200, _bill_items(self.service, "acme")["execution_started"]["unit_price"])
        self.service.delete_prices({}, "d1", "acme")
        items = _bill_items(self.service, "acme")
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"], items["execution_started"]["unit_price"]
        )
        self.assertEqual(
            USAGE_UNIT_PRICES["workflow_created"], items["workflow_created"]["unit_price"]
        )
        total = sum(entry["subtotal"] for entry in items.values())
        self.assertEqual(total, self.service.bill("acme")["bill"]["total"])
        # Recorded usage counts never move with the delete.
        self.assertEqual(
            [
                {"type": "execution_started", "count": 2},
                {"type": "workflow_created", "count": 1},
            ],
            self.service.usage("acme")["usage"],
        )

    def test_bucketed_bill_uses_defaults_after_delete(self):
        self._some_usage()
        self.service.declare_prices({"execution_started": 250}, "p", "acme")
        self.service.delete_prices({}, "d", "acme")
        bill = self.service.bill("acme", bucket="hour")["bill"]
        for bucket in bill["buckets"]:
            for item in bucket["items"]:
                self.assertEqual(USAGE_UNIT_PRICES[item["type"]], item["unit_price"])
                self.assertEqual(
                    item["count"] * USAGE_UNIT_PRICES[item["type"]], item["subtotal"]
                )
            self.assertEqual(sum(i["subtotal"] for i in bucket["items"]), bucket["subtotal"])
        self.assertEqual(sum(b["subtotal"] for b in bill["buckets"]), bill["total"])

    def test_deleting_prices_meters_nothing(self):
        self.service.delete_prices({}, "d", "acme")
        self.assertEqual({"usage": []}, self.service.usage("acme"))

    def test_delete_is_isolated_between_tenants(self):
        self._some_usage("acme")
        self._some_usage("beta")
        self.service.declare_prices({"execution_started": 200}, "p-a", "acme")
        self.service.declare_prices({"execution_started": 300}, "p-b", "beta")
        self.service.delete_prices({}, "d-a", "acme")
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))
        # Beta's table and pricing are untouched by acme's delete.
        self.assertEqual(
            {"prices": {"execution_started": 300}}, self.service.get_prices("beta")
        )
        self.assertEqual(
            300, _bill_items(self.service, "beta")["execution_started"]["unit_price"]
        )


class PriceHttpTests(unittest.TestCase):
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

    def test_price_routes_over_http(self):
        headers = {"X-Tenant-Id": "http-a"}
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(b'{"prices":null}\n', data)
        for method in ("PUT", "POST"):
            tenant = f"http-{method.lower()}"
            status, data = self.call(
                method,
                "/prices",
                {"workflow_created": 1500, "execution_started": 200},
                key=f"p-{method.lower()}",
                headers={"X-Tenant-Id": tenant},
            )
            self.assertEqual(200, status, data)
            self.assertEqual(
                b'{"prices":{"execution_started":200,"workflow_created":1500}}\n',
                data,
            )
            status, data = self.call("GET", "/prices", headers={"X-Tenant-Id": tenant})
            self.assertEqual(200, status)
            self.assertEqual(
                b'{"prices":{"execution_started":200,"workflow_created":1500}}\n',
                data,
            )

    def test_price_routes_without_tenant_are_validation_errors(self):
        status, data = self.call("GET", "/prices")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("PUT", "/prices", {"execution_started": 200}, key="p-no-tenant")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "PUT", "/prices", {"execution_started": 200}, key="p-empty-tenant",
            headers={"X-Tenant-Id": ""},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_price_read_rejects_unknown_query_parameters(self):
        for path in ("/prices?bogus=1", "/prices?type=execution_started"):
            status, data = self.call("GET", path, headers={"X-Tenant-Id": "http-q"})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_price_validation_errors_over_http(self):
        raw_bodies = (
            b'{}',
            b'[]',
            b'null',
            b'{"execution_started":0}',
            b'{"execution_started":-1}',
            b'{"execution_started":1.5}',
            b'{"execution_started":true}',
            b'{"execution_started":"200"}',
            b'{"execution_started":null}',
            b'{"not_a_type":100}',
            b'{"execution_started":200,"extra":1}',
            b'{"execution_started":1e400}',
        )
        for index, raw in enumerate(raw_bodies):
            status, data = self.call(
                "PUT", "/prices", raw=raw, key=f"p-bad-{index}", headers={"X-Tenant-Id": "http-bad"}
            )
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], raw)
        status, data = self.call("GET", "/prices", headers={"X-Tenant-Id": "http-bad"})
        self.assertEqual(200, status)
        self.assertEqual(b'{"prices":null}\n', data)

    def test_price_idempotency_replay_and_cross_operation_conflict(self):
        headers = {"X-Tenant-Id": "http-idem"}
        status, first = self.call(
            "PUT", "/prices", {"execution_started": 200}, key="p-idem", headers=headers
        )
        self.assertEqual(200, status)
        status, replay = self.call(
            "POST", "/prices", {"execution_started": 999}, key="p-idem", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(first, replay)
        status, data = self.call(
            "PUT", "/quotas", {"workflows": 1, "executions": 1},
            key="p-idem", headers=headers,
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(b'{"prices":{"execution_started":200}}\n', data)

    def test_delete_price_route_over_http(self):
        headers = {"X-Tenant-Id": "http-del"}
        # Deleting a never-declared table is the definite empty result.
        status, data = self.call("DELETE", "/prices", {}, key="d-empty", headers=headers)
        self.assertEqual(200, status, data)
        self.assertEqual(b'{"prices":null}\n', data)
        self.call(
            "PUT", "/prices",
            {"workflow_created": 1500, "execution_started": 200},
            key="d-declare", headers=headers,
        )
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(
            b'{"prices":{"execution_started":200,"workflow_created":1500}}\n', data
        )
        status, data = self.call("DELETE", "/prices", {}, key="d-del", headers=headers)
        self.assertEqual(200, status, data)
        self.assertEqual(b'{"prices":null}\n', data)
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(b'{"prices":null}\n', data)

    def test_delete_without_a_tenant_is_a_validation_error(self):
        status, data = self.call("DELETE", "/prices", {}, key="d-no-tenant")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "DELETE", "/prices", {}, key="d-empty-tenant", headers={"X-Tenant-Id": ""}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_delete_without_an_idempotency_key_is_a_validation_error(self):
        headers = {"X-Tenant-Id": "http-del-nokey"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p", headers=headers)
        status, data = self.call("DELETE", "/prices", {}, headers=headers)
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # The rejected delete touched no table.
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(b'{"prices":{"execution_started":200}}\n', data)

    def test_delete_validation_errors_over_http(self):
        headers = {"X-Tenant-Id": "http-del-bad"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p", headers=headers)
        raw_bodies = (
            b'{"execution_started":1}',
            b'{"extra":1}',
            b'[]',
            b'null',
            b'""',
            b'42',
            b'{"execution_started":1e400}',
        )
        for index, raw in enumerate(raw_bodies):
            status, data = self.call(
                "DELETE", "/prices", raw=raw, key=f"d-bad-{index}", headers=headers
            )
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], raw)
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(b'{"prices":{"execution_started":200}}\n', data)

    def test_delete_replay_and_cross_operation_conflict_over_http(self):
        headers = {"X-Tenant-Id": "http-del-idem"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p1", headers=headers)
        status, first = self.call("DELETE", "/prices", {}, key="d", headers=headers)
        self.assertEqual(200, status)
        status, replay = self.call("DELETE", "/prices", {}, key="d", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(first, replay)
        # Reusing a declaration key for a delete conflicts and leaves the table.
        self.call("PUT", "/prices", {"execution_started": 200}, key="p2", headers=headers)
        status, data = self.call("DELETE", "/prices", {}, key="p2", headers=headers)
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(b'{"prices":{"execution_started":200}}\n', data)
        # A delete key reused for a declaration conflicts and writes nothing.
        status, data = self.call(
            "PUT", "/prices", {"execution_started": 999}, key="d", headers=headers
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/prices", headers=headers)
        self.assertEqual(b'{"prices":{"execution_started":200}}\n', data)

    def test_delete_rejects_unknown_query_parameters_by_name(self):
        headers = {"X-Tenant-Id": "http-del-query"}
        for path in ("/prices?bogus=1", "/prices?type=execution_started"):
            status, data = self.call("DELETE", path, {}, key=f"dq-{path}", headers=headers)
            self.assertEqual(400, status, path)
            payload = json.loads(data)
            self.assertEqual("validation_error", payload["error"]["code"], path)
            parameter = "bogus" if "bogus" in path else "type"
            self.assertIn(parameter, payload["error"]["message"], path)

    def test_price_read_unknown_query_parameter_message_names_the_parameter(self):
        status, data = self.call(
            "GET", "/prices?bogus=1", headers={"X-Tenant-Id": "http-q-name"}
        )
        self.assertEqual(400, status)
        self.assertIn("bogus", json.loads(data)["error"]["message"])

    def test_delete_does_not_change_another_tenants_bill(self):
        alpha = {"X-Tenant-Id": "http-del-alpha"}
        beta = {"X-Tenant-Id": "http-del-beta"}
        for headers in (alpha, beta):
            self.call("POST", "/workflows", {"id": "wf", "nodes": TASK}, key="wf", headers=headers)
            self.call(
                "POST", "/executions",
                {"id": "r", "workflow_id": "wf", "input": {}},
                key="r", headers=headers,
            )
            self.call("PUT", "/prices", {"execution_started": 200}, key="p", headers=headers)
        self.call("DELETE", "/prices", {}, key="d", headers=alpha)
        status, data = self.call("GET", "/bill", headers=beta)
        self.assertEqual(200, status)
        items = json.loads(data)["bill"]["items"]
        self.assertEqual(
            200, next(item["unit_price"] for item in items if item["type"] == "execution_started")
        )
        status, data = self.call("GET", "/bill", headers=alpha)
        self.assertEqual(200, status)
        items = json.loads(data)["bill"]["items"]
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"],
            next(item["unit_price"] for item in items if item["type"] == "execution_started"),
        )

    def test_bill_without_a_declaration_is_byte_for_byte_unchanged(self):
        headers = {"X-Tenant-Id": "http-default"}
        self.call(
            "POST", "/workflows", {"id": "wf", "nodes": TASK}, key="wf", headers=headers
        )
        self.call(
            "POST", "/executions",
            {"id": "r1", "workflow_id": "wf", "input": {}},
            key="r1", headers=headers,
        )
        status, data = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        expected = (
            b'{"bill":{"items":[{"type":"execution_started","count":1,"unit_price":'
            + str(USAGE_UNIT_PRICES["execution_started"]).encode()
            + b',"subtotal":'
            + str(USAGE_UNIT_PRICES["execution_started"]).encode()
            + b'},{"type":"workflow_created","count":1,"unit_price":'
            + str(USAGE_UNIT_PRICES["workflow_created"]).encode()
            + b',"subtotal":'
            + str(USAGE_UNIT_PRICES["workflow_created"]).encode()
            + b'}],"total":'
            + str(
                USAGE_UNIT_PRICES["execution_started"] + USAGE_UNIT_PRICES["workflow_created"]
            ).encode()
            + b'}}\n'
        )
        self.assertEqual(expected, data)

    def test_bill_refuses_another_tenants_price_table(self):
        alpha = {"X-Tenant-Id": "http-alpha"}
        beta = {"X-Tenant-Id": "http-beta"}
        self.call(
            "POST", "/workflows", {"id": "wf", "nodes": TASK}, key="wf-a", headers=alpha
        )
        self.call(
            "POST", "/executions",
            {"id": "r", "workflow_id": "wf", "input": {}},
            key="r-a", headers=alpha,
        )
        self.call(
            "POST", "/workflows", {"id": "wf", "nodes": TASK}, key="wf-b", headers=beta
        )
        self.call(
            "POST", "/executions",
            {"id": "r", "workflow_id": "wf", "input": {}},
            key="r-b", headers=beta,
        )
        self.call("PUT", "/prices", {"execution_started": 200}, key="p-a", headers=alpha)
        status, data = self.call("GET", "/bill", headers=beta)
        self.assertEqual(200, status)
        items = json.loads(data)["bill"]["items"]
        self.assertEqual(
            USAGE_UNIT_PRICES["execution_started"],
            next(item["unit_price"] for item in items if item["type"] == "execution_started"),
        )


if __name__ == "__main__":
    unittest.main()
