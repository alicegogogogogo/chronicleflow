import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow, _parse_timestamp

T0 = "2026-01-01T08:00:00.000Z"
T1 = "2026-01-01T09:00:00.000Z"
T2 = "2026-01-01T10:00:00.000Z"
T_MID = "2026-01-01T08:30:00.000Z"


def _ts(value):
    return _parse_timestamp(value, "since")


class PriceHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "history.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _stamp(self, sequence, occurred_at, tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE price_history SET occurred_at = ? WHERE tenant = ? AND sequence = ?",
                (occurred_at, tenant, sequence),
            )

    def _declare(self, prices, key, tenant="acme"):
        return self.service.declare_prices(prices, key, tenant)

    def _delete(self, key, tenant="acme"):
        return self.service.delete_prices({}, key, tenant)

    def test_empty_tenant_gets_a_definite_empty_list(self):
        self.assertEqual({"history": []}, self.service.price_history("acme"))

    def test_a_tenant_is_required(self):
        with self.assertRaises(ValidationError):
            self.service.price_history("")

    def test_declare_records_one_history_entry_with_the_effective_snapshot(self):
        self._declare({"workflow_created": 1500, "execution_started": 200}, "p1")
        history = self.service.price_history("acme")["history"]
        self.assertEqual(1, len(history))
        record = history[0]
        self.assertEqual(1, record["sequence"])
        self.assertEqual("declare", record["action"])
        self.assertTrue(record["occurred_at"].endswith("Z"))
        # The snapshot lists only the named types, in ascending type order.
        self.assertEqual(
            {"execution_started": 200, "workflow_created": 1500}, record["snapshot"]
        )
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(record)
        )

    def test_delete_records_one_history_entry_with_a_null_snapshot(self):
        self._declare({"execution_started": 200}, "p1")
        self._delete("d1")
        history = self.service.price_history("acme")["history"]
        self.assertEqual(["declare", "delete"], [record["action"] for record in history])
        deleted = history[1]
        self.assertEqual(2, deleted["sequence"])
        self.assertEqual("delete", deleted["action"])
        self.assertIsNone(deleted["snapshot"])
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(deleted)
        )

    def test_deleting_a_never_declared_table_still_records_the_delete(self):
        self._delete("d1")
        history = self.service.price_history("acme")["history"]
        self.assertEqual(1, len(history))
        self.assertEqual("delete", history[0]["action"])
        self.assertIsNone(history[0]["snapshot"])

    def test_redeclaration_records_each_effective_table(self):
        self._declare({"execution_started": 200, "workflow_created": 1500}, "p1")
        self._declare({"execution_started": 300}, "p2")
        history = self.service.price_history("acme")["history"]
        self.assertEqual(
            [
                {"execution_started": 200, "workflow_created": 1500},
                {"execution_started": 300},
            ],
            [record["snapshot"] for record in history],
        )
        self.assertEqual([1, 2], [record["sequence"] for record in history])

    def test_records_order_by_occurrence_time_with_sequence_breaking_ties(self):
        # Three changes, stamped T1, T0, T0 so chronological order
        # (2, 3, 1) differs from sequence order and two share one instant.
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d1")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)
        history = self.service.price_history("acme")["history"]
        self.assertEqual([2, 3, 1], [record["sequence"] for record in history])
        self.assertEqual(
            ["declare", "delete", "declare"], [record["action"] for record in history]
        )
        self.assertEqual([T0, T0, T1], [record["occurred_at"] for record in history])
        for record in history:
            self.assertIsInstance(record["sequence"], int)
            self.assertGreaterEqual(record["sequence"], 1)
            self.assertEqual(
                ["sequence", "action", "occurred_at", "snapshot"], list(record)
            )

    def test_window_is_a_closed_interval(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d1")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)
        point = self.service.price_history("acme", since=_ts(T0), until=_ts(T0))["history"]
        self.assertEqual([2, 3], [record["sequence"] for record in point])
        open_since = self.service.price_history("acme", since=_ts(T_MID), until=None)["history"]
        self.assertEqual([1], [record["sequence"] for record in open_since])
        open_until = self.service.price_history("acme", since=None, until=_ts(T_MID))["history"]
        self.assertEqual([2, 3], [record["sequence"] for record in open_until])

    def test_reversed_window_is_an_empty_list_not_an_error(self):
        self._declare({"execution_started": 100}, "p1")
        self.assertEqual(
            {"history": []},
            self.service.price_history("acme", since=_ts(T2), until=_ts(T0)),
        )

    def test_window_does_not_renumber_sequences(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._stamp(1, T0)
        self._stamp(2, T2)
        window = self.service.price_history("acme", since=_ts(T1), until=_ts(T2))["history"]
        self.assertEqual([2], [record["sequence"] for record in window])

    def test_action_filter_keeps_only_named_actions(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d1")
        declares = self.service.price_history("acme", actions=("declare",))["history"]
        self.assertEqual([1, 2], [record["sequence"] for record in declares])
        self.assertTrue(all(record["action"] == "declare" for record in declares))
        deletes = self.service.price_history("acme", actions=("delete",))["history"]
        self.assertEqual([3], [record["sequence"] for record in deletes])
        both = self.service.price_history(
            "acme", actions=("declare", "delete"), limit=10
        )["history"]
        self.assertEqual([1, 2, 3], [record["sequence"] for record in both])

    def test_action_filter_intersects_the_window_and_cursor(self):
        self._declare({"execution_started": 100}, "p1")
        self._delete("d1")
        self._declare({"execution_started": 200}, "p2")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)
        # Only the delete at T0 survives action + window; cursor 2 then reaches
        # nothing past it.
        page = self.service.price_history(
            "acme",
            actions=("delete",),
            since=_ts(T0),
            until=_ts(T0),
            cursor=2,
            limit=10,
        )["history"]
        self.assertEqual([], [record["sequence"] for record in page])
        # The declaration at T0 survives action + window past cursor 2.
        page = self.service.price_history(
            "acme",
            actions=("declare",),
            since=_ts(T0),
            until=_ts(T0),
            cursor=2,
            limit=10,
        )["history"]
        self.assertEqual([3], [record["sequence"] for record in page])

    def test_cursor_keeps_only_strictly_greater_sequences(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d1")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)
        # The cursor compares sequences, not time positions: the later record
        # with sequence 1 is excluded along with sequence 2, leaving only 3.
        page = self.service.price_history("acme", cursor=2, limit=10)["history"]
        self.assertEqual([3], [record["sequence"] for record in page])

    def test_cursor_past_the_end_is_empty(self):
        self._declare({"execution_started": 100}, "p1")
        self.assertEqual([], self.service.price_history("acme", cursor=99)["history"])

    def test_limit_caps_the_page(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        page = self.service.price_history("acme", limit=1)["history"]
        self.assertEqual([1], [record["sequence"] for record in page])

    def test_consecutive_pages_cover_every_hit_once(self):
        self._declare({"execution_started": 100}, "p1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d1")
        collected = []
        cursor = None
        while True:
            page = self.service.price_history("acme", cursor=cursor, limit=1)["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(
            self.service.price_history("acme")["history"], collected
        )
        self.assertEqual([1, 2, 3], [record["sequence"] for record in collected])

    def test_filtered_pages_cover_every_hit_once(self):
        self._declare({"execution_started": 100}, "p1")
        self._delete("d1")
        self._declare({"execution_started": 200}, "p2")
        self._delete("d2")
        collected = []
        cursor = None
        while True:
            page = self.service.price_history(
                "acme", actions=("delete",), cursor=cursor, limit=1
            )["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual([2, 4], [record["sequence"] for record in collected])

    def test_replaying_a_declare_appends_no_second_record(self):
        first = self._declare({"execution_started": 200}, "p")
        replay = self._declare({"execution_started": 999}, "p")
        self.assertEqual(first, replay)
        history = self.service.price_history("acme")["history"]
        self.assertEqual(1, len(history))
        self.assertEqual({"execution_started": 200}, history[0]["snapshot"])

    def test_replaying_a_delete_appends_no_second_record(self):
        self._declare({"execution_started": 200}, "p1")
        first = self._delete("d")
        replay = self._delete("d")
        self.assertEqual(first, replay)
        history = self.service.price_history("acme")["history"]
        self.assertEqual(["declare", "delete"], [record["action"] for record in history])

    def test_cross_operation_key_reuse_writes_no_history(self):
        self.service.declare_quota({"workflows": 10, "executions": 10}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self._declare({"execution_started": 200}, "shared")
        self.assertEqual({"history": []}, self.service.price_history("acme"))
        # A delete key reused for a declaration likewise leaves no record.
        self._delete("shared2")
        with self.assertRaises(ConflictError):
            self._declare({"execution_started": 300}, "shared2")
        self.assertEqual(
            ["delete"],
            [record["action"] for record in self.service.price_history("acme")["history"]],
        )

    def test_rejected_declaration_writes_no_history(self):
        for body in ({}, [], None, {"not_a_metered_type": 100}, {"execution_started": 0}):
            with self.assertRaises(ValidationError):
                self.service.declare_prices(body, f"bad-{body!r}", "acme")
        self.assertEqual({"history": []}, self.service.price_history("acme"))
        self.assertEqual({"prices": None}, self.service.get_prices("acme"))

    def test_rejected_delete_writes_no_history(self):
        for body in ({"execution_started": 1}, [], None):
            with self.assertRaises(ValidationError):
                self.service.delete_prices(body, f"bad-{body!r}", "acme")
        self.assertEqual({"history": []}, self.service.price_history("acme"))

    def test_history_is_isolated_between_tenants(self):
        self._declare({"execution_started": 200}, "p-a", "acme")
        self._declare({"execution_started": 300}, "p-b", "beta")
        self._delete("d-a", "acme")
        acme = self.service.price_history("acme")["history"]
        beta = self.service.price_history("beta")["history"]
        self.assertEqual(["declare", "delete"], [record["action"] for record in acme])
        self.assertEqual(["declare"], [record["action"] for record in beta])
        # Sequences are per tenant, so beta's lone record is its own sequence 1.
        self.assertEqual([1], [record["sequence"] for record in beta])
        # Filters and pages never cross the tenant boundary.
        self.assertEqual(
            [2],
            [
                record["sequence"]
                for record in self.service.price_history(
                    "acme", actions=("delete",), cursor=1, limit=10
                )["history"]
            ],
        )
        self.assertEqual(
            [], self.service.price_history("beta", since=_ts(T1), until=_ts(T2))["history"]
        )

    def test_history_query_is_read_only(self):
        self._declare({"execution_started": 200}, "p1")
        self.service.price_history("acme")
        self.service.price_history(
            "acme",
            actions=("declare", "delete"),
            since=_ts(T0),
            until=_ts(T2),
            limit=1,
        )
        # The price table, usage, and the history itself are unchanged by reads.
        self.assertEqual({"prices": {"execution_started": 200}}, self.service.get_prices("acme"))
        self.assertEqual({"usage": []}, self.service.usage("acme"))
        history = self.service.price_history("acme")["history"]
        self.service.price_history("acme")
        self.assertEqual(history, self.service.price_history("acme")["history"])
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM price_history WHERE tenant = 'acme'"
        ).fetchone()
        self.assertEqual(1, rows["used"])


class PriceHistoryHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-history.db"))
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
        payload = json.dumps(body) if body is not None else None
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_requires_a_tenant_header(self):
        for path in ("/prices/history", "/prices/history?limit=1"):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)
            status, data = self.call("GET", path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_empty_result_is_a_definite_empty_list_with_one_trailing_newline(self):
        status, data = self.call(
            "GET", "/prices/history?limit=10", headers={"X-Tenant-Id": "hist-empty"}
        )
        self.assertEqual(200, status)
        self.assertEqual(b'{"history":[]}\n', data)

    def test_history_over_http_follows_declare_and_delete(self):
        headers = {"X-Tenant-Id": "hist-flow"}
        status, data = self.call(
            "PUT", "/prices",
            {"workflow_created": 1500, "execution_started": 200},
            key="p1", headers=headers,
        )
        self.assertEqual(200, status, data)
        status, data = self.call("DELETE", "/prices", {}, key="d1", headers=headers)
        self.assertEqual(200, status, data)
        status, data = self.call(
            "PUT", "/prices", {"execution_started": 250}, key="p2", headers=headers
        )
        self.assertEqual(200, status, data)
        status, data = self.call("GET", "/prices/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        history = json.loads(data)["history"]
        self.assertEqual(
            ["declare", "delete", "declare"], [record["action"] for record in history]
        )
        self.assertEqual([1, 2, 3], [record["sequence"] for record in history])
        self.assertEqual(
            {"execution_started": 200, "workflow_created": 1500}, history[0]["snapshot"]
        )
        self.assertIsNone(history[1]["snapshot"])
        self.assertEqual({"execution_started": 250}, history[2]["snapshot"])
        for record in history:
            self.assertEqual(
                ["sequence", "action", "occurred_at", "snapshot"], list(record)
            )
            self.assertTrue(record["occurred_at"].endswith("Z"))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_action_filter_over_http(self):
        headers = {"X-Tenant-Id": "hist-action"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p1", headers=headers)
        self.call("DELETE", "/prices", {}, key="d1", headers=headers)
        status, data = self.call(
            "GET", "/prices/history?limit=10&action=delete", headers=headers
        )
        self.assertEqual(200, status)
        history = json.loads(data)["history"]
        self.assertEqual(["delete"], [record["action"] for record in history])
        status, data = self.call(
            "GET", "/prices/history?limit=10&action=declare,delete", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(
            ["declare", "delete"],
            [record["action"] for record in json.loads(data)["history"]],
        )

    def test_window_parameters_are_honored(self):
        headers = {"X-Tenant-Id": "hist-window"}
        # A reversed window of a populated tenant is an empty list, not an error.
        query = "?limit=10&since=2099-01-01T00:00:00.000Z&until=2000-01-01T00:00:00.000Z"
        status, data = self.call("GET", "/prices/history" + query, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"history": []}, json.loads(data))

    def test_pagination_walks_every_record_once(self):
        headers = {"X-Tenant-Id": "hist-pages"}
        self.call("PUT", "/prices", {"execution_started": 100}, key="p1", headers=headers)
        self.call("DELETE", "/prices", {}, key="d1", headers=headers)
        self.call("PUT", "/prices", {"execution_started": 200}, key="p2", headers=headers)
        collected = []
        query = "?limit=2"
        while True:
            status, data = self.call("GET", "/prices/history" + query, headers=headers)
            self.assertEqual(200, status)
            page = json.loads(data)["history"]
            if not page:
                break
            collected.extend(page)
            query = f"?limit=2&cursor={page[-1]['sequence']}"
        self.assertEqual([1, 2, 3], [record["sequence"] for record in collected])
        self.assertEqual(
            ["declare", "delete", "declare"], [record["action"] for record in collected]
        )

    def test_validation_errors_over_http(self):
        headers = {"X-Tenant-Id": "hist-bad"}
        bad_paths = (
            "/prices/history",
            "/prices/history?limit=0",
            "/prices/history?limit=-1",
            "/prices/history?limit=1.5",
            "/prices/history?limit=",
            "/prices/history?limit=1&cursor=0",
            "/prices/history?limit=1&cursor=-3",
            "/prices/history?limit=1&cursor=1.5",
            "/prices/history?limit=1&cursor=",
            "/prices/history?limit=1&since=not-a-timestamp",
            "/prices/history?limit=1&until=2026-01-01T00:00:00",
            "/prices/history?limit=1&action=declare,declare",
            "/prices/history?limit=1&action=declare,",
            "/prices/history?limit=1&action=",
            "/prices/history?limit=1&action=unknown",
            "/prices/history?limit=1&bogus=1",
            "/prices/history?limit=1&limit=2",
            "/prices/history?action=declare&action=delete&limit=1",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)
        # Every rejected query reveals no records.
        status, data = self.call("GET", "/prices/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"history": []}, json.loads(data))

    def test_replay_and_conflict_over_http_change_no_history(self):
        headers = {"X-Tenant-Id": "hist-idem"}
        status, first = self.call(
            "PUT", "/prices", {"execution_started": 200}, key="p", headers=headers
        )
        self.assertEqual(200, status)
        status, replay = self.call(
            "POST", "/prices", {"execution_started": 999}, key="p", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(first, replay)
        # Reusing the declaration key for a delete conflicts and records nothing.
        status, data = self.call("DELETE", "/prices", {}, key="p", headers=headers)
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/prices/history?limit=10", headers=headers)
        history = json.loads(data)["history"]
        self.assertEqual(["declare"], [record["action"] for record in history])
        self.assertEqual({"execution_started": 200}, history[0]["snapshot"])

    def test_other_tenants_history_is_invisible(self):
        alpha = {"X-Tenant-Id": "hist-alpha"}
        beta = {"X-Tenant-Id": "hist-beta"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p-a", headers=alpha)
        self.call("DELETE", "/prices", {}, key="d-a", headers=alpha)
        self.call("PUT", "/prices", {"execution_started": 300}, key="p-b", headers=beta)
        for query in (
            "?limit=10",
            "?limit=10&action=declare,delete",
            "?limit=10&since=2000-01-01T00:00:00.000Z&until=2099-01-01T00:00:00.000Z",
        ):
            status, data = self.call("GET", "/prices/history" + query, headers=beta)
            self.assertEqual(200, status, query)
            history = json.loads(data)["history"]
            self.assertEqual(["declare"], [record["action"] for record in history], query)
            self.assertEqual(
                [{"execution_started": 300}], [record["snapshot"] for record in history], query
            )
        # A cursor at beta's only record yields the definite empty next page,
        # never one of alpha's records.
        status, data = self.call("GET", "/prices/history?limit=10&cursor=1", headers=beta)
        self.assertEqual(200, status)
        self.assertEqual({"history": []}, json.loads(data))

    def test_history_query_does_not_meter(self):
        headers = {"X-Tenant-Id": "hist-meter"}
        self.call("PUT", "/prices", {"execution_started": 200}, key="p", headers=headers)
        self.call("GET", "/prices/history?limit=10", headers=headers)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"usage": []}, json.loads(data))


if __name__ == "__main__":
    unittest.main()
