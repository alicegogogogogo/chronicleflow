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


def _ts(value):
    return _parse_timestamp(value, "since")


TASK = [{"id": "a", "kind": "task", "depends_on": []}]


class QuotaHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "history.db")
        self.service = ChronicleFlow(self.database)

    def tearDown(self):
        self.directory.cleanup()

    def _stamp(self, sequence, occurred_at, tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE quota_history SET occurred_at = ? WHERE tenant = ? AND sequence = ?",
                (occurred_at, tenant, sequence),
            )

    def _three_changes(self):
        """Three changes stamped out of order: seq1 T1 declare, seq2 T0 delete, seq3 T0 declare."""
        self.service.declare_quota({"workflows": 10, "executions": 100}, "q1", "acme")
        self.service.delete_quota({}, "d1", "acme")
        self.service.declare_quota({"workflows": 20, "executions": 200}, "q2", "acme")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)

    def test_a_tenant_with_no_changes_gets_a_definite_empty_list(self):
        self.assertEqual({"history": []}, self.service.quota_history("acme"))

    def test_history_requires_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.quota_history("")

    def test_a_declaration_records_its_action_and_the_effective_snapshot(self):
        result = self.service.declare_quota(
            {"workflows": 10, "executions": 100}, "q1", "acme"
        )
        self.assertEqual({"quota": {"workflows": 10, "executions": 100}}, result)
        history = self.service.quota_history("acme")["history"]
        self.assertEqual(1, len(history))
        record = history[0]
        self.assertEqual(1, record["sequence"])
        self.assertEqual("declare", record["action"])
        self.assertTrue(record["occurred_at"].endswith("Z"))
        self.assertEqual({"workflows": 10, "executions": 100}, record["snapshot"])
        # Keys appear in exactly sequence/action/occurred_at/snapshot order.
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(record)
        )

    def test_a_declaration_snapshot_limits_come_before_growth(self):
        self.service.declare_quota(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {
                    "executions": {"step": 50, "cap": 500},
                    "workflows": {"step": 5, "cap": 50},
                },
            },
            "q1",
            "acme",
        )
        snapshot = self.service.quota_history("acme")["history"][0]["snapshot"]
        self.assertEqual(
            ["workflows", "executions", "growth"], list(snapshot)
        )
        self.assertEqual(
            ["workflows", "executions"], list(snapshot["growth"])
        )
        for entry in snapshot["growth"].values():
            self.assertEqual(["step", "cap"], list(entry))
        self.assertEqual(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {
                    "workflows": {"step": 5, "cap": 50},
                    "executions": {"step": 50, "cap": 500},
                },
            },
            snapshot,
        )

    def test_a_snapshot_contains_only_the_policy_this_declaration_named(self):
        self.service.declare_quota(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {
                    "workflows": {"step": 5, "cap": 50},
                    "executions": {"step": 50, "cap": 500},
                },
            },
            "q1",
            "acme",
        )
        # A redeclaration names only the executions policy; the new snapshot
        # carries no workflows policy even though one was in force before.
        self.service.declare_quota(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {"executions": {"step": 25, "cap": 400}},
            },
            "q2",
            "acme",
        )
        # A declaration with no growth at all drops the policy from the snapshot.
        self.service.declare_quota(
            {"workflows": 5, "executions": 50}, "q3", "acme"
        )
        snapshots = [r["snapshot"] for r in self.service.quota_history("acme")["history"]]
        self.assertEqual(
            [
                {
                    "workflows": 10,
                    "executions": 100,
                    "growth": {
                        "workflows": {"step": 5, "cap": 50},
                        "executions": {"step": 50, "cap": 500},
                    },
                },
                {
                    "workflows": 10,
                    "executions": 100,
                    "growth": {"executions": {"step": 25, "cap": 400}},
                },
                {"workflows": 5, "executions": 50},
            ],
            snapshots,
        )

    def test_a_delete_records_a_null_snapshot(self):
        self.service.declare_quota({"workflows": 10, "executions": 100}, "q1", "acme")
        self.service.delete_quota({}, "d1", "acme")
        records = self.service.quota_history("acme")["history"]
        self.assertEqual(["declare", "delete"], [r["action"] for r in records])
        self.assertIsNone(records[1]["snapshot"])
        self.assertEqual({"workflows": 10, "executions": 100}, records[0]["snapshot"])

    def test_a_delete_without_a_declaration_still_records_a_change(self):
        self.service.delete_quota({}, "d1", "acme")
        records = self.service.quota_history("acme")["history"]
        self.assertEqual(1, len(records))
        self.assertEqual("delete", records[0]["action"])
        self.assertIsNone(records[0]["snapshot"])

    def test_records_order_by_occurrence_time_with_sequence_breaking_ties(self):
        self._three_changes()
        records = self.service.quota_history("acme")["history"]
        self.assertEqual([2, 3, 1], [r["sequence"] for r in records])
        self.assertEqual(["delete", "declare", "declare"], [r["action"] for r in records])
        self.assertEqual([T0, T0, T1], [r["occurred_at"] for r in records])

    def test_sequences_are_stable_positive_integers_shared_across_actions(self):
        self._three_changes()
        sequences = [r["sequence"] for r in self.service.quota_history("acme")["history"]]
        self.assertEqual({1, 2, 3}, set(sequences))
        for record in self.service.quota_history("acme")["history"]:
            self.assertIsInstance(record["sequence"], int)
            self.assertGreaterEqual(record["sequence"], 1)

    def test_action_filter_single_and_multiple_intersect_with_ordering(self):
        self._three_changes()
        only_deletes = self.service.quota_history("acme", actions=("delete",))["history"]
        self.assertEqual([2], [r["sequence"] for r in only_deletes])
        both = self.service.quota_history("acme", actions=("declare", "delete"))["history"]
        self.assertEqual([2, 3, 1], [r["sequence"] for r in both])

    def test_action_filter_keeps_sequences_unchanged(self):
        self._three_changes()
        declared = self.service.quota_history("acme", actions=("declare",))["history"]
        self.assertEqual([3, 1], [r["sequence"] for r in declared])

    def test_window_is_a_closed_interval(self):
        self._three_changes()
        point = self.service.quota_history("acme", since=_ts(T0), until=_ts(T0))["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in point])
        lower = self.service.quota_history("acme", since=_ts(T1))["history"]
        self.assertEqual([1], [r["sequence"] for r in lower])
        upper = self.service.quota_history("acme", until=_ts(T0))["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in upper])

    def test_a_window_with_since_later_than_until_matches_nothing(self):
        self._three_changes()
        self.assertEqual(
            {"history": []},
            self.service.quota_history("acme", since=_ts(T2), until=_ts(T0)),
        )

    def test_action_and_time_filters_intersect(self):
        self._three_changes()
        records = self.service.quota_history(
            "acme", actions=("declare",), since=_ts(T0), until=_ts(T0)
        )["history"]
        self.assertEqual([3], [r["sequence"] for r in records])

    def test_cursor_keeps_only_strictly_greater_sequences(self):
        self._three_changes()
        # The cursor compares sequences, not time positions: sequence 1 is
        # excluded even though it occurs later than sequence 2.
        page = self.service.quota_history("acme", cursor=2, limit=10)["history"]
        self.assertEqual([3], [r["sequence"] for r in page])
        # Cursor 3 likewise excludes the later-in-time sequence 1.
        self.assertEqual(
            [], self.service.quota_history("acme", cursor=3, limit=10)["history"]
        )

    def test_consecutive_pages_cover_every_change_once(self):
        for index in range(4):
            self.service.declare_quota(
                {"workflows": 10 + index, "executions": 100}, f"q{index}", "acme"
            )
        collected = []
        cursor = None
        while True:
            page = self.service.quota_history("acme", cursor=cursor, limit=2)["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(
            self.service.quota_history("acme")["history"], collected
        )
        self.assertEqual([1, 2, 3, 4], [r["sequence"] for r in collected])

    def test_filtered_pages_cover_every_match_once(self):
        self.service.declare_quota({"workflows": 1, "executions": 1}, "q1", "acme")
        self.service.delete_quota({}, "d1", "acme")
        self.service.declare_quota({"workflows": 2, "executions": 2}, "q2", "acme")
        self.service.delete_quota({}, "d2", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.quota_history(
                "acme", actions=("declare",), cursor=cursor, limit=1
            )["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual([1, 3], [r["sequence"] for r in collected])

    def test_a_cursor_past_the_end_returns_the_definite_empty_list(self):
        self._three_changes()
        self.assertEqual(
            {"history": []}, self.service.quota_history("acme", cursor=99, limit=10)
        )

    def test_replaying_a_declaration_appends_no_second_record(self):
        first = self.service.declare_quota(
            {"workflows": 10, "executions": 100}, "q", "acme"
        )
        replay = self.service.declare_quota(
            {"workflows": 99, "executions": 99}, "q", "acme"
        )
        self.assertEqual(first, replay)
        self.assertEqual(1, len(self.service.quota_history("acme")["history"]))
        self.assertEqual(
            {"workflows": 10, "executions": 100},
            self.service.quota_history("acme")["history"][0]["snapshot"],
        )

    def test_replaying_a_delete_appends_no_second_record(self):
        self.service.declare_quota({"workflows": 10, "executions": 100}, "q1", "acme")
        self.service.delete_quota({}, "d", "acme")
        self.service.delete_quota({}, "d", "acme")
        self.assertEqual(2, len(self.service.quota_history("acme")["history"]))

    def test_cross_operation_key_reuse_writes_no_history(self):
        self.service.declare_prices({"execution_started": 200}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.declare_quota(
                {"workflows": 10, "executions": 10}, "shared", "acme"
            )
        with self.assertRaises(ConflictError):
            self.service.delete_quota({}, "shared", "acme")
        self.assertEqual({"history": []}, self.service.quota_history("acme"))
        self.assertEqual({"quota": None}, self.service.get_quota("acme"))

    def test_a_rejected_declaration_writes_no_history(self):
        bodies = (
            {},
            {"workflows": 10},
            {"workflows": 0, "executions": 10},
            {"workflows": True, "executions": 10},
            {"workflows": 10, "executions": 10, "growth": {"workflows": {"step": 1, "cap": 5}}},
            ["not", "an", "object"],
        )
        for body in bodies:
            with self.assertRaises(ValidationError):
                self.service.declare_quota(body, f"bad-{body!r}", "acme")
        with self.assertRaises(ValidationError):
            self.service.delete_quota({"unexpected": 1}, "bad-delete", "acme")
        self.assertEqual({"history": []}, self.service.quota_history("acme"))
        self.assertEqual({"quota": None}, self.service.get_quota("acme"))

    def test_history_is_isolated_between_tenants(self):
        self.service.declare_quota({"workflows": 10, "executions": 10}, "q-a", "acme")
        self.service.delete_quota({}, "d-b", "beta")
        acme = self.service.quota_history("acme")["history"]
        beta = self.service.quota_history("beta")["history"]
        self.assertEqual(1, len(acme))
        self.assertEqual("declare", acme[0]["action"])
        self.assertEqual(1, len(beta))
        self.assertEqual("delete", beta[0]["action"])
        self.assertEqual(1, acme[0]["sequence"])
        self.assertEqual(1, beta[0]["sequence"])
        # No filter or page ever reveals the other tenant's records.
        self.assertEqual(
            {"history": []}, self.service.quota_history("acme", actions=("delete",))
        )
        self.assertEqual(
            {"history": []}, self.service.quota_history("acme", cursor=1, limit=10)
        )

    def test_quota_history_is_independent_of_price_history(self):
        self.service.declare_quota({"workflows": 10, "executions": 10}, "q", "acme")
        self.service.declare_prices({"execution_started": 200}, "p", "beta")
        self.assertEqual(1, len(self.service.quota_history("acme")["history"]))
        self.assertEqual({"history": []}, self.service.quota_history("beta"))

    def test_a_growth_expansion_writes_no_history(self):
        self.service.declare_quota(
            {
                "workflows": 1,
                "executions": 10,
                "growth": {"workflows": {"step": 1, "cap": 5}},
            },
            "q",
            "acme",
        )
        self.service.create_workflow({"id": "wf-1", "nodes": TASK}, "w1", "acme")
        # The second write is admitted by raising the limit a whole step; that
        # expansion is not a declaration and leaves no change record.
        self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "w2", "acme")
        records = self.service.quota_history("acme")["history"]
        self.assertEqual(1, len(records))
        self.assertEqual("declare", records[0]["action"])
        self.assertEqual(
            {"workflows": 1, "executions": 10, "growth": {"workflows": {"step": 1, "cap": 5}}},
            records[0]["snapshot"],
        )

    def test_the_history_query_is_read_only_on_quota_and_usage(self):
        self.service.declare_quota(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {"workflows": {"step": 5, "cap": 50}},
            },
            "q",
            "acme",
        )
        before_quota = self.service.get_quota("acme")
        before_usage = self.service.usage("acme")
        self.service.quota_history("acme")
        self.service.quota_history("acme", actions=("delete",), limit=1)
        self.service.quota_history("acme", since=_ts(T0), until=_ts(T2), cursor=1, limit=1)
        self.assertEqual(before_quota, self.service.get_quota("acme"))
        self.assertEqual(before_usage, self.service.usage("acme"))
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM quota_history WHERE tenant = 'acme'"
        ).fetchone()
        self.assertEqual(1, rows["used"])
        usage_rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = 'acme'"
        ).fetchone()
        self.assertEqual(0, usage_rows["used"])

    def test_records_survive_reopening_with_unchanged_sequence_time_and_snapshot(self):
        self.service.declare_quota(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {"executions": {"step": 50, "cap": 500}},
            },
            "q",
            "acme",
        )
        self.service.delete_quota({}, "d", "acme")
        written = self.service.quota_history("acme")["history"]
        reopened = ChronicleFlow(self.database).quota_history("acme")["history"]
        self.assertEqual(written, reopened)
        self.assertEqual(
            [
                {
                    "sequence": 1,
                    "action": "declare",
                    "snapshot": {
                        "workflows": 10,
                        "executions": 100,
                        "growth": {"executions": {"step": 50, "cap": 500}},
                    },
                },
                {"sequence": 2, "action": "delete", "snapshot": None},
            ],
            [
                {"sequence": r["sequence"], "action": r["action"], "snapshot": r["snapshot"]}
                for r in reopened
            ],
        )


class QuotaHistoryHttpTests(unittest.TestCase):
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

    def test_empty_history_is_compact_json_with_a_single_newline(self):
        headers = {"X-Tenant-Id": "qh-empty"}
        status, data = self.call("GET", "/quotas/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(b'{"history":[]}\n', data)

    def test_history_round_trip_over_http_with_stable_key_order(self):
        headers = {"X-Tenant-Id": "qh-flow"}
        status, data = self.call(
            "PUT",
            "/quotas",
            {
                "workflows": 10,
                "executions": 100,
                "growth": {
                    "executions": {"step": 50, "cap": 500},
                    "workflows": {"step": 5, "cap": 50},
                },
            },
            key="q1",
            headers=headers,
        )
        self.assertEqual(200, status, data)
        status, data = self.call("DELETE", "/quotas", {}, key="d1", headers=headers)
        self.assertEqual(200, status, data)
        status, data = self.call("GET", "/quotas/history?limit=10", headers=headers)
        self.assertEqual(200, status, data)
        records = json.loads(data)["history"]
        self.assertEqual(2, len(records))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(records[0])
        )
        self.assertEqual("declare", records[0]["action"])
        self.assertEqual(
            {
                "workflows": 10,
                "executions": 100,
                "growth": {
                    "workflows": {"step": 5, "cap": 50},
                    "executions": {"step": 50, "cap": 500},
                },
            },
            records[0]["snapshot"],
        )
        self.assertEqual(
            ["workflows", "executions", "growth"], list(records[0]["snapshot"])
        )
        self.assertEqual("delete", records[1]["action"])
        self.assertIsNone(records[1]["snapshot"])
        # The first record renders as one compact line with keys in order.
        first = json.loads(data)["history"][0]
        rendered = json.dumps(first, separators=(",", ":"))
        self.assertIn(rendered, data.decode())

    def test_missing_or_empty_tenant_is_a_validation_error(self):
        status, data = self.call("GET", "/quotas/history?limit=10")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "GET", "/quotas/history?limit=10", headers={"X-Tenant-Id": ""}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_limit_is_required_and_positive(self):
        headers = {"X-Tenant-Id": "qh-limit"}
        for path in ("/quotas/history", "/quotas/history?limit=0",
                     "/quotas/history?limit=-3", "/quotas/history?limit=1.5",
                     "/quotas/history?limit=abc"):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual(
                "validation_error", json.loads(data)["error"]["code"], path
            )

    def test_cursor_must_be_a_positive_integer(self):
        headers = {"X-Tenant-Id": "qh-cursor"}
        for path in ("/quotas/history?limit=10&cursor=0",
                     "/quotas/history?limit=10&cursor=-1",
                     "/quotas/history?limit=10&cursor=1.5",
                     "/quotas/history?limit=10&cursor=abc"):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual(
                "validation_error", json.loads(data)["error"]["code"], path
            )

    def test_action_filter_validates_entries(self):
        headers = {"X-Tenant-Id": "qh-action"}
        for path in (
            "/quotas/history?limit=10&action=declare,",
            "/quotas/history?limit=10&action=",
            "/quotas/history?limit=10&action=declare,declare",
            "/quotas/history?limit=10&action=removed",
            "/quotas/history?limit=10&action=DECLARE",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual(
                "validation_error", json.loads(data)["error"]["code"], path
            )

    def test_action_filter_returns_only_named_actions(self):
        headers = {"X-Tenant-Id": "qh-filter"}
        self.call(
            "PUT", "/quotas", {"workflows": 10, "executions": 100},
            key="q1", headers=headers,
        )
        self.call("DELETE", "/quotas", {}, key="d1", headers=headers)
        status, data = self.call(
            "GET", "/quotas/history?limit=10&action=delete", headers=headers
        )
        self.assertEqual(200, status)
        records = json.loads(data)["history"]
        self.assertEqual(["delete"], [r["action"] for r in records])
        status, data = self.call(
            "GET", "/quotas/history?limit=10&action=declare,delete", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(["declare", "delete"], [r["action"] for r in json.loads(data)["history"]])

    def test_bad_timestamps_are_rejected(self):
        headers = {"X-Tenant-Id": "qh-time"}
        for path in (
            "/quotas/history?limit=10&since=2026-01-01T08:00:00",
            "/quotas/history?limit=10&until=not-a-time",
            "/quotas/history?limit=10&since=2026-01-01T08:00:00+00:00",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual(
                "validation_error", json.loads(data)["error"]["code"], path
            )

    def test_closed_window_filters_over_http(self):
        headers = {"X-Tenant-Id": "qh-window"}
        self.call(
            "PUT", "/quotas", {"workflows": 10, "executions": 100},
            key="q1", headers=headers,
        )
        boundary = json.loads(
            self.call("GET", "/quotas/history?limit=10", headers=headers)[1]
        )["history"][0]["occurred_at"]
        status, data = self.call(
            "GET", f"/quotas/history?limit=10&since={boundary}&until={boundary}",
            headers=headers,
        )
        self.assertEqual(200, status, data)
        self.assertEqual(1, len(json.loads(data)["history"]))

    def test_repeated_and_unknown_parameters_are_rejected(self):
        headers = {"X-Tenant-Id": "qh-params"}
        for path in (
            "/quotas/history?limit=10&limit=20",
            "/quotas/history?limit=10&action=declare&action=delete",
            "/quotas/history?limit=10&bogus=1",
            "/quotas/history?limit=10&type=execution_started",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual(
                "validation_error", json.loads(data)["error"]["code"], path
            )

    def test_pagination_is_consecutive_with_cursor(self):
        headers = {"X-Tenant-Id": "qh-page"}
        for index in range(4):
            self.call(
                "PUT", "/quotas", {"workflows": 10 + index, "executions": 100},
                key=f"q{index}", headers=headers,
            )
        seen = []
        cursor = None
        for _ in range(10):
            suffix = f"&cursor={cursor}" if cursor is not None else ""
            status, data = self.call(
                "GET", f"/quotas/history?limit=2{suffix}", headers=headers
            )
            self.assertEqual(200, status, data)
            page = json.loads(data)["history"]
            if not page:
                break
            seen.extend(r["sequence"] for r in page)
            cursor = page[-1]["sequence"]
        self.assertEqual([1, 2, 3, 4], seen)

    def test_replay_and_conflict_add_no_history_over_http(self):
        headers = {"X-Tenant-Id": "qh-idem"}
        self.call(
            "PUT", "/quotas", {"workflows": 10, "executions": 100},
            key="q", headers=headers,
        )
        self.call(
            "POST", "/quotas", {"workflows": 99, "executions": 99},
            key="q", headers=headers,
        )
        status, data = self.call(
            "PUT", "/prices", {"execution_started": 200},
            key="q", headers=headers,
        )
        self.assertEqual(409, status)
        status, data = self.call("GET", "/quotas/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        records = json.loads(data)["history"]
        self.assertEqual(1, len(records))
        self.assertEqual("declare", records[0]["action"])
        self.assertEqual({"workflows": 10, "executions": 100}, records[0]["snapshot"])

    def test_history_is_tenant_isolated_over_http(self):
        alpha = {"X-Tenant-Id": "qh-alpha"}
        beta = {"X-Tenant-Id": "qh-beta"}
        self.call(
            "PUT", "/quotas", {"workflows": 10, "executions": 100},
            key="q", headers=alpha,
        )
        self.call("DELETE", "/quotas", {}, key="d", headers=beta)
        for headers, action in ((alpha, "declare"), (beta, "delete")):
            status, data = self.call("GET", "/quotas/history?limit=10", headers=headers)
            self.assertEqual(200, status)
            records = json.loads(data)["history"]
            self.assertEqual(1, len(records))
            self.assertEqual(action, records[0]["action"])
        # Alpha's records never appear under beta's filters or pages.
        status, data = self.call(
            "GET", "/quotas/history?limit=10&action=declare,delete", headers=beta
        )
        self.assertEqual(["delete"], [r["action"] for r in json.loads(data)["history"]])

    def test_history_query_does_not_change_quota_or_usage(self):
        headers = {"X-Tenant-Id": "qh-ro"}
        self.call(
            "PUT", "/quotas", {"workflows": 10, "executions": 100},
            key="q", headers=headers,
        )
        self.call("GET", "/quotas/history?limit=10", headers=headers)
        self.call("GET", "/quotas/history?limit=10&action=delete", headers=headers)
        status, data = self.call("GET", "/quotas", headers=headers)
        self.assertEqual(b'{"quota":{"workflows":10,"executions":100}}\n', data)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(b'{"usage":[]}\n', data)


if __name__ == "__main__":
    unittest.main()
