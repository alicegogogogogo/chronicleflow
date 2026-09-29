import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow, _parse_timestamp

T0 = "2026-01-01T08:00:00.000Z"
T1 = "2026-01-01T09:00:00.000Z"
T2 = "2026-01-01T10:00:00.000Z"

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


def _ts(value):
    return _parse_timestamp(value, "since")


def _interval(seconds=3600, input_data=None, policy="catch_up"):
    return {
        "interval_seconds": seconds,
        "input": {} if input_data is None else input_data,
        "missed_policy": policy,
    }


class ScheduleHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "history.db"))

    def tearDown(self):
        self.directory.cleanup()

    def create_workflow(self, workflow_id="wf", schedule=None, tenant="acme", key=None):
        body = {"id": workflow_id, "nodes": TASK}
        if schedule is not None:
            body["schedule"] = schedule
        self.service.create_workflow(body, key or f"create-{workflow_id}", tenant)

    def _stamp(self, workflow_id, sequence, occurred_at, tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE schedule_history SET occurred_at = ? "
                "WHERE tenant = ? AND workflow_id = ? AND sequence = ?",
                (occurred_at, tenant, workflow_id, sequence),
            )

    def _four_changes(self, workflow_id="wf"):
        """declare(T1 seq1), pause(T0 seq2), resume(T0 seq3), replace(T2 seq4)."""
        self.create_workflow(workflow_id, _interval(), key="c1")
        self.service.pause_schedule(workflow_id, {}, "p1", "acme")
        self.service.resume_schedule(workflow_id, {}, "r1", "acme")
        self.service.update_schedule(workflow_id, _interval(7200), "u1", "acme")
        self._stamp(workflow_id, 1, T1)
        self._stamp(workflow_id, 2, T0)
        self._stamp(workflow_id, 3, T0)
        self._stamp(workflow_id, 4, T2)

    def test_a_schedule_with_no_changes_gets_a_definite_empty_list(self):
        self.create_workflow("plain")
        self.assertEqual({"history": []}, self.service.schedule_history("plain", "acme"))

    def test_history_requires_a_tenant(self):
        self.create_workflow("wf")
        with self.assertRaises(ValidationError):
            self.service.schedule_history("wf", "")

    def test_missing_or_cross_tenant_workflow_is_not_found(self):
        self.create_workflow("wf")
        with self.assertRaises(NotFoundError):
            self.service.schedule_history("ghost", "acme")
        with self.assertRaises(NotFoundError):
            self.service.schedule_history("wf", "beta")

    def test_a_declaration_at_creation_records_its_action_and_snapshot(self):
        plan = _interval(3600, {"n": 1})
        self.create_workflow("wf", plan)
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual(1, record["sequence"])
        self.assertEqual("declare", record["action"])
        self.assertTrue(record["occurred_at"].endswith("Z"))
        self.assertEqual(plan, record["snapshot"]["schedule"])
        self.assertFalse(record["snapshot"]["paused"])
        # Record keys and snapshot keys appear in the documented order.
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(record)
        )
        self.assertEqual(["schedule", "paused"], list(record["snapshot"]))

    def test_replacement_is_declare_and_keeps_the_pause_flag(self):
        self.create_workflow("wf", _interval(3600))
        self.service.pause_schedule("wf", {}, "p1", "acme")
        new_plan = {"cron": "0 9 * * 1", "input": {"mode": "cron"}, "missed_policy": "skip"}
        self.service.update_schedule("wf", new_plan, "u1", "acme")
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare", "pause", "declare"], [r["action"] for r in records])
        # The replacement snapshot carries the new plan verbatim, paused flag kept.
        self.assertEqual(new_plan, records[2]["snapshot"]["schedule"])
        self.assertTrue(records[2]["snapshot"]["paused"])

    def test_pause_and_resume_record_their_actions_and_flags(self):
        self.create_workflow("wf", _interval())
        self.service.pause_schedule("wf", {}, "p1", "acme")
        self.service.resume_schedule("wf", {}, "r1", "acme")
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare", "pause", "resume"], [r["action"] for r in records])
        self.assertFalse(records[0]["snapshot"]["paused"])
        self.assertTrue(records[1]["snapshot"]["paused"])
        self.assertFalse(records[2]["snapshot"]["paused"])
        # The plan is identical on every record.
        plans = {json.dumps(r["snapshot"]["schedule"], sort_keys=True) for r in records}
        self.assertEqual(1, len(plans))

    def test_sequences_are_stable_positive_integers_shared_across_actions(self):
        self._four_changes()
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual({1, 2, 3, 4}, {r["sequence"] for r in records})
        for record in records:
            self.assertIsInstance(record["sequence"], int)
            self.assertGreaterEqual(record["sequence"], 1)

    def test_records_order_by_occurrence_time_with_sequence_breaking_ties(self):
        self._four_changes()
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual([2, 3, 1, 4], [r["sequence"] for r in records])
        self.assertEqual(["pause", "resume", "declare", "declare"], [r["action"] for r in records])
        self.assertEqual([T0, T0, T1, T2], [r["occurred_at"] for r in records])

    def test_action_filter_single_and_multiple(self):
        self._four_changes()
        only_pauses = self.service.schedule_history("wf", "acme", actions=("pause",))["history"]
        self.assertEqual([2], [r["sequence"] for r in only_pauses])
        declared = self.service.schedule_history(
            "wf", "acme", actions=("declare", "resume")
        )["history"]
        self.assertEqual([3, 1, 4], [r["sequence"] for r in declared])

    def test_action_filter_keeps_sequences_unchanged(self):
        self._four_changes()
        declared = self.service.schedule_history("wf", "acme", actions=("declare",))["history"]
        self.assertEqual([1, 4], [r["sequence"] for r in declared])

    def test_window_is_a_closed_interval(self):
        self._four_changes()
        point = self.service.schedule_history("wf", "acme", since=_ts(T0), until=_ts(T0))["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in point])
        lower = self.service.schedule_history("wf", "acme", since=_ts(T1))["history"]
        self.assertEqual([1, 4], [r["sequence"] for r in lower])
        upper = self.service.schedule_history("wf", "acme", until=_ts(T0))["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in upper])

    def test_a_window_with_since_later_than_until_matches_nothing(self):
        self._four_changes()
        self.assertEqual(
            {"history": []},
            self.service.schedule_history("wf", "acme", since=_ts(T2), until=_ts(T0)),
        )

    def test_action_and_time_filters_intersect(self):
        self._four_changes()
        records = self.service.schedule_history(
            "wf", "acme", actions=("resume",), since=_ts(T0), until=_ts(T0)
        )["history"]
        self.assertEqual([3], [r["sequence"] for r in records])

    def test_cursor_keeps_only_strictly_greater_sequences(self):
        self._four_changes()
        # The cursor compares sequences, not time positions: sequence 1 is
        # excluded even though it occurs later than sequences 2 and 3.
        page = self.service.schedule_history("wf", "acme", cursor=1, limit=10)["history"]
        self.assertEqual([2, 3, 4], [r["sequence"] for r in page])
        self.assertEqual(
            [], self.service.schedule_history("wf", "acme", cursor=4, limit=10)["history"]
        )

    def test_consecutive_pages_cover_every_change_once(self):
        self.create_workflow("wf", _interval(900))
        for index in range(4):
            self.service.update_schedule("wf", _interval(1000 + index), f"u{index}", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.schedule_history("wf", "acme", cursor=cursor, limit=2)["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(
            self.service.schedule_history("wf", "acme")["history"], collected
        )
        self.assertEqual([1, 2, 3, 4, 5], [r["sequence"] for r in collected])

    def test_filtered_pages_cover_every_match_once(self):
        self.create_workflow("wf", _interval())
        self.service.pause_schedule("wf", {}, "p1", "acme")
        self.service.resume_schedule("wf", {}, "r1", "acme")
        self.service.pause_schedule("wf", {}, "p2", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.schedule_history(
                "wf", "acme", actions=("pause",), cursor=cursor, limit=1
            )["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual([2, 4], [r["sequence"] for r in collected])

    def test_a_cursor_past_the_end_returns_the_definite_empty_list(self):
        self._four_changes()
        self.assertEqual(
            {"history": []}, self.service.schedule_history("wf", "acme", cursor=99, limit=10)
        )

    def test_replaying_a_change_appends_no_second_record_and_matches_first_response(self):
        self.create_workflow("wf", _interval(3600))
        first = self.service.pause_schedule("wf", {}, "shared", "acme")
        replay = self.service.pause_schedule("wf", {}, "shared", "acme")
        self.assertEqual(first, replay)
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in records])

    def test_cross_operation_key_reuse_writes_no_history(self):
        self.create_workflow("wf", _interval())
        self.service.pause_schedule("wf", {}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.resume_schedule("wf", {}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.update_schedule("wf", _interval(7200), "shared", "acme")
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in records])

    def test_a_rejected_replacement_writes_no_history(self):
        self.create_workflow("wf", _interval())
        for body in (
            {},
            {"interval_seconds": 0, "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "cron": "* * * * *", "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "input": {}, "missed_policy": "wait"},
        ):
            with self.assertRaises(ValidationError):
                self.service.update_schedule("wf", body, f"bad-{body}", "acme")
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare"], [r["action"] for r in records])

    def test_pause_or_resume_without_a_schedule_writes_no_history(self):
        self.create_workflow("plain")
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("plain", {}, "p1", "acme")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("plain", {}, "r1", "acme")
        self.assertEqual({"history": []}, self.service.schedule_history("plain", "acme"))

    def test_history_is_isolated_between_tenants_and_workflows(self):
        self.create_workflow("wf-a", _interval(), tenant="acme", key="ca")
        self.service.pause_schedule("wf-a", {}, "pa", "acme")
        self.create_workflow("wf-b", _interval(), tenant="beta", key="cb")
        acme = self.service.schedule_history("wf-a", "acme")["history"]
        beta = self.service.schedule_history("wf-b", "beta")["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in acme])
        self.assertEqual(["declare"], [r["action"] for r in beta])
        # Sequences restart per tenant and workflow.
        self.assertEqual([1, 2], [r["sequence"] for r in acme])
        self.assertEqual([1], [r["sequence"] for r in beta])
        # No filter or page ever reveals the other tenant's records.
        self.assertEqual(
            {"history": []},
            self.service.schedule_history("wf-a", "acme", actions=("resume",)),
        )

    def test_the_plan_text_is_kept_verbatim_including_float_precision(self):
        plan = {"cron": "*/5  9 * * 1", "input": {"x": 0.30000000000000004, "z": -0.0}, "missed_policy": "skip"}
        self.create_workflow("wf", plan)
        snapshot = self.service.schedule_history("wf", "acme")["history"][0]["snapshot"]["schedule"]
        self.assertEqual("*/5  9 * * 1", snapshot["cron"])
        self.assertEqual(0.30000000000000004, snapshot["input"]["x"])
        self.assertEqual(-0.0, snapshot["input"]["z"])
        # Negative zero keeps its sign through the snapshot.
        rendered = self.service.store.connection.execute(
            "SELECT snapshot FROM schedule_history WHERE tenant = 'acme' AND workflow_id = 'wf'"
        ).fetchone()["snapshot"]
        self.assertIn("-0.0", rendered)

    def test_a_schedule_firing_adds_no_history(self):
        self.create_workflow("wf", _interval(1, {}))
        # Let the background pass settle the first period via the status query.
        deadline = time.time() + 3
        while self.service.schedule_status("wf", "acme")["last_execution_id"] is None:
            self.assertLess(time.time(), deadline)
            time.sleep(0.05)
        records = self.service.schedule_history("wf", "acme")["history"]
        self.assertEqual(["declare"], [r["action"] for r in records])

    def test_the_history_query_is_read_only_on_usage_and_schedule(self):
        self.create_workflow("wf", _interval())
        before_status = self.service.schedule_status("wf", "acme")
        before_usage = self.service.usage("acme")
        self.service.schedule_history("wf", "acme")
        self.service.schedule_history("wf", "acme", actions=("pause",), limit=1)
        self.service.schedule_history(
            "wf", "acme", since=_ts(T0), until=_ts(T2), cursor=1, limit=1
        )
        self.assertEqual(before_status, self.service.schedule_status("wf", "acme"))
        self.assertEqual(before_usage, self.service.usage("acme"))
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM schedule_history WHERE tenant = 'acme' AND workflow_id = 'wf'"
        ).fetchone()
        self.assertEqual(1, rows["used"])


class ScheduleHistoryHttpTests(unittest.TestCase):
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

    def create_workflow(self, workflow_id, headers=None):
        body = {"id": workflow_id, "nodes": TASK, "schedule": _interval(3600, {"n": 1})}
        status, data = self.call("POST", "/workflows", body, key=f"create-{workflow_id}", headers=headers)
        self.assertEqual(201, status, data)

    def test_empty_history_is_compact_json_with_a_single_newline(self):
        headers = {"X-Tenant-Id": "sh-empty"}
        body = {"id": "sh-wf-empty", "nodes": TASK}
        status, data = self.call("POST", "/workflows", body, key="create-sh-wf-empty", headers=headers)
        self.assertEqual(201, status, data)
        status, data = self.call("GET", "/workflows/sh-wf-empty/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(b'{"history":[]}\n', data)

    def test_history_round_trips_over_http_with_stable_key_order(self):
        headers = {"X-Tenant-Id": "sh-flow"}
        self.create_workflow("sh-wf", headers)
        status, data = self.call("POST", "/workflows/sh-wf/schedule/pause", {}, key="p1", headers=headers)
        self.assertEqual(200, status, data)
        status, data = self.call("GET", "/workflows/sh-wf/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status, data)
        records = json.loads(data)["history"]
        self.assertEqual(2, len(records))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(records[0])
        )
        self.assertEqual("declare", records[0]["action"])
        self.assertEqual(_interval(3600, {"n": 1}), records[0]["snapshot"]["schedule"])
        self.assertFalse(records[0]["snapshot"]["paused"])
        self.assertEqual("pause", records[1]["action"])
        self.assertTrue(records[1]["snapshot"]["paused"])
        self.assertEqual(["schedule", "paused"], list(records[1]["snapshot"]))
        # The first record renders as one compact line with keys in order.
        rendered = json.dumps(records[0], separators=(",", ":"))
        self.assertIn(rendered, data.decode())

    def test_missing_or_empty_tenant_is_a_validation_error(self):
        status, data = self.call("GET", "/workflows/sh-wf/schedule/history?limit=10")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "GET", "/workflows/sh-wf/schedule/history?limit=10", headers={"X-Tenant-Id": ""}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_missing_or_cross_tenant_workflow_is_not_found(self):
        status, data = self.call(
            "GET", "/workflows/no-such-workflow/schedule/history?limit=10",
            headers={"X-Tenant-Id": "sh-404"},
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        headers = {"X-Tenant-Id": "sh-owner"}
        self.create_workflow("sh-owned", headers)
        status, data = self.call(
            "GET", "/workflows/sh-owned/schedule/history?limit=10",
            headers={"X-Tenant-Id": "sh-other"},
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_limit_is_required_and_positive(self):
        headers = {"X-Tenant-Id": "sh-limit"}
        self.create_workflow("sh-wf-limit", headers)
        for path in (
            "/workflows/sh-wf-limit/schedule/history",
            "/workflows/sh-wf-limit/schedule/history?limit=0",
            "/workflows/sh-wf-limit/schedule/history?limit=-3",
            "/workflows/sh-wf-limit/schedule/history?limit=1.5",
            "/workflows/sh-wf-limit/schedule/history?limit=abc",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_cursor_must_be_a_positive_integer(self):
        headers = {"X-Tenant-Id": "sh-cursor"}
        self.create_workflow("sh-wf-cursor", headers)
        for path in (
            "/workflows/sh-wf-cursor/schedule/history?limit=10&cursor=0",
            "/workflows/sh-wf-cursor/schedule/history?limit=10&cursor=-1",
            "/workflows/sh-wf-cursor/schedule/history?limit=10&cursor=1.5",
            "/workflows/sh-wf-cursor/schedule/history?limit=10&cursor=abc",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_action_filter_validates_entries(self):
        headers = {"X-Tenant-Id": "sh-action"}
        self.create_workflow("sh-wf-action", headers)
        for path in (
            "/workflows/sh-wf-action/schedule/history?limit=10&action=pause,",
            "/workflows/sh-wf-action/schedule/history?limit=10&action=",
            "/workflows/sh-wf-action/schedule/history?limit=10&action=pause,pause",
            "/workflows/sh-wf-action/schedule/history?limit=10&action=delete",
            "/workflows/sh-wf-action/schedule/history?limit=10&action=PAUSE",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_action_filter_returns_only_named_actions(self):
        headers = {"X-Tenant-Id": "sh-filter"}
        self.create_workflow("sh-wf-filter", headers)
        self.call("POST", "/workflows/sh-wf-filter/schedule/pause", {}, key="p1", headers=headers)
        self.call("POST", "/workflows/sh-wf-filter/schedule/resume", {}, key="r1", headers=headers)
        status, data = self.call(
            "GET", "/workflows/sh-wf-filter/schedule/history?limit=10&action=pause", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(["pause"], [r["action"] for r in json.loads(data)["history"]])
        status, data = self.call(
            "GET", "/workflows/sh-wf-filter/schedule/history?limit=10&action=declare,pause,resume",
            headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(
            ["declare", "pause", "resume"],
            [r["action"] for r in json.loads(data)["history"]],
        )

    def test_bad_timestamps_are_rejected(self):
        headers = {"X-Tenant-Id": "sh-time"}
        self.create_workflow("sh-wf-time", headers)
        for path in (
            "/workflows/sh-wf-time/schedule/history?limit=10&since=2026-01-01T08:00:00",
            "/workflows/sh-wf-time/schedule/history?limit=10&until=not-a-time",
            "/workflows/sh-wf-time/schedule/history?limit=10&since=2026-01-01T08:00:00+00:00",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_closed_window_filters_over_http(self):
        headers = {"X-Tenant-Id": "sh-window"}
        self.create_workflow("sh-wf-window", headers)
        boundary = json.loads(
            self.call("GET", "/workflows/sh-wf-window/schedule/history?limit=10", headers=headers)[1]
        )["history"][0]["occurred_at"]
        status, data = self.call(
            "GET",
            f"/workflows/sh-wf-window/schedule/history?limit=10&since={boundary}&until={boundary}",
            headers=headers,
        )
        self.assertEqual(200, status, data)
        self.assertEqual(1, len(json.loads(data)["history"]))

    def test_repeated_and_unknown_parameters_are_rejected(self):
        headers = {"X-Tenant-Id": "sh-params"}
        self.create_workflow("sh-wf-params", headers)
        for path in (
            "/workflows/sh-wf-params/schedule/history?limit=10&limit=20",
            "/workflows/sh-wf-params/schedule/history?limit=10&action=pause&action=resume",
            "/workflows/sh-wf-params/schedule/history?limit=10&bogus=1",
            "/workflows/sh-wf-params/schedule/history?limit=10&type=declare",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_pagination_is_consecutive_with_cursor(self):
        headers = {"X-Tenant-Id": "sh-page"}
        self.create_workflow("sh-wf-page", headers)
        for index in range(3):
            status, data = self.call(
                "PUT",
                "/workflows/sh-wf-page/schedule",
                _interval(1000 + index),
                key=f"u{index}",
                headers=headers,
            )
            self.assertEqual(200, status, data)
        seen = []
        cursor = None
        for _ in range(10):
            suffix = f"&cursor={cursor}" if cursor is not None else ""
            status, data = self.call(
                "GET", f"/workflows/sh-wf-page/schedule/history?limit=2{suffix}", headers=headers
            )
            self.assertEqual(200, status, data)
            page = json.loads(data)["history"]
            if not page:
                break
            seen.extend(r["sequence"] for r in page)
            cursor = page[-1]["sequence"]
        self.assertEqual([1, 2, 3, 4], seen)

    def test_replay_and_conflict_add_no_history_over_http(self):
        headers = {"X-Tenant-Id": "sh-idem"}
        self.create_workflow("sh-wf-idem", headers)
        self.call("POST", "/workflows/sh-wf-idem/schedule/pause", {}, key="shared", headers=headers)
        status, data = self.call(
            "POST", "/workflows/sh-wf-idem/schedule/pause", {}, key="shared", headers=headers
        )
        self.assertEqual(200, status)
        first_pause = json.loads(data)
        status, data = self.call(
            "POST", "/workflows/sh-wf-idem/schedule/resume", {}, key="shared", headers=headers
        )
        self.assertEqual(409, status)
        status, data = self.call(
            "GET", "/workflows/sh-wf-idem/schedule/history?limit=10", headers=headers
        )
        self.assertEqual(200, status)
        records = json.loads(data)["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in records])
        self.assertTrue(records[1]["snapshot"]["paused"])
        self.assertTrue(first_pause["paused"])

    def test_history_query_does_not_meter_or_change_the_schedule(self):
        headers = {"X-Tenant-Id": "sh-ro"}
        self.create_workflow("sh-wf-ro", headers)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        usage_before = data
        status, before = self.call("GET", "/workflows/sh-wf-ro/schedule", headers=headers)
        self.assertEqual(200, status)
        self.call("GET", "/workflows/sh-wf-ro/schedule/history?limit=10", headers=headers)
        self.call("GET", "/workflows/sh-wf-ro/schedule/history?limit=10&action=resume", headers=headers)
        status, data = self.call("GET", "/workflows/sh-wf-ro/schedule", headers=headers)
        self.assertEqual(before, data)
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(usage_before, data)


if __name__ == "__main__":
    unittest.main()
