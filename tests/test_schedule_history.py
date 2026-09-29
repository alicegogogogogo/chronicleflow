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


def _plan(interval=60, **overrides):
    plan = {"interval_seconds": interval, "input": {}, "missed_policy": "skip"}
    plan.update(overrides)
    return plan


class ScheduleHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "history.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _create_workflow(self, workflow_id="wf", tenant="acme", schedule=None):
        raw = {"id": workflow_id, "nodes": TASK}
        if schedule is not None:
            raw["schedule"] = schedule
        self.service.create_workflow(raw, f"create-{workflow_id}-{tenant}", tenant)

    def _declare(self, plan, key, workflow_id="wf", tenant="acme"):
        self.service.update_schedule(workflow_id, plan, key, tenant)

    def _stamp(self, sequence, occurred_at, workflow_id="wf", tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE schedule_history SET occurred_at = ? "
                "WHERE tenant = ? AND workflow_id = ? AND sequence = ?",
                (occurred_at, tenant, workflow_id, sequence),
            )

    def _three_changes(self):
        """Three changes stamped out of order: seq1 T1 declare, seq2 T0 pause, seq3 T0 resume."""
        self._create_workflow()
        self._declare(_plan(), "d1")
        self.service.pause_schedule("wf", {}, "p1", "acme")
        self.service.resume_schedule("wf", {}, "r1", "acme")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)

    def test_a_workflow_with_no_changes_gets_a_definite_empty_list(self):
        self._create_workflow()
        self.assertEqual({"history": []}, self.service.schedule_history("wf", "acme", limit=10))

    def test_history_requires_a_tenant(self):
        self._create_workflow()
        with self.assertRaises(ValidationError):
            self.service.schedule_history("wf", "", limit=10)

    def test_a_declaration_at_creation_records_its_action_and_snapshot(self):
        self._create_workflow(schedule=_plan(3600, input={"mode": "nightly"}, missed_policy="catch_up"))
        history = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(1, len(history))
        record = history[0]
        self.assertEqual(1, record["sequence"])
        self.assertEqual("declare", record["action"])
        self.assertTrue(record["occurred_at"].endswith("Z"))
        self.assertEqual(
            {"schedule": {"interval_seconds": 3600, "input": {"mode": "nightly"},
                          "missed_policy": "catch_up"}, "paused": False},
            record["snapshot"],
        )
        # Keys appear in exactly sequence/action/occurred_at/snapshot order.
        self.assertEqual(
            ["sequence", "action", "occurred_at", "snapshot"], list(record)
        )
        self.assertEqual(["schedule", "paused"], list(record["snapshot"]))

    def test_a_replacement_records_declare_and_keeps_the_pause_flag(self):
        self._create_workflow(schedule=_plan(60))
        self.service.pause_schedule("wf", {}, "p", "acme")
        # A replacement made while paused keeps the schedule paused.
        self._declare({"cron": "0 9 * * 1", "input": {}, "missed_policy": "catch_up"}, "d2")
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(["declare", "pause", "declare"], [r["action"] for r in records])
        replaced = records[2]
        self.assertEqual(
            {"schedule": {"cron": "0 9 * * 1", "input": {}, "missed_policy": "catch_up"},
             "paused": True},
            replaced["snapshot"],
        )
        self.assertTrue(self.service.schedule_status("wf", "acme")["paused"])

    def test_pause_and_resume_record_their_actions_and_pause_snapshots(self):
        self._create_workflow(schedule=_plan(60))
        self.service.pause_schedule("wf", {}, "p", "acme")
        self.service.resume_schedule("wf", {}, "r", "acme")
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(["declare", "pause", "resume"], [r["action"] for r in records])
        self.assertFalse(records[0]["snapshot"]["paused"])
        self.assertTrue(records[1]["snapshot"]["paused"])
        self.assertFalse(records[2]["snapshot"]["paused"])
        # The plan is carried verbatim by the pause and resume snapshots.
        for record in records:
            self.assertEqual(_plan(60), record["snapshot"]["schedule"])

    def test_the_plan_text_and_input_are_preserved_verbatim(self):
        plan = {
            "cron": "*/5 9-17 1,15 * 1-5",
            "input": {"ratio": 0.30000000000000004, "neg": -0.0, "label": "NiGhTlY"},
            "missed_policy": "catch_up",
        }
        self._create_workflow(schedule=plan)
        snapshot = self.service.schedule_history("wf", "acme", limit=10)["history"][0]["snapshot"]
        self.assertEqual("*/5 9-17 1,15 * 1-5", snapshot["schedule"]["cron"])
        self.assertEqual(0.30000000000000004, snapshot["schedule"]["input"]["ratio"])
        self.assertEqual(-0.0, snapshot["schedule"]["input"]["neg"])
        self.assertTrue(json.dumps(snapshot).count("-0.0") == 1)

    def test_pause_resume_of_a_missing_or_scheduleless_workflow_write_no_history(self):
        self._create_workflow("has")
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("nope", {}, "p", "acme")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("nope", {}, "r", "acme")
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("has", {}, "p2", "acme")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("has", {}, "r2", "acme")
        self.assertEqual(
            {"history": []}, self.service.schedule_history("has", "acme", limit=10)
        )
        with self.assertRaises(NotFoundError):
            self.service.schedule_history("nope", "acme", limit=10)

    def test_records_order_by_occurrence_time_with_sequence_breaking_ties(self):
        self._three_changes()
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual([2, 3, 1], [r["sequence"] for r in records])
        self.assertEqual(["pause", "resume", "declare"], [r["action"] for r in records])
        self.assertEqual([T0, T0, T1], [r["occurred_at"] for r in records])

    def test_sequences_are_stable_positive_integers_shared_across_actions(self):
        self._three_changes()
        sequences = [r["sequence"] for r in self.service.schedule_history("wf", "acme", limit=10)["history"]]
        self.assertEqual({1, 2, 3}, set(sequences))
        for record in self.service.schedule_history("wf", "acme", limit=10)["history"]:
            self.assertIsInstance(record["sequence"], int)
            self.assertGreaterEqual(record["sequence"], 1)

    def test_action_filter_single_and_multiple(self):
        self._three_changes()
        only_pause = self.service.schedule_history("wf", "acme", actions=("pause",), limit=10)["history"]
        self.assertEqual([2], [r["sequence"] for r in only_pause])
        declared = self.service.schedule_history(
            "wf", "acme", actions=("declare", "resume"), limit=10
        )["history"]
        self.assertEqual([3, 1], [r["sequence"] for r in declared])

    def test_action_filter_keeps_sequences_unchanged(self):
        self._three_changes()
        declared = self.service.schedule_history("wf", "acme", actions=("declare",), limit=10)["history"]
        self.assertEqual([1], [r["sequence"] for r in declared])

    def test_window_is_a_closed_interval(self):
        self._three_changes()
        point = self.service.schedule_history("wf", "acme", since=_ts(T0), until=_ts(T0), limit=10)["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in point])
        lower = self.service.schedule_history("wf", "acme", since=_ts(T1), limit=10)["history"]
        self.assertEqual([1], [r["sequence"] for r in lower])
        upper = self.service.schedule_history("wf", "acme", until=_ts(T0), limit=10)["history"]
        self.assertEqual([2, 3], [r["sequence"] for r in upper])

    def test_a_window_with_since_later_than_until_matches_nothing(self):
        self._three_changes()
        self.assertEqual(
            {"history": []},
            self.service.schedule_history("wf", "acme", since=_ts(T2), until=_ts(T0), limit=10),
        )

    def test_action_and_time_filters_intersect(self):
        self._three_changes()
        records = self.service.schedule_history(
            "wf", "acme", actions=("pause",), since=_ts(T0), until=_ts(T0), limit=10
        )["history"]
        self.assertEqual([2], [r["sequence"] for r in records])

    def test_cursor_keeps_only_strictly_greater_sequences(self):
        self._three_changes()
        page = self.service.schedule_history("wf", "acme", cursor=2, limit=10)["history"]
        self.assertEqual([3], [r["sequence"] for r in page])
        self.assertEqual(
            [], self.service.schedule_history("wf", "acme", cursor=3, limit=10)["history"]
        )

    def test_consecutive_pages_cover_every_change_once(self):
        self._create_workflow()
        self._declare(_plan(60), "d1")
        self.service.pause_schedule("wf", {}, "p1", "acme")
        self.service.resume_schedule("wf", {}, "r1", "acme")
        self._declare(_plan(120), "d2")
        collected = []
        cursor = None
        while True:
            page = self.service.schedule_history("wf", "acme", cursor=cursor, limit=2)["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(
            self.service.schedule_history("wf", "acme", limit=100)["history"], collected
        )
        self.assertEqual([1, 2, 3, 4], [r["sequence"] for r in collected])

    def test_filtered_pages_cover_every_match_once(self):
        self._create_workflow()
        self._declare(_plan(60), "d1")
        self.service.pause_schedule("wf", {}, "p1", "acme")
        self._declare(_plan(120), "d2")
        self.service.resume_schedule("wf", {}, "r1", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.schedule_history(
                "wf", "acme", actions=("declare",), cursor=cursor, limit=1
            )["history"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual([1, 3], [r["sequence"] for r in collected])

    def test_a_cursor_past_the_end_returns_the_definite_empty_list(self):
        self._three_changes()
        self.assertEqual(
            {"history": []}, self.service.schedule_history("wf", "acme", cursor=99, limit=10)
        )

    def test_replaying_a_change_appends_no_second_record_and_returns_the_first_result(self):
        self._create_workflow(schedule=_plan(60))
        first = self.service.pause_schedule("wf", {}, "p", "acme")
        replay = self.service.pause_schedule("wf", {}, "p", "acme")
        self.assertEqual(first, replay)
        first_resume = self.service.resume_schedule("wf", {}, "r", "acme")
        self.assertEqual(first_resume, self.service.resume_schedule("wf", {}, "r", "acme"))
        # A replayed replacement returns the first status and adds no record:
        # the second plan (999s) never takes effect.
        first_declare = self.service.update_schedule("wf", _plan(120), "d", "acme")
        replay_declare = self.service.update_schedule("wf", _plan(999), "d", "acme")
        self.assertEqual(first_declare, replay_declare)
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(["declare", "pause", "resume", "declare"], [r["action"] for r in records])
        self.assertEqual(120, records[3]["snapshot"]["schedule"]["interval_seconds"])

    def test_cross_operation_key_reuse_writes_no_history(self):
        self._create_workflow(schedule=_plan(60))
        self.service.pause_schedule("wf", {}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self.service.resume_schedule("wf", {}, "shared", "acme")
        with self.assertRaises(ConflictError):
            self._declare(_plan(120), "shared")
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in records])

    def test_a_rejected_declaration_writes_no_history(self):
        self._create_workflow()
        for index, bad in enumerate((
            {"interval_seconds": 0, "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "cron": "* * * * *", "input": {}, "missed_policy": "skip"},
            {"interval_seconds": 1, "input": {}, "missed_policy": "wait"},
            {"interval_seconds": 1, "input": {}, "missed_policy": "skip", "extra": 1},
        )):
            with self.assertRaises(ValidationError):
                self._declare(bad, f"bad-{index}")
        self.assertEqual({"history": []}, self.service.schedule_history("wf", "acme", limit=10))
        self.assertEqual({"schedule": None}, self.service.schedule_status("wf", "acme"))

    def test_a_due_period_trigger_writes_no_history(self):
        self._create_workflow(schedule=_plan(1))
        # Let the first period come due; the status query settles it exactly
        # like the background tick would.
        time.sleep(1.3)
        status = self.service.schedule_status("wf", "acme")
        self.assertIsNotNone(status["last_execution_id"])
        records = self.service.schedule_history("wf", "acme", limit=10)["history"]
        self.assertEqual(["declare"], [r["action"] for r in records])

    def test_history_is_independent_between_workflows_and_tenants(self):
        self._create_workflow("wf", "acme", schedule=_plan(60))
        self.service.pause_schedule("wf", {}, "p-a", "acme")
        # A different tenant may hold a workflow with the same id.
        self._create_workflow("wf", "beta", schedule=_plan(120))
        acme = self.service.schedule_history("wf", "acme", limit=10)["history"]
        beta = self.service.schedule_history("wf", "beta", limit=10)["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in acme])
        self.assertEqual(["declare"], [r["action"] for r in beta])
        self.assertEqual([1, 2], [r["sequence"] for r in acme])
        self.assertEqual([1], [r["sequence"] for r in beta])
        # No filter or page ever reveals another tenant's records: beta owns
        # its own wf and sees no pause records, while a tenant that never held
        # the workflow reads it as missing.
        self.assertEqual(
            {"history": []},
            self.service.schedule_history("wf", "acme", actions=("declare",), cursor=2, limit=10),
        )
        self.assertEqual(
            {"history": []},
            self.service.schedule_history("wf", "beta", actions=("pause",), limit=10),
        )
        with self.assertRaises(NotFoundError):
            self.service.schedule_history("wf", "gamma", limit=10)

    def test_the_history_query_is_read_only(self):
        self._create_workflow(schedule=_plan(3600))
        before_status = self.service.schedule_status("wf", "acme")
        before_usage = self.service.usage("acme")
        self.service.schedule_history("wf", "acme", limit=10)
        self.service.schedule_history("wf", "acme", actions=("pause",), limit=1)
        self.service.schedule_history("wf", "acme", since=_ts(T0), until=_ts(T2), limit=1)
        self.assertEqual(before_status, self.service.schedule_status("wf", "acme"))
        # The read settles no period, appends no record, and meters nothing.
        self.assertEqual(1, len(self.service.schedule_history("wf", "acme", limit=10)["history"]))
        self.assertEqual(before_usage, self.service.usage("acme"))


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

    def _workflow(self, workflow_id, headers, schedule=None, key=None):
        body = {"id": workflow_id, "nodes": TASK}
        if schedule is not None:
            body["schedule"] = schedule
        status, data = self.call("POST", "/workflows", body, key=key or f"create-{workflow_id}", headers=headers)
        self.assertEqual(201, status, data)

    def test_empty_history_is_compact_json_with_a_single_newline(self):
        headers = {"X-Tenant-Id": "sh-empty"}
        self._workflow("wf", headers)
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(b'{"history":[]}\n', data)

    def test_history_round_trips_over_http_with_stable_key_order(self):
        headers = {"X-Tenant-Id": "sh-flow"}
        self._workflow(
            "wf", headers,
            {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
        )
        self.call("POST", "/workflows/wf/schedule/pause", {}, key="p1", headers=headers)
        self.call("POST", "/workflows/wf/schedule/resume", {}, key="r1", headers=headers)
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status, data)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        records = json.loads(data)["history"]
        self.assertEqual(["declare", "pause", "resume"], [r["action"] for r in records])
        first = records[0]
        self.assertEqual(["sequence", "action", "occurred_at", "snapshot"], list(first))
        self.assertEqual(["schedule", "paused"], list(first["snapshot"]))
        self.assertEqual(
            {"interval_seconds": 3600, "input": {"mode": "nightly"}, "missed_policy": "catch_up"},
            first["snapshot"]["schedule"],
        )
        rendered = json.dumps(first, separators=(",", ":"))
        self.assertIn(rendered, data.decode())

    def test_float_precision_and_negative_zero_round_trip(self):
        headers = {"X-Tenant-Id": "sh-float"}
        self._workflow(
            "wf", headers,
            {"interval_seconds": 60, "input": {"ratio": 0.30000000000000004, "neg": -0.0},
             "missed_policy": "skip"},
        )
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertIn(b"0.30000000000000004", data)
        self.assertIn(b"-0.0", data)

    def test_missing_or_empty_tenant_is_a_validation_error(self):
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "GET", "/workflows/wf/schedule/history?limit=10", headers={"X-Tenant-Id": ""}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_missing_or_cross_tenant_workflow_is_not_found(self):
        headers = {"X-Tenant-Id": "sh-404"}
        status, data = self.call("GET", "/workflows/nope/schedule/history?limit=10", headers=headers)
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        other = {"X-Tenant-Id": "sh-other"}
        self._workflow("wf", headers, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=other)
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_limit_is_required_and_positive(self):
        headers = {"X-Tenant-Id": "sh-limit"}
        self._workflow("wf", headers)
        for path in (
            "/workflows/wf/schedule/history",
            "/workflows/wf/schedule/history?limit=0",
            "/workflows/wf/schedule/history?limit=-3",
            "/workflows/wf/schedule/history?limit=1.5",
            "/workflows/wf/schedule/history?limit=abc",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_cursor_must_be_a_positive_integer(self):
        headers = {"X-Tenant-Id": "sh-cursor"}
        self._workflow("wf", headers)
        for path in (
            "/workflows/wf/schedule/history?limit=10&cursor=0",
            "/workflows/wf/schedule/history?limit=10&cursor=-1",
            "/workflows/wf/schedule/history?limit=10&cursor=1.5",
            "/workflows/wf/schedule/history?limit=10&cursor=abc",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_action_filter_validates_entries(self):
        headers = {"X-Tenant-Id": "sh-action"}
        self._workflow("wf", headers)
        for path in (
            "/workflows/wf/schedule/history?limit=10&action=declare,",
            "/workflows/wf/schedule/history?limit=10&action=",
            "/workflows/wf/schedule/history?limit=10&action=pause,pause",
            "/workflows/wf/schedule/history?limit=10&action=delete",
            "/workflows/wf/schedule/history?limit=10&action=DECLARE",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_action_filter_returns_only_named_actions(self):
        headers = {"X-Tenant-Id": "sh-filter"}
        self._workflow("wf", headers, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        self.call("POST", "/workflows/wf/schedule/pause", {}, key="p1", headers=headers)
        self.call("POST", "/workflows/wf/schedule/resume", {}, key="r1", headers=headers)
        status, data = self.call(
            "GET", "/workflows/wf/schedule/history?limit=10&action=pause,resume", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(["pause", "resume"], [r["action"] for r in json.loads(data)["history"]])
        status, data = self.call(
            "GET", "/workflows/wf/schedule/history?limit=10&action=declare,pause,resume", headers=headers
        )
        self.assertEqual(["declare", "pause", "resume"], [r["action"] for r in json.loads(data)["history"]])

    def test_bad_timestamps_are_rejected(self):
        headers = {"X-Tenant-Id": "sh-time"}
        self._workflow("wf", headers)
        for path in (
            "/workflows/wf/schedule/history?limit=10&since=2026-01-01T08:00:00",
            "/workflows/wf/schedule/history?limit=10&until=not-a-time",
            "/workflows/wf/schedule/history?limit=10&since=2026-01-01T08:00:00+00:00",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_closed_window_filters_over_http(self):
        headers = {"X-Tenant-Id": "sh-window"}
        self._workflow("wf", headers, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        boundary = json.loads(
            self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)[1]
        )["history"][0]["occurred_at"]
        status, data = self.call(
            "GET", f"/workflows/wf/schedule/history?limit=10&since={boundary}&until={boundary}",
            headers=headers,
        )
        self.assertEqual(200, status, data)
        self.assertEqual(1, len(json.loads(data)["history"]))

    def test_repeated_and_unknown_parameters_are_rejected(self):
        headers = {"X-Tenant-Id": "sh-params"}
        self._workflow("wf", headers)
        for path in (
            "/workflows/wf/schedule/history?limit=10&limit=20",
            "/workflows/wf/schedule/history?limit=10&action=pause&action=resume",
            "/workflows/wf/schedule/history?limit=10&bogus=1",
            "/workflows/wf/schedule/history?limit=10&type=declare",
        ):
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_pagination_is_consecutive_with_cursor(self):
        headers = {"X-Tenant-Id": "sh-page"}
        self._workflow("wf", headers, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        self.call("POST", "/workflows/wf/schedule/pause", {}, key="p1", headers=headers)
        self.call("POST", "/workflows/wf/schedule/resume", {}, key="r1", headers=headers)
        self.call(
            "PUT", "/workflows/wf/schedule",
            {"interval_seconds": 120, "input": {}, "missed_policy": "skip"}, key="d1", headers=headers,
        )
        seen = []
        cursor = None
        for _ in range(10):
            suffix = f"&cursor={cursor}" if cursor is not None else ""
            status, data = self.call(
                "GET", f"/workflows/wf/schedule/history?limit=2{suffix}", headers=headers
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
        self._workflow("wf", headers, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        self.call("POST", "/workflows/wf/schedule/pause", {}, key="shared", headers=headers)
        status, _ = self.call("POST", "/workflows/wf/schedule/pause", {}, key="shared", headers=headers)
        self.assertEqual(200, status)
        status, _ = self.call("POST", "/workflows/wf/schedule/resume", {}, key="shared", headers=headers)
        self.assertEqual(409, status)
        status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)
        self.assertEqual(200, status)
        records = json.loads(data)["history"]
        self.assertEqual(["declare", "pause"], [r["action"] for r in records])

    def test_history_is_tenant_isolated_over_http(self):
        alpha = {"X-Tenant-Id": "sh-alpha"}
        beta = {"X-Tenant-Id": "sh-beta"}
        self._workflow("wf", alpha, {"interval_seconds": 60, "input": {}, "missed_policy": "skip"})
        self._workflow("wf", beta, {"interval_seconds": 120, "input": {}, "missed_policy": "skip"})
        for headers, action in ((alpha, "declare"), (beta, "declare")):
            status, data = self.call("GET", "/workflows/wf/schedule/history?limit=10", headers=headers)
            self.assertEqual(200, status)
            records = json.loads(data)["history"]
            self.assertEqual(1, len(records))
            self.assertEqual(action, records[0]["action"])
        status, data = self.call(
            "GET", "/workflows/wf/schedule/history?limit=10&action=declare,pause,resume", headers=beta
        )
        self.assertEqual(["declare"], [r["action"] for r in json.loads(data)["history"]])


if __name__ == "__main__":
    unittest.main()
