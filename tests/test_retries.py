import datetime as dt
import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow

UTC = dt.timezone.utc


class MutableClock:
    def __init__(self, moment):
        self.moment = moment

    def __call__(self):
        return self.moment

    def advance(self, seconds):
        self.moment += dt.timedelta(seconds=seconds)


class RetryTestBase(unittest.TestCase):
    workflow = {
        "id": "orders",
        "nodes": [
            {"id": "reserve", "kind": "task", "depends_on": [], "retries": 2},
            {"id": "charge", "kind": "task", "depends_on": ["reserve"], "retries": 1},
        ],
    }

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, execution_id="run-1", extra=None):
        body = {"id": execution_id, "workflow_id": "orders", "input": {}}
        if extra:
            body.update(extra)
        return self.service.create_execution(body, f"e-{execution_id}")

    def fail(self, execution_id, reason, key):
        return self.service.advance(execution_id, {"failure": {"reason": reason}}, key)

    def succeed(self, execution_id, output, key):
        return self.service.advance(execution_id, {"output": output}, key)


class RetryTests(RetryTestBase):
    def test_failure_then_retry_returns_to_pending_and_appends_events(self):
        created = self.start()
        self.assertEqual({"attempt": 1, "failures": 0, "status": "pending"}, created["nodes"]["reserve"])
        state = self.fail("run-1", "boom", "a1")
        self.assertEqual("running", state["status"])
        self.assertEqual({"attempt": 2, "failures": 1, "status": "pending"}, state["nodes"]["reserve"])
        events = [(e["type"], e["payload"]) for e in self.service.events("run-1")]
        self.assertEqual(
            [
                ("execution_started", events[0][1]),
                ("node_failed", {"node_id": "reserve", "attempt": 1, "reason": "boom"}),
                ("node_retried", {"node_id": "reserve", "attempt": 1, "next_attempt": 2}),
            ],
            events,
        )

    def test_succeeds_after_retries_and_completes(self):
        self.start()
        self.fail("run-1", "e1", "a1")
        state = self.fail("run-1", "e2", "a2")
        self.assertEqual({"attempt": 3, "failures": 2, "status": "pending"}, state["nodes"]["reserve"])
        state = self.succeed("run-1", {"reservation": 9}, "a3")
        self.assertEqual({"attempt": 3, "failures": 2, "status": "completed"}, state["nodes"]["reserve"])
        self.assertEqual(["reserve"], state["completed_nodes"])
        completed_event = next(e for e in self.service.events("run-1") if e["type"] == "node_completed")
        self.assertEqual(3, completed_event["payload"]["attempt"])
        # charge succeeds first try, no attempt field needed in its tracking
        state = self.succeed("run-1", {"charged": True}, "a4")
        self.assertEqual("completed", state["status"])
        self.assertEqual("completed", state["termination_reason"])
        self.assertEqual({"attempt": 1, "failures": 0, "status": "completed"}, state["nodes"]["charge"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_exhausting_retries_fails_node_and_terminates_execution(self):
        self.start()
        self.fail("run-1", "e1", "a1")
        self.fail("run-1", "e2", "a2")
        state = self.fail("run-1", "e3", "a3")
        self.assertEqual("failed", state["status"])
        self.assertEqual("failed", state["termination_reason"])
        self.assertEqual({"attempt": 3, "failures": 3, "status": "failed"}, state["nodes"]["reserve"])
        self.assertNotIn("charge", state["completed_nodes"])
        terminal = [e for e in self.service.events("run-1") if e["type"] == "execution_terminated"]
        self.assertEqual([{"reason": "failed", "node_id": "reserve"}], [e["payload"] for e in terminal])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_advance_after_termination_does_not_absorb_output(self):
        self.start()
        for key in ("a1", "a2", "a3"):
            state = self.fail("run-1", "x", key)
        event_count = len(self.service.events("run-1"))
        again = self.succeed("run-1", {"ignored": True}, "a4")
        self.assertEqual(state, again)
        self.assertNotIn("reserve", again["outputs"])
        # no extra events were appended
        self.assertEqual(event_count, len(self.service.events("run-1")))

    def test_default_retries_zero_fails_on_first_failure(self):
        self.service.create_workflow(
            {"id": "once", "nodes": [{"id": "only", "kind": "task", "depends_on": []}]},
            "w-once",
        )
        self.service.create_execution({"id": "run-once", "workflow_id": "once", "input": {}}, "e-once")
        state = self.fail("run-once", "nope", "a-once")
        self.assertEqual("failed", state["status"])
        self.assertTrue(self.service.replay("run-once")["consistent"])

    def test_failure_reason_is_required_and_string(self):
        self.start()
        for body in (
            {"failure": {}},
            {"failure": {"reason": ""}},
            {"failure": {"reason": 7}},
            {"failure": {"reason": "x", "extra": 1}},
            {"output": "nope"},
            {"foo": 1},
            {},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.advance("run-1", body, f"bad-{id(body)}")

    def test_invalid_retries_rejected(self):
        for bad in (-1, 11, 1.5, "2", True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow(
                        {"id": f"bad-{type(bad).__name__}", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": bad}]},
                        f"wk-{type(bad).__name__}",
                    )

    def test_retries_round_trips_only_when_nonzero(self):
        document = dict(self.workflow, id="orders-copy")
        self.assertEqual(2, self.service.create_workflow(document, "w-round")["nodes"][0]["retries"])
        plain = self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "w-plain",
        )
        self.assertNotIn("retries", plain["nodes"][0])

    def test_restart_continues_pending_retry(self):
        self.start()
        self.fail("run-1", "e1", "a1")
        resumed = ChronicleFlow(self.database)
        state = resumed.advance("run-1", {"output": {"ok": True}}, "a2")
        self.assertEqual({"attempt": 2, "failures": 1, "status": "completed"}, state["nodes"]["reserve"])
        self.assertTrue(resumed.replay("run-1")["consistent"])

    def test_idempotent_failed_advance_returns_original(self):
        self.start()
        first = self.fail("run-1", "first", "same")
        repeated = self.fail("run-1", "second", "same")
        self.assertEqual(first, repeated)
        reasons = [e["payload"].get("reason") for e in self.service.events("run-1") if e["type"] == "node_failed"]
        self.assertEqual(["first"], reasons)

    def test_run_if_skipped_retry_task_is_tracked_and_replays(self):
        self.service.create_workflow(
            {
                "id": "guarded",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "v", "equals": True},
                    {
                        "id": "g",
                        "kind": "task",
                        "depends_on": ["c"],
                        "retries": 3,
                        "run_if": {"condition_id": "c", "expected": True},
                    },
                ],
            },
            "w-guarded",
        )
        self.service.create_execution({"id": "run-g", "workflow_id": "guarded", "input": {"v": False}}, "e-g")
        state = self.service.advance("run-g", {"output": {"unused": True}}, "a-g")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["g"], state["skipped_nodes"])
        self.assertEqual("skipped", state["nodes"]["g"]["status"])
        self.assertTrue(self.service.replay("run-g")["consistent"])


class TimeoutTests(RetryTestBase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.clock = MutableClock(dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC))
        self.service = ChronicleFlow(self.database, clock=self.clock)
        self.service.create_workflow(self.workflow, "w1")

    def test_timeout_terminates_on_advance_without_consuming_output(self):
        state = self.start(extra={"timeout_seconds": 10})
        self.assertEqual(10, state["timeout_seconds"])
        self.assertIsNone(state["termination_reason"])
        self.clock.advance(10)
        state = self.succeed("run-1", {"late": True}, "a1")
        self.assertEqual("timed_out", state["status"])
        self.assertEqual("timed_out", state["termination_reason"])
        self.assertEqual({}, state["outputs"])
        self.assertTrue(self.service.replay("run-1")["consistent"])
        event_types = [e["type"] for e in self.service.events("run-1")]
        self.assertEqual(["execution_started", "execution_terminated"], event_types)

    def test_timeout_terminates_on_cancel(self):
        self.start(extra={"timeout_seconds": 5})
        self.clock.advance(6)
        state = self.service.cancel("run-1", {}, "c1")
        self.assertEqual("timed_out", state["status"])
        self.assertEqual("timed_out", state["termination_reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_not_timed_out_before_deadline(self):
        self.start(extra={"timeout_seconds": 10})
        self.clock.advance(9)
        state = self.succeed("run-1", {"on": "time"}, "a1")
        self.assertEqual("running", state["status"])

    def test_invalid_timeout_rejected(self):
        for bad in (0, -1, "10", True, float("nan"), float("inf")):
            with self.subTest(bad=bad):
                with self.assertRaises(ValidationError):
                    self.start(execution_id=f"to-{type(bad).__name__}-{id(bad)}", extra={"timeout_seconds": bad})

    def test_events_still_queryable_after_timeout(self):
        self.start(extra={"timeout_seconds": 1})
        self.clock.advance(2)
        self.succeed("run-1", {"x": 1}, "a1")
        self.assertEqual(2, len(self.service.events("run-1")))

    def test_timeout_materialized_on_read_without_advance(self):
        self.start(extra={"timeout_seconds": 10})
        self.clock.advance(11)
        state = self.service.inspect_execution("run-1")
        self.assertEqual("timed_out", state["status"])
        # the terminal event was appended and replay stays consistent
        self.assertEqual(
            ["execution_started", "execution_terminated"],
            [e["type"] for e in self.service.events("run-1")],
        )
        self.assertTrue(self.service.replay("run-1")["consistent"])


class CancelTests(RetryTestBase):
    def test_cancel_running_execution(self):
        self.start()
        state = self.service.cancel("run-1", {}, "c1")
        self.assertEqual("cancelled", state["status"])
        self.assertEqual("cancelled", state["termination_reason"])
        events = [e["payload"] for e in self.service.events("run-1") if e["type"] == "execution_terminated"]
        self.assertEqual([{"reason": "cancelled"}], events)
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_cancel_without_body_is_allowed(self):
        self.start()
        state = self.service.cancel("run-1", None, "c1")
        self.assertEqual("cancelled", state["status"])

    def test_cancel_completed_returns_state_unchanged(self):
        self.start()
        self.service.cancel("run-1", {}, "c1")
        state = self.service.get_execution("run-1")
        again = self.service.cancel("run-1", {}, "c2")
        self.assertEqual(state, again)

    def test_cancel_failed_returns_state_unchanged(self):
        self.start()
        self.fail("run-1", "x", "a1")
        self.fail("run-1", "x", "a2")
        failed = self.fail("run-1", "x", "a3")
        again = self.service.cancel("run-1", {}, "c1")
        self.assertEqual(failed, again)
        self.assertEqual("failed", again["status"])

    def test_cancel_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.cancel("ghost", {}, "c1")

    def test_cancel_rejects_nonempty_body(self):
        self.start()
        with self.assertRaises(ValidationError):
            self.service.cancel("run-1", {"unexpected": True}, "c1")

    def test_cancel_key_reuse_across_operation_conflicts(self):
        self.start()
        self.succeed("run-1", {"a": 1}, "shared")
        with self.assertRaises(ConflictError):
            self.service.cancel("run-1", {}, "shared")


class LoopBodyRetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {
                "id": "loopy",
                "nodes": [
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                    {"id": "attempt", "kind": "task", "depends_on": ["check"], "retries": 1},
                    {
                        "id": "loop",
                        "kind": "loop",
                        "depends_on": [],
                        "entry": "attempt",
                        "condition": "check",
                        "max_iterations": 2,
                    },
                ],
            },
            "w1",
        )

    def tearDown(self):
        self.directory.cleanup()

    def start(self):
        self.service.create_execution({"id": "run-1", "workflow_id": "loopy", "input": {"again": True}}, "e1")

    def fail(self, key):
        return self.service.advance("run-1", {"failure": {"reason": "x"}}, key)

    def succeed(self, key, value):
        return self.service.advance("run-1", {"output": {"v": value}}, key)

    def test_body_retry_is_scoped_per_iteration(self):
        self.start()
        self.fail("a1")
        state = self.succeed("a2", 1)
        first = state["loops"]["loop"]["iterations"][0]["nodes"]["attempt"]
        self.assertEqual({"attempt": 2, "failures": 1, "status": "completed"}, first)
        # second iteration resets the counter
        self.fail("a3")
        state = self.succeed("a4", 2)
        second = state["loops"]["loop"]["iterations"][1]["nodes"]["attempt"]
        self.assertEqual({"attempt": 2, "failures": 1, "status": "completed"}, second)
        self.assertEqual("completed", state["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_body_exhaustion_terminates_execution(self):
        self.start()
        self.fail("a1")
        state = self.fail("a2")
        self.assertEqual("failed", state["status"])
        self.assertEqual("failed", state["termination_reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


if __name__ == "__main__":
    unittest.main()
