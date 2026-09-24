import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def order_workflow(workflow_id="orders"):
    return {
        "id": workflow_id,
        "nodes": [
            {"id": "reserve", "kind": "task", "depends_on": []},
            {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
        ],
    }


def retry_loop_workflow(workflow_id="loop-retry"):
    return {
        "id": workflow_id,
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
    }


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(order_workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {"order": 7}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_checkpoint_is_written_at_each_node_boundary(self):
        self.assertEqual([], self.service.checkpoints("run-1")["checkpoints"])
        first = self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.assertEqual(["reserve"], first["completed_nodes"])
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(1, len(checkpoints))
        checkpoint = checkpoints[0]
        self.assertEqual(1, checkpoint["sequence"])
        self.assertEqual(2, checkpoint["event_sequence"])
        self.assertEqual(first, checkpoint["state"])
        self.service.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        sequences = [item["sequence"] for item in self.service.checkpoints("run-1")["checkpoints"]]
        # completion is terminal, so the last node boundary did not add a checkpoint
        self.assertEqual([1], sequences)

    def test_checkpoints_change_neither_state_shape_nor_event_stream(self):
        state = self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.assertEqual(
            {
                "id",
                "workflow_id",
                "status",
                "termination_reason",
                "timeout_seconds",
                "deadline_at",
                "input",
                "completed_nodes",
                "skipped_nodes",
                "failed_nodes",
                "condition_results",
                "outputs",
                "attempts",
                "loops",
            },
            set(state),
        )
        self.assertEqual(["execution_started", "node_completed"], [e["type"] for e in self.service.events("run-1")])

    def test_recover_returns_checkpoint_state_and_appends_nothing(self):
        first = self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        events_before = self.service.events("run-1")
        recovered = self.service.recover("run-1", {"from": "latest"}, "r1")
        self.assertEqual(first, recovered)
        self.assertEqual(events_before, self.service.events("run-1"))
        self.assertEqual(
            [item["sequence"] for item in self.service.checkpoints("run-1")["checkpoints"]],
            [1],
        )

    def test_recover_restarts_then_advancing_repeats_no_output(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        resumed = ChronicleFlow(self.database)
        recovered = resumed.recover("run-1", {"from": "latest"}, "r1")
        self.assertEqual(["reserve"], recovered["completed_nodes"])
        self.assertEqual({"reserve": {"reservation": 9}}, recovered["outputs"])
        completed = resumed.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        self.assertEqual("completed", completed["status"])
        self.assertEqual(
            ["reserve", "charge"],
            completed["completed_nodes"],
        )
        self.assertEqual({"reservation": 9}, completed["outputs"]["reserve"])
        event_types = [event["type"] for event in resumed.events("run-1")]
        self.assertEqual(["execution_started", "node_completed", "node_completed", "execution_completed"], event_types)
        self.assertEqual({"consistent": True, "execution": completed}, resumed.replay("run-1"))

    def test_recover_is_idempotent_on_repeated_key(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        first = self.service.recover("run-1", {"from": "latest"}, "same")
        repeated = self.service.recover("run-1", {"from": "latest"}, "same")
        self.assertEqual(first, repeated)
        self.assertEqual(1, len(self.service.checkpoints("run-1")["checkpoints"]))
        self.assertEqual(2, len(self.service.events("run-1")))

    def test_recover_key_cannot_be_reused_for_another_operation(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.recover("run-1", {"from": "latest"}, "shared")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {"charge": "ok"}}, "shared")

    def test_recover_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.recover("ghost", {"from": "latest"}, "r-ghost")

    def test_recover_without_a_checkpoint_conflicts(self):
        with self.assertRaises(ConflictError):
            self.service.recover("run-1", {"from": "latest"}, "r-empty")

    def test_completed_execution_recover_returns_state_unchanged_without_events(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        completed = self.service.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        events_before = self.service.events("run-1")
        recovered = self.service.recover("run-1", {"from": "latest"}, "r-done")
        self.assertEqual(completed, recovered)
        self.assertEqual(events_before, self.service.events("run-1"))

    def test_completed_without_any_checkpoint_still_returns_state(self):
        service = ChronicleFlow(str(Path(self.directory.name) / "auto.db"))
        service.create_workflow(
            {"id": "auto", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "ok", "equals": "yes"}]},
            "w-auto",
        )
        service.create_execution({"id": "run-auto", "workflow_id": "auto", "input": {"ok": "yes"}}, "e-auto")
        completed = service.advance("run-auto", {"output": {"never": "used"}}, "a-auto")
        self.assertEqual("completed", completed["status"])
        self.assertEqual([], service.checkpoints("run-auto")["checkpoints"])
        recovered = service.recover("run-auto", {"from": "latest"}, "r-auto")
        self.assertEqual(completed, recovered)

    def test_terminated_without_any_checkpoint_still_returns_state(self):
        service = ChronicleFlow(str(Path(self.directory.name) / "timed.db"))
        service.create_workflow(
            {"id": "slow", "nodes": [{"id": "work", "kind": "task", "depends_on": []}]},
            "w-slow",
        )
        service.create_execution(
            {"id": "run-timed", "workflow_id": "slow", "input": {}, "timeout_seconds": 0.05},
            "e-timed",
        )
        time.sleep(0.1)
        terminated = service.get_execution("run-timed")
        self.assertEqual("terminated", terminated["status"])
        self.assertEqual([], service.checkpoints("run-timed")["checkpoints"])
        recovered = service.recover("run-timed", {"from": "latest"}, "r-timed")
        self.assertEqual(terminated, recovered)
        self.assertEqual("timeout", recovered["termination_reason"])

    def test_invalid_recover_bodies_are_rejected(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        invalid_bodies = (
            {},
            {"from": "earliest"},
            {"from": "latest", "extra": 1},
            {"from": 1},
            {"checkpoint": "latest"},
        )
        for body in invalid_bodies:
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.recover("run-1", body, "r-bad")

    def test_checkpoints_of_missing_execution_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.checkpoints("ghost")

    def test_corrupt_checkpoint_conflicts(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        with self.service.store.transaction() as connection:
            connection.execute("UPDATE checkpoints SET document = ? WHERE execution_id = ?", ("{not json", "run-1"))
        with self.assertRaises(ConflictError):
            self.service.recover("run-1", {"from": "latest"}, "r-bad")


class CheckpointRetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {"id": "flaky", "nodes": [{"id": "fetch", "kind": "task", "depends_on": [], "retries": 2}]},
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "flaky", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_checkpoint_after_failure_retry_preserves_attempt(self):
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a1")
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(1, len(checkpoints))
        checkpoint = checkpoints[0]
        self.assertEqual(3, checkpoint["event_sequence"])
        self.assertEqual(state, checkpoint["state"])
        resumed = ChronicleFlow(self.database)
        recovered = resumed.recover("run-1", {"from": "latest"}, "r1")
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, recovered["attempts"])
        completed = resumed.advance("run-1", {"output": {"page": 1}}, "a2")
        self.assertEqual(["fetch"], completed["completed_nodes"])
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, completed["attempts"])
        self.assertEqual({"consistent": True, "execution": completed}, resumed.replay("run-1"))

    def test_no_checkpoint_when_retries_are_exhausted(self):
        self.service.advance("run-1", {"failure": {"reason": "boom-1"}}, "a1")
        self.service.advance("run-1", {"failure": {"reason": "boom-2"}}, "a2")
        terminated = self.service.advance("run-1", {"failure": {"reason": "boom-3"}}, "a3")
        self.assertEqual("terminated", terminated["status"])
        # only the two retried boundaries produced checkpoints; terminal failure did not
        self.assertEqual([1, 2], [c["sequence"] for c in self.service.checkpoints("run-1")["checkpoints"]])
        recovered = self.service.recover("run-1", {"from": "latest"}, "r1")
        self.assertEqual(terminated, recovered)
        self.assertEqual("retries_exhausted", recovered["termination_reason"])
        again = self.service.advance("run-1", {"output": {"late": True}}, "a4")
        self.assertEqual(terminated, again)


class CheckpointLoopTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(retry_loop_workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "loop-retry", "input": {"again": True}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_recovery_inside_loop_keeps_iteration_and_continues(self):
        # first advance: condition + first iteration start are automatic, then attempt fails and retries
        state = self.service.advance("run-1", {"failure": {"reason": "flaky"}}, "a1")
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual(1, state["loops"]["loop"]["current_iteration"])
        self.assertEqual({"attempt": {"attempt": 2, "failures": 1}}, iteration["attempts"])
        failed_event = self.service.checkpoints("run-1")["checkpoints"][0]["state"]
        self.assertEqual(1, failed_event["loops"]["loop"]["current_iteration"])
        resumed = ChronicleFlow(self.database)
        recovered = resumed.recover("run-1", {"from": "latest"}, "r1")
        self.assertEqual(1, recovered["loops"]["loop"]["current_iteration"])
        self.assertEqual({"attempt": {"attempt": 2, "failures": 1}}, recovered["loops"]["loop"]["iterations"][0]["attempts"])
        next_state = resumed.advance("run-1", {"output": {"try": 1}}, "a2")
        self.assertEqual(2, next_state["loops"]["loop"]["current_iteration"])
        final = resumed.advance("run-1", {"output": {"try": 2}}, "a3")
        loop = final["loops"]["loop"]
        self.assertEqual("completed", loop["status"])
        self.assertEqual("iteration_limit", loop["end_reason"])
        self.assertEqual(2, len(loop["iterations"]))
        self.assertEqual({"consistent": True, "execution": final}, resumed.replay("run-1"))
        failed = next(event for event in resumed.events("run-1") if event["type"] == "node_failed")
        self.assertEqual(
            {"node_id": "attempt", "attempt": 1, "reason": "flaky", "loop_id": "loop", "iteration": 1},
            failed["payload"],
        )


class CheckpointTimeoutCancelTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {"id": "slow", "nodes": [{"id": "work", "kind": "task", "depends_on": []}, {"id": "more", "kind": "task", "depends_on": ["work"]}]},
            "w1",
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_timeout_between_checkpoints_takes_precedence_over_recovery(self):
        self.service.create_execution(
            {"id": "run-timeout", "workflow_id": "slow", "input": {}, "timeout_seconds": 0.05},
            "e1",
        )
        self.service.advance("run-timeout", {"output": {"done": 1}}, "a1")
        time.sleep(0.1)
        recovered = self.service.recover("run-timeout", {"from": "latest"}, "r1")
        self.assertEqual("terminated", recovered["status"])
        self.assertEqual("timeout", recovered["termination_reason"])
        self.assertEqual(["work"], recovered["completed_nodes"])
        # events and checkpoints remain queryable and replay stays consistent
        self.assertEqual(["execution_started", "node_completed", "execution_terminated"], [e["type"] for e in self.service.events("run-timeout")])
        self.assertEqual(1, len(self.service.checkpoints("run-timeout")["checkpoints"]))
        self.assertEqual({"consistent": True, "execution": recovered}, self.service.replay("run-timeout"))

    def test_cancelled_execution_recover_returns_state_unchanged(self):
        self.service.create_execution({"id": "run-cancel", "workflow_id": "slow", "input": {}}, "e2")
        self.service.advance("run-cancel", {"output": {"done": 1}}, "a1")
        cancelled = self.service.cancel("run-cancel", "c1")
        events_before = self.service.events("run-cancel")
        recovered = self.service.recover("run-cancel", {"from": "latest"}, "r1")
        self.assertEqual(cancelled, recovered)
        self.assertEqual("cancelled", recovered["termination_reason"])
        self.assertEqual(events_before, self.service.events("run-cancel"))
        self.assertEqual({"consistent": True, "execution": recovered}, self.service.replay("run-cancel"))


if __name__ == "__main__":
    unittest.main()
