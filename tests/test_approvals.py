import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.workflow = {
            "id": "orders",
            "nodes": [
                {"id": "reserve", "kind": "task", "depends_on": []},
                {
                    "id": "charge",
                    "kind": "task",
                    "depends_on": ["reserve"],
                    "approval": {"approvers": ["alice", "bob"]},
                },
                {"id": "ship", "kind": "task", "depends_on": ["charge"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def event_types(self, execution_id="run-1"):
        return [event["type"] for event in self.service.events(execution_id)]

    def park(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        return self.service.advance("run-1", {"output": {"must": "not be consumed"}}, "a2")

    def test_advance_parks_without_output_or_completion(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        state = self.service.advance("run-1", {"output": {"must": "not be consumed"}}, "a2")
        self.assertEqual("running", state["status"])
        self.assertEqual(["reserve"], state["completed_nodes"])
        self.assertNotIn("charge", state["outputs"])
        self.assertEqual(
            {"node_id": "charge", "approvers": ["alice", "bob"]},
            state["waiting_approval"],
        )
        self.assertEqual([], state["approvals"])
        self.assertEqual(
            ["execution_started", "node_completed", "approval_requested"],
            self.event_types(),
        )
        requested = self.service.events("run-1")[-1]["payload"]
        self.assertEqual({"node_id": "charge", "approvers": ["alice", "bob"]}, requested)

    def test_advance_while_waiting_does_not_change_state(self):
        parked = self.park()
        again = self.service.advance("run-1", {"output": {"x": 1}}, "a3")
        self.assertEqual(parked, again)
        failure = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a4")
        self.assertEqual(parked, failure)
        self.assertEqual(3, len(self.service.events("run-1")))

    def test_approval_completes_task_and_unlocks_successors(self):
        self.park()
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"charged": True}}, "d1"
        )
        self.assertIsNone(state["waiting_approval"])
        self.assertEqual(["reserve", "charge"], state["completed_nodes"])
        self.assertEqual({"charged": True}, state["outputs"]["charge"])
        record = state["approvals"][0]
        self.assertEqual(
            {"node_id": "charge", "approver": "alice", "decision": "approved", "reason": None},
            record,
        )
        decided = next(event for event in self.service.events("run-1") if event["type"] == "approval_decided")
        self.assertEqual(
            {"node_id": "charge", "approver": "alice", "decision": "approved"},
            decided["payload"],
        )
        self.assertEqual("node_completed", self.service.events("run-1")[-1]["type"])
        final = self.service.advance("run-1", {"output": {"shipped": True}}, "a5")
        self.assertEqual("completed", final["status"])
        self.assertEqual(["reserve", "charge", "ship"], final["completed_nodes"])
        self.assertEqual({"consistent": True, "execution": final}, self.service.replay("run-1"))

    def test_rejection_permanently_fails_and_terminates(self):
        self.park()
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "rejected", "reason": "suspicious order"}, "d1"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["charge"], state["failed_nodes"])
        self.assertIsNone(state["waiting_approval"])
        self.assertNotIn("charge", state["outputs"])
        events = self.service.events("run-1")
        self.assertEqual("approval_decided", events[-2]["type"])
        self.assertEqual(
            {"node_id": "charge", "approver": "bob", "decision": "rejected", "reason": "suspicious order"},
            events[-2]["payload"],
        )
        self.assertEqual("execution_terminated", events[-1]["type"])
        self.assertEqual({"reason": "rejected", "node_id": "charge"}, events[-1]["payload"])
        # Termination is definite: outputs are no longer accepted.
        after = self.service.advance("run-1", {"output": {"late": True}}, "a6")
        self.assertEqual(state, after)
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_decision_by_non_approver_conflicts_and_keeps_waiting(self):
        parked = self.park()
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "carol", "decision": "approved", "output": {}}, "d-bad"
            )
        self.assertEqual(parked, self.service.get_execution("run-1"))
        self.assertEqual(3, len(self.service.events("run-1")))
        # the pending decision can still be made by an allowed approver
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {}}, "d1"
        )
        self.assertEqual("running", state["status"])

    def test_unknown_decision_value_is_validation_error(self):
        self.park()
        for body in (
            {"approver": "alice", "decision": "maybe", "output": {}},
            {"approver": "alice", "decision": True, "output": {}},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.decision("run-1", body, "d-bad")

    def test_invalid_decision_bodies_are_validation_errors(self):
        self.park()
        for body in (
            {},
            {"approver": "alice"},
            {"decision": "approved", "output": {}},
            {"approver": 7, "decision": "approved", "output": {}},
            {"approver": "alice", "decision": "approved"},
            {"approver": "alice", "decision": "rejected"},
            {"approver": "alice", "decision": "approved", "output": "not-an-object"},
            {"approver": "alice", "decision": "rejected", "reason": 5},
            {"approver": "alice", "decision": "approved", "output": {}, "reason": "x"},
            {"approver": "alice", "decision": "rejected", "reason": "x", "output": {}},
            {"approver": "alice", "decision": "approved", "output": {}, "extra": 1},
            ["alice"],
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.decision("run-1", body, "d-bad")

    def test_decision_with_non_finite_output_is_validation_error(self):
        self.park()
        with self.assertRaises(ValidationError):
            self.service.decision(
                "run-1", {"approver": "alice", "decision": "approved", "output": {"v": float("nan")}}, "d-nan"
            )

    def test_repeated_decision_returns_first_result_without_duplicate_events(self):
        self.park()
        first = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1"
        )
        repeated = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d2"
        )
        self.assertEqual(first, repeated)
        types = self.event_types()
        self.assertEqual(
            ["execution_started", "node_completed", "approval_requested", "approval_decided", "node_completed"],
            types,
        )
        self.assertEqual(1, sum(1 for event_type in types if event_type == "approval_decided"))
        # a repeat carrying a different output is not a duplicate: it conflicts
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 2}}, "d3"
            )

    def test_repeated_rejection_returns_first_termination(self):
        self.park()
        first = self.service.decision(
            "run-1", {"approver": "bob", "decision": "rejected", "reason": "no"}, "d1"
        )
        repeated = self.service.decision(
            "run-1", {"approver": "bob", "decision": "rejected", "reason": "no"}, "d2"
        )
        self.assertEqual(first, repeated)
        # a different reason makes it a fresh, conflicting request
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "bob", "decision": "rejected", "reason": "other"}, "d3"
            )
        self.assertEqual(5, len(self.service.events("run-1")))

    def test_decision_without_waiting_point_conflicts(self):
        # run-2's workflow declares no approval points
        self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "work", "kind": "task", "depends_on": []}]},
            "w-plain",
        )
        self.service.create_execution({"id": "run-2", "workflow_id": "plain", "input": {}}, "e2")
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-2", {"approver": "alice", "decision": "approved", "output": {}}, "d-plain"
            )

    def test_decision_for_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.decision(
                "ghost", {"approver": "alice", "decision": "approved", "output": {}}, "d-ghost"
            )

    def test_cancel_takes_effect_while_waiting(self):
        self.park()
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("cancelled", state["termination_reason"])
        self.assertIsNone(state["waiting_approval"])
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "alice", "decision": "approved", "output": {}}, "d-late"
            )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_timeout_takes_effect_while_waiting_and_rejects_output(self):
        self.service.create_execution(
            {"id": "run-timeout", "workflow_id": "orders", "input": {}, "timeout_seconds": 0.05},
            "e-timeout",
        )
        self.service.advance("run-timeout", {"output": {}}, "at1")
        self.service.advance("run-timeout", {"output": {}}, "at2")
        time.sleep(0.1)
        state = self.service.get_execution("run-timeout")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("timeout", state["termination_reason"])
        self.assertIsNone(state["waiting_approval"])
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-timeout", {"approver": "alice", "decision": "approved", "output": {}}, "dt1"
            )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-timeout"))

    def test_waiting_state_and_decisions_survive_restart(self):
        self.park()
        resumed = ChronicleFlow(self.database)
        waiting = resumed.get_execution("run-1")
        self.assertEqual({"node_id": "charge", "approvers": ["alice", "bob"]}, waiting["waiting_approval"])
        state = resumed.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1"
        )
        self.assertEqual(["reserve", "charge"], state["completed_nodes"])
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-1"))

    def test_decision_key_cannot_be_reused_for_another_operation(self):
        self.park()
        self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {}}, "shared"
        )
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "shared")

    def test_multiple_approval_points_accumulate_records(self):
        workflow = {
            "id": "gated",
            "nodes": [
                {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
                {"id": "b", "kind": "task", "depends_on": ["a"], "approval": {"approvers": ["bob"]}},
            ],
        }
        self.service.create_workflow(workflow, "w-gated")
        self.service.create_execution({"id": "run-g", "workflow_id": "gated", "input": {}}, "eg")
        self.service.advance("run-g", {"output": {}}, "ag1")
        self.service.decision("run-g", {"approver": "alice", "decision": "approved", "output": {"n": 1}}, "dg1")
        self.service.advance("run-g", {"output": {}}, "ag2")
        state = self.service.decision("run-g", {"approver": "bob", "decision": "rejected", "reason": "stop"}, "dg2")
        self.assertEqual("terminated", state["status"])
        self.assertEqual(["alice", "bob"], [record["approver"] for record in state["approvals"]])
        self.assertEqual(
            [
                "execution_started",
                "approval_requested",
                "approval_decided",
                "node_completed",
                "approval_requested",
                "approval_decided",
                "execution_terminated",
            ],
            self.event_types("run-g"),
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-g"))


class ApprovalDefinitionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_definition_is_round_tripped(self):
        workflow = {
            "id": "gated",
            "nodes": [
                {
                    "id": "a",
                    "kind": "task",
                    "depends_on": [],
                    "approval": {"approvers": ["alice", "bob"]},
                }
            ],
        }
        stored = self.service.create_workflow(workflow, "w1")
        self.assertEqual(workflow, stored)

    def test_invalid_approvals_are_rejected(self):
        def node(approval):
            return {"id": "a", "kind": "task", "depends_on": [], "approval": approval}

        invalid = [
            node({"approvers": []}),
            node({"approvers": ["alice", "alice"]}),
            node({"approvers": ["alice", 7]}),
            node({"approvers": [1, 2]}),
            node({"approvers": "alice"}),
            node({"approvers": ["alice"], "extra": 1}),
            node({}),
            node(["alice"]),
            # approval only belongs to task nodes
            {"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": 1, "approval": {"approvers": ["a"]}},
        ]
        for index, raw_node in enumerate(invalid):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-{index}", "nodes": [raw_node]}, f"w-bad-{index}")


class ApprovalBaselineShapeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "work", "kind": "task", "depends_on": []}]},
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "plain", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_state_and_events_keep_baseline_shape_without_approvals(self):
        state = self.service.advance("run-1", {"output": {"ok": 1}}, "a1")
        self.assertNotIn("waiting_approval", state)
        self.assertNotIn("approvals", state)
        started = self.service.events("run-1")[0]["payload"]
        self.assertNotIn("waiting_approval", started)
        self.assertNotIn("approvals", started)
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertNotIn("waiting_approval", checkpoints[0]["state"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


class ApprovalCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "gated",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "gated", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_park_and_decision_boundaries_are_checkpointed(self):
        parked = self.service.advance("run-1", {"output": {}}, "a1")
        checkpoint = self.service.checkpoints("run-1")["checkpoints"][-1]
        self.assertEqual(parked, checkpoint["state"])
        self.assertEqual(
            {"node_id": "a", "approvers": ["alice"]},
            checkpoint["state"]["waiting_approval"],
        )
        approved = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1"
        )
        checkpoint = self.service.checkpoints("run-1")["checkpoints"][-1]
        self.assertEqual(approved, checkpoint["state"])
        recovered = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(approved, recovered)
        self.assertEqual({"consistent": True, "execution": approved}, self.service.replay("run-1"))

    def test_decision_keeps_float_precision_and_negative_zero(self):
        self.service.advance("run-1", {"output": {}}, "a1")
        state = self.service.decision(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"precise": 0.30000000000000004, "neg": -0.0}},
            "d1",
        )
        output = state["outputs"]["a"]
        self.assertEqual(0.30000000000000004, output["precise"])
        self.assertEqual(-0.0, output["neg"])
        self.assertTrue(str(output["neg"]).startswith("-"))
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


class ApprovalInLoopTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        workflow = {
            "id": "loop-gated",
            "nodes": [
                {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                {
                    "id": "attempt",
                    "kind": "task",
                    "depends_on": ["check"],
                    "approval": {"approvers": ["alice"]},
                },
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
        self.service.create_workflow(workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "loop-gated", "input": {"again": True}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_inside_loop_iteration(self):
        parked = self.service.advance("run-1", {"output": {}}, "a1")
        waiting = parked["waiting_approval"]
        self.assertEqual({"node_id": "attempt", "approvers": ["alice"], "loop_id": "loop", "iteration": 1}, waiting)
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"try": 1}}, "d1"
        )
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        # the next iteration's task parks at the same approval point on the following advance
        state = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        self.assertEqual(
            {"node_id": "attempt", "approvers": ["alice"], "loop_id": "loop", "iteration": 2},
            state["waiting_approval"],
        )
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "rejected", "reason": "enough"}, "d2"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["attempt"], state["failed_nodes"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


if __name__ == "__main__":
    unittest.main()
