import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def gated_workflow(workflow_id="approvals", approvers=("alice", "bob")):
    return {
        "id": workflow_id,
        "nodes": [
            {"id": "prepare", "kind": "task", "depends_on": []},
            {
                "id": "signoff",
                "kind": "task",
                "depends_on": ["prepare"],
                "approval": {"approvers": list(approvers)},
            },
            {"id": "ship", "kind": "task", "depends_on": ["signoff"]},
        ],
    }


class ApprovalPointDefinitionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_definition_round_trips(self):
        document = gated_workflow()
        stored = self.service.create_workflow(document, "w1")
        self.assertEqual(document, stored)

    def test_invalid_approval_points_are_rejected(self):
        invalid_approvers = (
            [],
            ["alice", "alice"],
            ["alice", 7],
            [1],
            [None],
            [True],
        )
        for index, approvers in enumerate(invalid_approvers):
            with self.subTest(approvers=approvers):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow(
                        {
                            "id": f"bad-{index}",
                            "nodes": [
                                {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": approvers}},
                            ],
                        },
                        f"w-bad-{index}",
                    )

    def test_approval_shape_is_validated(self):
        invalid_nodes = [
            {"id": "a", "kind": "task", "depends_on": [], "approval": {}},
            {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["a"], "extra": 1}},
            {"id": "a", "kind": "task", "depends_on": [], "approval": []},
            {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["a"], "approvers_extra": ["b"]}},
        ]
        for index, node in enumerate(invalid_nodes):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-shape-{index}", "nodes": [node]}, f"w-shape-{index}")

    def test_task_without_approval_keeps_baseline_definition(self):
        stored = self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "w-plain",
        )
        self.assertEqual({"id": "a", "kind": "task", "depends_on": []}, stored["nodes"][0])


class ApprovalExecutionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(gated_workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "approvals", "input": {"order": 7}}, "e1")
        # prepare completes normally
        self.service.advance("run-1", {"output": {"prepared": True}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def park(self):
        return self.service.advance("run-1", {"output": {"not": "consumed"}}, "a2")

    def test_advance_parks_without_output_or_completion(self):
        state = self.park()
        self.assertEqual("running", state["status"])
        self.assertEqual("signoff", state["pending_approval"])
        self.assertEqual(["prepare"], state["completed_nodes"])
        self.assertNotIn("signoff", state["outputs"])
        self.assertEqual([], state["approvals"])
        events = self.service.events("run-1")
        self.assertEqual("approval_requested", events[-1]["type"])
        self.assertEqual(
            {"node_id": "signoff", "approvers": ["alice", "bob"]},
            events[-1]["payload"],
        )
        # parking settles no node boundary, so no checkpoint is written
        self.assertEqual(1, len(self.service.checkpoints("run-1")["checkpoints"]))

    def test_advance_while_waiting_does_nothing(self):
        parked = self.park()
        for body in ({"output": {"late": True}}, {"failure": {"reason": "nope"}}):
            with self.subTest(body=body):
                again = self.service.advance("run-1", body, f"again-{body.get('failure') is not None}")
                self.assertEqual(parked, again)
        events = self.service.events("run-1")
        self.assertEqual(["execution_started", "node_completed", "approval_requested"], [e["type"] for e in events])

    def test_approve_completes_task_and_unlocks_successors(self):
        self.park()
        state = self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}},
            "d1",
        )
        self.assertIsNone(state["pending_approval"])
        self.assertEqual(["prepare", "signoff"], state["completed_nodes"])
        self.assertEqual({"receipt": "r-9"}, state["outputs"]["signoff"])
        record = state["approvals"][0]
        self.assertEqual(
            {"node_id": "signoff", "approver": "alice", "decision": "approved", "reason": None},
            record,
        )
        decided = self.service.events("run-1")[-2]
        self.assertEqual("approval_decided", decided["type"])
        self.assertEqual(record, decided["payload"])
        # the decision settled a boundary, so it checkpointed
        self.assertEqual(2, len(self.service.checkpoints("run-1")["checkpoints"]))
        final = self.service.advance("run-1", {"output": {"shipped": True}}, "a3")
        self.assertEqual("completed", final["status"])
        self.assertEqual({"consistent": True, "execution": final}, self.service.replay("run-1"))

    def test_reject_permanently_fails_the_task_and_terminates(self):
        self.park()
        state = self.service.decide(
            "run-1",
            {"approver": "bob", "decision": "rejected", "reason": "budget exceeded"},
            "d1",
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertIsNone(state["pending_approval"])
        self.assertEqual(["signoff"], state["failed_nodes"])
        self.assertNotIn("signoff", state["outputs"])
        self.assertEqual(
            {"node_id": "signoff", "approver": "bob", "decision": "rejected", "reason": "budget exceeded"},
            state["approvals"][0],
        )
        events = self.service.events("run-1")
        self.assertEqual("approval_decided", events[-2]["type"])
        self.assertEqual(
            {"node_id": "signoff", "approver": "bob", "decision": "rejected", "reason": "budget exceeded"},
            events[-2]["payload"],
        )
        self.assertEqual("execution_terminated", events[-1]["type"])
        self.assertEqual({"reason": "rejected", "node_id": "signoff"}, events[-1]["payload"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))
        # a rejected execution neither advances nor decides
        self.assertEqual(state, self.service.advance("run-1", {"output": {"late": True}}, "a-late"))
        with self.assertRaises(ConflictError):
            self.service.decide(
                "run-1",
                {"approver": "alice", "decision": "approved", "output": {}},
                "d-late",
            )

    def test_unknown_approver_is_conflict_and_changes_nothing(self):
        parked = self.park()
        with self.assertRaises(ConflictError):
            self.service.decide(
                "run-1",
                {"approver": "carol", "decision": "approved", "output": {}},
                "d-bad",
            )
        self.assertEqual(parked, self.service.get_execution("run-1"))
        self.assertEqual(3, len(self.service.events("run-1")))

    def test_invalid_decide_bodies_are_validation_errors(self):
        self.park()
        bodies = (
            {},
            {"approver": "alice"},
            {"approver": "alice", "decision": "approved"},
            {"approver": "alice", "decision": "maybe", "output": {}},
            {"approver": "alice", "decision": "approved", "output": "nope"},
            {"approver": "alice", "decision": "rejected", "reason": 9},
            {"approver": "alice", "decision": "rejected"},
            {"approver": "", "decision": "rejected", "reason": "x"},
            {"approver": 7, "decision": "rejected", "reason": "x"},
            {"approver": "alice", "decision": "approved", "output": {}, "extra": 1},
            {"approver": "alice", "decision": "rejected", "reason": "x", "extra": 1},
            ["alice"],
        )
        for index, body in enumerate(bodies):
            with self.subTest(index=index, body=body):
                with self.assertRaises(ValidationError):
                    self.service.decide("run-1", body, f"d-bad-{index}")
        # no validation failure changed state or appended events
        self.assertEqual(3, len(self.service.events("run-1")))

    def test_decide_without_pending_approval_conflicts(self):
        # signoff is not yet reached: prepare is still ready
        fresh = ChronicleFlow(self.database)
        fresh.create_workflow(gated_workflow("approvals-2"), "w2")
        fresh.create_execution({"id": "run-2", "workflow_id": "approvals-2", "input": {}}, "e2")
        with self.assertRaises(ConflictError):
            fresh.decide("run-2", {"approver": "alice", "decision": "approved", "output": {}}, "d2")

    def test_decide_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.decide(
                "ghost",
                {"approver": "alice", "decision": "approved", "output": {}},
                "d-ghost",
            )

    def test_repeated_decision_returns_first_result_without_replay(self):
        self.park()
        body = {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}}
        first = self.service.decide("run-1", body, "same")
        repeated = self.service.decide(
            "run-1",
            {"approver": "bob", "decision": "rejected", "reason": "changed mind"},
            "same",
        )
        self.assertEqual(first, repeated)
        types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            ["execution_started", "node_completed", "approval_requested", "approval_decided", "node_completed"],
            types,
        )
        self.assertEqual(1, len(first["approvals"]))

    def test_decision_key_cannot_be_reused_for_another_operation(self):
        self.park()
        self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {}},
            "shared-decide",
        )
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "shared-decide")

    def test_cancel_while_waiting_terminates_immediately(self):
        self.park()
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("cancelled", state["termination_reason"])
        self.assertIsNone(state["pending_approval"])
        with self.assertRaises(ConflictError):
            self.service.decide(
                "run-1",
                {"approver": "alice", "decision": "approved", "output": {}},
                "d1",
            )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_timeout_while_waiting_terminates_and_rejects_the_decision(self):
        self.service.create_workflow(gated_workflow("timed"), "w-t")
        self.service.create_execution(
            {"id": "run-t", "workflow_id": "timed", "input": {}, "timeout_seconds": 0.05},
            "e-t",
        )
        self.service.advance("run-t", {"output": {}}, "at-1")
        self.service.advance("run-t", {"output": {}}, "at-2")
        self.assertEqual("signoff", self.service.get_execution("run-t")["pending_approval"])
        time.sleep(0.1)
        with self.assertRaises(ConflictError):
            self.service.decide(
                "run-t",
                {"approver": "alice", "decision": "approved", "output": {}},
                "dt-1",
            )
        state = self.service.get_execution("run-t")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("timeout", state["termination_reason"])
        self.assertIsNone(state["pending_approval"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-t"))

    def test_waiting_approval_survives_restart_and_decision_continues(self):
        self.park()
        resumed = ChronicleFlow(self.database)
        state = resumed.get_execution("run-1")
        self.assertEqual("running", state["status"])
        self.assertEqual("signoff", state["pending_approval"])
        state = resumed.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}},
            "d1",
        )
        self.assertEqual(["prepare", "signoff"], state["completed_nodes"])
        final = resumed.advance("run-1", {"output": {"shipped": True}}, "a3")
        self.assertEqual("completed", final["status"])
        self.assertEqual({"consistent": True, "execution": final}, resumed.replay("run-1"))

    def test_recover_parked_execution_folds_the_waiting_event(self):
        uninterrupted_db = str(Path(self.directory.name) / "plain.db")
        uninterrupted = ChronicleFlow(uninterrupted_db)
        uninterrupted.create_workflow(gated_workflow("approvals-u"), "wu")
        uninterrupted.create_execution({"id": "run-u", "workflow_id": "approvals-u", "input": {}}, "eu")
        uninterrupted.advance("run-u", {"output": {"prepared": True}}, "au1")
        uninterrupted.advance("run-u", {"output": {}}, "au2")
        decided = uninterrupted.decide(
            "run-u",
            {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}},
            "du1",
        )

        self.park()
        resumed = ChronicleFlow(self.database)
        rebuilt = resumed.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual("running", rebuilt["status"])
        self.assertEqual("signoff", rebuilt["pending_approval"])
        continued = resumed.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}},
            "d1",
        )
        self.assertEqual(decided["completed_nodes"], continued["completed_nodes"])
        self.assertEqual(decided["approvals"], continued["approvals"])
        self.assertEqual({"consistent": True, "execution": continued}, resumed.replay("run-1"))

    def test_claim_does_not_gate_decisions(self):
        self.park()
        self.service.claim("run-1", {"worker_id": "worker-7", "lease_seconds": 30}, "cl1")
        # decisions are gated by approver identity, not by the worker lease
        state = self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"receipt": "r-9"}},
            "d1",
        )
        self.assertEqual(["prepare", "signoff"], state["completed_nodes"])
        # advancing the unlocked successor still requires the lease holder
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {"shipped": True}}, "a3")
        final = self.service.advance(
            "run-1",
            {"output": {"shipped": True}, "worker_id": "worker-7"},
            "a3",
        )
        self.assertEqual("completed", final["status"])

    def test_baseline_workflow_has_no_approval_state_or_events(self):
        self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "only", "kind": "task", "depends_on": []}]},
            "w-plain",
        )
        self.service.create_execution({"id": "run-plain", "workflow_id": "plain", "input": {}}, "e-plain")
        state = self.service.advance("run-plain", {"output": {"ok": 1}}, "a-plain")
        self.assertNotIn("pending_approval", state)
        self.assertNotIn("approvals", state)
        started = self.service.events("run-plain")[0]
        self.assertEqual(
            {"workflow_id", "input", "loops", "timeout_seconds", "deadline_at"},
            set(started["payload"]),
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-plain"))

    def test_approval_preserves_float_precision_and_negative_zero(self):
        self.park()
        state = self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"zero": -0.0, "precise": 0.30000000000000004}},
            "df",
        )
        self.assertEqual(-0.0, state["outputs"]["signoff"]["zero"])
        self.assertTrue(str(state["outputs"]["signoff"]["zero"]).startswith("-"))
        self.assertEqual(0.30000000000000004, state["outputs"]["signoff"]["precise"])


class ApprovalInsideLoopTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        workflow = {
            "id": "loop-approval",
            "nodes": [
                {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                {
                    "id": "review",
                    "kind": "task",
                    "depends_on": ["check"],
                    "approval": {"approvers": ["alice"]},
                },
                {
                    "id": "loop",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "review",
                    "condition": "check",
                    "max_iterations": 2,
                },
            ],
        }
        self.service.create_workflow(workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "loop-approval", "input": {"again": True}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_in_loop_body_parks_with_iteration_context_and_decides(self):
        parked = self.service.advance("run-1", {"output": {"ignored": True}}, "a1")
        self.assertEqual("review", parked["pending_approval"])
        requested = self.service.events("run-1")[-1]
        self.assertEqual(
            {"node_id": "review", "approvers": ["alice"], "loop_id": "loop", "iteration": 1},
            requested["payload"],
        )
        state = self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"ok": True}},
            "d1",
        )
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertIn("review", iteration["completed_nodes"])
        self.assertEqual({"ok": True}, iteration["outputs"]["review"])
        self.assertEqual(
            {"node_id": "review", "approver": "alice", "decision": "approved", "reason": None, "loop_id": "loop", "iteration": 1},
            state["approvals"][0],
        )
        # the second iteration reaches the same approval point again
        parked_again = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        self.assertEqual("review", parked_again["pending_approval"])
        self.service.decide(
            "run-1",
            {"approver": "alice", "decision": "rejected", "reason": "second look failed"},
            "d2",
        )
        state = self.service.get_execution("run-1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["review"], state["failed_nodes"])
        self.assertEqual(2, len(state["approvals"]))
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


if __name__ == "__main__":
    unittest.main()
