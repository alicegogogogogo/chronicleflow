import tempfile
import threading
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.service import ChronicleFlow


class QuorumDeclarationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def task(self, approval):
        return {"id": "a", "kind": "task", "depends_on": [], "approval": approval}

    def test_required_approvals_is_round_tripped(self):
        workflow = {
            "id": "gated",
            "nodes": [self.task({"approvers": ["alice", "bob", "carol"], "required_approvals": 2})],
        }
        stored = self.service.create_workflow(workflow, "w1")
        self.assertEqual(workflow, stored)
        self.assertEqual([workflow], self.service.get_workflow("gated")["versions"])

    def test_required_approvals_bounds_are_accepted(self):
        for required in (1, 2):
            with self.subTest(required=required):
                workflow = {
                    "id": f"gated-{required}",
                    "nodes": [self.task({"approvers": ["alice", "bob"], "required_approvals": required})],
                }
                self.assertEqual(workflow, self.service.create_workflow(workflow, f"w{required}"))

    def test_map_template_required_approvals_is_round_tripped(self):
        workflow = {
            "id": "mapped",
            "nodes": [
                {"id": "collect", "kind": "task", "depends_on": []},
                {
                    "id": "ship",
                    "kind": "map",
                    "depends_on": ["collect"],
                    "source": "collect",
                    "path": "items",
                    "max_instances": 5,
                    "template": {
                        "id": "ship-item",
                        "approval": {"approvers": ["alice", "bob"], "required_approvals": 2},
                    },
                },
            ],
        }
        self.assertEqual(workflow, self.service.create_workflow(workflow, "w1"))
        self.assertEqual([workflow], self.service.get_workflow("mapped")["versions"])

    def test_invalid_required_approvals_are_rejected(self):
        invalid = [
            self.task({"approvers": ["alice", "bob"], "required_approvals": None}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": True}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": False}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": 1.5}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": "2"}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": 0}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": -1}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": 3}),
            self.task({"approvers": ["alice"], "required_approvals": 2}),
            self.task({"approvers": ["alice", "bob"], "required_approvals": 2, "extra": 1}),
            self.task({"required_approvals": 2}),
        ]
        for index, raw_node in enumerate(invalid):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-{index}", "nodes": [raw_node]}, f"w-bad-{index}")
                # the rejection writes nothing: the workflow stays unknown
                with self.assertRaises(Exception):
                    self.service.get_workflow(f"bad-{index}")


class QuorumFlowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {"id": "reserve", "kind": "task", "depends_on": []},
                    {
                        "id": "charge",
                        "kind": "task",
                        "depends_on": ["reserve"],
                        "approval": {"approvers": ["alice", "bob", "carol"], "required_approvals": 2},
                    },
                    {"id": "ship", "kind": "task", "depends_on": ["charge"]},
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.parked = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")

    def tearDown(self):
        self.directory.cleanup()

    def event_types(self, execution_id="run-1"):
        return [event["type"] for event in self.service.events(execution_id)]

    def test_waiting_state_exposes_quorum_progress(self):
        waiting = self.parked["waiting_approval"]
        self.assertEqual(
            {
                "node_id": "charge",
                "approvers": ["alice", "bob", "carol"],
                "required_approvals": 2,
                "approved": [],
                "remaining": 2,
            },
            waiting,
        )
        requested = self.service.events("run-1")[-1]
        self.assertEqual("approval_requested", requested["type"])
        self.assertEqual(
            {"node_id": "charge", "approvers": ["alice", "bob", "carol"], "required_approvals": 2},
            requested["payload"],
        )

    def test_partial_approval_stays_parked_and_checkpoints(self):
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1"
        )
        self.assertEqual("running", state["status"])
        self.assertEqual(["reserve"], state["completed_nodes"])
        self.assertNotIn("charge", state["outputs"])
        waiting = state["waiting_approval"]
        self.assertEqual(["alice"], waiting["approved"])
        self.assertEqual(1, waiting["remaining"])
        self.assertEqual(2, waiting["required_approvals"])
        record = state["approvals"][0]
        self.assertEqual(
            {"node_id": "charge", "approver": "alice", "decision": "approved", "reason": None, "output": {"v": 1}},
            record,
        )
        decided = self.service.events("run-1")[-1]
        self.assertEqual("approval_decided", decided["type"])
        self.assertEqual(
            {"node_id": "charge", "approver": "alice", "decision": "approved", "output": {"v": 1}},
            decided["payload"],
        )
        # the partial approval is a persisted checkpoint boundary
        checkpoint = self.service.checkpoints("run-1")["checkpoints"][-1]
        self.assertEqual(state, checkpoint["state"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_threshold_approval_completes_with_its_output(self):
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1")
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "approved", "output": {"v": 2}}, "d2"
        )
        self.assertIsNone(state["waiting_approval"])
        self.assertEqual(["reserve", "charge"], state["completed_nodes"])
        # the deciding approval's output becomes the task output
        self.assertEqual({"v": 2}, state["outputs"]["charge"])
        self.assertEqual(
            ["execution_started", "node_completed", "approval_requested",
             "approval_decided", "approval_decided", "node_completed"],
            self.event_types(),
        )
        self.assertEqual(["alice", "bob"], [record["approver"] for record in state["approvals"]])
        final = self.service.advance("run-1", {"output": {"shipped": True}}, "a3")
        self.assertEqual("completed", final["status"])
        self.assertEqual({"consistent": True, "execution": final}, self.service.replay("run-1"))

    def test_repeated_identical_approval_returns_state_without_new_events(self):
        first = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1"
        )
        repeated = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d2"
        )
        self.assertEqual(first, repeated)
        self.assertEqual(1, sum(1 for t in self.event_types() if t == "approval_decided"))
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(first, checkpoints[-1]["state"])

    def test_changed_vote_conflicts(self):
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1")
        for body in (
            {"approver": "alice", "decision": "approved", "output": {"v": 2}},
            {"approver": "alice", "decision": "rejected", "reason": "changed my mind"},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ConflictError):
                    self.service.decision("run-1", body, "d-changed")
        # the point is still parked with the original progress
        state = self.service.get_execution("run-1")
        self.assertEqual(["alice"], state["waiting_approval"]["approved"])
        self.assertEqual(1, state["waiting_approval"]["remaining"])

    def test_non_approver_conflicts(self):
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "dave", "decision": "approved", "output": {}}, "d-bad"
            )
        self.assertEqual(self.parked, self.service.get_execution("run-1"))

    def test_rejection_terminates_immediately_despite_partial_approvals(self):
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1")
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "rejected", "reason": "suspicious"}, "d2"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["charge"], state["failed_nodes"])
        self.assertIsNone(state["waiting_approval"])
        self.assertNotIn("charge", state["outputs"])
        self.assertEqual(["alice", "bob"], [record["approver"] for record in state["approvals"]])
        self.assertEqual("execution_terminated", self.service.events("run-1")[-1]["type"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_quorum_progress_survives_restart(self):
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1")
        resumed = ChronicleFlow(self.database)
        waiting = resumed.get_execution("run-1")["waiting_approval"]
        self.assertEqual(
            {
                "node_id": "charge",
                "approvers": ["alice", "bob", "carol"],
                "required_approvals": 2,
                "approved": ["alice"],
                "remaining": 1,
            },
            waiting,
        )
        state = resumed.decision("run-1", {"approver": "carol", "decision": "approved", "output": {"v": 3}}, "d2")
        self.assertEqual(["reserve", "charge"], state["completed_nodes"])
        self.assertEqual({"v": 3}, state["outputs"]["charge"])
        recovered = resumed.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(state, recovered)
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-1"))

    def test_unanimous_quorum(self):
        self.service.create_workflow(
            {
                "id": "all",
                "nodes": [
                    {
                        "id": "a",
                        "kind": "task",
                        "depends_on": [],
                        "approval": {"approvers": ["alice", "bob"], "required_approvals": 2},
                    }
                ],
            },
            "w-all",
        )
        self.service.create_execution({"id": "run-all", "workflow_id": "all", "input": {}}, "e-all")
        self.service.advance("run-all", {"output": {}}, "aa1")
        state = self.service.decision("run-all", {"approver": "alice", "decision": "approved", "output": {}}, "da1")
        self.assertEqual("running", state["status"])
        state = self.service.decision("run-all", {"approver": "bob", "decision": "approved", "output": {"x": 1}}, "da2")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"x": 1}, state["outputs"]["a"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-all"))

    def test_explicit_single_required_approval_completes_at_once(self):
        self.service.create_workflow(
            {
                "id": "single",
                "nodes": [
                    {
                        "id": "a",
                        "kind": "task",
                        "depends_on": [],
                        "approval": {"approvers": ["alice", "bob"], "required_approvals": 1},
                    }
                ],
            },
            "w-single",
        )
        self.service.create_execution({"id": "run-single", "workflow_id": "single", "input": {}}, "e-single")
        parked = self.service.advance("run-single", {"output": {}}, "as1")
        self.assertEqual(
            {
                "node_id": "a",
                "approvers": ["alice", "bob"],
                "required_approvals": 1,
                "approved": [],
                "remaining": 1,
            },
            parked["waiting_approval"],
        )
        state = self.service.decision(
            "run-single", {"approver": "bob", "decision": "approved", "output": {"y": 2}}, "ds1"
        )
        self.assertEqual("completed", state["status"])
        self.assertEqual({"y": 2}, state["outputs"]["a"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-single"))


class QuorumInLoopTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "loop-gated",
                "nodes": [
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                    {
                        "id": "attempt",
                        "kind": "task",
                        "depends_on": ["check"],
                        "approval": {"approvers": ["alice", "bob"], "required_approvals": 2},
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
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "loop-gated", "input": {"again": True}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_quorum_inside_loop_iterations(self):
        parked = self.service.advance("run-1", {"output": {}}, "a1")
        self.assertEqual(
            {
                "node_id": "attempt",
                "approvers": ["alice", "bob"],
                "required_approvals": 2,
                "approved": [],
                "remaining": 2,
                "loop_id": "loop",
                "iteration": 1,
            },
            parked["waiting_approval"],
        )
        state = self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"t": 1}}, "d1")
        self.assertEqual(1, state["loops"]["loop"]["current_iteration"])
        self.assertEqual(["alice"], state["waiting_approval"]["approved"])
        state = self.service.decision("run-1", {"approver": "bob", "decision": "approved", "output": {"t": 2}}, "d2")
        self.assertIsNone(state["waiting_approval"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertEqual({"t": 2}, state["loops"]["loop"]["iterations"][0]["outputs"]["attempt"])
        # the next iteration parks at the same point with fresh progress
        parked = self.service.advance("run-1", {"output": {}}, "a2")
        self.assertEqual([], parked["waiting_approval"]["approved"])
        self.assertEqual(2, parked["waiting_approval"]["remaining"])
        self.assertEqual(2, parked["waiting_approval"]["iteration"])
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"t": 3}}, "d3")
        state = self.service.decision("run-1", {"approver": "bob", "decision": "approved", "output": {"t": 4}}, "d4")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


class QuorumInMapTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "mapped",
                "nodes": [
                    {"id": "collect", "kind": "task", "depends_on": []},
                    {
                        "id": "ship",
                        "kind": "map",
                        "depends_on": ["collect"],
                        "source": "collect",
                        "path": "items",
                        "max_instances": 5,
                        "template": {
                            "id": "ship-item",
                            "approval": {"approvers": ["alice", "bob"], "required_approvals": 2},
                        },
                    },
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "mapped", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def test_quorum_per_instance(self):
        parked = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        self.assertEqual(
            {
                "node_id": "ship-item",
                "approvers": ["alice", "bob"],
                "required_approvals": 2,
                "approved": [],
                "remaining": 2,
                "map_id": "ship",
                "index": 0,
            },
            parked["waiting_approval"],
        )
        state = self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"s": 1}}, "d1")
        instance = state["maps"]["ship"]["instances"][0]
        self.assertEqual("waiting", instance["status"])
        self.assertEqual(["alice"], state["waiting_approval"]["approved"])
        state = self.service.decision("run-1", {"approver": "bob", "decision": "approved", "output": {"s": 2}}, "d2")
        self.assertIsNone(state["waiting_approval"])
        instance = state["maps"]["ship"]["instances"][0]
        self.assertEqual("completed", instance["status"])
        self.assertEqual({"s": 2}, instance["output"])
        # the second instance parks independently with fresh progress
        parked = self.service.advance("run-1", {"output": {}}, "a3")
        self.assertEqual([], parked["waiting_approval"]["approved"])
        self.assertEqual(1, parked["waiting_approval"]["index"])
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"s": 3}}, "d3")
        state = self.service.decision("run-1", {"approver": "bob", "decision": "approved", "output": {"s": 4}}, "d4")
        self.assertEqual("completed", state["status"])
        self.assertEqual([{"s": 2}, {"s": 4}], state["outputs"]["ship"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_instance_rejection_terminates_despite_partial_approval(self):
        self.service.advance("run-1", {"output": {}}, "a2")
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"s": 1}}, "d1")
        state = self.service.decision("run-1", {"approver": "bob", "decision": "rejected", "reason": "no"}, "d2")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["ship"], state["failed_nodes"])
        instance = state["maps"]["ship"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("no", instance["failure_reason"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


class QuorumConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {
                        "id": "charge",
                        "kind": "task",
                        "depends_on": [],
                        "approval": {"approvers": ["alice", "bob", "carol"], "required_approvals": 2},
                    }
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def test_concurrent_decisions_settle_in_one_order_and_complete_once(self):
        barrier = threading.Barrier(3)
        results = [None] * 3
        approvers = ["alice", "bob", "carol"]

        def worker(index):
            barrier.wait()
            try:
                results[index] = (
                    "ok",
                    self.service.decision(
                        "run-1",
                        {"approver": approvers[index], "decision": "approved", "output": {"by": approvers[index]}},
                        f"d{index}",
                    ),
                )
            except ConflictError:
                results[index] = ("conflict", None)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # exactly two approvals were admitted; the latecomer found no pending point
        outcomes = [outcome for outcome, _ in results]
        self.assertEqual(2, outcomes.count("ok"))
        self.assertEqual(1, outcomes.count("conflict"))
        state = self.service.get_execution("run-1")
        self.assertEqual("completed", state["status"])
        self.assertEqual(2, len(state["approvals"]))
        types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(2, types.count("approval_decided"))
        self.assertEqual(1, types.count("node_completed"))
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))


if __name__ == "__main__":
    unittest.main()
