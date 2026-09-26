import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def nested_workflow(template=None, max_instances=3, with_successor=True):
    nodes = [
        {"id": "entry", "kind": "task", "depends_on": []},
        {
            "id": "fanout",
            "kind": "map",
            "depends_on": ["entry"],
            "source": "entry",
            "path": "items",
            "max_instances": max_instances,
            "template": template or {"id": "work"},
        },
        {"id": "check", "kind": "condition", "depends_on": ["fanout"], "path": "again", "equals": True},
        {"id": "loop", "kind": "loop", "depends_on": [], "entry": "entry", "condition": "check", "max_iterations": 3},
    ]
    if with_successor:
        nodes.append({"id": "after", "kind": "task", "depends_on": ["loop"]})
    return {"id": "orders", "nodes": nodes}


class NestedMapTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(nested_workflow(), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"again": True}}, "e1"
        )

    def tearDown(self):
        self.directory.cleanup()

    def advance(self, key, payload):
        if isinstance(payload, dict) and ("output" in payload or "failure" in payload):
            body = payload
        else:
            body = {"output": payload}
        return self.service.advance("run-1", body, key)

    def iteration(self, state, number):
        return state["loops"]["loop"]["iterations"][number - 1]

    def test_expands_independently_per_iteration(self):
        state = self.advance("a1", {"items": [10, 20]})
        self.assertNotIn("maps", state)
        first = self.iteration(state, 1)["maps"]["fanout"]
        self.assertEqual("running", first["status"])
        self.assertEqual([0, 1], [instance["index"] for instance in first["instances"]])
        self.advance("a2", {"v": 0})
        state = self.advance("a3", {"v": 1})
        first = self.iteration(state, 1)["maps"]["fanout"]
        self.assertEqual("completed", first["status"])
        self.assertEqual([{"v": 0}, {"v": 1}], first["outputs"])
        self.assertEqual([{"v": 0}, {"v": 1}], self.iteration(state, 1)["outputs"]["fanout"])
        self.assertEqual(["entry", "fanout", "check"], self.iteration(state, 1)["completed_nodes"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        # The second iteration expands from its own source output.
        state = self.advance("a4", {"items": [7]})
        second = self.iteration(state, 2)["maps"]["fanout"]
        self.assertEqual("running", second["status"])
        self.assertEqual([0], [instance["index"] for instance in second["instances"]])
        # The first iteration's results are untouched.
        self.assertEqual([{"v": 0}, {"v": 1}], self.iteration(state, 1)["maps"]["fanout"]["outputs"])
        state = self.advance("a5", {"v": 9})
        self.assertEqual([{"v": 9}], self.iteration(state, 2)["maps"]["fanout"]["outputs"])
        self.assertEqual(3, state["loops"]["loop"]["current_iteration"])

    def test_events_carry_loop_context_and_replay_is_consistent(self):
        self.advance("a1", {"items": [10, 20]})
        self.advance("a2", {"v": 0})
        self.advance("a3", {"v": 1})
        self.advance("a4", {"items": []})
        self.advance("a5", {"items": [7]})
        state = self.advance("a6", {"v": 9})
        self.assertEqual("completed", state["loops"]["loop"]["status"])
        self.assertEqual("iteration_limit", state["loops"]["loop"]["end_reason"])
        self.assertIn("fanout", state["completed_nodes"])
        self.assertNotIn("fanout", state["outputs"])
        state = self.advance("a7", {"done": True})
        self.assertEqual("completed", state["status"])
        events = self.service.events("run-1")
        expanded = [event for event in events if event["type"] == "map_expanded"]
        self.assertEqual(3, len(expanded))
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 2, "exceeded": False, "loop_id": "loop", "iteration": 1},
            expanded[0]["payload"],
        )
        self.assertEqual(0, expanded[1]["payload"]["instance_count"])
        self.assertEqual(2, expanded[1]["payload"]["iteration"])
        instance_completed = [
            event for event in events if event["type"] == "node_completed" and "map_id" in event["payload"]
        ]
        self.assertEqual("loop", instance_completed[0]["payload"]["loop_id"])
        self.assertEqual(1, instance_completed[0]["payload"]["iteration"])
        self.assertEqual(0, instance_completed[0]["payload"]["index"])
        self.assertEqual(3, instance_completed[-1]["payload"]["iteration"])
        map_completed = [event for event in events if event["type"] == "map_completed"]
        self.assertEqual([1, 2, 3], [event["payload"]["iteration"] for event in map_completed])
        started = [event for event in events if event["type"] == "iteration_started"]
        self.assertEqual({"fanout": {"status": "pending", "instances": [], "outputs": [], "failure_reason": None}},
                         started[0]["payload"]["maps"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_missing_path_expands_to_zero_instances(self):
        state = self.advance("a1", {"other": 1})
        first = self.iteration(state, 1)["maps"]["fanout"]
        self.assertEqual("completed", first["status"])
        self.assertEqual([], first["outputs"])
        self.assertEqual([], first["instances"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])

    def test_exceeding_the_bound_fails_the_execution(self):
        state = self.advance("a1", {"items": [1, 2, 3, 4]})
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        first = self.iteration(state, 1)["maps"]["fanout"]
        self.assertEqual("failed", first["status"])
        self.assertEqual([], first["instances"])
        self.assertIn("max_instances", first["failure_reason"])
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(1, len(expanded))
        self.assertEqual(
            {
                "map_id": "fanout",
                "instance_count": 0,
                "element_count": 4,
                "max_instances": 3,
                "exceeded": True,
                "reason": first["failure_reason"],
                "loop_id": "loop",
                "iteration": 1,
            },
            expanded[0]["payload"],
        )
        terminated = [event for event in self.service.events("run-1") if event["type"] == "execution_terminated"]
        self.assertEqual("loop", terminated[0]["payload"]["loop_id"])
        self.assertEqual(1, terminated[0]["payload"]["iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_instance_retries_are_per_iteration(self):
        workflow = nested_workflow(template={"id": "work", "retries": 1})
        workflow["id"] = "retried"
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "run-2", "workflow_id": "retried", "input": {"again": True}}, "e2")
        self.service.advance("run-2", {"output": {"items": [1]}}, "b1")
        state = self.service.advance("run-2", {"failure": {"reason": "boom"}}, "b2")
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("ready", instance["status"])
        retried = [event for event in self.service.events("run-2") if event["type"] == "node_retried"]
        self.assertEqual(2, retried[0]["payload"]["attempt"])
        self.assertEqual("loop", retried[0]["payload"]["loop_id"])
        self.assertEqual(1, retried[0]["payload"]["iteration"])
        state = self.service.advance("run-2", {"output": {"v": 0}}, "b3")
        self.assertEqual("completed", state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["status"])
        self.assertTrue(self.service.replay("run-2")["consistent"])

    def test_exhausted_instance_failure_terminates_with_loop_context(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "b1")
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "b2")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("boom", instance["failure_reason"])
        terminated = [event for event in self.service.events("run-1") if event["type"] == "execution_terminated"]
        self.assertEqual("loop", terminated[0]["payload"]["loop_id"])
        self.assertEqual(1, terminated[0]["payload"]["iteration"])
        self.assertEqual(0, terminated[0]["payload"]["index"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_cancel_with_running_instances_keeps_cancelled_reason(self):
        self.advance("a1", {"items": [1]})
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("cancelled", state["termination_reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_recovery_continues_unfinished_instances(self):
        self.advance("a1", {"items": [1, 2, 3]})
        self.advance("a2", {"v": 0})
        # A fresh service over the same database simulates a restart.
        restarted = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        state = restarted.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        instances = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"]
        self.assertEqual("completed", instances[0]["status"])
        self.assertEqual("ready", instances[1]["status"])
        state = restarted.advance("run-1", {"output": {"v": 1}}, "a3")
        state = restarted.advance("run-1", {"output": {"v": 2}}, "a4")
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        outputs = [
            event["payload"]["output"]
            for event in restarted.events("run-1")
            if event["type"] == "node_completed" and "map_id" in event["payload"]
        ]
        self.assertEqual([{"v": 0}, {"v": 1}, {"v": 2}], outputs)
        self.assertTrue(restarted.replay("run-1")["consistent"])


class NestedMapApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            nested_workflow(template={"id": "work", "approval": {"approvers": ["alice"]}}), "w1"
        )
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"again": True}}, "e1"
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_parks_and_decides_with_loop_context(self):
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        state = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        self.assertEqual(
            {
                "node_id": "work",
                "approvers": ["alice"],
                "loop_id": "loop",
                "iteration": 1,
                "map_id": "fanout",
                "index": 0,
            },
            state["waiting_approval"],
        )
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("waiting", instance["status"])
        requested = [event for event in self.service.events("run-1") if event["type"] == "approval_requested"]
        self.assertEqual("loop", requested[0]["payload"]["loop_id"])
        self.assertEqual(1, requested[0]["payload"]["iteration"])
        self.assertEqual(0, requested[0]["payload"]["index"])
        # An advance while parked is absorbed without consuming the output.
        parked = self.service.advance("run-1", {"output": {"ignored": True}}, "a3")
        self.assertEqual(state["waiting_approval"], parked["waiting_approval"])
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 0}}, "d1"
        )
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("completed", instance["status"])
        self.assertEqual({"v": 0}, instance["output"])
        record = state["approvals"][0]
        self.assertEqual("loop", record["loop_id"])
        self.assertEqual(1, record["iteration"])
        self.assertEqual("fanout", record["map_id"])
        self.assertEqual(0, record["index"])
        # Repeating the same decision returns the first result.
        repeated = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"v": 0}}, "d2"
        )
        self.assertEqual(state, repeated)
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_rejection_terminates_with_rejected_reason(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "rejected", "reason": "no"}, "d1"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        map_state = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]
        self.assertEqual("failed", map_state["status"])
        self.assertEqual("no", map_state["failure_reason"])
        self.assertEqual("no", map_state["instances"][0]["failure_reason"])
        terminated = [event for event in self.service.events("run-1") if event["type"] == "execution_terminated"]
        self.assertEqual("loop", terminated[0]["payload"]["loop_id"])
        self.assertEqual(1, terminated[0]["payload"]["iteration"])
        self.assertEqual(0, terminated[0]["payload"]["index"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class NestedMapValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_invalid_nested_definition_is_rejected_wholesale(self):
        document = nested_workflow()
        document["nodes"][1]["unexpected"] = 1
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "w1")
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("orders")
        document = nested_workflow(max_instances=0)
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "w2")
        document = nested_workflow()
        document["nodes"][1]["path"] = "a..b"
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "w3")
        document = nested_workflow(template={"id": "work", "retries": 11})
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "w4")

    def test_missing_workflow_and_execution_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.create_execution({"id": "run-1", "workflow_id": "ghost", "input": {}}, "e1")
        self.service.create_workflow(nested_workflow(), "w1")
        with self.assertRaises(NotFoundError):
            self.service.get_execution("ghost")

    def test_idempotency_key_reuse_across_operations_conflicts(self):
        self.service.create_workflow(nested_workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {"again": True}}, "e1")
        self.service.advance("run-1", {"output": {"items": [1]}}, "shared")
        with self.assertRaises(ConflictError):
            self.service.cancel("run-1", "shared")


class QueueNameLengthTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_long_queue_names_are_accepted(self):
        name = "q" * 200
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [{"id": "only", "kind": "task", "depends_on": []}],
                "subscriptions": [{"queue": name, "events": ["node_completed"]}],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"done": True}}, "a1")
        queues = self.service.queues("run-1")["queues"]
        self.assertEqual(name, queues[0]["queue"])
        self.assertEqual(1, len(queues[0]["messages"]))

    def test_empty_queue_name_is_still_rejected(self):
        with self.assertRaises(ValidationError):
            self.service.create_workflow(
                {
                    "id": "orders",
                    "nodes": [{"id": "only", "kind": "task", "depends_on": []}],
                    "subscriptions": [{"queue": "", "events": ["node_completed"]}],
                },
                "w1",
            )


if __name__ == "__main__":
    unittest.main()
