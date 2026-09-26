import tempfile
import unittest
from pathlib import Path

from chronicleflow.service import ChronicleFlow


def nested_workflow(template=None, max_instances=5, source_path="items", extra_nodes=None):
    """A loop whose body expands a map node once per iteration.

    The body is collect -> fanout (map) -> settle -> again (condition); the
    loop repeats while the input flag `more` stays true, bounded at 3 rounds.
    """
    nodes = [
        {"id": "collect", "kind": "task", "depends_on": []},
        {
            "id": "fanout",
            "kind": "map",
            "depends_on": ["collect"],
            "source": "collect",
            "path": source_path,
            "max_instances": max_instances,
            "template": template or {"id": "work"},
        },
        {"id": "settle", "kind": "task", "depends_on": ["fanout"]},
        {"id": "again", "kind": "condition", "depends_on": ["settle"], "path": "more", "equals": True},
        {"id": "loop", "kind": "loop", "depends_on": [], "entry": "settle",
         "condition": "again", "max_iterations": 3},
    ]
    if extra_nodes:
        nodes.extend(extra_nodes)
    return {"id": "orders", "nodes": nodes}


class NestedMapTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(nested_workflow(), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )

    def tearDown(self):
        self.directory.cleanup()

    def advance(self, key, payload):
        if isinstance(payload, dict) and ("output" in payload or "failure" in payload):
            body = payload
        else:
            body = {"output": payload}
        return self.service.advance("run-1", body, key)

    def run_iteration(self, round_number, items, keys):
        """Complete one loop round: source task, one advance per instance, settle."""
        state = self.advance(keys[0], {"output": {"items": items}})
        self.assertEqual({"items": items}, state["loops"]["loop"]["iterations"][round_number - 1]["outputs"]["collect"])
        for index in range(len(items)):
            state = self.advance(keys[index + 1], {"output": {"round": round_number, "index": index}})
        state = self.advance(keys[len(items) + 1], {"output": {"settled": round_number}})
        return state

    def test_expands_independently_per_iteration(self):
        state = self.run_iteration(1, [1, 2], ["a1", "a2", "a3", "a4"])
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual("completed", iteration["maps"]["fanout"]["status"])
        self.assertEqual([{"round": 1, "index": 0}, {"round": 1, "index": 1}], iteration["outputs"]["fanout"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])

        state = self.run_iteration(2, [5], ["a5", "a6", "a7"])
        iteration = state["loops"]["loop"]["iterations"][1]
        self.assertEqual([{"round": 2, "index": 0}], iteration["outputs"]["fanout"])
        # The first iteration's results are untouched by the second round.
        self.assertEqual(
            [{"round": 1, "index": 0}, {"round": 1, "index": 1}],
            state["loops"]["loop"]["iterations"][0]["outputs"]["fanout"],
        )

        state = self.run_iteration(3, [7, 8, 9], ["a8", "a9", "a10", "a11", "a12"])
        loop = state["loops"]["loop"]
        self.assertEqual("completed", loop["status"])
        self.assertEqual("iteration_limit", loop["end_reason"])
        self.assertEqual(
            [{"round": 3, "index": 0}, {"round": 3, "index": 1}, {"round": 3, "index": 2}],
            loop["iterations"][2]["outputs"]["fanout"],
        )
        # The finished loop surfaces each body node once, then the loop node.
        self.assertEqual(["collect", "fanout", "settle", "again", "loop"], state["completed_nodes"])
        # Body outputs stay in their iterations, not the execution outputs.
        self.assertEqual({}, state["outputs"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_instance_events_carry_loop_iteration_and_index(self):
        self.run_iteration(1, [1], ["a1", "a2", "a3"])
        events = self.service.events("run-1")
        expanded = [event for event in events if event["type"] == "map_expanded"]
        self.assertEqual(1, len(expanded))
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 1, "exceeded": False, "loop_id": "loop", "iteration": 1},
            expanded[0]["payload"],
        )
        completed = [
            event for event in events
            if event["type"] == "node_completed" and event["payload"]["node_id"] == "work"
        ]
        self.assertEqual(1, len(completed))
        self.assertEqual("fanout", completed[0]["payload"]["map_id"])
        self.assertEqual(0, completed[0]["payload"]["index"])
        self.assertEqual("loop", completed[0]["payload"]["loop_id"])
        self.assertEqual(1, completed[0]["payload"]["iteration"])
        finished = [event for event in events if event["type"] == "map_completed"]
        self.assertEqual("loop", finished[0]["payload"]["loop_id"])
        self.assertEqual(1, finished[0]["payload"]["iteration"])

    def test_zero_instance_iteration_completes_with_empty_list(self):
        state = self.advance("a1", {"output": {"other": 1}})
        iteration = state["loops"]["loop"]["iterations"][0]
        map_state = iteration["maps"]["fanout"]
        self.assertEqual("completed", map_state["status"])
        self.assertEqual([], map_state["instances"])
        self.assertEqual([], iteration["outputs"]["fanout"])
        # The iteration continues to the next round's decision immediately.
        state = self.advance("a2", {"output": {"settled": 1}})
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_over_limit_iteration_fails_the_node_and_terminates(self):
        self.service = ChronicleFlow(str(Path(self.directory.name) / "small.db"))
        self.service.create_workflow(nested_workflow(max_instances=1), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )
        state = self.advance("a1", {"output": {"items": [1, 2]}})
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        map_state = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]
        self.assertEqual("failed", map_state["status"])
        self.assertEqual([], map_state["instances"])
        self.assertIsNotNone(map_state["failure_reason"])
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertTrue(expanded[0]["payload"]["exceeded"])
        self.assertEqual("loop", expanded[0]["payload"]["loop_id"])
        self.assertEqual(1, expanded[0]["payload"]["iteration"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_instance_retry_requeues_within_the_iteration(self):
        self.service = ChronicleFlow(str(Path(self.directory.name) / "retry.db"))
        self.service.create_workflow(nested_workflow({"id": "work", "retries": 1}), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )
        self.advance("a1", {"output": {"items": [1]}})
        state = self.advance("a2", {"failure": {"reason": "boom"}})
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("ready", instance["status"])
        retried = [event for event in self.service.events("run-1") if event["type"] == "node_retried"]
        self.assertEqual(1, len(retried))
        self.assertEqual("loop", retried[0]["payload"]["loop_id"])
        self.assertEqual(1, retried[0]["payload"]["iteration"])
        self.assertEqual(0, retried[0]["payload"]["index"])
        state = self.advance("a3", {"output": {"fixed": True}})
        self.assertEqual("completed", state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_instance_retries_exhausted_terminates_execution(self):
        self.service = ChronicleFlow(str(Path(self.directory.name) / "exhausted.db"))
        self.service.create_workflow(nested_workflow({"id": "work", "retries": 1}), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )
        self.advance("a1", {"output": {"items": [1]}})
        self.advance("a2", {"failure": {"reason": "first"}})
        state = self.advance("a3", {"failure": {"reason": "second"}})
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("second", instance["failure_reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_recovery_continues_unfinished_instances_without_duplication(self):
        self.advance("a1", {"output": {"items": [1, 2]}})
        self.advance("a2", {"output": {"shipped": 0}})
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rc1")
        instances = snapshot["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"]
        self.assertEqual("completed", instances[0]["status"])
        self.assertEqual("ready", instances[1]["status"])
        self.advance("a3", {"output": {"shipped": 1}})
        state = self.advance("a4", {"output": {"settled": 1}})
        self.assertEqual(
            [{"shipped": 0}, {"shipped": 1}],
            state["loops"]["loop"]["iterations"][0]["outputs"]["fanout"],
        )
        completed_events = [
            event for event in self.service.events("run-1")
            if event["type"] == "node_completed" and event["payload"]["node_id"] == "work"
        ]
        # No instance output is repeated after the recovery.
        self.assertEqual(2, len(completed_events))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_checkpoints_record_instance_boundaries(self):
        self.advance("a1", {"output": {"items": [1, 2]}})
        self.advance("a2", {"output": {"shipped": 0}})
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        latest = checkpoints[-1]["state"]
        instances = latest["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"]
        self.assertEqual("completed", instances[0]["status"])
        self.assertEqual("ready", instances[1]["status"])

    def test_map_outside_loop_keeps_top_level_maps_field(self):
        # A workflow with only a nested map has no execution-level maps field.
        state = self.service.get_execution("run-1")
        self.assertNotIn("maps", state)
        started = [event for event in self.service.events("run-1") if event["type"] == "execution_started"][0]
        self.assertNotIn("maps", started["payload"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_nested_map_template_may_share_an_id_with_a_declared_node(self):
        self.service = ChronicleFlow(str(Path(self.directory.name) / "shared.db"))
        self.service.create_workflow(nested_workflow({"id": "collect"}), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )
        self.advance("a1", {"output": {"items": [1]}})
        state = self.advance("a2", {"output": {"ok": True}})
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("completed", instance["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class NestedMapApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            nested_workflow({"id": "work", "approval": {"approvers": ["alice", "bob"]}}), "w1"
        )
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"more": True}}, "e1"
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_approval_parks_and_completes_the_instance(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        state = self.service.advance("run-1", {"output": {}}, "a2")
        waiting = state["waiting_approval"]
        self.assertEqual("work", waiting["node_id"])
        self.assertEqual("fanout", waiting["map_id"])
        self.assertEqual(0, waiting["index"])
        self.assertEqual("loop", waiting["loop_id"])
        self.assertEqual(1, waiting["iteration"])
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("waiting", instance["status"])
        # A parked execution absorbs further advances.
        absorbed = self.service.advance("run-1", {"output": {"ignored": True}}, "a3")
        self.assertEqual(state, absorbed)
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 1}}, "d1"
        )
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("completed", instance["status"])
        self.assertEqual({"ok": 1}, instance["output"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_rejection_terminates_with_rejected_reason(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "rejected", "reason": "nope"}, "d1"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        instance = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("nope", instance["failure_reason"])
        decided = [event for event in self.service.events("run-1") if event["type"] == "approval_decided"]
        self.assertEqual("loop", decided[0]["payload"]["loop_id"])
        self.assertEqual(1, decided[0]["payload"]["iteration"])
        self.assertEqual(0, decided[0]["payload"]["index"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_second_iteration_approval_belongs_to_that_round(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"v": 1}}, "d1")
        self.service.advance("run-1", {"output": {"settled": 1}}, "a3")
        # Iteration 2 starts; the new instance parks at the same approval point.
        self.service.advance("run-1", {"output": {"items": [2]}}, "a4")
        state = self.service.advance("run-1", {"output": {}}, "a5")
        waiting = state["waiting_approval"]
        self.assertEqual(2, waiting["iteration"])
        self.assertEqual(0, waiting["index"])
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "approved", "output": {"v": 2}}, "d2"
        )
        iterations = state["loops"]["loop"]["iterations"]
        self.assertEqual([{"v": 1}], iterations[0]["outputs"]["fanout"])
        self.assertEqual([{"v": 2}], iterations[1]["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


if __name__ == "__main__":
    unittest.main()
