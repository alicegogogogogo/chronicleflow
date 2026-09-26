import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def workflow(template=None, max_instances=10, nodes_extra=None):
    nodes = [
        {"id": "collect", "kind": "task", "depends_on": []},
        {
            "id": "fanout",
            "kind": "map",
            "depends_on": ["collect"],
            "source": "collect",
            "path": "items",
            "max_instances": max_instances,
            "template": template or {"id": "work"},
        },
    ]
    if nodes_extra:
        nodes.extend(nodes_extra)
    return {"id": "orders", "nodes": nodes}


def nested_workflow(template=None):
    return {
        "id": "orders",
        "nodes": [
            {"id": "entry", "kind": "task", "depends_on": []},
            {
                "id": "fanout",
                "kind": "map",
                "depends_on": ["entry"],
                "source": "entry",
                "path": "items",
                "max_instances": 5,
                "template": template or {"id": "work"},
            },
            {"id": "check", "kind": "condition", "depends_on": ["fanout"], "path": "x", "equals": 1},
            {
                "id": "loop",
                "kind": "loop",
                "depends_on": [],
                "entry": "entry",
                "condition": "check",
                "max_iterations": 3,
            },
        ],
    }


class InstanceDeleteTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def expand(self, items=(1, 2, 3)):
        return self.service.advance("run-1", {"output": {"items": list(items)}}, "a1")

    def delete(self, body, key):
        return self.service.delete_instance("run-1", body, key)

    def deleted_events(self):
        return [e for e in self.service.events("run-1") if e["type"] == "map_instance_deleted"]

    def test_delete_ready_instance_keeps_remaining_order_and_outputs(self):
        self.expand()
        state = self.delete({"map_id": "fanout", "index": 1}, "d1")
        instances = state["maps"]["fanout"]["instances"]
        self.assertEqual([0, 2], [instance["index"] for instance in instances])
        self.assertTrue(all(instance["status"] == "ready" for instance in instances))
        state = self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        state = self.service.advance("run-1", {"output": {"v": 2}}, "a3")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        # The result list holds only the retained instances' outputs, in
        # ascending element index order.
        self.assertEqual([{"v": 0}, {"v": 2}], state["outputs"]["fanout"])
        self.assertEqual("completed", state["status"])
        completed = [e for e in self.service.events("run-1") if e["type"] == "map_completed"]
        self.assertEqual(2, completed[0]["payload"]["instance_count"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_delete_event_records_map_index_and_status_without_loop_context(self):
        self.expand()
        self.delete({"map_id": "fanout", "index": 0}, "d1")
        events = self.deleted_events()
        self.assertEqual(1, len(events))
        self.assertEqual(
            {"map_id": "fanout", "index": 0, "status": "ready"}, events[0]["payload"]
        )

    def test_deleting_last_ready_instance_completes_map_with_empty_output(self):
        self.expand(items=(1,))
        state = self.delete({"map_id": "fanout", "index": 0}, "d1")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([], state["outputs"]["fanout"])
        self.assertEqual("completed", state["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_after_partial_completion_finishes_map(self):
        self.expand()
        self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        self.service.advance("run-1", {"output": {"v": 1}}, "a3")
        # Only instance 2 is still ready; removing it finishes the map.
        state = self.delete({"map_id": "fanout", "index": 2}, "d1")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([{"v": 0}, {"v": 1}], state["outputs"]["fanout"])
        self.assertEqual("completed", state["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_completed_instance_conflicts_and_changes_nothing(self):
        self.expand()
        before = self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        with self.assertRaises(ConflictError):
            self.delete({"map_id": "fanout", "index": 0}, "d1")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual([], self.deleted_events())

    def test_delete_waiting_instance_conflicts(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            workflow({"id": "work", "approval": {"approvers": ["alice"]}}), "w1"
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        with self.assertRaises(ConflictError):
            self.delete({"map_id": "fanout", "index": 0}, "d1")
        # A ready sibling of the parked instance may still be deleted.
        state = self.delete({"map_id": "fanout", "index": 1}, "d2")
        self.assertEqual([0], [i["index"] for i in state["maps"]["fanout"]["instances"]])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_permanently_failed_instance_on_terminated_execution(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow({"id": "work", "retries": 0}), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        self.assertEqual("terminated", state["status"])
        state = self.delete({"map_id": "fanout", "index": 0}, "d1")
        instances = state["maps"]["fanout"]["instances"]
        self.assertEqual([1], [instance["index"] for instance in instances])
        events = self.deleted_events()
        self.assertEqual("failed", events[0]["payload"]["status"])
        # A still-ready instance of the terminated execution may go too.
        state = self.delete({"map_id": "fanout", "index": 1}, "d2")
        self.assertEqual([], state["maps"]["fanout"]["instances"])
        self.assertEqual("terminated", state["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_unknown_map_index_iteration_or_execution_is_not_found(self):
        self.expand()
        with self.assertRaises(NotFoundError):
            self.delete({"map_id": "ghost", "index": 0}, "d1")
        with self.assertRaises(NotFoundError):
            self.delete({"map_id": "collect", "index": 0}, "d2")
        with self.assertRaises(NotFoundError):
            self.delete({"map_id": "fanout", "index": 3}, "d3")
        with self.assertRaises(NotFoundError):
            self.delete({"map_id": "fanout", "index": -1}, "d4")
        with self.assertRaises(NotFoundError):
            self.service.delete_instance("ghost", {"map_id": "fanout", "index": 0}, "d5")
        # A deleted instance is gone: addressing it again is missing too.
        self.delete({"map_id": "fanout", "index": 0}, "d6")
        with self.assertRaises(NotFoundError):
            self.service.delete_instance("run-1", {"map_id": "fanout", "index": 0}, "d7")

    def test_delete_cross_tenant_reference_is_not_found(self):
        self.expand()
        with self.assertRaises(NotFoundError):
            self.service.delete_instance(
                "run-1", {"map_id": "fanout", "index": 0}, "d1", tenant="acme"
            )

    def test_delete_validation_errors_write_nothing(self):
        self.expand()
        before = self.service.get_execution("run-1")
        bad_bodies = [
            None,
            {},
            {"map_id": "fanout"},
            {"index": 0},
            {"map_id": "fanout", "index": 0, "extra": 1},
            {"map_id": "", "index": 0},
            {"map_id": 1, "index": 0},
            {"map_id": "fanout", "index": True},
            {"map_id": "fanout", "index": "0"},
            {"map_id": "fanout", "index": 0.5},
            {"map_id": "fanout", "index": 0, "loop_id": "loop"},
            {"map_id": "fanout", "index": 0, "iteration": 1},
            {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": "1"},
        ]
        for number, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.delete(body, f"bad-{number}")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual([], self.deleted_events())

    def test_delete_idempotency(self):
        self.expand()
        first = self.delete({"map_id": "fanout", "index": 0}, "d1")
        again = self.delete({"map_id": "fanout", "index": 0}, "d1")
        self.assertEqual(first, again)
        self.assertEqual(1, len(self.deleted_events()))
        # The same key naming another instance is a cross-operation conflict.
        with self.assertRaises(ConflictError):
            self.delete({"map_id": "fanout", "index": 1}, "d1")
        # A key already used by another operation conflicts as usual.
        with self.assertRaises(ConflictError):
            self.delete({"map_id": "fanout", "index": 1}, "a1")

    def test_delete_checkpoint_and_recovery_do_not_resurrect(self):
        self.expand()
        self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        self.delete({"map_id": "fanout", "index": 1}, "d1")
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual([0, 2], [i["index"] for i in snapshot["maps"]["fanout"]["instances"]])
        state = self.service.advance("run-1", {"output": {"v": 2}}, "a3")
        self.assertEqual([{"v": 0}, {"v": 2}], state["outputs"]["fanout"])
        completed = [e for e in self.service.events("run-1") if e["type"] == "node_completed"]
        # Source plus two retained instances: no output is repeated.
        self.assertEqual(3, len(completed))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_executions_without_delete_keep_baseline_shape(self):
        self.expand()
        state = self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        instance = state["maps"]["fanout"]["instances"][0]
        self.assertEqual({"index", "status", "output", "failure_reason"}, set(instance))
        self.assertNotIn("input", instance)


class InstanceModifyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": [1, 2, 3]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def modify(self, body, key):
        return self.service.modify_instance("run-1", body, key)

    def modified_events(self):
        return [e for e in self.service.events("run-1") if e["type"] == "map_instance_modified"]

    def test_modify_ready_instance_records_input_and_event(self):
        state = self.modify({"map_id": "fanout", "index": 1, "input": {"sku": "s-9"}}, "m1")
        instance = state["maps"]["fanout"]["instances"][1]
        self.assertEqual({"sku": "s-9"}, instance["input"])
        # Only this instance is rewritten.
        self.assertNotIn("input", state["maps"]["fanout"]["instances"][0])
        self.assertNotIn("input", state["maps"]["fanout"]["instances"][2])
        events = self.modified_events()
        self.assertEqual(1, len(events))
        self.assertEqual(
            {"map_id": "fanout", "index": 1, "before": None, "after": {"sku": "s-9"}},
            events[0]["payload"],
        )
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_second_modify_records_previous_value_as_before(self):
        self.modify({"map_id": "fanout", "index": 0, "input": "first"}, "m1")
        state = self.modify({"map_id": "fanout", "index": 0, "input": "second"}, "m2")
        self.assertEqual("second", state["maps"]["fanout"]["instances"][0]["input"])
        events = self.modified_events()
        self.assertEqual("first", events[1]["payload"]["before"])
        self.assertEqual("second", events[1]["payload"]["after"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_modify_accepts_any_finite_json_value(self):
        for number, value in enumerate((None, True, 0, -0.0, 0.30000000000000004, "text", [1, 2], {"a": 1})):
            with self.subTest(value=value):
                state = self.modify({"map_id": "fanout", "index": 0, "input": value}, f"m{number}")
                self.assertEqual(value, state["maps"]["fanout"]["instances"][0]["input"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_modify_negative_zero_round_trips(self):
        state = self.modify({"map_id": "fanout", "index": 0, "input": -0.0}, "m1")
        value = state["maps"]["fanout"]["instances"][0]["input"]
        self.assertEqual(-0.0, value)
        self.assertTrue(str(value).startswith("-"))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_modify_advanced_instance_conflicts_and_changes_nothing(self):
        before = self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        with self.assertRaises(ConflictError):
            self.modify({"map_id": "fanout", "index": 0, "input": {}}, "m1")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual([], self.modified_events())
        # The recorded output and history of the advanced instance are intact.
        self.assertEqual({"v": 0}, before["maps"]["fanout"]["instances"][0]["output"])

    def test_modify_failed_instance_conflicts(self):
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        with self.assertRaises(ConflictError):
            self.modify({"map_id": "fanout", "index": 0, "input": {}}, "m1")

    def test_modify_missing_references_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.modify({"map_id": "ghost", "index": 0, "input": {}}, "m1")
        with self.assertRaises(NotFoundError):
            self.modify({"map_id": "fanout", "index": 9, "input": {}}, "m2")
        with self.assertRaises(NotFoundError):
            self.service.modify_instance("ghost", {"map_id": "fanout", "index": 0, "input": {}}, "m3")

    def test_modify_validation_errors_write_nothing(self):
        before = self.service.get_execution("run-1")
        bad_bodies = [
            {"map_id": "fanout", "index": 0},
            {"map_id": "fanout", "input": {}},
            {"index": 0, "input": {}},
            {"map_id": "fanout", "index": 0, "input": {}, "extra": 1},
            {"map_id": "fanout", "index": 0, "input": float("nan")},
            {"map_id": "fanout", "index": 0, "input": float("inf")},
            {"map_id": "fanout", "index": 0, "input": {"nested": float("-inf")}},
        ]
        for number, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.modify(body, f"bad-{number}")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual([], self.modified_events())

    def test_modify_idempotency(self):
        first = self.modify({"map_id": "fanout", "index": 0, "input": 1}, "m1")
        again = self.modify({"map_id": "fanout", "index": 0, "input": 1}, "m1")
        self.assertEqual(first, again)
        self.assertEqual(1, len(self.modified_events()))
        with self.assertRaises(ConflictError):
            self.modify({"map_id": "fanout", "index": 1, "input": 1}, "m1")
        with self.assertRaises(ConflictError):
            self.modify({"map_id": "fanout", "index": 1, "input": 1}, "a1")

    def test_modify_checkpoint_and_recovery(self):
        self.modify({"map_id": "fanout", "index": 2, "input": {"n": 2}}, "m1")
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual({"n": 2}, snapshot["maps"]["fanout"]["instances"][2]["input"])
        state = self.service.get_execution("run-1")
        self.assertEqual(snapshot, state)


class NestedInstanceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(nested_workflow(), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"x": 1}}, "e1"
        )
        # Iteration 1: entry completes, the nested map expands three instances.
        self.service.advance("run-1", {"output": {"items": [1, 2, 3]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def iteration(self, state, number=1):
        return state["loops"]["loop"]["iterations"][number - 1]

    def test_nested_delete_carries_loop_context_and_replays(self):
        state = self.service.delete_instance(
            "run-1", {"map_id": "fanout", "index": 1, "loop_id": "loop", "iteration": 1}, "d1"
        )
        instances = self.iteration(state)["maps"]["fanout"]["instances"]
        self.assertEqual([0, 2], [instance["index"] for instance in instances])
        events = [e for e in self.service.events("run-1") if e["type"] == "map_instance_deleted"]
        self.assertEqual(
            {"map_id": "fanout", "index": 1, "status": "ready", "loop_id": "loop", "iteration": 1},
            events[0]["payload"],
        )
        # The remaining instances of the round advance as usual.
        state = self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        state = self.service.advance("run-1", {"output": {"v": 2}}, "a3")
        self.assertEqual([{"v": 0}, {"v": 2}], self.iteration(state)["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_nested_delete_finishing_iteration_lets_loop_proceed(self):
        self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        self.service.delete_instance(
            "run-1", {"map_id": "fanout", "index": 1, "loop_id": "loop", "iteration": 1}, "d1"
        )
        # Removing the last ready instance finishes the map for this round;
        # the loop boundary is evaluated under the usual rules.
        state = self.service.delete_instance(
            "run-1", {"map_id": "fanout", "index": 2, "loop_id": "loop", "iteration": 1}, "d2"
        )
        self.assertEqual("completed", self.iteration(state)["maps"]["fanout"]["status"])
        self.assertEqual([{"v": 0}], self.iteration(state)["outputs"]["fanout"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_nested_modify_carries_loop_context(self):
        state = self.service.modify_instance(
            "run-1",
            {"map_id": "fanout", "index": 2, "loop_id": "loop", "iteration": 1, "input": 7},
            "m1",
        )
        instance = self.iteration(state)["maps"]["fanout"]["instances"][2]
        self.assertEqual(7, instance["input"])
        events = [e for e in self.service.events("run-1") if e["type"] == "map_instance_modified"]
        self.assertEqual(
            {"map_id": "fanout", "index": 2, "before": None, "after": 7, "loop_id": "loop", "iteration": 1},
            events[0]["payload"],
        )
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_nested_addressing_requires_matching_loop_context(self):
        # A nested map is not addressable without its loop context.
        with self.assertRaises(NotFoundError):
            self.service.delete_instance("run-1", {"map_id": "fanout", "index": 0}, "d1")
        # An unknown loop, a wrong loop, and an unreached iteration are missing.
        with self.assertRaises(NotFoundError):
            self.service.delete_instance(
                "run-1", {"map_id": "fanout", "index": 0, "loop_id": "ghost", "iteration": 1}, "d2"
            )
        with self.assertRaises(NotFoundError):
            self.service.delete_instance(
                "run-1", {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": 2}, "d3"
            )
        with self.assertRaises(NotFoundError):
            self.service.delete_instance(
                "run-1", {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": 0}, "d4"
            )

    def test_nested_instances_of_later_iterations_are_independent(self):
        self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        self.service.advance("run-1", {"output": {"v": 1}}, "a3")
        self.service.advance("run-1", {"output": {"v": 2}}, "a4")
        # Iteration 2 begins; entry completes and the map expands again.
        state = self.service.advance("run-1", {"output": {"items": [4, 5]}}, "a5")
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertEqual(
            2, len(self.iteration(state, 2)["maps"]["fanout"]["instances"])
        )
        # Deleting in iteration 2 leaves iteration 1 untouched.
        state = self.service.delete_instance(
            "run-1", {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": 2}, "d1"
        )
        self.assertEqual(3, len(self.iteration(state, 1)["maps"]["fanout"]["instances"]))
        self.assertEqual(
            [1], [i["index"] for i in self.iteration(state, 2)["maps"]["fanout"]["instances"]]
        )
        self.assertTrue(self.service.replay("run-1")["consistent"])


class ExecutionLevelLoopContextTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def test_loop_context_on_an_execution_level_map_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.delete_instance(
                "run-1", {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": 1}, "d1"
            )
        with self.assertRaises(NotFoundError):
            self.service.modify_instance(
                "run-1",
                {"map_id": "fanout", "index": 0, "loop_id": "loop", "iteration": 1, "input": 1},
                "m1",
            )


if __name__ == "__main__":
    unittest.main()
