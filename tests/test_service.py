import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, ValidationError
from chronicleflow.service import ChronicleFlow


class ChronicleFlowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.workflow = {
            "id": "orders",
            "nodes": [
                {"id": "reserve", "kind": "task", "depends_on": []},
                {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
            ],
        }

    def tearDown(self):
        self.directory.cleanup()

    def test_execution_advances_and_replays(self):
        self.service.create_workflow(self.workflow, "w1")
        created = self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {"order": 7}}, "e1")
        self.assertEqual("running", created["status"])
        first = self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.assertEqual(["reserve"], first["completed_nodes"])
        completed = self.service.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        self.assertEqual("completed", completed["status"])
        self.assertEqual({"consistent": True, "execution": completed}, self.service.replay("run-1"))

    def test_idempotent_advance_returns_original_result(self):
        self.service.create_workflow(self.workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        first = self.service.advance("run-1", {"output": {"value": 1}}, "same")
        repeated = self.service.advance("run-1", {"output": {"value": 999}}, "same")
        self.assertEqual(first, repeated)
        self.assertEqual(2, len(self.service.events("run-1")))

    def test_cycle_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "cycle"):
            self.service.create_workflow(
                {
                    "id": "cyclic",
                    "nodes": [
                        {"id": "a", "kind": "task", "depends_on": ["b"]},
                        {"id": "b", "kind": "task", "depends_on": ["a"]},
                    ],
                },
                "w1",
            )

    def test_key_cannot_be_reused_for_another_operation(self):
        self.service.create_workflow(self.workflow, "shared")
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "shared")


class ConditionWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.workflow = {
            "id": "approval",
            "nodes": [
                {"id": "collect", "kind": "task", "depends_on": []},
                {"id": "is_vip", "kind": "condition", "depends_on": ["collect"], "path": "customer.vip", "equals": True},
                {
                    "id": "expedite",
                    "kind": "task",
                    "depends_on": ["is_vip"],
                    "run_if": {"condition_id": "is_vip", "expected": True},
                },
                {
                    "id": "standard",
                    "kind": "task",
                    "depends_on": ["is_vip"],
                    "run_if": {"condition_id": "is_vip", "expected": False},
                },
            ],
        }
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, input_data, execution_id="run-1"):
        return self.service.create_execution({"id": execution_id, "workflow_id": "approval", "input": input_data}, f"e-{execution_id}")

    def test_matching_run_if_completes_task_and_skips_other_branch(self):
        self.start({"customer": {"vip": True}})
        self.service.advance("run-1", {"output": {"collected": 1}}, "a1")
        state = self.service.advance("run-1", {"output": {"fast": True}}, "a2")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["collect", "is_vip", "expedite"], state["completed_nodes"])
        self.assertEqual(["standard"], state["skipped_nodes"])
        self.assertEqual({"is_vip": True}, state["condition_results"])
        self.assertEqual({"collect": {"collected": 1}, "expedite": {"fast": True}}, state["outputs"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            ["execution_started", "node_completed", "condition_evaluated", "node_skipped", "node_completed", "execution_completed"],
            event_types,
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_non_matching_condition_skips_task_and_satisfies_successors(self):
        self.start({"customer": {"vip": False}})
        self.service.advance("run-1", {"output": {}}, "a1")
        state = self.service.advance("run-1", {"output": {"slow": True}}, "a2")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["expedite"], state["skipped_nodes"])
        self.assertEqual({"is_vip": False}, state["condition_results"])
        self.assertNotIn("expedite", state["outputs"])
        self.assertEqual({"slow": True}, state["outputs"]["standard"])

    def test_missing_path_evaluates_to_false(self):
        self.start({"customer": {}})
        self.service.advance("run-1", {"output": {}}, "a1")
        state = self.service.advance("run-1", {"output": {}}, "a2")
        self.assertEqual({"is_vip": False}, state["condition_results"])
        self.assertEqual(["expedite"], state["skipped_nodes"])

    def test_equals_compares_json_type_and_value(self):
        workflow = {
            "id": "typed",
            "nodes": [
                {"id": "flag", "kind": "condition", "depends_on": [], "path": "flag", "equals": 1},
                {"id": "act", "kind": "task", "depends_on": ["flag"], "run_if": {"condition_id": "flag", "expected": True}},
            ],
        }
        self.service.create_workflow(workflow, "w-typed")
        # boolean true is not the JSON number 1, so the condition is false
        self.service.create_execution({"id": "run-t", "workflow_id": "typed", "input": {"flag": True}}, "e-t")
        state = self.service.advance("run-t", {"output": {"ignored": True}}, "a-t")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"flag": False}, state["condition_results"])
        self.assertEqual(["act"], state["skipped_nodes"])
        self.assertEqual({}, state["outputs"])

    def test_auto_processing_completion_returns_state_without_consuming_output(self):
        self.start({"customer": {"vip": False}})
        self.service.advance("run-1", {"output": {}}, "a1")
        state = self.service.advance("run-1", {"output": {"slow": True}}, "a2")
        self.assertEqual("completed", state["status"])
        again = self.service.advance("run-1", {"output": {"extra": 1}}, "a3")
        self.assertEqual(state, again)
        self.assertEqual(6, len(self.service.events("run-1")))

    def test_condition_only_workflow_completes_during_auto_processing(self):
        workflow = {
            "id": "check",
            "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "ok", "equals": "yes"}],
        }
        self.service.create_workflow(workflow, "w-check")
        self.service.create_execution({"id": "run-c", "workflow_id": "check", "input": {"ok": "yes"}}, "e-c")
        state = self.service.advance("run-c", {"output": {"never": "used"}}, "a-c")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"c": True}, state["condition_results"])
        self.assertEqual({}, state["outputs"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-c"))

    def test_condition_definition_is_round_tripped(self):
        document = dict(self.workflow, id="approval-copy")
        stored = self.service.create_workflow(document, "w-copy")
        self.assertEqual(document, stored)

    def test_invalid_nodes_are_rejected(self):
        base = [{"id": "c", "kind": "condition", "depends_on": [], "path": "a.b", "equals": 1}]
        invalid_workflows = [
            # unknown kind
            [{"id": "x", "kind": "gate", "depends_on": []}],
            # unknown field on a task
            [{"id": "x", "kind": "task", "depends_on": [], "path": "a"}],
            # condition missing equals
            [{"id": "c", "kind": "condition", "depends_on": [], "path": "a"}],
            # condition with run_if
            [dict(base[0], run_if={"condition_id": "c", "expected": True})],
            # empty path
            [dict(base[0], path="")],
            # empty path segment
            [dict(base[0], path="a..b")],
            # non-string path
            [dict(base[0], path=7)],
            # non-scalar equals
            [dict(base[0], equals=[1])],
            [dict(base[0], equals={"a": 1})],
            # run_if expected not boolean
            [{"id": "t", "kind": "task", "depends_on": [], "run_if": {"condition_id": "c", "expected": 1}}] + base,
            # run_if with unknown field
            [
                {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True, "x": 1}}
            ]
            + base,
            # run_if referencing a task
            [
                {"id": "a", "kind": "task", "depends_on": []},
                {"id": "b", "kind": "task", "depends_on": ["a"], "run_if": {"condition_id": "a", "expected": True}},
            ],
            # run_if referencing an unknown node
            [{"id": "t", "kind": "task", "depends_on": [], "run_if": {"condition_id": "ghost", "expected": True}}],
            # run_if condition not listed in depends_on
            [{"id": "t", "kind": "task", "depends_on": [], "run_if": {"condition_id": "c", "expected": True}}] + base,
        ]
        for index, nodes in enumerate(invalid_workflows):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-{index}", "nodes": nodes}, f"w-bad-{index}")


if __name__ == "__main__":
    unittest.main()
