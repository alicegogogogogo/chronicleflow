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


class LoopWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.workflow = {
            "id": "polling",
            "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "more", "kind": "condition", "depends_on": [], "path": "more", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["more"]},
                {
                    "id": "retry",
                    "kind": "loop",
                    "depends_on": ["prepare"],
                    "entry": "attempt",
                    "condition_id": "more",
                    "max_iterations": 3,
                },
                {"id": "finish", "kind": "task", "depends_on": ["retry"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, input_data, execution_id="run-1"):
        return self.service.create_execution({"id": execution_id, "workflow_id": "polling", "input": input_data}, f"e-{execution_id}")

    def test_loop_definition_is_round_tripped(self):
        document = dict(self.workflow, id="polling-copy")
        stored = self.service.create_workflow(document, "w-copy")
        self.assertEqual(document, stored)

    def test_condition_false_ends_with_zero_iterations_without_consuming_output(self):
        workflow = {
            "id": "terminal-loop",
            "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "more", "kind": "condition", "depends_on": [], "path": "more", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["more"]},
                {
                    "id": "retry",
                    "kind": "loop",
                    "depends_on": ["prepare"],
                    "entry": "attempt",
                    "condition_id": "more",
                    "max_iterations": 3,
                },
            ],
        }
        self.service.create_workflow(workflow, "w-terminal")
        self.service.create_execution({"id": "run-z", "workflow_id": "terminal-loop", "input": {"more": False}}, "e-z")
        self.service.advance("run-z", {"output": {"prepared": 1}}, "a1")
        state = self.service.advance("run-z", {"output": {"never": "used"}}, "a2")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"prepare": {"prepared": 1}}, state["outputs"])
        self.assertEqual(["prepare", "retry"], state["completed_nodes"])
        self.assertEqual({"retry": {"iteration": 0, "iterations": [], "end_reason": "condition_false"}}, state["loops"])
        event_types = [event["type"] for event in self.service.events("run-z")]
        self.assertEqual(["execution_started", "node_completed", "loop_judgment", "loop_ended", "execution_completed"], event_types)
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-z"))

    def test_iterations_run_to_limit_and_outputs_stay_per_iteration(self):
        self.start({"more": True})
        self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        self.service.advance("run-1", {"output": {"n": 1}}, "a2")
        self.service.advance("run-1", {"output": {"n": 2}}, "a3")
        self.service.advance("run-1", {"output": {"n": 3}}, "a4")
        state = self.service.advance("run-1", {"output": {"done": True}}, "a5")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["prepare", "retry", "finish"], state["completed_nodes"])
        self.assertEqual({"prepare": {"prepared": 1}, "finish": {"done": True}}, state["outputs"])
        loop = state["loops"]["retry"]
        self.assertEqual(3, loop["iteration"])
        self.assertEqual("iteration_limit", loop["end_reason"])
        self.assertEqual([1, 2, 3], [iteration["number"] for iteration in loop["iterations"]])
        for index, iteration in enumerate(loop["iterations"], start=1):
            self.assertEqual(["more", "attempt"], iteration["nodes"])
            self.assertEqual([], iteration["skipped"])
            self.assertEqual({"attempt": {"n": index}}, iteration["outputs"])
            self.assertEqual({"more": True}, iteration["conditions"])
        self.assertEqual({}, state["condition_results"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            [
                "execution_started",
                "node_completed",
                "loop_judgment",
                "loop_iteration_started",
                "condition_evaluated",
                "node_completed",
                "loop_judgment",
                "loop_iteration_started",
                "condition_evaluated",
                "node_completed",
                "loop_judgment",
                "loop_iteration_started",
                "condition_evaluated",
                "node_completed",
                "loop_ended",
                "node_completed",
                "execution_completed",
            ],
            event_types,
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))
        again = self.service.advance("run-1", {"output": {"extra": 1}}, "a6")
        self.assertEqual(state, again)
        self.assertEqual(17, len(self.service.events("run-1")))

    def test_guarded_body_task_is_skipped_each_iteration(self):
        workflow = {
            "id": "guarded",
            "nodes": [
                {"id": "judge", "kind": "condition", "depends_on": [], "path": "go", "equals": True},
                {"id": "flag", "kind": "condition", "depends_on": [], "path": "flag", "equals": True},
                {
                    "id": "extra",
                    "kind": "task",
                    "depends_on": ["flag"],
                    "run_if": {"condition_id": "flag", "expected": True},
                },
                {"id": "attempt", "kind": "task", "depends_on": ["judge", "extra"]},
                {"id": "loop", "kind": "loop", "depends_on": [], "entry": "attempt", "condition_id": "judge", "max_iterations": 2},
            ],
        }
        self.service.create_workflow(workflow, "w-guarded")
        self.service.create_execution({"id": "run-g", "workflow_id": "guarded", "input": {"go": True, "flag": False}}, "e-g")
        self.service.advance("run-g", {"output": {"n": 1}}, "a1")
        self.service.advance("run-g", {"output": {"n": 2}}, "a2")
        state = self.service.advance("run-g", {"output": {"never": "used"}}, "a3")
        self.assertEqual("completed", state["status"])
        loop = state["loops"]["loop"]
        self.assertEqual("iteration_limit", loop["end_reason"])
        self.assertEqual(2, len(loop["iterations"]))
        for index, iteration in enumerate(loop["iterations"], start=1):
            self.assertEqual(["flag", "judge", "extra", "attempt"], iteration["nodes"])
            self.assertEqual(["extra"], iteration["skipped"])
            self.assertEqual({"attempt": {"n": index}}, iteration["outputs"])
            self.assertEqual({"flag": False, "judge": True}, iteration["conditions"])
        self.assertEqual({}, state["outputs"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-g"))

    def test_restart_continues_unfinished_iterations(self):
        self.start({"more": True})
        self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        self.service.advance("run-1", {"output": {"n": 1}}, "a2")
        reopened = ChronicleFlow(self.database)
        state = reopened.advance("run-1", {"output": {"n": 2}}, "a3")
        self.assertEqual(2, state["loops"]["retry"]["iteration"])
        self.assertEqual({"attempt": {"n": 2}}, state["loops"]["retry"]["iterations"][1]["outputs"])

    def test_idempotent_advance_returns_original_result(self):
        self.start({"more": True})
        self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        first = self.service.advance("run-1", {"output": {"n": 1}}, "same")
        repeated = self.service.advance("run-1", {"output": {"n": 999}}, "same")
        self.assertEqual(first, repeated)
        self.assertEqual({"attempt": {"n": 1}}, first["loops"]["retry"]["iterations"][0]["outputs"])

    def test_invalid_loop_definitions_are_rejected(self):
        base = [
            {"id": "more", "kind": "condition", "depends_on": [], "path": "more", "equals": True},
            {"id": "attempt", "kind": "task", "depends_on": ["more"]},
            {"id": "retry", "kind": "loop", "depends_on": [], "entry": "attempt", "condition_id": "more", "max_iterations": 2},
        ]
        loop = base[2]
        invalid_workflows = [
            # unknown field on a loop
            [dict(loop, note="x")] + base[:2],
            # missing max_iterations
            [{key: value for key, value in loop.items() if key != "max_iterations"}] + base[:2],
            # entry not found
            [dict(loop, entry="ghost")] + base[:2],
            # entry is not a task
            [dict(loop, entry="more")] + base[:2],
            # judgment is not a condition
            [dict(loop, condition_id="attempt")] + base[:2],
            # judgment outside the body
            [dict(loop, condition_id="other")]
            + base[:2]
            + [{"id": "other", "kind": "condition", "depends_on": [], "path": "x", "equals": 1}],
            # zero iterations
            [dict(loop, max_iterations=0)] + base[:2],
            # negative iterations
            [dict(loop, max_iterations=-2)] + base[:2],
            # iterations above the limit
            [dict(loop, max_iterations=101)] + base[:2],
            # non-finite iterations
            [dict(loop, max_iterations=float("nan"))] + base[:2],
            [dict(loop, max_iterations=float("inf"))] + base[:2],
            # non-integer iterations
            [dict(loop, max_iterations=2.5)] + base[:2],
            [dict(loop, max_iterations="3")] + base[:2],
            [dict(loop, max_iterations=True)] + base[:2],
            # outside node depends on a body node
            base + [{"id": "spy", "kind": "task", "depends_on": ["attempt"]}],
            # loop depends on its own body node
            [dict(loop, depends_on=["attempt"])] + base[:2],
            # entry depends on the loop itself
            [dict(base[1], depends_on=["more", "retry"]), base[0], base[2]],
            # overlapping loop bodies
            base + [dict(loop, id="again")],
            # loop body contains another loop
            [
                {"id": "c2", "kind": "condition", "depends_on": [], "path": "m", "equals": 1},
                {"id": "t2", "kind": "task", "depends_on": ["c2"]},
                {"id": "inner", "kind": "loop", "depends_on": [], "entry": "t2", "condition_id": "c2", "max_iterations": 1},
                {"id": "more", "kind": "condition", "depends_on": [], "path": "more", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["inner", "more"]},
                dict(loop, depends_on=[]),
            ],
        ]
        for index, nodes in enumerate(invalid_workflows):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-loop-{index}", "nodes": nodes}, f"w-bad-loop-{index}")


if __name__ == "__main__":
    unittest.main()
