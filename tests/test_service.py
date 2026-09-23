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
            "id": "retry",
            "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["check"]},
                {
                    "id": "retry_loop",
                    "kind": "loop",
                    "depends_on": ["prepare"],
                    "entry": "attempt",
                    "condition": "check",
                    "max_iterations": 2,
                },
                {"id": "finalize", "kind": "task", "depends_on": ["retry_loop"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, input_data, execution_id="run-1"):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": "retry", "input": input_data}, f"e-{execution_id}"
        )

    def test_loop_runs_to_iteration_limit_and_replays(self):
        self.start({"again": True})
        first = self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        self.assertEqual(["prepare"], first["completed_nodes"])
        self.assertEqual("running", first["loops"]["retry_loop"]["status"])
        self.assertEqual(1, first["loops"]["retry_loop"]["current_iteration"])
        second = self.service.advance("run-1", {"output": {"try": 1}}, "a2")
        self.assertEqual(2, second["loops"]["retry_loop"]["current_iteration"])
        third = self.service.advance("run-1", {"output": {"try": 2}}, "a3")
        loop = third["loops"]["retry_loop"]
        self.assertEqual("completed", loop["status"])
        self.assertEqual("iteration_limit", loop["end_reason"])
        self.assertEqual(2, len(loop["iterations"]))
        for iteration, expected in zip(loop["iterations"], ({"try": 1}, {"try": 2})):
            self.assertEqual(["check", "attempt"], iteration["completed_nodes"])
            self.assertEqual({"check": True}, iteration["condition_results"])
            self.assertEqual({"attempt": expected}, iteration["outputs"])
        # body outputs belong to their iterations, not the execution outputs
        self.assertEqual({"prepare": {"prepared": 1}}, third["outputs"])
        self.assertEqual(["prepare", "retry_loop"], third["completed_nodes"])
        final = self.service.advance("run-1", {"output": {"done": True}}, "a4")
        self.assertEqual("completed", final["status"])
        self.assertEqual(["prepare", "retry_loop", "finalize"], final["completed_nodes"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            [
                "execution_started",
                "node_completed",
                "loop_condition_evaluated",
                "iteration_started",
                "condition_evaluated",
                "node_completed",
                "loop_condition_evaluated",
                "iteration_started",
                "condition_evaluated",
                "node_completed",
                "loop_condition_evaluated",
                "loop_completed",
                "node_completed",
                "execution_completed",
            ],
            event_types,
        )
        self.assertEqual({"consistent": True, "execution": final}, self.service.replay("run-1"))

    def test_false_start_condition_completes_zero_iterations(self):
        self.start({"again": False})
        self.service.advance("run-1", {"output": {}}, "a1")
        state = self.service.advance("run-1", {"output": {"done": True}}, "a2")
        self.assertEqual("completed", state["status"])
        loop = state["loops"]["retry_loop"]
        self.assertEqual("completed", loop["status"])
        self.assertEqual("condition_false", loop["end_reason"])
        self.assertEqual(0, loop["current_iteration"])
        self.assertEqual([], loop["iterations"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_false_start_condition_does_not_consume_output(self):
        workflow = {
            "id": "loop-only",
            "nodes": [
                {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["check"]},
                {
                    "id": "loop",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "attempt",
                    "condition": "check",
                    "max_iterations": 3,
                },
            ],
        }
        self.service.create_workflow(workflow, "w-loop-only")
        self.service.create_execution({"id": "run-x", "workflow_id": "loop-only", "input": {"again": False}}, "e-x")
        state = self.service.advance("run-x", {"output": {"never": "used"}}, "a-x")
        self.assertEqual("completed", state["status"])
        self.assertEqual({}, state["outputs"])
        self.assertEqual("condition_false", state["loops"]["loop"]["end_reason"])
        again = self.service.advance("run-x", {"output": {"extra": 1}}, "a-x2")
        self.assertEqual(state, again)

    def test_run_if_skip_inside_iteration_satisfies_body(self):
        workflow = {
            "id": "guarded",
            "nodes": [
                {"id": "check", "kind": "condition", "depends_on": [], "path": "flag", "equals": True},
                {
                    "id": "maybe",
                    "kind": "task",
                    "depends_on": ["check"],
                    "run_if": {"condition_id": "check", "expected": False},
                },
                {"id": "act", "kind": "task", "depends_on": ["maybe"]},
                {
                    "id": "loop",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "act",
                    "condition": "check",
                    "max_iterations": 1,
                },
            ],
        }
        self.service.create_workflow(workflow, "w-guarded")
        self.service.create_execution({"id": "run-g", "workflow_id": "guarded", "input": {"flag": True}}, "e-g")
        state = self.service.advance("run-g", {"output": {"acted": 1}}, "a-g")
        self.assertEqual("completed", state["status"])
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual(["maybe"], iteration["skipped_nodes"])
        self.assertEqual(["act", "check"], sorted(iteration["completed_nodes"]))
        self.assertEqual("iteration_limit", state["loops"]["loop"]["end_reason"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-g"))

    def test_restart_continues_unfinished_iterations(self):
        self.start({"again": True})
        self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        self.service.advance("run-1", {"output": {"try": 1}}, "a2")
        events_before = len(self.service.events("run-1"))
        resumed = ChronicleFlow(self.database)
        resumed.advance("run-1", {"output": {"try": 2}}, "a3")
        state = resumed.advance("run-1", {"output": {"done": True}}, "a4")
        self.assertEqual("completed", state["status"])
        self.assertEqual("iteration_limit", state["loops"]["retry_loop"]["end_reason"])
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-1"))
        self.assertEqual(14, len(self.service.events("run-1")))
        self.assertGreater(events_before, 0)

    def test_idempotent_advance_during_loop(self):
        self.start({"again": True})
        self.service.advance("run-1", {"output": {"prepared": 1}}, "a1")
        first = self.service.advance("run-1", {"output": {"try": 1}}, "same")
        repeated = self.service.advance("run-1", {"output": {"try": 999}}, "same")
        self.assertEqual(first, repeated)
        self.assertEqual(2, self.service.get_execution("run-1")["loops"]["retry_loop"]["current_iteration"])

    def test_loop_definition_is_round_tripped(self):
        document = dict(self.workflow, id="retry-copy")
        stored = self.service.create_workflow(document, "w-copy")
        self.assertEqual(document, stored)

    def test_invalid_loops_are_rejected(self):
        check = {"id": "check", "kind": "condition", "depends_on": [], "path": "a", "equals": True}
        attempt = {"id": "attempt", "kind": "task", "depends_on": ["check"]}

        def loop(**overrides):
            node = {"id": "loop", "kind": "loop", "depends_on": [], "entry": "attempt", "condition": "check", "max_iterations": 2}
            node.update(overrides)
            return node

        invalid = [
            # zero iterations
            [check, attempt, loop(max_iterations=0)],
            # negative iterations
            [check, attempt, loop(max_iterations=-1)],
            # over the limit
            [check, attempt, loop(max_iterations=101)],
            # non-integer iterations
            [check, attempt, loop(max_iterations="2")],
            [check, attempt, loop(max_iterations=True)],
            # unknown field
            [check, attempt, loop(path="a")],
            # missing entry
            [check, attempt, {"id": "loop", "kind": "loop", "depends_on": [], "condition": "check", "max_iterations": 2}],
            # entry references an unknown node
            [check, attempt, loop(entry="ghost")],
            # entry is not a task
            [check, attempt, loop(entry="check")],
            # condition references an unknown node
            [check, attempt, loop(condition="ghost")],
            # condition is not a condition node
            [check, attempt, loop(condition="attempt")],
            # condition outside the body
            [
                check,
                attempt,
                {"id": "other", "kind": "condition", "depends_on": [], "path": "b", "equals": 1},
                loop(condition="other"),
            ],
            # entry depends back on the loop
            [
                check,
                {"id": "attempt", "kind": "task", "depends_on": ["check", "loop"]},
                loop(),
            ],
            # loop depends on a body member
            [check, attempt, loop(depends_on=["check"])],
            # outside node depends on a body member
            [check, attempt, loop(), {"id": "tail", "kind": "task", "depends_on": ["attempt"]}],
            # nested loop inside the body
            [
                check,
                attempt,
                loop(),
                {"id": "inner_attempt", "kind": "task", "depends_on": ["loop"]},
                {
                    "id": "inner",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "inner_attempt",
                    "condition": "check",
                    "max_iterations": 1,
                },
            ],
            # overlapping loop bodies
            [
                check,
                attempt,
                loop(),
                {
                    "id": "second",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "attempt",
                    "condition": "check",
                    "max_iterations": 1,
                },
            ],
        ]
        for index, nodes in enumerate(invalid):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow({"id": f"bad-loop-{index}", "nodes": nodes}, f"w-bad-loop-{index}")


if __name__ == "__main__":
    unittest.main()
