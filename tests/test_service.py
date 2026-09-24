import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
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


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.workflow = {
            "id": "flaky",
            "nodes": [
                {"id": "fetch", "kind": "task", "depends_on": [], "retries": 2},
                {"id": "report", "kind": "task", "depends_on": ["fetch"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "flaky", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_failed_node_is_retried_and_event_stream_records_attempts(self):
        state = self.service.advance("run-1", {"failure": {"reason": "timeout"}}, "a1")
        self.assertEqual("running", state["status"])
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        self.assertEqual([], state["completed_nodes"])
        state = self.service.advance("run-1", {"output": {"page": 1}}, "a2")
        self.assertEqual(["fetch"], state["completed_nodes"])
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(["execution_started", "node_failed", "node_retried", "node_completed"], event_types)
        failed = self.service.events("run-1")[1]
        self.assertEqual({"node_id": "fetch", "attempt": 1, "reason": "timeout"}, failed["payload"])
        retried = self.service.events("run-1")[2]
        self.assertEqual({"node_id": "fetch", "attempt": 2, "reason": "timeout"}, retried["payload"])

    def test_retries_exhausted_terminates_execution(self):
        self.service.advance("run-1", {"failure": {"reason": "boom-1"}}, "a1")
        self.service.advance("run-1", {"failure": {"reason": "boom-2"}}, "a2")
        state = self.service.advance("run-1", {"failure": {"reason": "boom-3"}}, "a3")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fetch"], state["failed_nodes"])
        self.assertEqual({"fetch": {"attempt": 3, "failures": 3}}, state["attempts"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            [
                "execution_started",
                "node_failed",
                "node_retried",
                "node_failed",
                "node_retried",
                "node_failed",
                "execution_terminated",
            ],
            event_types,
        )
        terminated = self.service.events("run-1")[-1]
        self.assertEqual({"reason": "retries_exhausted", "node_id": "fetch"}, terminated["payload"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_terminated_execution_does_not_absorb_output(self):
        self.service.advance("run-1", {"failure": {"reason": "x"}}, "a1")
        self.service.advance("run-1", {"failure": {"reason": "x"}}, "a2")
        state = self.service.advance("run-1", {"failure": {"reason": "x"}}, "a3")
        again = self.service.advance("run-1", {"output": {"late": True}}, "a4")
        self.assertEqual(state, again)
        self.assertEqual({}, again["outputs"])
        self.assertEqual(7, len(self.service.events("run-1")))

    def test_restart_continues_unfinished_retries(self):
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a1")
        resumed = ChronicleFlow(self.database)
        state = resumed.advance("run-1", {"output": {"page": 1}}, "a2")
        self.assertEqual(["fetch"], state["completed_nodes"])
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        state = resumed.advance("run-1", {"output": {"done": True}}, "a3")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-1"))

    def test_zero_retries_is_the_default_and_fails_permanently(self):
        workflow = {
            "id": "once",
            "nodes": [{"id": "only", "kind": "task", "depends_on": []}],
        }
        self.service.create_workflow(workflow, "w-once")
        self.service.create_execution({"id": "run-once", "workflow_id": "once", "input": {}}, "e-once")
        state = self.service.advance("run-once", {"failure": {"reason": "nope"}}, "a-once")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["only"], state["failed_nodes"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-once"))

    def test_retry_definition_is_round_tripped(self):
        document = dict(self.workflow, id="flaky-copy")
        stored = self.service.create_workflow(document, "w-copy")
        self.assertEqual(document, stored)

    def test_invalid_retries_are_rejected(self):
        for index, retries in enumerate((-1, 11, 1.5, "2", True)):
            with self.subTest(retries=retries):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow(
                        {"id": f"bad-retry-{index}", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": retries}]},
                        f"w-bad-retry-{index}",
                    )

    def test_invalid_failure_bodies_are_rejected(self):
        for body in ({"failure": {"note": "x"}}, {"failure": "x"}, {"failure": {"reason": 1}}, {"output": {}, "failure": {"reason": "x"}}):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.advance("run-1", body, "a-bad")


class TimeoutTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {"id": "slow", "nodes": [{"id": "work", "kind": "task", "depends_on": []}]},
            "w1",
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_timeout_terminates_execution_and_rejects_output(self):
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "slow", "input": {}, "timeout_seconds": 0.05},
            "e1",
        )
        time.sleep(0.1)
        state = self.service.advance("run-1", {"output": {"late": True}}, "a1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("timeout", state["termination_reason"])
        self.assertEqual({}, state["outputs"])
        self.assertEqual([], state["completed_nodes"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(["execution_started", "execution_terminated"], event_types)
        self.assertEqual({"reason": "timeout"}, self.service.events("run-1")[-1]["payload"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_execution_without_timeout_is_unaffected(self):
        created = self.service.create_execution({"id": "run-2", "workflow_id": "slow", "input": {}}, "e2")
        self.assertIsNone(created["timeout_seconds"])
        self.assertIsNone(created["deadline_at"])
        state = self.service.advance("run-2", {"output": {"ok": 1}}, "a2")
        self.assertEqual("completed", state["status"])
        self.assertIsNone(state["termination_reason"])

    def test_invalid_timeouts_are_rejected(self):
        for index, timeout in enumerate((0, -1, -0.5, "5", True)):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValidationError):
                    self.service.create_execution(
                        {"id": f"run-bad-{index}", "workflow_id": "slow", "input": {}, "timeout_seconds": timeout},
                        f"e-bad-{index}",
                    )


class CancelTests(unittest.TestCase):
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

    def test_cancel_terminates_running_execution(self):
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("cancelled", state["termination_reason"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(["execution_started", "execution_terminated"], event_types)
        self.assertEqual({"reason": "cancelled"}, self.service.events("run-1")[-1]["payload"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))
        again = self.service.advance("run-1", {"output": {"late": 1}}, "a1")
        self.assertEqual(state, again)
        self.assertEqual({}, again["outputs"])

    def test_cancel_is_idempotent_and_returns_same_state_for_terminated(self):
        first = self.service.cancel("run-1", "c1")
        repeated = self.service.cancel("run-1", "c1")
        self.assertEqual(first, repeated)
        other_key = self.service.cancel("run-1", "c2")
        self.assertEqual(first, other_key)
        self.assertEqual(2, len(self.service.events("run-1")))

    def test_cancel_completed_execution_returns_it_unchanged(self):
        self.service.advance("run-1", {"output": {"ok": 1}}, "a1")
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("completed", state["status"])
        self.assertIsNone(state["termination_reason"])
        self.assertEqual("node_completed", self.service.events("run-1")[-2]["type"])
        self.assertEqual("execution_completed", self.service.events("run-1")[-1]["type"])

    def test_cancel_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.cancel("ghost", "c-ghost")

    def test_cancel_key_cannot_be_reused_for_another_operation(self):
        self.service.cancel("run-1", "shared-cancel")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "shared-cancel")


class LoopRetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.workflow = {
            "id": "loop-retry",
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
        self.service.create_workflow(self.workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "loop-retry", "input": {"again": True}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_failure_inside_loop_retries_within_iteration(self):
        state = self.service.advance("run-1", {"failure": {"reason": "flaky"}}, "a1")
        self.assertEqual("running", state["status"])
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual({"attempt": {"attempt": 2, "failures": 1}}, iteration["attempts"])
        self.assertNotIn("attempt", iteration["completed_nodes"])
        state = self.service.advance("run-1", {"output": {"try": 1}}, "a2")
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual({"attempt": {"try": 1}}, iteration["outputs"])
        failed = next(event for event in self.service.events("run-1") if event["type"] == "node_failed")
        self.assertEqual({"node_id": "attempt", "attempt": 1, "reason": "flaky", "loop_id": "loop", "iteration": 1}, failed["payload"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_exhausted_retries_inside_loop_terminate_execution(self):
        workflow = {
            "id": "loop-fatal",
            "nodes": [
                {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["check"]},
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
        self.service.create_workflow(workflow, "w-fatal")
        self.service.create_execution({"id": "run-fatal", "workflow_id": "loop-fatal", "input": {"again": True}}, "e-fatal")
        state = self.service.advance("run-fatal", {"failure": {"reason": "dead"}}, "a-fatal")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["attempt"], state["failed_nodes"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-fatal"))


class CheckpointRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.workflow = {
            "id": "orders",
            "nodes": [
                {"id": "reserve", "kind": "task", "depends_on": []},
                {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {"order": 7}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def event_payloads(self, service, execution_id="run-1"):
        return [(event["type"], event["payload"]) for event in service.events(execution_id)]

    def test_checkpoint_is_written_at_each_node_boundary_and_matches_state(self):
        first = self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        second = self.service.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(2, len(checkpoints))
        self.assertEqual([1, 2], [checkpoint["sequence"] for checkpoint in checkpoints])
        # execution_started then the first node completion; the final completion
        # adds node_completed plus execution_completed
        self.assertEqual(2, checkpoints[0]["event_sequence"])
        self.assertEqual(4, checkpoints[1]["event_sequence"])
        self.assertEqual(first, checkpoints[0]["state"])
        self.assertEqual(second, checkpoints[1]["state"])
        self.assertIn("created_at", checkpoints[0])

    def test_recover_rebuilds_materialized_state_without_new_events(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        state = self.service.get_execution("run-1")
        events_before = self.service.events("run-1")
        rebuilt = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(state, rebuilt)
        self.assertEqual(events_before, self.service.events("run-1"))
        self.assertEqual({"consistent": True, "execution": rebuilt}, self.service.replay("run-1"))

    def test_recover_is_idempotent_with_the_same_key(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        first = self.service.recover("run-1", {"from": "latest_checkpoint"}, "same")
        repeated = self.service.recover("run-1", {"from": "latest_checkpoint"}, "same")
        self.assertEqual(first, repeated)

    def test_restart_recover_and_continue_matches_uninterrupted_run(self):
        uninterrupted_db = str(Path(self.directory.name) / "plain.db")
        uninterrupted = ChronicleFlow(uninterrupted_db)
        uninterrupted.create_workflow(self.workflow, "w1")
        uninterrupted.create_execution({"id": "run-1", "workflow_id": "orders", "input": {"order": 7}}, "e1")
        uninterrupted.advance("run-1", {"output": {"reservation": 9}}, "a1")
        final_uninterrupted = uninterrupted.advance("run-1", {"output": {"charge": "ok"}}, "a2")

        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        resumed = ChronicleFlow(self.database)
        rebuilt = resumed.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(["reserve"], rebuilt["completed_nodes"])
        continued = resumed.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        self.assertEqual(final_uninterrupted, continued)
        self.assertEqual(self.event_payloads(uninterrupted), self.event_payloads(resumed))
        self.assertEqual({"consistent": True, "execution": continued}, resumed.replay("run-1"))

    def test_restart_recover_continues_unfinished_retries_without_duplicate_output(self):
        workflow = {
            "id": "flaky",
            "nodes": [
                {"id": "fetch", "kind": "task", "depends_on": [], "retries": 2},
                {"id": "report", "kind": "task", "depends_on": ["fetch"]},
            ],
        }
        self.service.create_workflow(workflow, "w-flaky")
        self.service.create_execution({"id": "run-flaky", "workflow_id": "flaky", "input": {}}, "e-flaky")
        state = self.service.advance("run-flaky", {"failure": {"reason": "boom"}}, "af1")
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        checkpoint = self.service.checkpoints("run-flaky")["checkpoints"][-1]
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, checkpoint["state"]["attempts"])
        resumed = ChronicleFlow(self.database)
        resumed.recover("run-flaky", {"from": "latest_checkpoint"}, "rf1")
        state = resumed.advance("run-flaky", {"output": {"page": 1}}, "af2")
        self.assertEqual({"fetch": {"attempt": 2, "failures": 1}}, state["attempts"])
        state = resumed.advance("run-flaky", {"output": {"done": True}}, "af3")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"fetch": {"page": 1}, "report": {"done": True}}, state["outputs"])
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-flaky"))
        types = [event[0] for event in self.event_payloads(resumed, "run-flaky")]
        self.assertEqual(
            ["execution_started", "node_failed", "node_retried", "node_completed", "node_completed", "execution_completed"],
            types,
        )

    def test_loop_failure_checkpoint_and_recovery_keeps_iteration_identity(self):
        workflow = {
            "id": "loop-retry",
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
        self.service.create_workflow(workflow, "w-loop")
        self.service.create_execution({"id": "run-loop", "workflow_id": "loop-retry", "input": {"again": True}}, "e-loop")
        state = self.service.advance("run-loop", {"failure": {"reason": "flaky"}}, "al1")
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual({"attempt": {"attempt": 2, "failures": 1}}, iteration["attempts"])
        resumed = ChronicleFlow(self.database)
        rebuilt = resumed.recover("run-loop", {"from": "latest_checkpoint"}, "rl1")
        self.assertEqual(
            {"attempt": {"attempt": 2, "failures": 1}},
            rebuilt["loops"]["loop"]["iterations"][0]["attempts"],
        )
        state = resumed.advance("run-loop", {"output": {"try": 1}}, "al2")
        failed = next(event for event in resumed.events("run-loop") if event["type"] == "node_failed")
        self.assertEqual(
            {"node_id": "attempt", "attempt": 1, "reason": "flaky", "loop_id": "loop", "iteration": 1},
            failed["payload"],
        )
        self.assertEqual({"consistent": True, "execution": state}, resumed.replay("run-loop"))

    def test_completed_execution_recovers_unchanged_without_events(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        completed = self.service.advance("run-1", {"output": {"charge": "ok"}}, "a2")
        events_before = self.service.events("run-1")
        recovered = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(completed, recovered)
        self.assertEqual(events_before, self.service.events("run-1"))

    def test_terminated_execution_recovers_unchanged(self):
        self.service.cancel("run-1", "c1")
        state = self.service.get_execution("run-1")
        recovered = self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual(state, recovered)
        self.assertEqual("cancelled", recovered["termination_reason"])

    def test_timeout_takes_precedence_over_recovery(self):
        self.service.create_execution(
            {"id": "run-timeout", "workflow_id": "orders", "input": {}, "timeout_seconds": 0.05},
            "e-timeout",
        )
        self.service.advance("run-timeout", {"output": {"reservation": 9}}, "a-timeout")
        # the checkpoint captured a running execution before the deadline
        self.assertEqual("running", self.service.checkpoints("run-timeout")["checkpoints"][-1]["state"]["status"])
        time.sleep(0.1)
        recovered = self.service.recover("run-timeout", {"from": "latest_checkpoint"}, "r-timeout")
        self.assertEqual("terminated", recovered["status"])
        self.assertEqual("timeout", recovered["termination_reason"])
        # recovery absorbs no input: the pre-deadline boundary is all that exists
        self.assertEqual(["reserve"], recovered["completed_nodes"])
        self.assertEqual({"reserve": {"reservation": 9}}, recovered["outputs"])

    def test_events_and_checkpoints_remain_queryable_after_cancellation(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.cancel("run-1", "c1")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(1, len(checkpoints))
        self.assertEqual(["reserve"], checkpoints[-1]["state"]["completed_nodes"])
        self.assertEqual("cancelled", self.service.events("run-1")[-1]["payload"]["reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_advancing_terminated_execution_writes_no_checkpoint(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.cancel("run-1", "c1")
        self.service.advance("run-1", {"output": {"late": True}}, "a2")
        self.assertEqual(1, len(self.service.checkpoints("run-1")["checkpoints"]))

    def test_recover_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.recover("ghost", {"from": "latest_checkpoint"}, "r-ghost")

    def test_recover_without_checkpoint_conflicts(self):
        with self.assertRaisesRegex(ConflictError, "no checkpoint"):
            self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")

    def test_unparseable_checkpoint_conflicts(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.store.connection.execute(
            "UPDATE checkpoints SET document = ? WHERE execution_id = ?",
            ("{not valid json", "run-1"),
        )
        with self.assertRaisesRegex(ConflictError, "parseable"):
            self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")

    def test_malformed_checkpoint_document_conflicts(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.store.connection.execute(
            "UPDATE checkpoints SET document = ? WHERE execution_id = ?",
            ('{"event_sequence": 1}', "run-1"),
        )
        with self.assertRaises(ConflictError):
            self.service.recover("run-1", {"from": "latest_checkpoint"}, "r1")

    def test_checkpoints_preserve_float_precision_and_negative_zero(self):
        workflow = {
            "id": "floats",
            "nodes": [{"id": "only", "kind": "task", "depends_on": []}],
        }
        self.service.create_workflow(workflow, "w-floats")
        self.service.create_execution(
            {"id": "run-floats", "workflow_id": "floats", "input": {"value": -0.0}},
            "e-floats",
        )
        self.service.advance("run-floats", {"output": {"precise": 0.30000000000000004}}, "af")
        recovered = self.service.recover("run-floats", {"from": "latest_checkpoint"}, "rf")
        self.assertEqual(-0.0, recovered["input"]["value"])
        self.assertTrue(str(recovered["input"]["value"]).startswith("-"))
        self.assertEqual(0.30000000000000004, recovered["outputs"]["only"]["precise"])

    def test_invalid_recover_bodies_are_rejected(self):
        for body in ({}, {"from": 5}, {"from": "elsewhere"}, {"from": "latest_checkpoint", "extra": 1}, ["latest_checkpoint"]):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.recover("run-1", body, "r-bad")

    def test_recover_key_cannot_be_reused_for_another_operation(self):
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a1")
        self.service.advance("run-1", {"output": {"charge": "ok"}}, "shared")
        with self.assertRaises(ConflictError):
            self.service.recover("run-1", {"from": "latest_checkpoint"}, "shared")

    def test_checkpoints_for_missing_execution_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.checkpoints("ghost")


if __name__ == "__main__":
    unittest.main()