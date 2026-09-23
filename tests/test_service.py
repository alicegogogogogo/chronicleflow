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
        self.db_path = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.db_path)
        # The body is derived from the entry task: attempt runs first and the
        # continue condition "again" depends on it, so both belong to the
        # body even though no body list is declared.
        self.workflow = {
            "id": "retries",
            "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "attempt", "kind": "task", "depends_on": []},
                {"id": "again", "kind": "condition", "depends_on": ["attempt"], "path": "keep", "equals": True},
                {
                    "id": "retry",
                    "kind": "loop",
                    "depends_on": ["prepare"],
                    "entry": "attempt",
                    "condition_id": "again",
                    "max_iterations": 3,
                },
                {"id": "report", "kind": "task", "depends_on": ["retry"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, input_data, execution_id="run-1"):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": "retries", "input": input_data}, f"e-{execution_id}"
        )

    def advance(self, key_index, output=None):
        return self.service.advance(
            "run-1", {"output": output if output is not None else {"n": key_index}}, f"a{key_index}"
        )

    def test_false_entry_judgment_runs_zero_iterations(self):
        self.start({"keep": False})
        state = self.advance(1)
        # prepare completed; the loop auto-ended with zero iterations, leaving
        # report as the only remaining ready task, and the output is unused by
        # the loop.
        self.assertEqual("running", state["status"])
        loop = state["loops"]["retry"]
        self.assertEqual("condition_false", loop["reason"])
        self.assertEqual(0, loop["current_iteration"])
        self.assertEqual([], loop["iterations"])
        self.assertEqual(["prepare", "retry"], state["completed_nodes"])
        self.assertNotIn("attempt", state["outputs"])
        state = self.advance(2)
        self.assertEqual("completed", state["status"])
        # body nodes are encapsulated: only prepare, the loop, and report ran
        self.assertEqual(["prepare", "retry", "report"], state["completed_nodes"])
        self.assertEqual({"n": 1}, state["outputs"]["prepare"])
        self.assertEqual({"n": 2}, state["outputs"]["report"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            [
                "execution_started",
                "node_completed",
                "loop_started",
                "loop_condition_evaluated",
                "loop_completed",
                "node_completed",
                "execution_completed",
            ],
            event_types,
        )
        entry_event = self.service.events("run-1")[3]
        self.assertEqual("entry", entry_event["payload"]["phase"])
        self.assertFalse(entry_event["payload"]["result"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_loop_runs_to_iteration_limit_with_per_iteration_state(self):
        self.start({"keep": True})
        state = None
        for index in range(1, 6):
            state = self.advance(index)
            if state["status"] == "completed":
                break
        self.assertEqual("completed", state["status"])
        loop = state["loops"]["retry"]
        self.assertEqual("iteration_limit", loop["reason"])
        self.assertEqual(3, loop["current_iteration"])
        self.assertEqual([1, 2, 3], [entry["iteration"] for entry in loop["iterations"]])
        for entry in loop["iterations"]:
            self.assertEqual(["attempt", "again"], entry["completed_nodes"])
            self.assertEqual([], entry["skipped_nodes"])
            self.assertEqual({"again": True}, entry["condition_results"])
        self.assertEqual(["prepare", "retry", "report"], state["completed_nodes"])
        # body task outputs accumulate in round order
        self.assertEqual([{"n": 2}, {"n": 3}, {"n": 4}], state["outputs"]["attempt"])
        self.assertEqual({"n": 1}, state["outputs"]["prepare"])
        self.assertEqual({"n": 5}, state["outputs"]["report"])
        # one condition result per evaluation, including the between judgments
        self.assertEqual([True, True, True], state["condition_results"]["again"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(
            [
                "execution_started",
                "node_completed",
                "loop_started",
                "loop_condition_evaluated",
                "iteration_started",
                "node_completed",
                "condition_evaluated",
                "iteration_completed",
                "loop_condition_evaluated",
                "iteration_started",
                "node_completed",
                "condition_evaluated",
                "iteration_completed",
                "loop_condition_evaluated",
                "iteration_started",
                "node_completed",
                "condition_evaluated",
                "iteration_completed",
                "loop_condition_evaluated",
                "loop_completed",
                "node_completed",
                "execution_completed",
            ],
            event_types,
        )
        between = [
            event
            for event in self.service.events("run-1")
            if event["type"] == "loop_condition_evaluated"
        ]
        self.assertEqual(["entry", "between", "between", "between"], [event["payload"]["phase"] for event in between])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_run_if_branch_is_skipped_inside_each_iteration(self):
        workflow = {
            "id": "branched",
            "nodes": [
                {"id": "start", "kind": "task", "depends_on": []},
                {"id": "switch", "kind": "condition", "depends_on": ["start"], "path": "go", "equals": True},
                {"id": "do", "kind": "task", "depends_on": ["switch"],
                 "run_if": {"condition_id": "switch", "expected": True}},
                {"id": "skip_me", "kind": "task", "depends_on": ["switch"],
                 "run_if": {"condition_id": "switch", "expected": False}},
                {"id": "again", "kind": "condition", "depends_on": ["do"], "path": "go", "equals": True},
                {"id": "lp", "kind": "loop", "depends_on": [], "entry": "start",
                 "condition_id": "again", "max_iterations": 2},
            ],
        }
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "run-2", "workflow_id": "branched", "input": {"go": True}}, "e2")
        state = None
        for index in range(1, 6):
            state = self.service.advance("run-2", {"output": {"n": index}}, f"b{index}")
            if state["status"] == "completed":
                break
        self.assertEqual("completed", state["status"])
        self.assertEqual("iteration_limit", state["loops"]["lp"]["reason"])
        for entry in state["loops"]["lp"]["iterations"]:
            self.assertEqual(["start", "switch", "do", "again"], entry["completed_nodes"])
            self.assertEqual(["skip_me"], entry["skipped_nodes"])
        self.assertNotIn("skip_me", state["outputs"])
        self.assertEqual([{"n": 2}, {"n": 4}], state["outputs"]["do"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-2"))

    def test_global_ready_ordering_mixes_body_and_outer_tasks(self):
        # The loop (no dependencies, true entry) and outer task aaa are both
        # immediately available; aaa sorts before the body task bbb.
        workflow = {
            "id": "ordered",
            "nodes": [
                {"id": "bbb", "kind": "task", "depends_on": []},
                {"id": "cond", "kind": "condition", "depends_on": ["bbb"], "path": "go", "equals": True},
                {"id": "lp", "kind": "loop", "depends_on": [], "entry": "bbb",
                 "condition_id": "cond", "max_iterations": 1},
                {"id": "aaa", "kind": "task", "depends_on": []},
            ],
        }
        self.service.create_workflow(workflow, "w3")
        self.service.create_execution({"id": "run-3", "workflow_id": "ordered", "input": {"go": True}}, "e3")
        state = self.service.advance("run-3", {"output": {"who": "outer"}}, "c1")
        # the loop is already running but the outer task sorts first
        self.assertIn("aaa", state["completed_nodes"])
        self.assertEqual([], state["loops"]["lp"]["iterations"][0]["completed_nodes"])
        state = self.service.advance("run-3", {"output": {"who": "body"}}, "c2")
        self.assertEqual("completed", state["status"])
        self.assertEqual([{"who": "body"}], state["outputs"]["bbb"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-3"))

    def test_successor_is_not_ready_until_loop_completes(self):
        self.start({"keep": True})
        self.advance(1)  # prepare -> loop starts
        self.advance(2)  # first attempt
        state = self.service.get_execution("run-1")
        self.assertEqual("running", state["status"])
        self.assertNotIn("retry", state["completed_nodes"])
        self.assertNotIn("report", state["completed_nodes"])

    def test_execution_resumes_mid_loop_from_persisted_state(self):
        self.start({"keep": True})
        self.advance(1)
        state = self.advance(2)  # first iteration done, second is running
        self.assertEqual(2, state["loops"]["retry"]["current_iteration"])
        # simulate a process restart against the same database
        restarted = ChronicleFlow(self.db_path)
        for index in range(3, 6):
            state = restarted.advance("run-1", {"output": {"n": index}}, f"a{index}")
            if state["status"] == "completed":
                break
        self.assertEqual("completed", state["status"])
        self.assertEqual("iteration_limit", state["loops"]["retry"]["reason"])
        self.assertEqual([{"n": 2}, {"n": 3}, {"n": 4}], state["outputs"]["attempt"])
        self.assertEqual({"consistent": True, "execution": state}, restarted.replay("run-1"))

    def test_repeated_idempotency_key_mid_loop_returns_original_result(self):
        self.start({"keep": True})
        self.advance(1)
        first = self.service.advance("run-1", {"output": {"n": 2}}, "shared")
        # completing the first attempt auto-closes the round and starts the next
        self.assertEqual(2, first["loops"]["retry"]["current_iteration"])
        repeated = self.service.advance("run-1", {"output": {"n": 999}}, "shared")
        self.assertEqual(first, repeated)
        self.assertEqual(1, len(first["outputs"]["attempt"]))
        events = self.service.events("run-1")
        self.assertEqual(
            1,
            sum(
                1
                for event in events
                if event["type"] == "node_completed" and event["payload"].get("node_id") == "attempt"
            ),
        )

    def test_terminal_advance_returns_state_without_consuming_output(self):
        self.start({"keep": False})
        self.advance(1)  # prepare, then the loop auto-ends with zero iterations
        state = self.advance(2)  # report
        self.assertEqual("completed", state["status"])
        again = self.service.advance("run-1", {"output": {"extra": True}}, "later")
        self.assertEqual(state, again)
        event_count = len(self.service.events("run-1"))
        self.service.advance("run-1", {"output": {"extra": True}}, "later")
        self.assertEqual(event_count, len(self.service.events("run-1")))

    def test_loop_definition_is_round_tripped(self):
        document = dict(self.workflow, id="retries-copy")
        stored = self.service.create_workflow(document, "w-copy")
        self.assertEqual(document, stored)

    def test_invalid_loop_definitions_are_rejected(self):
        import copy

        template = self.workflow

        def loop_doc(mutate, doc_id):
            document = copy.deepcopy(template)
            document["id"] = doc_id
            mutate(document["nodes"][3])
            return document

        cases = [
            lambda loop: loop.update(entry="again"),            # entry is a condition
            lambda loop: loop.update(condition_id="attempt"),   # condition id is a task
            lambda loop: loop.update(entry="ghost"),            # unknown entry
            lambda loop: loop.update(condition_id="ghost"),     # unknown condition
            lambda loop: loop.update(max_iterations=0),
            lambda loop: loop.update(max_iterations=1001),
            lambda loop: loop.update(max_iterations="3"),
            lambda loop: loop.update(max_iterations=True),
            lambda loop: loop.update(unexpected=1),
            lambda loop: loop.pop("entry"),
        ]
        for index, mutate in enumerate(cases):
            with self.subTest(index=index):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow(loop_doc(mutate, f"bad-loop-{index}"), f"w-bad-loop-{index}")

    def test_condition_unreachable_from_entry_is_rejected(self):
        # The continue condition must belong to the body, i.e. be reachable
        # forward from the entry task.
        import copy

        document = copy.deepcopy(self.workflow)
        document["id"] = "unreachable"
        # "again" now hangs off prepare instead of attempt, so the entry
        # closure is just {attempt} and again is outside it.
        document["nodes"][2]["depends_on"] = ["prepare"]
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "wu")

    def test_loop_boundary_violations_are_rejected(self):
        import copy

        # body node depending on a node outside the body
        document = copy.deepcopy(self.workflow)
        document["id"] = "boundary-1"
        document["nodes"][1]["depends_on"] = ["prepare"]
        with self.assertRaisesRegex(ValidationError, "out-of-body"):
            self.service.create_workflow(document, "wb1")
        # overlapping loop bodies
        document = copy.deepcopy(self.workflow)
        document["id"] = "boundary-2"
        document["nodes"][4] = {
            "id": "report",
            "kind": "loop",
            "depends_on": ["retry"],
            "entry": "attempt",
            "condition_id": "again",
            "max_iterations": 1,
        }
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "wb2")
        # a second loop reached from the first body nests loops
        document = copy.deepcopy(self.workflow)
        document["id"] = "boundary-3"
        document["nodes"].append(
            {"id": "inner", "kind": "loop", "depends_on": ["attempt"], "entry": "attempt",
             "condition_id": "again", "max_iterations": 1}
        )
        with self.assertRaisesRegex(ValidationError, "must not contain another loop"):
            self.service.create_workflow(document, "wb3")
        # dependency cycle inside a body
        document = copy.deepcopy(self.workflow)
        document["id"] = "boundary-4"
        document["nodes"][1]["depends_on"] = ["again"]
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "wb4")


if __name__ == "__main__":
    unittest.main()
