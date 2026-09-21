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


def _branch_workflow():
    return {
        "id": "orders",
        "nodes": [
            {"id": "is-vip", "kind": "condition", "depends_on": [], "path": "customer.tier", "equals": "vip"},
            {"id": "gift", "kind": "task", "depends_on": ["is-vip"], "run_if": {"condition_id": "is-vip", "expected": True}},
            {"id": "receipt", "kind": "task", "depends_on": ["gift"]},
        ],
    }


class ConditionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(_branch_workflow(), "w1")

    def tearDown(self):
        self.directory.cleanup()

    def _event_types(self, execution_id):
        return [event["type"] for event in self.service.events(execution_id)]

    def _event_payloads(self, execution_id, event_type):
        return [event["payload"] for event in self.service.events(execution_id) if event["type"] == event_type]

    def test_matching_condition_runs_guarded_task(self):
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"customer": {"tier": "vip"}}}, "e1"
        )
        state = self.service.advance("run-1", {"output": {"gift": "wine"}}, "a1")
        self.assertEqual({"is-vip": True}, state["condition_results"])
        self.assertEqual(["is-vip", "gift"], state["completed_nodes"])
        self.assertEqual([], state["skipped_nodes"])
        state = self.service.advance("run-1", {"output": {"receipt": 1}}, "a2")
        self.assertEqual(["is-vip", "gift", "receipt"], state["completed_nodes"])
        self.assertEqual("completed", state["status"])
        self.assertEqual(
            [
                {"node_id": "is-vip", "result": True},
            ],
            self._event_payloads("run-1", "condition_evaluated"),
        )
        self.assertEqual([], self._event_payloads("run-1", "node_skipped"))
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_non_matching_condition_skips_task_and_unblocks_successor(self):
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {"customer": {"tier": "standard"}}}, "e1"
        )
        # First advance: condition evaluates false, gift is skipped, receipt is
        # the lexicographically first (only) executable task and consumes output.
        state = self.service.advance("run-1", {"output": {"receipt": 1}}, "a1")
        self.assertEqual({"is-vip": False}, state["condition_results"])
        self.assertEqual(["gift"], state["skipped_nodes"])
        self.assertEqual(["is-vip", "receipt"], state["completed_nodes"])
        self.assertNotIn("gift", state["outputs"])
        self.assertEqual("completed", state["status"])
        self.assertEqual(
            ["execution_started", "condition_evaluated", "node_skipped", "node_completed", "execution_completed"],
            self._event_types("run-1"),
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_missing_path_evaluates_false(self):
        workflow = {
            "id": "w",
            "nodes": [
                {"id": "c", "kind": "condition", "depends_on": [], "path": "a.b", "equals": 1},
                {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True}},
                {"id": "after", "kind": "task", "depends_on": ["t"]},
            ],
        }
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "r", "workflow_id": "w", "input": {"a": {}}}, "e2")
        state = self.service.advance("r", {"output": {}}, "a1")
        self.assertFalse(state["condition_results"]["c"])
        self.assertEqual(["t"], state["skipped_nodes"])
        # traversing a non-object also counts as a missing path
        self.service.create_execution({"id": "r2", "workflow_id": "w", "input": {"a": 5}}, "e3")
        state = self.service.advance("r2", {"output": {}}, "a2")
        self.assertFalse(state["condition_results"]["c"])

    def test_skip_chain_auto_completes_without_consuming_output(self):
        workflow = {
            "id": "w",
            "nodes": [
                {"id": "c", "kind": "condition", "depends_on": [], "path": "flag", "equals": True},
                {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True}},
            ],
        }
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "r", "workflow_id": "w", "input": {"flag": False}}, "e2")
        state = self.service.advance("r", {"output": {"ignored": True}}, "a1")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["c"], state["completed_nodes"])
        self.assertEqual(["t"], state["skipped_nodes"])
        self.assertEqual({}, state["outputs"])
        self.assertEqual(
            ["execution_started", "condition_evaluated", "node_skipped", "execution_completed"],
            self._event_types("r"),
        )
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("r"))
        # execution has ended: further advance is a conflict, not a silent no-op
        with self.assertRaises(ConflictError):
            self.service.advance("r", {"output": {}}, "a2")

    def test_chained_conditions_evaluate_in_dependency_order(self):
        workflow = {
            "id": "w",
            "nodes": [
                {"id": "c1", "kind": "condition", "depends_on": [], "path": "a", "equals": "x"},
                {"id": "c2", "kind": "condition", "depends_on": ["c1"], "path": "b", "equals": 2},
                {"id": "t", "kind": "task", "depends_on": ["c2"], "run_if": {"condition_id": "c2", "expected": True}},
            ],
        }
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "r", "workflow_id": "w", "input": {"a": "x", "b": 2}}, "e2")
        state = self.service.advance("r", {"output": {"ok": True}}, "a1")
        self.assertEqual({"c1": True, "c2": True}, state["condition_results"])
        self.assertEqual(["c1", "c2", "t"], state["completed_nodes"])
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("r"))

    def test_json_type_strict_comparison(self):
        cases = [
            ("s", "1", 1, False),
            ("n", 1, "1", False),
            ("b", True, 1, False),
            ("n2", 1, True, False),
            ("null", None, False, False),
            ("match-s", "vip", "vip", True),
            ("match-n", 3, 3, True),
            ("match-b", False, False, True),
            ("match-null", None, None, True),
        ]
        for label, value, expected, result in cases:
            workflow = {
                "id": f"w-{label}",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "v", "equals": expected},
                    {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True}},
                    {"id": "after", "kind": "task", "depends_on": ["t"]},
                ],
            }
            self.service.create_workflow(workflow, f"wf-{label}")
            self.service.create_execution({"id": f"r-{label}", "workflow_id": f"w-{label}", "input": {"v": value}}, f"e-{label}")
            state = self.service.advance(f"r-{label}", {"output": {}}, f"a-{label}")
            self.assertEqual(result, state["condition_results"]["c"], label)
            self.assertEqual([] if result else ["t"], state["skipped_nodes"], label)

    def test_task_without_run_if_runs_regardless_of_condition(self):
        workflow = {
            "id": "w",
            "nodes": [
                {"id": "c", "kind": "condition", "depends_on": [], "path": "flag", "equals": True},
                {"id": "guarded", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True}},
                {"id": "always", "kind": "task", "depends_on": ["guarded"]},
            ],
        }
        self.service.create_workflow(workflow, "w2")
        self.service.create_execution({"id": "r", "workflow_id": "w", "input": {"flag": False}}, "e2")
        state = self.service.advance("r", {"output": {"done": True}}, "a1")
        self.assertEqual(["guarded"], state["skipped_nodes"])
        self.assertEqual(["c", "always"], state["completed_nodes"])
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("r"))


class ConditionValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _reject(self, workflow, match):
        with self.assertRaisesRegex(ValidationError, match):
            self.service.create_workflow(workflow, "w1")

    def test_condition_requires_path_and_equals(self):
        self._reject(
            {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": []}]},
            "path",
        )
        self._reject(
            {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "a"}]},
            "equals",
        )

    def test_path_must_be_non_empty_dot_separated(self):
        for bad in ("", ".a", "a.", "a..b"):
            self._reject(
                {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": bad, "equals": 1}]},
                "path",
            )

    def test_equals_must_be_scalar(self):
        self._reject(
            {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": [1]}]},
            "equals",
        )
        self._reject(
            {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": {"x": 1}}]},
            "equals",
        )

    def test_unknown_fields_are_rejected(self):
        self._reject(
            {"id": "w", "nodes": [{"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": 1, "extra": 2}]},
            "unknown",
        )
        self._reject(
            {"id": "w", "nodes": [{"id": "t", "kind": "task", "depends_on": [], "path": "a"}]},
            "unknown",
        )

    def test_run_if_must_reference_condition(self):
        self._reject(
            {
                "id": "w",
                "nodes": [
                    {"id": "t1", "kind": "task", "depends_on": []},
                    {"id": "t2", "kind": "task", "depends_on": ["t1"], "run_if": {"condition_id": "t1", "expected": True}},
                ],
            },
            "condition node",
        )

    def test_run_if_unknown_reference(self):
        self._reject(
            {
                "id": "w",
                "nodes": [
                    {"id": "t", "kind": "task", "depends_on": [], "run_if": {"condition_id": "ghost", "expected": True}},
                ],
            },
            "unknown condition",
        )

    def test_run_if_condition_must_be_dependency(self):
        self._reject(
            {
                "id": "w",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": 1},
                    {"id": "t", "kind": "task", "depends_on": [], "run_if": {"condition_id": "c", "expected": False}},
                ],
            },
            "depends_on",
        )

    def test_run_if_expected_must_be_boolean(self):
        self._reject(
            {
                "id": "w",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": 1},
                    {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": "true"}},
                ],
            },
            "expected",
        )

    def test_run_if_unknown_fields_are_rejected(self):
        self._reject(
            {
                "id": "w",
                "nodes": [
                    {"id": "c", "kind": "condition", "depends_on": [], "path": "a", "equals": 1},
                    {"id": "t", "kind": "task", "depends_on": ["c"], "run_if": {"condition_id": "c", "expected": True, "extra": 1}},
                ],
            },
            "run_if",
        )

    def test_invalid_kind_is_rejected(self):
        self._reject(
            {"id": "w", "nodes": [{"id": "n", "kind": "gate", "depends_on": []}]},
            "kind",
        )


if __name__ == "__main__":
    unittest.main()
