import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


class MapWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.workflow = {
            "id": "fan-out",
            "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {
                    "id": "dispatch",
                    "kind": "map",
                    "depends_on": ["prepare"],
                    "source": "prepare",
                    "path": "items",
                    "max_instances": 5,
                    "template": {"id": "work"},
                },
                {"id": "finalize", "kind": "task", "depends_on": ["dispatch"]},
            ],
        }
        self.service.create_workflow(self.workflow, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, input_data=None, execution_id="run-1"):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": "fan-out", "input": input_data or {}}, f"e-{execution_id}"
        )

    def test_expansion_queues_instances_in_index_order(self):
        self.start()
        state = self.service.advance("run-1", {"output": {"items": ["a", "b", "c"]}}, "a1")
        dispatch = state["maps"]["dispatch"]
        self.assertEqual("running", dispatch["status"])
        self.assertEqual(3, len(dispatch["instances"]))
        self.assertEqual([0, 1, 2], [instance["index"] for instance in dispatch["instances"]])
        self.assertEqual("ready", dispatch["instances"][0]["status"])
        self.assertEqual("waiting", dispatch["instances"][1]["status"])
        self.assertEqual("waiting", dispatch["instances"][2]["status"])
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(1, len(expanded))
        self.assertEqual(3, expanded[0]["payload"]["count"])

    def test_instances_complete_one_per_advance_and_outputs_are_ordered(self):
        self.start()
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")
        state = self.service.advance("run-1", {"output": {"done": 0}}, "a2")
        self.assertEqual("completed", state["maps"]["dispatch"]["instances"][0]["status"])
        self.assertEqual({"done": 0}, state["maps"]["dispatch"]["instances"][0]["output"])
        self.assertEqual("ready", state["maps"]["dispatch"]["instances"][1]["status"])
        self.assertNotIn("dispatch", state["outputs"])
        state = self.service.advance("run-1", {"output": {"done": 1}}, "a3")
        self.assertEqual("completed", state["maps"]["dispatch"]["status"])
        self.assertEqual([{"done": 0}, {"done": 1}], state["outputs"]["dispatch"])
        self.assertIn("dispatch", state["completed_nodes"])
        state = self.service.advance("run-1", {"output": {"end": True}}, "a4")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_instance_events_carry_the_instance_index(self):
        self.start()
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")
        self.service.advance("run-1", {"output": {"done": 0}}, "a2")
        completed = [
            event["payload"]
            for event in self.service.events("run-1")
            if event["type"] == "node_completed" and "map_id" in event["payload"]
        ]
        self.assertEqual([{"node_id": "work", "output": {"done": 0}, "map_id": "dispatch", "index": 0}], completed)

    def test_missing_path_expands_to_zero_instances(self):
        self.start()
        state = self.service.advance("run-1", {"output": {"other": 1}}, "a1")
        self.assertEqual("completed", state["maps"]["dispatch"]["status"])
        self.assertEqual([], state["maps"]["dispatch"]["instances"])
        self.assertEqual([], state["outputs"]["dispatch"])
        state = self.service.advance("run-1", {"output": {"end": True}}, "a2")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_non_array_value_expands_to_zero_instances(self):
        self.start()
        state = self.service.advance("run-1", {"output": {"items": {"not": "a list"}}}, "a1")
        self.assertEqual("completed", state["maps"]["dispatch"]["status"])
        self.assertEqual([], state["outputs"]["dispatch"])

    def test_skipped_source_expands_to_zero_instances(self):
        self.service.create_workflow(
            {
                "id": "guarded",
                "nodes": [
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "go", "equals": True},
                    {"id": "prepare", "kind": "task", "depends_on": ["check"], "run_if": {"condition_id": "check", "expected": True}},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work"},
                    },
                ],
            },
            "w-guarded",
        )
        self.service.create_execution({"id": "run-guarded", "workflow_id": "guarded", "input": {"go": False}}, "e-guarded")
        # The first advance evaluates the condition, skips the source task,
        # expands the map to zero instances, and finishes without consuming
        # the submitted output.
        state = self.service.advance("run-guarded", {"output": {"unused": True}}, "a-guarded")
        self.assertEqual("completed", state["status"])
        self.assertEqual(["prepare"], state["skipped_nodes"])
        self.assertEqual([], state["outputs"]["dispatch"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-guarded"))

    def test_exceeding_the_instance_cap_fails_the_node_permanently(self):
        self.start()
        state = self.service.advance("run-1", {"output": {"items": [1, 2, 3, 4, 5, 6]}}, "a1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["dispatch"], state["failed_nodes"])
        dispatch = state["maps"]["dispatch"]
        self.assertEqual("failed", dispatch["status"])
        self.assertEqual([], dispatch["instances"])
        self.assertIsNotNone(dispatch["failure_reason"])
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(6, expanded[0]["payload"]["count"])
        self.assertTrue(expanded[0]["payload"]["exceeded"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_execution_without_map_nodes_keeps_baseline_shape(self):
        self.service.create_workflow(
            {"id": "plain", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]}, "w-plain"
        )
        state = self.service.create_execution({"id": "run-plain", "workflow_id": "plain", "input": {}}, "e-plain")
        self.assertNotIn("maps", state)
        self.assertFalse(any("maps" in event["payload"] for event in self.service.events("run-plain")))
        state = self.service.advance("run-plain", {"output": {}}, "a-plain")
        self.assertNotIn("maps", state)
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-plain"))


class MapRetryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {
                "id": "fan-out",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work", "retries": 1},
                    },
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "fan-out", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def test_failed_instance_is_retried_within_its_attempts(self):
        state = self.service.advance("run-1", {"failure": {"reason": "flaky"}}, "a2")
        instance = state["maps"]["dispatch"]["instances"][0]
        self.assertEqual("ready", instance["status"])
        self.assertEqual({"attempt": 2, "failures": 1}, instance["attempts"])
        state = self.service.advance("run-1", {"output": {"done": 0}}, "a3")
        self.assertEqual("completed", state["maps"]["dispatch"]["instances"][0]["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_exhausted_instance_retries_terminate_the_execution(self):
        self.service.advance("run-1", {"failure": {"reason": "first"}}, "a2")
        state = self.service.advance("run-1", {"failure": {"reason": "second"}}, "a3")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["dispatch"], state["failed_nodes"])
        instance = state["maps"]["dispatch"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("second", instance["failure_reason"])
        self.assertEqual("failed", state["maps"]["dispatch"]["status"])
        event_types = [event["type"] for event in self.service.events("run-1")]
        self.assertIn("map_instance_failed", event_types)
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_restart_continues_unfinished_instances(self):
        self.service.advance("run-1", {"output": {"done": 0}}, "a2")
        recovered = ChronicleFlow(self.database)
        state = recovered.recover("run-1", {"from": "latest_checkpoint"}, "r1")
        self.assertEqual("completed", state["maps"]["dispatch"]["instances"][0]["status"])
        state = recovered.advance("run-1", {"output": {"done": 1}}, "a3")
        self.assertEqual("completed", state["status"])
        self.assertEqual([{"done": 0}, {"done": 1}], state["outputs"]["dispatch"])
        self.assertEqual({"consistent": True, "execution": state}, recovered.replay("run-1"))


class MapApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "test.db")
        self.service = ChronicleFlow(self.database)
        self.service.create_workflow(
            {
                "id": "fan-out",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work", "approval": {"approvers": ["alice", "bob"]}},
                    },
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "fan-out", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")

    def tearDown(self):
        self.directory.cleanup()

    def test_instance_parks_at_the_approval_point(self):
        state = self.service.advance("run-1", {"output": {"ignored": True}}, "a2")
        self.assertEqual(
            {"node_id": "work", "approvers": ["alice", "bob"], "map_id": "dispatch", "index": 0},
            state["waiting_approval"],
        )
        events = self.service.events("run-1")
        self.assertEqual("approval_requested", events[-1]["type"])
        self.assertEqual({"map_id": "dispatch", "index": 0}, {k: events[-1]["payload"][k] for k in ("map_id", "index")})
        # A further advance absorbs its output and changes nothing.
        parked = self.service.advance("run-1", {"output": {"absorbed": True}}, "a3")
        self.assertEqual(state, parked)
        self.assertEqual(len(events), len(self.service.events("run-1")))

    def test_approval_completes_the_instance_and_parks_the_next(self):
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 0}}, "d1")
        self.assertIsNone(state["waiting_approval"])
        self.assertEqual("completed", state["maps"]["dispatch"]["instances"][0]["status"])
        self.assertEqual({"ok": 0}, state["maps"]["dispatch"]["instances"][0]["output"])
        state = self.service.advance("run-1", {"output": {}}, "a3")
        self.assertEqual(1, state["waiting_approval"]["index"])
        state = self.service.decision("run-1", {"approver": "bob", "decision": "approved", "output": {"ok": 1}}, "d2")
        self.assertEqual("completed", state["status"])
        self.assertEqual([{"ok": 0}, {"ok": 1}], state["outputs"]["dispatch"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_rejection_fails_the_instance_and_terminates(self):
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.decision("run-1", {"approver": "bob", "decision": "rejected", "reason": "no"}, "d1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        self.assertEqual(["dispatch"], state["failed_nodes"])
        instance = state["maps"]["dispatch"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("no", instance["failure_reason"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_repeated_decision_returns_the_first_result(self):
        self.service.advance("run-1", {"output": {}}, "a2")
        first = self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 0}}, "d1")
        repeated = self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 0}}, "d2")
        self.assertEqual(first, repeated)
        with self.assertRaises(ConflictError):
            self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 9}}, "d3")


class MapValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def map_node(self, **overrides):
        node = {
            "id": "dispatch",
            "kind": "map",
            "depends_on": ["prepare"],
            "source": "prepare",
            "path": "items",
            "max_instances": 5,
            "template": {"id": "work"},
        }
        node.update(overrides)
        return node

    def define(self, nodes):
        return self.service.create_workflow({"id": "wf", "nodes": nodes}, "w1")

    def test_valid_definition_round_trips(self):
        self.define(
            [
                {"id": "prepare", "kind": "task", "depends_on": []},
                self.map_node(template={"id": "work", "retries": 2, "approval": {"approvers": ["alice"]}}),
            ]
        )
        document = self.service.get_workflow("wf")["versions"][0]
        self.assertEqual(
            {
                "id": "dispatch",
                "kind": "map",
                "depends_on": ["prepare"],
                "source": "prepare",
                "path": "items",
                "max_instances": 5,
                "template": {"id": "work", "retries": 2, "approval": {"approvers": ["alice"]}},
            },
            document["nodes"][1],
        )

    def test_unknown_and_missing_fields_are_rejected(self):
        prepare = {"id": "prepare", "kind": "task", "depends_on": []}
        with self.assertRaises(ValidationError):
            self.define([prepare, self.map_node(bogus=1)])
        for field in ("source", "path", "max_instances", "template"):
            node = self.map_node()
            del node[field]
            with self.assertRaises(ValidationError, msg=field):
                self.define([prepare, node])

    def test_illegal_max_instances_is_rejected(self):
        prepare = {"id": "prepare", "kind": "task", "depends_on": []}
        for value in (0, -1, 1.5, True, "5", 1001):
            with self.assertRaises(ValidationError, msg=repr(value)):
                self.define([prepare, self.map_node(max_instances=value)])

    def test_illegal_source_is_rejected(self):
        prepare = {"id": "prepare", "kind": "task", "depends_on": []}
        other = {"id": "other", "kind": "task", "depends_on": []}
        condition = {"id": "check", "kind": "condition", "depends_on": [], "path": "x", "equals": 1}
        with self.assertRaises(ValidationError):
            self.define([prepare, other, self.map_node(source="other")])
        with self.assertRaises(ValidationError):
            self.define([prepare, condition, self.map_node(depends_on=["check"], source="check")])
        with self.assertRaises(ValidationError):
            self.define([prepare, self.map_node(source="missing", depends_on=["prepare", "missing"])])

    def test_illegal_path_is_rejected(self):
        prepare = {"id": "prepare", "kind": "task", "depends_on": []}
        for value in ("", "a..b", 3):
            with self.assertRaises(ValidationError, msg=repr(value)):
                self.define([prepare, self.map_node(path=value)])

    def test_illegal_template_is_rejected(self):
        prepare = {"id": "prepare", "kind": "task", "depends_on": []}
        for template in (
            {},
            {"id": "work", "run_if": {"condition_id": "c", "expected": True}},
            {"id": "work", "retries": 11},
            {"id": "work", "retries": -1},
            {"id": "work", "approval": {"approvers": []}},
            {"id": ""},
            "work",
        ):
            with self.assertRaises(ValidationError, msg=repr(template)):
                self.define([prepare, self.map_node(template=template)])

    def test_map_inside_a_loop_body_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.define(
                [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "x", "equals": True},
                    self.map_node(),
                    {"id": "loop", "kind": "loop", "depends_on": [], "entry": "prepare", "condition": "check", "max_iterations": 2},
                ]
            )

    def test_map_source_must_not_be_a_loop_body_task(self):
        with self.assertRaises(ValidationError):
            self.define(
                [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {"id": "check", "kind": "condition", "depends_on": [], "path": "x", "equals": True},
                    {"id": "loop", "kind": "loop", "depends_on": [], "entry": "prepare", "condition": "check", "max_iterations": 2},
                    self.map_node(depends_on=["loop"], source="prepare"),
                ]
            )


class MapObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_instance_completions_and_failures_count_per_instance(self):
        self.service.create_workflow(
            {
                "id": "fan-out",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work", "retries": 1},
                    },
                ],
            },
            "w1",
            "acme",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "fan-out", "input": {}}, "e1", "acme")
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1", "acme")
        self.service.advance("run-1", {"failure": {"reason": "flaky"}}, "a2", "acme")
        self.service.advance("run-1", {"output": {"done": 0}}, "a3", "acme")
        self.service.advance("run-1", {"output": {"done": 1}}, "a4", "acme")
        metrics = self.service.metrics("acme")
        self.assertEqual({"prepare": 1, "work": 2}, metrics["node_completions"])
        self.assertEqual({"work": 1}, metrics["node_failures"])
        self.assertEqual({"work": 1}, metrics["retry_consumption"])

    def test_queue_subscriptions_receive_instance_events(self):
        self.service.create_workflow(
            {
                "id": "fan-out",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work"},
                    },
                ],
                "subscriptions": [{"queue": "events", "events": ["node_completed"]}],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "fan-out", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": ["a"]}}, "a1")
        self.service.advance("run-1", {"output": {"done": 0}}, "a2")
        queues = self.service.queues("run-1")["queues"]
        payloads = [message["payload"] for queue in queues for message in queue["messages"]]
        instance_messages = [payload for payload in payloads if payload.get("map_id") == "dispatch"]
        self.assertEqual(1, len(instance_messages))
        self.assertEqual(0, instance_messages[0]["index"])
        self.assertEqual("work", instance_messages[0]["node_id"])

    def test_checkpoints_are_written_at_instance_boundaries(self):
        self.service.create_workflow(
            {
                "id": "fan-out",
                "nodes": [
                    {"id": "prepare", "kind": "task", "depends_on": []},
                    {
                        "id": "dispatch",
                        "kind": "map",
                        "depends_on": ["prepare"],
                        "source": "prepare",
                        "path": "items",
                        "max_instances": 5,
                        "template": {"id": "work"},
                    },
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "fan-out", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": ["a", "b"]}}, "a1")
        self.service.advance("run-1", {"output": {"done": 0}}, "a2")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(2, len(checkpoints))
        self.assertEqual(
            "completed", checkpoints[-1]["state"]["maps"]["dispatch"]["instances"][0]["status"]
        )
        self.assertEqual("ready", checkpoints[-1]["state"]["maps"]["dispatch"]["instances"][1]["status"])

    def test_missing_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.get_execution("missing")


if __name__ == "__main__":
    unittest.main()
