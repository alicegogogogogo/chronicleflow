import tempfile
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def workflow(template=None, max_instances=10, source_path="items", nodes_extra=None):
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
    ]
    if nodes_extra:
        nodes.extend(nodes_extra)
    return {"id": "orders", "nodes": nodes}


class MapExpansionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(), "w1")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, execution_id="run-1"):
        self.service.create_execution(
            {"id": execution_id, "workflow_id": "orders", "input": {}}, f"e-{execution_id}"
        )

    def advance(self, key, payload):
        if isinstance(payload, dict) and ("output" in payload or "failure" in payload):
            body = payload
        else:
            body = {"output": payload}
        return self.service.advance("run-1", body, key)

    def test_expands_one_instance_per_element_in_index_order(self):
        self.start()
        state = self.advance("a1", {"output": {"items": [{"a": 1}, {"a": 2}, {"a": 3}]}})
        self.assertEqual(["collect"], state["completed_nodes"])
        map_state = state["maps"]["fanout"]
        self.assertEqual("running", map_state["status"])
        self.assertEqual([0, 1, 2], [instance["index"] for instance in map_state["instances"]])
        self.assertTrue(all(instance["status"] == "ready" for instance in map_state["instances"]))
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(1, len(expanded))
        self.assertEqual(3, expanded[0]["payload"]["instance_count"])
        self.assertFalse(expanded[0]["payload"]["exceeded"])

        first = self.advance("a2", {"output": {"shipped": 0}})
        self.assertEqual("completed", first["maps"]["fanout"]["instances"][0]["status"])
        self.assertEqual("ready", first["maps"]["fanout"]["instances"][1]["status"])
        self.advance("a3", {"output": {"shipped": 1}})
        finished = self.advance("a4", {"output": {"shipped": 2}})
        self.assertEqual("completed", finished["maps"]["fanout"]["status"])
        self.assertEqual(
            [{"shipped": 0}, {"shipped": 1}, {"shipped": 2}], finished["outputs"]["fanout"]
        )
        self.assertEqual(["collect", "fanout"], finished["completed_nodes"])
        completed_event = [e for e in self.service.events("run-1") if e["type"] == "map_completed"]
        self.assertEqual(3, completed_event[0]["payload"]["instance_count"])
        self.assertEqual({"consistent": True, "execution": finished}, self.service.replay("run-1"))

    def test_nested_element_path(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(source_path="payload.items"), "w1")
        self.start()
        state = self.advance("a1", {"output": {"payload": {"items": [1, 2]}}})
        self.assertEqual(2, len(state["maps"]["fanout"]["instances"]))

    def test_instance_events_carry_map_id_and_index(self):
        self.start()
        self.advance("a1", {"output": {"items": [1]}})
        self.advance("a2", {"output": {"ok": True}})
        completed = [
            event
            for event in self.service.events("run-1")
            if event["type"] == "node_completed" and event["payload"]["node_id"] == "work"
        ]
        self.assertEqual(1, len(completed))
        self.assertEqual("fanout", completed[0]["payload"]["map_id"])
        self.assertEqual(0, completed[0]["payload"]["index"])

    def test_missing_or_non_array_path_expands_to_zero_and_completes(self):
        for source_output in ({"other": 1}, {"items": "scalar"}, {"items": 12}, {}):
            with self.subTest(source_output=source_output):
                directory = tempfile.TemporaryDirectory()
                service = ChronicleFlow(str(Path(directory.name) / "other.db"))
                service.create_workflow(workflow(), "w1")
                service.create_execution({"id": "run-x", "workflow_id": "orders", "input": {}}, "e1")
                state = service.advance("run-x", {"output": source_output}, "a1")
                map_state = state["maps"]["fanout"]
                self.assertEqual("completed", map_state["status"])
                self.assertEqual([], map_state["instances"])
                self.assertEqual([], state["outputs"]["fanout"])
                self.assertEqual("completed", state["status"])
                self.assertTrue(service.replay("run-x")["consistent"])
                directory.cleanup()

    def test_null_element_value_is_not_an_array(self):
        self.start()
        state = self.advance("a1", {"output": {"items": None}})
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([], state["maps"]["fanout"]["instances"])

    def test_over_limit_creates_nothing_and_terminates(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(max_instances=2), "w1")
        self.start()
        state = self.advance("a1", {"output": {"items": [1, 2, 3]}})
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        map_state = state["maps"]["fanout"]
        self.assertEqual("failed", map_state["status"])
        self.assertEqual([], map_state["instances"])
        self.assertIsNotNone(map_state["failure_reason"])
        expanded = [e for e in self.service.events("run-1") if e["type"] == "map_expanded"]
        self.assertTrue(expanded[0]["payload"]["exceeded"])
        self.assertEqual(0, expanded[0]["payload"]["instance_count"])
        self.assertEqual(3, expanded[0]["payload"]["element_count"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))
        # A terminated execution absorbs later output.
        again = self.advance("a2", {"output": {"never": True}})
        self.assertEqual(state, again)

    def test_boundary_value_is_allowed(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow(max_instances=2), "w1")
        self.start()
        self.advance("a1", {"output": {"items": [1, 2]}})
        state = self.advance("a2", {"output": {"v": 0}})
        state = self.advance("a3", {"output": {"v": 1}})
        self.assertEqual("completed", state["maps"]["fanout"]["status"])

    def test_instance_retry_then_success(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow({"id": "work", "retries": 1}), "w1")
        self.start()
        self.advance("a1", {"output": {"items": [1, 2]}})
        state = self.advance("a2", {"failure": {"reason": "boom"}})
        instance = state["maps"]["fanout"]["instances"][0]
        self.assertEqual("ready", instance["status"])
        self.assertIsNone(instance["failure_reason"])
        retried = [e for e in self.service.events("run-1") if e["type"] == "node_retried"]
        self.assertEqual(1, len(retried))
        self.assertEqual(2, retried[0]["payload"]["attempt"])
        state = self.advance("a3", {"output": {"shipped": 0}})
        state = self.advance("a4", {"output": {"shipped": 1}})
        self.assertEqual([{"shipped": 0}, {"shipped": 1}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_instance_retries_exhausted_terminates_execution(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(workflow({"id": "work", "retries": 1}), "w1")
        self.start()
        self.advance("a1", {"output": {"items": [1, 2]}})
        self.advance("a2", {"failure": {"reason": "first"}})
        state = self.advance("a3", {"failure": {"reason": "second"}})
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        instances = state["maps"]["fanout"]["instances"]
        self.assertEqual("failed", instances[0]["status"])
        self.assertEqual("second", instances[0]["failure_reason"])
        # The second instance never started and keeps no output.
        self.assertEqual("ready", instances[1]["status"])
        self.assertIsNone(instances[1]["output"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_successors_unlock_after_map_completes(self):
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            workflow(nodes_extra=[{"id": "report", "kind": "task", "depends_on": ["fanout"]}]),
            "w1",
        )
        self.start()
        self.advance("a1", {"output": {"items": [1]}})
        state = self.advance("a2", {"output": {"shipped": 0}})
        self.assertEqual(["collect", "fanout"], state["completed_nodes"])
        state = self.advance("a3", {"output": {"done": True}})
        self.assertEqual("completed", state["status"])

    def test_instance_outputs_do_not_overwrite(self):
        self.start()
        self.advance("a1", {"output": {"items": [1, 2]}})
        self.advance("a2", {"output": {"id": "same", "n": 0}})
        state = self.advance("a3", {"output": {"id": "same", "n": 1}})
        outputs = state["outputs"]["fanout"]
        self.assertEqual([{"id": "same", "n": 0}, {"id": "same", "n": 1}], outputs)

    def test_recovery_continues_unfinished_instances_without_duplication(self):
        self.start()
        self.advance("a1", {"output": {"items": [1, 2, 3]}})
        self.advance("a2", {"output": {"shipped": 0}})
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rc1")
        self.assertEqual("completed", snapshot["maps"]["fanout"]["instances"][0]["status"])
        self.assertEqual("ready", snapshot["maps"]["fanout"]["instances"][1]["status"])
        self.advance("a3", {"output": {"shipped": 1}})
        state = self.advance("a4", {"output": {"shipped": 2}})
        self.assertEqual(
            [{"shipped": 0}, {"shipped": 1}, {"shipped": 2}], state["outputs"]["fanout"]
        )
        completed_events = [
            e for e in self.service.events("run-1") if e["type"] == "node_completed"
        ]
        # Source plus three instances, with no duplicated instance output.
        self.assertEqual(4, len(completed_events))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_checkpoints_record_instance_boundaries(self):
        self.start()
        self.advance("a1", {"output": {"items": [1, 2]}})
        self.advance("a2", {"output": {"shipped": 0}})
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        latest = checkpoints[-1]["state"]
        self.assertEqual("completed", latest["maps"]["fanout"]["instances"][0]["status"])


class MapApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            workflow({"id": "work", "approval": {"approvers": ["alice", "bob"]}}), "w1"
        )
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {}}, "e1"
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_instance_parks_at_approval_point(self):
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        state = self.service.advance("run-1", {"output": {}}, "a2")
        waiting = state["waiting_approval"]
        self.assertEqual("work", waiting["node_id"])
        self.assertEqual(["alice", "bob"], waiting["approvers"])
        self.assertEqual("fanout", waiting["map_id"])
        self.assertEqual(0, waiting["index"])
        self.assertEqual("waiting", state["maps"]["fanout"]["instances"][0]["status"])
        # Further advances are absorbed.
        absorbed = self.service.advance("run-1", {"output": {"ignored": True}}, "a3")
        self.assertEqual(state, absorbed)
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_deciding_each_instance_runs_in_index_order(self):
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 0}}, "d1"
        )
        self.assertEqual("completed", state["maps"]["fanout"]["instances"][0]["status"])
        self.service.advance("run-1", {"output": {}}, "a3")
        state = self.service.decision(
            "run-1", {"approver": "bob", "decision": "approved", "output": {"ok": 1}}, "d2"
        )
        self.assertEqual([{"ok": 0}, {"ok": 1}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_unlisted_approver_conflicts_and_state_holds(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "carol", "decision": "approved", "output": {}}, "d1"
            )

    def test_rejecting_an_instance_terminates_with_rejected_reason(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.decision(
            "run-1", {"approver": "alice", "decision": "rejected", "reason": "nope"}, "d1"
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])
        instance = state["maps"]["fanout"]["instances"][0]
        self.assertEqual("failed", instance["status"])
        self.assertEqual("nope", instance["failure_reason"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_cancellation_dismisses_pending_instance_point(self):
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.advance("run-1", {"output": {}}, "a2")
        state = self.service.cancel("run-1", "c1")
        self.assertEqual("cancelled", state["termination_reason"])
        self.assertIsNone(state["waiting_approval"])
        self.assertTrue(self.service.replay("run-1")["consistent"])
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "alice", "decision": "approved", "output": {}}, "d1"
            )


class MapValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def base(self, **overrides):
        node = {
            "id": "fanout",
            "kind": "map",
            "depends_on": ["collect"],
            "source": "collect",
            "path": "items",
            "max_instances": 5,
            "template": {"id": "work"},
        }
        node.update(overrides)
        return {
            "id": "orders",
            "nodes": [{"id": "collect", "kind": "task", "depends_on": []}, node],
        }

    def assert_invalid(self, document):
        with self.assertRaises(ValidationError):
            self.service.create_workflow(document, "w1")

    def test_valid_definition_round_trips(self):
        stored = self.service.create_workflow(self.base(), "w1")
        self.assertEqual("map", stored["nodes"][1]["kind"])
        self.assertEqual({"id": "work"}, stored["nodes"][1]["template"])

    def test_unknown_fields_are_rejected(self):
        self.assert_invalid(self.base(unexpected=1))
        self.assert_invalid(self.base(template={"id": "work", "unexpected": 1}))

    def test_missing_required_fields_are_rejected(self):
        for field in ("source", "path", "max_instances", "template"):
            node = {
                "id": "fanout",
                "kind": "map",
                "depends_on": ["collect"],
                "source": "collect",
                "path": "items",
                "max_instances": 5,
                "template": {"id": "work"},
            }
            del node[field]
            with self.subTest(field=field):
                self.assert_invalid({"id": "orders", "nodes": [
                    {"id": "collect", "kind": "task", "depends_on": []}, node
                ]})

    def test_max_instances_must_be_a_positive_integer(self):
        for value in (0, -1, 2.5, True, "5", None):
            with self.subTest(value=value):
                self.assert_invalid(self.base(max_instances=value))

    def test_source_must_be_a_task_listed_as_dependency(self):
        self.assert_invalid(self.base(source="ghost"))
        # source present but missing from depends_on
        document = self.base()
        document["nodes"][1]["depends_on"] = []
        self.assert_invalid(document)
        # source naming a condition node
        document = {
            "id": "orders",
            "nodes": [
                {"id": "collect", "kind": "task", "depends_on": []},
                {"id": "check", "kind": "condition", "depends_on": ["collect"], "path": "x", "equals": 1},
                {
                    "id": "fanout",
                    "kind": "map",
                    "depends_on": ["check"],
                    "source": "check",
                    "path": "items",
                    "max_instances": 5,
                    "template": {"id": "work"},
                },
            ],
        }
        self.assert_invalid(document)

    def test_path_must_be_a_dotted_path(self):
        for value in ("", ".items", "items.", "a..b"):
            with self.subTest(value=value):
                self.assert_invalid(self.base(path=value))

    def test_template_must_have_a_valid_identifier_retries_and_approval(self):
        self.assert_invalid(self.base(template={"id": ""}))
        self.assert_invalid(self.base(template={"id": 123}))
        for retries in (-1, 11, 1.0, True, "2"):
            with self.subTest(retries=retries):
                self.assert_invalid(self.base(template={"id": "work", "retries": retries}))
        self.assert_invalid(
            self.base(template={"id": "work", "approval": {"approvers": []}})
        )
        self.assert_invalid(
            self.base(template={"id": "work", "approval": {"approvers": ["a", "a"]}})
        )

    def test_template_id_may_collide_with_a_declared_node(self):
        # Template identifiers are free-form: they only name the dynamically
        # expanded instances, so matching a declared node id is allowed.
        stored = self.service.create_workflow(self.base(template={"id": "collect"}), "w1")
        self.assertEqual({"id": "collect"}, stored["nodes"][1]["template"])

    def test_template_ids_may_repeat_across_maps(self):
        document = {
            "id": "orders",
            "nodes": [
                {"id": "collect", "kind": "task", "depends_on": []},
                {
                    "id": "m1",
                    "kind": "map",
                    "depends_on": ["collect"],
                    "source": "collect",
                    "path": "a",
                    "max_instances": 5,
                    "template": {"id": "work"},
                },
                {
                    "id": "m2",
                    "kind": "map",
                    "depends_on": ["collect", "m1"],
                    "source": "collect",
                    "path": "b",
                    "max_instances": 5,
                    "template": {"id": "work"},
                },
            ],
        }
        stored = self.service.create_workflow(document, "w1")
        self.assertEqual(
            [{"id": "work"}, {"id": "work"}],
            [node["template"] for node in stored["nodes"][1:]],
        )

    def test_map_may_nest_in_a_loop_body(self):
        # A map node inside a loop body expands independently per iteration.
        document = {
            "id": "orders",
            "nodes": [
                {"id": "collect", "kind": "task", "depends_on": []},
                {
                    "id": "fanout",
                    "kind": "map",
                    "depends_on": ["collect"],
                    "source": "collect",
                    "path": "items",
                    "max_instances": 5,
                    "template": {"id": "work"},
                },
                {"id": "settle", "kind": "task", "depends_on": ["fanout"]},
                {"id": "again", "kind": "condition", "depends_on": ["settle"], "path": "more", "equals": True},
                {"id": "loop", "kind": "loop", "depends_on": [], "entry": "settle",
                 "condition": "again", "max_iterations": 3},
            ],
        }
        stored = self.service.create_workflow(document, "w1")
        self.assertEqual("map", stored["nodes"][1]["kind"])


class MapObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_metrics_count_instances_under_template_node(self):
        self.service.create_workflow(workflow({"id": "work", "retries": 1}), "w1", tenant="acme")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {}}, "e1", tenant="acme"
        )
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1", tenant="acme")
        self.service.advance("run-1", {"failure": {"reason": "x"}}, "a2", tenant="acme")
        self.service.advance("run-1", {"output": {"v": 0}}, "a3", tenant="acme")
        self.service.advance("run-1", {"output": {"v": 1}}, "a4", tenant="acme")
        metrics = self.service.metrics("acme")
        self.assertEqual(2, metrics["node_completions"]["work"])
        self.assertEqual(1, metrics["node_failures"]["work"])
        self.assertEqual(1, metrics["retry_consumption"]["work"])

    def test_queue_subscriptions_receive_instance_events(self):
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution(
            {
                "id": "run-1",
                "workflow_id": "orders",
                "input": {},
                "subscriptions": [{"queue": "events", "events": ["node_completed"]}],
            },
            "e1",
        )
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        self.service.advance("run-1", {"output": {"v": 1}}, "a3")
        messages = self.service.queues("run-1")["queues"][0]["messages"]
        instance_messages = [m for m in messages if m["payload"]["node_id"] == "work"]
        self.assertEqual([0, 1], [m["payload"]["index"] for m in instance_messages])

    def test_lease_is_enforced_for_instance_submissions(self):
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {}}, "e1"
        )
        self.service.advance("run-1", {"output": {"items": [1]}}, "a1")
        self.service.claim("run-1", {"worker_id": "worker-1", "lease_seconds": 30}, "c1")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {"v": 0}}, "a2")
        state = self.service.advance(
            "run-1", {"output": {"v": 0}, "worker_id": "worker-1"}, "a3"
        )
        self.assertEqual("completed", state["maps"]["fanout"]["instances"][0]["status"])


class MapStateShapeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_execution_without_map_has_no_maps_field(self):
        self.service.create_workflow(
            {
                "id": "plain",
                "nodes": [
                    {"id": "only", "kind": "task", "depends_on": []},
                ],
            },
            "w1",
        )
        state = self.service.create_execution(
            {"id": "run-1", "workflow_id": "plain", "input": {}}, "e1"
        )
        self.assertNotIn("maps", state)
        started = [e for e in self.service.events("run-1") if e["type"] == "execution_started"][0]
        self.assertNotIn("maps", started["payload"])
        state = self.service.advance("run-1", {"output": {"done": True}}, "a1")
        self.assertNotIn("maps", state)
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_missing_execution_is_not_found_and_duplicate_id_conflicts(self):
        self.service.create_workflow(workflow(), "w1")
        with self.assertRaises(NotFoundError):
            self.service.get_execution("ghost")
        self.service.create_execution(
            {"id": "run-1", "workflow_id": "orders", "input": {}}, "e1"
        )
        with self.assertRaises(ConflictError):
            self.service.create_execution(
                {"id": "run-1", "workflow_id": "orders", "input": {}}, "e2"
            )
        # Reusing an idempotency key across a different operation conflicts.
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {"items": []}}, "e1")


if __name__ == "__main__":
    unittest.main()
