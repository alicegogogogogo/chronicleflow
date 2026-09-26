import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
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


def nested_workflow(template=None, max_instances=5):
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
                "max_instances": max_instances,
                "template": template or {"id": "work"},
            },
            {"id": "check", "kind": "condition", "depends_on": ["fanout"], "path": "again", "equals": True},
            {"id": "loop", "kind": "loop", "depends_on": [], "entry": "entry",
             "condition": "check", "max_iterations": 3},
        ],
    }


class InstanceOperationTestBase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def expand(
        self,
        document=workflow(),
        execution_id="run-1",
        items=(1, 2, 3),
        advance_key="a1",
        tenant="",
        execution_input=None,
    ):
        self.service.create_workflow(document, "w1", tenant=tenant)
        self.service.create_execution(
            {"id": execution_id, "workflow_id": "orders", "input": execution_input or {}},
            f"e-{execution_id}",
            tenant=tenant,
        )
        state = self.service.advance(
            execution_id, {"output": {"items": list(items)}}, advance_key, tenant=tenant
        )
        return state

    def complete(self, execution_id, key, output=None, tenant=""):
        return self.service.advance(execution_id, {"output": output if output is not None else {}}, key, tenant=tenant)


class DeleteInstanceTests(InstanceOperationTestBase):
    def test_deleting_a_ready_instance_keeps_retained_order_and_outputs(self):
        state = self.expand(items=(1, 2, 3))
        state = self.complete("run-1", "a2", {"v": 0})
        state = self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        self.assertEqual([0, 2], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        # The next ready work is the retained instance with the lowest index.
        state = self.complete("run-1", "a3", {"v": 2})
        self.assertEqual(2, state["maps"]["fanout"]["instances"][1]["index"])
        state = self.complete("run-1", "a4", {"v": 3})
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([{"v": 0}, {"v": 2}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_deleting_last_unfinished_instance_completes_the_node(self):
        self.expand(items=(1, 2))
        self.complete("run-1", "a2", {"v": 0})
        state = self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([{"v": 0}], state["outputs"]["fanout"])
        self.assertIn("fanout", state["completed_nodes"])

    def test_deleting_every_instance_completes_with_empty_output_list(self):
        self.expand(items=(1,))
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([], state["maps"]["fanout"]["outputs"])
        self.assertEqual([], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_deleting_completed_or_waiting_instance_conflicts_without_changes(self):
        self.expand(items=(1,))
        before = self.complete("run-1", "a2", {"v": 0})
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual(before, self.service.get_execution("run-1"))
        # A waiting instance conflicts too.
        self.directory.cleanup()
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.expand(
            workflow({"id": "work", "approval": {"approvers": ["alice"]}}),
            items=(1, 2),
        )
        self.complete("run-1", "a2")
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        # The still-ready sibling is unaffected.
        self.assertEqual(
            "ready", self.service.get_execution("run-1")["maps"]["fanout"]["instances"][1]["status"]
        )

    def test_failed_instance_is_removable_but_termination_stands(self):
        self.expand(workflow({"id": "work", "retries": 0}), items=(1, 2))
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        self.assertEqual("terminated", state["status"])
        failed, ready = state["maps"]["fanout"]["instances"]
        self.assertEqual("failed", failed["status"])
        self.assertEqual("ready", ready["status"])
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertEqual("failed", state["maps"]["fanout"]["status"])
        self.assertEqual(["fanout"], state["failed_nodes"])
        self.assertEqual([1], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        # The never-advanced sibling can be removed after termination as well.
        state = self.service.delete_map_instance("run-1", "fanout", 1, {}, "d2")
        self.assertEqual([], state["maps"]["fanout"]["instances"])
        self.assertEqual("failed", state["maps"]["fanout"]["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_deleted_or_out_of_bounds_index_is_not_found(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d2")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 9, {}, "d3")

    def test_unknown_map_node_or_execution_is_not_found(self):
        self.expand(items=(1,))
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "ghost", 0, {}, "d1")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("missing", "fanout", 0, {}, "d2")

    def test_non_empty_body_is_a_validation_error(self):
        self.expand(items=(1,))
        with self.assertRaises(ValidationError):
            self.service.delete_map_instance("run-1", "fanout", 0, {"unexpected": 1}, "d1")
        with self.assertRaises(ValidationError):
            self.service.delete_map_instance("run-1", "fanout", 0, [], "d2")
        with self.assertRaises(ValidationError):
            self.service.delete_map_instance("run-1", "fanout", "x", {}, "d3")

    def test_delete_event_records_action_node_index_and_status(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        deleted = [event for event in self.service.events("run-1") if event["type"] == "instance_deleted"]
        self.assertEqual(1, len(deleted))
        self.assertEqual(
            {"map_id": "fanout", "index": 0, "status": "ready"}, deleted[0]["payload"]
        )
        # No loop context outside a loop body.
        self.assertNotIn("loop_id", deleted[0]["payload"])

    def test_delete_is_idempotent_and_keys_are_scoped_per_operation(self):
        self.expand(items=(1, 2, 3))
        first = self.service.delete_map_instance("run-1", "fanout", 1, {}, "shared")
        second = self.service.delete_map_instance("run-1", "fanout", 1, {}, "shared")
        self.assertEqual(first, second)
        events = [event for event in self.service.events("run-1") if event["type"] == "instance_deleted"]
        self.assertEqual(1, len(events))
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 2, {"input": {}}, "shared")
        # A different index with the same key is another operation.
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 2, {}, "shared")

    def test_recovery_never_resurrects_or_repeats_a_deleted_instance(self):
        self.expand(items=(1, 2, 3))
        self.complete("run-1", "a2", {"v": 0})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rc1")
        self.assertEqual([0, 2], [item["index"] for item in snapshot["maps"]["fanout"]["instances"]])
        self.complete("run-1", "a3", {"v": 2})
        self.complete("run-1", "a4", {"v": 3})
        completed = [
            event for event in self.service.events("run-1") if event["type"] == "node_completed"
        ]
        # The source plus two retained instances: the deleted one never completes.
        self.assertEqual(3, len(completed))
        self.assertTrue(self.service.replay("run-1")["consistent"])


class ModifyInstanceTests(InstanceOperationTestBase):
    def test_rewriting_input_only_affects_that_instance(self):
        state = self.expand(items=(1, 2))
        state = self.service.modify_map_instance("run-1", "fanout", 1, {"input": {"k": 42}}, "m1")
        instances = state["maps"]["fanout"]["instances"]
        self.assertNotIn("input", instances[0])
        self.assertEqual({"k": 42}, instances[1]["input"])
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual(
            {"map_id": "fanout", "index": 1, "before": None, "after": {"k": 42}},
            modified[0]["payload"],
        )
        # A second rewrite records the previous content.
        self.service.modify_map_instance("run-1", "fanout", 1, {"input": {"k": 43}}, "m2")
        events = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual({"k": 42}, events[1]["payload"]["before"])
        self.assertEqual({"k": 43}, events[1]["payload"]["after"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_advanced_instances_cannot_be_modified(self):
        self.expand(items=(1,))
        before = self.complete("run-1", "a2", {"v": 0})
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1")
        self.assertEqual(before, self.service.get_execution("run-1"))

    def test_modify_failed_instance_conflicts_but_ready_sibling_stays_rewritable(self):
        self.expand(workflow({"id": "work", "retries": 0}), items=(1, 2))
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1")
        # The termination conclusion is unchanged; the ready instance is still
        # rewritable (its input has never advanced).
        state = self.service.modify_map_instance("run-1", "fanout", 1, {"input": {"x": 1}}, "m2")
        self.assertEqual("terminated", state["status"])
        self.assertEqual({"x": 1}, state["maps"]["fanout"]["instances"][1]["input"])

    def test_unknown_targets_are_not_found(self):
        self.expand(items=(1,))
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("run-1", "fanout", 7, {"input": {}}, "m1")
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("run-1", "ghost", 0, {"input": {}}, "m2")
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("missing", "fanout", 0, {"input": {}}, "m3")

    def test_invalid_bodies_are_validation_errors_without_partial_writes(self):
        self.expand(items=(1, 2))
        for body in ({}, {"output": {}}, {"input": []}, {"input": {}, "extra": 1}, []):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.modify_map_instance(
                        "run-1", "fanout", 0, body, f"bad-{type(body).__name__}-{len(str(body))}"
                    )
        # Nothing partial was written by the rejected bodies: the first
        # successful rewrite on instance 0 records a null predecessor.
        state = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m-empty")
        self.assertEqual({}, state["maps"]["fanout"]["instances"][0]["input"])
        # Instance 1 was never named by a successful rewrite.
        self.assertNotIn("input", self.service.get_execution("run-1")["maps"]["fanout"]["instances"][1])
        # Non-finite numbers are rejected and leave the earlier rewrite intact.
        with self.assertRaises(ValidationError):
            self.service.modify_map_instance(
                "run-1", "fanout", 0, {"input": {"v": float("nan")}}, "m-nan"
            )
        self.assertEqual({}, self.service.get_execution("run-1")["maps"]["fanout"]["instances"][0]["input"])

    def test_modify_is_idempotent_and_checkpointed(self):
        self.expand(items=(1,))
        first = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"v": 1}}, "shared")
        second = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"v": 1}}, "shared")
        self.assertEqual(first, second)
        # A different rewrite with the same key returns the first result.
        third = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"v": 2}}, "shared")
        self.assertEqual({"v": 1}, third["maps"]["fanout"]["instances"][0]["input"])
        events = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual(1, len(events))
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(
            {"v": 1}, checkpoints[-1]["state"]["maps"]["fanout"]["instances"][0]["input"]
        )

    def test_rewritten_input_keeps_float_precision_and_negative_zero(self):
        self.expand(items=(1,))
        state = self.service.modify_map_instance(
            "run-1", "fanout", 0, {"input": {"a": 0.30000000000000004, "b": -0.0}}, "m1"
        )
        value = state["maps"]["fanout"]["instances"][0]["input"]
        self.assertEqual(0.30000000000000004, value["a"])
        self.assertEqual(-0.0, value["b"])
        self.assertTrue(str(value["b"]).startswith("-"))
        self.assertTrue(self.service.replay("run-1")["consistent"])


class NestedInstanceOperationTests(InstanceOperationTestBase):
    def iteration(self, state, number=1):
        return state["loops"]["loop"]["iterations"][number - 1]

    def test_events_carry_loop_context_and_iteration_finishes_normally(self):
        self.expand(nested_workflow(), items=(10, 20, 30), execution_input={"again": True})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        state = self.service.modify_map_instance("run-1", "fanout", 2, {"input": {"q": 1}}, "m1")
        deleted = [event for event in self.service.events("run-1") if event["type"] == "instance_deleted"]
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual(
            {"map_id": "fanout", "index": 1, "status": "ready", "loop_id": "loop", "iteration": 1},
            deleted[0]["payload"],
        )
        self.assertEqual("loop", modified[0]["payload"]["loop_id"])
        self.assertEqual(1, modified[0]["payload"]["iteration"])
        # The retained instances advance; the iteration then rolls as usual.
        self.complete("run-1", "a2", {"v": 0})
        state = self.complete("run-1", "a3", {"v": 2})
        map_state = self.iteration(state)["maps"]["fanout"]
        self.assertEqual("completed", map_state["status"])
        self.assertEqual([{"v": 0}, {"v": 2}], map_state["outputs"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_deleting_only_instance_ends_the_iteration(self):
        self.expand(nested_workflow(), items=(1,), execution_input={"again": True})
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_past_iteration_instances_conflict_after_the_loop_finishes(self):
        self.expand(nested_workflow(max_instances=5), items=(1,), execution_input={"again": True})
        self.complete("run-1", "a2", {"v": 0})
        # Iteration 2 and 3 expand empty (condition path is absent in outputs),
        # ending the loop at its iteration limit.
        self.complete("run-1", "a3", {"other": 1})
        state = self.complete("run-1", "a4", {"other": 1})
        self.assertEqual("completed", state["loops"]["loop"]["status"])
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1")

    def test_index_absent_from_every_iteration_is_not_found(self):
        self.expand(nested_workflow(), items=(1,), execution_input={"again": True})
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 4, {}, "d1")

    def test_current_iteration_wins_the_same_index_over_a_past_one(self):
        self.expand(nested_workflow(max_instances=5), items=(1,), execution_input={"again": True})
        self.complete("run-1", "a2", {"v": 0})
        # Iteration 2 expands the same index 0; modifying it must target the
        # new ready instance, not the completed one from iteration 1.
        state = self.complete("run-1", "a3", {"items": [9]})
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        state = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"k": 2}}, "m1")
        current = state["loops"]["loop"]["iterations"][1]["maps"]["fanout"]["instances"][0]
        self.assertEqual({"k": 2}, current["input"])
        events = [
            event for event in self.service.events("run-1") if event["type"] == "instance_modified"
        ]
        self.assertEqual(2, events[0]["payload"]["iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class InstanceOperationTenancyTests(InstanceOperationTestBase):
    def test_operations_are_tenant_scoped(self):
        self.expand(items=(1,), tenant="acme")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1", tenant="other")
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1", tenant="other")
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d2", tenant="acme")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        # The legacy namespace never saw the execution.
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d3")

    def test_operations_meter_nothing_and_leave_subscriptions_to_instance_events(self):
        # A tenant-scoped delete/modify is not a billable action; only the
        # existing metered action types may ever appear.
        self.expand(
            workflow(nodes_extra=[]),
            items=(1, 2),
            tenant="acme",
        )
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1", tenant="acme")
        self.service.modify_map_instance("run-1", "fanout", 1, {"input": {}}, "m1", tenant="acme")
        usage = {entry["type"] for entry in self.service.usage("acme")["usage"]}
        self.assertNotIn("instance_deleted", usage)
        self.assertNotIn("instance_modified", usage)


class InstanceOperationHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.call("POST", "/workflows", workflow(), "wf1")
        cls.call("POST", "/executions", {"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        cls.call("POST", "/executions/run-1/advance", {"output": {"items": [1, 2, 3]}}, "a1")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    @classmethod
    def call(cls, method, path, body=None, key=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", cls.port)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_delete_and_modify_round_trip_over_http(self):
        status, data = self.call(
            "POST", "/executions/run-1/maps/fanout/instances/2/delete", {}, "hd1"
        )
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual([0, 1], [item["index"] for item in json.loads(data)["maps"]["fanout"]["instances"]])
        status, data = self.call(
            "POST", "/executions/run-1/maps/fanout/instances/1/modify", {"input": {"k": 1}}, "hm1"
        )
        self.assertEqual(200, status)
        self.assertEqual({"k": 1}, json.loads(data)["maps"]["fanout"]["instances"][1]["input"])

    def test_index_segment_errors(self):
        status, data = self.call("POST", "/executions/run-1/maps/fanout/instances/-1/delete", {}, "hd2")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-1/maps/fanout/instances/two/delete", {}, "hd3")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_non_finite_modify_is_rejected(self):
        status, data = self.call(
            "POST",
            "/executions/run-1/maps/fanout/instances/1/modify",
            raw=b'{"input":{"v":NaN}}',
            key="hmnan",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

