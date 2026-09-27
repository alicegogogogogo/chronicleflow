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


class ReexpandTestBase(unittest.TestCase):
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
        return self.service.advance(
            execution_id, {"output": {"items": list(items)}}, advance_key, tenant=tenant
        )

    def complete(self, execution_id, key, output=None, tenant=""):
        return self.service.advance(execution_id, {"output": output if output is not None else {}}, key, tenant=tenant)

    def reexpand_events(self, execution_id="run-1"):
        return [event for event in self.service.events(execution_id) if event["type"] == "map_reexpanded"]


class ReexpandTests(ReexpandTestBase):
    def test_reexpand_replaces_instances_and_queues_from_zero(self):
        self.expand(items=(1, 2, 3))
        self.complete("run-1", "a2", {"v": 0})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        events_before = len(self.service.events("run-1"))
        state = self.service.reexpand_map("run-1", "fanout", {}, "r1")
        map_state = state["maps"]["fanout"]
        self.assertEqual("running", map_state["status"])
        self.assertEqual([0, 1, 2], [item["index"] for item in map_state["instances"]])
        self.assertEqual(
            ["ready", "ready", "ready"], [item["status"] for item in map_state["instances"]]
        )
        self.assertEqual([], map_state["outputs"])
        # Every regenerated record carries exactly the unmodified fields.
        for item in map_state["instances"]:
            self.assertEqual({"index", "status", "output", "failure_reason"}, set(item))
        # Exactly one event, recording the action, the node, and the count.
        events = self.service.events("run-1")
        self.assertEqual(events_before + 1, len(events))
        self.assertEqual("map_reexpanded", events[-1]["type"])
        self.assertEqual({"map_id": "fanout", "instance_count": 3}, events[-1]["payload"])
        # The regenerated instances advance in ascending index order.
        self.complete("run-1", "a3", {"w": 0})
        self.complete("run-1", "a4", {"w": 1})
        state = self.complete("run-1", "a5", {"w": 2})
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([{"w": 0}, {"w": 1}, {"w": 2}], state["outputs"]["fanout"])
        self.assertEqual("completed", state["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_after_deleting_every_instance(self):
        self.expand(workflow(nodes_extra=[{"id": "after", "kind": "task", "depends_on": ["fanout"]}]), items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        state = self.service.delete_map_instance("run-1", "fanout", 1, {}, "d2")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertIn("fanout", state["completed_nodes"])
        state = self.service.reexpand_map("run-1", "fanout", {}, "r1")
        # The node returns to running: it leaves the completed list and its
        # recorded output entry is discarded.
        self.assertEqual("running", state["maps"]["fanout"]["status"])
        self.assertNotIn("fanout", state["completed_nodes"])
        self.assertNotIn("fanout", state["outputs"])
        self.assertEqual([0, 1], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        self.complete("run-1", "a2", {"v": 0})
        state = self.complete("run-1", "a3", {"v": 1})
        self.assertEqual([{"v": 0}, {"v": 1}], state["outputs"]["fanout"])
        self.assertEqual(1, state["completed_nodes"].count("fanout"))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpanded_instances_retry_from_zero(self):
        self.expand(workflow({"id": "work", "retries": 1}), items=(1,))
        # The first generation consumes its single retry.
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        self.service.reexpand_map("run-1", "fanout", {}, "r1")
        # The regenerated instance starts its retry bound from zero.
        state = self.service.advance("run-1", {"failure": {"reason": "again"}}, "a3")
        self.assertEqual("ready", state["maps"]["fanout"]["instances"][0]["status"])
        failed = [
            event["payload"] for event in self.service.events("run-1")
            if event["type"] == "node_failed" and event["payload"].get("map_id")
        ]
        self.assertEqual([1, 1], [payload["attempt"] for payload in failed])
        # The second failure of the regenerated instance exhausts the bound.
        state = self.service.advance("run-1", {"failure": {"reason": "again"}}, "a4")
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpanded_instances_use_the_same_delete_and_modify_entries(self):
        self.expand(items=(1, 2, 3))
        self.service.reexpand_map("run-1", "fanout", {}, "r1")
        state = self.service.modify_map_instance("run-1", "fanout", 1, {"input": {"k": 1}}, "m1")
        self.assertEqual({"k": 1}, state["maps"]["fanout"]["instances"][1]["input"])
        # The regenerated instance was never rewritten before.
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertIsNone(modified[0]["payload"]["before"])
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual([1, 2], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_with_zero_elements_completes_again_at_once(self):
        self.expand(
            workflow(nodes_extra=[{"id": "after", "kind": "task", "depends_on": ["fanout"]}]),
            items=(),
        )
        state = self.service.reexpand_map("run-1", "fanout", {}, "r1")
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        self.assertEqual([], state["maps"]["fanout"]["outputs"])
        self.assertEqual([], state["outputs"]["fanout"])
        self.assertEqual("running", state["status"])
        types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(1, types.count("map_reexpanded"))
        self.assertEqual(2, types.count("map_completed"))
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_is_idempotent_and_keys_are_scoped_per_operation(self):
        self.expand(items=(1, 2))
        first = self.service.reexpand_map("run-1", "fanout", {}, "shared")
        second = self.service.reexpand_map("run-1", "fanout", {}, "shared")
        self.assertEqual(first, second)
        self.assertEqual(1, len(self.reexpand_events()))
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "shared")
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "shared")

    def test_reexpand_conflicts_without_changes(self):
        # A node that has not expanded yet.
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution({"id": "run-pending", "workflow_id": "orders", "input": {}}, "e-pending")
        with self.assertRaises(ConflictError):
            self.service.reexpand_map("run-pending", "fanout", {}, "r1")
        # A permanently failed node.
        self.expand(items=(1, 2), advance_key="a1")
        before = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        self.assertEqual("terminated", before["status"])
        with self.assertRaises(ConflictError):
            self.service.reexpand_map("run-1", "fanout", {}, "r2")
        self.assertEqual(before, self.service.get_execution("run-1"))

    def test_reexpand_waiting_instance_conflicts(self):
        self.expand(workflow({"id": "work", "approval": {"approvers": ["alice"]}}), items=(1, 2))
        before = self.complete("run-1", "a2")
        self.assertEqual("waiting", before["maps"]["fanout"]["instances"][0]["status"])
        with self.assertRaises(ConflictError):
            self.service.reexpand_map("run-1", "fanout", {}, "r1")
        self.assertEqual(before, self.service.get_execution("run-1"))
        # Once the point is decided the expansion can be replaced.
        self.service.decision("run-1", {"approver": "alice", "decision": "approved", "output": {"ok": 1}}, "dec1")
        state = self.service.reexpand_map("run-1", "fanout", {}, "r2")
        self.assertEqual(["ready", "ready"], [item["status"] for item in state["maps"]["fanout"]["instances"]])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_completed_execution_conflicts(self):
        self.expand(items=(1,))
        before = self.complete("run-1", "a2", {"v": 0})
        self.assertEqual("completed", before["status"])
        with self.assertRaises(ConflictError):
            self.service.reexpand_map("run-1", "fanout", {}, "r1")
        self.assertEqual(before, self.service.get_execution("run-1"))

    def test_reexpand_unknown_targets_are_not_found(self):
        self.expand(items=(1,))
        with self.assertRaises(NotFoundError):
            self.service.reexpand_map("run-1", "ghost", {}, "r1")
        with self.assertRaises(NotFoundError):
            self.service.reexpand_map("run-1", "collect", {}, "r2")
        with self.assertRaises(NotFoundError):
            self.service.reexpand_map("missing", "fanout", {}, "r3")

    def test_reexpand_invalid_bodies_are_validation_errors(self):
        self.expand(items=(1,))
        events_before = self.service.events("run-1")
        for body in ({"unexpected": 1}, [], "x"):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.reexpand_map("run-1", "fanout", body, f"bad-{type(body).__name__}")
        self.assertEqual(events_before, self.service.events("run-1"))

    def test_reexpand_writes_a_checkpoint_and_recovery_continues(self):
        self.expand(items=(1, 2, 3))
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        count_before = len(self.service.checkpoints("run-1")["checkpoints"])
        self.service.reexpand_map("run-1", "fanout", {}, "r1")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(count_before + 1, len(checkpoints))
        self.assertEqual(
            [0, 1, 2],
            [item["index"] for item in checkpoints[-1]["state"]["maps"]["fanout"]["instances"]],
        )
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rc1")
        self.assertEqual(snapshot, self.service.get_execution("run-1"))
        self.complete("run-1", "a2", {"v": 0})
        self.complete("run-1", "a3", {"v": 1})
        state = self.complete("run-1", "a4", {"v": 2})
        # Recovery repeated no output and the regenerated list completed once.
        completed = [event for event in self.service.events("run-1") if event["type"] == "node_completed"]
        self.assertEqual(4, len(completed))
        self.assertEqual([{"v": 0}, {"v": 1}, {"v": 2}], state["outputs"]["fanout"])


class ReexpandNestedTests(ReexpandTestBase):
    def test_nested_reexpand_replaces_only_the_current_iteration(self):
        self.expand(nested_workflow(), items=(10, 20, 30), execution_input={"again": True})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        state = self.service.reexpand_map("run-1", "fanout", {}, "r1")
        event = self.reexpand_events()[0]
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 3, "loop_id": "loop", "iteration": 1},
            event["payload"],
        )
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual([0, 1, 2], [item["index"] for item in iteration["maps"]["fanout"]["instances"]])
        self.complete("run-1", "a2", {"v": 0})
        self.complete("run-1", "a3", {"v": 1})
        state = self.complete("run-1", "a4", {"v": 2})
        iteration = state["loops"]["loop"]["iterations"][0]
        self.assertEqual("completed", iteration["maps"]["fanout"]["status"])
        self.assertEqual([{"v": 0}, {"v": 1}, {"v": 2}], iteration["outputs"]["fanout"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_nested_reexpand_after_loop_finished_conflicts(self):
        self.expand(nested_workflow(max_instances=5), items=(1,), execution_input={"again": True})
        self.complete("run-1", "a2", {"v": 0})
        self.complete("run-1", "a3", {"other": 1})
        state = self.complete("run-1", "a4", {"other": 1})
        self.assertEqual("completed", state["loops"]["loop"]["status"])
        with self.assertRaises(ConflictError):
            self.service.reexpand_map("run-1", "fanout", {}, "r1")


class ReexpandTenancyTests(ReexpandTestBase):
    def test_reexpand_is_tenant_scoped_and_meters_nothing(self):
        self.expand(items=(1,), tenant="acme")
        with self.assertRaises(NotFoundError):
            self.service.reexpand_map("run-1", "fanout", {}, "r1", tenant="other")
        with self.assertRaises(NotFoundError):
            self.service.reexpand_map("run-1", "fanout", {}, "r2")
        state = self.service.reexpand_map("run-1", "fanout", {}, "r3", tenant="acme")
        self.assertEqual(1, len(state["maps"]["fanout"]["instances"]))
        usage = {entry["type"] for entry in self.service.usage("acme")["usage"]}
        self.assertNotIn("map_reexpanded", usage)


class InterleavingTests(ReexpandTestBase):
    def test_modify_then_delete_settles_in_call_order(self):
        self.expand(items=(1, 2))
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"k": 1}}, "m1")
        # A rewritten instance has still never advanced: it stays deletable.
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual([1], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        deleted = [event for event in self.service.events("run-1") if event["type"] == "instance_deleted"]
        self.assertEqual("ready", deleted[0]["payload"]["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_then_modify_or_delete_is_not_found(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d2")

    def test_repeated_modifies_chain_before_and_after(self):
        self.expand(items=(1,))
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"k": 1}}, "m1")
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"k": 2}}, "m2")
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual(2, len(modified))
        self.assertIsNone(modified[0]["payload"]["before"])
        self.assertEqual({"k": 1}, modified[1]["payload"]["before"])
        self.assertEqual({"k": 2}, modified[1]["payload"]["after"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_then_modify_records_a_null_predecessor(self):
        self.expand(items=(1,))
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"old": True}}, "m1")
        self.service.reexpand_map("run-1", "fanout", {}, "r1")
        state = self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"new": True}}, "m2")
        self.assertEqual({"new": True}, state["maps"]["fanout"]["instances"][0]["input"])
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertIsNone(modified[-1]["payload"]["before"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class ReexpandHttpTests(unittest.TestCase):
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

    def test_reexpand_round_trip_over_http(self):
        status, data = self.call("POST", "/executions/run-1/maps/fanout/instances/2/delete", {}, "hd1")
        self.assertEqual(200, status)
        status, data = self.call("POST", "/executions/run-1/maps/fanout/reexpand", {}, "hr1")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual(
            [0, 1, 2], [item["index"] for item in json.loads(data)["maps"]["fanout"]["instances"]]
        )
        status, data = self.call("GET", "/executions/run-1/events")
        reexpanded = [event for event in json.loads(data)["events"] if event["type"] == "map_reexpanded"]
        self.assertEqual(1, len(reexpanded))
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 3}, reexpanded[0]["payload"]
        )

    def test_expand_alias_route(self):
        self.call("POST", "/executions", {"id": "run-alias", "workflow_id": "orders", "input": {}}, "e-alias")
        self.call("POST", "/executions/run-alias/advance", {"output": {"items": [7]}}, "a-alias")
        status, data = self.call("POST", "/executions/run-alias/maps/fanout/expand", {}, "hr2")
        self.assertEqual(200, status)
        self.assertEqual(1, len(json.loads(data)["maps"]["fanout"]["instances"]))

    def test_reexpand_rejects_a_non_empty_body(self):
        status, data = self.call("POST", "/executions/run-1/maps/fanout/reexpand", {"unexpected": 1}, "hr3")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_reexpand_unknown_node_is_not_found(self):
        status, data = self.call("POST", "/executions/run-1/maps/ghost/reexpand", {}, "hr4")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
