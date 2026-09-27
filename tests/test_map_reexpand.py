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


def workflow(template=None, max_instances=10, source_path="items"):
    return {
        "id": "orders",
        "nodes": [
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
        ],
    }


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

    def expand(self, document=None, execution_id="run-1", items=(1, 2, 3), tenant="", execution_input=None):
        self.service.create_workflow(document or workflow(), "w1", tenant=tenant)
        self.service.create_execution(
            {"id": execution_id, "workflow_id": "orders", "input": execution_input or {}},
            f"e-{execution_id}",
            tenant=tenant,
        )
        return self.service.advance(
            execution_id, {"output": {"items": list(items)}}, f"a1-{execution_id}", tenant=tenant
        )

    def complete(self, execution_id, key, output=None, tenant=""):
        return self.service.advance(execution_id, {"output": output if output is not None else {}}, key, tenant=tenant)


class ReexpandTests(ReexpandTestBase):
    def test_reexpand_after_delete_replaces_every_record_and_queues_from_zero(self):
        self.expand(items=(1, 2, 3))
        self.complete("run-1", "a2", {"v": 0})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        state = self.service.expand_map("run-1", "fanout", {}, "x1")
        instances = state["maps"]["fanout"]["instances"]
        self.assertEqual([0, 1, 2], [item["index"] for item in instances])
        self.assertTrue(all(item["status"] == "ready" for item in instances))
        self.assertEqual("running", state["maps"]["fanout"]["status"])
        # The completed instance's old output is gone with its record.
        self.assertTrue(all(item["output"] is None for item in instances))
        # The fresh instances advance one per call, in index order.
        self.complete("run-1", "a3", {"v": 10})
        self.complete("run-1", "a4", {"v": 11})
        state = self.complete("run-1", "a5", {"v": 12})
        self.assertEqual("completed", state["maps"]["fanout"]["status"])
        # The result list holds only the finally retained instances' outputs.
        self.assertEqual([{"v": 10}, {"v": 11}, {"v": 12}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpand_event_matches_first_expansion_shape(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.service.expand_map("run-1", "fanout", {}, "x1")
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(2, len(expanded))
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 2, "exceeded": False}, expanded[-1]["payload"]
        )
        self.assertNotIn("loop_id", expanded[-1]["payload"])

    def test_reexpanded_instances_retry_and_delete_like_first_expansion(self):
        self.expand(workflow({"id": "work", "retries": 1}), items=(1, 2))
        # Burn the retry budget of instance 0 before re-expanding.
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a2")
        self.service.expand_map("run-1", "fanout", {}, "x1")
        # The fresh instance 0 has a fresh budget: its first failure retries.
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a3")
        self.assertEqual("ready", state["maps"]["fanout"]["instances"][0]["status"])
        self.assertEqual("running", state["status"])
        # Delete and modify entries apply to the new list exactly as before.
        self.service.modify_map_instance("run-1", "fanout", 1, {"input": {"k": 1}}, "m1")
        state = self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        self.assertEqual([0], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        state = self.complete("run-1", "a4", {"v": 0})
        self.assertEqual([{"v": 0}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_reexpanded_instances_park_for_approval_like_first_expansion(self):
        self.expand(workflow({"id": "work", "approval": {"approvers": ["alice"]}}), items=(1,))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        # Deleting the only instance completed the node, so this map is done.
        with self.assertRaises(ConflictError):
            self.service.expand_map("run-1", "fanout", {}, "x1")

    def test_pending_completed_or_failed_map_cannot_reexpand(self):
        # Pending: the map never expanded (its source has not completed).
        self.service.create_workflow(workflow(), "w1")
        self.service.create_execution({"id": "run-p", "workflow_id": "orders", "input": {}}, "e-p")
        with self.assertRaises(ConflictError):
            self.service.expand_map("run-p", "fanout", {}, "x-p")
        # Completed: every retained instance finished.
        self.expand(items=(1,))
        self.complete("run-1", "a2", {"v": 0})
        with self.assertRaises(ConflictError):
            self.service.expand_map("run-1", "fanout", {}, "x-c")
        # Failed: retry exhaustion terminated the execution.
        self.expand(workflow({"id": "work", "retries": 0}), execution_id="run-2", items=(1,))
        self.service.advance("run-2", {"failure": {"reason": "boom"}}, "a-f")
        with self.assertRaises(ConflictError):
            self.service.expand_map("run-2", "fanout", {}, "x-f")

    def test_unknown_targets_are_not_found(self):
        self.expand(items=(1,))
        with self.assertRaises(NotFoundError):
            self.service.expand_map("run-1", "ghost", {}, "x1")
        with self.assertRaises(NotFoundError):
            self.service.expand_map("missing", "fanout", {}, "x2")
        # A node that is not a dynamic map is not found either.
        with self.assertRaises(NotFoundError):
            self.service.expand_map("run-1", "collect", {}, "x3")

    def test_non_empty_body_is_a_validation_error(self):
        self.expand(items=(1,))
        with self.assertRaises(ValidationError):
            self.service.expand_map("run-1", "fanout", {"unexpected": 1}, "x1")
        with self.assertRaises(ValidationError):
            self.service.expand_map("run-1", "fanout", [], "x2")

    def test_expand_is_idempotent_and_scoped_per_operation(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        first = self.service.expand_map("run-1", "fanout", {}, "shared")
        second = self.service.expand_map("run-1", "fanout", {}, "shared")
        self.assertEqual(first, second)
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(2, len(expanded))
        # The same key names another operation everywhere else.
        with self.assertRaises(ConflictError):
            self.service.delete_map_instance("run-1", "fanout", 1, {}, "shared")
        with self.assertRaises(ConflictError):
            self.service.modify_map_instance("run-1", "fanout", 1, {"input": {}}, "shared")

    def test_reexpand_is_checkpointed_and_recovers_without_resurrecting(self):
        self.expand(items=(1, 2, 3))
        self.complete("run-1", "a2", {"v": 0})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        self.service.expand_map("run-1", "fanout", {}, "x1")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(
            [0, 1, 2],
            [item["index"] for item in checkpoints[-1]["state"]["maps"]["fanout"]["instances"]],
        )
        snapshot = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rc1")
        self.assertEqual(
            [0, 1, 2], [item["index"] for item in snapshot["maps"]["fanout"]["instances"]]
        )
        self.complete("run-1", "a3", {"v": 10})
        self.complete("run-1", "a4", {"v": 11})
        state = self.complete("run-1", "a5", {"v": 12})
        self.assertEqual([{"v": 10}, {"v": 11}, {"v": 12}], state["outputs"]["fanout"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_operations_are_tenant_scoped(self):
        self.expand(items=(1,), tenant="acme")
        with self.assertRaises(NotFoundError):
            self.service.expand_map("run-1", "fanout", {}, "x1", tenant="other")
        with self.assertRaises(NotFoundError):
            self.service.expand_map("run-1", "fanout", {}, "x2")
        state = self.service.expand_map("run-1", "fanout", {}, "x3", tenant="acme")
        self.assertEqual("running", state["maps"]["fanout"]["status"])


class NestedReexpandTests(ReexpandTestBase):
    def test_nested_reexpand_carries_loop_context(self):
        self.expand(nested_workflow(), items=(10, 20, 30), execution_input={"again": True})
        self.service.delete_map_instance("run-1", "fanout", 1, {}, "d1")
        state = self.service.expand_map("run-1", "fanout", {}, "x1")
        map_state = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]
        self.assertEqual([0, 1, 2], [item["index"] for item in map_state["instances"]])
        expanded = [event for event in self.service.events("run-1") if event["type"] == "map_expanded"]
        self.assertEqual(
            {"map_id": "fanout", "instance_count": 3, "exceeded": False, "loop_id": "loop", "iteration": 1},
            expanded[-1]["payload"],
        )
        self.complete("run-1", "a2", {"v": 0})
        self.complete("run-1", "a3", {"v": 1})
        state = self.complete("run-1", "a4", {"v": 2})
        map_state = state["loops"]["loop"]["iterations"][0]["maps"]["fanout"]
        self.assertEqual([{"v": 0}, {"v": 1}, {"v": 2}], map_state["outputs"])
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_finished_loop_has_no_expandable_iteration(self):
        self.expand(nested_workflow(), items=(1,), execution_input={"again": False})
        self.complete("run-1", "a2", {"v": 0})
        state = self.service.get_execution("run-1")
        self.assertEqual("completed", state["loops"]["loop"]["status"])
        with self.assertRaises(ConflictError):
            self.service.expand_map("run-1", "fanout", {}, "x1")


class InterleavingTests(ReexpandTestBase):
    def test_modify_then_delete_settles_in_call_order(self):
        self.expand(items=(1, 2))
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"k": 1}}, "m1")
        # A rewritten instance is still a never-advanced one: it deletes.
        state = self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        self.assertEqual([1], [item["index"] for item in state["maps"]["fanout"]["instances"]])
        deleted = [event for event in self.service.events("run-1") if event["type"] == "instance_deleted"]
        self.assertEqual({"map_id": "fanout", "index": 0, "status": "ready"}, deleted[0]["payload"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_delete_then_modify_or_delete_is_not_found(self):
        self.expand(items=(1, 2))
        self.service.delete_map_instance("run-1", "fanout", 0, {}, "d1")
        with self.assertRaises(NotFoundError):
            self.service.modify_map_instance("run-1", "fanout", 0, {"input": {}}, "m1")
        with self.assertRaises(NotFoundError):
            self.service.delete_map_instance("run-1", "fanout", 0, {}, "d2")
        # The retained instance is untouched by the rejected calls.
        state = self.service.get_execution("run-1")
        self.assertEqual([1], [item["index"] for item in state["maps"]["fanout"]["instances"]])

    def test_repeated_modifies_chain_before_and_after(self):
        self.expand(items=(1,))
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"v": 1}}, "m1")
        self.service.modify_map_instance("run-1", "fanout", 0, {"input": {"v": 2}}, "m2")
        modified = [event for event in self.service.events("run-1") if event["type"] == "instance_modified"]
        self.assertEqual(None, modified[0]["payload"]["before"])
        self.assertEqual({"v": 1}, modified[0]["payload"]["after"])
        self.assertEqual({"v": 1}, modified[1]["payload"]["before"])
        self.assertEqual({"v": 2}, modified[1]["payload"]["after"])
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
        cls.call("POST", "/executions/run-1/advance", {"output": {"items": [1, 2]}}, "a1")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    @classmethod
    def call(cls, method, path, body=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", cls.port)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_expand_round_trip_over_http(self):
        status, data = self.call("POST", "/executions/run-1/maps/fanout/instances/0/delete", {}, "hd1")
        self.assertEqual(200, status)
        status, data = self.call("POST", "/executions/run-1/maps/fanout/expand", {}, "hx1")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        body = json.loads(data)
        self.assertEqual([0, 1], [item["index"] for item in body["maps"]["fanout"]["instances"]])
        self.assertTrue(all(item["status"] == "ready" for item in body["maps"]["fanout"]["instances"]))

    def test_expand_body_and_target_errors_over_http(self):
        status, data = self.call("POST", "/executions/run-1/maps/fanout/expand", {"x": 1}, "hx2")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-1/maps/ghost/expand", {}, "hx3")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-1/maps/collect/expand", {}, "hx4")
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
