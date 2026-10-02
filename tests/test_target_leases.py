import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


class TargetLeaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))

    def tearDown(self):
        self.directory.cleanup()

    def make_execution(self, nodes, execution_id="run-1", workflow_id="wf", **extra):
        self.service.create_workflow({"id": workflow_id, "nodes": nodes}, f"wf-{workflow_id}")
        body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
        body.update(extra)
        self.service.create_execution(body, f"ex-{execution_id}")

    def make_parallel_execution(self):
        self.make_execution(
            [
                {"id": "a", "kind": "task", "depends_on": []},
                {"id": "b", "kind": "task", "depends_on": []},
            ]
        )

    def test_claim_target_returns_work_item_and_lease(self):
        self.make_parallel_execution()
        result = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual(
            {"execution_id": "run-1", "workflow_id": "wf", "node_id": "a"}, result["work_item"]
        )
        lease = result["lease"]
        self.assertEqual("task:a", lease["work_item_id"])
        self.assertEqual("w1", lease["worker_id"])
        self.assertEqual(30.0, lease["lease_seconds"])
        self.assertTrue(lease["expires_at"].endswith("Z"))
        self.assertTrue(lease["heartbeat_at"].endswith("Z"))
        self.assertGreater(lease["expires_at"], lease["heartbeat_at"])

    def test_claim_target_picks_unleased_targets_in_ready_order(self):
        self.make_parallel_execution()
        first = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        second = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "c2")
        self.assertEqual("a", first["work_item"]["node_id"])
        self.assertEqual("b", second["work_item"]["node_id"])
        with self.assertRaises(ConflictError):
            self.service.claim("run-1", {"worker_id": "w3", "scope": "target"}, "c3")

    def test_claim_target_without_ready_targets_is_empty(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        self.service.advance("run-1", {"output": {}}, "a1")
        result = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual({"work_item": None, "lease": None}, result)

    def test_claim_target_on_finished_execution_is_empty(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        self.service.advance("run-1", {"output": {}}, "a1")
        self.assertEqual("completed", self.service.get_execution("run-1")["status"])
        result = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual({"work_item": None, "lease": None}, result)

    def test_claim_target_missing_and_cross_tenant_execution_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.claim("ghost", {"worker_id": "w1", "scope": "target"}, "c1")
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        with self.assertRaises(NotFoundError):
            self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c2", tenant="acme")

    def test_claim_scope_defaults_to_execution_mode(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        result = self.service.claim("run-1", {"worker_id": "w1"}, "c1")
        self.assertEqual({"execution_id": "run-1", "workflow_id": "wf"}, result["work_item"])
        self.assertNotIn("work_item_id", result["lease"])

    def test_invalid_claim_bodies_are_rejected(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        for body in (
            {"worker_id": "w1", "scope": "node"},
            {"worker_id": "w1", "scope": 5},
            {"worker_id": "w1", "scope": "target", "extra": 1},
            {"scope": "target"},
            {"worker_id": "w1", "scope": "target", "lease_seconds": 0},
            {"worker_id": "w1", "scope": "target", "lease_seconds": float("inf")},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.claim("run-1", body, "cl-bad")

    def test_target_advance_settles_only_that_target(self):
        self.make_execution(
            [
                {"id": "a", "kind": "task", "depends_on": []},
                {"id": "b", "kind": "task", "depends_on": ["a"]},
            ]
        )
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        work_item_id = claimed["lease"]["work_item_id"]
        state = self.service.advance(
            "run-1", {"output": {"x": 1}, "worker_id": "w1", "work_item_id": work_item_id}, "a1"
        )
        self.assertEqual(["a"], state["completed_nodes"])
        self.assertEqual({"a": {"x": 1}}, state["outputs"])
        events = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(["execution_started", "node_completed"], events)
        self.assertEqual(1, len(self.service.checkpoints("run-1")["checkpoints"]))
        # The settled work item is consumed; referencing it is a missing resource.
        with self.assertRaises(NotFoundError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": work_item_id}, "a2"
            )
        # The successor is now claimable.
        followup = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "c2")
        self.assertEqual("b", followup["work_item"]["node_id"])

    def test_target_advance_failure_keeps_retry_semantics(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": [], "retries": 1}])
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        state = self.service.advance(
            "run-1",
            {"failure": {"reason": "boom"}, "worker_id": "w1", "work_item_id": claimed["lease"]["work_item_id"]},
            "a1",
        )
        self.assertEqual("running", state["status"])
        self.assertEqual({"attempt": 2, "failures": 1}, state["attempts"]["a"])
        types = [event["type"] for event in self.service.events("run-1")]
        self.assertEqual(["execution_started", "node_failed", "node_retried"], types)
        reclaimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c2")
        state = self.service.advance(
            "run-1",
            {"failure": {"reason": "boom"}, "worker_id": "w1", "work_item_id": reclaimed["lease"]["work_item_id"]},
            "a2",
        )
        self.assertEqual("terminated", state["status"])
        self.assertEqual("retries_exhausted", state["termination_reason"])

    def test_target_heartbeat_extends_lease(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        work_item_id = claimed["lease"]["work_item_id"]
        renewed = self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "h1")
        self.assertEqual(work_item_id, renewed["lease"]["work_item_id"])
        self.assertGreaterEqual(renewed["lease"]["expires_at"], claimed["lease"]["expires_at"])
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w2", "work_item_id": work_item_id}, "h2")
        # A heartbeat writes no events.
        self.assertEqual(1, len(self.service.events("run-1")))

    def test_target_release_returns_target_to_claimable_set(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        work_item_id = claimed["lease"]["work_item_id"]
        result = self.service.release("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "r1")
        self.assertEqual({"released": True}, result)
        # The old holder conflicts on submit, heartbeat, and release.
        with self.assertRaises(ConflictError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": work_item_id}, "a1"
            )
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "h1")
        with self.assertRaises(ConflictError):
            self.service.release("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "r2")
        # The target can be claimed again right away.
        reclaimed = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "c2")
        self.assertEqual(work_item_id, reclaimed["lease"]["work_item_id"])
        self.assertEqual("w2", reclaimed["lease"]["worker_id"])

    def test_expired_target_lease_can_be_reclaimed(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        claimed = self.service.claim(
            "run-1", {"worker_id": "w1", "scope": "target", "lease_seconds": 0.05}, "c1"
        )
        work_item_id = claimed["lease"]["work_item_id"]
        time.sleep(0.1)
        with self.assertRaises(ConflictError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": work_item_id}, "a1"
            )
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "h1")
        reclaimed = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "c2")
        self.assertEqual(work_item_id, reclaimed["lease"]["work_item_id"])

    def test_unknown_work_item_id_is_not_found(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        with self.assertRaises(NotFoundError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": "task:nope"}, "a1"
            )
        with self.assertRaises(NotFoundError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": "task:nope"}, "h1")
        with self.assertRaises(NotFoundError):
            self.service.release("run-1", {"worker_id": "w1", "work_item_id": "task:nope"}, "r1")

    def test_invalid_target_bodies_are_rejected(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        with self.assertRaises(ValidationError):
            self.service.advance("run-1", {"output": {}, "work_item_id": "task:a"}, "a1")
        with self.assertRaises(ValidationError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": 5}, "a2"
            )
        with self.assertRaises(ValidationError):
            self.service.heartbeat("run-1", {"work_item_id": "task:a"}, "h1")
        with self.assertRaises(ValidationError):
            self.service.release("run-1", {"worker_id": "w1", "work_item_id": ""}, "r1")

    def test_valid_target_lease_blocks_execution_mode_operations(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        work_item_id = claimed["lease"]["work_item_id"]
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "a1")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}, "worker_id": "w1"}, "a2")
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1"}, "h1")
        with self.assertRaises(ConflictError):
            self.service.release("run-1", {"worker_id": "w1"}, "r1")
        with self.assertRaises(ConflictError):
            self.service.claim("run-1", {"worker_id": "w2"}, "c2")
        with self.assertRaises(ConflictError):
            self.service.claim("run-1", {"worker_id": "w2", "scope": "execution"}, "c3")
        # Once every target lease is released, execution mode is restored.
        self.service.release("run-1", {"worker_id": "w1", "work_item_id": work_item_id}, "r2")
        state = self.service.advance("run-1", {"output": {"done": True}}, "a3")
        self.assertEqual("completed", state["status"])

    def test_map_instances_claim_with_map_context(self):
        self.make_execution(
            [
                {"id": "collect", "kind": "task", "depends_on": []},
                {
                    "id": "ship",
                    "kind": "map",
                    "depends_on": ["collect"],
                    "source": "collect",
                    "path": "items",
                    "max_instances": 10,
                    "template": {"id": "ship-item"},
                },
            ]
        )
        self.service.advance("run-1", {"output": {"items": [1, 2]}}, "a1")
        first = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual("ship-item", first["work_item"]["node_id"])
        self.assertEqual("ship", first["work_item"]["map_id"])
        self.assertEqual(0, first["work_item"]["index"])
        second = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "c2")
        self.assertEqual(1, second["work_item"]["index"])
        # Instances settle independently, in any order.
        state = self.service.advance(
            "run-1",
            {"output": {"v": 2}, "worker_id": "w2", "work_item_id": second["lease"]["work_item_id"]},
            "a2",
        )
        self.assertEqual("running", state["maps"]["ship"]["status"])
        state = self.service.advance(
            "run-1",
            {"output": {"v": 1}, "worker_id": "w1", "work_item_id": first["lease"]["work_item_id"]},
            "a3",
        )
        self.assertEqual("completed", state["maps"]["ship"]["status"])
        self.assertEqual([{"v": 1}, {"v": 2}], state["maps"]["ship"]["outputs"])
        self.assertEqual("completed", state["status"])
        self.assertEqual({"consistent": True, "execution": state}, self.service.replay("run-1"))

    def test_nested_map_instance_carries_loop_context(self):
        self.make_execution(
            [
                {"id": "attempt", "kind": "task", "depends_on": []},
                {
                    "id": "fanout",
                    "kind": "map",
                    "depends_on": ["attempt"],
                    "source": "attempt",
                    "path": "items",
                    "max_instances": 10,
                    "template": {"id": "work"},
                },
                {"id": "keep", "kind": "condition", "depends_on": ["fanout"], "path": "go", "equals": True},
                {
                    "id": "loop",
                    "kind": "loop",
                    "depends_on": [],
                    "entry": "attempt",
                    "condition": "keep",
                    "max_iterations": 2,
                },
            ],
            input={"go": True},
        )
        self.service.advance("run-1", {"output": {"items": ["x"]}}, "a1")
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        item = claimed["work_item"]
        self.assertEqual("fanout", item["map_id"])
        self.assertEqual(0, item["index"])
        self.assertEqual("loop", item["loop_id"])
        self.assertEqual(1, item["iteration"])
        state = self.service.advance(
            "run-1",
            {"output": {"done": 1}, "worker_id": "w1", "work_item_id": claimed["lease"]["work_item_id"]},
            "a2",
        )
        self.assertEqual(2, state["loops"]["loop"]["current_iteration"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_repeated_target_claim_with_same_key_returns_first_result(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        first = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        repeated = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual(first, repeated)
        with self.assertRaises(ConflictError):
            self.service.claim("run-1", {"worker_id": "w1"}, "c1")

    def test_target_claim_writes_no_events_or_state_fields(self):
        self.make_execution([{"id": "a", "kind": "task", "depends_on": []}])
        before = self.service.get_execution("run-1")
        self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual(1, len(self.service.events("run-1")))

    def test_recovery_continues_after_target_settlement(self):
        self.make_execution(
            [
                {"id": "a", "kind": "task", "depends_on": []},
                {"id": "b", "kind": "task", "depends_on": ["a"]},
            ]
        )
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "c1")
        self.service.advance(
            "run-1",
            {"output": {"v": 1}, "worker_id": "w1", "work_item_id": claimed["lease"]["work_item_id"]},
            "a1",
        )
        state = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rec1")
        self.assertEqual(["a"], state["completed_nodes"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


if __name__ == "__main__":
    unittest.main()
