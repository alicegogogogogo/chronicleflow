import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def _parse(value):
    return datetime.fromisoformat(value[:-1] + "+00:00")


class TargetLeaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        # Two independent tasks become ready at once, so two workers can hold
        # one target each in parallel.
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {"id": "b", "kind": "task", "depends_on": []},
                    {"id": "c", "kind": "task", "depends_on": ["a", "b"]},
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def event_types(self, execution_id="run-1"):
        return [event["type"] for event in self.service.events(execution_id)]

    def claim(self, worker, key, scope="target", **extra):
        body = {"worker_id": worker, "scope": scope, "lease_seconds": 30, **extra}
        return self.service.claim("run-1", body, key)

    def test_scope_target_claims_independent_ready_targets_in_ready_order(self):
        first = self.claim("w1", "k1")
        second = self.claim("w2", "k2")
        self.assertEqual("a", first["work_item"]["node_id"])
        self.assertEqual("b", second["work_item"]["node_id"])
        self.assertEqual("run-1", first["work_item"]["execution_id"])
        self.assertEqual("orders", first["work_item"]["workflow_id"])
        self.assertNotIn("map_id", first["work_item"])

    def test_lease_carries_work_item_id_and_iso_times(self):
        claimed = self.claim("w1", "k1")
        lease = claimed["lease"]
        self.assertEqual(
            ["work_item_id", "worker_id", "lease_seconds", "expires_at", "heartbeat_at"],
            list(lease),
        )
        self.assertIsInstance(lease["work_item_id"], str)
        self.assertTrue(lease["work_item_id"])
        self.assertEqual("w1", lease["worker_id"])
        self.assertEqual(30, lease["lease_seconds"])
        self.assertTrue(lease["expires_at"].endswith("Z"))
        self.assertTrue(lease["heartbeat_at"].endswith("Z"))
        self.assertGreater(_parse(lease["expires_at"]), _parse(lease["heartbeat_at"]))

    def test_claiming_a_validly_held_target_conflicts(self):
        self.claim("w1", "k1")
        self.claim("w2", "k2")
        # Both ready targets are held: another claim is a conflict rather than
        # the empty "nothing to lease" result.
        with self.assertRaises(ConflictError):
            self.claim("w3", "k3")
        # Even the same worker re-claiming conflicts.
        with self.assertRaises(ConflictError):
            self.claim("w1", "k4")

    def test_no_target_left_returns_definite_empty_result(self):
        self.service.advance("run-1", {"output": {}}, "a0")
        self.service.advance("run-1", {"output": {}}, "b0")
        self.service.advance("run-1", {"output": {}}, "c0")
        # After every task is settled the execution has completed, so both a
        # target and an execution claim return the definite empty pair.
        result = self.claim("w1", "k1")
        self.assertEqual({"work_item": None, "lease": None}, result)
        execution_result = self.service.claim("run-1", {"worker_id": "w1"}, "k2")
        self.assertEqual({"work_item": None, "lease": None}, execution_result)

    def test_claims_append_no_events_and_change_no_state(self):
        before = self.service.get_execution("run-1")
        self.claim("w1", "k1")
        self.claim("w2", "k2")
        self.assertEqual(before, self.service.get_execution("run-1"))
        self.assertEqual(["execution_started"], self.event_types())

    def test_target_advance_settles_only_its_holder_target(self):
        first = self.claim("w1", "k1")
        second = self.claim("w2", "k2")
        state = self.service.advance(
            "run-1",
            {"output": {"from": "a"}, "worker_id": "w1", "work_item_id": first["lease"]["work_item_id"]},
            "a-done",
        )
        self.assertEqual(["a"], state["completed_nodes"])
        self.assertEqual({"from": "a"}, state["outputs"]["a"])
        self.assertNotIn("b", state["outputs"])
        # b's holder still holds it and can settle it independently.
        state = self.service.advance(
            "run-1",
            {"output": {"from": "b"}, "worker_id": "w2", "work_item_id": second["lease"]["work_item_id"]},
            "b-done",
        )
        self.assertEqual(["a", "b"], state["completed_nodes"])

    def test_only_the_lease_holder_may_settle_a_target(self):
        first = self.claim("w1", "k1")
        with self.assertRaises(ConflictError):
            self.service.advance(
                "run-1",
                {"output": {}, "worker_id": "w2", "work_item_id": first["lease"]["work_item_id"]},
                "wrong",
            )

    def test_unknown_work_item_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.advance(
                "run-1", {"output": {}, "worker_id": "w1", "work_item_id": "ghost"}, "g1"
            )
        with self.assertRaises(NotFoundError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": "ghost"}, "g2")
        with self.assertRaises(NotFoundError):
            self.service.release("run-1", {"worker_id": "w1", "work_item_id": "ghost"}, "g3")

    def test_old_holder_is_rejected_after_release_and_target_is_reclaimable(self):
        first = self.claim("w1", "k1")
        wid = first["lease"]["work_item_id"]
        self.assertEqual({"released": True}, self.service.release("run-1", {"worker_id": "w1", "work_item_id": wid}, "r1"))
        # The old holder can no longer submit, heartbeat, or release it.
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}, "worker_id": "w1", "work_item_id": wid}, "a1")
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": wid}, "h1")
        with self.assertRaises(ConflictError):
            self.service.release("run-1", {"worker_id": "w1", "work_item_id": wid}, "r2")
        # The released target is immediately claimable again.
        reclaimed = self.claim("w9", "k2")
        self.assertEqual("a", reclaimed["work_item"]["node_id"])
        self.assertNotEqual(wid, reclaimed["lease"]["work_item_id"])

    def test_settled_lease_rejects_its_old_holder(self):
        first = self.claim("w1", "k1")
        wid = first["lease"]["work_item_id"]
        self.service.advance("run-1", {"output": {}, "worker_id": "w1", "work_item_id": wid}, "a1")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}, "worker_id": "w1", "work_item_id": wid}, "a2")
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": wid}, "h1")

    def test_heartbeat_extends_only_the_named_target_lease(self):
        first = self.claim("w1", "k1")
        wid = first["lease"]["work_item_id"]
        renewed = self.service.heartbeat("run-1", {"worker_id": "w1", "work_item_id": wid}, "h1")
        self.assertEqual(wid, renewed["lease"]["work_item_id"])
        self.assertGreaterEqual(
            _parse(renewed["lease"]["expires_at"]), _parse(first["lease"]["expires_at"])
        )
        self.assertEqual(["execution_started"], self.event_types())
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w2", "work_item_id": wid}, "h2")

    def test_target_leases_block_legacy_execution_operations(self):
        claimed = self.claim("w1", "k1")
        wid = claimed["lease"]["work_item_id"]
        # Legacy execution claim, advance, heartbeat, and release all conflict
        # while a target lease is active.
        with self.assertRaises(ConflictError):
            self.service.claim("run-1", {"worker_id": "w9"}, "ec1")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}, "worker_id": "w1"}, "ea1")
        with self.assertRaises(ConflictError):
            self.service.heartbeat("run-1", {"worker_id": "w1"}, "eh1")
        with self.assertRaises(ConflictError):
            self.service.release("run-1", {"worker_id": "w1"}, "er1")
        # Releasing the target lease restores the execution-wide mode.
        result = self.service.release("run-1", {"worker_id": "w1", "work_item_id": wid}, "rel")
        self.assertEqual({"released": True}, result)
        execution_claim = self.service.claim("run-1", {"worker_id": "w9"}, "ec2")
        self.assertEqual({"execution_id": "run-1", "workflow_id": "orders"}, execution_claim["work_item"])

    def test_validation_errors(self):
        for body in (
            {},
            {"lease_seconds": 30},
            {"worker_id": "w1", "scope": "nope"},
            {"worker_id": "w1", "scope": 7},
            {"worker_id": "w1", "scope": "target", "lease_seconds": 0},
            {"worker_id": "w1", "scope": "target", "lease_seconds": -1},
            {"worker_id": "w1", "scope": "target", "lease_seconds": "x"},
            {"worker_id": "w1", "scope": "target", "extra": 1},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.claim("run-1", body, "bad")
        claimed = self.claim("w1", "kv")
        wid = claimed["lease"]["work_item_id"]
        # Malformed target/execution bodies are validation errors.
        for body in (
            {"worker_id": "w1", "work_item_id": wid},
            {"output": {}, "work_item_id": wid},
            {"output": {}, "worker_id": "w1", "work_item_id": wid, "extra": 1},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.advance("run-1", body, "bad-adv")
        # A legacy-shaped advance (no work_item_id) is well-formed but conflicts
        # while a target lease is active.
        for body in ({"output": {}}, {"output": {}, "worker_id": "w1"}):
            with self.subTest(body=body):
                with self.assertRaises(ConflictError):
                    self.service.advance("run-1", body, "conflict-adv")
        for body in ({"worker_id": "w1", "work_item_id": wid, "extra": 1}, {"work_item_id": wid}):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.heartbeat("run-1", body, "bad-hb")
                with self.assertRaises(ValidationError):
                    self.service.release("run-1", body, "bad-rl")

    def test_missing_execution_claim_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.claim("ghost", {"worker_id": "w1", "scope": "target"}, "missing")

    def test_failure_keeps_retry_semantics_and_releases_the_target(self):
        service = ChronicleFlow(str(Path(self.directory.name) / "retry.db"))
        service.create_workflow(
            {"id": "wf", "nodes": [{"id": "t", "kind": "task", "depends_on": [], "retries": 1}]},
            "w",
        )
        service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "e")
        claimed = service.claim("r", {"worker_id": "w1", "scope": "target"}, "k1")
        wid = claimed["lease"]["work_item_id"]
        state = service.advance(
            "r", {"failure": {"reason": "boom"}, "worker_id": "w1", "work_item_id": wid}, "f1"
        )
        self.assertEqual("running", state["status"])
        # The failure consumed an attempt and re-queued the target, so it is
        # claimable again.
        again = service.claim("r", {"worker_id": "w2", "scope": "target"}, "k2")
        self.assertEqual("t", again["work_item"]["node_id"])
        types = [event["type"] for event in service.events("r")]
        self.assertIn("node_failed", types)
        self.assertIn("node_retried", types)

    def test_settlement_writes_a_checkpoint(self):
        first = self.claim("w1", "k1")
        self.service.advance(
            "run-1",
            {"output": {}, "worker_id": "w1", "work_item_id": first["lease"]["work_item_id"]},
            "a1",
        )
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertTrue(checkpoints)
        self.assertEqual(["a"], checkpoints[-1]["state"]["completed_nodes"])


class TargetLeaseMapTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {"id": "collect", "kind": "task", "depends_on": []},
                    {
                        "id": "fan",
                        "kind": "map",
                        "depends_on": ["collect"],
                        "source": "collect",
                        "path": "items",
                        "max_instances": 10,
                        "template": {"id": "work"},
                    },
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"output": {"items": [10, 20]}}, "collect")

    def tearDown(self):
        self.directory.cleanup()

    def test_map_work_item_carries_map_and_index(self):
        first = self.service.claim("run-1", {"worker_id": "w1", "scope": "target"}, "k1")
        second = self.service.claim("run-1", {"worker_id": "w2", "scope": "target"}, "k2")
        self.assertEqual(
            {"execution_id": "run-1", "workflow_id": "orders", "node_id": "work", "map_id": "fan", "index": 0},
            first["work_item"],
        )
        self.assertEqual(1, second["work_item"]["index"])
        self.service.advance(
            "run-1",
            {"output": {"v": 10}, "worker_id": "w1", "work_item_id": first["lease"]["work_item_id"]},
            "i0",
        )
        self.service.advance(
            "run-1",
            {"output": {"v": 20}, "worker_id": "w2", "work_item_id": second["lease"]["work_item_id"]},
            "i1",
        )
        state = self.service.get_execution("run-1")
        self.assertEqual("completed", state["status"])
        self.assertEqual([{"v": 10}, {"v": 20}], state["outputs"]["fan"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class TargetLeaseApprovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["boss"]}},
                    {"id": "b", "kind": "task", "depends_on": []},
                    {"id": "after", "kind": "task", "depends_on": ["a"]},
                    {"id": "later", "kind": "task", "depends_on": ["b"]},
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_parked_approval_target_is_not_reclaimed_while_other_targets_settle(self):
        parked = self.service.claim("run-1", {"worker_id": "wa", "scope": "target"}, "ka")
        other = self.service.claim("run-1", {"worker_id": "wb", "scope": "target"}, "kb")
        # Settling a parks it at the approval point and releases its lease.
        state = self.service.advance(
            "run-1",
            {"output": {"x": 1}, "worker_id": "wa", "work_item_id": parked["lease"]["work_item_id"]},
            "aa",
        )
        self.assertEqual("a", state["waiting_approval"]["node_id"])
        # The other target settles independently.
        state = self.service.advance(
            "run-1",
            {"output": {"y": 2}, "worker_id": "wb", "work_item_id": other["lease"]["work_item_id"]},
            "bb",
        )
        self.assertEqual(["b"], state["completed_nodes"])
        # The parked task a is not claimable; b's successor is next instead.
        nxt = self.service.claim("run-1", {"worker_id": "wc", "scope": "target"}, "kc")
        self.assertEqual("later", nxt["work_item"]["node_id"])
        # Exactly one approval request was ever appended.
        requests = [event for event in self.service.events("run-1") if event["type"] == "approval_requested"]
        self.assertEqual(1, len(requests))
        # The decision resolves a and unlocks its successor.
        self.service.decision(
            "run-1", {"approver": "boss", "decision": "approved", "output": {"ok": True}}, "dec"
        )
        final = self.service.claim("run-1", {"worker_id": "wd", "scope": "target"}, "kd")
        self.assertEqual("after", final["work_item"]["node_id"])
        self.assertTrue(self.service.replay("run-1")["consistent"])


class ExecutionScopeUnchangedTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": [
                    {"id": "b", "kind": "task", "depends_on": []},
                    {"id": "a", "kind": "task", "depends_on": []},
                ],
            },
            "w1",
        )
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_omitted_scope_keeps_single_lease_and_lexicographic_order(self):
        claimed = self.service.claim("run-1", {"worker_id": "w1"}, "k1")
        # The execution-level lease keeps its legacy shape with no work_item_id.
        self.assertEqual(
            ["worker_id", "lease_seconds", "expires_at", "heartbeat_at"],
            list(claimed["lease"]),
        )
        # Advancing without a work item settles the lexicographically first
        # ready task (a, not b).
        state = self.service.advance("run-1", {"output": {}, "worker_id": "w1"}, "a1")
        self.assertEqual(["a"], state["completed_nodes"])

    def test_explicit_execution_scope_matches_legacy(self):
        claimed = self.service.claim("run-1", {"worker_id": "w1", "scope": "execution"}, "k1")
        self.assertEqual({"execution_id": "run-1", "workflow_id": "orders"}, claimed["work_item"])


if __name__ == "__main__":
    unittest.main()
