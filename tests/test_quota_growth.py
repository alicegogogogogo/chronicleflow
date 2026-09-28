import tempfile
import time
import unittest
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "t1", "kind": "task", "depends_on": []}]


class QuotaGrowthTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "growth.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_declaration_without_growth_is_the_baseline_shape(self):
        result = self.service.declare_quota({"workflows": 2, "executions": 5}, "q1", "alpha")
        self.assertEqual({"quota": {"workflows": 2, "executions": 5}}, result)
        self.assertEqual({"quota": {"workflows": 2, "executions": 5}}, self.service.get_quota("alpha"))

    def test_declared_policy_is_read_back_step_then_cap(self):
        body = {
            "workflows": 2,
            "executions": 5,
            "growth": {"workflows": {"step": 1, "cap": 9}, "executions": {"step": 5, "cap": 50}},
        }
        result = self.service.declare_quota(body, "q1", "alpha")
        self.assertEqual(["workflows", "executions", "growth"], list(result["quota"]))
        self.assertEqual(["step", "cap"], list(result["quota"]["growth"]["workflows"]))
        self.assertEqual(result["quota"], self.service.get_quota("alpha")["quota"])

    def test_policy_covers_only_named_resources(self):
        self.service.declare_quota(
            {"workflows": 1, "executions": 100,
             "growth": {"executions": {"step": 10, "cap": 200}}},
            "q", "alpha",
        )
        self.service.create_workflow({"id": "wf-1", "nodes": TASK}, "wf-1", "alpha")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2", "alpha")
        # executions auto-grow by whole steps
        for index in range(101):
            self.service.create_execution(
                {"id": f"r{index}", "workflow_id": "wf-1", "input": {}}, f"r{index}", "alpha"
            )
        self.assertEqual(110, self.service.quota_status("alpha")["status"]["executions"]["limit"])

    def test_limit_raises_in_whole_steps_until_it_admits_the_write(self):
        self.service.declare_quota(
            {"workflows": 10, "executions": 2,
             "growth": {"executions": {"step": 2, "cap": 6}}},
            "q", "alpha",
        )
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "alpha")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "alpha")
        self.service.create_execution({"id": "r3", "workflow_id": "wf", "input": {}}, "r3", "alpha")
        self.assertEqual(
            {"limit": 4, "held": 3, "remaining": 1},
            self.service.quota_status("alpha")["status"]["executions"],
        )

    def test_write_above_the_cap_conflicts_and_writes_nothing(self):
        self.service.declare_quota(
            {"workflows": 10, "executions": 1,
             "growth": {"executions": {"step": 2, "cap": 3}}},
            "q", "alpha",
        )
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "alpha")
        # held 1 == limit 1: a step of 2 raises to the cap 3, so r2 and r3 fit.
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "alpha")
        self.service.create_execution({"id": "r3", "workflow_id": "wf", "input": {}}, "r3", "alpha")
        # held 3 == cap 3: admitting one more would need limit 5 > cap 3.
        with self.assertRaises(ConflictError) as caught:
            self.service.create_execution({"id": "r4", "workflow_id": "wf", "input": {}}, "r4", "alpha")
        self.assertIn("quota", str(caught.exception))
        self.assertIn("executions", str(caught.exception))
        with self.assertRaises(NotFoundError):
            self.service.get_execution("r4", "alpha")
        self.assertEqual(3, self.service.quota_status("alpha")["status"]["executions"]["limit"])

    def test_redeclaration_replaces_limits_and_policy_together(self):
        self.service.declare_quota(
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 5}, "executions": {"step": 1, "cap": 5}}},
            "q1", "alpha",
        )
        # Omitting growth removes the policy even though the limits stay small.
        self.service.declare_quota({"workflows": 1, "executions": 1}, "q2", "alpha")
        self.assertEqual({"quota": {"workflows": 1, "executions": 1}}, self.service.get_quota("alpha"))
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2", "alpha")

    def test_delete_removes_limits_and_policy(self):
        self.service.declare_quota(
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 1, "cap": 5}}},
            "q1", "alpha",
        )
        self.assertEqual({"quota": None}, self.service.delete_quota({}, "d", "alpha"))
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))
        # Later writes run without a quota check and never resurrect a limit.
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2", "alpha")
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))

    def test_invalid_policy_is_a_validation_error_with_zero_writes(self):
        bad_bodies = [
            {"workflows": 1, "executions": 1, "growth": {}},
            {"workflows": 1, "executions": 1, "growth": {"schedules": {"step": 1, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 1}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 0, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": -1, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 2, "cap": 0}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": True, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 1.0, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 1, "cap": 2.0}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": "1"}},
            {"workflows": 1, "executions": 1, "growth": []},
            {"workflows": 1, "executions": 1, "growth": None},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2}, "extra": 1}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2, "extra": 3}}},
            {"workflows": 5, "executions": 1, "growth": {"workflows": {"step": 1, "cap": 4}}},
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.declare_quota(body, f"bad-{index}", "alpha")
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))

    def test_idempotent_replay_never_grows_twice(self):
        self.service.declare_quota(
            {"workflows": 10, "executions": 1,
             "growth": {"executions": {"step": 1, "cap": 10}}},
            "q", "alpha",
        )
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "r0", "workflow_id": "wf", "input": {}}, "r0", "alpha")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "k", "alpha")
        self.assertEqual(2, self.service.quota_status("alpha")["status"]["executions"]["limit"])
        # Replaying the same write returns the first result and does not raise again.
        replayed = self.service.create_execution(
            {"id": "r1", "workflow_id": "wf", "input": {}}, "k", "alpha"
        )
        self.assertEqual("r1", replayed["id"])
        self.assertEqual(2, self.service.quota_status("alpha")["status"]["executions"]["limit"])

    def test_cross_operation_key_reuse_conflicts_and_changes_nothing(self):
        self.service.declare_quota(
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 5}}},
            "shared", "alpha",
        )
        with self.assertRaises(ConflictError):
            self.service.delete_quota({}, "shared", "alpha")
        self.assertEqual(
            {"step": 1, "cap": 5}, self.service.get_quota("alpha")["quota"]["growth"]["workflows"]
        )

    def test_policies_and_limits_are_isolated_per_tenant(self):
        self.service.declare_quota(
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 5}}},
            "q", "alpha",
        )
        self.assertEqual({"quota": None}, self.service.get_quota("beta"))
        self.assertEqual({"status": None}, self.service.quota_status("beta"))
        # Beta's own policy-less quota blocks the same write alpha would grow for.
        self.service.declare_quota({"workflows": 1, "executions": 1}, "qb", "beta")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "beta")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2b", "beta")

    def test_schedule_firing_grows_the_execution_limit_then_blocks_at_cap(self):
        self.service.declare_quota(
            {"workflows": 10, "executions": 1,
             "growth": {"executions": {"step": 1, "cap": 2}}},
            "q", "alpha",
        )
        self.service.create_workflow(
            {"id": "wfs", "nodes": TASK,
             "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"}},
            "wfs", "alpha",
        )
        self.service.create_execution(
            {"id": "full", "workflow_id": "wfs", "input": {}}, "full", "alpha"
        )
        time.sleep(1.2)
        status = self.service.schedule_status("wfs", "alpha")
        self.assertIsNotNone(status["last_execution_id"])
        self.assertEqual(2, self.service.quota_status("alpha")["status"]["executions"]["limit"])
        fired_at, fired_id = status["last_triggered_at"], status["last_execution_id"]
        # The next period cannot be admitted at the cap: schedule stays as it was.
        time.sleep(1.2)
        blocked = self.service.schedule_status("wfs", "alpha")
        self.assertEqual((fired_at, fired_id), (blocked["last_triggered_at"], blocked["last_execution_id"]))
        self.assertEqual(2, self.service.quota_status("alpha")["status"]["executions"]["held"])


if __name__ == "__main__":
    unittest.main()
