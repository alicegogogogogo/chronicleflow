import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow


def nodes_v1():
    return [
        {"id": "reserve", "kind": "task", "depends_on": []},
        {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
    ]


def nodes_v2():
    return [
        {"id": "reserve", "kind": "task", "depends_on": []},
        {"id": "ship", "kind": "task", "depends_on": ["reserve"]},
        {"id": "notify", "kind": "task", "depends_on": ["ship"]},
    ]


class MigrationServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "migrate.db"))
        self.service.create_workflow(
            {"id": "orders", "version": "v1", "nodes": nodes_v1()}, "wf-v1"
        )
        self.service.create_workflow(
            {"id": "orders", "version": "v2", "nodes": nodes_v2()}, "wf-v2"
        )

    def tearDown(self):
        self.directory.cleanup()

    def start(self, execution_id="run-1", version="v1"):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": "orders", "version": version, "input": {}},
            f"ex-{execution_id}",
        )

    def test_migration_binds_target_and_keeps_history(self):
        self.start()
        first = self.service.advance("run-1", {"output": {"id": 1}}, "adv-1")
        self.assertEqual(["reserve"], first["completed_nodes"])
        migrated = self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        self.assertEqual("v2", migrated["version"])
        # Pre-migration history stays exactly as recorded.
        self.assertEqual(["reserve"], migrated["completed_nodes"])
        self.assertEqual({"reserve": {"id": 1}}, migrated["outputs"])

    def test_advancement_follows_target_definition(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        # v2 has ship where v1 had charge, plus an extra notify task.
        second = self.service.advance("run-1", {"output": {}}, "adv-2")
        self.assertEqual(["reserve", "ship"], second["completed_nodes"])
        done = self.service.advance("run-1", {"output": {}}, "adv-3")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "ship", "notify"], done["completed_nodes"])
        self.assertEqual("v2", done["version"])

    def test_migration_appends_event_and_checkpoint_at_same_boundary(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        events_before = self.service.events("run-1")
        checkpoints_before = len(self.service.checkpoints("run-1")["checkpoints"])
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        events = self.service.events("run-1")
        self.assertEqual(len(events_before) + 1, len(events))
        event = events[-1]
        self.assertEqual("version_migrated", event["type"])
        self.assertEqual({"from_version": "v1", "to_version": "v2"}, event["payload"])
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(checkpoints_before + 1, len(checkpoints))
        checkpoint = checkpoints[-1]
        self.assertEqual(event["sequence"], checkpoint["event_sequence"])
        self.assertEqual("v2", checkpoint["state"]["version"])

    def test_migration_event_is_visible_in_event_list(self):
        self.start()
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        types = [event["type"] for event in self.service.events("run-1")]
        self.assertIn("version_migrated", types)

    def test_same_version_is_a_noop(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        events_before = self.service.events("run-1")
        checkpoints_before = self.service.checkpoints("run-1")["checkpoints"]
        result = self.service.migrate("run-1", {"version": "v1"}, "mig-same")
        self.assertEqual("v1", result["version"])
        self.assertEqual(events_before, self.service.events("run-1"))
        self.assertEqual(checkpoints_before, self.service.checkpoints("run-1")["checkpoints"])

    def test_replay_rebuilds_migration_from_event_stream(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        self.service.advance("run-1", {"output": {}}, "adv-2")
        result = self.service.replay("run-1")
        self.assertTrue(result["consistent"])
        self.assertEqual("v2", result["execution"]["version"])

    def test_recovery_continues_on_migrated_version(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        database = str(Path(self.directory.name) / "migrate.db")
        resumed = ChronicleFlow(database)
        recovered = resumed.recover("run-1", {"from": "latest_checkpoint"}, "rec-1")
        self.assertEqual("v2", recovered["version"])
        done = resumed.advance("run-1", {"output": {}}, "adv-2")
        self.assertEqual(["reserve", "ship"], done["completed_nodes"])
        self.assertTrue(resumed.replay("run-1")["consistent"])

    def test_completed_execution_cannot_be_migrated(self):
        self.start()
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.advance("run-1", {"output": {}}, "adv-2")
        self.assertEqual("completed", self.service.get_execution("run-1")["status"])
        with self.assertRaises(ConflictError):
            self.service.migrate("run-1", {"version": "v2"}, "mig-done")
        events = [event["type"] for event in self.service.events("run-1")]
        self.assertNotIn("version_migrated", events)
        self.assertEqual("v1", self.service.get_execution("run-1")["version"])

    def test_terminated_execution_cannot_be_migrated(self):
        self.service.create_workflow(
            {"id": "flaky", "version": "v1", "nodes": [
                {"id": "task", "kind": "task", "depends_on": [], "retries": 0},
            ]},
            "wf-flaky",
        )
        self.service.create_workflow(
            {"id": "flaky", "version": "v2", "nodes": [
                {"id": "task", "kind": "task", "depends_on": [], "retries": 5},
            ]},
            "wf-flaky-v2",
        )
        self.service.create_execution(
            {"id": "run-x", "workflow_id": "flaky", "version": "v1", "input": {}}, "ex-x"
        )
        self.service.advance("run-x", {"failure": {"reason": "boom"}}, "fail-1")
        self.assertEqual("terminated", self.service.get_execution("run-x")["status"])
        with self.assertRaises(ConflictError):
            self.service.migrate("run-x", {"version": "v2"}, "mig-term")

    def test_missing_execution_and_version_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.migrate("ghost", {"version": "v1"}, "mig-ghost")
        self.start()
        with self.assertRaises(NotFoundError):
            self.service.migrate("run-1", {"version": "nope"}, "mig-nope")

    def test_invalid_bodies_are_validation_errors(self):
        self.start()
        for body in (
            None,
            [],
            {},
            {"version": "v2", "extra": 1},
            {"version": 5},
            {"version": True},
            {"version": ""},
            {"version": "v" * 101},
        ):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.migrate("run-1", body, f"mig-bad-{body!r}")

    def test_idempotency_key_reuse_across_operations_conflicts(self):
        self.start()
        self.service.migrate("run-1", {"version": "v2"}, "shared")
        repeated = self.service.migrate("run-1", {"version": "v2"}, "shared")
        self.assertEqual("v2", repeated["version"])
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "shared")

    def test_lease_survives_migration(self):
        self.start()
        self.service.claim("run-1", {"worker_id": "worker-1", "lease_seconds": 60}, "claim-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        lease = self.service.heartbeat("run-1", {"worker_id": "worker-1"}, "hb-1")["lease"]
        self.assertEqual("worker-1", lease["worker_id"])
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "adv-other")

    def test_migration_while_waiting_for_approval_keeps_the_point(self):
        self.service.create_workflow(
            {"id": "approvals", "version": "v1", "nodes": [
                {"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
            ]},
            "wf-appr-v1",
        )
        self.service.create_workflow(
            {"id": "approvals", "version": "v2", "nodes": [
                {"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
                {"id": "after", "kind": "task", "depends_on": ["gate"]},
            ]},
            "wf-appr-v2",
        )
        self.service.create_execution(
            {"id": "run-a", "workflow_id": "approvals", "version": "v1", "input": {}}, "ex-a"
        )
        self.service.advance("run-a", {"output": {}}, "park")
        migrated = self.service.migrate("run-a", {"version": "v2"}, "mig-a")
        waiting = migrated["waiting_approval"]
        self.assertEqual("gate", waiting["node_id"])
        self.assertEqual(["alice"], waiting["approvers"])
        # A decision by the recorded approver resolves the point; subsequent
        # progress follows the target version.
        decided = self.service.decision(
            "run-a", {"approver": "alice", "decision": "approved", "output": {}}, "dec-a"
        )
        self.assertEqual("running", decided["status"])
        done = self.service.advance("run-a", {"output": {}}, "after-1")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["gate", "after"], done["completed_nodes"])
        self.assertTrue(self.service.replay("run-a")["consistent"])

    def test_target_loop_is_introduced_at_migration(self):
        self.service.create_workflow(
            {"id": "loopy", "version": "v1", "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "gate", "kind": "task", "depends_on": ["prepare"]},
            ]},
            "wf-loop-v1",
        )
        self.service.create_workflow(
            {"id": "loopy", "version": "v2", "nodes": [
                {"id": "prepare", "kind": "task", "depends_on": []},
                {"id": "check", "kind": "condition", "depends_on": [], "path": "go", "equals": True},
                {"id": "attempt", "kind": "task", "depends_on": ["check"]},
                {
                    "id": "rounds",
                    "kind": "loop",
                    "depends_on": ["prepare"],
                    "entry": "attempt",
                    "condition": "check",
                    "max_iterations": 1,
                },
            ]},
            "wf-loop-v2",
        )
        self.service.create_execution(
            {"id": "run-l", "workflow_id": "loopy", "version": "v1", "input": {"go": True}}, "ex-l"
        )
        self.service.advance("run-l", {"output": {}}, "prep")
        migrated = self.service.migrate("run-l", {"version": "v2"}, "mig-l")
        self.assertEqual("pending", migrated["loops"]["rounds"]["status"])
        done = self.service.advance("run-l", {"output": {}}, "body-1")
        self.assertEqual("completed", done["status"])
        self.assertEqual("iteration_limit", done["loops"]["rounds"]["end_reason"])
        self.assertTrue(self.service.replay("run-l")["consistent"])

    def test_unmigrated_execution_stays_on_original_version(self):
        self.start("run-old", "v1")
        self.service.advance("run-old", {"output": {}}, "old-1")
        done = self.service.advance("run-old", {"output": {}}, "old-2")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "charge"], done["completed_nodes"])
        self.assertEqual("v1", done["version"])

    def test_migration_is_tenant_scoped(self):
        self.service.create_workflow(
            {"id": "tenant-wf", "version": "v1", "nodes": nodes_v1()}, "tw-v1", "alpha"
        )
        self.service.create_execution(
            {"id": "tr", "workflow_id": "tenant-wf", "version": "v1", "input": {}}, "te", "alpha"
        )
        with self.assertRaises(NotFoundError):
            self.service.migrate("tr", {"version": "v1"}, "other-key", "beta")


class MigrationHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        from chronicleflow.server import Handler

        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-migrate.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = json.dumps(body) if body is not None else None
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_migrate_over_http(self):
        wid, rid = "http-mig-wf", "http-mig-run"
        self.call("POST", "/workflows", {"id": wid, "version": "v1", "nodes": nodes_v1()}, f"{wid}-v1")
        self.call("POST", "/workflows", {"id": wid, "version": "v2", "nodes": nodes_v2()}, f"{wid}-v2")
        self.call("POST", "/executions", {"id": rid, "workflow_id": wid, "version": "v1", "input": {}}, f"{rid}-e")
        self.call("POST", f"/executions/{rid}/advance", {"output": {}}, f"{rid}-a1")
        status, data = self.call("POST", f"/executions/{rid}/migrate", {"version": "v2"}, f"{rid}-mig")
        self.assertEqual(200, status)
        self.assertEqual("v2", json.loads(data)["version"])
        self.assertTrue(data.endswith(b"\n") and not data.endswith(b"\n\n"))
        status, data = self.call("GET", f"/executions/{rid}/events")
        events = json.loads(data)["events"]
        self.assertTrue(any(event["type"] == "version_migrated" for event in events))


if __name__ == "__main__":
    unittest.main()
