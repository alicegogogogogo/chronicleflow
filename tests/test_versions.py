import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow

TASK_A = [{"id": "a", "kind": "task", "depends_on": []}]
V1 = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]
V2 = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "c", "kind": "task", "depends_on": ["a"]},
]


class WorkflowVersionServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "versions.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_create_with_version_carries_tag_and_definition_query(self):
        stored = self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK_A}, "k1")
        self.assertEqual({"id": "wf", "nodes": TASK_A, "version": "v1"}, stored)
        definition = self.service.get_workflow("wf")
        self.assertEqual("v1", definition["current_version"])
        self.assertEqual([{"id": "wf", "nodes": TASK_A, "version": "v1"}], definition["versions"])

    def test_unversioned_workflow_has_single_untagged_revision(self):
        stored = self.service.create_workflow({"id": "plain", "nodes": TASK_A}, "k1")
        self.assertEqual({"id": "plain", "nodes": TASK_A}, stored)
        definition = self.service.get_workflow("plain")
        self.assertIsNone(definition["current_version"])
        self.assertEqual([{"id": "plain", "nodes": TASK_A}], definition["versions"])

    def test_adding_a_version_keeps_history_and_moves_current(self):
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": V1}, "k1")
        self.service.create_workflow({"id": "wf", "version": "v2", "nodes": V2}, "k2")
        definition = self.service.get_workflow("wf")
        self.assertEqual("v2", definition["current_version"])
        self.assertEqual(["v1", "v2"], [version["version"] for version in definition["versions"]])
        self.assertEqual(V1, definition["versions"][0]["nodes"])
        self.assertEqual(V2, definition["versions"][1]["nodes"])

    def test_duplicate_version_tag_conflicts(self):
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK_A}, "k1")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf", "version": "v1", "nodes": V2}, "k2")
        # The conflicting write added nothing.
        definition = self.service.get_workflow("wf")
        self.assertEqual(["v1"], [version["version"] for version in definition["versions"]])

    def test_versionless_post_to_existing_workflow_still_conflicts(self):
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK_A}, "k1")
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf", "nodes": V2}, "k2")

    def test_invalid_version_tags_are_validation_errors(self):
        for index, version in enumerate(("", 7, True, ["v1"], None)):
            with self.subTest(version=version):
                with self.assertRaises(ValidationError):
                    self.service.create_workflow(
                        {"id": f"wf-bad-{index}", "version": version, "nodes": TASK_A},
                        f"k-bad-{index}",
                    )

    def test_missing_workflow_and_missing_version_are_not_found(self):
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": TASK_A}, "k1")
        with self.assertRaises(NotFoundError):
            self.service.create_execution(
                {"id": "run-missing-workflow", "workflow_id": "ghost", "input": {}}, "k2"
            )
        with self.assertRaises(NotFoundError):
            self.service.create_execution(
                {"id": "run-missing-version", "workflow_id": "wf", "version": "v9", "input": {}}, "k3"
            )
        # No partial writes from either rejection.
        with self.assertRaises(NotFoundError):
            self.service.get_execution("run-missing-version")

    def test_definition_query_of_missing_workflow_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("ghost")


class VersionBindingServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "binding.db"))
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": V1}, "w1")

    def tearDown(self):
        self.directory.cleanup()

    def upgrade(self):
        self.service.create_workflow({"id": "wf", "version": "v2", "nodes": V2}, "w2")

    def start(self, execution_id, version=None, key=None):
        body = {"id": execution_id, "workflow_id": "wf", "input": {}}
        if version is not None:
            body["version"] = version
        return self.service.create_execution(body, key or f"e-{execution_id}")

    def test_execution_binds_current_version_at_start(self):
        state = self.start("run-current")
        self.assertEqual("v1", state["version"])
        self.assertEqual("v1", self.service.events("run-current")[0]["payload"]["version"])
        self.upgrade()
        later = self.start("run-later")
        self.assertEqual("v2", later["version"])

    def test_named_version_binds_even_after_upgrade(self):
        self.upgrade()
        state = self.start("run-old", version="v1")
        self.assertEqual("v1", state["version"])
        self.service.advance("run-old", {"output": {}}, "a1")
        completed = self.service.advance("run-old", {"output": {}}, "a2")
        # v1 ends with node b, which does not exist in v2.
        self.assertEqual("completed", completed["status"])
        self.assertEqual(["a", "b"], completed["completed_nodes"])
        self.assertTrue(self.service.replay("run-old")["consistent"])

    def test_running_execution_keeps_old_version_after_upgrade(self):
        self.start("run-old")
        self.service.advance("run-old", {"output": {}}, "a1")
        self.upgrade()
        # The v1 execution completes v1's node b; it never sees v2's node c.
        completed = self.service.advance("run-old", {"output": {}}, "a2")
        self.assertEqual("completed", completed["status"])
        self.assertEqual(["a", "b"], completed["completed_nodes"])
        self.assertEqual("v1", completed["version"])
        replayed = self.service.replay("run-old")
        self.assertTrue(replayed["consistent"])
        self.assertEqual("v1", replayed["execution"]["version"])
        # A fresh execution uses the new current version.
        self.start("run-new")
        self.service.advance("run-new", {"output": {}}, "b1")
        completed_new = self.service.advance("run-new", {"output": {}}, "b2")
        self.assertEqual(["a", "c"], completed_new["completed_nodes"])
        self.assertEqual("v2", completed_new["version"])

    def test_checkpoint_recovery_uses_bound_version_across_upgrade(self):
        self.start("run-cp")
        self.service.advance("run-cp", {"output": {"v": 1}}, "a1")
        self.upgrade()
        recovered = self.service.recover("run-cp", {"from": "latest_checkpoint"}, "rec1")
        self.assertEqual(["a"], recovered["completed_nodes"])
        self.assertEqual("v1", recovered["version"])
        completed = self.service.advance("run-cp", {"output": {"v": 2}}, "a2")
        self.assertEqual(["a", "b"], completed["completed_nodes"])
        checkpoints = self.service.checkpoints("run-cp")["checkpoints"]
        self.assertTrue(all(point["state"].get("version") == "v1" for point in checkpoints))

    def test_approval_parked_before_upgrade_resolves_with_old_definition(self):
        nodes = [
            {"id": "a", "kind": "task", "depends_on": []},
            {"id": "b", "kind": "task", "depends_on": ["a"], "approval": {"approvers": ["alice"]}},
        ]
        self.service.create_workflow({"id": "wf-app", "version": "v1", "nodes": nodes}, "wa1")
        state = self.service.create_execution({"id": "run-app", "workflow_id": "wf-app", "input": {}}, "ea1")
        self.assertEqual("v1", state["version"])
        self.service.advance("run-app", {"output": {}}, "ax1")
        parked = self.service.advance("run-app", {"output": {}}, "ax2")
        self.assertEqual("b", parked["waiting_approval"]["node_id"])
        # v2 drops the approval point entirely; the parked execution still
        # resolves with the v1 definition it is bound to.
        self.service.create_workflow(
            {"id": "wf-app", "version": "v2", "nodes": TASK_A}, "wa2"
        )
        decided = self.service.decision(
            "run-app", {"approver": "alice", "decision": "approved", "output": {"ok": True}}, "d1"
        )
        self.assertEqual("completed", decided["status"])
        self.assertEqual(["a", "b"], decided["completed_nodes"])
        self.assertTrue(self.service.replay("run-app")["consistent"])

    def test_retries_follow_bound_version(self):
        nodes = [{"id": "a", "kind": "task", "depends_on": [], "retries": 1}]
        nodes_v2 = [{"id": "a", "kind": "task", "depends_on": [], "retries": 0}]
        self.service.create_workflow({"id": "wf-r", "version": "v1", "nodes": nodes}, "wr1")
        self.service.create_workflow({"id": "wf-r", "version": "v2", "nodes": nodes_v2}, "wr2")
        old = self.service.create_execution(
            {"id": "run-r1", "workflow_id": "wf-r", "version": "v1", "input": {}}, "er1"
        )
        self.assertEqual("v1", old["version"])
        failed_once = self.service.advance("run-r1", {"failure": {"reason": "boom"}}, "arf")
        self.assertEqual("running", failed_once["status"])
        self.assertEqual({"a": {"attempt": 2, "failures": 1}}, failed_once["attempts"])
        new = self.service.create_execution(
            {"id": "run-r2", "workflow_id": "wf-r", "input": {}}, "er2"
        )
        self.assertEqual("v2", new["version"])
        terminated = self.service.advance("run-r2", {"failure": {"reason": "boom"}}, "arf2")
        self.assertEqual("terminated", terminated["status"])
        self.assertEqual("retries_exhausted", terminated["termination_reason"])

    def test_leases_govern_the_bound_execution_after_upgrade(self):
        self.start("run-lease")
        self.service.claim("run-lease", {"worker_id": "w1", "lease_seconds": 30}, "cl1")
        self.upgrade()
        with self.assertRaises(ConflictError):
            self.service.advance("run-lease", {"output": {}}, "a-anon")
        advanced = self.service.advance("run-lease", {"output": {}, "worker_id": "w1"}, "a1")
        self.assertEqual("v1", advanced["version"])

    def test_version_addition_idempotency(self):
        first = self.service.create_workflow({"id": "wf", "version": "v2", "nodes": V2}, "shared-version-key")
        repeated = self.service.create_workflow({"id": "wf", "version": "v2", "nodes": V2}, "shared-version-key")
        self.assertEqual(first, repeated)
        # Reusing the key for a different version of the same workflow conflicts.
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf", "version": "v3", "nodes": V1}, "shared-version-key")
        # Reusing the key for another operation conflicts as well.
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "shared-version-key")

    def test_versions_are_scoped_per_tenant(self):
        other = self.service.create_workflow({"id": "wf", "nodes": V1}, "k-beta", "beta")
        self.assertEqual({"id": "wf", "nodes": V1}, other)
        definition = self.service.get_workflow("wf", "beta")
        self.assertIsNone(definition["current_version"])
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("wf", "gamma")
        # Beta can use alpha's version tag independently.
        beta_v1 = self.service.create_workflow(
            {"id": "wf", "version": "v1", "nodes": V2}, "k-beta-v1", "beta"
        )
        self.assertEqual("v1", beta_v1["version"])
        self.assertEqual(["v1"], [v["version"] for v in self.service.get_workflow("wf")["versions"]])
        self.assertIsNone(self.service.get_workflow("wf", "beta")["versions"][0].get("version"))
        self.assertEqual("v1", self.service.get_workflow("wf", "beta")["versions"][1]["version"])


class UnversionedWorkflowUnchangedTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "unversioned.db"))
        self.service.create_workflow({"id": "wf", "nodes": TASK_A}, "w1")
        self.state = self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "e1")

    def tearDown(self):
        self.directory.cleanup()

    def test_state_shape_and_events_gain_no_fields(self):
        self.assertNotIn("version", self.state)
        events = self.service.events("run")
        self.assertEqual(["execution_started"], [event["type"] for event in events])
        self.assertNotIn("version", events[0]["payload"])
        completed = self.service.advance("run", {"output": {"v": -0.0}}, "a1")
        self.assertNotIn("version", completed)
        self.assertTrue(self.service.replay("run")["consistent"])

    def test_unversioned_workflow_can_still_be_upgraded_and_old_execution_binds_untagged_revision(self):
        self.service.advance("run", {"output": {}}, "a1")
        self.service.create_workflow({"id": "wf", "version": "v1", "nodes": V2}, "w2")
        # The pre-upgrade execution follows the preserved untagged revision.
        completed = self.service.advance("run", {"output": {}}, "a2")
        self.assertEqual("completed", completed["status"])
        self.assertNotIn("version", completed)
        self.assertTrue(self.service.replay("run")["consistent"])
        definition = self.service.get_workflow("wf")
        self.assertEqual("v1", definition["current_version"])
        self.assertIsNone(definition["versions"][0].get("version"))
        self.assertEqual(TASK_A, definition["versions"][0]["nodes"])


class LegacyDatabaseMigrationTests(unittest.TestCase):
    def test_pre_versioning_database_is_migrated(self):
        directory = tempfile.TemporaryDirectory()
        path = str(Path(directory.name) / "legacy.db")
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            CREATE TABLE workflows (tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE executions (tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL, workflow_id TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE subscriptions (tenant TEXT NOT NULL DEFAULT '', owner_type TEXT NOT NULL, owner_id TEXT NOT NULL, position INTEGER NOT NULL, document TEXT NOT NULL, PRIMARY KEY (tenant, owner_type, owner_id, position));
            INSERT INTO workflows VALUES ('', 'wf', '{"id":"wf","nodes":[{"depends_on":[],"id":"a","kind":"task"}]}');
            INSERT INTO executions VALUES ('', 'run', 'wf', '{}');
            INSERT INTO subscriptions VALUES ('', 'workflow', 'wf', 0, '{"url":"http://example","events":["node_completed"],"timeout_seconds":5,"max_attempts":1}');
            """
        )
        connection.commit()
        connection.close()
        service = ChronicleFlow(path)
        try:
            definition = service.get_workflow("wf")
            self.assertIsNone(definition["current_version"])
            self.assertEqual([{"id": "wf", "nodes": TASK_A}], definition["versions"])
            pairs = service._subscriptions_for("run", "")
            self.assertEqual(1, len(pairs))
            self.assertEqual("http://example", pairs[0][1]["url"])
        finally:
            directory.cleanup()


    def test_historical_delivery_attempts_are_aligned_with_the_documented_shape(self):
        directory = tempfile.TemporaryDirectory()
        path = str(Path(directory.name) / "deliveries.db")
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            CREATE TABLE workflows (tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE deliveries (tenant TEXT NOT NULL DEFAULT '', execution_id TEXT NOT NULL, sequence INTEGER NOT NULL, document TEXT NOT NULL);
            INSERT INTO workflows VALUES ('', 'wf', '{}');
            INSERT INTO deliveries VALUES ('', 'run', 1, '{"attempt_count":2,"attempts":[{"attempt":1,"status_code":500},{"attempt":2,"error":"boom"}],"status":"failed"}');
            """
        )
        connection.commit()
        connection.close()
        store = ChronicleFlow(path).store
        document = json.loads(store.connection.execute("SELECT document FROM deliveries").fetchone()["document"])
        self.assertEqual([{"status_code": 500}, {"error": "boom"}], document["attempts"])
        self.assertEqual("failed", document["status"])
        self.assertEqual(2, document["attempt_count"])
        directory.cleanup()


class WorkflowVersionHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-versions.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_version_lifecycle_over_http(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "wf-http", "version": "v1", "nodes": V1},
            "wh1",
        )
        self.assertEqual(201, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual("v1", json.loads(data)["version"])
        # Definition query shows the version list and current version.
        status, data = self.call("GET", "/workflows/wf-http")
        self.assertEqual(200, status)
        definition = json.loads(data)
        self.assertEqual("v1", definition["current_version"])
        self.assertEqual(["v1"], [v["version"] for v in definition["versions"]])
        self.assertEqual(["current_version", "id", "versions"], list(definition))
        # Add a second version.
        status, _ = self.call("POST", "/workflows", {"id": "wf-http", "version": "v2", "nodes": V2}, "wh2")
        self.assertEqual(201, status)
        status, data = self.call("GET", "/workflows/wf-http")
        self.assertEqual(["v1", "v2"], [v["version"] for v in json.loads(data)["versions"]])
        # Duplicate tag is a conflict.
        status, data = self.call("POST", "/workflows", {"id": "wf-http", "version": "v1", "nodes": V2}, "wh3")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_execution_binds_and_advances_on_old_version_after_upgrade(self):
        self.call("POST", "/workflows", {"id": "wf-bind", "version": "v1", "nodes": V1}, "wb1")
        status, data = self.call(
            "POST", "/executions", {"id": "run-bind", "workflow_id": "wf-bind", "input": {}}, "eb1"
        )
        self.assertEqual(201, status)
        self.assertEqual("v1", json.loads(data)["version"])
        self.call("POST", "/executions/run-bind/advance", {"output": {}}, "ab1")
        self.call("POST", "/workflows", {"id": "wf-bind", "version": "v2", "nodes": V2}, "wb2")
        status, data = self.call("POST", "/executions/run-bind/advance", {"output": {}}, "ab2")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("completed", state["status"])
        self.assertEqual(["a", "b"], state["completed_nodes"])
        self.assertEqual("v1", state["version"])
        status, data = self.call("POST", "/executions/run-bind/replay")
        self.assertEqual(200, status)
        replayed = json.loads(data)
        self.assertTrue(replayed["consistent"])
        self.assertEqual("v1", replayed["execution"]["version"])

    def test_missing_version_is_not_found(self):
        self.call("POST", "/workflows", {"id": "wf-miss", "version": "v1", "nodes": TASK_A}, "wm1")
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-miss", "workflow_id": "wf-miss", "version": "ghost", "input": {}},
            "em1",
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, _ = self.call("GET", "/workflows/ghost")
        self.assertEqual(404, status)

    def test_invalid_version_is_validation_error(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "wf-badver", "version": "", "nodes": TASK_A},
            "wbad1",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST",
            "/executions",
            raw=b'{"id":"run-badver","workflow_id":"wf-miss","version":5,"input":{}}',
            key="ebad1",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_unknown_fields_still_rejected(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "wf-extra", "version": "v1", "nodes": TASK_A, "extra": 1},
            "we1",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
