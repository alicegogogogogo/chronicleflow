import http.client
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


def nodes_v1():
    return [
        {"id": "reserve", "kind": "task", "depends_on": []},
        {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
    ]


def nodes_v2():
    return nodes_v1() + [{"id": "ship", "kind": "task", "depends_on": ["charge"]}]


class VersionServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "versions.db"))

    def tearDown(self):
        self.directory.cleanup()

    def create(self, workflow_id, version, nodes, key=None, **extra):
        body = {"id": workflow_id, "nodes": nodes}
        if version is not None:
            body["version"] = version
        body.update(extra)
        return self.service.create_workflow(body, key or f"wf-{workflow_id}-{version}")

    def start(self, execution_id, workflow_id, version=None, key=None, **extra):
        body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
        if version is not None:
            body["version"] = version
        body.update(extra)
        return self.service.create_execution(body, key or f"ex-{execution_id}")

    def test_versioned_create_response_carries_tag_and_definition_lists_versions(self):
        created = self.create("orders", "v1", nodes_v1())
        self.assertEqual({"id": "orders", "nodes": nodes_v1(), "version": "v1"}, created)
        definition = self.service.get_workflow("orders")
        self.assertEqual("v1", definition["current_version"])
        self.assertEqual(
            [{"id": "orders", "nodes": nodes_v1(), "version": "v1"}], definition["versions"]
        )

    def test_versionless_keeps_baseline_shape(self):
        created = self.service.create_workflow({"id": "plain", "nodes": nodes_v1()}, "wf-plain")
        self.assertEqual({"id": "plain", "nodes": nodes_v1()}, created)
        # The definition view of a versionless workflow is a single untagged
        # revision with a null current version.
        definition = self.service.get_workflow("plain")
        self.assertIsNone(definition["current_version"])
        self.assertEqual([{"id": "plain", "nodes": nodes_v1()}], definition["versions"])
        state = self.service.create_execution(
            {"id": "run-plain", "workflow_id": "plain", "input": {}}, "ex-plain"
        )
        self.assertNotIn("version", state)

    def test_adding_version_keeps_history_and_changes_current(self):
        self.create("orders", "v1", nodes_v1())
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        definition = self.service.get_workflow("orders")
        self.assertEqual("v2", definition["current_version"])
        self.assertEqual(["v1", "v2"], [entry["version"] for entry in definition["versions"]])
        self.assertEqual(nodes_v1(), definition["versions"][0]["nodes"])
        self.assertEqual(nodes_v2(), definition["versions"][1]["nodes"])

    def test_duplicate_version_conflicts_and_writes_nothing(self):
        self.create("orders", "v1", nodes_v1())
        with self.assertRaises(ConflictError):
            self.create("orders", "v1", nodes_v2(), "wf-dup")
        definition = self.service.get_workflow("orders")
        self.assertEqual("v1", definition["current_version"])
        self.assertEqual(["v1"], [entry["version"] for entry in definition["versions"]])

    def test_untagged_post_to_existing_workflow_conflicts(self):
        self.create("orders", "v1", nodes_v1())
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "orders", "nodes": nodes_v2()}, "wf-untagged")

    def test_first_named_version_can_be_added_to_a_versionless_workflow(self):
        self.service.create_workflow({"id": "orders", "nodes": nodes_v1()}, "wf-plain")
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        definition = self.service.get_workflow("orders")
        self.assertEqual("v2", definition["current_version"])
        self.assertEqual([None, "v2"], [entry.get("version") for entry in definition["versions"]])

    def test_invalid_version_identifier_is_a_validation_error(self):
        for bad in (5, True, "", "x" * 101):
            with self.subTest(bad=bad):
                with self.assertRaises(ValidationError):
                    self.create("orders", bad, nodes_v1(), f"wf-bad-{bad!r}")

    def test_execution_binds_current_version_implicitly(self):
        self.create("orders", "v1", nodes_v1())
        self.start("run-1", "orders")
        self.assertEqual("v1", self.service.get_execution("run-1")["version"])
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        self.start("run-2", "orders", key="ex-run-2")
        self.assertEqual("v2", self.service.get_execution("run-2")["version"])
        # The first execution stays pinned to the version current at its start.
        self.assertEqual("v1", self.service.get_execution("run-1")["version"])

    def test_execution_can_name_any_existing_version(self):
        self.create("orders", "v1", nodes_v1())
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        state = self.start("run-old", "orders", version="v1")
        self.assertEqual("v1", state["version"])

    def test_starting_against_missing_workflow_or_version_is_not_found(self):
        self.create("orders", "v1", nodes_v1())
        with self.assertRaises(NotFoundError):
            self.start("run-a", "ghost")
        with self.assertRaises(NotFoundError):
            self.start("run-b", "orders", version="nope")

    def test_running_execution_keeps_advancing_on_old_version_after_upgrade(self):
        self.create("orders", "v1", nodes_v1())
        self.start("run-old", "orders")
        first = self.service.advance("run-old", {"output": {}}, "adv-old-1")
        self.assertEqual(["reserve"], first["completed_nodes"])
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        # The old version has two nodes, so it completes after a second
        # advance; the new third node never appears in this execution.
        done = self.service.advance("run-old", {"output": {}}, "adv-old-2")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "charge"], done["completed_nodes"])
        self.assertEqual("v1", done["version"])
        self.assertTrue(self.service.replay("run-old")["consistent"])

    def test_new_execution_after_upgrade_uses_new_version(self):
        self.create("orders", "v1", nodes_v1())
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        self.start("run-new", "orders")
        self.service.advance("run-new", {"output": {}}, "adv-new-1")
        self.service.advance("run-new", {"output": {}}, "adv-new-2")
        done = self.service.advance("run-new", {"output": {}}, "adv-new-3")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "charge", "ship"], done["completed_nodes"])
        self.assertEqual("v2", done["version"])

    def test_upgrade_does_not_move_old_execution_when_workflow_started_versionless(self):
        self.service.create_workflow({"id": "orders", "nodes": nodes_v1()}, "wf-plain")
        self.service.create_execution(
            {"id": "run-plain", "workflow_id": "orders", "input": {}}, "ex-plain"
        )
        self.service.advance("run-plain", {"output": {}}, "adv-plain-1")
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        done = self.service.advance("run-plain", {"output": {}}, "adv-plain-2")
        self.assertEqual("completed", done["status"])
        self.assertNotIn("version", done)
        self.assertTrue(self.service.replay("run-plain")["consistent"])

    def test_checkpoint_and_recovery_follow_bound_version(self):
        self.create("orders", "v1", nodes_v1())
        self.start("run-1", "orders")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual("v1", checkpoints[-1]["state"]["version"])
        recovered = self.service.recover("run-1", {"from": "latest_checkpoint"}, "rec-1")
        self.assertEqual("v1", recovered["version"])
        done = self.service.advance("run-1", {"output": {}}, "adv-2")
        self.assertEqual("completed", done["status"])

    def test_replay_rebuilds_binding_from_event_stream(self):
        self.create("orders", "v1", nodes_v1())
        self.start("run-1", "orders")
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        result = self.service.replay("run-1")
        self.assertTrue(result["consistent"])
        self.assertEqual("v1", result["execution"]["version"])
        events = self.service.events("run-1")
        self.assertEqual("v1", events[0]["payload"]["version"])

    def test_retries_follow_bound_version(self):
        self.create(
            "retry-wf",
            "v1",
            [{"id": "task", "kind": "task", "depends_on": [], "retries": 1}],
        )
        self.start("run-1", "retry-wf")
        # v2 declares no retries; the running execution must keep v1's retry.
        self.create(
            "retry-wf",
            "v2",
            [{"id": "task", "kind": "task", "depends_on": [], "retries": 0}],
            "wf-retry-v2",
        )
        state = self.service.advance("run-1", {"failure": {"reason": "boom"}}, "adv-fail")
        self.assertEqual("running", state["status"])
        self.assertEqual({"attempt": 2, "failures": 1}, state["attempts"]["task"])
        done = self.service.advance("run-1", {"output": {}}, "adv-ok")
        self.assertEqual("completed", done["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_approval_decided_after_upgrade_uses_bound_version(self):
        self.create(
            "approvals",
            "v1",
            [{"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}}],
        )
        self.start("run-1", "approvals")
        self.service.advance("run-1", {"output": {}}, "adv-park")
        self.assertEqual("gate", self.service.get_execution("run-1")["waiting_approval"]["node_id"])
        self.create(
            "approvals",
            "v2",
            [{"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["bob"]}}],
            "wf-approvals-v2",
        )
        # alice remains the approver of the v1-bound waiting point.
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1",
                {"approver": "bob", "decision": "approved", "output": {}},
                "dec-bob",
            )
        done = self.service.decision(
            "run-1",
            {"approver": "alice", "decision": "approved", "output": {"ok": True}},
            "dec-alice",
        )
        self.assertEqual("completed", done["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_idempotency_key_is_scoped_to_declared_version(self):
        self.create("orders", "v1", nodes_v1())
        first = self.create("orders", "v2", nodes_v2(), "shared-version-key")
        repeated = self.create("orders", "v2", nodes_v2(), "shared-version-key")
        self.assertEqual(first, repeated)
        with self.assertRaises(ConflictError):
            self.create("orders", "v3", nodes_v2(), "shared-version-key")

    def test_versioned_workflow_leases_keep_bound_definition(self):
        self.create("orders", "v1", nodes_v1())
        self.start("run-1", "orders")
        self.service.claim("run-1", {"worker_id": "worker-1", "lease_seconds": 30}, "claim-1")
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        state = self.service.advance(
            "run-1", {"output": {}, "worker_id": "worker-1"}, "adv-1"
        )
        self.assertEqual(["reserve"], state["completed_nodes"])
        self.assertEqual("v1", state["version"])

    def test_same_workflow_id_in_other_tenant_is_independent(self):
        self.create("orders", "v1", nodes_v1(), key="wf-alpha")
        self.service.create_workflow(
            {"id": "orders", "version": "other-1", "nodes": nodes_v1()}, "wf-beta", "beta"
        )
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("orders", "gamma")
        alpha = self.service.get_workflow("orders")
        beta = self.service.get_workflow("orders", "beta")
        self.assertEqual("v1", alpha["current_version"])
        self.assertEqual("other-1", beta["current_version"])

    def test_subscriptions_are_scoped_to_bound_version(self):
        receiver = _RecordingServer()
        receiver.start()
        try:
            self.create(
                "subs",
                "v1",
                nodes_v1(),
                "wf-subs-v1",
                subscriptions=[{"url": receiver.url, "events": ["node_completed"]}],
            )
            # v2 carries no subscriptions; absent subscriptions replace none.
            self.create("subs", "v2", nodes_v2(), "wf-subs-v2")
            self.start("run-v1", "subs", version="v1")
            self.start("run-v2", "subs", version="v2")
            self.service.advance("run-v1", {"output": {}}, "adv-v1-1")
            self.service.advance("run-v2", {"output": {}}, "adv-v2-1")
            self.assertEqual(1, len(self.service.deliveries("run-v1")["deliveries"]))
            self.assertEqual(0, len(self.service.deliveries("run-v2")["deliveries"]))
            self.assertEqual(1, len(receiver.requests))
        finally:
            receiver.shutdown()

    def test_scheduled_run_binds_current_version(self):
        self.create(
            "scheduled",
            "s1",
            nodes_v1(),
            "wf-scheduled",
            schedule={"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"},
        )
        deadline = time.time() + 5
        execution_id = None
        while time.time() < deadline:
            status = self.service.schedule_status("scheduled")["schedule"]
            execution_id = status["last_execution_id"]
            if execution_id:
                break
            time.sleep(0.05)
        self.assertIsNotNone(execution_id)
        self.assertEqual("s1", self.service.get_execution(execution_id)["version"])

    def test_pre_versioning_database_is_migrated(self):
        directory = tempfile.TemporaryDirectory()
        path = str(Path(directory.name) / "legacy.db")
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            CREATE TABLE workflows (
              tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL,
              document TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE executions (
              tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL,
              workflow_id TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE subscriptions (
              tenant TEXT NOT NULL DEFAULT '', owner_type TEXT NOT NULL,
              owner_id TEXT NOT NULL, position INTEGER NOT NULL, document TEXT NOT NULL,
              PRIMARY KEY (tenant, owner_type, owner_id, position));
            INSERT INTO workflows VALUES ('', 'wf',
              '{"id":"wf","nodes":[{"depends_on":[],"id":"a","kind":"task"}]}');
            INSERT INTO executions VALUES ('', 'run', 'wf', '{}');
            INSERT INTO subscriptions VALUES ('', 'workflow', 'wf', 0,
              '{"url":"http://example","events":["node_completed"],"timeout_seconds":5,"max_attempts":1}');
            """
        )
        connection.commit()
        connection.close()
        service = ChronicleFlow(path)
        try:
            definition = service.get_workflow("wf")
            self.assertIsNone(definition["current_version"])
            self.assertEqual(
                [{"id": "wf", "nodes": [{"depends_on": [], "id": "a", "kind": "task"}]}],
                definition["versions"],
            )
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
            CREATE TABLE workflows (
              tenant TEXT NOT NULL DEFAULT '', id TEXT NOT NULL,
              document TEXT NOT NULL, PRIMARY KEY (tenant, id));
            CREATE TABLE deliveries (
              tenant TEXT NOT NULL DEFAULT '', execution_id TEXT NOT NULL,
              sequence INTEGER NOT NULL, document TEXT NOT NULL);
            INSERT INTO workflows VALUES ('', 'wf', '{}');
            """
        )
        record = json.dumps(
            {
                "attempt_count": 2,
                "attempts": [{"attempt": 1, "status_code": 500}, {"attempt": 2, "error": "boom"}],
                "status": "failed",
            }
        )
        connection.execute("INSERT INTO deliveries VALUES ('', 'run', 1, ?)", (record,))
        connection.commit()
        connection.close()
        service = ChronicleFlow(path)
        try:
            document = json.loads(
                service.store.connection.execute("SELECT document FROM deliveries").fetchone()["document"]
            )
            self.assertEqual([{"status_code": 500}, {"error": "boom"}], document["attempts"])
            self.assertEqual("failed", document["status"])
            self.assertEqual(2, document["attempt_count"])
        finally:
            directory.cleanup()


class _RecordingServer:
    def __init__(self):
        class Receiver(BaseHTTPRequestHandler):
            def log_message(self, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                self.requests_attr.append(self.path)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.requests = []
        Receiver.requests_attr = self.requests
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/hook"
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def shutdown(self):
        self.server.shutdown()
        self.server.server_close()


class VersionHttpTests(unittest.TestCase):
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

    def call(self, method, path, body=None, key=None, raw=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_version_lifecycle_over_http(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "http-wf", "version": "v1", "nodes": nodes_v1()},
            "http-wf-v1",
        )
        self.assertEqual(201, status)
        self.assertEqual(
            {"id": "http-wf", "nodes": nodes_v1(), "version": "v1"}, json.loads(data)
        )
        self.assertTrue(data.endswith(b"\n"))
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "http-wf", "version": "v2", "nodes": nodes_v2()},
            "http-wf-v2",
        )
        self.assertEqual(201, status)
        status, data = self.call("GET", "/workflows/http-wf")
        self.assertEqual(200, status)
        definition = json.loads(data)
        self.assertEqual("v2", definition["current_version"])
        self.assertEqual(["v1", "v2"], [entry["version"] for entry in definition["versions"]])

    def test_versionless_definition_query(self):
        self.call("POST", "/workflows", {"id": "http-plain", "nodes": nodes_v1()}, "http-plain")
        status, data = self.call("GET", "/workflows/http-plain")
        self.assertEqual(200, status)
        definition = json.loads(data)
        self.assertIsNone(definition["current_version"])
        self.assertEqual([{"id": "http-plain", "nodes": nodes_v1()}], definition["versions"])

    def test_error_codes_over_http(self):
        self.call("POST", "/workflows", {"id": "http-err", "version": "v1", "nodes": nodes_v1()}, "http-err-v1")
        cases = [
            ("POST", "/workflows", {"id": "http-err", "version": "v1", "nodes": nodes_v2()}, "dup-key", 409),
            ("POST", "/workflows", {"id": "http-err", "nodes": nodes_v2()}, "untagged-key", 409),
            ("POST", "/workflows", {"id": "http-err", "version": 7, "nodes": nodes_v2()}, "bad-type", 400),
            (
                "POST",
                "/workflows",
                {"id": "http-err", "version": "v3", "nodes": nodes_v2(), "extra": 1},
                "unknown-field",
                400,
            ),
            ("POST", "/executions", {"id": "x1", "workflow_id": "http-missing", "input": {}}, "missing-wf", 404),
            (
                "POST",
                "/executions",
                {"id": "x2", "workflow_id": "http-err", "version": "nope", "input": {}},
                "missing-version",
                404,
            ),
            (
                "POST",
                "/executions",
                {"id": "x3", "workflow_id": "http-err", "version": 9, "input": {}},
                "bad-exec-version",
                400,
            ),
        ]
        for method, path, body, key, expected in cases:
            with self.subTest(key=key):
                status, data = self.call(method, path, body, key)
                self.assertEqual(expected, status, data)

    def test_non_finite_versioned_body_is_rejected(self):
        status, data = self.call(
            "POST",
            "/workflows",
            raw=(
                b'{"id":"http-nan","version":"v1","nodes":'
                b'[{"id":"a","kind":"task","depends_on":[]}],"subscriptions":'
                b'[{"url":"http://127.0.0.1/h","events":["node_completed"],"timeout_seconds":NaN}]}'
            ),
            key="http-nan",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_definition_query_is_tenant_scoped(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "http-tenant", "version": "v1", "nodes": nodes_v1()},
            "http-tenant-v1",
        )
        status, data = self.call(
            "GET", "/workflows/http-tenant", headers={"X-Tenant-Id": "other"}
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_old_execution_advances_and_replays_on_bound_version_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "http-flow", "version": "v1", "nodes": nodes_v1()},
            "http-flow-v1",
        )
        self.call(
            "POST",
            "/executions",
            {"id": "http-run", "workflow_id": "http-flow", "input": {}},
            "http-run",
        )
        self.call(
            "POST",
            "/workflows",
            {"id": "http-flow", "version": "v2", "nodes": nodes_v2()},
            "http-flow-v2",
        )
        self.call("POST", "/executions/http-run/advance", {"output": {}}, "http-adv-1")
        status, data = self.call("POST", "/executions/http-run/advance", {"output": {}}, "http-adv-2")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("completed", state["status"])
        self.assertEqual("v1", state["version"])
        status, data = self.call("POST", "/executions/http-run/replay")
        self.assertTrue(json.loads(data)["consistent"])


if __name__ == "__main__":
    unittest.main()
