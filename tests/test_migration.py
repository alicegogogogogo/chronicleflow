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


def nodes_v1():
    return [
        {"id": "reserve", "kind": "task", "depends_on": []},
        {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
    ]


def nodes_v2():
    return nodes_v1() + [{"id": "ship", "kind": "task", "depends_on": ["charge"]}]


class MigrationServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "migrate.db")
        self.service = ChronicleFlow(self.database)

    def tearDown(self):
        self.directory.cleanup()

    def create(self, workflow_id, version, nodes, key=None, **extra):
        body = {"id": workflow_id, "nodes": nodes}
        if version is not None:
            body["version"] = version
        body.update(extra)
        return self.service.create_workflow(body, key or f"wf-{workflow_id}-{version}")

    def start(self, execution_id, workflow_id, version=None, key=None):
        body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
        if version is not None:
            body["version"] = version
        return self.service.create_execution(body, key or f"ex-{execution_id}")

    def versions(self, workflow_id="orders"):
        self.create(workflow_id, "v1", nodes_v1())
        self.create(workflow_id, "v2", nodes_v2(), f"wf-{workflow_id}-v2")

    def test_migration_appends_event_checkpoint_and_rebinds_state(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.advance("run-1", {"output": {"r": 1}}, "adv-1")
        state = self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        self.assertEqual("v2", state["version"])
        # Prior conclusions are untouched.
        self.assertEqual(["reserve"], state["completed_nodes"])
        self.assertEqual({"reserve": {"r": 1}}, state["outputs"])
        event = self.service.events("run-1")[-1]
        self.assertEqual("version_migrated", event["type"])
        self.assertEqual({"from_version": "v1", "to_version": "v2"},
                         {k: event["payload"][k] for k in ("from_version", "to_version")})
        checkpoints = self.service.checkpoints("run-1")["checkpoints"]
        self.assertEqual(event["sequence"], checkpoints[-1]["event_sequence"])
        self.assertEqual("v2", checkpoints[-1]["state"]["version"])

    def test_advancement_after_migration_follows_target_definition(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        self.service.advance("run-1", {"output": {}}, "adv-2")
        done = self.service.advance("run-1", {"output": {}}, "adv-3")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "charge", "ship"], done["completed_nodes"])
        self.assertEqual("v2", done["version"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_replay_rebuilds_migration_point_from_event_stream(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        result = self.service.replay("run-1")
        self.assertTrue(result["consistent"])
        self.assertEqual("v2", result["execution"]["version"])
        self.assertEqual(["reserve"], result["execution"]["completed_nodes"])
        self.assertIn("version_migrated", [event["type"] for event in self.service.events("run-1")])

    def test_recovery_after_restart_continues_on_target_version(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        self.service.migrate("run-1", {"version": "v2"}, "mig-1")
        resumed = ChronicleFlow(self.database)
        recovered = resumed.recover("run-1", {"from": "latest_checkpoint"}, "rec-1")
        self.assertEqual("v2", recovered["version"])
        resumed.advance("run-1", {"output": {}}, "adv-2")
        done = resumed.advance("run-1", {"output": {}}, "adv-3")
        self.assertEqual(["reserve", "charge", "ship"], done["completed_nodes"])
        self.assertTrue(resumed.replay("run-1")["consistent"])

    def test_same_version_is_a_noop_without_event_or_checkpoint(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.advance("run-1", {"output": {}}, "adv-1")
        events_before = len(self.service.events("run-1"))
        checkpoints_before = len(self.service.checkpoints("run-1")["checkpoints"])
        state = self.service.migrate("run-1", {"version": "v1"}, "mig-same")
        self.assertEqual("v1", state["version"])
        self.assertEqual(events_before, len(self.service.events("run-1")))
        self.assertEqual(checkpoints_before, len(self.service.checkpoints("run-1")["checkpoints"]))

    def test_completed_or_terminated_execution_conflicts(self):
        self.versions()
        self.start("run-done", "orders", version="v1")
        self.service.advance("run-done", {"output": {}}, "d1")
        self.service.advance("run-done", {"output": {}}, "d2")
        with self.assertRaises(ConflictError):
            self.service.migrate("run-done", {"version": "v2"}, "mig-done")
        self.start("run-term", "orders", version="v1", key="ex-run-term")
        self.service.cancel("run-term", "cancel-run-term")
        with self.assertRaises(ConflictError):
            self.service.migrate("run-term", {"version": "v2"}, "mig-term")
        # Nothing was written.
        self.assertNotIn("version_migrated",
                         [event["type"] for event in self.service.events("run-done")])
        self.assertNotIn("version_migrated",
                         [event["type"] for event in self.service.events("run-term")])

    def test_missing_execution_or_version_is_not_found(self):
        self.versions()
        with self.assertRaises(NotFoundError):
            self.service.migrate("ghost", {"version": "v1"}, "mig-ghost")
        self.start("run-1", "orders", version="v1")
        with self.assertRaises(NotFoundError):
            self.service.migrate("run-1", {"version": "nope"}, "mig-bad-version")

    def test_invalid_bodies_are_validation_errors(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        bad_bodies = [
            {},
            {"version": "v1", "extra": 1},
            {"version": 7},
            {"version": True},
            {"version": ""},
            {"version": "x" * 101},
            ["v1"],
            "v1",
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.migrate("run-1", body, f"mig-bad-{index}")

    def test_non_finite_number_is_rejected_even_with_valid_fields(self):
        # The HTTP layer rejects NaN at parse time; at the service boundary an
        # unknown field still makes an auxiliary float a validation error.
        self.versions()
        self.start("run-1", "orders", version="v1")
        with self.assertRaises(ValidationError):
            self.service.migrate("run-1", {"version": "v2", "n": float("nan")}, "mig-nan")

    def test_idempotent_migration_replays_result(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        first = self.service.migrate("run-1", {"version": "v2"}, "shared")
        repeated = self.service.migrate("run-1", {"version": "v2"}, "shared")
        self.assertEqual(first, repeated)
        self.assertEqual(1, [event["type"] for event in self.service.events("run-1")].count("version_migrated"))

    def test_idempotency_key_reuse_across_operations_conflicts(self):
        self.versions()
        self.start("run-1", "orders", version="v1")
        self.service.migrate("run-1", {"version": "v2"}, "shared")
        with self.assertRaises(ConflictError):
            self.service.advance("run-1", {"output": {}}, "shared")
        with self.assertRaises(ConflictError):
            self.service.cancel("run-1", "shared")

    def test_migration_to_different_target_with_same_key_conflicts(self):
        self.create("orders", "v1", nodes_v1())
        self.create("orders", "v2", nodes_v2(), "wf-orders-v2")
        self.create(
            "orders",
            "v3",
            nodes_v2() + [{"id": "review", "kind": "task", "depends_on": ["ship"]}],
            "wf-orders-v3",
        )
        self.start("run-1", "orders", version="v1")
        self.service.migrate("run-1", {"version": "v2"}, "shared")
        with self.assertRaises(ConflictError):
            self.service.migrate("run-1", {"version": "v3"}, "shared")

    def test_unmigrated_execution_keeps_old_version_to_completion(self):
        self.versions()
        self.start("run-old", "orders", version="v1")
        self.service.advance("run-old", {"output": {}}, "o1")
        # A sibling execution migrates; it must not affect run-old.
        self.start("run-moving", "orders", version="v1", key="ex-run-moving")
        self.service.migrate("run-moving", {"version": "v2"}, "mig-moving")
        done = self.service.advance("run-old", {"output": {}}, "o2")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["reserve", "charge"], done["completed_nodes"])
        self.assertEqual("v1", done["version"])

    def test_waiting_approval_keeps_recorded_point_after_migration(self):
        v1 = [{"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
              {"id": "after", "kind": "task", "depends_on": ["gate"]}]
        v2 = [{"id": "gate", "kind": "task", "depends_on": [], "approval": {"approvers": ["bob"]}},
              {"id": "after", "kind": "task", "depends_on": ["gate"]},
              {"id": "tail", "kind": "task", "depends_on": ["after"]}]
        self.create("approvals", "v1", v1)
        self.create("approvals", "v2", v2, "wf-approvals-v2")
        self.start("run-1", "approvals", version="v1")
        self.service.advance("run-1", {"output": {}}, "park")
        state = self.service.migrate("run-1", {"version": "v2"}, "mig")
        self.assertEqual("v2", state["version"])
        waiting = state["waiting_approval"]
        self.assertEqual("gate", waiting["node_id"])
        self.assertEqual(["alice"], waiting["approvers"])
        # The v2 approver still cannot decide the recorded point.
        with self.assertRaises(ConflictError):
            self.service.decision(
                "run-1", {"approver": "bob", "decision": "approved", "output": {}}, "dec-bob"
            )
        decided = self.service.decision(
            "run-1", {"approver": "alice", "decision": "approved", "output": {"ok": True}}, "dec-alice"
        )
        # After the recorded decision, advancement follows v2 (tail exists).
        self.service.advance("run-1", {"output": {}}, "adv-after")
        done = self.service.advance("run-1", {"output": {}}, "adv-tail")
        self.assertEqual("completed", done["status"])
        self.assertEqual(["gate", "after", "tail"], done["completed_nodes"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_loop_rounds_follow_target_version_after_migration(self):
        loop = lambda limit: [
            {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
            {"id": "attempt", "kind": "task", "depends_on": ["check"]},
            {"id": "loop", "kind": "loop", "depends_on": [], "entry": "attempt",
             "condition": "check", "max_iterations": limit},
        ]
        self.create("loopy", "v1", loop(2))
        self.create("loopy", "v2", loop(3), "wf-loopy-v2")
        self.service.create_execution(
            {"id": "run-l", "workflow_id": "loopy", "version": "v1", "input": {"again": True}},
            "ex-run-l",
        )
        self.service.advance("run-l", {"output": {}}, "la0")  # first attempt; iteration 2 starts
        self.service.migrate("run-l", {"version": "v2"}, "lmig")
        # Under v1 the next attempt would end the loop at its limit of 2; v2
        # allows three rounds, so the loop keeps running.
        state = self.service.advance("run-l", {"output": {}}, "la1")
        self.assertEqual("running", state["loops"]["loop"]["status"])
        state = self.service.advance("run-l", {"output": {}}, "la2")
        self.assertEqual("completed", state["loops"]["loop"]["status"])
        self.assertEqual("iteration_limit", state["loops"]["loop"]["end_reason"])
        self.assertEqual("completed", state["status"])
        self.assertTrue(self.service.replay("run-l")["consistent"])

    def test_target_version_introducing_a_loop_gains_initial_loop_state(self):
        plain = [
            {"id": "reserve", "kind": "task", "depends_on": []},
            {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
        ]
        with_loop = [
            {"id": "reserve", "kind": "task", "depends_on": []},
            {"id": "check", "kind": "condition", "depends_on": [], "path": "again", "equals": True},
            {"id": "attempt", "kind": "task", "depends_on": ["check"]},
            {"id": "retry", "kind": "loop", "depends_on": ["reserve"], "entry": "attempt",
             "condition": "check", "max_iterations": 2},
        ]
        self.create("evolving", "v1", plain)
        self.create("evolving", "v2", with_loop, "wf-evolving-v2")
        self.start("run-1", "evolving", version="v1")
        state = self.service.migrate("run-1", {"version": "v2"}, "mig")
        self.assertIn("retry", state["loops"])
        self.assertEqual("pending", state["loops"]["retry"]["status"])
        self.assertTrue(self.service.replay("run-1")["consistent"])

    def test_migration_is_tenant_scoped(self):
        self.versions()
        self.service.create_workflow(
            {"id": "orders", "version": "other-1", "nodes": nodes_v1()}, "wf-other", "beta"
        )
        self.start("run-1", "orders", version="v1")
        # The beta tenant cannot see the legacy execution or its v2 version.
        with self.assertRaises(NotFoundError):
            self.service.migrate("run-1", {"version": "v2"}, "mig-beta", "beta")
        with self.assertRaises(NotFoundError):
            self.service.migrate("run-1", {"version": "other-1"}, "mig-cross", "beta")
        # The legacy execution is still on v1.
        self.assertEqual("v1", self.service.get_execution("run-1")["version"])


class MigrationHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
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

    def test_migration_lifecycle_over_http(self):
        self.call("POST", "/workflows", {"id": "http-mig", "version": "v1", "nodes": nodes_v1()}, "hm-v1")
        self.call("POST", "/workflows", {"id": "http-mig", "version": "v2", "nodes": nodes_v2()}, "hm-v2")
        self.call("POST", "/executions",
                  {"id": "http-run", "workflow_id": "http-mig", "version": "v1", "input": {}}, "hm-ex")
        self.call("POST", "/executions/http-run/advance", {"output": {}}, "hm-adv1")
        status, data = self.call("POST", "/executions/http-run/migrate", {"version": "v2"}, "hm-mig")
        self.assertEqual(200, status)
        self.assertEqual("v2", json.loads(data)["version"])
        self.assertTrue(data.endswith(b"\n"))
        status, data = self.call("GET", "/executions/http-run/events")
        types = [event["type"] for event in json.loads(data)["events"]]
        self.assertIn("version_migrated", types)
        self.call("POST", "/executions/http-run/advance", {"output": {}}, "hm-adv2")
        status, data = self.call("POST", "/executions/http-run/advance", {"output": {}}, "hm-adv3")
        self.assertEqual("completed", json.loads(data)["status"])
        status, data = self.call("POST", "/executions/http-run/replay")
        self.assertTrue(json.loads(data)["consistent"])

    def test_migration_http_error_codes(self):
        self.call("POST", "/workflows", {"id": "http-e", "version": "v1", "nodes": nodes_v1()}, "he-v1")
        self.call("POST", "/workflows", {"id": "http-e", "version": "v2", "nodes": nodes_v2()}, "he-v2")
        self.call("POST", "/executions",
                  {"id": "http-e-run", "workflow_id": "http-e", "version": "v1", "input": {}}, "he-ex")
        cases = [
            ("POST", "/executions/no-such/migrate", {"version": "v1"}, "m-missing-ex", 404),
            ("POST", "/executions/http-e-run/migrate", {"version": "ghost"}, "m-missing-ver", 404),
            ("POST", "/executions/http-e-run/migrate", {}, "m-empty", 400),
            ("POST", "/executions/http-e-run/migrate", {"version": 7}, "m-type", 400),
            ("POST", "/executions/http-e-run/migrate", {"version": "v2", "x": 1}, "m-unknown", 400),
        ]
        for method, path, body, key, expected in cases:
            with self.subTest(key=key):
                status, data = self.call(method, path, body, key)
                self.assertEqual(expected, status, data)
        status, data = self.call(
            "POST", "/executions/http-e-run/migrate", raw=b'{"version":"v2","n":NaN}', key="m-nan"
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # Same-version migration is a 200 no-op.
        status, _ = self.call("POST", "/executions/http-e-run/migrate", {"version": "v1"}, "m-same")
        self.assertEqual(200, status)
        # Finishing the execution then migrating is a conflict.
        self.call("POST", "/executions/http-e-run/migrate", {"version": "v2"}, "m-do")
        self.call("POST", "/executions/http-e-run/advance", {"output": {}}, "m-a1")
        self.call("POST", "/executions/http-e-run/advance", {"output": {}}, "m-a2")
        self.call("POST", "/executions/http-e-run/advance", {"output": {}}, "m-a3")
        status, data = self.call("POST", "/executions/http-e-run/migrate", {"version": "v1"}, "m-after")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        # Cross-operation idempotency key reuse.
        self.call("POST", "/executions",
                  {"id": "http-e-run2", "workflow_id": "http-e", "version": "v1", "input": {}}, "he-ex2")
        self.call("POST", "/executions/http-e-run2/migrate", {"version": "v2"}, "m-shared")
        status, data = self.call("POST", "/executions/http-e-run2/cancel", key="m-shared")
        self.assertEqual(409, status)

    def test_migration_is_tenant_scoped_over_http(self):
        self.call("POST", "/workflows", {"id": "http-t", "version": "v1", "nodes": nodes_v1()}, "ht-v1")
        self.call("POST", "/workflows", {"id": "http-t", "version": "v2", "nodes": nodes_v2()}, "ht-v2")
        self.call("POST", "/executions",
                  {"id": "http-t-run", "workflow_id": "http-t", "version": "v1", "input": {}}, "ht-ex")
        status, data = self.call(
            "POST", "/executions/http-t-run/migrate", {"version": "v2"}, "ht-mig",
            headers={"X-Tenant-Id": "other"},
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
