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

TASK = [{"id": "a", "kind": "task", "depends_on": []}]
TWO_TASKS = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]


class Receiver(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        Receiver.requests.append({"body": json.loads(body)})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class TenantIsolationServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "tenants.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_same_workflow_and_execution_ids_coexist_per_tenant(self):
        for tenant in ("alpha", "beta"):
            self.service.create_workflow({"id": "shared", "nodes": TASK}, f"wf-{tenant}", tenant)
            self.service.create_execution(
                {"id": "run-shared", "workflow_id": "shared", "input": {"tenant": tenant}},
                f"ex-{tenant}",
                tenant,
            )
        alpha = self.service.get_execution("run-shared", "alpha")
        beta = self.service.get_execution("run-shared", "beta")
        self.assertEqual({"tenant": "alpha"}, alpha["input"])
        self.assertEqual({"tenant": "beta"}, beta["input"])
        self.service.advance("run-shared", {"output": {"who": "alpha"}}, f"adv-alpha", "alpha")
        # Beta's execution is untouched by alpha's advance.
        self.assertEqual("running", self.service.get_execution("run-shared", "beta")["status"])
        self.assertEqual({"who": "alpha"}, self.service.get_execution("run-shared", "alpha")["outputs"]["a"])

    def test_cross_tenant_reads_are_not_found(self):
        self.service.create_workflow({"id": "wf", "nodes": TWO_TASKS}, "wf-a", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex-a", "alpha")
        for operation in (
            lambda: self.service.get_execution("run", "beta"),
            lambda: self.service.events("run", "beta"),
            lambda: self.service.checkpoints("run", "beta"),
            lambda: self.service.deliveries("run", "beta"),
            lambda: self.service.schedule_status("wf", "beta"),
        ):
            with self.subTest(operation=operation.__doc__ or operation):
                self.assertRaises(NotFoundError, operation)

    def test_cross_tenant_writes_are_not_found(self):
        self.service.create_workflow({"id": "wf", "nodes": TWO_TASKS}, "wf-a", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex-a", "alpha")
        cross_tenant = [
            lambda: self.service.advance("run", {"output": {}}, "k", "beta"),
            lambda: self.service.cancel("run", "k2", "beta"),
            lambda: self.service.claim("run", {"worker_id": "w"}, "k3", "beta"),
            lambda: self.service.heartbeat("run", {"worker_id": "w"}, "k4", "beta"),
            lambda: self.service.release("run", {"worker_id": "w"}, "k5", "beta"),
            lambda: self.service.recover("run", {"from": "latest_checkpoint"}, "k6", "beta"),
            lambda: self.service.replay("run", "beta"),
        ]
        for index, operation in enumerate(cross_tenant):
            with self.subTest(index=index):
                self.assertRaises(NotFoundError, operation)

    def test_execution_referencing_another_tenants_workflow_is_not_found(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-a", "alpha")
        with self.assertRaises(NotFoundError):
            self.service.create_execution(
                {"id": "run", "workflow_id": "wf", "input": {}}, "ex-b", "beta"
            )
        # No partial write.
        with self.assertRaises(NotFoundError):
            self.service.get_execution("run", "beta")

    def test_schedule_operations_are_tenant_scoped(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-a", "alpha")
        self.service.update_schedule(
            "wf", {"interval_seconds": 60, "input": {}, "missed_policy": "skip"}, "sch-a", "alpha"
        )
        # Beta has no view of alpha's workflow or schedule.
        with self.assertRaises(NotFoundError):
            self.service.schedule_status("wf", "beta")
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("wf", {}, "pause-b", "beta")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("wf", {}, "resume-b", "beta")
        with self.assertRaises(NotFoundError):
            self.service.update_schedule(
                "wf", {"interval_seconds": 30, "input": {}, "missed_policy": "skip"}, "upd-b", "beta"
            )
        # Beta may have its own workflow with the same id and no schedule.
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-b", "beta")
        self.assertEqual({"schedule": None}, self.service.schedule_status("wf", "beta"))
        self.assertIsNotNone(self.service.schedule_status("wf", "alpha")["schedule"])

    def test_idempotency_keys_are_scoped_per_tenant(self):
        for tenant in ("alpha", "beta"):
            self.service.create_workflow({"id": "wf", "nodes": TASK}, "shared-key", tenant)
        # Reusing the same key for another operation in the *same* tenant conflicts.
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "shared-key", "beta")

    def test_default_namespace_is_independent_of_tenants(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-default")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex-default")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-tenant", "alpha")
        self.assertEqual("running", self.service.get_execution("run")["status"])
        with self.assertRaises(NotFoundError):
            self.service.get_execution("run", "alpha")
        with self.assertRaises(NotFoundError):
            self.service.get_execution("does-not-exist")

    def test_tenant_scoped_lease_blocks_only_same_tenant_holder(self):
        self.service.create_workflow({"id": "wf", "nodes": TWO_TASKS}, "wf-l", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex-l", "alpha")
        self.service.claim("run", {"worker_id": "w1"}, "claim-l", "alpha")
        # Another worker in the same tenant is blocked.
        with self.assertRaises(ConflictError):
            self.service.advance("run", {"output": {}, "worker_id": "w2"}, "adv-other", "alpha")
        # The cross-tenant attempt is a missing resource, not a lease conflict.
        with self.assertRaises(NotFoundError):
            self.service.advance("run", {"output": {}, "worker_id": "w3"}, "adv-cross", "beta")


class QuotaServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "quotas.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_unknown_quota_is_definite_empty_result(self):
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))

    def test_quota_declared_and_queried(self):
        result = self.service.declare_quota({"workflows": 2, "executions": 5}, "q1", "alpha")
        self.assertEqual({"quota": {"workflows": 2, "executions": 5}}, result)
        self.assertEqual({"quota": {"workflows": 2, "executions": 5}}, self.service.get_quota("alpha"))
        # Re-declaring replaces the limits.
        self.service.declare_quota({"workflows": 3, "executions": 9}, "q2", "alpha")
        self.assertEqual({"quota": {"workflows": 3, "executions": 9}}, self.service.get_quota("alpha"))

    def test_invalid_quota_declarations_are_validation_errors(self):
        bad_bodies = [
            {"workflows": 0, "executions": 1},
            {"workflows": -1, "executions": 1},
            {"workflows": 1.5, "executions": 1},
            {"workflows": "2", "executions": 1},
            {"workflows": True, "executions": 1},
            {"workflows": 1},
            {"executions": 1},
            {"workflows": 1, "executions": 1, "extra": 2},
            {},
            [],
            {"workflows": 1, "executions": None},
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.service.declare_quota(body, f"bad-{index}", "alpha")

    def test_quota_requires_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.declare_quota({"workflows": 1, "executions": 1}, "q-default", "")
        with self.assertRaises(ValidationError):
            self.service.get_quota("")

    def test_workflow_quota_is_enforced_without_partial_write(self):
        self.service.declare_quota({"workflows": 1, "executions": 10}, "q", "alpha")
        self.service.create_workflow({"id": "wf-1", "nodes": TASK}, "wf-1", "alpha")
        with self.assertRaises(ConflictError) as caught:
            self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2", "alpha")
        self.assertIn("quota", str(caught.exception))
        # The rejected workflow is invisible and the quota idempotency key was not consumed.
        with self.assertRaises(NotFoundError):
            self.service.schedule_status("wf-2", "alpha")
        # A different tenant is unaffected.
        self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2b", "beta")

    def test_execution_quota_is_enforced_without_partial_write(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-b", "beta")
        self.service.declare_quota({"workflows": 10, "executions": 1}, "q", "alpha")
        self.service.create_execution({"id": "run-1", "workflow_id": "wf", "input": {}}, "run-1", "alpha")
        with self.assertRaises(ConflictError) as caught:
            self.service.create_execution({"id": "run-2", "workflow_id": "wf", "input": {}}, "run-2", "alpha")
        self.assertIn("quota", str(caught.exception))
        with self.assertRaises(NotFoundError):
            self.service.get_execution("run-2", "alpha")
        # Beta has no quota, so the same write succeeds there.
        self.service.create_execution({"id": "run-2", "workflow_id": "wf", "input": {}}, "run-2b", "beta")

    def test_duplicate_identifier_at_quota_keeps_identifier_conflict(self):
        self.service.declare_quota({"workflows": 1, "executions": 1}, "q", "alpha")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "run", "alpha")
        # Tenant is at both limits; repeats of existing ids are ordinary
        # identifier conflicts, not quota conflicts, and change nothing.
        with self.assertRaises(ConflictError) as caught:
            self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf-dup", "alpha")
        self.assertNotIn("quota", str(caught.exception))
        with self.assertRaises(ConflictError) as caught:
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "run-dup", "alpha")
        self.assertNotIn("quota", str(caught.exception))

    def test_lowering_quota_keeps_existing_data_but_blocks_new_writes(self):
        self.service.declare_quota({"workflows": 10, "executions": 10}, "q1", "alpha")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "alpha")
        for index in range(3):
            self.service.create_execution(
                {"id": f"run-{index}", "workflow_id": "wf", "input": {}}, f"run-{index}", "alpha"
            )
        self.service.declare_quota({"workflows": 1, "executions": 2}, "q2", "alpha")
        # Existing data is untouched.
        for index in range(3):
            self.assertEqual("running", self.service.get_execution(f"run-{index}", "alpha")["status"])
        self.assertEqual({"quota": {"workflows": 1, "executions": 2}}, self.service.get_quota("alpha"))
        # Further writes are rejected for being over the (lowered) limit.
        with self.assertRaises(ConflictError):
            self.service.create_workflow({"id": "wf-2", "nodes": TASK}, "wf-2", "alpha")
        with self.assertRaises(ConflictError):
            self.service.create_execution({"id": "run-3", "workflow_id": "wf", "input": {}}, "run-3", "alpha")

    def test_scheduled_execution_counts_against_execution_quota(self):
        self.service.declare_quota({"workflows": 10, "executions": 1}, "q", "alpha")
        self.service.create_workflow(
            {"id": "wf-sched", "nodes": TASK, "schedule": {
                "interval_seconds": 1, "input": {}, "missed_policy": "catch_up"
            }},
            "wf-sched",
            "alpha",
        )
        self.service.create_execution({"id": "run-full", "workflow_id": "wf-sched", "input": {}}, "run-full", "alpha")
        time.sleep(1.3)
        status = self.service.schedule_status("wf-sched", "alpha")["schedule"]
        # Quota full: no execution was created and the schedule status is unchanged.
        self.assertIsNone(status["last_execution_id"])
        self.assertIsNone(status["last_triggered_at"])
        # Raise the quota; the next due period now fires into the tenant.
        self.service.declare_quota({"workflows": 10, "executions": 10}, "q2", "alpha")
        deadline = time.time() + 3
        while time.time() < deadline:
            status = self.service.schedule_status("wf-sched", "alpha")["schedule"]
            if status["last_execution_id"]:
                break
            time.sleep(0.05)
        self.assertIsNotNone(status["last_execution_id"])
        scheduled_id = status["last_execution_id"]
        self.assertEqual("running", self.service.get_execution(scheduled_id, "alpha")["status"])
        # The scheduled execution is invisible outside its tenant.
        with self.assertRaises(NotFoundError):
            self.service.get_execution(scheduled_id, "beta")
        with self.assertRaises(NotFoundError):
            self.service.get_execution(scheduled_id)

    def test_deliveries_are_isolated_by_execution_tenant(self):
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        thread = threading.Thread(target=receiver.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {"id": "wf", "nodes": TASK, "subscriptions": [{"url": url, "events": ["node_completed"]}]},
                "wf",
                "alpha",
            )
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex", "alpha")
            self.service.advance("run", {"output": {"v": 1}}, "adv", "alpha")
            records = self.service.deliveries("run", "alpha")["deliveries"]
            self.assertEqual(1, len(records))
            self.assertEqual("delivered", records[0]["status"])
            with self.assertRaises(NotFoundError):
                self.service.deliveries("run", "beta")
        finally:
            receiver.shutdown()
            receiver.server_close()

    def test_delivery_history_write_failure_is_recorded_as_failed(self):
        Receiver.requests = []
        receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        thread = threading.Thread(target=receiver.serve_forever, daemon=True)
        thread.start()
        state = {"failed": False}
        try:
            url = f"http://127.0.0.1:{receiver.server_address[1]}/hook"
            self.service.create_workflow(
                {"id": "wf", "nodes": TASK, "subscriptions": [{"url": url, "events": ["node_completed"]}]},
                "wf",
                "alpha",
            )
            self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "ex", "alpha")
            original_insert = self.service._insert_delivery
            state = {"failed": False}

            def flaky_insert(tenant, execution_id, record):
                if not state["failed"]:
                    state["failed"] = True
                    raise sqlite3.OperationalError("forced history failure")
                return original_insert(tenant, execution_id, record)

            self.service._insert_delivery = flaky_insert
            self.service.advance("run", {"output": {"v": 1}}, "adv", "alpha")
            self.service._insert_delivery = original_insert
        finally:
            receiver.shutdown()
            receiver.server_close()
        # The HTTP attempt itself happened, and the failed history write is
        # explicitly visible as a failed record instead of disappearing.
        self.assertEqual(1, len(Receiver.requests))
        records = self.service.deliveries("run", "alpha")["deliveries"]
        self.assertEqual(1, len(records))
        self.assertEqual("failed", records[0]["status"])
        self.assertIn("forced history failure", records[0]["persistence_error"])
        self.assertEqual(1, records[0]["attempt_count"])


class TenantHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-tenants.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, headers=None, raw=None):
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

    def test_empty_tenant_header_is_validation_error(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "wf-empty", "nodes": TASK},
            key="wf-empty",
            headers={"X-Tenant-Id": ""},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_quota_routes_over_http(self):
        status, data = self.call("GET", "/quotas", headers={"X-Tenant-Id": "http-a"})
        self.assertEqual(200, status)
        self.assertEqual({"quota": None}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call(
            "PUT",
            "/quotas",
            {"workflows": 1, "executions": 2},
            key="quota-a",
            headers={"X-Tenant-Id": "http-a"},
        )
        self.assertEqual(200, status)
        self.assertEqual({"quota": {"workflows": 1, "executions": 2}}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        status, data = self.call("GET", "/quotas", headers={"X-Tenant-Id": "http-a"})
        self.assertEqual(200, status)
        self.assertEqual({"quota": {"workflows": 1, "executions": 2}}, json.loads(data))

    def test_quota_routes_without_tenant_are_validation_errors(self):
        status, data = self.call("GET", "/quotas")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("PUT", "/quotas", {"workflows": 1, "executions": 1}, key="q-no-tenant")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_quota_validation_errors_over_http(self):
        for index, raw in enumerate((b'{"workflows":0,"executions":1}', b'{"workflows":1.5,"executions":1}')):
            status, data = self.call(
                "PUT",
                "/quotas",
                raw=raw,
                key=f"q-bad-{index}",
                headers={"X-Tenant-Id": "http-b"},
            )
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "PUT",
            "/quotas",
            raw=b'{"workflows":1e400,"executions":1}',
            key="q-nan",
            headers={"X-Tenant-Id": "http-b"},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_quota_exceeded_is_conflict_over_http(self):
        headers = {"X-Tenant-Id": "http-c"}
        self.call("PUT", "/quotas", {"workflows": 1, "executions": 1}, key="q-c", headers=headers)
        status, _ = self.call(
            "POST", "/workflows", {"id": "wf-c", "nodes": TASK}, key="wf-c", headers=headers
        )
        self.assertEqual(201, status)
        status, data = self.call(
            "POST", "/workflows", {"id": "wf-c2", "nodes": TASK}, key="wf-c2", headers=headers
        )
        self.assertEqual(409, status)
        error = json.loads(data)["error"]
        self.assertEqual("conflict", error["code"])
        self.assertIn("quota", error["message"])

    def test_tenant_isolation_over_http(self):
        for tenant in ("http-d", "http-e"):
            status, _ = self.call(
                "POST",
                "/workflows",
                {"id": "wf-same", "nodes": TASK},
                key=f"wf-{tenant}",
                headers={"X-Tenant-Id": tenant},
            )
            self.assertEqual(201, status)
            status, _ = self.call(
                "POST",
                "/executions",
                {"id": "run-same", "workflow_id": "wf-same", "input": {}},
                key=f"ex-{tenant}",
                headers={"X-Tenant-Id": tenant},
            )
            self.assertEqual(201, status)
        status, data = self.call("GET", "/executions/run-same", headers={"X-Tenant-Id": "http-d"})
        self.assertEqual(200, status)
        self.assertEqual("wf-same", json.loads(data)["workflow_id"])
        # Cross-tenant query is indistinguishable from a missing resource.
        status, data = self.call("GET", "/executions/run-missing", headers={"X-Tenant-Id": "http-d"})
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
