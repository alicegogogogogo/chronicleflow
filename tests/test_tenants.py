import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


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

    def call(self, method, path, body=None, key=None, raw=None, tenant=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        if tenant is not None:
            headers["X-Tenant"] = tenant
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def make_workflow(self, workflow_id, tenant=None, key=None, nodes=None):
        body = {"id": workflow_id, "nodes": nodes or [{"id": "a", "kind": "task", "depends_on": []}]}
        return self.call("POST", "/workflows", body, key or f"wf-{workflow_id}-{tenant}", tenant=tenant)

    def start(self, execution_id, workflow_id, tenant=None, key=None):
        body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
        return self.call("POST", "/executions", body, key or f"ex-{execution_id}-{tenant}", tenant=tenant)

    def test_same_identifiers_coexist_across_tenants(self):
        for tenant in ("acme", "globex", None):
            status, _ = self.make_workflow("shared-wf", tenant=tenant)
            self.assertEqual(201, status)
            status, _ = self.start("shared-run", "shared-wf", tenant=tenant)
            self.assertEqual(201, status)
        # advancing one tenant's execution leaves the others untouched
        status, data = self.call(
            "POST", "/executions/shared-run/advance", {"output": {"v": 1}}, "adv-shared-acme", tenant="acme"
        )
        self.assertEqual(200, status)
        self.assertEqual({"a": {"v": 1}}, json.loads(data)["outputs"])
        for tenant in ("globex", None):
            status, data = self.call("GET", "/executions/shared-run", tenant=tenant)
            self.assertEqual(200, status)
            state = json.loads(data)
            self.assertEqual("running", state["status"])
            self.assertEqual({}, state["outputs"])

    def test_cross_tenant_access_is_not_found(self):
        self.make_workflow("wf-iso", tenant="acme")
        self.start("run-iso", "wf-iso", tenant="acme")
        # every execution endpoint 404s for another tenant and for the default namespace
        for tenant in ("globex", None):
            with self.subTest(tenant=tenant):
                for method, path, body in (
                    ("GET", "/executions/run-iso", None),
                    ("GET", "/executions/run-iso/events", None),
                    ("GET", "/executions/run-iso/checkpoints", None),
                    ("GET", "/executions/run-iso/deliveries", None),
                    ("POST", "/executions/run-iso/advance", {"output": {}}),
                    ("POST", "/executions/run-iso/decision", {"approver": "a", "decision": "approved", "output": {}}),
                    ("POST", "/executions/run-iso/claim", {"worker_id": "w"}),
                    ("POST", "/executions/run-iso/heartbeat", {"worker_id": "w"}),
                    ("POST", "/executions/run-iso/release", {"worker_id": "w"}),
                    ("POST", "/executions/run-iso/cancel", None),
                    ("POST", "/executions/run-iso/recover", {"from": "latest_checkpoint"}),
                    ("POST", "/executions/run-iso/replay", None),
                ):
                    status, data = self.call(method, path, body, key=f"k-{method}-{path}-{tenant}", tenant=tenant)
                    self.assertEqual(404, status, (method, path, tenant))
                    self.assertEqual("not_found", json.loads(data)["error"]["code"])
                # schedule endpoints 404 for the other tenant's workflow
                for method, path, body in (
                    ("GET", "/workflows/wf-iso/schedule", None),
                    ("PUT", "/workflows/wf-iso/schedule", {"interval_seconds": 5, "input": {}, "missed_policy": "skip"}),
                    ("POST", "/workflows/wf-iso/schedule/pause", {}),
                    ("POST", "/workflows/wf-iso/schedule/resume", {}),
                ):
                    status, data = self.call(method, path, body, key=f"k-{method}-{path}-{tenant}", tenant=tenant)
                    self.assertEqual(404, status, (method, path, tenant))
        # an execution cannot reference another tenant's workflow
        status, data = self.call(
            "POST", "/executions", {"id": "run-x", "workflow_id": "wf-iso", "input": {}}, "ex-x-globex", tenant="globex"
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_duplicate_identifiers_within_a_tenant_conflict(self):
        self.make_workflow("wf-dup", tenant="acme")
        status, data = self.make_workflow("wf-dup", tenant="acme", key="wf-dup-2")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        self.start("run-dup", "wf-dup", tenant="acme")
        status, data = self.start("run-dup", "wf-dup", tenant="acme", key="ex-dup-2")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_idempotency_keys_are_scoped_per_tenant(self):
        status, _ = self.call(
            "POST", "/workflows", {"id": "wf-key-a", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "shared-tenant-key", tenant="acme",
        )
        self.assertEqual(201, status)
        # the same key is free to use in another tenant
        status, _ = self.call(
            "POST", "/workflows", {"id": "wf-key-b", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "shared-tenant-key", tenant="globex",
        )
        self.assertEqual(201, status)
        # but reusing it across operations within one tenant still conflicts
        status, data = self.call(
            "POST", "/executions", {"id": "run-key", "workflow_id": "wf-key-a", "input": {}},
            "shared-tenant-key", tenant="acme",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_empty_tenant_header_is_a_validation_error(self):
        status, data = self.call(
            "POST", "/workflows", {"id": "wf-empty-tenant", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf-empty-tenant", tenant="",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/executions/run-1", tenant="")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_tenant_state_and_events_carry_no_tenant_fields(self):
        self.make_workflow("wf-shape-t", tenant="acme")
        self.make_workflow("wf-shape-d")
        self.start("run-shape-t", "wf-shape-t", tenant="acme")
        self.start("run-shape-d", "wf-shape-d")
        status, data = self.call("GET", "/executions/run-shape-t", tenant="acme")
        tenant_keys = set(json.loads(data))
        status, data = self.call("GET", "/executions/run-shape-d")
        self.assertEqual(tenant_keys, set(json.loads(data)))
        self.assertNotIn("tenant", tenant_keys)
        status, data = self.call("GET", "/executions/run-shape-t/events", tenant="acme")
        for event in json.loads(data)["events"]:
            self.assertNotIn("tenant", event)
            self.assertNotIn("tenant", event["payload"])

    def test_delivery_history_is_isolated_by_tenant(self):
        unreachable = "http://127.0.0.1:1/hook"
        self.call(
            "POST", "/workflows",
            {"id": "wf-deliv", "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
             "subscriptions": [{"url": unreachable, "events": ["node_completed"]}]},
            "wf-deliv", tenant="acme",
        )
        self.start("run-deliv", "wf-deliv", tenant="acme")
        status, _ = self.call(
            "POST", "/executions/run-deliv/advance", {"output": {}}, "adv-deliv", tenant="acme"
        )
        self.assertEqual(200, status)
        status, data = self.call("GET", "/executions/run-deliv/deliveries", tenant="acme")
        self.assertEqual(200, status)
        records = json.loads(data)["deliveries"]
        self.assertEqual(1, len(records))
        self.assertEqual("failed", records[0]["status"])
        self.assertEqual(unreachable, records[0]["url"])
        for tenant in ("globex", None):
            status, data = self.call("GET", "/executions/run-deliv/deliveries", tenant=tenant)
            self.assertEqual(404, status)
            self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_schedules_are_isolated_by_tenant(self):
        self.call(
            "POST", "/workflows",
            {"id": "wf-sched-iso", "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
             "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "skip"}},
            "wf-sched-iso", tenant="acme",
        )
        for tenant in ("globex", None):
            status, data = self.call("GET", "/workflows/wf-sched-iso/schedule", tenant=tenant)
            self.assertEqual(404, status)
        time.sleep(1.3)
        status, data = self.call("GET", "/workflows/wf-sched-iso/schedule", tenant="acme")
        self.assertEqual(200, status)
        execution_id = json.loads(data)["last_execution_id"]
        self.assertIsNotNone(execution_id)
        status, _ = self.call("GET", f"/executions/{execution_id}", tenant="acme")
        self.assertEqual(200, status)
        for tenant in ("globex", None):
            status, _ = self.call("GET", f"/executions/{execution_id}", tenant=tenant)
            self.assertEqual(404, status)


class QuotaHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-quotas.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None, tenant=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        if tenant is not None:
            headers["X-Tenant"] = tenant
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def put_quota(self, tenant, quota, key=None):
        return self.call("PUT", f"/tenants/{tenant}/quota", quota, key or f"quota-{tenant}-{json.dumps(quota)}")

    def test_quota_declare_replace_and_query(self):
        status, data = self.call("GET", "/tenants/acme/quota")
        self.assertEqual(200, status)
        self.assertEqual({"quota": None}, json.loads(data))
        status, data = self.put_quota("acme", {"workflows": 2, "executions": 3})
        self.assertEqual(200, status)
        self.assertEqual({"quota": {"workflows": 2, "executions": 3}}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call("GET", "/tenants/acme/quota")
        self.assertEqual({"quota": {"workflows": 2, "executions": 3}}, json.loads(data))
        # declaring again replaces the quota
        status, data = self.put_quota("acme", {"workflows": 5, "executions": 6})
        self.assertEqual(200, status)
        status, data = self.call("GET", "/tenants/acme/quota")
        self.assertEqual({"quota": {"workflows": 5, "executions": 6}}, json.loads(data))
        # POST is accepted as well, and other tenants are unaffected
        status, data = self.call("POST", "/tenants/acme/quota", {"workflows": 7, "executions": 8}, "quota-acme-post")
        self.assertEqual(200, status)
        status, data = self.call("GET", "/tenants/globex/quota")
        self.assertEqual({"quota": None}, json.loads(data))

    def test_quota_validation_errors(self):
        for index, body in enumerate(
            (
                {},
                {"workflows": 1},
                {"executions": 1},
                {"workflows": 1, "executions": 1, "extra": 1},
                {"workflows": 0, "executions": 1},
                {"workflows": 1, "executions": -2},
                {"workflows": 1.5, "executions": 1},
                {"workflows": "2", "executions": 1},
                {"workflows": True, "executions": 1},
                {"workflows": None, "executions": 1},
            )
        ):
            with self.subTest(body=body):
                status, data = self.put_quota("badq", body, key=f"quota-bad-{index}")
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "PUT", "/tenants/badq/quota", raw=b'{"workflows":NaN,"executions":1}', key="quota-bad-nan"
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # a missing idempotency key is a validation error
        status, data = self.call("PUT", "/tenants/badq/quota", {"workflows": 1, "executions": 1})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # none of the invalid declarations stuck
        status, data = self.call("GET", "/tenants/badq/quota")
        self.assertEqual({"quota": None}, json.loads(data))

    def test_quota_key_reused_across_operations_conflicts(self):
        self.put_quota("keyq", {"workflows": 3, "executions": 3}, key="quota-key-shared")
        status, data = self.call(
            "POST", "/workflows", {"id": "wf-keyq", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "quota-key-shared", tenant="keyq",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_workflow_quota_is_enforced_atomically(self):
        self.put_quota("capped", {"workflows": 2, "executions": 10})
        for index in (1, 2):
            status, _ = self.call(
                "POST", "/workflows", {"id": f"wf-cap-{index}", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
                f"wf-cap-{index}", tenant="capped",
            )
            self.assertEqual(201, status)
        status, data = self.call(
            "POST", "/workflows",
            {"id": "wf-cap-3", "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
             "subscriptions": [{"url": "http://127.0.0.1:1/hook", "events": ["node_completed"]}],
             "schedule": {"interval_seconds": 3600, "input": {}, "missed_policy": "skip"}},
            "wf-cap-3", tenant="capped",
        )
        self.assertEqual(409, status)
        error = json.loads(data)["error"]
        self.assertEqual("conflict", error["code"])
        self.assertIn("quota", error["message"])
        # the rejected request wrote nothing: no workflow, no schedule
        status, _ = self.call("GET", "/workflows/wf-cap-3/schedule", tenant="capped")
        self.assertEqual(404, status)
        status, _ = self.call(
            "POST", "/executions", {"id": "run-cap-3", "workflow_id": "wf-cap-3", "input": {}},
            "ex-cap-3", tenant="capped",
        )
        self.assertEqual(404, status)
        # other namespaces are unaffected by the tenant's quota
        status, _ = self.call(
            "POST", "/workflows", {"id": "wf-cap-3", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf-cap-3-default",
        )
        self.assertEqual(201, status)

    def test_execution_quota_is_enforced(self):
        self.put_quota("execq", {"workflows": 5, "executions": 1})
        self.call(
            "POST", "/workflows", {"id": "wf-execq", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf-execq", tenant="execq",
        )
        status, _ = self.call(
            "POST", "/executions", {"id": "run-execq-1", "workflow_id": "wf-execq", "input": {}},
            "ex-execq-1", tenant="execq",
        )
        self.assertEqual(201, status)
        status, data = self.call(
            "POST", "/executions", {"id": "run-execq-2", "workflow_id": "wf-execq", "input": {}},
            "ex-execq-2", tenant="execq",
        )
        self.assertEqual(409, status)
        error = json.loads(data)["error"]
        self.assertEqual("conflict", error["code"])
        self.assertIn("quota", error["message"])
        status, _ = self.call("GET", "/executions/run-execq-2", tenant="execq")
        self.assertEqual(404, status)
        # the same execution id is still free in another tenant
        status, _ = self.call(
            "POST", "/executions", {"id": "run-execq-2", "workflow_id": "wf-execq", "input": {}},
            "ex-execq-2-other", tenant="otherq",
        )
        # the workflow does not exist in the other tenant, so this is a 404, not a quota error
        self.assertEqual(404, status)

    def test_lowering_quota_below_holdings_keeps_data(self):
        self.put_quota("flex", {"workflows": 2, "executions": 5})
        for index in (1, 2):
            self.call(
                "POST", "/workflows", {"id": f"wf-flex-{index}", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
                f"wf-flex-{index}", tenant="flex",
            )
        # lowering below the current holdings is allowed and deletes nothing
        status, data = self.put_quota("flex", {"workflows": 1, "executions": 5})
        self.assertEqual(200, status)
        for index in (1, 2):
            status, _ = self.call(
                "POST", "/executions", {"id": f"run-flex-{index}", "workflow_id": f"wf-flex-{index}", "input": {}},
                f"ex-flex-{index}", tenant="flex",
            )
            self.assertEqual(201, status)
        # but new workflow writes beyond the lowered quota are rejected
        status, data = self.call(
            "POST", "/workflows", {"id": "wf-flex-3", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf-flex-3", tenant="flex",
        )
        self.assertEqual(409, status)
        self.assertIn("quota", json.loads(data)["error"]["message"])
        # raising the quota again admits new writes
        self.put_quota("flex", {"workflows": 3, "executions": 5})
        status, _ = self.call(
            "POST", "/workflows", {"id": "wf-flex-3", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "wf-flex-3b", tenant="flex",
        )
        self.assertEqual(201, status)

    def test_schedule_fire_counts_against_execution_quota(self):
        self.put_quota("schedq", {"workflows": 5, "executions": 1})
        self.call(
            "POST", "/workflows",
            {"id": "wf-schedq", "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
             "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"}},
            "wf-schedq", tenant="schedq",
        )
        # the single execution slot is taken manually, so the schedule cannot fire
        status, _ = self.call(
            "POST", "/executions", {"id": "run-schedq", "workflow_id": "wf-schedq", "input": {}},
            "ex-schedq", tenant="schedq",
        )
        self.assertEqual(201, status)
        time.sleep(1.5)
        status, data = self.call("GET", "/workflows/wf-schedq/schedule", tenant="schedq")
        self.assertEqual(200, status)
        payload = json.loads(data)
        self.assertIsNone(payload["last_triggered_at"])
        self.assertIsNone(payload["last_execution_id"])
        status, _ = self.call("GET", "/executions/wf-schedq-scheduled-i:1", tenant="schedq")
        self.assertEqual(404, status)
        # once the quota allows it, the schedule fires again
        self.put_quota("schedq", {"workflows": 5, "executions": 10})
        time.sleep(1.5)
        status, data = self.call("GET", "/workflows/wf-schedq/schedule", tenant="schedq")
        payload = json.loads(data)
        self.assertIsNotNone(payload["last_triggered_at"])
        self.assertIsNotNone(payload["last_execution_id"])
        status, _ = self.call("GET", f"/executions/{payload['last_execution_id']}", tenant="schedq")
        self.assertEqual(200, status)


class DeliveryRecordReceiver(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class DeliveryPersistenceTests(unittest.TestCase):
    """A failed delivery-history write is recorded as failed, not swallowed."""

    @classmethod
    def setUpClass(cls):
        cls.receiver = ThreadingHTTPServer(("127.0.0.1", 0), DeliveryRecordReceiver)
        cls.target_port = cls.receiver.server_address[1]
        cls.thread = threading.Thread(target=cls.receiver.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.receiver.shutdown()
        cls.receiver.server_close()

    def test_failed_history_write_is_recorded_as_failed(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        service = ChronicleFlow(str(Path(directory.name) / "delivery.db"))
        service.create_workflow(
            {
                "id": "wf-del",
                "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
                "subscriptions": [
                    {"url": f"http://127.0.0.1:{self.target_port}/hook", "events": ["node_completed"]}
                ],
            },
            "w1",
        )
        service.create_execution({"id": "run-del", "workflow_id": "wf-del", "input": {}}, "e1")
        original = service._insert_delivery
        attempts = {"count": 0}

        def flaky_insert(notice, record):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("simulated persistence failure")
            return original(notice, record)

        service._insert_delivery = flaky_insert
        state = service.advance("run-del", {"output": {"v": 1}}, "a1")
        self.assertEqual("completed", state["status"])
        self.assertEqual(2, attempts["count"])
        records = service.deliveries("run-del")["deliveries"]
        self.assertEqual(1, len(records))
        record = records[0]
        # the delivery itself succeeded, but the first history write failed,
        # so the attempt is recorded explicitly as failed
        self.assertEqual("failed", record["status"])
        self.assertEqual([{"attempt": 1, "status_code": 200}], record["attempts"])


if __name__ == "__main__":
    unittest.main()
