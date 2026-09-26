import http.client
import json
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import (
    USAGE_UNIT_PRICES,
    ChronicleFlow,
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]
TWO_TASKS = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]
SHORT_VISIBILITY = 0.05


class QueueServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "queues.db"))

    def tearDown(self):
        self.service.close()
        self.directory.cleanup()

    def make(self, workflow_id="wf", queues=None, nodes=None, version=None, tenant="acme", key=None):
        body = {"id": workflow_id, "nodes": nodes or TASK}
        if queues is not None:
            body["queues"] = queues
        if version is not None:
            body["version"] = version
        self.service.create_workflow(body, key or f"wf-{workflow_id}-{version or 'v0'}", tenant)

    def start(self, execution_id="r", workflow_id="wf", queues=None, tenant="acme", key=None, input_data=None):
        body = {"id": execution_id, "workflow_id": workflow_id, "input": input_data or {}}
        if queues is not None:
            body["queues"] = queues
        self.service.create_execution(body, key or f"ex-{execution_id}", tenant)

    def advance(self, execution_id="r", output=None, key=None, tenant="acme"):
        return self.service.advance(execution_id, {"output": output if output is not None else {}}, key or f"adv-{execution_id}", tenant)

    def pull(self, queue_name, key, tenant="acme", execution_id=None):
        return self.service.pull_queue(queue_name, {}, key, tenant, execution_id)

    def ack(self, queue_name, receipt, key, tenant="acme", execution_id=None):
        return self.service.acknowledge_queue(queue_name, {"receipt_id": receipt}, key, tenant, execution_id)

    def test_events_enter_workflow_queue_in_occurrence_order(self):
        self.make(nodes=TWO_TASKS, queues=[{"name": "q", "events": ["node_completed", "execution_completed"]}])
        self.start()
        self.advance(key="adv-1")
        self.advance(key="adv-2")
        status = self.service.queue_status("r", "acme")
        self.assertEqual(["q"], [queue["name"] for queue in status["queues"]])
        records = status["queues"][0]["messages"]
        self.assertEqual(["node_completed", "node_completed", "execution_completed"], [m["event_type"] for m in records])
        self.assertEqual([1, 2, 3], [m["sequence"] for m in records])
        self.assertEqual([2, 3, 4], [m["event_sequence"] for m in records])

    def test_pull_returns_messages_in_entry_order_with_payload_and_key(self):
        self.make(nodes=TWO_TASKS, queues=[{"name": "q", "events": ["node_completed", "execution_completed"]}])
        self.start()
        self.advance(key="adv-1")
        pulled = self.pull("q", "p1")["messages"]
        self.assertEqual("node_completed", pulled[0]["event_type"])
        self.assertEqual("a", pulled[0]["payload"]["node_id"])
        self.assertEqual("r", pulled[0]["payload"]["execution_id"])
        self.assertEqual("q", pulled[0]["queue_name"])
        self.assertEqual(1, pulled[0]["sequence"])
        self.assertTrue(pulled[0]["receipt_id"])
        self.assertGreater(pulled[0]["visible_until"], time.time())
        self.assertTrue(pulled[0]["idempotency_key"])
        self.assertTrue(pulled[0]["enqueued_at"].endswith("Z"))
        self.advance(key="adv-2")
        pulled2 = self.pull("q", "p2")["messages"]
        # Only the second node and completion are still deliverable; ordering is by entry.
        self.assertEqual(["node_completed", "execution_completed"], [m["event_type"] for m in pulled2])
        self.assertEqual([2, 3], [m["sequence"] for m in pulled2])
        # Different events never share an idempotency key.
        keys = {pulled[0]["idempotency_key"], pulled2[0]["idempotency_key"], pulled2[1]["idempotency_key"]}
        self.assertEqual(3, len(keys))

    def test_message_is_invisible_after_pull_and_redelivered_after_timeout(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": SHORT_VISIBILITY}])
        self.start()
        self.advance()
        first = self.pull("q", "p1")["messages"]
        self.assertEqual(1, len(first))
        self.assertEqual([], self.pull("q", "p2")["messages"])
        time.sleep(SHORT_VISIBILITY + 0.05)
        redelivered = self.pull("q", "p3")["messages"]
        self.assertEqual(1, len(redelivered))
        # Redelivery keeps the message's idempotency key but issues a new receipt.
        self.assertEqual(first[0]["idempotency_key"], redelivered[0]["idempotency_key"])
        self.assertNotEqual(first[0]["receipt_id"], redelivered[0]["receipt_id"])
        self.assertEqual(1, redelivered[0]["sequence"])
        record = self.service.queue_status("r", "acme")["queues"][0]["messages"][0]
        self.assertEqual(2, record["delivery_count"])
        self.assertEqual("in_flight", record["status"])

    def test_acknowledgement_removes_message_permanently(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": 30}])
        self.start()
        self.advance()
        receipt = self.pull("q", "p1")["messages"][0]["receipt_id"]
        self.assertEqual({"acknowledged": True}, self.ack("q", receipt, "a1"))
        self.assertEqual([], self.pull("q", "p2")["messages"])
        record = self.service.queue_status("r", "acme")["queues"][0]["messages"][0]
        self.assertEqual("acknowledged", record["status"])
        self.assertEqual(1, record["delivery_count"])

    def test_repeated_unknown_and_expired_receipts_are_not_found(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": 30}])
        self.start()
        self.advance()
        receipt = self.pull("q", "p1")["messages"][0]["receipt_id"]
        self.ack("q", receipt, "a1")
        with self.assertRaises(NotFoundError):
            self.ack("q", receipt, "a2")
        with self.assertRaises(NotFoundError):
            self.ack("q", "unknown-receipt", "a3")
        # A fresh message with a short visibility: acknowledging after the
        # deadline fails even before the next pull redelivers it.
        self.make(
            workflow_id="wf2",
            queues=[{"name": "q2", "events": ["node_completed"], "visibility_seconds": SHORT_VISIBILITY}],
            key="wf2",
        )
        self.start("r2", workflow_id="wf2", key="ex-r2")
        self.advance("r2", key="adv-r2")
        expired = self.pull("q2", "p2")["messages"][0]["receipt_id"]
        time.sleep(SHORT_VISIBILITY + 0.05)
        with self.assertRaises(NotFoundError):
            self.ack("q2", expired, "a4")
        # The redelivered message carries a fresh valid receipt.
        redelivered = self.pull("q2", "p3")["messages"][0]["receipt_id"]
        self.assertNotEqual(expired, redelivered)
        self.assertEqual({"acknowledged": True}, self.ack("q2", redelivered, "a5"))
        with self.assertRaises(NotFoundError):
            self.ack("q2", redelivered, "a6")

    def test_status_reports_expired_in_flight_message_as_pending_again(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": SHORT_VISIBILITY}])
        self.start()
        self.advance()
        self.pull("q", "p1")
        self.assertEqual("in_flight", self.service.queue_status("r", "acme")["queues"][0]["messages"][0]["status"])
        time.sleep(SHORT_VISIBILITY + 0.05)
        record = self.service.queue_status("r", "acme")["queues"][0]["messages"][0]
        self.assertEqual("pending", record["status"])
        self.assertEqual(1, record["delivery_count"])

    def test_empty_pull_is_a_definite_empty_result(self):
        self.make(queues=[{"name": "q", "events": ["execution_completed"]}])
        self.start()
        self.assertEqual({"messages": []}, self.pull("q", "p1"))

    def test_unknown_queue_and_unknown_execution_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.pull("nope", "p1")
        with self.assertRaises(NotFoundError):
            self.service.acknowledge_queue("nope", {"receipt_id": "x"}, "a1")
        with self.assertRaises(NotFoundError):
            self.service.queue_status("nope", "acme")

    def test_status_lists_every_declared_queue_including_empty_ones(self):
        self.make(
            queues=[
                {"name": "busy", "events": ["node_completed"]},
                {"name": "idle", "events": ["execution_terminated"]},
            ]
        )
        self.start()
        self.advance()
        status = self.service.queue_status("r", "acme")
        self.assertEqual(["busy", "idle"], [queue["name"] for queue in status["queues"]])
        self.assertEqual([], status["queues"][1]["messages"])
        record = status["queues"][0]["messages"][0]
        self.assertEqual(
            {"delivery_count", "enqueued_at", "event_sequence", "event_type", "idempotency_key", "sequence", "status"},
            set(record),
        )
        self.assertEqual("pending", record["status"])
        self.assertEqual(0, record["delivery_count"])

    def test_execution_without_queues_has_definite_empty_status(self):
        self.make(workflow_id="plain")
        self.start(workflow_id="plain")
        self.assertEqual({"queues": []}, self.service.queue_status("r", "acme"))

    def test_multiple_queues_each_receive_matching_events(self):
        self.make(
            queues=[
                {"name": "q1", "events": ["node_completed"]},
                {"name": "q2", "events": ["execution_completed"]},
            ]
        )
        self.start()
        self.advance()
        self.assertEqual(["node_completed"], [m["event_type"] for m in self.pull("q1", "p1")["messages"]])
        self.assertEqual(["execution_completed"], [m["event_type"] for m in self.pull("q2", "p2")["messages"]])

    def test_execution_queues_layer_over_workflow_queues(self):
        self.make(workflow_id="wf", queues=[{"name": "wf-q", "events": ["node_completed"]}])
        self.start(queues=[{"name": "ex-q", "events": ["node_completed"]}])
        self.advance()
        names = sorted(queue["name"] for queue in self.service.queue_status("r", "acme")["queues"])
        self.assertEqual(["ex-q", "wf-q"], names)

    def test_unmatched_event_does_not_enter_queue(self):
        self.service.create_workflow(
            {
                "id": "wf-fail",
                "nodes": [{"id": "a", "kind": "task", "depends_on": []}],
                "queues": [{"name": "q", "events": ["execution_terminated"]}],
            },
            "wf-fail",
            "acme",
        )
        self.start(workflow_id="wf-fail")
        self.service.advance("r", {"failure": {"reason": "boom"}}, "adv-fail", "acme")
        self.assertEqual(["execution_terminated"], [m["event_type"] for m in self.pull("q", "p1")["messages"]])

    def test_approval_decided_event_is_queued(self):
        self.service.create_workflow(
            {
                "id": "wf-app",
                "nodes": [{"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}}],
                "queues": [{"name": "q", "events": ["approval_decided"]}],
            },
            "wf-app",
            "acme",
        )
        self.start(workflow_id="wf-app")
        self.service.advance("r", {"output": {}}, "adv-app", "acme")
        self.assertEqual([], self.pull("q", "p1")["messages"])
        self.service.decision(
            "r", {"approver": "alice", "decision": "approved", "output": {}}, "dec-app", "acme"
        )
        message = self.pull("q", "p2")["messages"][0]
        self.assertEqual("approval_decided", message["event_type"])
        self.assertEqual("alice", message["payload"]["approver"])

    def test_pull_replay_returns_first_result_without_redelivering(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": 30}])
        self.start()
        self.advance()
        first = self.pull("q", "shared-pull")
        second = self.pull("q", "shared-pull")
        self.assertEqual(first, second)
        self.assertEqual([], self.pull("q", "other-pull")["messages"])

    def test_ack_replay_is_idempotent_and_key_reuse_conflicts(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"]}])
        self.start()
        self.advance()
        receipt = self.pull("q", "p1")["messages"][0]["receipt_id"]
        self.assertEqual({"acknowledged": True}, self.ack("q", receipt, "shared-ack"))
        self.assertEqual({"acknowledged": True}, self.ack("q", receipt, "shared-ack"))
        with self.assertRaises(ConflictError):
            self.ack("q", receipt, "p1")

    def test_default_visibility_is_thirty_seconds(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"]}])
        self.start()
        self.advance()
        message = self.pull("q", "p1")["messages"][0]
        self.assertAlmostEqual(30.0, message["visible_until"] - time.time(), delta=2.0)

    def test_invalid_queue_declarations_are_validation_errors_without_partial_writes(self):
        self.make(workflow_id="plain")
        bad_queues = [
            [{"name": "", "events": ["node_completed"]}],
            [{"name": "q", "events": ["node_completed"], "visibility_seconds": 0}],
            [{"name": "q", "events": ["node_completed"], "visibility_seconds": -2}],
            [{"name": "q", "events": ["node_completed"], "visibility_seconds": True}],
            [{"name": "q", "events": []}],
            [{"name": "q", "events": ["unknown"]}],
            [{"name": "q", "events": ["node_completed", "node_completed"]}],
            [{"name": "q", "events": ["node_completed"], "extra": 1}],
            [{"events": ["node_completed"]}],
            "not-a-list",
        ]
        for index, queues in enumerate(bad_queues):
            with self.subTest(queues=queues):
                with self.assertRaises(ValidationError):
                    self.start(f"bad-{index}", workflow_id="plain", queues=queues, key=f"bad-{index}")
                with self.assertRaises(NotFoundError):
                    self.service.get_execution(f"bad-{index}", "acme")

    def test_duplicate_queue_name_in_one_declaration_conflicts(self):
        self.make(workflow_id="plain")
        with self.assertRaises(ConflictError):
            self.start(
                "dup",
                workflow_id="plain",
                queues=[{"name": "d", "events": ["node_completed"]}, {"name": "d", "events": ["execution_completed"]}],
            )

    def test_same_queue_name_in_another_execution_conflicts_but_coexists_across_tenants(self):
        self.make(workflow_id="wf", queues=[{"name": "shared", "events": ["node_completed"]}])
        self.start("r1")
        self.make(workflow_id="wf2", queues=[{"name": "shared", "events": ["node_completed"]}], tenant="acme")
        with self.assertRaises(ConflictError):
            self.start("r2", workflow_id="wf2")
        # Another tenant may use the same name without collision.
        self.service.create_workflow(
            {"id": "wf", "nodes": TASK, "queues": [{"name": "shared", "events": ["node_completed"]}]},
            "wf-beta",
            "beta",
        )
        self.start("r1", tenant="beta", key="ex-r1-beta")
        self.advance("r1", tenant="beta", key="adv-r1-beta")
        self.assertEqual(1, len(self.pull("shared", "p-beta", tenant="beta")["messages"]))

    def test_cross_tenant_pull_ack_and_status_are_not_found(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"]}])
        self.start()
        self.advance()
        receipt = self.pull("q", "p1")["messages"][0]["receipt_id"]
        with self.assertRaises(NotFoundError):
            self.pull("q", "p2", tenant="beta")
        with self.assertRaises(NotFoundError):
            self.ack("q", receipt, "a1", tenant="beta")
        with self.assertRaises(NotFoundError):
            self.service.queue_status("r", "beta")

    def test_nested_execution_route_scope_rejects_another_executions_queue(self):
        self.make(workflow_id="wf", queues=[{"name": "q", "events": ["node_completed"]}])
        self.start("r1")
        self.advance("r1", key="adv-r1")
        receipt = self.pull("q", "p1")["messages"][0]["receipt_id"]
        with self.assertRaises(NotFoundError):
            self.pull("q", "p2", execution_id="other")
        with self.assertRaises(NotFoundError):
            self.ack("q", receipt, "a1", execution_id="other")

    def test_each_delivery_is_metered_and_billed(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"], "visibility_seconds": SHORT_VISIBILITY}])
        self.start()
        self.advance()
        self.pull("q", "p1")
        time.sleep(SHORT_VISIBILITY + 0.05)
        self.pull("q", "p2")
        counts = {entry["type"]: entry["count"] for entry in self.service.usage("acme")["usage"]}
        self.assertEqual(2, counts["delivery_attempted"])
        bill = {item["type"]: item for item in self.service.bill("acme")["bill"]["items"]}
        self.assertEqual(2, bill["delivery_attempted"]["count"])
        self.assertEqual(
            2 * USAGE_UNIT_PRICES["delivery_attempted"], bill["delivery_attempted"]["subtotal"]
        )

    def test_legacy_namespace_queue_deliveries_are_not_metered(self):
        self.service.create_workflow(
            {"id": "wf", "nodes": TASK, "queues": [{"name": "q", "events": ["node_completed"]}]},
            "wf",
        )
        self.service.create_execution({"id": "r", "workflow_id": "wf", "input": {}}, "r")
        self.service.advance("r", {"output": {}}, "adv")
        self.assertEqual(1, len(self.service.pull_queue("q", {}, "p1")["messages"]))
        used = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = ''"
        ).fetchone()["used"]
        self.assertEqual(0, used)

    def test_queues_add_no_events_or_state_fields_and_replay_stays_consistent(self):
        self.make(nodes=TWO_TASKS, queues=[{"name": "q", "events": ["node_completed", "execution_completed"]}])
        self.start()
        self.advance(key="adv-1")
        state = self.advance(key="adv-2")
        self.assertNotIn("queues", state)
        types = [event["type"] for event in self.service.events("r", "acme")]
        self.assertEqual(
            ["execution_started", "node_completed", "node_completed", "execution_completed"], types
        )
        self.assertTrue(self.service.replay("r", "acme")["consistent"])

    def test_replay_recovery_and_queries_do_not_enqueue(self):
        self.make(queues=[{"name": "q", "events": ["node_completed", "execution_completed"]}])
        self.start()
        self.advance()
        before = self.service.queue_status("r", "acme")
        self.service.replay("r", "acme")
        self.service.recover("r", {"from": "latest_checkpoint"}, "rec1", "acme")
        self.service.get_execution("r", "acme")
        self.service.events("r", "acme")
        self.assertEqual(before, self.service.queue_status("r", "acme"))

    def test_migration_registers_new_revision_queues_and_rebinds_enqueueing(self):
        self.service.create_workflow(
            {"id": "wf", "nodes": TWO_TASKS, "queues": [{"name": "old", "events": ["node_completed"]}]},
            "wf-v1",
            "acme",
        )
        # The execution starts bound to v1 before the newer revision exists.
        self.start()
        self.service.create_workflow(
            {
                "id": "wf",
                "version": "v2",
                "nodes": TWO_TASKS,
                "queues": [{"name": "new", "events": ["node_completed"]}],
            },
            "wf-v2",
            "acme",
        )
        self.advance(key="adv-before")
        self.assertEqual(1, len(self.pull("old", "p-old")["messages"]))
        migrated = self.service.migrate("r", {"version": "v2"}, "mig1", "acme")
        self.assertEqual("v2", migrated["version"])
        self.assertEqual("r", self.service._queue_row("new", "acme")["execution_id"])
        self.advance(key="adv-after")
        # After the migration the event enters the new revision's queue only.
        self.assertEqual([], self.pull("old", "p-old2")["messages"])
        self.assertEqual(1, len(self.pull("new", "p-new")["messages"]))

    def test_migration_naming_another_executions_queue_conflicts_and_rolls_back(self):
        self.service.create_workflow(
            {"id": "wf", "nodes": TWO_TASKS, "queues": [{"name": "q1", "events": ["node_completed"]}]},
            "wf-v1",
            "acme",
        )
        self.start("r1", key="ex-r1")
        self.service.create_workflow(
            {
                "id": "wf",
                "version": "v2",
                "nodes": TWO_TASKS,
                "queues": [{"name": "taken", "events": ["node_completed"]}],
            },
            "wf-v2",
            "acme",
        )
        self.service.create_workflow(
            {"id": "wf2", "nodes": TASK, "queues": [{"name": "taken", "events": ["node_completed"]}]},
            "wf2",
            "acme",
        )
        self.start("r2", workflow_id="wf2", key="ex-r2")
        with self.assertRaises(ConflictError):
            self.service.migrate("r1", {"version": "v2"}, "mig-bad", "acme")
        self.assertNotIn("version", self.service.get_execution("r1", "acme"))

    def test_pull_and_ack_bodies_must_be_valid(self):
        self.make(queues=[{"name": "q", "events": ["node_completed"]}])
        self.start()
        for bad_pull in (None, [], {"x": 1}):
            with self.subTest(bad_pull=bad_pull):
                with self.assertRaises(ValidationError):
                    self.service.pull_queue("q", bad_pull, f"bp-{id(bad_pull)}", "acme")
        for bad_ack in (None, {}, [], {"receipt_id": ""}, {"receipt_id": 7}, {"receipt_id": "x", "extra": 1}):
            with self.subTest(bad_ack=bad_ack):
                with self.assertRaises(ValidationError):
                    self.service.acknowledge_queue("q", bad_ack, f"ba-{id(bad_ack)}", "acme")


class QueueHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.service = ChronicleFlow(str(Path(cls.directory.name) / "http-queues.db"))
        Handler.service = cls.service
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.service.close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, raw=None, key=None, tenant="acme"):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        if tenant is not None:
            headers["X-Tenant-Id"] = tenant
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_queue_round_trip_over_http(self):
        status, _ = self.call(
            "POST",
            "/workflows",
            {"id": "wf-http", "nodes": TASK, "queues": [{"name": "orders", "events": ["node_completed"]}]},
            key="wf-http",
        )
        self.assertEqual(201, status)
        status, _ = self.call(
            "POST", "/executions", {"id": "run-http", "workflow_id": "wf-http", "input": {}}, key="ex-http"
        )
        self.assertEqual(201, status)
        status, _ = self.call("POST", "/executions/run-http/advance", {"output": {}}, key="adv-http")
        self.assertEqual(200, status)
        status, data = self.call("POST", "/queues/orders/pull", {}, key="pull-http")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        messages = json.loads(data)["messages"]
        self.assertEqual(1, len(messages))
        self.assertEqual("orders", messages[0]["queue_name"])
        receipt = messages[0]["receipt_id"]
        status, data = self.call(
            "POST", "/executions/run-http/queues/orders/acknowledge", {"receipt_id": receipt}, key="ack-http"
        )
        self.assertEqual(200, status)
        self.assertEqual({"acknowledged": True}, json.loads(data))
        status, data = self.call("GET", "/executions/run-http/queues")
        self.assertEqual(200, status)
        payload = json.loads(data)
        self.assertEqual("orders", payload["queues"][0]["name"])
        self.assertEqual("acknowledged", payload["queues"][0]["messages"][0]["status"])

    def test_empty_pull_and_status_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-empty", "nodes": TASK, "queues": [{"name": "empty-q", "events": ["execution_terminated"]}]},
            key="wf-empty",
        )
        self.call("POST", "/executions", {"id": "run-empty", "workflow_id": "wf-empty", "input": {}}, key="ex-empty")
        status, data = self.call("POST", "/queues/empty-q/pull", {}, key="pull-empty")
        self.assertEqual(200, status)
        self.assertEqual({"messages": []}, json.loads(data))
        status, data = self.call("GET", "/executions/run-empty/queues")
        self.assertEqual(200, status)
        self.assertEqual([{"name": "empty-q", "messages": []}], json.loads(data)["queues"])

    def test_queue_http_errors(self):
        status, data = self.call("POST", "/queues/missing/pull", {}, key="pull-missing")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/queues/orders/acknowledge", {"receipt_id": "nope"}, key="ack-missing")
        self.assertEqual(404, status)
        status, data = self.call("POST", "/queues/orders/pull", {"extra": 1}, key="pull-bad")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/queues/orders/pull", {}, key=None)
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST",
            "/workflows",
            raw=b'{"id":"wf-nan-q","nodes":[],"queues":[{"name":"q","events":["node_completed"],"visibility_seconds":NaN}]}',
            key="wf-nan-q",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_duplicate_queue_name_is_conflict_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-dup-q", "nodes": TASK, "queues": [{"name": "http-dup", "events": ["node_completed"]}]},
            key="wf-dup-q-1",
        )
        self.call(
            "POST",
            "/executions",
            {"id": "run-dup-q-1", "workflow_id": "wf-dup-q", "input": {}},
            key="ex-dup-q-1",
        )
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-dup-q-2", "nodes": TASK, "queues": [{"name": "http-dup", "events": ["node_completed"]}]},
            key="wf-dup-q-2",
        )
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-dup-q-2", "workflow_id": "wf-dup-q-2", "input": {}},
            key="ex-dup-q-2",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
