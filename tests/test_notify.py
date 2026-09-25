import http.client
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


class Receiver(BaseHTTPRequestHandler):
    requests = []
    failures_before_success = 0

    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        Receiver.requests.append(
            {"path": self.path, "idempotency_key": self.headers.get("Idempotency-Key"), "body": json.loads(body)}
        )
        if Receiver.failures_before_success > 0:
            Receiver.failures_before_success -= 1
            self.send_response(500)
        else:
            self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class WebhookHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-notify.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        cls.target_port = cls.receiver.server_address[1]
        cls.receiver_thread = threading.Thread(target=cls.receiver.serve_forever, daemon=True)
        cls.receiver_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.receiver.shutdown()
        cls.receiver.server_close()
        cls.directory.cleanup()

    def setUp(self):
        Receiver.requests = []
        Receiver.failures_before_success = 0

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

    @property
    def target(self):
        return f"http://127.0.0.1:{self.target_port}/hook"

    def make_workflow(self, workflow_id, nodes, subscriptions=None, key=None):
        body = {"id": workflow_id, "nodes": nodes}
        if subscriptions is not None:
            body["subscriptions"] = subscriptions
        return self.call("POST", "/workflows", body, key or f"wf-{workflow_id}")

    def start(self, execution_id, workflow_id, subscriptions=None, key=None):
        body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
        if subscriptions is not None:
            body["subscriptions"] = subscriptions
        return self.call("POST", "/executions", body, key or f"ex-{execution_id}")

    def deliveries(self, execution_id):
        status, data = self.call("GET", f"/executions/{execution_id}/deliveries")
        self.assertEqual(200, status)
        return json.loads(data)["deliveries"]

    def test_node_completed_is_delivered_and_recorded(self):
        self.make_workflow("wf-n1", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start("run-n1", "wf-n1", [{"url": self.target, "events": ["node_completed"]}])
        status, _ = self.call("POST", "/executions/run-n1/advance", {"output": {"v": 1}}, "adv-n1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(Receiver.requests))
        message = Receiver.requests[0]["body"]
        self.assertEqual("node_completed", message["event_type"])
        self.assertEqual("run-n1", message["execution_id"])
        self.assertEqual("a", message["node_id"])
        self.assertTrue(Receiver.requests[0]["idempotency_key"])
        records = self.deliveries("run-n1")
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual(self.target, record["url"])
        self.assertEqual("node_completed", record["event_type"])
        self.assertEqual(1, record["attempt_count"])
        self.assertEqual("delivered", record["status"])
        self.assertEqual([{"attempt": 1, "status_code": 200}], record["attempts"])
        self.assertEqual(Receiver.requests[0]["idempotency_key"], record["idempotency_key"])
        # new fields are emitted in a stable key order
        self.assertEqual(["sequence"] + sorted(k for k in record if k != "sequence"), list(record))

    def test_workflow_subscription_applies_to_executions(self):
        self.make_workflow(
            "wf-n2",
            [{"id": "a", "kind": "task", "depends_on": []}],
            [{"url": self.target, "events": ["execution_completed"]}],
        )
        self.start("run-n2", "wf-n2")
        status, data = self.call("POST", "/executions/run-n2/advance", {"output": {}}, "adv-n2")
        self.assertEqual(200, status)
        self.assertEqual("completed", json.loads(data)["status"])
        self.assertEqual(1, len(Receiver.requests))
        message = Receiver.requests[0]["body"]
        self.assertEqual("execution_completed", message["event_type"])
        self.assertEqual("run-n2", message["execution_id"])
        records = self.deliveries("run-n2")
        self.assertEqual(1, len(records))
        self.assertEqual("execution_completed", records[0]["event_type"])

    def test_termination_is_a_single_event_type(self):
        self.make_workflow("wf-n3", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start("run-n3", "wf-n3", [{"url": self.target, "events": ["execution_terminated"]}])
        status, _ = self.call("POST", "/executions/run-n3/cancel", key="cx-n3")
        self.assertEqual(200, status)
        self.assertEqual(1, len(Receiver.requests))
        message = Receiver.requests[0]["body"]
        self.assertEqual("execution_terminated", message["event_type"])
        self.assertEqual("cancelled", message["reason"])

    def test_approval_decision_is_delivered_with_approver(self):
        self.make_workflow(
            "wf-n4",
            [{"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}}],
        )
        self.start("run-n4", "wf-n4", [{"url": self.target, "events": ["approval_decided"]}])
        self.call("POST", "/executions/run-n4/advance", {"output": {}}, "adv-n4")
        self.assertEqual(0, len(Receiver.requests))
        status, _ = self.call(
            "POST",
            "/executions/run-n4/decision",
            {"approver": "alice", "decision": "approved", "output": {}},
            "dec-n4",
        )
        self.assertEqual(200, status)
        self.assertEqual(1, len(Receiver.requests))
        message = Receiver.requests[0]["body"]
        self.assertEqual("approval_decided", message["event_type"])
        self.assertEqual("alice", message["approver"])
        self.assertEqual("a", message["node_id"])

    def test_unreachable_target_does_not_change_the_outcome(self):
        self.make_workflow("wf-n5", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start(
            "run-n5",
            "wf-n5",
            [{"url": "http://127.0.0.1:1/hook", "events": ["node_completed"], "timeout_seconds": 1}],
        )
        status, data = self.call("POST", "/executions/run-n5/advance", {"output": {"v": 1}}, "adv-n5")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("completed", state["status"])
        self.assertEqual({"a": {"v": 1}}, state["outputs"])
        records = self.deliveries("run-n5")
        self.assertEqual(1, len(records))
        self.assertEqual("failed", records[0]["status"])
        self.assertEqual(1, records[0]["attempt_count"])
        self.assertIn("error", records[0]["attempts"][0])

    def test_retry_then_success_is_recorded_attempt_by_attempt(self):
        Receiver.failures_before_success = 1
        self.make_workflow("wf-n6", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start(
            "run-n6",
            "wf-n6",
            [{"url": self.target, "events": ["node_completed"], "max_attempts": 3}],
        )
        status, _ = self.call("POST", "/executions/run-n6/advance", {"output": {}}, "adv-n6")
        self.assertEqual(200, status)
        self.assertEqual(2, len(Receiver.requests))
        # retries of the same event reuse the idempotency key
        self.assertEqual(Receiver.requests[0]["idempotency_key"], Receiver.requests[1]["idempotency_key"])
        records = self.deliveries("run-n6")
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual("delivered", record["status"])
        self.assertEqual(2, record["attempt_count"])
        self.assertEqual(
            [{"attempt": 1, "status_code": 500}, {"attempt": 2, "status_code": 200}],
            record["attempts"],
        )

    def test_distinct_events_use_distinct_idempotency_keys(self):
        self.make_workflow(
            "wf-n7",
            [
                {"id": "a", "kind": "task", "depends_on": []},
                {"id": "b", "kind": "task", "depends_on": ["a"]},
            ],
        )
        self.start("run-n7", "wf-n7", [{"url": self.target, "events": ["node_completed", "execution_completed"]}])
        self.call("POST", "/executions/run-n7/advance", {"output": {}}, "adv-n7-1")
        self.call("POST", "/executions/run-n7/advance", {"output": {}}, "adv-n7-2")
        records = self.deliveries("run-n7")
        self.assertEqual(["node_completed", "node_completed", "execution_completed"], [r["event_type"] for r in records])
        keys = [record["idempotency_key"] for record in records]
        self.assertEqual(3, len(set(keys)))
        self.assertEqual([1, 2, 3], [record["sequence"] for record in records])

    def test_unsubscribed_execution_is_unchanged(self):
        self.make_workflow("wf-n8", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start("run-n8", "wf-n8")
        status, data = self.call("POST", "/executions/run-n8/advance", {"output": {"v": -0.0}}, "adv-n8")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual(
            {
                "id",
                "workflow_id",
                "status",
                "termination_reason",
                "timeout_seconds",
                "deadline_at",
                "input",
                "completed_nodes",
                "skipped_nodes",
                "failed_nodes",
                "condition_results",
                "outputs",
                "attempts",
                "loops",
            },
            set(state),
        )
        status, data = self.call("GET", "/executions/run-n8/events")
        self.assertEqual(
            ["execution_started", "node_completed", "execution_completed"],
            [event["type"] for event in json.loads(data)["events"]],
        )
        self.assertEqual([], self.deliveries("run-n8"))
        status, data = self.call("POST", "/executions/run-n8/replay")
        self.assertTrue(json.loads(data)["consistent"])

    def test_replay_and_recover_do_not_trigger_new_deliveries(self):
        self.make_workflow("wf-n9", [{"id": "a", "kind": "task", "depends_on": []}])
        self.start("run-n9", "wf-n9", [{"url": self.target, "events": ["node_completed", "execution_completed"]}])
        self.call("POST", "/executions/run-n9/advance", {"output": {}}, "adv-n9")
        self.assertEqual(2, len(self.deliveries("run-n9")))
        self.call("POST", "/executions/run-n9/replay")
        self.call("POST", "/executions/run-n9/recover", {"from": "latest_checkpoint"}, "rec-n9")
        self.call("GET", "/executions/run-n9")
        self.call("GET", "/executions/run-n9/events")
        self.assertEqual(2, len(self.deliveries("run-n9")))

    def test_deliveries_of_missing_execution_are_not_found(self):
        status, data = self.call("GET", "/executions/nope/deliveries")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_invalid_subscriptions_are_validation_errors(self):
        bad_subscriptions = [
            [{"url": self.target, "events": []}],
            [{"url": self.target, "events": ["node_completed", "node_completed"]}],
            [{"url": self.target, "events": ["node_started"]}],
            [{"url": "", "events": ["node_completed"]}],
            [{"url": "ftp://example.com/hook", "events": ["node_completed"]}],
            [{"url": self.target}],
            [{"events": ["node_completed"]}],
            [{"url": self.target, "events": ["node_completed"], "extra": 1}],
            [{"url": self.target, "events": ["node_completed"], "timeout_seconds": 0}],
            [{"url": self.target, "events": ["node_completed"], "timeout_seconds": -1}],
            [{"url": self.target, "events": ["node_completed"], "max_attempts": 0}],
            [{"url": self.target, "events": ["node_completed"], "max_attempts": 11}],
            [{"url": self.target, "events": ["node_completed"], "max_attempts": 1.5}],
            "not-an-object",
        ]
        for index, subscriptions in enumerate(bad_subscriptions):
            with self.subTest(subscriptions=subscriptions):
                status, data = self.start("run-bad-sub", "wf-n8", subscriptions, key=f"ex-bad-sub-{index}")
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # no partial writes: the rejected execution was never created
        status, _ = self.call("GET", "/executions/run-bad-sub")
        self.assertEqual(404, status)
        status, data = self.make_workflow(
            "wf-bad-sub",
            [{"id": "a", "kind": "task", "depends_on": []}],
            [{"url": self.target, "events": ["unknown"]}],
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-bad-sub-2", "workflow_id": "wf-bad-sub", "input": {}},
            "ex-bad-sub-2",
        )
        self.assertEqual(404, status)

    def test_non_finite_subscription_numbers_are_rejected(self):
        status, data = self.call(
            "POST",
            "/executions",
            raw=(
                b'{"id":"run-nan-sub","workflow_id":"wf-n8","input":{},'
                b'"subscriptions":[{"url":"http://127.0.0.1/h","events":["node_completed"],"timeout_seconds":NaN}]}'
            ),
            key="ex-nan-sub",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_idempotency_key_reuse_across_operations_conflicts(self):
        self.make_workflow(
            "wf-n10",
            [{"id": "a", "kind": "task", "depends_on": []}],
            [{"url": self.target, "events": ["node_completed"]}],
            key="shared-notify-key",
        )
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-n10", "workflow_id": "wf-n10", "input": {}, "subscriptions": []},
            "shared-notify-key",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_workflow_and_execution_subscriptions_both_fire_in_order(self):
        self.make_workflow(
            "wf-n11",
            [{"id": "a", "kind": "task", "depends_on": []}],
            [{"url": self.target, "events": ["node_completed"]}],
        )
        self.start("run-n11", "wf-n11", [{"url": self.target, "events": ["node_completed"]}])
        self.call("POST", "/executions/run-n11/advance", {"output": {}}, "adv-n11")
        records = self.deliveries("run-n11")
        self.assertEqual(2, len(records))
        self.assertEqual(2, len(Receiver.requests))
        self.assertNotEqual(records[0]["idempotency_key"], records[1]["idempotency_key"])


if __name__ == "__main__":
    unittest.main()
