import http.client
import json
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


def make_workflow(service, workflow_id="wf", nodes=None, subscriptions=None, key=None, tenant=""):
    body = {"id": workflow_id, "nodes": nodes or TASK}
    if subscriptions is not None:
        body["subscriptions"] = subscriptions
    return service.create_workflow(body, key or f"wf-{workflow_id}", tenant)


def start(service, execution_id="run", workflow_id="wf", subscriptions=None, key=None, tenant=""):
    body = {"id": execution_id, "workflow_id": workflow_id, "input": {}}
    if subscriptions is not None:
        body["subscriptions"] = subscriptions
    return service.create_execution(body, key or f"ex-{execution_id}", tenant)


def advance(service, execution_id="run", output=None, key=None, tenant=""):
    return service.advance(
        execution_id, {"output": {} if output is None else output}, key or f"adv-{execution_id}", tenant
    )


class QueueServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "queues.db"))

    def tearDown(self):
        self.directory.cleanup()

    # --- declaration -------------------------------------------------

    def test_execution_without_queues_reports_empty_list(self):
        make_workflow(self.service)
        start(self.service)
        self.assertEqual({"queues": []}, self.service.queues("run"))

    def test_workflow_and_execution_queue_targets_both_become_queues(self):
        make_workflow(
            self.service,
            "wf-wq",
            TASK,
            [{"queue": "wf-q", "events": ["node_completed"]}],
            "wf-wq",
        )
        start(
            self.service,
            "run-wq",
            "wf-wq",
            [{"queue": "run-q", "events": ["node_completed"]}],
            "run-wq",
        )
        status = self.service.queues("run-wq")
        self.assertEqual(["wf-q", "run-q"], [queue["queue"] for queue in status["queues"]])
        # The default visibility timeout is thirty seconds.
        self.assertEqual(30, status["queues"][0]["visibility_seconds"])

    def test_execution_queue_defaults_and_custom_visibility(self):
        make_workflow(self.service, "wf-v", TASK, None, "wf-v")
        start(
            self.service,
            "run-v",
            "wf-v",
            [
                {"queue": "default-q", "events": ["node_completed"]},
                {"queue": "fast-q", "events": ["node_completed"], "visibility_seconds": 0.05},
            ],
            "run-v",
        )
        queues = {q["queue"]: q for q in self.service.queues("run-v")["queues"]}
        self.assertEqual(30, queues["default-q"]["visibility_seconds"])
        self.assertEqual(0.05, queues["fast-q"]["visibility_seconds"])

    def test_invalid_queue_declarations_are_validation_errors(self):
        make_workflow(self.service, "wf-bad", TASK, None, "wf-bad")
        bad = [
            [{"queue": "", "events": ["node_completed"]}],
            [{"queue": "q", "events": ["node_completed"], "visibility_seconds": 0}],
            [{"queue": "q", "events": ["node_completed"], "visibility_seconds": -1}],
            [{"queue": "q", "events": ["node_completed"], "visibility_seconds": "1"}],
            [{"queue": 1, "events": ["node_completed"]}],
            [{"queue": "q", "events": ["bogus"]}],
            [{"queue": "q", "events": []}],
            [{"queue": "q", "events": ["node_completed", "node_completed"]}],
            [{"queue": "q", "url": "http://x", "events": ["node_completed"]}],
            [{"events": ["node_completed"]}],
            [{"queue": "q"}],
            [{"queue": "q", "events": ["node_completed"], "extra": 1}],
        ]
        for index, subscriptions in enumerate(bad):
            with self.subTest(subscriptions=subscriptions):
                with self.assertRaises(ValidationError):
                    start(self.service, f"run-bad-{index}", "wf-bad", subscriptions, f"ex-bad-{index}")
        # No partial writes: every rejected execution is missing.
        for index in range(len(bad)):
            with self.assertRaises(NotFoundError):
                self.service.get_execution(f"run-bad-{index}")

    def test_queue_names_are_unique_per_tenant_across_owners(self):
        make_workflow(self.service, "wf-q1", TASK, [{"queue": "shared", "events": ["node_completed"]}], "wf-q1")
        make_workflow(self.service, "wf-q2", TASK, None, "wf-q2")
        with self.assertRaises(ConflictError):
            start(
                self.service,
                "run-dup",
                "wf-q2",
                [{"queue": "shared", "events": ["node_completed"]}],
                "run-dup",
            )
        # The rejected execution was not created.
        with self.assertRaises(NotFoundError):
            self.service.get_execution("run-dup")

    def test_queue_names_can_be_reused_by_different_tenants(self):
        make_workflow(self.service, "wf-t", TASK, [{"queue": "q", "events": ["node_completed"]}], "wf-ta", "acme")
        make_workflow(self.service, "wf-t", TASK, [{"queue": "q", "events": ["node_completed"]}], "wf-tb", "beta")

    def test_workflow_may_redeclare_its_own_queue_name_on_a_new_version(self):
        make_workflow(
            self.service,
            "wf-ver",
            TASK,
            [{"queue": "ver-q", "events": ["node_completed"]}],
            "wf-v1",
        )
        self.service.create_workflow(
            {
                "id": "wf-ver",
                "version": "v2",
                "nodes": TASK,
                "subscriptions": [{"queue": "ver-q", "events": ["node_completed"]}],
            },
            "wf-v2",
        )

    def test_duplicate_queue_name_in_one_declaration_conflicts(self):
        make_workflow(self.service, "wf-dup", TASK, None, "wf-dup")
        with self.assertRaises(ConflictError):
            start(
                self.service,
                "run-dup2",
                "wf-dup",
                [
                    {"queue": "q", "events": ["node_completed"]},
                    {"queue": "q", "events": ["execution_completed"]},
                ],
                "run-dup2",
            )

    # --- enqueue order and identity ---------------------------------

    def test_events_enter_each_queue_in_occurrence_order(self):
        make_workflow(
            self.service,
            "wf-order",
            TWO_TASKS,
            [{"queue": "order-q", "events": ["node_completed", "execution_completed"]}],
            "wf-order",
        )
        start(self.service, "run-order", "wf-order", None, "run-order")
        advance(self.service, "run-order", {"x": 1}, "adv-order-1")
        advance(self.service, "run-order", {"x": 2}, "adv-order-2")
        messages = self.service.queues("run-order")["queues"][0]["messages"]
        self.assertEqual(
            ["node_completed", "node_completed", "execution_completed"],
            [m["event_type"] for m in messages],
        )
        self.assertEqual([1, 2, 3], [m["sequence"] for m in messages])
        self.assertEqual(
            ["a", "b", None],
            [m["payload"].get("node_id") for m in messages],
        )
        self.assertEqual(["pending"] * 3, [m["status"] for m in messages])
        self.assertEqual([0] * 3, [m["delivery_count"] for m in messages])
        for message in messages:
            self.assertTrue(message["idempotency_key"])
        self.assertEqual(3, len({m["idempotency_key"] for m in messages}))

    def test_unsubscribed_events_do_not_enter_the_queue(self):
        make_workflow(
            self.service,
            "wf-sub",
            TWO_TASKS,
            [{"queue": "sub-q", "events": ["execution_completed"]}],
            "wf-sub",
        )
        start(self.service, "run-sub", "wf-sub", None, "run-sub")
        advance(self.service, "run-sub", key="adv-sub-1")
        advance(self.service, "run-sub", key="adv-sub-2")
        messages = self.service.queues("run-sub")["queues"][0]["messages"]
        self.assertEqual(["execution_completed"], [m["event_type"] for m in messages])

    def test_multiple_queues_each_get_every_matching_event(self):
        make_workflow(self.service, "wf-multi", TASK, None, "wf-multi")
        start(
            self.service,
            "run-multi",
            "wf-multi",
            [
                {"queue": "q1", "events": ["node_completed", "execution_completed"]},
                {"queue": "q2", "events": ["execution_completed"]},
            ],
            "run-multi",
        )
        advance(self.service, "run-multi", key="adv-multi")
        queues = {q["queue"]: q for q in self.service.queues("run-multi")["queues"]}
        self.assertEqual(2, len(queues["q1"]["messages"]))
        self.assertEqual(1, len(queues["q2"]["messages"]))
        # The same event yields distinct keys per queue target.
        self.assertNotEqual(
            queues["q1"]["messages"][1]["idempotency_key"],
            queues["q2"]["messages"][0]["idempotency_key"],
        )

    def test_status_message_fields_have_stable_key_order(self):
        make_workflow(self.service, "wf-keys", TASK, [{"queue": "k-q", "events": ["node_completed"]}], "wf-keys")
        start(self.service, "run-keys", "wf-keys", None, "run-keys")
        advance(self.service, "run-keys", key="adv-keys")
        message = self.service.queues("run-keys")["queues"][0]["messages"][0]
        self.assertEqual(["sequence"] + sorted(k for k in message if k != "sequence"), list(message))

    # --- pull, visibility, and redelivery ---------------------------

    def test_pull_returns_pending_messages_in_order_then_empty(self):
        make_workflow(
            self.service,
            "wf-pull",
            TWO_TASKS,
            [{"queue": "pull-q", "events": ["node_completed"]}],
            "wf-pull",
        )
        start(self.service, "run-pull", "wf-pull", None, "run-pull")
        advance(self.service, "run-pull", key="adv-pull-1")
        advance(self.service, "run-pull", key="adv-pull-2")
        pulled = self.service.pull_queue("run-pull", "pull-q", {}, "pull-pull-1")["messages"]
        self.assertEqual(["a", "b"], [m["payload"]["node_id"] for m in pulled])
        self.assertEqual([1, 2], [m["sequence"] for m in pulled])
        # Once pulled the messages are invisible and the queue looks empty.
        self.assertEqual([], self.service.pull_queue("run-pull", "pull-q", {}, "pull-pull-2")["messages"])

    def test_empty_queue_pull_is_a_definite_empty_result(self):
        make_workflow(self.service, "wf-empty", TASK, [{"queue": "empty-q", "events": ["node_completed"]}], "wf-empty")
        start(self.service, "run-empty", "wf-empty", None, "run-empty")
        self.assertEqual({"messages": []}, self.service.pull_queue("run-empty", "empty-q", {}, "pull-empty"))

    def test_unacknowledged_message_returns_after_the_visibility_deadline(self):
        make_workflow(self.service, "wf-vis", TASK, None, "wf-vis")
        start(
            self.service,
            "run-vis",
            "wf-vis",
            [{"queue": "vis-q", "events": ["node_completed"], "visibility_seconds": 0.05}],
            "run-vis",
        )
        advance(self.service, "run-vis", key="adv-vis")
        first = self.service.pull_queue("run-vis", "vis-q", {}, "pull-vis-1")["messages"]
        self.assertEqual(1, len(first))
        self.assertEqual("delivered", first[0]["status"])
        self.assertEqual(1, first[0]["delivery_count"])
        self.assertEqual([], self.service.pull_queue("run-vis", "vis-q", {}, "pull-vis-2")["messages"])
        time.sleep(0.1)
        # Expired visibility puts the message back into the pending set,
        # redelivered under the same idempotency key.
        second = self.service.pull_queue("run-vis", "vis-q", {}, "pull-vis-3")["messages"]
        self.assertEqual(1, len(second))
        self.assertEqual(first[0]["idempotency_key"], second[0]["idempotency_key"])
        self.assertEqual(2, second[0]["delivery_count"])
        history = self.service.queues("run-vis")["queues"][0]["messages"][0]
        self.assertEqual(2, len(history["deliveries"]))
        self.assertEqual([1, 2], [d["delivery"] for d in history["deliveries"]])
        self.assertTrue(all(d.get("delivered_at") for d in history["deliveries"]))

    def test_status_shows_pending_again_after_visibility_expires(self):
        make_workflow(self.service, "wf-st", TASK, None, "wf-st")
        start(
            self.service,
            "run-st",
            "wf-st",
            [{"queue": "st-q", "events": ["node_completed"], "visibility_seconds": 0.05}],
            "run-st",
        )
        advance(self.service, "run-st", key="adv-st")
        self.service.pull_queue("run-st", "st-q", {}, "pull-st")
        self.assertEqual("delivered", self.service.queues("run-st")["queues"][0]["messages"][0]["status"])
        time.sleep(0.1)
        self.assertEqual("pending", self.service.queues("run-st")["queues"][0]["messages"][0]["status"])

    def test_acknowledgement_removes_the_message_permanently(self):
        make_workflow(self.service, "wf-ack", TASK, None, "wf-ack")
        start(
            self.service,
            "run-ack",
            "wf-ack",
            [{"queue": "ack-q", "events": ["node_completed"], "visibility_seconds": 0.05}],
            "run-ack",
        )
        advance(self.service, "run-ack", key="adv-ack")
        message = self.service.pull_queue("run-ack", "ack-q", {}, "pull-ack")["messages"][0]
        result = self.service.ack_queue(
            "run-ack", "ack-q", {"idempotency_key": message["idempotency_key"]}, "ack-ack"
        )
        self.assertEqual({"acknowledged": True}, result)
        time.sleep(0.1)
        self.assertEqual([], self.service.pull_queue("run-ack", "ack-q", {}, "pull-ack-2")["messages"])
        history = self.service.queues("run-ack")["queues"][0]["messages"][0]
        self.assertEqual("acknowledged", history["status"])

    def test_repeated_or_unknown_acknowledgement_is_not_found(self):
        make_workflow(self.service, "wf-a2", TASK, None, "wf-a2")
        start(
            self.service,
            "run-a2",
            "wf-a2",
            [{"queue": "a2-q", "events": ["node_completed"]}],
            "run-a2",
        )
        advance(self.service, "run-a2", key="adv-a2")
        message = self.service.pull_queue("run-a2", "a2-q", {}, "pull-a2")["messages"][0]
        body = {"idempotency_key": message["idempotency_key"]}
        self.service.ack_queue("run-a2", "a2-q", body, "ack-a2-1")
        with self.assertRaises(NotFoundError):
            self.service.ack_queue("run-a2", "a2-q", body, "ack-a2-2")
        with self.assertRaises(NotFoundError):
            self.service.ack_queue("run-a2", "a2-q", {"idempotency_key": "never-seen"}, "ack-a2-3")

    def test_pull_and_ack_on_unknown_queue_or_execution_are_not_found(self):
        make_workflow(self.service, "wf-miss", TASK, [{"queue": "present", "events": ["node_completed"]}], "wf-miss")
        start(self.service, "run-miss", "wf-miss", None, "run-miss")
        with self.assertRaises(NotFoundError):
            self.service.pull_queue("run-miss", "absent", {}, "pull-miss-1")
        with self.assertRaises(NotFoundError):
            self.service.ack_queue("run-miss", "absent", {"idempotency_key": "x"}, "ack-miss-1")
        with self.assertRaises(NotFoundError):
            self.service.queues("run-missing")
        with self.assertRaises(NotFoundError):
            self.service.pull_queue("run-missing", "present", {}, "pull-miss-2")

    def test_pull_and_ack_bodies_are_validated(self):
        make_workflow(self.service, "wf-body", TASK, [{"queue": "b-q", "events": ["node_completed"]}], "wf-body")
        start(self.service, "run-body", "wf-body", None, "run-body")
        for raw in (None, [], "x", {"extra": 1}, {"max_messages": 1}):
            with self.subTest(raw=raw):
                with self.assertRaises(ValidationError):
                    self.service.pull_queue("run-body", "b-q", raw, f"pull-bad-{type(raw).__name__}")
        for raw in (None, {}, [], {"idempotency_key": ""}, {"idempotency_key": 1}, {"idempotency_key": "k", "extra": 1}):
            with self.subTest(raw=raw):
                with self.assertRaises(ValidationError):
                    self.service.ack_queue("run-body", "b-q", raw, f"ack-bad-{type(raw).__name__}")

    # --- metering ----------------------------------------------------

    def test_every_pull_delivery_is_metered(self):
        make_workflow(self.service, "wf-met", TASK, None, "wf-met", tenant="acme")
        start(
            self.service,
            "run-met",
            "wf-met",
            [{"queue": "met-q", "events": ["node_completed"], "visibility_seconds": 0.05}],
            "run-met",
            tenant="acme",
        )
        advance(self.service, "run-met", key="adv-met", tenant="acme")
        self.service.pull_queue("run-met", "met-q", {}, "pull-met-1", "acme")
        counts = {item["type"]: item["count"] for item in self.service.usage("acme")["usage"]}
        self.assertEqual(1, counts["delivery_attempted"])
        # An empty pull meters nothing.
        self.service.pull_queue("run-met", "met-q", {}, "pull-met-2", "acme")
        counts = {item["type"]: item["count"] for item in self.service.usage("acme")["usage"]}
        self.assertEqual(1, counts["delivery_attempted"])
        # The visibility-timeout redelivery meters again, exactly once.
        time.sleep(0.1)
        self.service.pull_queue("run-met", "met-q", {}, "pull-met-3", "acme")
        counts = {item["type"]: item["count"] for item in self.service.usage("acme")["usage"]}
        self.assertEqual(2, counts["delivery_attempted"])

    def test_legacy_namespace_queue_pulls_are_not_metered(self):
        make_workflow(self.service, "wf-leg", TASK, None, "wf-leg")
        start(
            self.service,
            "run-leg",
            "wf-leg",
            [{"queue": "leg-q", "events": ["node_completed"]}],
            "run-leg",
        )
        advance(self.service, "run-leg", key="adv-leg")
        self.service.pull_queue("run-leg", "leg-q", {}, "pull-leg")
        row = self.service.store.connection.execute("SELECT COUNT(*) AS count FROM usage_records").fetchone()
        self.assertEqual(0, row["count"])

    def test_repeated_idempotent_pull_returns_first_result_and_meters_once(self):
        make_workflow(self.service, "wf-idem", TASK, None, "wf-idem", tenant="acme")
        start(
            self.service,
            "run-idem",
            "wf-idem",
            [{"queue": "idem-q", "events": ["node_completed"]}],
            "run-idem",
            tenant="acme",
        )
        advance(self.service, "run-idem", key="adv-idem", tenant="acme")
        first = self.service.pull_queue("run-idem", "idem-q", {}, "same-pull-key", "acme")
        second = self.service.pull_queue("run-idem", "idem-q", {}, "same-pull-key", "acme")
        self.assertEqual(first, second)
        self.assertEqual(1, len(first["messages"]))
        counts = {item["type"]: item["count"] for item in self.service.usage("acme")["usage"]}
        self.assertEqual(1, counts["delivery_attempted"])

    # --- tenancy -----------------------------------------------------

    def test_queues_follow_the_execution_tenant(self):
        make_workflow(self.service, "wf-ten", TASK, [{"queue": "ten-q", "events": ["node_completed"]}], "wf-ten", "acme")
        start(self.service, "run-ten", "wf-ten", None, "run-ten", "acme")
        advance(self.service, "run-ten", key="adv-ten", tenant="acme")
        # Another tenant sees the execution as missing, for every entry point.
        with self.assertRaises(NotFoundError):
            self.service.queues("run-ten", "beta")
        with self.assertRaises(NotFoundError):
            self.service.pull_queue("run-ten", "ten-q", {}, "pull-ten", "beta")
        with self.assertRaises(NotFoundError):
            self.service.ack_queue("run-ten", "ten-q", {"idempotency_key": "x"}, "ack-ten", "beta")
        # The owner tenant can pull the message.
        self.assertEqual(1, len(self.service.pull_queue("run-ten", "ten-q", {}, "pull-ten-owner", "acme")["messages"]))

    # --- no interference with existing semantics --------------------

    def test_webhook_and_queue_targets_fire_independently(self):
        make_workflow(self.service, "wf-both", TASK, None, "wf-both", tenant="acme")
        # The HTTP receiver is exercised by the HTTP tests below; here just
        # assert the two subscription kinds coexist and feed separately.
        start(
            self.service,
            "run-both",
            "wf-both",
            [{"queue": "both-q", "events": ["node_completed", "execution_completed"]}],
            "run-both",
            tenant="acme",
        )
        advance(self.service, "run-both", key="adv-both", tenant="acme")
        messages = self.service.queues("run-both", "acme")["queues"][0]["messages"]
        self.assertEqual(["node_completed", "execution_completed"], [m["event_type"] for m in messages])

    def test_replay_and_queries_do_not_enqueue(self):
        make_workflow(self.service, "wf-rep", TASK, None, "wf-rep")
        start(
            self.service,
            "run-rep",
            "wf-rep",
            [{"queue": "rep-q", "events": ["node_completed", "execution_completed"]}],
            "run-rep",
        )
        advance(self.service, "run-rep", key="adv-rep")
        self.assertEqual(2, len(self.service.queues("run-rep")["queues"][0]["messages"]))
        self.service.replay("run-rep")
        self.service.get_execution("run-rep")
        self.service.events("run-rep")
        self.service.checkpoints("run-rep")
        self.assertEqual(2, len(self.service.queues("run-rep")["queues"][0]["messages"]))

    def test_queues_survive_a_restart(self):
        database = str(Path(self.directory.name) / "queues.db")
        make_workflow(self.service, "wf-live", TASK, [{"queue": "live-q", "events": ["node_completed"]}], "wf-live")
        start(self.service, "run-live", "wf-live", None, "run-live")
        advance(self.service, "run-live", key="adv-live")
        pulled = self.service.pull_queue("run-live", "live-q", {}, "pull-live")["messages"]
        # A fresh service on the same database sees the same queue state.
        restarted = ChronicleFlow(database)
        status = restarted.queues("run-live")["queues"][0]
        self.assertEqual("live-q", status["queue"])
        self.assertEqual(1, len(status["messages"]))
        self.assertEqual("delivered", status["messages"][0]["status"])
        self.assertEqual(pulled[0]["idempotency_key"], status["messages"][0]["idempotency_key"])
        result = restarted.ack_queue(
            "run-live", "live-q", {"idempotency_key": pulled[0]["idempotency_key"]}, "ack-live"
        )
        self.assertEqual({"acknowledged": True}, result)


class Receiver(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        Receiver.requests.append(json.loads(self.rfile.read(length)))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class QueueHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-queues.db"))
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

    def call(self, method, path, body=None, key=None, raw=None, tenant=None):
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

    def target(self, path="/hook"):
        return f"http://127.0.0.1:{self.target_port}{path}"

    def test_full_pull_ack_lifecycle_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-h1", "nodes": TASK, "subscriptions": [
                {"url": self.target(), "events": ["node_completed"]},
                {"queue": "hq", "events": ["node_completed", "execution_completed"], "visibility_seconds": 0.05},
            ]},
            key="wf-h1",
            tenant="acme",
        )
        self.call("POST", "/executions", {"id": "run-h1", "workflow_id": "wf-h1", "input": {}}, key="ex-h1", tenant="acme")
        status, data = self.call("POST", "/executions/run-h1/advance", {"output": {}}, "adv-h1", tenant="acme")
        self.assertEqual(200, status)
        # The webhook target still fired; the queue target did not post HTTP.
        self.assertEqual(1, len(Receiver.requests))

        status, data = self.call("GET", "/executions/run-h1/queues", tenant="acme")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        document = json.loads(data)
        self.assertEqual(["hq"], [q["queue"] for q in document["queues"]])
        messages = document["queues"][0]["messages"]
        self.assertEqual(2, len(messages))
        self.assertEqual(
            ["node_completed", "execution_completed"], [m["event_type"] for m in messages]
        )

        status, data = self.call("POST", "/executions/run-h1/queues/hq/pull", {}, key="pull-h1", tenant="acme")
        self.assertEqual(200, status)
        pulled = json.loads(data)["messages"]
        self.assertEqual(2, len(pulled))
        self.assertEqual(1, len(Receiver.requests))

        # The pulled message is invisible; ack the first one.
        status, _ = self.call(
            "POST",
            "/executions/run-h1/queues/hq/ack",
            {"idempotency_key": pulled[0]["idempotency_key"]},
            key="ack-h1",
            tenant="acme",
        )
        self.assertEqual(200, status)

        time.sleep(0.1)
        status, data = self.call("POST", "/executions/run-h1/queues/hq/pull", {}, key="pull-h1-2", tenant="acme")
        self.assertEqual(200, status)
        redelivered = json.loads(data)["messages"]
        self.assertEqual(1, len(redelivered))
        self.assertEqual("execution_completed", redelivered[0]["event_type"])
        self.assertEqual(2, redelivered[0]["delivery_count"])

    def test_empty_pull_and_empty_status_over_http(self):
        self.call("POST", "/workflows", {"id": "wf-h2", "nodes": TASK}, key="wf-h2", tenant="acme")
        self.call(
            "POST",
            "/executions",
            {"id": "run-h2", "workflow_id": "wf-h2", "input": {},
             "subscriptions": [{"queue": "h2q", "events": ["node_completed"]}]},
            key="ex-h2",
            tenant="acme",
        )
        status, data = self.call("POST", "/executions/run-h2/queues/h2q/pull", {}, key="pull-h2", tenant="acme")
        self.assertEqual(200, status)
        self.assertEqual({"messages": []}, json.loads(data))
        status, data = self.call("GET", "/executions/run-never-exists/queues", tenant="acme")
        self.assertEqual(404, status)

    def test_cross_tenant_queue_access_is_not_found(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-h3", "nodes": TASK, "subscriptions": [{"queue": "h3q", "events": ["node_completed"]}]},
            key="wf-h3",
            tenant="acme",
        )
        self.call("POST", "/executions", {"id": "run-h3", "workflow_id": "wf-h3", "input": {}}, key="ex-h3", tenant="acme")
        self.call("POST", "/executions/run-h3/advance", {"output": {}}, "adv-h3", tenant="acme")
        status, data = self.call("GET", "/executions/run-h3/queues", tenant="beta")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-h3/queues/h3q/pull", {}, key="pull-h3", tenant="beta")
        self.assertEqual(404, status)
        status, data = self.call(
            "POST",
            "/executions/run-h3/queues/h3q/ack",
            {"idempotency_key": "x"},
            key="ack-h3",
            tenant="beta",
        )
        self.assertEqual(404, status)

    def test_queue_validation_errors_over_http(self):
        self.call("POST", "/workflows", {"id": "wf-h4", "nodes": TASK}, key="wf-h4", tenant="acme")
        status, data = self.call(
            "POST",
            "/executions",
            raw=(
                b'{"id":"run-h4","workflow_id":"wf-h4","input":{},'
                b'"subscriptions":[{"queue":"q","events":["node_completed"],"visibility_seconds":NaN}]}'
            ),
            key="ex-h4",
            tenant="acme",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-h4b", "workflow_id": "wf-h4", "input": {},
             "subscriptions": [{"queue": "", "events": ["node_completed"]}]},
            key="ex-h4b",
            tenant="acme",
        )
        self.assertEqual(400, status)
        self.call(
            "POST",
            "/executions",
            {"id": "run-h4c", "workflow_id": "wf-h4", "input": {},
             "subscriptions": [{"queue": "q4", "events": ["node_completed"]}]},
            key="ex-h4c",
            tenant="acme",
        )
        # Pull body unknown field.
        status, _ = self.call(
            "POST", "/executions/run-h4c/queues/q4/pull", {"unexpected": 1}, key="pull-h4", tenant="acme"
        )
        self.assertEqual(400, status)
        # Ack body missing field.
        status, data = self.call("POST", "/executions/run-h4c/queues/q4/ack", {}, key="ack-h4", tenant="acme")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_duplicate_queue_name_across_owners_is_conflict_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-h5", "nodes": TASK, "subscriptions": [{"queue": "dup-h5", "events": ["node_completed"]}]},
            key="wf-h5a",
            tenant="acme",
        )
        self.call("POST", "/workflows", {"id": "wf-h5b", "nodes": TASK}, key="wf-h5b", tenant="acme")
        status, data = self.call(
            "POST",
            "/executions",
            {"id": "run-h5", "workflow_id": "wf-h5b", "input": {},
             "subscriptions": [{"queue": "dup-h5", "events": ["node_completed"]}]},
            key="ex-h5",
            tenant="acme",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_ack_of_unknown_message_is_not_found_over_http(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-h6", "nodes": TASK, "subscriptions": [{"queue": "h6q", "events": ["node_completed"]}]},
            key="wf-h6",
            tenant="acme",
        )
        self.call("POST", "/executions", {"id": "run-h6", "workflow_id": "wf-h6", "input": {}}, key="ex-h6", tenant="acme")
        status, data = self.call(
            "POST", "/executions/run-h6/queues/h6q/ack", {"idempotency_key": "nope"}, key="ack-h6", tenant="acme"
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
