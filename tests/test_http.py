import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


class HttpTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def request(self, method, path, body=None, raw=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        all_headers = {"Content-Type": "application/json"}
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_health_response_ends_with_single_newline(self):
        status, data = self.request("GET", "/health")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.assertEqual({"status": "ok"}, json.loads(data))

    def test_workflow_creation_response_ends_with_newline(self):
        status, data = self.request(
            "POST",
            "/workflows",
            {"id": "wf-nl", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            headers={"Idempotency-Key": "wf-nl"},
        )
        self.assertEqual(201, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_error_response_ends_with_newline(self):
        status, data = self.request("GET", "/executions/missing")
        self.assertEqual(404, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_negative_zero_and_float_precision_are_preserved(self):
        self.request(
            "POST",
            "/workflows",
            {"id": "wf-float", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            headers={"Idempotency-Key": "wf-float"},
        )
        self.request(
            "POST",
            "/executions",
            {"id": "run-float", "workflow_id": "wf-float", "input": {"value": -0.0}},
            headers={"Idempotency-Key": "ex-float"},
        )
        status, data = self.request(
            "POST",
            "/executions/run-float/advance",
            {"output": {"result": -0.0, "precise": 0.30000000000000004}},
            headers={"Idempotency-Key": "adv-float"},
        )
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual(-0.0, state["input"]["value"])
        self.assertTrue(json.dumps(state["input"]["value"]).startswith("-"))
        self.assertEqual(-0.0, state["outputs"]["a"]["result"])
        self.assertEqual(0.30000000000000004, state["outputs"]["a"]["precise"])
        self.assertIn(b"-0.0", data)

    def test_submitted_float_lexeme_is_echoed_unchanged(self):
        self.request(
            "POST",
            "/workflows",
            {"id": "wf-lex", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            headers={"Idempotency-Key": "wf-lex"},
        )
        self.request(
            "POST",
            "/executions",
            raw=b'{"id":"run-lex","workflow_id":"wf-lex","input":{}}',
            headers={"Idempotency-Key": "ex-lex"},
        )
        # The token 0.12345678901234567 parses to the same double as the
        # shorter ...566; the submitted spelling must nevertheless survive.
        status, data = self.request(
            "POST",
            "/executions/run-lex/advance",
            raw=b'{"output":{"p":0.12345678901234567,"z":-0.0}}',
            headers={"Idempotency-Key": "adv-lex"},
        )
        self.assertEqual(200, status)
        self.assertIn(b"0.12345678901234567", data)
        self.assertIn(b"-0.0", data)
        status, data = self.request("GET", "/executions/run-lex")
        self.assertIn(b"0.12345678901234567", data)
        self.assertIn(b"-0.0", data)

    def test_non_finite_constants_are_rejected(self):
        for constant in (b"NaN", b"Infinity", b"-Infinity"):
            with self.subTest(constant=constant):
                status, data = self.request(
                    "POST",
                    "/executions",
                    raw=b'{"id":"run-nan","workflow_id":"wf-float","input":{"value":' + constant + b"}}",
                    headers={"Idempotency-Key": "ex-nan-" + constant.decode().lstrip("-").lower()},
                )
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_overflowing_number_is_rejected(self):
        status, data = self.request(
            "POST",
            "/executions",
            raw=b'{"id":"run-inf","workflow_id":"wf-float","input":{"value":1e400}}',
            headers={"Idempotency-Key": "ex-inf"},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_missing_idempotency_key_is_rejected(self):
        status, data = self.request(
            "POST",
            "/workflows",
            {"id": "wf-nokey", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_key_reused_across_operations_conflicts(self):
        self.request(
            "POST",
            "/workflows",
            {"id": "wf-shared", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            headers={"Idempotency-Key": "shared-key"},
        )
        status, data = self.request(
            "POST",
            "/executions",
            {"id": "run-shared", "workflow_id": "wf-shared", "input": {}},
            headers={"Idempotency-Key": "shared-key"},
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])


class CheckpointHttpTransportTests(unittest.TestCase):
    @staticmethod
    def _request(port, method, path, body=None, raw=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-checkpoints.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls._request(
            cls.port,
            "POST",
            "/workflows",
            {
                "id": "wf-cp",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            key="wf-cp",
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None):
        return self._request(self.port, method, path, body=body, raw=raw, key=key)

    def test_checkpoints_are_queryable_after_advance(self):
        self.call("POST", "/executions", {"id": "run-cp", "workflow_id": "wf-cp", "input": {}}, "ex-cp")
        status, _ = self.call("POST", "/executions/run-cp/advance", {"output": {"v": 1}}, "adv-cp-1")
        self.assertEqual(200, status)
        status, data = self.call("GET", "/executions/run-cp/checkpoints")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        payload = json.loads(data)
        self.assertEqual(1, len(payload["checkpoints"]))
        checkpoint = payload["checkpoints"][0]
        self.assertEqual(1, checkpoint["sequence"])
        self.assertEqual(2, checkpoint["event_sequence"])
        self.assertEqual(["a"], checkpoint["state"]["completed_nodes"])

    def test_recover_returns_rebuilt_execution(self):
        self.call("POST", "/executions", {"id": "run-rec", "workflow_id": "wf-cp", "input": {}}, "ex-rec")
        self.call("POST", "/executions/run-rec/advance", {"output": {"v": 1}}, "adv-rec-1")
        status, data = self.call(
            "POST",
            "/executions/run-rec/recover",
            {"from": "latest_checkpoint"},
            "rec-1",
        )
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        state = json.loads(data)
        self.assertEqual("running", state["status"])
        self.assertEqual(["a"], state["completed_nodes"])

    def test_recover_missing_execution_is_not_found(self):
        status, data = self.call(
            "POST",
            "/executions/nope/recover",
            {"from": "latest_checkpoint"},
            "rec-missing",
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_checkpoints_of_missing_execution_are_not_found(self):
        status, data = self.call("GET", "/executions/nope/checkpoints")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_recover_without_checkpoint_conflicts(self):
        self.call("POST", "/executions", {"id": "run-nocp", "workflow_id": "wf-cp", "input": {}}, "ex-nocp")
        status, data = self.call(
            "POST",
            "/executions/run-nocp/recover",
            {"from": "latest_checkpoint"},
            "rec-nocp",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_invalid_recover_bodies_are_validation_errors(self):
        self.call("POST", "/executions", {"id": "run-bad", "workflow_id": "wf-cp", "input": {}}, "ex-bad")
        for index, raw in enumerate((b"{}", b'{"from":5}', b'{"from":"elsewhere"}', b'{"from":"latest_checkpoint","x":1}', b"[]")):
            status, data = self.call("POST", "/executions/run-bad/recover", raw=raw, key=f"rec-bad-{index}")
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_non_finite_recover_body_is_rejected(self):
        status, data = self.call(
            "POST",
            "/executions/run-bad/recover",
            raw=b'{"from":"latest_checkpoint","v":NaN}',
            key="rec-nan",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_recover_key_reused_from_advance_conflicts(self):
        self.call("POST", "/executions", {"id": "run-key", "workflow_id": "wf-cp", "input": {}}, "ex-key")
        self.call("POST", "/executions/run-key/advance", {"output": {}}, "shared-rec-key")
        status, data = self.call(
            "POST",
            "/executions/run-key/recover",
            {"from": "latest_checkpoint"},
            "shared-rec-key",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_recover_then_continue_after_restart_is_consistent(self):
        self.call("POST", "/executions", {"id": "run-restart", "workflow_id": "wf-cp", "input": {}}, "ex-restart")
        self.call("POST", "/executions/run-restart/advance", {"output": {"v": 1}}, "adv-restart-1")
        status, _ = self.call(
            "POST",
            "/executions/run-restart/recover",
            {"from": "latest_checkpoint"},
            "rec-restart",
        )
        self.assertEqual(200, status)
        status, data = self.call("POST", "/executions/run-restart/advance", {"output": {"v": 2}}, "adv-restart-2")
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("completed", state["status"])
        self.assertEqual({"a": {"v": 1}, "b": {"v": 2}}, state["outputs"])
        status, data = self.call("POST", "/executions/run-restart/replay")
        self.assertEqual(200, status)
        self.assertTrue(json.loads(data)["consistent"])


class WorkerLeaseHttpTests(unittest.TestCase):
    @staticmethod
    def _request(port, method, path, body=None, raw=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-leases.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls._request(
            cls.port,
            "POST",
            "/workflows",
            {
                "id": "wf-lease",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            key="wf-lease",
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None):
        return self._request(self.port, method, path, body=body, raw=raw, key=key)

    def start(self, execution_id):
        self.call("POST", "/executions", {"id": execution_id, "workflow_id": "wf-lease", "input": {}}, f"ex-{execution_id}")

    def test_claim_heartbeat_release_round_trip(self):
        self.start("run-l1")
        status, data = self.call("POST", "/executions/run-l1/claim", {"worker_id": "w-1", "lease_seconds": 30}, "cl-l1")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        payload = json.loads(data)
        self.assertEqual({"execution_id": "run-l1", "workflow_id": "wf-lease"}, payload["work_item"])
        self.assertEqual("w-1", payload["lease"]["worker_id"])
        status, data = self.call("POST", "/executions/run-l1/heartbeat", {"worker_id": "w-1"}, "hb-l1")
        self.assertEqual(200, status)
        self.assertEqual("w-1", json.loads(data)["lease"]["worker_id"])
        status, data = self.call("POST", "/executions/run-l1/release", {"worker_id": "w-1"}, "rl-l1")
        self.assertEqual(200, status)
        self.assertEqual({"released": True}, json.loads(data))
        status, data = self.call("POST", "/executions/run-l1/claim", {"worker_id": "w-2"}, "cl-l1b")
        self.assertEqual(200, status)
        self.assertEqual("w-2", json.loads(data)["lease"]["worker_id"])
        # claims, heartbeats, and releases append no events
        status, data = self.call("GET", "/executions/run-l1/events")
        self.assertEqual(["execution_started"], [event["type"] for event in json.loads(data)["events"]])

    def test_held_work_item_gates_advance(self):
        self.start("run-l2")
        self.call("POST", "/executions/run-l2/claim", {"worker_id": "w-1"}, "cl-l2")
        status, data = self.call("POST", "/executions/run-l2/advance", {"output": {"v": 1}}, "adv-l2-anon")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST", "/executions/run-l2/advance", {"output": {"v": 1}, "worker_id": "w-2"}, "adv-l2-other"
        )
        self.assertEqual(409, status)
        status, data = self.call("POST", "/executions/run-l2/advance", {"output": {"v": 1}, "worker_id": "w-1"}, "adv-l2")
        self.assertEqual(200, status)
        self.assertEqual({"a": {"v": 1}}, json.loads(data)["outputs"])

    def test_double_claim_conflicts(self):
        self.start("run-l3")
        self.call("POST", "/executions/run-l3/claim", {"worker_id": "w-1"}, "cl-l3")
        status, data = self.call("POST", "/executions/run-l3/claim", {"worker_id": "w-2"}, "cl-l3b")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_claim_on_finished_execution_is_empty(self):
        self.start("run-l4")
        self.call("POST", "/executions/run-l4/advance", {"output": {}}, "adv-l4-1")
        self.call("POST", "/executions/run-l4/advance", {"output": {}}, "adv-l4-2")
        status, data = self.call("POST", "/executions/run-l4/claim", {"worker_id": "w-1"}, "cl-l4")
        self.assertEqual(200, status)
        self.assertEqual({"work_item": None, "lease": None}, json.loads(data))

    def test_lease_errors(self):
        self.start("run-l5")
        status, data = self.call("POST", "/executions/nope/claim", {"worker_id": "w-1"}, "cl-nope")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-l5/heartbeat", {"worker_id": "w-1"}, "hb-none")
        self.assertEqual(404, status)
        status, data = self.call("POST", "/executions/run-l5/release", {"worker_id": "w-1"}, "rl-none")
        self.assertEqual(404, status)
        status, data = self.call("POST", "/executions/run-l5/claim", {"lease_seconds": 30}, "cl-bad")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call(
            "POST", "/executions/run-l5/claim", raw=b'{"worker_id":"w-1","lease_seconds":1e400}', key="cl-nan"
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_lease_idempotency_key_scoping(self):
        self.start("run-l6")
        self.call("POST", "/executions/run-l6/claim", {"worker_id": "w-1"}, "shared-lease-key")
        status, data = self.call("POST", "/executions/run-l6/heartbeat", {"worker_id": "w-1"}, "shared-lease-key")
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])
        status, data = self.call("POST", "/executions/run-l6/advance", {"output": {}, "worker_id": "w-1"}, "shared-lease-key")
        self.assertEqual(409, status)


class ApprovalHttpTests(unittest.TestCase):
    @staticmethod
    def _request(port, method, path, body=None, raw=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-approvals.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls._request(
            cls.port,
            "POST",
            "/workflows",
            {
                "id": "wf-approval",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {
                        "id": "b",
                        "kind": "task",
                        "depends_on": ["a"],
                        "approval": {"approvers": ["alice", "bob"]},
                    },
                ],
            },
            key="wf-approval",
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, raw=None):
        return self._request(self.port, method, path, body=body, raw=raw, key=key)

    def park(self, execution_id, key_prefix):
        self.call("POST", f"/executions/{execution_id}/advance", {"output": {}}, f"{key_prefix}-a1")
        status, data = self.call("POST", f"/executions/{execution_id}/advance", {"output": {}}, f"{key_prefix}-a2")
        self.assertEqual(200, status, data)
        return json.loads(data)

    def test_approve_and_reject_over_http(self):
        self.call("POST", "/executions", {"id": "run-ok", "workflow_id": "wf-approval", "input": {}}, "ex-ok")
        parked = self.park("run-ok", "ok")
        self.assertEqual({"node_id": "b", "approvers": ["alice", "bob"]}, parked["waiting_approval"])
        status, data = self.call(
            "POST",
            "/executions/run-ok/decision",
            {"approver": "alice", "decision": "approved", "output": {"v": 1}},
            "dec-ok",
        )
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        state = json.loads(data)
        self.assertEqual(["a", "b"], state["completed_nodes"])

    def test_rejection_terminates(self):
        self.call("POST", "/executions", {"id": "run-no", "workflow_id": "wf-approval", "input": {}}, "ex-no")
        self.park("run-no", "no")
        status, data = self.call(
            "POST",
            "/executions/run-no/decision",
            {"approver": "bob", "decision": "rejected", "reason": "denied"},
            "dec-no",
        )
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("terminated", state["status"])
        self.assertEqual("rejected", state["termination_reason"])

    def test_non_approver_conflicts(self):
        self.call("POST", "/executions", {"id": "run-who", "workflow_id": "wf-approval", "input": {}}, "ex-who")
        self.park("run-who", "who")
        status, data = self.call(
            "POST",
            "/executions/run-who/decision",
            {"approver": "carol", "decision": "approved", "output": {}},
            "dec-who",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_decision_without_waiting_point_conflicts(self):
        self.call(
            "POST",
            "/workflows",
            {"id": "wf-plain-http", "nodes": [{"id": "x", "kind": "task", "depends_on": []}]},
            "wf-plain-http",
        )
        self.call(
            "POST",
            "/executions",
            {"id": "run-plain-http", "workflow_id": "wf-plain-http", "input": {}},
            "ex-plain-http",
        )
        status, data = self.call(
            "POST",
            "/executions/run-plain-http/decision",
            {"approver": "alice", "decision": "approved", "output": {}},
            "dec-plain-http",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_missing_execution_decision_is_not_found(self):
        status, data = self.call(
            "POST",
            "/executions/nope/decision",
            {"approver": "alice", "decision": "approved", "output": {}},
            "dec-nope",
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_invalid_decision_bodies_are_validation_errors(self):
        self.call("POST", "/executions", {"id": "run-bad-dec", "workflow_id": "wf-approval", "input": {}}, "ex-bad-dec")
        self.park("run-bad-dec", "baddec")
        for index, raw in enumerate(
            (
                b"{}",
                b'{"approver":"alice","decision":"maybe","output":{}}',
                b'{"approver":"alice","decision":"approved"}',
                b'{"approver":"alice","decision":"rejected"}',
                b'{"approver":7,"decision":"approved","output":{}}',
                b'{"approver":"alice","decision":"approved","output":[]}',
            )
        ):
            status, data = self.call("POST", "/executions/run-bad-dec/decision", raw=raw, key=f"dec-bad-{index}")
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_non_finite_decision_output_is_rejected(self):
        self.call("POST", "/executions", {"id": "run-nan-dec", "workflow_id": "wf-approval", "input": {}}, "ex-nan-dec")
        self.park("run-nan-dec", "nandec")
        status, data = self.call(
            "POST",
            "/executions/run-nan-dec/decision",
            raw=b'{"approver":"alice","decision":"approved","output":{"v":NaN}}',
            key="dec-nan",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_decision_key_reused_across_operations_conflicts(self):
        self.call("POST", "/executions", {"id": "run-key-dec", "workflow_id": "wf-approval", "input": {}}, "ex-key-dec")
        self.park("run-key-dec", "keydec")
        self.call(
            "POST",
            "/executions/run-key-dec/decision",
            {"approver": "alice", "decision": "approved", "output": {}},
            "shared-decision-key",
        )
        status, data = self.call(
            "POST",
            "/executions/run-key-dec/advance",
            {"output": {}},
            "shared-decision-key",
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_invalid_approval_definition_is_rejected(self):
        status, data = self.call(
            "POST",
            "/workflows",
            {"id": "wf-bad-approval", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": []}}]},
            key="wf-bad-approval",
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()