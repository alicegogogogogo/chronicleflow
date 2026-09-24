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


class RetryAndCancellationHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-retry.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = json.dumps(body) if body is not None else None
        all_headers = {"Content-Type": "application/json"}
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def setUp(self):
        self.request(
            "POST",
            "/workflows",
            {
                "id": "wf-retry",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": [], "retries": 1},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            headers={"Idempotency-Key": "wf-retry"},
        )

    def test_invalid_retries_returns_400(self):
        status, data = self.request(
            "POST",
            "/workflows",
            {"id": "wf-bad-retries", "nodes": [{"id": "a", "kind": "task", "depends_on": [], "retries": 11}]},
            headers={"Idempotency-Key": "wf-bad-retries"},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_invalid_timeout_returns_400(self):
        status, data = self.request(
            "POST",
            "/executions",
            {"id": "run-bad-timeout", "workflow_id": "wf-retry", "input": {}, "timeout_seconds": -3},
            headers={"Idempotency-Key": "run-bad-timeout"},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_failure_retries_then_terminates(self):
        self.request(
            "POST", "/executions", {"id": "run-r", "workflow_id": "wf-retry", "input": {}},
            headers={"Idempotency-Key": "run-r"},
        )
        status, data = self.request(
            "POST", "/executions/run-r/advance", {"failure": {"reason": "boom"}},
            headers={"Idempotency-Key": "run-r-a1"},
        )
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("running", state["status"])
        self.assertEqual(2, state["nodes"]["a"]["attempt"])
        status, data = self.request(
            "POST", "/executions/run-r/advance", {"failure": {"reason": "boom again"}},
            headers={"Idempotency-Key": "run-r-a2"},
        )
        self.assertEqual(200, status)
        state = json.loads(data)
        self.assertEqual("failed", state["status"])
        self.assertEqual("failed", state["termination_reason"])
        status, data = self.request("GET", "/executions/run-r/events")
        self.assertEqual(200, status)
        events = json.loads(data)["events"]
        self.assertEqual("execution_terminated", events[-1]["type"])

    def test_cancel_running_then_advance_is_idempotent(self):
        self.request(
            "POST", "/executions", {"id": "run-c", "workflow_id": "wf-retry", "input": {}},
            headers={"Idempotency-Key": "run-c"},
        )
        status, data = self.request("POST", "/executions/run-c/cancel", {}, headers={"Idempotency-Key": "run-c-c1"})
        self.assertEqual(200, status)
        cancelled = json.loads(data)
        self.assertEqual("cancelled", cancelled["status"])
        self.assertEqual("cancelled", cancelled["termination_reason"])
        # advancing the cancelled execution returns it unchanged
        status, data = self.request(
            "POST", "/executions/run-c/advance", {"output": {"ignored": True}},
            headers={"Idempotency-Key": "run-c-a1"},
        )
        self.assertEqual(cancelled, json.loads(data))
        # cancelling again returns the same state
        status, data = self.request("POST", "/executions/run-c/cancel", {}, headers={"Idempotency-Key": "run-c-c2"})
        self.assertEqual(cancelled, json.loads(data))

    def test_cancel_without_body_is_allowed(self):
        self.request(
            "POST", "/executions", {"id": "run-cancel-nobody", "workflow_id": "wf-retry", "input": {}},
            headers={"Idempotency-Key": "run-cancel-nobody-create"},
        )
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST", "/executions/run-cancel-nobody/cancel", None,
            {"Idempotency-Key": "run-cancel-nobody-cancel", "Content-Type": "application/json"},
        )
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(200, response.status)
        self.assertEqual("cancelled", json.loads(data)["status"])

    def test_cancel_missing_execution_returns_404(self):
        status, data = self.request(
            "POST", "/executions/no-such-execution/cancel", {},
            headers={"Idempotency-Key": "cancel-missing"},
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
