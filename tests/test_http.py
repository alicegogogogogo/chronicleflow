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


if __name__ == "__main__":
    unittest.main()
