import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow


class CheckpointHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "checkpoint-http.db"))
        cls.service = Handler.service
        cls.service.create_workflow(
            {
                "id": "wf",
                "nodes": [
                    {"id": "a", "kind": "task", "depends_on": []},
                    {"id": "b", "kind": "task", "depends_on": ["a"]},
                ],
            },
            "wf-key",
        )
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

    def test_full_recover_cycle_over_http(self):
        self.request("POST", "/executions", {"id": "run-1", "workflow_id": "wf", "input": {}}, headers={"Idempotency-Key": "e1"})
        status, data = self.request(
            "POST", "/executions/run-1/advance", {"output": {"v": 1}}, headers={"Idempotency-Key": "a1"}
        )
        self.assertEqual(200, status)
        status, data = self.request("GET", "/executions/run-1/checkpoints")
        self.assertEqual(200, status)
        listing = json.loads(data)
        self.assertEqual(1, len(listing["checkpoints"]))
        self.assertEqual(["a"], listing["checkpoints"][0]["state"]["completed_nodes"])
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.request(
            "POST", "/executions/run-1/recover", {"from": "latest"}, headers={"Idempotency-Key": "r1"}
        )
        self.assertEqual(200, status)
        self.assertEqual(["a"], json.loads(data)["completed_nodes"])
        status, data = self.request(
            "POST", "/executions/run-1/advance", {"output": {"v": 2}}, headers={"Idempotency-Key": "a2"}
        )
        self.assertEqual(200, status)
        self.assertEqual("completed", json.loads(data)["status"])

    def test_recover_missing_execution_is_404(self):
        status, data = self.request(
            "POST", "/executions/nope/recover", {"from": "latest"}, headers={"Idempotency-Key": "r-nope"}
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_recover_without_checkpoint_is_409(self):
        self.request("POST", "/executions", {"id": "run-empty", "workflow_id": "wf", "input": {}}, headers={"Idempotency-Key": "e-empty"})
        status, data = self.request(
            "POST", "/executions/run-empty/recover", {"from": "latest"}, headers={"Idempotency-Key": "r-empty"}
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_recover_validation_errors_are_400(self):
        self.request("POST", "/executions", {"id": "run-bad", "workflow_id": "wf", "input": {}}, headers={"Idempotency-Key": "e-bad"})
        for body in (b"{}", b'{"from":"earliest"}', b'{"from":"latest","x":1}', b'{"from":1}'):
            with self.subTest(body=body):
                status, data = self.request(
                    "POST", "/executions/run-bad/recover", raw=body, headers={"Idempotency-Key": "rb-" + body.hex()[:8]}
                )
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_recover_non_finite_body_is_400(self):
        self.request("POST", "/executions", {"id": "run-nan", "workflow_id": "wf", "input": {}}, headers={"Idempotency-Key": "e-nan"})
        status, data = self.request(
            "POST",
            "/executions/run-nan/recover",
            raw=b'{"from":"latest","v":NaN}',
            headers={"Idempotency-Key": "r-nan"},
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_recover_key_reused_across_operations_is_409(self):
        self.request("POST", "/executions", {"id": "run-shared", "workflow_id": "wf", "input": {}}, headers={"Idempotency-Key": "e-shared"})
        self.request(
            "POST", "/executions/run-shared/advance", {"output": {"v": 1}}, headers={"Idempotency-Key": "shared-ck"}
        )
        status, data = self.request(
            "POST", "/executions/run-shared/recover", {"from": "latest"}, headers={"Idempotency-Key": "shared-ck"}
        )
        self.assertEqual(409, status)
        self.assertEqual("conflict", json.loads(data)["error"]["code"])

    def test_recover_requires_idempotency_key(self):
        status, data = self.request("POST", "/executions/run-1/recover", {"from": "latest"})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_checkpoints_missing_execution_is_404(self):
        status, data = self.request("GET", "/executions/ghost/checkpoints")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
