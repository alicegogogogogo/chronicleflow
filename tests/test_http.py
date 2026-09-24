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


if __name__ == "__main__":
    unittest.main()
