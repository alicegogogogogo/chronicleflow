import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]
TWO_TASKS = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]
APPROVAL_TASK = [
    {"id": "a", "kind": "task", "depends_on": [], "approval": {"approvers": ["alice"]}},
]


class WorkflowListServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "lists.db"))

    def tearDown(self):
        self.directory.cleanup()

    def test_empty_namespace_is_a_definite_empty_list(self):
        self.assertEqual({"workflows": []}, self.service.list_workflows(limit=100))

    def test_identifiers_are_returned_in_ascending_order(self):
        for index, workflow_id in enumerate(("zeta", "alpha", "mu")):
            self.service.create_workflow({"id": workflow_id, "nodes": TASK}, f"wf-{workflow_id}")
        result = self.service.list_workflows(limit=100)
        self.assertEqual(["alpha", "mu", "zeta"], [entry["id"] for entry in result["workflows"]])

    def test_pages_neither_overlap_nor_skip(self):
        for index in range(5):
            workflow_id = f"wf-{index}"
            self.service.create_workflow({"id": workflow_id, "nodes": TASK}, f"key-{index}")
        collected = []
        cursor = None
        while True:
            page = self.service.list_workflows(cursor=cursor, limit=2)["workflows"]
            if not page:
                break
            collected.extend(entry["id"] for entry in page)
            cursor = page[-1]["id"]
        self.assertEqual(
            [entry["id"] for entry in self.service.list_workflows(limit=100)["workflows"]],
            collected,
        )

    def test_cursor_past_the_end_returns_empty(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf")
        self.assertEqual([], self.service.list_workflows(cursor="zzz", limit=10)["workflows"])

    def test_list_is_scoped_to_the_tenant(self):
        self.service.create_workflow({"id": "shared", "nodes": TASK}, "wf-a", "alpha")
        self.service.create_workflow({"id": "shared", "nodes": TASK}, "wf-b", "beta")
        self.service.create_workflow({"id": "only-alpha", "nodes": TASK}, "wf-c", "alpha")
        alpha = self.service.list_workflows(tenant="alpha", limit=100)["workflows"]
        beta = self.service.list_workflows(tenant="beta", limit=100)["workflows"]
        self.assertEqual(["only-alpha", "shared"], [entry["id"] for entry in alpha])
        self.assertEqual(["shared"], [entry["id"] for entry in beta])

    def test_legacy_namespace_is_the_empty_tenant(self):
        self.service.create_workflow({"id": "legacy", "nodes": TASK}, "wf-legacy")
        self.service.create_workflow({"id": "tenant", "nodes": TASK}, "wf-tenant", "acme")
        self.assertEqual(["legacy"], [e["id"] for e in self.service.list_workflows(limit=100)["workflows"]])
        self.assertEqual(
            ["tenant"],
            [e["id"] for e in self.service.list_workflows(tenant="acme", limit=100)["workflows"]],
        )


class ExecutionListServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "exec-lists.db"))
        self.service.create_workflow({"id": "wf1", "nodes": TWO_TASKS}, "wf1")
        self.service.create_workflow({"id": "wf2", "nodes": TASK}, "wf2")
        self.service.create_workflow({"id": "approvals", "nodes": APPROVAL_TASK}, "wf3")

    def tearDown(self):
        self.directory.cleanup()

    def start(self, execution_id, workflow_id="wf1", key=None, tenant=""):
        self.service.create_execution(
            {"id": execution_id, "workflow_id": workflow_id, "input": {}},
            key or f"start-{execution_id}",
            tenant,
        )

    def complete(self, execution_id):
        self.service.advance(execution_id, {"output": {}}, f"a1-{execution_id}")
        self.service.advance(execution_id, {"output": {}}, f"a2-{execution_id}")

    def exhaust(self, execution_id):
        # wf2's single task has no retries: one failure terminates it.
        self.service.advance(execution_id, {"failure": {"reason": "boom"}}, f"f-{execution_id}")

    def reject(self, execution_id):
        self.service.advance(execution_id, {"output": {}}, f"park-{execution_id}")
        self.service.decision(
            execution_id,
            {"approver": "alice", "decision": "rejected", "reason": "nope"},
            f"dec-{execution_id}",
        )

    def cancel(self, execution_id):
        self.service.cancel(execution_id, f"cancel-{execution_id}")

    def test_empty_namespace_is_a_definite_empty_list(self):
        result = self.service.list_executions(limit=100)
        self.assertEqual({"executions": []}, result)

    def test_entries_carry_identifiers_status_reason_and_created_at_in_key_order(self):
        self.start("run-1")
        self.start("run-2", workflow_id="wf2")
        self.exhaust("run-2")
        result = {e["id"]: e for e in self.service.list_executions(limit=100)["executions"]}
        self.assertEqual(
            ["id", "workflow_id", "status", "termination_reason", "created_at"], list(result["run-1"])
        )
        self.assertEqual(list(result["run-1"]), list(result["run-2"]))
        self.assertEqual("running", result["run-1"]["status"])
        self.assertIsNone(result["run-1"]["termination_reason"])
        self.assertEqual("wf2", result["run-2"]["workflow_id"])
        self.assertEqual("terminated", result["run-2"]["status"])
        self.assertEqual("retries_exhausted", result["run-2"]["termination_reason"])
        # The creation time is the occurrence time of the start event.
        for execution_id in ("run-1", "run-2"):
            created_at = result[execution_id]["created_at"]
            self.assertIsInstance(created_at, str)
            self.assertTrue(created_at.endswith("Z"))
            started = next(
                event
                for event in self.service.events(execution_id)
                if event["type"] == "execution_started"
            )
            self.assertEqual(started["occurred_at"], created_at)

    def test_ascending_identifier_order_and_keyset_paging(self):
        for execution_id in ("run-c", "run-a", "run-b"):
            self.start(execution_id)
        collected = []
        cursor = None
        while True:
            page = self.service.list_executions(cursor=cursor, limit=2)["executions"]
            if not page:
                break
            self.assertLessEqual(len(page), 2)
            collected.extend(e["id"] for e in page)
            cursor = page[-1]["id"]
        self.assertEqual(["run-a", "run-b", "run-c"], collected)

    def test_filter_by_workflow_id(self):
        self.start("run-1", workflow_id="wf1")
        self.start("run-2", workflow_id="wf2")
        result = self.service.list_executions(workflow_id="wf2", limit=100)["executions"]
        self.assertEqual(["run-2"], [e["id"] for e in result])

    def test_filter_by_status(self):
        self.start("run-running")
        self.start("run-completed")
        self.complete("run-completed")
        self.start("run-failed", workflow_id="wf2")
        self.exhaust("run-failed")
        for status, expected in (
            ("running", ["run-running"]),
            ("completed", ["run-completed"]),
            ("terminated", ["run-failed"]),
        ):
            with self.subTest(status=status):
                ids = [e["id"] for e in self.service.list_executions(status=status, limit=100)["executions"]]
                self.assertEqual(expected, ids)

    def test_filter_by_termination_reason_excludes_running_and_completed(self):
        self.start("run-running")
        self.start("run-completed")
        self.complete("run-completed")
        self.start("run-exhausted", workflow_id="wf2")
        self.exhaust("run-exhausted")
        self.start("run-cancelled")
        self.cancel("run-cancelled")
        self.start("run-rejected", workflow_id="approvals")
        self.reject("run-rejected")
        by_reason = {
            reason: [e["id"] for e in self.service.list_executions(termination_reason=reason, limit=100)["executions"]]
            for reason in ("cancelled", "rejected", "retries_exhausted", "timeout")
        }
        self.assertEqual(["run-cancelled"], by_reason["cancelled"])
        self.assertEqual(["run-rejected"], by_reason["rejected"])
        self.assertEqual(["run-exhausted"], by_reason["retries_exhausted"])
        self.assertEqual([], by_reason["timeout"])

    def test_filters_combine_as_an_intersection(self):
        self.start("run-cancelled-wf1")
        self.cancel("run-cancelled-wf1")
        self.start("run-cancelled-wf2", workflow_id="wf2")
        self.cancel("run-cancelled-wf2")
        result = self.service.list_executions(
            workflow_id="wf1", status="terminated", termination_reason="cancelled", limit=100
        )["executions"]
        self.assertEqual(["run-cancelled-wf1"], [e["id"] for e in result])
        # status=running together with a termination reason matches nothing.
        result = self.service.list_executions(
            workflow_id="wf1", status="running", termination_reason="cancelled", limit=100
        )["executions"]
        self.assertEqual([], result)

    def _created_at(self, execution_id):
        from chronicleflow.service import _parse_stored_time

        entry = next(
            e
            for e in self.service.list_executions(limit=100)["executions"]
            if e["id"] == execution_id
        )
        return entry["created_at"], _parse_stored_time(entry["created_at"])

    def test_time_window_is_closed_at_both_endpoints(self):
        import time as time_module

        self.start("run-a")
        time_module.sleep(0.002)
        self.start("run-b")
        time_module.sleep(0.002)
        self.start("run-c")
        first_text, first = self._created_at("run-a")
        last_text, last = self._created_at("run-c")
        ids = [
            e["id"]
            for e in self.service.list_executions(since=first, until=last, limit=100)["executions"]
        ]
        self.assertEqual(["run-a", "run-b", "run-c"], ids)
        # The boundary values come straight from the entries, so an execution
        # created exactly at since or until is matched; the response echoes
        # the identical timestamp string.
        entries = {
            e["id"]: e
            for e in self.service.list_executions(since=first, until=first, limit=100)["executions"]
        }
        self.assertEqual(["run-a"], list(entries))
        self.assertEqual(first_text, entries["run-a"]["created_at"])

    def test_open_bounds_filter_only_one_side(self):
        import time as time_module

        self.start("run-a")
        time_module.sleep(0.002)
        self.start("run-b")
        middle_text, middle = self._created_at("run-b")
        time_module.sleep(0.002)
        self.start("run-c")
        since_ids = [
            e["id"]
            for e in self.service.list_executions(since=middle, limit=100)["executions"]
        ]
        self.assertEqual(["run-b", "run-c"], since_ids)
        until_ids = [
            e["id"]
            for e in self.service.list_executions(until=middle, limit=100)["executions"]
        ]
        self.assertEqual(["run-a", "run-b"], until_ids)
        # No window means every execution, including one later observed.
        self.assertEqual(
            ["run-a", "run-b", "run-c"],
            [e["id"] for e in self.service.list_executions(limit=100)["executions"]],
        )
        self.assertTrue(middle_text.endswith("Z"))

    def test_reversed_window_returns_definite_empty_list(self):
        self.start("run-a")
        self.start("run-b")
        _, first = self._created_at("run-a")
        _, last = self._created_at("run-b")
        result = self.service.list_executions(since=last, until=first, limit=100)
        self.assertEqual({"executions": []}, result)
        # A reversed window stays empty even when other filters would match,
        # and the value is a stable empty list rather than an error.
        self.assertEqual(
            [],
            self.service.list_executions(
                workflow_id="wf1", status="running", since=last, until=first, limit=100
            )["executions"],
        )

    def test_time_window_paging_neither_overlaps_nor_skips(self):
        import time as time_module

        for execution_id in ("run-a", "run-b", "run-c", "run-d"):
            self.start(execution_id)
            time_module.sleep(0.001)
        entries = self.service.list_executions(limit=100)["executions"]
        since = self._created_at("run-a")[1]
        until = self._created_at("run-d")[1]
        collected = []
        cursor = None
        while True:
            page = self.service.list_executions(
                since=since, until=until, cursor=cursor, limit=2
            )["executions"]
            if not page:
                break
            collected.extend(e["id"] for e in page)
            cursor = page[-1]["id"]
        self.assertEqual([e["id"] for e in entries], collected)

    def test_time_window_combines_with_status_filter(self):
        import time as time_module

        self.start("run-running")
        time_module.sleep(0.002)
        self.start("run-completed")
        self.complete("run-completed")
        time_module.sleep(0.002)
        self.start("run-running-2")
        first = self._created_at("run-running")[1]
        until_completed = self._created_at("run-completed")[1]
        ids = [
            e["id"]
            for e in self.service.list_executions(
                status="running", since=first, until=until_completed, limit=100
            )["executions"]
        ]
        self.assertEqual(["run-running"], ids)

    def test_window_query_is_read_only(self):
        self.start("run-1")
        _, created = self._created_at("run-1")
        before = self.service.events("run-1")
        for _ in range(3):
            self.service.list_executions(since=created, until=created, limit=100)
        self.assertEqual(before, self.service.events("run-1"))
        entry = self.service.list_executions(since=created, until=created, limit=100)["executions"][0]
        self.assertEqual("running", entry["status"])

    def test_filter_by_missing_workflow_is_not_found(self):
        from chronicleflow.errors import NotFoundError

        with self.assertRaises(NotFoundError):
            self.service.list_executions(workflow_id="ghost", limit=100)

    def test_filter_by_another_tenants_workflow_is_not_found(self):
        from chronicleflow.errors import NotFoundError

        self.service.create_workflow({"id": "owned", "nodes": TASK}, "owned", "alpha")
        with self.assertRaises(NotFoundError):
            self.service.list_executions(workflow_id="owned", tenant="beta", limit=100)

    def test_tenant_isolation(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wfa", "alpha")
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wfb", "beta")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "exa", "alpha")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "exb", "beta")
        self.assertEqual(
            ["run"], [e["id"] for e in self.service.list_executions(tenant="alpha", limit=100)["executions"]]
        )
        self.assertEqual(
            ["run"], [e["id"] for e in self.service.list_executions(tenant="beta", limit=100)["executions"]]
        )

    def test_query_is_read_only(self):
        self.start("run-1")
        before = self.service.events("run-1")
        for _ in range(3):
            self.service.list_executions(limit=100)
            self.service.list_workflows(limit=100)
        self.assertEqual(before, self.service.events("run-1"))
        # A due timeout is not settled by the list query.
        self.service.create_execution(
            {"id": "run-timeout", "workflow_id": "wf1", "input": {}, "timeout_seconds": 0.01},
            "start-timeout",
        )
        import time

        time.sleep(0.05)
        listed = {e["id"]: e for e in self.service.list_executions(limit=100)["executions"]}
        self.assertEqual("running", listed["run-timeout"]["status"])


class ListHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-lists.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def request(self, path, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_workflow_list_empty_result_and_newline(self):
        status, data = self.request("/workflows?limit=10", {"X-Tenant-Id": f"empty-{id(self)}"})
        self.assertEqual(200, status)
        self.assertEqual({"workflows": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_execution_list_empty_result_and_newline(self):
        status, data = self.request("/executions?limit=10", {"X-Tenant-Id": f"empty-ex-{id(self)}"})
        self.assertEqual(200, status)
        self.assertEqual({"executions": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_missing_limit_is_rejected(self):
        for path in ("/workflows", "/executions"):
            with self.subTest(path=path):
                status, data = self.request(path)
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_non_positive_integer_limit_is_rejected(self):
        for value in ("0", "-1", "1.5", "abc", "1e3", ""):
            with self.subTest(value=value):
                status, _ = self.request(f"/workflows?limit={value}")
                self.assertEqual(400, status)

    def test_unknown_parameter_is_rejected(self):
        for path in ("/workflows?limit=1&bogus=1", "/executions?limit=1&bogus=1"):
            with self.subTest(path=path):
                status, data = self.request(path)
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_repeated_parameter_is_rejected(self):
        for path in ("/workflows?limit=1&limit=2", "/executions?limit=1&cursor=a&cursor=b",
                     "/executions?status=running&status=completed"):
            with self.subTest(path=path):
                status, _ = self.request(path)
                self.assertEqual(400, status)

    def test_illegal_status_and_reason_are_rejected(self):
        for query in ("status=bogus", "termination_reason=bogus", "status=terminated&termination_reason=wat"):
            with self.subTest(query=query):
                status, data = self.request(f"/executions?limit=10&{query}")
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_malformed_window_is_rejected(self):
        for query in ("since=bogus", "until=2026-09-26%2008:00:00", "since=2026-09-26T08:00:00%2B00:00"):
            with self.subTest(query=query):
                status, data = self.request(f"/executions?limit=10&{query}")
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_repeated_window_parameter_is_rejected(self):
        stamp = "2026-09-26T08:00:00.000Z"
        status, _ = self.request(f"/executions?limit=10&since={stamp}&since={stamp}")
        self.assertEqual(400, status)
        status, _ = self.request(f"/executions?limit=10&until={stamp}&until={stamp}")
        self.assertEqual(400, status)

    def test_workflow_filter_parameter_is_rejected_on_workflow_list(self):
        status, _ = self.request("/workflows?limit=10&status=running")
        self.assertEqual(400, status)

    def test_filter_by_missing_workflow_is_not_found(self):
        status, data = self.request("/executions?limit=10&workflow_id=missing")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_filter_by_other_tenants_workflow_is_not_found(self):
        tenant = f"owner-{id(self)}"
        status_create, _ = self.request_workflow(tenant)
        self.assertEqual(201, status_create)
        status, data = self.request(
            f"/executions?limit=10&workflow_id=listed-wf", {"X-Tenant-Id": f"other-{id(self)}"}
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def request_workflow(self, tenant):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/workflows",
            json.dumps({"id": "listed-wf", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]}),
            {"Content-Type": "application/json", "Idempotency-Key": f"listed-wf-{tenant}", "X-Tenant-Id": tenant},
        )
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_empty_tenant_header_is_rejected(self):
        status, data = self.request("/workflows?limit=10", {"X-Tenant-Id": ""})
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_cursor_pagination_over_http(self):
        tenant = f"page-{id(self)}"
        self.create_workflow_http(tenant, "page-wf")
        for index in range(5):
            self.create_execution_http(tenant, f"run-{index}")
        collected = []
        cursor = None
        while True:
            suffix = f"&cursor={cursor}" if cursor else ""
            status, data = self.request(f"/executions?limit=2&workflow_id=page-wf{suffix}", {"X-Tenant-Id": tenant})
            self.assertEqual(200, status)
            page = json.loads(data)["executions"]
            if not page:
                break
            collected.extend(e["id"] for e in page)
            cursor = page[-1]["id"]
        self.assertEqual([f"run-{index}" for index in range(5)], collected)

    def test_time_window_over_http(self):
        import time as time_module

        tenant = f"window-{id(self)}"
        self.create_workflow_http(tenant, "page-wf")
        self.create_execution_http(tenant, "run-a")
        time_module.sleep(0.002)
        self.create_execution_http(tenant, "run-b")
        status, data = self.request("/executions?limit=10", {"X-Tenant-Id": tenant})
        self.assertEqual(200, status)
        entries = json.loads(data)["executions"]
        self.assertEqual(["run-a", "run-b"], [e["id"] for e in entries])
        for entry in entries:
            self.assertEqual(
                ["id", "workflow_id", "status", "termination_reason", "created_at"], list(entry)
            )
            self.assertTrue(entry["created_at"].endswith("Z"))
        first, last = entries[0]["created_at"], entries[-1]["created_at"]
        # Closed interval between the two creation times returns both.
        status, data = self.request(
            f"/executions?limit=10&since={first}&until={last}", {"X-Tenant-Id": tenant}
        )
        self.assertEqual(200, status)
        self.assertEqual(["run-a", "run-b"], [e["id"] for e in json.loads(data)["executions"]])
        # A boundary equal to one creation time matches just that one.
        status, data = self.request(
            f"/executions?limit=10&until={first}", {"X-Tenant-Id": tenant}
        )
        self.assertEqual(["run-a"], [e["id"] for e in json.loads(data)["executions"]])
        # Reversed window is a definite empty list, not an error.
        status, data = self.request(
            f"/executions?limit=10&since={last}&until={first}", {"X-Tenant-Id": tenant}
        )
        self.assertEqual(200, status)
        self.assertEqual({"executions": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def create_workflow_http(self, tenant, workflow_id):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/workflows",
            json.dumps({"id": workflow_id, "nodes": TASK}),
            {"Content-Type": "application/json", "Idempotency-Key": workflow_id, "X-Tenant-Id": tenant},
        )
        response = connection.getresponse()
        response.read()
        connection.close()

    def create_execution_http(self, tenant, execution_id):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/executions",
            json.dumps({"id": execution_id, "workflow_id": "page-wf", "input": {}}),
            {"Content-Type": "application/json", "Idempotency-Key": execution_id, "X-Tenant-Id": tenant},
        )
        response = connection.getresponse()
        response.read()
        connection.close()


if __name__ == "__main__":
    unittest.main()
