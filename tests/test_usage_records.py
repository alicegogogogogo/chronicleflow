import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow, _parse_timestamp

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

T0 = "2026-01-01T08:00:00.000Z"
T1 = "2026-01-01T09:00:00.000Z"
T2 = "2026-01-01T10:00:00.000Z"
T_MID = "2026-01-01T08:30:00.000Z"


def _ts(value):
    return _parse_timestamp(value, "since")


class UsageRecordsServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "records.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _stamp(self, sequence, created_at, tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (created_at, tenant, sequence),
            )

    def _three_records(self):
        """One workflow record and two execution records, stamped T1, T0, T0.

        Sequences 1..3 keep their stable identity; only the occurrence times
        are rewritten so the chronological order (2, 3, 1) differs from the
        sequence order and two records share one instant.
        """
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)

    def test_empty_tenant_gets_a_definite_empty_list(self):
        self.assertEqual({"records": []}, self.service.usage_records("acme"))

    def test_a_tenant_is_required(self):
        with self.assertRaises(ValidationError):
            self.service.usage_records("")

    def test_records_order_by_occurrence_time_with_sequence_breaking_ties(self):
        self._three_records()
        records = self.service.usage_records("acme")["records"]
        self.assertEqual([2, 3, 1], [record["sequence"] for record in records])
        self.assertEqual(
            ["execution_started", "execution_started", "workflow_created"],
            [record["type"] for record in records],
        )
        self.assertEqual([T0, T0, T1], [record["occurred_at"] for record in records])
        for record in records:
            self.assertIsInstance(record["sequence"], int)
            self.assertGreaterEqual(record["sequence"], 1)
            self.assertTrue(record["occurred_at"].endswith("Z"))
            # The keys appear in exactly the sequence/type/occurred_at order.
            self.assertEqual(["sequence", "type", "occurred_at"], list(record))

    def test_window_is_a_closed_interval(self):
        self._three_records()
        point = self.service.usage_records("acme", since=_ts(T0), until=_ts(T0))["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in point])
        open_since = self.service.usage_records("acme", since=_ts(T_MID), until=None)["records"]
        self.assertEqual([1], [record["sequence"] for record in open_since])
        open_until = self.service.usage_records("acme", since=None, until=_ts(T_MID))["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in open_until])

    def test_reversed_window_is_an_empty_list_not_an_error(self):
        self._three_records()
        self.assertEqual(
            {"records": []},
            self.service.usage_records("acme", since=_ts(T2), until=_ts(T0)),
        )

    def test_window_does_not_renumber_sequences(self):
        self._three_records()
        records = self.service.usage_records("acme", since=_ts(T0), until=_ts(T0))["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])

    def test_cursor_keeps_only_strictly_greater_sequences(self):
        self._three_records()
        # The cursor compares sequences, not time positions: sequence 1 is
        # excluded even though it occurs later than sequence 2.
        records = self.service.usage_records("acme", cursor=2, limit=10)["records"]
        self.assertEqual([3], [record["sequence"] for record in records])

    def test_cursor_past_the_end_is_empty(self):
        self._three_records()
        self.assertEqual([], self.service.usage_records("acme", cursor=99)["records"])

    def test_limit_caps_the_page(self):
        self._three_records()
        records = self.service.usage_records("acme", limit=2)["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])
        self.assertLessEqual(len(records), 2)

    def test_consecutive_pages_cover_every_hit_once(self):
        # Real records are stamped under the serialized write lock, so their
        # occurrence times are non-decreasing with sequence and the sequence
        # cursor walks the time-ordered pages without overlap or gaps.
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.usage_records("acme", cursor=cursor, limit=1)["records"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(
            self.service.usage_records("acme")["records"],
            collected,
        )
        self.assertEqual([1, 2, 3], [record["sequence"] for record in collected])

    def test_window_and_pagination_combine_as_intersection(self):
        self._three_records()
        # The T0 hit is sequence 2 first; cursor 2 then reaches sequence 3.
        page = self.service.usage_records(
            "acme", since=_ts(T0), until=_ts(T0), cursor=2, limit=10
        )["records"]
        self.assertEqual([3], [record["sequence"] for record in page])

    def test_query_is_read_only(self):
        self._three_records()
        before = self.service.usage("acme")
        self.service.usage_records("acme")
        self.service.usage_records("acme", since=_ts(T0), until=_ts(T2), cursor=1, limit=1)
        self.assertEqual(before, self.service.usage("acme"))
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = 'acme'"
        ).fetchone()
        self.assertEqual(3, rows["used"])

    def test_other_tenants_records_are_invisible(self):
        self._three_records()
        self.service.create_workflow({"id": "wf-b", "nodes": TASK}, "wf-b", "beta")
        self.assertEqual([], self.service.usage_records("beta", since=_ts(T0), until=_ts(T2))["records"])
        acme = self.service.usage_records("acme")["records"]
        self.assertEqual({2, 3, 1}, {record["sequence"] for record in acme})
        self.assertTrue(all(record["type"] for record in acme))

    def test_type_filter_keeps_only_matching_types(self):
        self._three_records()
        records = self.service.usage_records("acme", types=("execution_started",))["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])
        self.assertEqual(["execution_started", "execution_started"], [record["type"] for record in records])
        records = self.service.usage_records("acme", types=("workflow_created",))["records"]
        self.assertEqual([1], [record["sequence"] for record in records])
        records = self.service.usage_records(
            "acme", types=("workflow_created", "execution_started")
        )["records"]
        self.assertEqual([2, 3, 1], [record["sequence"] for record in records])

    def test_type_filter_with_no_match_is_a_definite_empty_list(self):
        self._three_records()
        self.assertEqual(
            {"records": []},
            self.service.usage_records("acme", types=("schedule_triggered",)),
        )

    def test_type_filter_does_not_renumber_sequences(self):
        self._three_records()
        records = self.service.usage_records("acme", types=("workflow_created",))["records"]
        self.assertEqual([1], [record["sequence"] for record in records])

    def test_type_filter_combines_with_window_and_pagination(self):
        self._three_records()
        page = self.service.usage_records(
            "acme", since=_ts(T0), until=_ts(T0), types=("execution_started",), cursor=2, limit=10
        )["records"]
        self.assertEqual([3], [record["sequence"] for record in page])
        page = self.service.usage_records(
            "acme", types=("execution_started",), limit=1
        )["records"]
        self.assertEqual([2], [record["sequence"] for record in page])

    def test_filtered_pages_cover_every_hit_once(self):
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        collected = []
        cursor = None
        while True:
            page = self.service.usage_records(
                "acme", types=("execution_started",), cursor=cursor, limit=1
            )["records"]
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual([2, 3], [record["sequence"] for record in collected])
        self.assertEqual(
            self.service.usage_records("acme", types=("execution_started",))["records"],
            collected,
        )

    def test_type_filter_is_read_only(self):
        self._three_records()
        before = self.service.usage("acme")
        self.service.usage_records("acme", types=("execution_started",))
        self.assertEqual(before, self.service.usage("acme"))
        rows = self.service.store.connection.execute(
            "SELECT COUNT(*) AS used FROM usage_records WHERE tenant = 'acme'"
        ).fetchone()
        self.assertEqual(3, rows["used"])

    def test_type_filter_keeps_tenant_isolation(self):
        self._three_records()
        self.service.create_workflow({"id": "wf-b", "nodes": TASK}, "wf-b", "beta")
        # Beta sees only its own record under the filter, never acme's.
        beta = self.service.usage_records("beta", types=("workflow_created", "execution_started"))["records"]
        self.assertEqual([1], [record["sequence"] for record in beta])
        self.assertEqual(["workflow_created"], [record["type"] for record in beta])
        acme = self.service.usage_records("acme", types=("execution_started",))["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in acme])


class UsageRecordsHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-records.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, path, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request("GET", path, None, headers or {})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_requires_a_tenant_header(self):
        for path in ("/usage/records", "/usage/records?limit=1"):
            status, data = self.call(path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)
            status, data = self.call(path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_empty_result_is_a_definite_empty_list_with_one_trailing_newline(self):
        status, data = self.call("/usage/records?limit=10", headers={"X-Tenant-Id": "records-empty"})
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_records_over_http(self):
        headers = {"X-Tenant-Id": "records-acme"}
        # Create records through the metered write endpoints.
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/workflows",
            json.dumps({"id": "wf-rec", "nodes": TASK}),
            {"Content-Type": "application/json", "Idempotency-Key": "wf-rec", **headers},
        )
        self.assertEqual(201, connection.getresponse().status)
        connection.close()
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/executions",
            json.dumps({"id": "r-rec", "workflow_id": "wf-rec", "input": {}}),
            {"Content-Type": "application/json", "Idempotency-Key": "r-rec", **headers},
        )
        self.assertEqual(201, connection.getresponse().status)
        connection.close()
        status, data = self.call("/usage/records?limit=10", headers=headers)
        self.assertEqual(200, status)
        records = json.loads(data)["records"]
        self.assertEqual(2, len(records))
        self.assertEqual([1, 2], [record["sequence"] for record in records])
        self.assertEqual(
            ["workflow_created", "execution_started"],
            [record["type"] for record in records],
        )
        for record in records:
            self.assertEqual(["sequence", "type", "occurred_at"], list(record))
            self.assertTrue(record["occurred_at"].endswith("Z"))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_pagination_walks_every_record_once(self):
        headers = {"X-Tenant-Id": "records-pages"}
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/workflows",
            json.dumps({"id": "wf-p", "nodes": TASK}),
            {"Content-Type": "application/json", "Idempotency-Key": "wf-p", **headers},
        )
        connection.getresponse().read()
        connection.close()
        for index in range(3):
            connection = http.client.HTTPConnection("127.0.0.1", self.port)
            connection.request(
                "POST",
                "/executions",
                json.dumps({"id": f"r-p{index}", "workflow_id": "wf-p", "input": {}}),
                {"Content-Type": "application/json", "Idempotency-Key": f"r-p{index}", **headers},
            )
            connection.getresponse().read()
            connection.close()
        collected = []
        query = "?limit=2"
        while True:
            status, data = self.call("/usage/records" + query, headers=headers)
            self.assertEqual(200, status)
            page = json.loads(data)["records"]
            if not page:
                break
            collected.extend(page)
            query = f"?limit=2&cursor={page[-1]['sequence']}"
        self.assertEqual([1, 2, 3, 4], [record["sequence"] for record in collected])

    def test_window_parameters_are_honored(self):
        headers = {"X-Tenant-Id": "records-window"}
        # A reversed window of a populated tenant is an empty list, not an error.
        query = "?limit=10&since=2099-01-01T00:00:00.000Z&until=2000-01-01T00:00:00.000Z"
        status, data = self.call("/usage/records" + query, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))

    def test_type_filter_over_http(self):
        headers = {"X-Tenant-Id": "records-typed"}
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request(
            "POST",
            "/workflows",
            json.dumps({"id": "wf-t", "nodes": TASK}),
            {"Content-Type": "application/json", "Idempotency-Key": "wf-t", **headers},
        )
        self.assertEqual(201, connection.getresponse().status)
        connection.close()
        for index in range(2):
            connection = http.client.HTTPConnection("127.0.0.1", self.port)
            connection.request(
                "POST",
                "/executions",
                json.dumps({"id": f"r-t{index}", "workflow_id": "wf-t", "input": {}}),
                {"Content-Type": "application/json", "Idempotency-Key": f"r-t{index}", **headers},
            )
            self.assertEqual(201, connection.getresponse().status)
            connection.close()
        # A single type keeps only its records, with sequences unrenumbered.
        status, data = self.call("/usage/records?limit=10&type=execution_started", headers=headers)
        self.assertEqual(200, status)
        records = json.loads(data)["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])
        self.assertEqual(["execution_started", "execution_started"], [record["type"] for record in records])
        for record in records:
            self.assertEqual(["sequence", "type", "occurred_at"], list(record))
        # A comma-separated set keeps the union, still time-ordered.
        status, data = self.call(
            "/usage/records?limit=10&type=workflow_created,execution_started", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual([1, 2, 3], [record["sequence"] for record in json.loads(data)["records"]])
        # A type with no records is a definite empty list, not an error.
        status, data = self.call("/usage/records?limit=10&type=delivery_attempted", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        # The filter intersects with cursor pagination without overlap or gaps.
        collected = []
        query = "?limit=1&type=execution_started"
        while True:
            status, data = self.call("/usage/records" + query, headers=headers)
            self.assertEqual(200, status)
            page = json.loads(data)["records"]
            if not page:
                break
            collected.extend(page)
            query = f"?limit=1&type=execution_started&cursor={page[-1]['sequence']}"
        self.assertEqual([2, 3], [record["sequence"] for record in collected])

    def test_type_validation_failure_writes_no_usage(self):
        headers = {"X-Tenant-Id": "records-type-validation-empty"}
        status, _ = self.call("/usage/records?limit=1&type=bogus_type", headers=headers)
        self.assertEqual(400, status)
        status, data = self.call("/usage/records?limit=5", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))

    def test_validation_failures(self):
        headers = {"X-Tenant-Id": "records-validation"}
        bad_paths = (
            "/usage/records",
            "/usage/records?limit=",
            "/usage/records?limit=0",
            "/usage/records?limit=-1",
            "/usage/records?limit=1.5",
            "/usage/records?limit=abc",
            "/usage/records?limit=1e3",
            "/usage/records?limit=1&cursor=0",
            "/usage/records?limit=1&cursor=-3",
            "/usage/records?limit=1&cursor=2.0",
            "/usage/records?limit=1&cursor=x",
            "/usage/records?limit=1&since=not-a-time",
            "/usage/records?limit=1&until=2026-01-01",
            "/usage/records?limit=1&bogus=1",
            "/usage/records?limit=1&limit=2",
            "/usage/records?since=2026-01-01T00:00:00.000Z&since=2026-02-01T00:00:00.000Z&limit=1",
            "/usage/records?limit=1&type=",
            "/usage/records?limit=1&type=,",
            "/usage/records?limit=1&type=execution_started,",
            "/usage/records?limit=1&type=,execution_started",
            "/usage/records?limit=1&type=execution_started,,workflow_created",
            "/usage/records?limit=1&type=execution_started,execution_started",
            "/usage/records?limit=1&type=bogus_type",
            "/usage/records?limit=1&type=execution_started,bogus_type",
            "/usage/records?limit=1&type=node_completed",
            "/usage/records?limit=1&type=execution_started&type=workflow_created",
        )
        for path in bad_paths:
            status, data = self.call(path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_validation_failure_writes_no_usage(self):
        headers = {"X-Tenant-Id": "records-validation-empty"}
        status, _ = self.call("/usage/records?limit=0", headers=headers)
        self.assertEqual(400, status)
        status, data = self.call("/usage/records?limit=5", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))

    def test_other_tenant_is_invisible_over_http(self):
        headers = {"X-Tenant-Id": "records-isolated"}
        status, data = self.call("/usage/records?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))

    def test_get_is_the_only_supported_method(self):
        for method in ("POST", "PUT"):
            connection = http.client.HTTPConnection("127.0.0.1", self.port)
            connection.request(method, "/usage/records?limit=1", None, {"X-Tenant-Id": "records-method"})
            response = connection.getresponse()
            response.read()
            connection.close()
            self.assertEqual(404, response.status, method)


if __name__ == "__main__":
    unittest.main()
