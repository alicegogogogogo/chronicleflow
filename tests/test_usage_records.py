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
        self.service = ChronicleFlow(str(Path(self.directory.name) / "usage-records.db"))

    def tearDown(self):
        self.directory.cleanup()

    def _stamp(self, sequence, created_at, tenant="acme"):
        with self.service.store.transaction() as connection:
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (created_at, tenant, sequence),
            )

    def _metered_records(self):
        """Create one workflow record and two execution records at known times.

        Record sequence 1 is the workflow creation; sequences 2 and 3 are the
        two execution starts. Their occurrence times are stamped explicitly so
        the window behavior is deterministic.
        """
        self.service.create_workflow({"id": "wf", "nodes": TASK}, "wf", "acme")
        self.service.create_execution({"id": "r1", "workflow_id": "wf", "input": {}}, "r1", "acme")
        self.service.create_execution({"id": "r2", "workflow_id": "wf", "input": {}}, "r2", "acme")
        self._stamp(1, T0)
        self._stamp(2, T_MID)
        self._stamp(3, T1)

    def test_empty_records_are_a_definite_empty_list(self):
        self.assertEqual({"records": []}, self.service.usage_records("acme"))

    def test_records_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage_records("")

    def test_records_carry_sequence_type_and_time_in_key_order(self):
        self._metered_records()
        records = self.service.usage_records("acme")["records"]
        self.assertEqual(
            [
                {"sequence": 1, "type": "workflow_created", "occurred_at": T0},
                {"sequence": 2, "type": "execution_started", "occurred_at": T_MID},
                {"sequence": 3, "type": "execution_started", "occurred_at": T1},
            ],
            records,
        )
        for record in records:
            self.assertEqual(["sequence", "type", "occurred_at"], list(record))

    def test_records_are_ordered_by_time_then_sequence(self):
        self._metered_records()
        # Re-stamp so sequence and time order disagree: time wins, and records
        # sharing a time order by ascending sequence.
        self._stamp(1, T1)
        self._stamp(2, T0)
        self._stamp(3, T0)
        records = self.service.usage_records("acme")["records"]
        self.assertEqual([2, 3, 1], [record["sequence"] for record in records])
        times = [record["occurred_at"] for record in records]
        self.assertEqual(sorted(times), times)
        for record in records:
            self.assertTrue(record["occurred_at"].endswith("Z"))

    def test_window_is_closed_at_both_endpoints(self):
        self._metered_records()
        records = self.service.usage_records("acme", _ts(T0), _ts(T_MID))["records"]
        self.assertEqual([1, 2], [record["sequence"] for record in records])
        point = self.service.usage_records("acme", _ts(T1), _ts(T1))["records"]
        self.assertEqual([3], [record["sequence"] for record in point])

    def test_open_bounds_and_reverse_window(self):
        self._metered_records()
        records = self.service.usage_records("acme", _ts(T_MID), None)["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])
        records = self.service.usage_records("acme", None, _ts(T_MID))["records"]
        self.assertEqual([1, 2], [record["sequence"] for record in records])
        self.assertEqual({"records": []}, self.service.usage_records("acme", _ts(T1), _ts(T0)))

    def test_cursor_returns_only_strictly_later_sequences(self):
        self._metered_records()
        records = self.service.usage_records("acme", cursor=1)["records"]
        self.assertEqual([2, 3], [record["sequence"] for record in records])
        self.assertEqual([], self.service.usage_records("acme", cursor=100)["records"])

    def test_limit_caps_the_page(self):
        self._metered_records()
        records = self.service.usage_records("acme", limit=2)["records"]
        self.assertEqual([1, 2], [record["sequence"] for record in records])

    def test_pagination_neither_overlaps_nor_skips(self):
        self._metered_records()
        seen = []
        cursor = None
        while True:
            page = self.service.usage_records("acme", cursor=cursor, limit=1)["records"]
            if not page:
                break
            seen.extend(record["sequence"] for record in page)
            cursor = page[-1]["sequence"]
        self.assertEqual([1, 2, 3], seen)

    def test_window_and_cursor_combine_without_renumbering(self):
        self._metered_records()
        records = self.service.usage_records("acme", _ts(T_MID), None, cursor=2, limit=1)["records"]
        self.assertEqual([{"sequence": 3, "type": "execution_started", "occurred_at": T1}], records)

    def test_records_are_isolated_per_tenant(self):
        self._metered_records()
        self.service.create_workflow({"id": "wf-b", "nodes": TASK}, "wf-b", "beta")
        self.assertEqual([1, 2, 3], [r["sequence"] for r in self.service.usage_records("acme")["records"]])
        beta = self.service.usage_records("beta")["records"]
        self.assertEqual([1], [record["sequence"] for record in beta])
        # Another tenant's records stay invisible in every window and page.
        self.assertEqual(
            {"records": []},
            self.service.usage_records("beta", _ts(T0), _ts(T1)),
        )

    def test_records_query_is_read_only(self):
        self._metered_records()
        before = self.service.usage("acme")
        self.service.usage_records("acme", _ts(T0), _ts(T1), cursor=1, limit=2)
        self.assertEqual(before, self.service.usage("acme"))
        self.assertEqual(3, len(self.service.usage_records("acme")["records"]))


class UsageRecordsHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-usage-records.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, json.dumps(body) if body is not None else None, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_records_require_a_tenant_header(self):
        for headers in (None, {"X-Tenant-Id": ""}):
            status, data = self.call("GET", "/usage/records?limit=10", headers=headers)
            self.assertEqual(400, status)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_records_over_http(self):
        headers = {"X-Tenant-Id": "http-records"}
        status, data = self.call("GET", "/usage/records?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        self.call("POST", "/workflows", {"id": "wf-rec", "nodes": TASK}, key="wf-rec", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-rec", "workflow_id": "wf-rec", "input": {}},
            key="r-rec",
            headers=headers,
        )
        status, data = self.call("GET", "/usage/records?limit=10", headers=headers)
        self.assertEqual(200, status)
        records = json.loads(data)["records"]
        self.assertEqual([1, 2], [record["sequence"] for record in records])
        self.assertEqual(["workflow_created", "execution_started"], [record["type"] for record in records])
        for record in records:
            self.assertEqual(["sequence", "type", "occurred_at"], list(record))
            self.assertTrue(record["occurred_at"].endswith("Z"))
        # Paging by the last record's sequence returns the rest, then nothing.
        status, data = self.call("GET", "/usage/records?cursor=1&limit=1", headers=headers)
        self.assertEqual([2], [record["sequence"] for record in json.loads(data)["records"]])
        status, data = self.call("GET", "/usage/records?cursor=2&limit=1", headers=headers)
        self.assertEqual({"records": []}, json.loads(data))

    def test_records_do_not_leak_across_tenants(self):
        headers = {"X-Tenant-Id": "http-records-other"}
        status, data = self.call("GET", "/usage/records?limit=10", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))

    def test_records_parameters_are_validated(self):
        headers = {"X-Tenant-Id": "http-records-validation"}
        bad_paths = (
            "/usage/records",
            "/usage/records?limit=0",
            "/usage/records?limit=-1",
            "/usage/records?limit=1.5",
            "/usage/records?limit=abc",
            "/usage/records?limit=",
            "/usage/records?limit=1&limit=2",
            "/usage/records?limit=1&cursor=0",
            "/usage/records?limit=1&cursor=-2",
            "/usage/records?limit=1&cursor=1e3",
            "/usage/records?limit=1&cursor=",
            "/usage/records?limit=1&since=not-a-time",
            "/usage/records?limit=1&until=2026-01-01",
            "/usage/records?limit=1&since=2026-01-01T00:00:00Z&since=2026-02-01T00:00:00Z",
            "/usage/records?limit=1&bogus=1",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_records_window_and_pagination_over_http(self):
        headers = {"X-Tenant-Id": "http-records-window"}
        self.call("POST", "/workflows", {"id": "wf-rw", "nodes": TASK}, key="wf-rw", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-rw", "workflow_id": "wf-rw", "input": {}},
            key="r-rw",
            headers=headers,
        )
        # A window entirely in the past matches nothing and is not an error.
        past = "&since=2000-01-01T00:00:00.000Z&until=2000-02-01T00:00:00.000Z"
        status, data = self.call("GET", "/usage/records?limit=10" + past, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))
        # A reversed window is an empty result, not an error.
        reverse = "&since=2099-01-01T00:00:00.000Z&until=2000-01-01T00:00:00.000Z"
        status, data = self.call("GET", "/usage/records?limit=10" + reverse, headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"records": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        # A window around now matches both records; the cumulative query is unchanged.
        wide = "&since=2000-01-01T00:00:00.000Z&until=2099-01-01T00:00:00.000Z"
        status, data = self.call("GET", "/usage/records?limit=10" + wide, headers=headers)
        self.assertEqual([1, 2], [record["sequence"] for record in json.loads(data)["records"]])
        status, data = self.call("GET", "/usage", headers=headers)
        self.assertEqual(
            [
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            json.loads(data)["usage"],
        )


if __name__ == "__main__":
    unittest.main()
