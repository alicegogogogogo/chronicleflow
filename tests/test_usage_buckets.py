import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import (
    USAGE_UNIT_PRICES,
    ChronicleFlow,
    _parse_timestamp,
)

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

# Deterministic occurrence times spanning several UTC hours and two UTC days.
T_H1 = "2026-09-26T07:45:00.000Z"
T_B8 = "2026-09-26T08:00:00.000Z"
T_H8 = "2026-09-26T08:15:30.500Z"
T_H8_EDGE = "2026-09-26T08:59:59.999Z"
T_B9 = "2026-09-26T09:00:00.000Z"
T_NEXTDAY = "2026-09-27T00:00:00.000Z"
T_NEXTDAY_NOON = "2026-09-27T12:00:00.000Z"


def _ts(value):
    return _parse_timestamp(value, "since")


class BucketServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "buckets.db"))
        # sequence, type, occurred_at
        self._records = [
            (1, "execution_started", T_H1),
            (2, "workflow_created", T_B8),
            (3, "execution_started", T_H8),
            (4, "delivery_attempted", T_H8_EDGE),
            (5, "schedule_triggered", T_B9),
            (6, "execution_started", T_NEXTDAY),
            (7, "workflow_created", T_NEXTDAY_NOON),
        ]
        with self.service.store.transaction() as connection:
            for sequence, usage_type, created_at in self._records:
                connection.execute(
                    "INSERT INTO usage_records (tenant, sequence, type, created_at) VALUES (?, ?, ?, ?)",
                    ("acme", sequence, usage_type, created_at),
                )

    def tearDown(self):
        self.directory.cleanup()

    def test_empty_bucketed_usage_and_bill_are_definite_empty_results(self):
        self.assertEqual({"buckets": []}, self.service.usage("beta", bucket="hour"))
        self.assertEqual({"buckets": []}, self.service.usage("beta", bucket="day"))
        self.assertEqual(
            {"bill": {"buckets": [], "total": 0}},
            self.service.bill("beta", bucket="hour"),
        )
        self.assertEqual(
            {"bill": {"buckets": [], "total": 0}},
            self.service.bill("beta", bucket="day"),
        )

    def test_bucketed_queries_require_a_tenant(self):
        with self.assertRaises(ValidationError):
            self.service.usage("", bucket="hour")
        with self.assertRaises(ValidationError):
            self.service.bill("", bucket="day")

    def test_bucket_value_is_validated(self):
        for bad in ("", "week", "HOUR", "Hour", "daily", "minute"):
            with self.assertRaises(ValidationError):
                self.service.usage("acme", bucket=bad)
            with self.assertRaises(ValidationError):
                self.service.bill("acme", bucket=bad)

    def test_hour_buckets_align_to_the_utc_hour_and_sort_ascending(self):
        result = self.service.usage("acme", bucket="hour")
        self.assertEqual(
            [
                {"bucket_start": "2026-09-26T07:00:00Z", "counts": [
                    {"type": "execution_started", "count": 1},
                ]},
                {"bucket_start": "2026-09-26T08:00:00Z", "counts": [
                    {"type": "delivery_attempted", "count": 1},
                    {"type": "execution_started", "count": 1},
                    {"type": "workflow_created", "count": 1},
                ]},
                {"bucket_start": "2026-09-26T09:00:00Z", "counts": [
                    {"type": "schedule_triggered", "count": 1},
                ]},
                {"bucket_start": "2026-09-27T00:00:00Z", "counts": [
                    {"type": "execution_started", "count": 1},
                ]},
                {"bucket_start": "2026-09-27T12:00:00Z", "counts": [
                    {"type": "workflow_created", "count": 1},
                ]},
            ],
            result["buckets"],
        )
        starts = [bucket["bucket_start"] for bucket in result["buckets"]]
        self.assertEqual(starts, sorted(starts))
        for bucket in result["buckets"]:
            self.assertTrue(bucket["bucket_start"].endswith("Z"))
            types = [entry["type"] for entry in bucket["counts"]]
            self.assertEqual(types, sorted(types))

    def test_day_buckets_align_to_utc_midnight(self):
        result = self.service.usage("acme", bucket="day")
        self.assertEqual(
            [
                {"bucket_start": "2026-09-26T00:00:00Z", "counts": [
                    {"type": "delivery_attempted", "count": 1},
                    {"type": "execution_started", "count": 2},
                    {"type": "schedule_triggered", "count": 1},
                    {"type": "workflow_created", "count": 1},
                ]},
                {"bucket_start": "2026-09-27T00:00:00Z", "counts": [
                    {"type": "execution_started", "count": 1},
                    {"type": "workflow_created", "count": 1},
                ]},
            ],
            result["buckets"],
        )

    def test_empty_buckets_between_recorded_ones_never_appear(self):
        # Records at 07:45 and 09:00 leave the 08:00? bucket present (other
        # records live there); check the empty 10:00..11:00 span is not padded.
        with self.service.store.transaction() as connection:
            connection.execute("DELETE FROM usage_records WHERE sequence IN (2, 3, 4)")
        result = self.service.usage("acme", bucket="hour")
        self.assertEqual(
            ["2026-09-26T07:00:00Z", "2026-09-26T09:00:00Z",
             "2026-09-27T00:00:00Z", "2026-09-27T12:00:00Z"],
            [bucket["bucket_start"] for bucket in result["buckets"]],
        )

    def test_bucketed_counts_match_the_unbucketed_totals(self):
        for bucket in ("hour", "day"):
            bucketed = self.service.usage("acme", bucket=bucket)["buckets"]
            totals = {}
            for entry in bucketed:
                for count in entry["counts"]:
                    totals[count["type"]] = totals.get(count["type"], 0) + count["count"]
            plain = {
                entry["type"]: entry["count"]
                for entry in self.service.usage("acme")["usage"]
            }
            self.assertEqual(plain, totals)

    def test_window_intersects_buckets_with_closed_endpoints(self):
        # since at exactly 08:00 includes the workflow_created stamped at 08:00
        # together with the other records occurring later in the same hour.
        result = self.service.usage("acme", _ts(T_B8), None, None, "hour")
        starts = {bucket["bucket_start"]: bucket["counts"] for bucket in result["buckets"]}
        self.assertNotIn("2026-09-26T07:00:00Z", starts)
        self.assertEqual(
            [
                {"type": "delivery_attempted", "count": 1},
                {"type": "execution_started", "count": 1},
                {"type": "workflow_created", "count": 1},
            ],
            starts["2026-09-26T08:00:00Z"],
        )
        self.assertIn("2026-09-26T09:00:00Z", starts)
        # A point window at exactly 08:00 keeps only that boundary record.
        result = self.service.usage("acme", _ts(T_B8), _ts(T_B8), None, "hour")
        self.assertEqual(
            ["2026-09-26T08:00:00Z"],
            [bucket["bucket_start"] for bucket in result["buckets"]],
        )
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            result["buckets"][0]["counts"],
        )

    def test_reverse_window_gives_empty_buckets_not_an_error(self):
        self.assertEqual(
            {"buckets": []},
            self.service.usage("acme", _ts(T_NEXTDAY), _ts(T_H1), None, "hour"),
        )
        self.assertEqual(
            {"bill": {"buckets": [], "total": 0}},
            self.service.bill("acme", _ts(T_NEXTDAY), _ts(T_H1), None, "day"),
        )

    def test_type_filter_intersects_buckets(self):
        result = self.service.usage("acme", bucket="hour", types=("execution_started",))
        self.assertEqual(
            [
                ("2026-09-26T07:00:00Z", 1),
                ("2026-09-26T08:00:00Z", 1),
                ("2026-09-27T00:00:00Z", 1),
            ],
            [
                (bucket["bucket_start"], bucket["counts"][0]["count"])
                for bucket in result["buckets"]
            ],
        )
        for bucket in result["buckets"]:
            self.assertEqual(
                ["execution_started"], [entry["type"] for entry in bucket["counts"]]
            )
        # A named type that never matches in the window yields the definite
        # empty bucket list: both named types only occur before the next day.
        self.assertEqual(
            {"buckets": []},
            self.service.usage(
                "acme",
                _ts(T_NEXTDAY),
                None,
                ("schedule_triggered", "delivery_attempted"),
                "hour",
            ),
        )

    def test_bill_buckets_carry_items_subtotal_and_window_total(self):
        result = self.service.bill("acme", bucket="hour")["bill"]
        self.assertEqual(
            [
                "2026-09-26T07:00:00Z",
                "2026-09-26T08:00:00Z",
                "2026-09-26T09:00:00Z",
                "2026-09-27T00:00:00Z",
                "2026-09-27T12:00:00Z",
            ],
            [bucket["bucket_start"] for bucket in result["buckets"]],
        )
        running_total = 0
        for bucket in result["buckets"]:
            types = [item["type"] for item in bucket["items"]]
            self.assertEqual(types, sorted(types))
            subtotal = 0
            for item in bucket["items"]:
                self.assertIsInstance(item["unit_price"], int)
                self.assertEqual(USAGE_UNIT_PRICES[item["type"]], item["unit_price"])
                self.assertEqual(item["count"] * item["unit_price"], item["subtotal"])
                self.assertIsInstance(item["subtotal"], int)
                subtotal += item["subtotal"]
            self.assertEqual(subtotal, bucket["subtotal"])
            running_total += subtotal
        self.assertEqual(running_total, result["total"])
        # The window total equals the unbucketed bill total over the same data.
        self.assertEqual(self.service.bill("acme")["bill"]["total"], result["total"])
        # The 08:00 bucket holds three different types.
        eight = result["buckets"][1]
        self.assertEqual(
            ["delivery_attempted", "execution_started", "workflow_created"],
            [item["type"] for item in eight["items"]],
        )
        self.assertEqual(
            USAGE_UNIT_PRICES["delivery_attempted"]
            + USAGE_UNIT_PRICES["execution_started"]
            + USAGE_UNIT_PRICES["workflow_created"],
            eight["subtotal"],
        )

    def test_day_bill_total_is_sum_of_bucket_subtotals(self):
        result = self.service.bill("acme", bucket="day")["bill"]
        self.assertEqual(2, len(result["buckets"]))
        self.assertEqual(
            sum(bucket["subtotal"] for bucket in result["buckets"]),
            result["total"],
        )
        self.assertIsInstance(result["total"], int)

    def test_buckets_are_read_only_and_tenant_isolated(self):
        before = self.service.usage("acme")
        self.service.usage("acme", bucket="hour")
        self.service.bill("acme", bucket="day")
        self.assertEqual(before, self.service.usage("acme"))
        # Another tenant never sees acme's buckets under any window or filter.
        with self.service.store.transaction() as connection:
            connection.execute(
                "INSERT INTO usage_records (tenant, sequence, type, created_at) VALUES (?, ?, ?, ?)",
                ("beta", 1, "workflow_created", T_B8),
            )
        beta = self.service.usage("beta", bucket="hour")
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            beta["buckets"][0]["counts"],
        )
        acme_starts = [
            bucket["bucket_start"]
            for bucket in self.service.usage("acme", bucket="hour")["buckets"]
        ]
        beta_starts = [
            bucket["bucket_start"]
            for bucket in self.service.usage("beta", bucket="hour")["buckets"]
        ]
        # Both tenants have a record aligned to 08:00, but only their own
        # records shape their own bucket lists.
        self.assertEqual(1, len(beta_starts))
        self.assertNotEqual(acme_starts, beta_starts)
        self.assertEqual(
            {"bill": {"buckets": [], "total": 0}},
            self.service.bill("gamma", _ts(T_H1), _ts(T_NEXTDAY_NOON), None, "day"),
        )

    def test_without_bucket_the_response_shape_is_unchanged(self):
        self.assertNotIn("buckets", self.service.usage("acme"))
        self.assertNotIn("buckets", self.service.bill("acme")["bill"])
        self.assertEqual(
            ["items", "total"], list(self.service.bill("acme")["bill"].keys())
        )


class BucketHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-buckets.db"))
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

    def test_bucketed_routes_over_http(self):
        headers = {"X-Tenant-Id": "http-buckets"}
        status, data = self.call("GET", "/usage?bucket=hour", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"buckets": []}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        status, data = self.call("GET", "/bill?bucket=day", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual({"bill": {"buckets": [], "total": 0}}, json.loads(data))
        self.assertTrue(data.endswith(b"\n"))

        self.call("POST", "/workflows", {"id": "wf-b", "nodes": TASK}, key="wf-b", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-b", "workflow_id": "wf-b", "input": {}},
            key="r-b",
            headers=headers,
        )
        # Stamp the two records into distinct UTC hours deterministically.
        with Handler.service.store.transaction() as connection:
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (T_H1, "http-buckets", 1),
            )
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (T_B8, "http-buckets", 2),
            )
        status, data = self.call("GET", "/usage?bucket=hour", headers=headers)
        self.assertEqual(200, status)
        payload = json.loads(data)
        self.assertEqual(
            ["2026-09-26T07:00:00Z", "2026-09-26T08:00:00Z"],
            [bucket["bucket_start"] for bucket in payload["buckets"]],
        )
        self.assertEqual(
            [{"type": "workflow_created", "count": 1}],
            payload["buckets"][0]["counts"],
        )
        self.assertEqual(
            [{"type": "execution_started", "count": 1}],
            payload["buckets"][1]["counts"],
        )
        status, data = self.call("GET", "/bill?bucket=day", headers=headers)
        self.assertEqual(200, status)
        bill = json.loads(data)["bill"]
        self.assertEqual(1, len(bill["buckets"]))
        self.assertEqual(bill["total"], sum(item["subtotal"] for item in bill["buckets"][0]["items"]))
        self.assertEqual(bill["buckets"][0]["subtotal"], bill["total"])

    def test_bucket_filters_combine_over_http(self):
        headers = {"X-Tenant-Id": "http-bucket-combine"}
        self.call("POST", "/workflows", {"id": "wf-c", "nodes": TASK}, key="wf-c", headers=headers)
        self.call(
            "POST",
            "/executions",
            {"id": "r-c", "workflow_id": "wf-c", "input": {}},
            key="r-c",
            headers=headers,
        )
        # workflow_created (sequence 1) at 07:45, execution_started
        # (sequence 2) exactly at 08:00.
        with Handler.service.store.transaction() as connection:
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (T_H1, "http-bucket-combine", 1),
            )
            connection.execute(
                "UPDATE usage_records SET created_at = ? WHERE tenant = ? AND sequence = ?",
                (T_B8, "http-bucket-combine", 2),
            )
        status, data = self.call(
            "GET",
            "/usage?bucket=hour&type=workflow_created",
            headers=headers,
        )
        self.assertEqual(200, status)
        buckets = json.loads(data)["buckets"]
        self.assertEqual(["2026-09-26T07:00:00Z"], [b["bucket_start"] for b in buckets])
        status, data = self.call(
            "GET",
            "/bill?bucket=hour&since=2026-09-26T08:00:00.000Z&until=2026-09-26T08:00:00.000Z",
            headers=headers,
        )
        self.assertEqual(200, status)
        bill = json.loads(data)["bill"]
        self.assertEqual(1, len(bill["buckets"]))
        self.assertEqual(["execution_started"], [i["type"] for i in bill["buckets"][0]["items"]])
        self.assertEqual(bill["buckets"][0]["subtotal"], bill["total"])

    def test_bucket_is_validated_on_both_routes(self):
        headers = {"X-Tenant-Id": "http-bucket-bad"}
        bad_paths = (
            "/usage?bucket=",
            "/bill?bucket=",
            "/usage?bucket=week",
            "/bill?bucket=day%20",
            "/usage?bucket=HOUR",
            "/bill?bucket=1",
            "/usage?bucket=hour&bucket=day",
            "/bill?bucket=day&bogus=1",
            "/usage?bucket=hour&since=not-a-time",
            "/bill?bucket=day&type=not_a_type",
        )
        for path in bad_paths:
            status, data = self.call("GET", path, headers=headers)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_bucket_requires_tenant_and_reveals_nothing(self):
        for path in ("/usage?bucket=hour", "/bill?bucket=day"):
            status, data = self.call("GET", path)
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)
            status, data = self.call("GET", path, headers={"X-Tenant-Id": ""})
            self.assertEqual(400, status, path)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], path)

    def test_without_bucket_the_response_is_byte_for_byte_unchanged(self):
        headers = {"X-Tenant-Id": "http-bucket-plain"}
        self.call("POST", "/workflows", {"id": "wf-p", "nodes": TASK}, key="wf-p", headers=headers)
        status, plain = self.call("GET", "/usage", headers=headers)
        self.assertEqual(200, status)
        status, again = self.call("GET", "/usage", headers=headers)
        self.assertEqual(plain, again)
        self.assertNotIn(b"bucket", plain)
        status, bill_plain = self.call("GET", "/bill", headers=headers)
        self.assertEqual(200, status)
        self.assertNotIn(b"bucket", bill_plain)
        self.assertTrue(bill_plain.endswith(b"\n"))
        self.assertFalse(bill_plain.endswith(b"\n\n"))


if __name__ == "__main__":
    unittest.main()
