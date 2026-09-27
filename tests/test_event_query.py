import http.client
import json
import tempfile
import threading
import unittest
from datetime import datetime
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import EVENT_TYPES, ChronicleFlow

WORKFLOW = {
    "id": "orders",
    "nodes": [
        {"id": "reserve", "kind": "task", "depends_on": [], "retries": 1},
        {"id": "charge", "kind": "task", "depends_on": ["reserve"]},
    ],
}


class EventQueryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"))
        self.service.create_workflow(WORKFLOW, "w1")
        self.service.create_execution({"id": "run-1", "workflow_id": "orders", "input": {}}, "e1")
        self.service.advance("run-1", {"failure": {"reason": "boom"}}, "a1")
        self.service.advance("run-1", {"output": {"reservation": 9}}, "a2")
        self.service.advance("run-1", {"output": {"charge": "ok"}}, "a3")
        self.all_events = self.service.events("run-1")

    def tearDown(self):
        self.directory.cleanup()

    def test_no_arguments_returns_the_full_stream(self):
        self.assertEqual(self.all_events, self.service.events("run-1"))
        self.assertEqual([1, 2, 3, 4, 5, 6], [event["sequence"] for event in self.all_events])

    def test_type_filter_keeps_only_named_types_with_their_sequences(self):
        filtered = self.service.events("run-1", types=frozenset({"node_completed"}))
        self.assertEqual(["node_completed", "node_completed"], [event["type"] for event in filtered])
        self.assertEqual([4, 5], [event["sequence"] for event in filtered])

    def test_multiple_types_take_their_union(self):
        filtered = self.service.events(
            "run-1", types=frozenset({"node_failed", "node_retried"})
        )
        self.assertEqual(["node_failed", "node_retried"], [event["type"] for event in filtered])
        self.assertEqual([2, 3], [event["sequence"] for event in filtered])

    def test_closed_time_window_includes_boundaries(self):
        events = self.all_events
        since = events[1]["occurred_at"]
        until = events[3]["occurred_at"]
        filtered = self.service.events(
            "run-1",
            since=datetime.fromisoformat(since[:-1] + "+00:00"),
            until=datetime.fromisoformat(until[:-1] + "+00:00"),
        )
        self.assertEqual([2, 3, 4], [event["sequence"] for event in filtered])

    def test_reversed_window_is_an_empty_list(self):
        events = self.all_events
        self.assertEqual(
            [],
            self.service.events(
                "run-1",
                since=datetime.fromisoformat(events[-1]["occurred_at"][:-1] + "+00:00"),
                until=datetime.fromisoformat(events[0]["occurred_at"][:-1] + "+00:00"),
            ),
        )

    def test_cursor_is_strict_and_pages_do_not_overlap_or_gap(self):
        page1 = self.service.events("run-1", types=frozenset({"node_completed"}), cursor=0, limit=1)
        self.assertEqual([4], [event["sequence"] for event in page1])
        page2 = self.service.events("run-1", types=frozenset({"node_completed"}), cursor=4, limit=1)
        self.assertEqual([5], [event["sequence"] for event in page2])
        page3 = self.service.events("run-1", types=frozenset({"node_completed"}), cursor=5, limit=1)
        self.assertEqual([], page3)

    def test_filters_and_pagination_apply_together(self):
        page = self.service.events(
            "run-1",
            types=frozenset({"node_completed", "node_failed"}),
            cursor=2,
            limit=1,
        )
        self.assertEqual([(4, "node_completed")], [(e["sequence"], e["type"]) for e in page])

    def test_cursor_past_the_end_and_unmatched_type_are_empty_lists(self):
        self.assertEqual([], self.service.events("run-1", cursor=999))
        self.assertEqual([], self.service.events("run-1", types=frozenset({"loop_completed"})))

    def test_missing_execution_is_not_found(self):
        from chronicleflow.errors import NotFoundError

        with self.assertRaises(NotFoundError):
            self.service.events("nope", cursor=1)

    def test_cross_tenant_execution_is_not_found(self):
        from chronicleflow.errors import NotFoundError

        self.service.create_workflow(
            {"id": "wf", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "w2",
            "other",
        )
        self.service.create_execution(
            {"id": "r", "workflow_id": "wf", "input": {}}, "e2", "other"
        )
        with self.assertRaises(NotFoundError):
            self.service.events("r", "acme", types=frozenset({"execution_started"}))

    def test_query_is_read_only(self):
        self.service.events("run-1", types=frozenset({"node_completed"}), cursor=1, limit=1)
        self.service.events(
            "run-1",
            since=datetime.fromisoformat(self.all_events[0]["occurred_at"][:-1] + "+00:00"),
        )
        self.assertEqual(self.all_events, self.service.events("run-1"))

    def test_declared_event_type_set_covers_every_produced_event(self):
        produced = {event["type"] for event in self.all_events}
        self.assertLessEqual(produced, set(EVENT_TYPES))


class EventQueryHttpTests(unittest.TestCase):
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

    def setUp(self):
        self.service = Handler.service
        suffix = self._testMethodName
        self.workflow_id = f"orders-{suffix}"
        self.run_id = f"run-http-{suffix}"
        self.service.create_workflow({**WORKFLOW, "id": self.workflow_id}, f"w-{suffix}")
        self.service.create_execution(
            {"id": self.run_id, "workflow_id": self.workflow_id, "input": {}}, f"e-{suffix}"
        )
        self.service.advance(self.run_id, {"failure": {"reason": "boom"}}, f"a1-{suffix}")
        self.service.advance(self.run_id, {"output": {"reservation": 9}}, f"a2-{suffix}")
        self.service.advance(self.run_id, {"output": {"charge": "ok"}}, f"a3-{suffix}")

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        all_headers = {"Content-Type": "application/json"}
        all_headers.update(headers or {})
        connection.request(method, path, json.dumps(body) if body is not None else None, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def events(self, query=""):
        status, data = self.request("GET", f"/executions/{self.run_id}/events{query}")
        self.assertEqual(200, status)
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))
        return json.loads(data)["events"]

    def test_no_parameters_returns_the_full_stream_byte_for_byte(self):
        status, plain = self.request("GET", f"/executions/{self.run_id}/events")
        self.assertEqual(200, status)
        events = json.loads(plain)["events"]
        self.assertEqual([1, 2, 3, 4, 5, 6], [event["sequence"] for event in events])
        self.assertEqual(
            [
                "execution_started",
                "node_failed",
                "node_retried",
                "node_completed",
                "node_completed",
                "execution_completed",
            ],
            [event["type"] for event in events],
        )

    def test_type_filter(self):
        events = self.events("?types=node_completed")
        self.assertEqual([4, 5], [event["sequence"] for event in events])
        events = self.events("?types=node_completed,node_failed")
        self.assertEqual(
            [(2, "node_failed"), (4, "node_completed"), (5, "node_completed")],
            [(event["sequence"], event["type"]) for event in events],
        )

    def test_time_window_is_closed(self):
        events = self.events()
        since = events[1]["occurred_at"]
        until = events[3]["occurred_at"]
        windowed = self.events(f"?since={since}&until={until}")
        self.assertEqual([2, 3, 4], [event["sequence"] for event in windowed])

    def test_reversed_window_is_empty_not_an_error(self):
        events = self.events()
        windowed = self.events(f"?since={events[-1]['occurred_at']}&until={events[0]['occurred_at']}")
        self.assertEqual([], windowed)

    def test_cursor_and_limit_page_through_a_filtered_stream(self):
        page1 = self.events("?types=node_completed&limit=1")
        self.assertEqual([4], [event["sequence"] for event in page1])
        page2 = self.events(f"?types=node_completed&cursor={page1[-1]['sequence']}&limit=1")
        self.assertEqual([5], [event["sequence"] for event in page2])
        page3 = self.events(f"?types=node_completed&cursor={page2[-1]['sequence']}&limit=1")
        self.assertEqual([], page3)

    def test_filters_and_pagination_combine(self):
        events = self.events("?types=node_completed,node_failed&cursor=2&limit=1")
        self.assertEqual([(4, "node_completed")], [(e["sequence"], e["type"]) for e in events])

    def test_unmatched_filter_is_a_definite_empty_list(self):
        status, data = self.request("GET", f"/executions/{self.run_id}/events?types=loop_completed")
        self.assertEqual(200, status)
        self.assertEqual(b'{"events":[]}\n', data)

    def test_validation_errors(self):
        bad_paths = [
            f"/executions/{self.run_id}/events?bogus=1",
            f"/executions/{self.run_id}/events?types=node_completed&types=node_failed",
            f"/executions/{self.run_id}/events?cursor=1&cursor=2",
            f"/executions/{self.run_id}/events?types=",
            f"/executions/{self.run_id}/events?types=node_completed,",
            f"/executions/{self.run_id}/events?types=,node_completed",
            f"/executions/{self.run_id}/events?types=node_completed,node_completed",
            f"/executions/{self.run_id}/events?types=node_completed,bogus",
            f"/executions/{self.run_id}/events?since=not-a-time",
            f"/executions/{self.run_id}/events?until=2026-01-01",
            f"/executions/{self.run_id}/events?cursor=0",
            f"/executions/{self.run_id}/events?cursor=-1",
            f"/executions/{self.run_id}/events?cursor=1.5",
            f"/executions/{self.run_id}/events?cursor=abc",
            f"/executions/{self.run_id}/events?cursor=",
            f"/executions/{self.run_id}/events?limit=0",
            f"/executions/{self.run_id}/events?limit=-2",
            f"/executions/{self.run_id}/events?limit=1e3",
            f"/executions/{self.run_id}/events?limit=Infinity",
            f"/executions/{self.run_id}/events?limit=NaN",
        ]
        for path in bad_paths:
            with self.subTest(path=path):
                status, data = self.request("GET", path)
                self.assertEqual(400, status)
                self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        # A rejected query writes nothing: the stream is intact afterwards.
        self.assertEqual([1, 2, 3, 4, 5, 6], [event["sequence"] for event in self.events()])

    def test_missing_and_cross_tenant_executions_are_not_found(self):
        status, data = self.request("GET", "/executions/missing/events?types=node_completed")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])
        self.service.create_workflow(
            {"id": "wf-other", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            f"w-other-{self.id()}",
            "other",
        )
        self.service.create_execution(
            {"id": "run-other", "workflow_id": "wf-other", "input": {}}, f"e-other-{self.id()}", "other"
        )
        status, data = self.request(
            "GET", "/executions/run-other/events?limit=1", headers={"X-Tenant-Id": "acme"}
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_tenant_scoped_query_writes_no_usage(self):
        headers = {"X-Tenant-Id": "acme-events"}
        self.service.create_workflow(
            {"id": "wf-acme", "nodes": [{"id": "a", "kind": "task", "depends_on": []}]},
            "w-acme",
            "acme-events",
        )
        self.service.create_execution(
            {"id": "run-acme", "workflow_id": "wf-acme", "input": {}}, "e-acme", "acme-events"
        )
        before = self.service.usage("acme-events")
        status, _ = self.request("GET", "/executions/run-acme/events?limit=1", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(before, self.service.usage("acme-events"))
