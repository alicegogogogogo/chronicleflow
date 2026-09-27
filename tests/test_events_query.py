import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow

TWO_TASKS = [
    {"id": "a", "kind": "task", "depends_on": []},
    {"id": "b", "kind": "task", "depends_on": ["a"]},
]


class EventsQueryServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "events.db"))
        self.service.create_workflow({"id": "wf", "nodes": TWO_TASKS}, "wf-1")
        self.service.create_execution({"id": "run", "workflow_id": "wf", "input": {}}, "run-1")
        self.service.advance("run", {"output": {"x": 1}}, "adv-1")
        self.service.advance("run", {"output": {"y": 2}}, "adv-2")

    def tearDown(self):
        self.directory.cleanup()

    def types(self, events):
        return [event["type"] for event in events]

    def test_no_arguments_returns_full_ordered_stream(self):
        events = self.service.events("run")
        self.assertEqual(
            ["execution_started", "node_completed", "node_completed", "execution_completed"],
            self.types(events),
        )
        self.assertEqual([1, 2, 3, 4], [event["sequence"] for event in events])

    def test_type_filter_keeps_only_matching_types(self):
        events = self.service.events("run", types=("node_completed",))
        self.assertEqual(["node_completed", "node_completed"], self.types(events))
        self.assertEqual([2, 3], [event["sequence"] for event in events])

    def test_type_filter_with_no_match_returns_empty_list(self):
        self.assertEqual([], self.service.events("run", types=("node_failed",)))

    def test_sequences_are_not_renumbered_by_filtering(self):
        events = self.service.events("run", types=("execution_completed",))
        self.assertEqual([4], [event["sequence"] for event in events])

    def test_time_window_is_a_closed_interval(self):
        events = self.service.events("run")
        first, last = events[0]["occurred_at"], events[-1]["occurred_at"]
        windowed = self.service.events("run", since=self._at(first), until=self._at(last))
        self.assertEqual([event["sequence"] for event in events if first <= event["occurred_at"] <= last],
                         [event["sequence"] for event in windowed])
        self.assertIn(events[0]["sequence"], [event["sequence"] for event in windowed])
        self.assertIn(events[-1]["sequence"], [event["sequence"] for event in windowed])

    def test_reversed_window_returns_empty_list(self):
        events = self.service.events("run")
        later, earlier = events[-1]["occurred_at"], events[0]["occurred_at"]
        self.assertEqual([], self.service.events("run", since=self._at(later), until=self._at(earlier)))

    def test_cursor_returns_only_strictly_later_sequences(self):
        events = self.service.events("run", cursor=2)
        self.assertEqual([3, 4], [event["sequence"] for event in events])

    def test_limit_caps_the_page(self):
        events = self.service.events("run", limit=2)
        self.assertEqual([1, 2], [event["sequence"] for event in events])

    def test_consecutive_pages_neither_overlap_nor_skip(self):
        collected = []
        cursor = None
        while True:
            page = self.service.events("run", cursor=cursor, limit=1)
            if not page:
                break
            collected.extend(page)
            cursor = page[-1]["sequence"]
        self.assertEqual(self.service.events("run"), collected)

    def test_cursor_past_the_end_returns_empty_list(self):
        self.assertEqual([], self.service.events("run", cursor=100))

    def test_filters_and_pagination_combine_as_intersection(self):
        events = self.service.events("run", types=("node_completed",), cursor=2, limit=1)
        self.assertEqual([3], [event["sequence"] for event in events])

    def _at(self, occurred_at):
        from chronicleflow.service import _parse_stored_time

        return _parse_stored_time(occurred_at)


class EventsQueryHttpTests(unittest.TestCase):
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
        self.run_id = f"run-{id(self)}"
        Handler.service.create_workflow({"id": f"wf-{id(self)}", "nodes": TWO_TASKS}, f"wf-{id(self)}")
        Handler.service.create_execution(
            {"id": self.run_id, "workflow_id": f"wf-{id(self)}", "input": {}}, f"ex-{id(self)}"
        )
        Handler.service.advance(self.run_id, {"output": {"x": 1}}, f"a1-{id(self)}")
        Handler.service.advance(self.run_id, {"output": {"y": 2}}, f"a2-{id(self)}")

    def request(self, path):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request("GET", path)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def events_path(self, query=""):
        return f"/executions/{self.run_id}/events{query}"

    def test_unparameterized_response_is_unchanged(self):
        status, data = self.request(self.events_path())
        self.assertEqual(200, status)
        body = json.loads(data)
        self.assertEqual(["events"], list(body))
        self.assertEqual(
            ["execution_started", "node_completed", "node_completed", "execution_completed"],
            [event["type"] for event in body["events"]],
        )
        self.assertTrue(data.endswith(b"\n"))
        self.assertFalse(data.endswith(b"\n\n"))

    def test_type_filter(self):
        status, data = self.request(self.events_path("?types=node_completed"))
        self.assertEqual(200, status)
        events = json.loads(data)["events"]
        self.assertEqual([2, 3], [event["sequence"] for event in events])

    def test_cursor_and_limit(self):
        status, data = self.request(self.events_path("?cursor=1&limit=2"))
        self.assertEqual(200, status)
        events = json.loads(data)["events"]
        self.assertEqual([2, 3], [event["sequence"] for event in events])

    def test_empty_result_is_a_definite_empty_list(self):
        status, data = self.request(self.events_path("?types=node_failed"))
        self.assertEqual(200, status)
        self.assertEqual({"events": []}, json.loads(data))

    def test_unknown_parameter_is_rejected(self):
        status, data = self.request(self.events_path("?bogus=1"))
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])

    def test_repeated_parameter_is_rejected(self):
        status, _ = self.request(self.events_path("?limit=1&limit=2"))
        self.assertEqual(400, status)

    def test_types_with_empty_entry_is_rejected(self):
        for query in ("?types=", "?types=node_completed,", "?types=,node_completed"):
            status, _ = self.request(self.events_path(query))
            self.assertEqual(400, status, query)

    def test_types_with_duplicate_entry_is_rejected(self):
        status, _ = self.request(self.events_path("?types=node_completed,node_completed"))
        self.assertEqual(400, status)

    def test_types_with_unknown_type_is_rejected(self):
        status, _ = self.request(self.events_path("?types=node_completed,bogus"))
        self.assertEqual(400, status)

    def test_malformed_timestamps_are_rejected(self):
        for query in ("?since=yesterday", "?until=2026-09-26%2008:00:00", "?since="):
            status, _ = self.request(self.events_path(query))
            self.assertEqual(400, status, query)

    def test_non_positive_integer_cursor_and_limit_are_rejected(self):
        for query in ("?cursor=0", "?cursor=-1", "?cursor=1.5", "?cursor=abc", "?limit=0", "?limit=-2", "?limit=1e3"):
            status, _ = self.request(self.events_path(query))
            self.assertEqual(400, status, query)

    def test_reversed_window_returns_empty_list(self):
        status, data = self.request(
            self.events_path("?since=2026-09-27T00:00:00.000Z&until=2026-09-26T00:00:00.000Z")
        )
        self.assertEqual(200, status)
        self.assertEqual({"events": []}, json.loads(data))

    def test_missing_execution_is_not_found(self):
        status, data = self.request("/executions/missing/events?limit=1")
        self.assertEqual(404, status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])

    def test_other_tenants_execution_is_not_found(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        connection.request("GET", self.events_path("?limit=1"), None, {"X-Tenant-Id": "someone-else"})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(404, response.status)
        self.assertEqual("not_found", json.loads(data)["error"]["code"])


if __name__ == "__main__":
    unittest.main()
