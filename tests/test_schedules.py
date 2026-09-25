import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow
from chronicleflow.schedule import cron_previous, parse_cron, parse_schedule


NODES = [{"id": "a", "kind": "task", "depends_on": []}]


class CronParsingTests(unittest.TestCase):
    def test_field_count_must_be_five(self):
        for expression in ("* * * *", "* * * * * *", ""):
            with self.subTest(expression=expression):
                with self.assertRaises(ValidationError):
                    parse_cron(expression)

    def test_ranges_are_bounded(self):
        for expression in ("60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *", "* * * 13 *", "* * * * 7"):
            with self.subTest(expression=expression):
                with self.assertRaises(ValidationError):
                    parse_cron(expression)

    def test_unparseable_fragments_are_rejected(self):
        for expression in (
            "a * * * *",
            "*/0 * * * *",
            "*-5 * * * *",
            "5-1 * * * *",
            "1,,2 * * * *",
            "1/2/3 * * * *",
            "* * * * -1",
        ):
            with self.subTest(expression=expression):
                with self.assertRaises(ValidationError):
                    parse_cron(expression)

    def test_lists_ranges_and_steps(self):
        parsed = parse_cron("0,30 9-17 */5 1,7 1-5")
        self.assertEqual({0, 30}, parsed["minutes"])
        self.assertEqual(set(range(9, 18)), parsed["hours"])
        self.assertEqual({1, 6, 11, 16, 21, 26, 31}, parsed["days_of_month"])
        self.assertEqual({1, 7}, parsed["months"])
        self.assertEqual({1, 2, 3, 4, 5}, parsed["days_of_week"])

    def test_previous_period_walks_back(self):
        # 2026-09-25 is a Friday (cron dow 5).
        now = datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc).timestamp()
        at = datetime.fromtimestamp(cron_previous("0 12 * * 1-5", now), tz=timezone.utc)
        self.assertEqual(datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc), at)
        at = datetime.fromtimestamp(cron_previous("*/15 * * * *", now), tz=timezone.utc)
        self.assertEqual(datetime(2026, 9, 25, 13, 30, tzinfo=timezone.utc), at)
        at = datetime.fromtimestamp(cron_previous("0 0 1 * *", now), tz=timezone.utc)
        self.assertEqual(datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc), at)
        at = datetime.fromtimestamp(cron_previous("0 14 * * *", now), tz=timezone.utc)
        # Before 14:00, the previous match is yesterday at 14:00.
        self.assertEqual(datetime(2026, 9, 24, 14, 0, tzinfo=timezone.utc), at)

    def test_restricted_dom_and_dow_combine_with_or(self):
        # Monday 2026-09-21 matches the dow restriction even though day 21 is
        # not the 1st; Friday 2026-09-25 matches neither.
        friday = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc).timestamp()
        at = datetime.fromtimestamp(cron_previous("0 0 1 * 1", friday), tz=timezone.utc)
        self.assertEqual(21, at.day)


class ScheduleDeclarationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"), start_scheduler=False)

    def tearDown(self):
        self.service.close()
        self.directory.cleanup()

    def create_workflow(self, workflow_id="orders", schedule=None, key="w1"):
        body: dict = {"id": workflow_id, "nodes": NODES}
        if schedule is not None:
            body["schedule"] = schedule
        return self.service.create_workflow(body, key)

    def test_exactly_one_plan_is_required(self):
        with self.assertRaises(ValidationError):
            parse_schedule({"input": {}, "misfire_policy": "catch_up"})
        with self.assertRaises(ValidationError):
            parse_schedule({"interval_seconds": 60, "cron": "* * * * *", "input": {}, "misfire_policy": "catch_up"})

    def test_interval_must_be_positive_integer(self):
        for interval in (0, -1, 1.5, True, "60", 1.0):
            with self.subTest(interval=interval):
                with self.assertRaises(ValidationError):
                    parse_schedule({"interval_seconds": interval, "input": {}, "misfire_policy": "catch_up"})

    def test_misfire_policy_must_be_known(self):
        with self.assertRaises(ValidationError):
            parse_schedule({"interval_seconds": 60, "input": {}, "misfire_policy": "later"})

    def test_input_must_be_an_object_of_finite_numbers(self):
        with self.assertRaises(ValidationError):
            parse_schedule({"interval_seconds": 60, "input": [], "misfire_policy": "catch_up"})
        with self.assertRaises(ValidationError):
            parse_schedule({"interval_seconds": 60, "input": {"v": float("inf")}, "misfire_policy": "catch_up"})

    def test_unknown_fields_are_rejected(self):
        with self.assertRaises(ValidationError):
            parse_schedule({"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up", "extra": 1})

    def test_schedule_is_returned_with_the_workflow(self):
        stored = self.create_workflow(
            schedule={"interval_seconds": 60, "input": {"k": 1}, "misfire_policy": "catch_up"}
        )
        self.assertEqual(60, stored["schedule"]["interval_seconds"])
        self.assertEqual({"k": 1}, stored["schedule"]["input"])
        self.assertEqual("catch_up", stored["schedule"]["misfire_policy"])

    def test_invalid_schedule_rejects_whole_workflow_request(self):
        with self.assertRaises(ValidationError):
            self.create_workflow(schedule={"interval_seconds": 0, "input": {}, "misfire_policy": "catch_up"})
        with self.assertRaises(NotFoundError):
            self.service.get_schedule("orders")

    def test_workflow_without_schedule_is_unchanged(self):
        stored = self.service.create_workflow({"id": "plain", "nodes": NODES}, "w0")
        self.assertNotIn("schedule", stored)
        self.assertEqual({"schedule": None}, self.service.get_schedule("plain"))

    def test_declare_on_existing_workflow(self):
        self.create_workflow()
        status = self.service.declare_schedule(
            "orders", {"cron": "0 12 * * *", "input": {}, "misfire_policy": "skip"}, "d1"
        )["schedule"]
        self.assertEqual("0 12 * * *", status["cron"])
        self.assertFalse(status["paused"])
        self.assertIsNone(status["last_fired_at"])
        self.assertIsNone(status["last_execution_id"])

    def test_replace_schedule_validates_before_writing(self):
        self.create_workflow(schedule={"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"})
        with self.assertRaises(ValidationError):
            self.service.declare_schedule("orders", {"interval_seconds": -5, "input": {}, "misfire_policy": "catch_up"}, "d2")
        status = self.service.get_schedule("orders")["schedule"]
        self.assertEqual(60, status["interval_seconds"])

    def test_declare_for_missing_workflow_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.declare_schedule("ghost", {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"}, "d3")

    def test_schedule_query_for_missing_workflow_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.get_schedule("ghost")

    def test_reusing_idempotency_key_across_schedule_operations_conflicts(self):
        self.create_workflow(schedule={"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"})
        self.service.pause_schedule("orders", "shared-key")
        with self.assertRaises(ConflictError):
            self.service.resume_schedule("orders", "shared-key")

    def test_replace_preserves_pause_but_restarts_plan(self):
        self.create_workflow(schedule={"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"})
        self.service.pump_schedules(time.time() + 90)
        self.service.pause_schedule("orders", "p1")
        self.service.declare_schedule(
            "orders", {"interval_seconds": 120, "input": {}, "misfire_policy": "skip"}, "d1"
        )
        status = self.service.get_schedule("orders")["schedule"]
        self.assertEqual(120, status["interval_seconds"])
        self.assertEqual("skip", status["misfire_policy"])
        self.assertTrue(status["paused"])
        self.assertIsNone(status["last_execution_id"])
        self.assertIsNone(status["last_fired_at"])
        # Periods recorded under the old plan are dropped with the replacement.
        self.assertEqual(
            0,
            self.service.store.connection.execute(
                "SELECT COUNT(*) AS n FROM schedule_periods WHERE workflow_id = 'orders'"
            ).fetchone()["n"],
        )


class IntervalSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"), start_scheduler=False)
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": NODES,
                "schedule": {"interval_seconds": 60, "input": {"order": 7}, "misfire_policy": "catch_up"},
            },
            "w1",
        )
        self.activation = self.service.store.connection.execute(
            "SELECT activated_at FROM schedules WHERE workflow_id = 'orders'"
        ).fetchone()["activated_at"]

    def tearDown(self):
        self.service.close()
        self.directory.cleanup()

    def test_nothing_fires_before_first_period(self):
        self.assertEqual([], self.service.pump_schedules(self.activation + 30))

    def test_due_period_creates_one_execution(self):
        fired = self.service.pump_schedules(self.activation + 90)
        self.assertEqual(1, len(fired))
        execution_id = fired[0]["execution_id"]
        self.assertEqual(self.activation + 60, fired[0]["period_start"])
        state = self.service.get_execution(execution_id)
        self.assertEqual("running", state["status"])
        self.assertEqual({"order": 7}, state["input"])
        status = self.service.get_schedule("orders")["schedule"]
        self.assertEqual(execution_id, status["last_execution_id"])
        self.assertIsNotNone(status["last_fired_at"])

    def test_repeated_trigger_for_same_period_is_idempotent(self):
        first = self.service.pump_schedules(self.activation + 90)
        second = self.service.pump_schedules(self.activation + 95)
        self.assertEqual(1, len(first))
        self.assertEqual([], second)
        rows = self.service.store.connection.execute("SELECT id FROM executions").fetchall()
        self.assertEqual(1, len(rows))
        periods = self.service.store.connection.execute("SELECT * FROM schedule_periods").fetchall()
        self.assertEqual(1, len(periods))

    def test_created_execution_has_exactly_manual_shape(self):
        fired = self.service.pump_schedules(self.activation + 90)
        state = self.service.get_execution(fired[0]["execution_id"])
        self.assertEqual(
            sorted(state),
            sorted(
                [
                    "id",
                    "workflow_id",
                    "status",
                    "termination_reason",
                    "timeout_seconds",
                    "deadline_at",
                    "input",
                    "completed_nodes",
                    "skipped_nodes",
                    "failed_nodes",
                    "condition_results",
                    "outputs",
                    "attempts",
                    "loops",
                ]
            ),
        )
        events = self.service.events(fired[0]["execution_id"])
        self.assertEqual(["execution_started"], [event["type"] for event in events])

    def test_scheduled_execution_advances_like_a_manual_one(self):
        fired = self.service.pump_schedules(self.activation + 90)
        state = self.service.advance(fired[0]["execution_id"], {"output": {"done": True}}, "a1")
        self.assertEqual("completed", state["status"])
        self.assertEqual({"a": {"done": True}}, state["outputs"])
        self.assertTrue(self.service.replay(fired[0]["execution_id"])["consistent"])

    def test_schedule_history_records_the_trigger_only(self):
        fired = self.service.pump_schedules(self.activation + 90)
        history = self.service.schedule_events("orders")["events"]
        self.assertEqual(1, len(history))
        self.assertEqual(fired[0]["execution_id"], history[0]["execution_id"])
        self.assertEqual(self.activation + 60, history[0]["period_start"])
        self.assertEqual(1, history[0]["sequence"])
        self.assertIn("occurred_at", history[0])

    def test_schedule_history_is_empty_for_unscheduled_workflow(self):
        self.service.create_workflow({"id": "plain2", "nodes": NODES}, "w0")
        self.assertEqual({"events": []}, self.service.schedule_events("plain2"))

    def test_schedule_history_for_missing_workflow_is_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.schedule_events("ghost")

    def test_catch_up_creates_only_the_most_recent_missed_period(self):        # Periods at +60, +120, +180 elapsed without a pump; only one catch-up
        # execution, for the most recent period, is ever created.
        fired = self.service.pump_schedules(self.activation + 200)
        self.assertEqual(1, len(fired))
        self.assertEqual(self.activation + 180, fired[0]["period_start"])
        self.assertEqual([], self.service.pump_schedules(self.activation + 205))
        self.assertEqual(1, len(self.service.store.connection.execute("SELECT id FROM executions").fetchall()))

    def test_skip_policy_never_runs_missed_periods(self):
        self.service.create_workflow(
            {
                "id": "skipper",
                "nodes": NODES,
                "schedule": {"interval_seconds": 60, "input": {}, "misfire_policy": "skip"},
            },
            "w2",
        )
        activation = self.service.store.connection.execute(
            "SELECT activated_at FROM schedules WHERE workflow_id = 'skipper'"
        ).fetchone()["activated_at"]
        fired = [record for record in self.service.pump_schedules(activation + 200) if record["workflow_id"] == "skipper"]
        self.assertEqual([], fired)
        self.assertEqual(
            0,
            len(
                self.service.store.connection.execute(
                    "SELECT id FROM executions WHERE workflow_id = 'skipper'"
                ).fetchall()
            ),
        )
        # The anchor advanced past the skipped period but no trigger recorded.
        self.assertEqual(
            0,
            self.service.store.connection.execute(
                "SELECT COUNT(*) AS n FROM schedule_periods WHERE workflow_id = 'skipper'"
            ).fetchone()["n"],
        )


class PauseResumeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "test.db"), start_scheduler=False)
        self.service.create_workflow(
            {
                "id": "orders",
                "nodes": NODES,
                "schedule": {"interval_seconds": 60, "input": {}, "misfire_policy": "catch_up"},
            },
            "w1",
        )
        self.service.create_workflow({"id": "plain", "nodes": NODES}, "w2")

    def tearDown(self):
        self.service.close()
        self.directory.cleanup()

    def activation(self):
        return self.service.store.connection.execute(
            "SELECT activated_at FROM schedules WHERE workflow_id = 'orders'"
        ).fetchone()["activated_at"]

    def set_activation(self, value):
        self.service.store.connection.execute("UPDATE schedules SET activated_at = ?", (value,))

    def test_pause_blocks_firings(self):
        self.service.pause_schedule("orders", "p1")
        self.service.pump_schedules(time.time() + 3600)
        self.assertEqual(0, len(self.service.store.connection.execute("SELECT id FROM executions").fetchall()))
        self.assertTrue(self.service.get_schedule("orders")["schedule"]["paused"])

    def test_resume_with_catch_up_creates_one_recent_period(self):
        self.service.pause_schedule("orders", "p2")
        # Move the anchor far enough back that several periods elapsed during
        # the pause; resume must settle only the most recent one.
        self.set_activation(time.time() - 200)
        status = self.service.resume_schedule("orders", "r2")["schedule"]
        self.assertFalse(status["paused"])
        self.assertEqual(1, len(self.service.store.connection.execute("SELECT id FROM executions").fetchall()))

    def test_resume_with_skip_creates_nothing(self):
        self.service.create_workflow(
            {
                "id": "skipper",
                "nodes": NODES,
                "schedule": {"interval_seconds": 60, "input": {}, "misfire_policy": "skip"},
            },
            "w3",
        )
        self.service.pause_schedule("skipper", "p3")
        self.service.store.connection.execute("UPDATE schedules SET activated_at = ?", (time.time() - 200,))
        self.service.resume_schedule("skipper", "r3")
        self.assertEqual(
            0,
            len(
                self.service.store.connection.execute(
                    "SELECT id FROM executions WHERE workflow_id = 'skipper'"
                ).fetchall()
            ),
        )

    def test_pause_and_resume_without_schedule_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("plain", "p4")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("plain", "r4")

    def test_pause_and_resume_missing_workflow_are_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.pause_schedule("ghost", "p5")
        with self.assertRaises(NotFoundError):
            self.service.resume_schedule("ghost", "r5")

    def test_repeated_resume_does_not_double_fire(self):
        self.service.store.connection.execute("UPDATE schedules SET activated_at = ?", (time.time() - 200,))
        self.service.resume_schedule("orders", "r6")
        self.service.resume_schedule("orders", "r7")
        self.service.pump_schedules()
        self.assertEqual(1, len(self.service.store.connection.execute("SELECT id FROM executions").fetchall()))


class LiveSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "live.db"))
        self.service.create_workflow(
            {
                "id": "fast",
                "nodes": NODES,
                "schedule": {"interval_seconds": 1, "input": {"src": "timer"}, "misfire_policy": "catch_up"},
            },
            "w1",
        )

    def tearDown(self):
        self.service.close()
        self.directory.cleanup()

    def test_background_thread_fires_automatically(self):
        deadline = time.time() + 3
        execution_id = None
        while time.time() < deadline:
            execution_id = self.service.get_schedule("fast")["schedule"]["last_execution_id"]
            if execution_id:
                break
            time.sleep(0.05)
        self.assertIsNotNone(execution_id)
        state = self.service.get_execution(execution_id)
        self.assertEqual({"src": "timer"}, state["input"])


if __name__ == "__main__":
    unittest.main()
