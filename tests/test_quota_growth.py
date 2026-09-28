import http.client
import json
import math
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.server import Handler
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]

POLICY_BOTH = {
    "workflows": {"step": 3, "cap": 8},
    "executions": {"step": 20, "cap": 100},
}


class QuotaGrowthServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "growth.db"))

    def tearDown(self):
        self.directory.cleanup()

    def declare(self, body, key="q", tenant="alpha"):
        return self.service.declare_quota(body, key, tenant)

    def workflow(self, workflow_id, tenant="alpha", key=None):
        return self.service.create_workflow(
            {"id": workflow_id, "nodes": TASK}, key or workflow_id, tenant
        )

    def execution(self, execution_id, workflow_id="wf", tenant="alpha", key=None):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": workflow_id, "input": {}},
            key or execution_id,
            tenant,
        )

    def limit(self, kind="workflows", tenant="alpha"):
        return self.service.quota_status(tenant)["status"][kind]["limit"]

    # --- declaration and read ------------------------------------------

    def test_policy_is_declared_and_read_back_in_stable_key_order(self):
        result = self.declare(
            {"workflows": 2, "executions": 10, "growth": POLICY_BOTH}, key="g1"
        )
        self.assertEqual(
            {"quota": {"workflows": 2, "executions": 10, "growth": POLICY_BOTH}}, result
        )
        self.assertEqual(result, self.service.get_quota("alpha"))
        quota = result["quota"]
        self.assertEqual(["workflows", "executions", "growth"], list(quota))
        self.assertEqual(["workflows", "executions"], list(quota["growth"]))
        for entry in quota["growth"].values():
            self.assertEqual(["step", "cap"], list(entry))

    def test_policy_names_only_the_resources_it_covers(self):
        self.declare(
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 2, "cap": 5}}},
            key="g",
        )
        quota = self.service.get_quota("alpha")["quota"]
        self.assertEqual(["workflows"], list(quota["growth"]))
        # Workflows expand by their step; executions have no policy and stay hard-limited.
        self.workflow("w1")
        self.workflow("w2")  # workflows 1 -> 3
        self.assertEqual(3, self.limit("workflows"))
        self.execution("r1", workflow_id="w1")
        with self.assertRaises(ConflictError) as caught:
            self.execution("r2", workflow_id="w1")
        self.assertIn("quota", str(caught.exception))
        self.assertEqual(1, self.limit("executions"))

    def test_cap_equal_to_the_limit_is_accepted(self):
        result = self.declare(
            {"workflows": 5, "executions": 1, "growth": {"workflows": {"step": 1, "cap": 5}}},
            key="g",
        )
        self.assertEqual(
            {"quota": {"workflows": 5, "executions": 1,
                       "growth": {"workflows": {"step": 1, "cap": 5}}}},
            result,
        )

    # --- expansion ------------------------------------------------------

    def test_workflow_write_raises_the_limit_by_whole_steps_until_it_fits(self):
        self.declare(
            {"workflows": 2, "executions": 100, "growth": {"workflows": {"step": 2, "cap": 10}}},
            key="g",
        )
        self.workflow("w1")
        self.workflow("w2")
        self.assertEqual(2, self.limit())
        self.workflow("w3")  # held 2 at limit 2: the limit moves 2 -> 4
        self.assertEqual(4, self.limit())
        self.workflow("w4")
        self.workflow("w5")  # 4 -> 6
        self.assertEqual(
            {"limit": 6, "held": 5, "remaining": 1},
            self.service.quota_status("alpha")["status"]["workflows"],
        )
        self.workflow("w6")
        self.workflow("w7")  # 6 -> 8
        self.workflow("w8")
        self.workflow("w9")  # 8 -> 10 (the cap)
        self.workflow("w10")
        with self.assertRaises(ConflictError) as caught:
            self.workflow("w11")
        self.assertIn("quota", str(caught.exception))
        self.assertEqual(
            {"limit": 10, "held": 10, "remaining": 0},
            self.service.quota_status("alpha")["status"]["workflows"],
        )
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("w11", "alpha")

    def test_execution_write_raises_the_limit_by_whole_steps_until_the_cap(self):
        self.workflow("wf")
        self.declare(
            {"workflows": 100, "executions": 2, "growth": {"executions": {"step": 2, "cap": 6}}},
            key="g",
        )
        self.execution("r1")
        self.execution("r2")
        self.execution("r3")  # 2 -> 4
        self.assertEqual(4, self.limit("executions"))
        self.execution("r4")
        self.execution("r5")  # 4 -> 6
        self.execution("r6")
        with self.assertRaises(ConflictError):
            self.execution("r7")
        self.assertEqual(
            {"limit": 6, "held": 6, "remaining": 0},
            self.service.quota_status("alpha")["status"]["executions"],
        )
        with self.assertRaises(NotFoundError):
            self.service.get_execution("r7", "alpha")

    def test_cap_blocks_when_no_whole_step_raise_reaches_it(self):
        # limit 2, step 3, cap 9: the boundaries are 2 -> 5 -> 8, and the next
        # raise would be 11, past the cap even though the cap itself could
        # hold one more. The limit never moves by a partial step.
        self.declare(
            {"workflows": 2, "executions": 100, "growth": {"workflows": {"step": 3, "cap": 9}}},
            key="g",
        )
        self.workflow("w1")
        self.workflow("w2")
        self.workflow("w3")  # 2 -> 5
        self.workflow("w4")
        self.workflow("w5")
        self.workflow("w6")  # 5 -> 8
        self.workflow("w7")
        self.workflow("w8")
        with self.assertRaises(ConflictError) as caught:
            self.workflow("w9")
        self.assertIn("quota", str(caught.exception))
        self.assertEqual(8, self.limit())
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("w9", "alpha")

    def test_step_larger_than_cap_means_raise_never_happens(self):
        self.declare(
            {"workflows": 1, "executions": 100, "growth": {"workflows": {"step": 10, "cap": 5}}},
            key="g",
        )
        self.workflow("w1")
        with self.assertRaises(ConflictError):
            self.workflow("w2")
        self.assertEqual(1, self.limit())

    def test_replayed_write_returns_the_first_result_without_a_second_expansion(self):
        # A holding already sits at the limit, so the next write must expand.
        self.workflow("seed")
        self.declare(
            {"workflows": 1, "executions": 100, "growth": {"workflows": {"step": 5, "cap": 100}}},
            key="g",
        )
        first = self.workflow("grow", key="grow-key")
        self.assertEqual(6, self.limit())
        replay = self.workflow("grow", key="grow-key")
        self.assertEqual(first, replay)
        # The replay grew nothing a second time: same limit, two holdings.
        self.assertEqual(6, self.limit())
        self.assertEqual(2, self.service.quota_status("alpha")["status"]["workflows"]["held"])

    def test_expansion_commits_with_the_write_and_writes_no_metering_of_its_own(self):
        self.workflow("wf")
        self.declare(
            {"workflows": 100, "executions": 1, "growth": {"executions": {"step": 2, "cap": 3}}},
            key="g",
        )
        before = {entry["type"]: entry["count"] for entry in
                  self.service.usage("alpha", None, None, None, None)["usage"]}
        self.execution("r1", workflow_id="wf")
        self.execution("r2", workflow_id="wf")  # raises 1 -> 3
        self.execution("r3", workflow_id="wf")
        after = {entry["type"]: entry["count"] for entry in
                 self.service.usage("alpha", None, None, None, None)["usage"]}
        # The only new records are the three execution starts; the raised
        # limit added no record of any type.
        self.assertEqual(before.get("execution_started", 0) + 3, after.get("execution_started", 0))
        self.assertEqual(set(after), set(before) | {"execution_started"})
        # A further raise would pass the cap: the write is rejected and the
        # rejected request adds no metering record either.
        with self.assertRaises(ConflictError):
            self.execution("r4-blocked", workflow_id="wf")
        rejected = {entry["type"]: entry["count"] for entry in
                    self.service.usage("alpha", None, None, None, None)["usage"]}
        self.assertEqual(after, rejected)

    def test_duplicate_identifier_at_a_raised_limit_neither_expands_nor_loses_conflict(self):
        self.declare(
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 2, "cap": 5}}},
            key="g",
        )
        self.workflow("wf")
        self.execution("run", workflow_id="wf")
        # Both resources are exactly at their limits; repeats of existing ids
        # stay ordinary identifier conflicts and raise neither limit.
        with self.assertRaises(ConflictError) as caught:
            self.workflow("wf", key="wf-dup")
        self.assertNotIn("quota", str(caught.exception))
        with self.assertRaises(ConflictError) as caught:
            self.execution("run", workflow_id="wf", key="run-dup")
        self.assertNotIn("quota", str(caught.exception))
        self.assertEqual(1, self.limit("workflows"))
        self.assertEqual(1, self.limit("executions"))

    # --- replacement and deletion --------------------------------------

    def test_redeclaration_replaces_limits_and_policy_as_a_whole(self):
        self.declare(
            {"workflows": 1, "executions": 1, "growth": {
                "workflows": {"step": 2, "cap": 10},
                "executions": {"step": 2, "cap": 10},
            }},
            key="g1",
        )
        self.workflow("w1")
        self.workflow("w2")  # workflows 1 -> 3
        self.assertEqual(3, self.limit("workflows"))
        # New declaration resets the limit and leaves the policy on
        # executions only; the old workflow policy is gone.
        self.declare(
            {"workflows": 2, "executions": 1, "growth": {"executions": {"step": 3, "cap": 7}}},
            key="g2",
        )
        self.assertEqual(
            {"quota": {"workflows": 2, "executions": 1,
                       "growth": {"executions": {"step": 3, "cap": 7}}}},
            self.service.get_quota("alpha"),
        )
        with self.assertRaises(ConflictError):
            self.workflow("w3")
        self.assertEqual(2, self.limit("workflows"))
        self.execution("r1", workflow_id="w1")
        self.execution("r2", workflow_id="w1")  # executions 1 -> 4
        self.assertEqual(4, self.limit("executions"))

    def test_redeclaration_without_growth_drops_the_policy(self):
        self.declare(
            {"workflows": 1, "executions": 100, "growth": {"workflows": {"step": 2, "cap": 10}}},
            key="g1",
        )
        self.workflow("w1")
        self.workflow("w2")  # 1 -> 3
        self.declare({"workflows": 3, "executions": 100}, key="g2")
        self.assertEqual({"quota": {"workflows": 3, "executions": 100}},
                         self.service.get_quota("alpha"))
        self.workflow("w3")
        with self.assertRaises(ConflictError):
            self.workflow("w4")
        self.assertEqual(3, self.limit())

    def test_delete_removes_limits_and_policy_and_later_writes_run_unchecked(self):
        self.workflow("wf")
        self.declare(
            {"workflows": 1, "executions": 1, "growth": {
                "workflows": {"step": 2, "cap": 10},
                "executions": {"step": 2, "cap": 10},
            }},
            key="g",
        )
        self.assertEqual({"quota": None}, self.service.delete_quota({}, "d", "alpha"))
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))
        self.workflow("w1")
        self.workflow("w2")
        self.workflow("w3")
        self.execution("r1", workflow_id="wf")
        self.execution("r2", workflow_id="wf")
        # The unchecked writes never restore a declaration or a limit.
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))

    # --- validation -----------------------------------------------------

    def test_invalid_policies_are_validation_errors_with_zero_writes(self):
        bad_bodies = [
            {"workflows": 1, "executions": 1, "growth": {}},
            {"workflows": 1, "executions": 1, "growth": []},
            {"workflows": 1, "executions": 1, "growth": None},
            {"workflows": 1, "executions": 1, "growth": True},
            {"workflows": 1, "executions": 1,
             "growth": {"unknown": {"step": 1, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": "workflows"},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2}, "unknown": {"step": 1, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2, "extra": 3}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 1}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"workflows": []}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 0, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": -2, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1.5, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": True, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": "1", "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 0}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": -3}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2.0}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": False}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": None}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": math.inf, "cap": 2}}},
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": math.nan}}},
            # A cap below the resource's declared limit is a validation error.
            {"workflows": 5, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 4}}},
            {"workflows": 1, "executions": 5,
             "growth": {"executions": {"step": 1, "cap": 4}}},
            # Unknown top-level fields are still rejected alongside a policy.
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2}}, "extra": 1},
            # The baseline limits keep their own rules.
            {"workflows": 0, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 2}}},
        ]
        for index, body in enumerate(bad_bodies):
            with self.subTest(body=body):
                with self.assertRaises(ValidationError):
                    self.declare(body, f"bad-{index}")
        # Every rejection left no declaration behind.
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))

    def test_policy_declaration_requires_tenant_and_idempotency_key_without_leaking(self):
        body = {"workflows": 1, "executions": 1,
                "growth": {"workflows": {"step": 1, "cap": 2}}}
        with self.assertRaises(ValidationError):
            self.declare(body, "g", "")
        with self.assertRaises(ValidationError):
            self.declare(body, None, "alpha")
        with self.assertRaises(ValidationError):
            self.service.get_quota("")
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))

    def test_cross_operation_key_reuse_conflicts_and_touches_nothing(self):
        body = {"workflows": 1, "executions": 100,
                "growth": {"workflows": {"step": 2, "cap": 10}}}
        # A workflow-creation key reused on a declaration conflicts.
        self.workflow("wf", key="wf")
        with self.assertRaises(ConflictError):
            self.declare(body, key="wf")
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        # A declaration key reused on a workflow creation conflicts and grows nothing.
        self.declare(body, key="q")
        with self.assertRaises(ConflictError):
            self.workflow("wf2", key="q")
        self.assertEqual(1, self.limit())
        with self.assertRaises(NotFoundError):
            self.service.get_workflow("wf2", "alpha")

    # --- tenant isolation -----------------------------------------------

    def test_policy_and_limits_are_invisible_across_tenants(self):
        self.declare(
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 2, "cap": 5}}},
            key="qa", tenant="alpha",
        )
        self.assertEqual({"quota": None}, self.service.get_quota("beta"))
        self.assertEqual({"status": None}, self.service.quota_status("beta"))
        # Beta holds no quota, so its writes run unchecked and never expand alpha.
        self.workflow("w", tenant="beta", key="wb")
        self.workflow("w2", tenant="beta", key="wb2")
        # Alpha expands independently from its own write.
        self.workflow("wa1", tenant="alpha", key="wa1")
        self.workflow("wa2", tenant="alpha", key="wa2")
        self.assertEqual(3, self.limit(tenant="alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("beta"))

    # --- scheduled firing -----------------------------------------------

    def _schedule(self):
        return {
            "interval_seconds": 1, "input": {}, "missed_policy": "catch_up",
        }

    def _wait_for_period(self, execution_id):
        deadline = time.time() + 4
        status = self.service.schedule_status("wfs", "alpha")
        while status["last_execution_id"] != execution_id and time.time() < deadline:
            time.sleep(0.02)
            status = self.service.schedule_status("wfs", "alpha")
        self.assertEqual(execution_id, status["last_execution_id"])

    def test_scheduled_execution_expands_the_limit_when_the_policy_allows(self):
        self.declare(
            {"workflows": 100, "executions": 1, "growth": {"executions": {"step": 2, "cap": 10}}},
            key="g",
        )
        self.service.create_workflow(
            {"id": "wfs", "nodes": TASK, "schedule": self._schedule()},
            "wfs",
            "alpha",
        )
        # The first period fills the limit without expanding; pause before the
        # second period comes due so its settlement stays under our control.
        self._wait_for_period("wfs-scheduled-i:1")
        self.service.pause_schedule("wfs", {}, "pause-1", "alpha")
        time.sleep(1.1)
        # Resuming settles the due period synchronously; pausing again at once
        # keeps later periods from racing the assertions.
        self.service.resume_schedule("wfs", {}, "resume-1", "alpha")
        self.service.pause_schedule("wfs", {}, "pause-2", "alpha")
        self.assertEqual(
            {"limit": 3, "held": 2, "remaining": 1},
            self.service.quota_status("alpha")["status"]["executions"],
        )
        self.assertEqual("running", self.service.get_execution("wfs-scheduled-i:2", "alpha")["status"])

    def test_scheduled_execution_at_the_cap_creates_nothing_and_keeps_the_schedule(self):
        # step 1, cap 1: no raise is possible past the cap, so the first
        # period fires normally and every later period stays pending.
        self.declare(
            {"workflows": 100, "executions": 1, "growth": {"executions": {"step": 1, "cap": 1}}},
            key="g",
        )
        self.service.create_workflow(
            {"id": "wfs", "nodes": TASK, "schedule": self._schedule()},
            "wfs",
            "alpha",
        )
        self._wait_for_period("wfs-scheduled-i:1")
        self.service.pause_schedule("wfs", {}, "pause-1", "alpha")
        time.sleep(1.1)
        status = self.service.resume_schedule("wfs", {}, "resume-1", "alpha")
        # The blocked period created nothing, grew nothing, and left the
        # schedule anchored to the period that did fire.
        self.assertEqual("wfs-scheduled-i:1", status["last_execution_id"])
        self.assertEqual(1, self.limit("executions"))
        with self.assertRaises(NotFoundError):
            self.service.get_execution("wfs-scheduled-i:2", "alpha")


class QuotaGrowthMigrationTests(unittest.TestCase):
    def test_quota_table_created_before_growth_gains_nullable_policy_columns(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = str(Path(directory.name) / "old.db")
        connection = sqlite3.connect(path)
        connection.execute(
            "CREATE TABLE quotas (tenant TEXT PRIMARY KEY, workflows INTEGER NOT NULL, "
            "executions INTEGER NOT NULL, updated_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO quotas(tenant, workflows, executions, updated_at) "
            "VALUES ('alpha', 1, 5, '2026-01-01T00:00:00Z')"
        )
        connection.commit()
        connection.close()
        service = ChronicleFlow(path)
        # The migrated row reads exactly as before: no policy appears.
        self.assertEqual({"quota": {"workflows": 1, "executions": 5}},
                         service.get_quota("alpha"))
        service.create_workflow({"id": "w1", "nodes": TASK}, "w1", "alpha")
        with self.assertRaises(ConflictError) as caught:
            service.create_workflow({"id": "w2", "nodes": TASK}, "w2", "alpha")
        self.assertIn("quota", str(caught.exception))


class QuotaGrowthHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-growth.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, headers=None, raw=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = raw if raw is not None else (json.dumps(body) if body is not None else None)
        all_headers = {"Content-Type": "application/json"}
        if key is not None:
            all_headers["Idempotency-Key"] = key
        all_headers.update(headers or {})
        connection.request(method, path, payload, all_headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    def test_declaration_with_policy_renders_compact_stable_json(self):
        headers = {"X-Tenant-Id": "g-declare"}
        status, data = self.call(
            "PUT", "/quotas",
            {"workflows": 2, "executions": 10, "growth": POLICY_BOTH},
            key="q1", headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(
            b'{"quota":{"workflows":2,"executions":10,'
            b'"growth":{"workflows":{"step":3,"cap":8},'
            b'"executions":{"step":20,"cap":100}}}}\n',
            data,
        )
        status, again = self.call("GET", "/quotas", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(data, again)
        # POST is accepted just like PUT and replaces the declaration.
        status, data = self.call(
            "POST", "/quotas",
            {"workflows": 7, "executions": 70,
             "growth": {"executions": {"step": 5, "cap": 90}}},
            key="q2", headers=headers,
        )
        self.assertEqual(200, status)
        self.assertEqual(
            b'{"quota":{"workflows":7,"executions":70,'
            b'"growth":{"executions":{"step":5,"cap":90}}}}\n',
            data,
        )

    def test_declaration_without_policy_is_byte_for_byte_the_baseline_response(self):
        headers = {"X-Tenant-Id": "g-plain"}
        status, data = self.call(
            "PUT", "/quotas", {"workflows": 1, "executions": 2}, key="q", headers=headers
        )
        self.assertEqual(200, status)
        self.assertEqual(b'{"quota":{"workflows":1,"executions":2}}\n', data)
        status, data = self.call("GET", "/quotas", headers=headers)
        self.assertEqual(b'{"quota":{"workflows":1,"executions":2}}\n', data)

    def test_policy_validation_failures_are_400_and_write_nothing(self):
        headers = {"X-Tenant-Id": "g-bad"}
        raw_bodies = (
            b'{"workflows":1,"executions":1,"growth":{"workflows":{"step":0,"cap":2}}}',
            b'{"workflows":1,"executions":1,"growth":{"workflows":{"step":1,"cap":false}}}',
            b'{"workflows":1,"executions":1,"growth":{"workflows":{"step":1.5,"cap":2}}}',
            b'{"workflows":1,"executions":1,"growth":{"workflows":{"step":1}}}',
            b'{"workflows":1,"executions":1,"growth":{"nope":{"step":1,"cap":2}}}',
            b'{"workflows":5,"executions":1,"growth":{"workflows":{"step":1,"cap":4}}}',
            b'{"workflows":1,"executions":1,"growth":{}}',
            b'{"workflows":1,"executions":1,"growth":{"workflows":{"step":1e400,"cap":2}}}',
        )
        for index, raw in enumerate(raw_bodies):
            status, data = self.call("PUT", "/quotas", raw=raw, key=f"bad-{index}", headers=headers)
            self.assertEqual(400, status, raw)
            self.assertEqual("validation_error", json.loads(data)["error"]["code"], raw)
        status, data = self.call("GET", "/quotas", headers=headers)
        self.assertEqual(b'{"quota":null}\n', data)

    def test_policy_declaration_without_tenant_or_key_is_400_and_leaks_nothing(self):
        body = {"workflows": 1, "executions": 1,
                "growth": {"workflows": {"step": 1, "cap": 2}}}
        status, data = self.call("PUT", "/quotas", body, key="g")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        self.assertNotIn("status", data.decode())
        status, data = self.call(
            "PUT", "/quotas", body, headers={"X-Tenant-Id": "g-nokey"}
        )
        self.assertEqual(400, status)
        self.assertEqual("validation_error", json.loads(data)["error"]["code"])
        status, data = self.call("GET", "/quotas", headers={"X-Tenant-Id": "g-nokey"})
        self.assertEqual(b'{"quota":null}\n', data)

    def test_writes_expand_over_http_and_status_reports_the_raised_limit(self):
        headers = {"X-Tenant-Id": "g-run"}
        self.assertEqual(200, self.call(
            "PUT", "/quotas",
            {"workflows": 1, "executions": 100, "growth": {"workflows": {"step": 2, "cap": 4}}},
            key="q", headers=headers,
        )[0])
        self.assertEqual(201, self.call(
            "POST", "/workflows", {"id": "w1", "nodes": TASK}, key="w1", headers=headers
        )[0])
        self.assertEqual(201, self.call(
            "POST", "/workflows", {"id": "w2", "nodes": TASK}, key="w2", headers=headers
        )[0])
        self.assertEqual(201, self.call(
            "POST", "/workflows", {"id": "w3", "nodes": TASK}, key="w3", headers=headers
        )[0])
        status, data = self.call("GET", "/quotas/status", headers=headers)
        self.assertEqual(200, status)
        self.assertEqual(
            b'{"status":{"workflows":{"limit":3,"held":3,"remaining":0},'
            b'"executions":{"limit":100,"held":0,"remaining":100}}}\n',
            data,
        )
        # The next whole step would pass the cap: the write is a quota
        # conflict and the limit stays where it was.
        status, data = self.call(
            "POST", "/workflows", {"id": "w4", "nodes": TASK}, key="w4", headers=headers
        )
        self.assertEqual(409, status)
        self.assertIn("quota", json.loads(data)["error"]["message"])
        status, data = self.call("GET", "/quotas/status", headers=headers)
        self.assertIn(b'"limit":3,"held":3,"remaining":0', data)

    def test_replayed_write_does_not_expand_twice_over_http(self):
        headers = {"X-Tenant-Id": "g-replay"}
        self.call(
            "PUT", "/quotas",
            {"workflows": 1, "executions": 100, "growth": {"workflows": {"step": 4, "cap": 100}}},
            key="q", headers=headers,
        )
        # Fill the limit with a seed workflow so the next write must expand.
        self.assertEqual(201, self.call(
            "POST", "/workflows", {"id": "seed", "nodes": TASK}, key="seed", headers=headers
        )[0])
        first_status, first = self.call(
            "POST", "/workflows", {"id": "w", "nodes": TASK}, key="w", headers=headers
        )
        self.assertEqual(201, first_status)
        replay_status, replay = self.call(
            "POST", "/workflows", {"id": "w", "nodes": TASK}, key="w", headers=headers
        )
        self.assertEqual(201, replay_status)
        self.assertEqual(json.loads(first), json.loads(replay))
        status, data = self.call("GET", "/quotas/status", headers=headers)
        self.assertEqual(200, status)
        # One expansion (1 -> 5), two holdings; the replay changed neither.
        self.assertIn(b'"limit":5,"held":2,"remaining":3', data)


if __name__ == "__main__":
    unittest.main()
