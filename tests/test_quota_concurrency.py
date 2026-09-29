import http.client
import json
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

# Fan-out size for the barrier-synchronized concurrent operations.
FANOUT = 24


class QuotaConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.service = ChronicleFlow(str(Path(self.directory.name) / "concurrency.db"))

    def tearDown(self):
        self.directory.cleanup()

    def declare(self, body, key, tenant="alpha"):
        return self.service.declare_quota(body, key, tenant)

    def workflow(self, workflow_id, tenant="alpha", key=None):
        return self.service.create_workflow(
            {"id": workflow_id, "nodes": TASK}, key or workflow_id, tenant
        )

    def execution(self, execution_id, key=None, workflow_id="wf", tenant="alpha"):
        return self.service.create_execution(
            {"id": execution_id, "workflow_id": workflow_id, "input": {}},
            key or execution_id,
            tenant,
        )

    @staticmethod
    def _run_concurrently(target, count):
        barrier = threading.Barrier(count)
        results = [None] * count

        def worker(index):
            barrier.wait()
            try:
                results[index] = ("ok", target(index))
            except Exception as error:  # noqa: BLE001 - each branch asserts on the captured error
                results[index] = ("error", error)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    # --- whole-declaration visibility -----------------------------------

    def test_concurrent_declarations_leave_exactly_one_whole_declaration(self):
        bodies = [
            {"workflows": 10, "executions": 100,
             "growth": {"workflows": {"step": 2, "cap": 20},
                        "executions": {"step": 20, "cap": 200}}},
            {"workflows": 5, "executions": 50},
            {"workflows": 7, "executions": 70,
             "growth": {"executions": {"step": 5, "cap": 90}}},
        ]
        results = self._run_concurrently(
            lambda i: self.declare(bodies[i % len(bodies)], f"d{i}"), FANOUT
        )
        # Every declaration succeeds; each response is itself a whole declaration.
        for kind, value in results:
            self.assertEqual("ok", kind, value)
            quota = value["quota"]
            self.assertTrue(any(
                quota["workflows"] == body["workflows"]
                and quota["executions"] == body["executions"]
                and quota.get("growth") == body.get("growth")
                for body in bodies
            ))
        # The stored row is one of the submitted declarations, never a mix:
        # each cap stays paired with its own step and its own limit.
        final = self.service.get_quota("alpha")["quota"]
        matching = next(body for body in bodies
                        if body["workflows"] == final["workflows"]
                        and body["executions"] == final["executions"])
        self.assertEqual(matching.get("growth"), final.get("growth"))
        for kind in ("workflows", "executions"):
            if "growth" in final and kind in final["growth"]:
                self.assertGreaterEqual(final["growth"][kind]["cap"], final[kind])

    def test_readers_see_only_a_complete_declaration_while_writers_churn(self):
        bodies = [
            {"workflows": 11, "executions": 110,
             "growth": {"workflows": {"step": 1, "cap": 11},
                        "executions": {"step": 10, "cap": 110}}},
            {"workflows": 22, "executions": 220},
            {"workflows": 33, "executions": 330,
             "growth": {"executions": {"step": 30, "cap": 330}}},
        ]
        stop = threading.Event()
        problems = []

        def reader():
            while not stop.is_set():
                quota = self.service.get_quota("alpha")["quota"]
                if quota is None:
                    continue
                keys = set(quota)
                if keys not in ({"workflows", "executions"},
                                {"workflows", "executions", "growth"}):
                    problems.append(("keys", quota))
                    return
                if not any(
                    quota["workflows"] == body["workflows"]
                    and quota["executions"] == body["executions"]
                    and quota.get("growth") == body.get("growth")
                    for body in bodies
                ):
                    problems.append(("mixed", quota))
                    return
                for kind, entry in quota.get("growth", {}).items():
                    if set(entry) != {"step", "cap"} or entry["cap"] < quota[kind]:
                        problems.append(("policy", quota))
                        return

        def writer():
            index = 0
            while not stop.is_set():
                self.declare(bodies[index % len(bodies)], f"churn-{index}")
                # A delete in the mix means readers must also accept the definite
                # empty result, but never a half-cleared declaration.
                self.service.delete_quota({}, f"churn-del-{index}", "alpha")
                index += 1

        readers = [threading.Thread(target=reader) for _ in range(6)]
        writer_thread = threading.Thread(target=writer)
        for thread in readers:
            thread.start()
        writer_thread.start()
        time.sleep(1.0)
        stop.set()
        writer_thread.join()
        for thread in readers:
            thread.join()
        self.assertEqual([], problems)
        # The delete was the last write of every churn cycle; the final read is
        # the definite empty result and status lists neither resource.
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))

    def test_declare_racing_delete_ends_whole_or_empty(self):
        new = {"workflows": 9, "executions": 90,
               "growth": {"workflows": {"step": 1, "cap": 9}}}
        mixed = 0
        for roundno in range(40):
            self.declare({"workflows": 1, "executions": 1}, f"init-{roundno}")
            barrier = threading.Barrier(2)
            outcomes = {}

            def declare():
                barrier.wait()
                outcomes["declare"] = self.declare(new, f"new-{roundno}")

            def delete():
                barrier.wait()
                outcomes["delete"] = self.service.delete_quota({}, f"del-{roundno}", "alpha")

            t1 = threading.Thread(target=declare)
            t2 = threading.Thread(target=delete)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.assertEqual({"quota": new}, outcomes["declare"])
            self.assertEqual({"quota": None}, outcomes["delete"])
            final = self.service.get_quota("alpha")["quota"]
            if final is not None and final != new:
                mixed += 1
        self.assertEqual(0, mixed)

    # --- idempotency under concurrency -----------------------------------

    def test_same_key_duplicate_declarations_take_effect_once(self):
        body = {"workflows": 3, "executions": 30,
                "growth": {"workflows": {"step": 3, "cap": 30}}}
        results = self._run_concurrently(
            lambda i: self.service.declare_quota(body, "shared-key", "alpha"), FANOUT
        )
        responses = [value for kind, value in results if kind == "ok"]
        self.assertEqual(FANOUT, len(responses))
        first = responses[0]
        for response in responses:
            self.assertEqual(first, response)
        self.assertEqual(first, self.service.get_quota("alpha"))

    def test_same_key_duplicate_deletes_take_effect_once(self):
        self.declare({"workflows": 1, "executions": 1}, "q")
        self.workflow("kept")
        results = self._run_concurrently(
            lambda i: self.service.delete_quota({}, "shared-delete", "alpha"), FANOUT
        )
        responses = [value for kind, value in results if kind == "ok"]
        self.assertEqual(FANOUT, len(responses))
        self.assertTrue(all(response == {"quota": None} for response in responses))
        self.assertEqual({"quota": None}, self.service.get_quota("alpha"))
        self.assertEqual({"status": None}, self.service.quota_status("alpha"))
        # The delete removed no held data.
        self.assertIsNotNone(self.service.get_workflow("kept", "alpha"))

    def test_same_key_duplicate_growth_write_expands_exactly_once(self):
        self.workflow("wf")
        self.declare({"workflows": 100, "executions": 1,
                      "growth": {"executions": {"step": 5, "cap": 100}}}, "q")
        # Fill the one slot first so the duplicate write must grow the limit.
        self.execution("seed", key="seed")
        results = self._run_concurrently(
            lambda i: self.execution("grow", key="grow-key"), FANOUT
        )
        responses = [value for kind, value in results if kind == "ok"]
        self.assertEqual(FANOUT, len(responses))
        first = responses[0]
        for response in responses:
            self.assertEqual(first, response)
        status = self.service.quota_status("alpha")["status"]["executions"]
        # One expansion (1 -> 6) admitted the grow; the duplicates replayed it
        # and changed neither the limit nor the holding (two executions total).
        self.assertEqual({"limit": 6, "held": 2, "remaining": 4}, status)

    def test_cross_operation_key_reuse_concurrently_conflicts_and_touches_nothing(self):
        for roundno in range(20):
            self.declare({"workflows": 1, "executions": 1}, f"base-{roundno}")
            barrier = threading.Barrier(2)
            captured = {}

            def declare():
                barrier.wait()
                try:
                    captured["declare"] = ("ok", self.declare(
                        {"workflows": 8, "executions": 80}, f"k-{roundno}"))
                except ConflictError as error:
                    captured["declare"] = ("conflict", str(error))

            def delete():
                barrier.wait()
                try:
                    captured["delete"] = ("ok", self.service.delete_quota(
                        {}, f"k-{roundno}", "alpha"))
                except ConflictError as error:
                    captured["delete"] = ("conflict", str(error))

            t1 = threading.Thread(target=declare)
            t2 = threading.Thread(target=delete)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            outcomes = [captured["declare"][0], captured["delete"][0]]
            # Exactly one side takes effect; the other is a conflict, regardless
            # of which wins the race.
            self.assertEqual(sorted(outcomes), ["conflict", "ok"])
            final = self.service.get_quota("alpha")["quota"]
            if captured["declare"][0] == "ok":
                self.assertEqual({"workflows": 8, "executions": 80}, final)
            else:
                self.assertIsNone(final)

    # --- growth under concurrency ----------------------------------------

    def test_concurrent_writes_grow_to_the_serial_equivalent_limit(self):
        self.workflow("wf")
        limit, step, cap, writes = 2, 7, 100, 30
        self.declare({"workflows": 100, "executions": limit,
                      "growth": {"executions": {"step": step, "cap": cap}}}, "q")
        results = self._run_concurrently(
            lambda i: self.execution(f"r{i}", key=f"r{i}"), writes
        )
        self.assertTrue(all(kind == "ok" for kind, _ in results))
        status = self.service.quota_status("alpha")["status"]["executions"]
        self.assertEqual(writes, status["held"])
        # The smallest number of whole steps that admits the same writes serially.
        expected = limit + ((writes - limit + step - 1) // step) * step
        self.assertEqual(expected, status["limit"])
        self.assertEqual(expected - writes, status["remaining"])
        # The limit stayed on the declaration's step grid and under the cap.
        self.assertEqual(0, (status["limit"] - limit) % step)
        self.assertLessEqual(status["limit"], cap)

    def test_concurrent_writes_at_the_cap_admit_only_whole_steps_and_then_conflict(self):
        self.workflow("wf")
        limit, step, cap, writes = 2, 7, 23, FANOUT
        self.declare({"workflows": 100, "executions": limit,
                      "growth": {"executions": {"step": step, "cap": cap}}}, "q")
        results = self._run_concurrently(
            lambda i: self.execution(f"r{i}", key=f"r{i}"), writes
        )
        admitted = [value for kind, value in results if kind == "ok"]
        rejected = [error for kind, error in results if kind == "error"]
        # Grid points from 2: 2, 9, 16, 23; the next whole step (30) passes the
        # cap, so at most 23 holdings are admitted and every later write loses.
        self.assertEqual(cap, len(admitted))
        self.assertEqual(writes - cap, len(rejected))
        for error in rejected:
            self.assertIsInstance(error, ConflictError)
            self.assertIn("quota", str(error))
        status = self.service.quota_status("alpha")["status"]["executions"]
        self.assertEqual({"limit": cap, "held": cap, "remaining": 0}, status)
        # A rejected write inserted nothing and left the limit untouched.
        for index, (kind, value) in enumerate(results):
            if kind == "error":
                with self.assertRaises(NotFoundError):
                    self.service.get_execution(f"r{index}", "alpha")
        self.assertEqual(cap, self.service.get_quota("alpha")["quota"]["executions"])

    def test_growth_never_splices_into_a_concurrent_redeclaration(self):
        for roundno in range(30):
            tenant = f"t{roundno}"
            self.workflow("wf", tenant=tenant, key=f"wf{roundno}")
            self.service.declare_quota(
                {"workflows": 100, "executions": 1,
                 "growth": {"executions": {"step": 1, "cap": 5}}},
                f"old-{roundno}", tenant,
            )
            barrier = threading.Barrier(2)

            def grow():
                barrier.wait()
                try:
                    self.service.create_execution(
                        {"id": "r1", "workflow_id": "wf", "input": {}},
                        f"r1-{roundno}", tenant,
                    )
                except ConflictError:
                    pass

            def redeclare():
                barrier.wait()
                self.service.declare_quota(
                    {"workflows": 100, "executions": 1,
                     "growth": {"executions": {"step": 10, "cap": 100}}},
                    f"new-{roundno}", tenant,
                )

            t1 = threading.Thread(target=grow)
            t2 = threading.Thread(target=redeclare)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            quota = self.service.get_quota(tenant)["quota"]
            # The redeclaration always commits and replaces every column as a
            # whole, so the surviving policy is exactly the new one. A splice
            # (the grow's raised limit paired with the new step/cap, or the new
            # declaration paired with the old policy) must never appear.
            self.assertEqual(
                {"workflows": 100, "executions": 1,
                 "growth": {"executions": {"step": 10, "cap": 100}}},
                quota,
            )
            # The one growth write either was reset by the declaration or ran
            # against the new limit without needing a raise; either way the
            # holding exists and the limit is the declared 1, never 2/11.
            self.assertIsNotNone(self.service.get_execution("r1", tenant))

    def test_rejected_writes_under_concurrency_drift_no_limit_and_write_nothing(self):
        self.workflow("wf")
        self.declare({"workflows": 100, "executions": 1}, "q")
        results = self._run_concurrently(
            lambda i: self.execution(f"r{i}", key=f"r{i}"), FANOUT
        )
        admitted = sum(1 for kind, _ in results if kind == "ok")
        self.assertEqual(1, admitted)
        for kind, value in results:
            if kind == "error":
                self.assertIsInstance(value, ConflictError)
                self.assertIn("quota", str(value))
        # The hard limit never moved and only one execution exists.
        self.assertEqual(
            {"limit": 1, "held": 1, "remaining": 0},
            self.service.quota_status("alpha")["status"]["executions"],
        )
        usage = {entry["type"]: entry["count"]
                 for entry in self.service.usage("alpha")["usage"]}
        self.assertEqual(1, usage.get("execution_started"))

    def test_validation_failures_under_concurrency_write_nothing(self):
        self.workflow("wf")
        self.declare({"workflows": 100, "executions": 2,
                      "growth": {"executions": {"step": 2, "cap": 10}}}, "q")
        bad_bodies = [
            {"workflows": 0, "executions": 1},
            {"workflows": 1, "executions": 1, "growth": {"workflows": {"step": 0, "cap": 2}}},
            {"workflows": 1, "executions": 1, "growth": {"nope": {"step": 1, "cap": 2}}},
            {"workflows": 1, "executions": 1, "extra": 1},
        ]

        def mixed(index):
            if index % 2 == 0:
                # Invalid declarations must reject with zero writes.
                with self.assertRaises(ValidationError):
                    self.declare(bad_bodies[(index // 2) % len(bad_bodies)], f"bad-{index}")
                return "rejected"
            # Valid growth writes interleaved with the rejected declarations;
            # the last two may legitimately hit the cap and conflict.
            try:
                self.execution(f"ok-{index}", key=f"ok-{index}")
                return "admitted"
            except ConflictError:
                return "quota-conflict"

        results = self._run_concurrently(mixed, FANOUT)
        self.assertTrue(all(kind == "ok" for kind, _ in results), results)
        quota = self.service.get_quota("alpha")["quota"]
        # The rejected declarations changed neither limits nor policy; only the
        # whole-step growth (2 -> 4 ... up to the cap) moved the execution limit.
        self.assertEqual(100, quota["workflows"])
        self.assertEqual({"step": 2, "cap": 10}, quota["growth"]["executions"])
        self.assertIn(quota["executions"], (2, 4, 6, 8, 10))

    # --- tenant isolation under concurrency ------------------------------

    def test_tenants_never_share_limits_or_growth_under_concurrency(self):
        for tenant in ("alpha", "beta"):
            self.declare({"workflows": 5, "executions": 100,
                          "growth": {"workflows": {"step": 5, "cap": 50}}},
                         f"q-{tenant}", tenant)

        def work(index):
            tenant = "alpha" if index % 2 == 0 else "beta"
            self.workflow(f"w{index // 2}", tenant=tenant, key=f"w{index}")

        self._run_concurrently(work, FANOUT)
        for tenant in ("alpha", "beta"):
            status = self.service.quota_status(tenant)["status"]["workflows"]
            # Each tenant ran 12 creates: 5 fit, the rest grew 5 -> 10 -> 15.
            self.assertEqual(12, status["held"])
            self.assertEqual(15, status["limit"])
            self.assertEqual(3, status["remaining"])
        # A third tenant still sees the definite empty result.
        self.assertEqual({"quota": None}, self.service.get_quota("gamma"))
        self.assertEqual({"status": None}, self.service.quota_status("gamma"))

    def test_scheduled_firing_concurrent_with_manual_writes_stays_on_the_grid(self):
        self.declare({"workflows": 100, "executions": 1,
                      "growth": {"executions": {"step": 1, "cap": 200}}}, "q")
        self.service.create_workflow(
            {"id": "wfs", "nodes": TASK,
             "schedule": {"interval_seconds": 1, "input": {}, "missed_policy": "catch_up"}},
            "wfs", "alpha",
        )
        # Give the first period time to fire and grow to 1.
        deadline = time.time() + 4
        while self.service.quota_status("alpha")["status"]["executions"]["held"] < 1 \
                and time.time() < deadline:
            time.sleep(0.02)
        barrier = threading.Barrier(8)

        def manual(index):
            barrier.wait()
            try:
                self.execution(f"manual-{index}", key=f"manual-{index}", workflow_id="wfs")
            except ConflictError:
                pass

        threads = [threading.Thread(target=manual, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        time.sleep(1.2)
        status = self.service.quota_status("alpha")["status"]["executions"]
        # However the scheduler and the manual writes interleave, the limit
        # admits every holding on a whole-step grid under the cap, with
        # remaining exactly limit minus held.
        self.assertLessEqual(status["held"], status["limit"])
        self.assertEqual(status["limit"] - status["held"], status["remaining"])
        self.assertLessEqual(status["limit"], 200)
        self.assertEqual(0, (status["limit"] - 1) % 1)


class QuotaConcurrencyHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        Handler.service = ChronicleFlow(str(Path(cls.directory.name) / "http-concurrency.db"))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.directory.cleanup()

    def call(self, method, path, body=None, key=None, tenant=None):
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        if tenant is not None:
            headers["X-Tenant-Id"] = tenant
        payload = json.dumps(body) if body is not None else None
        # The development server may refuse a brand-new connection during a
        # tight fan-out; an idempotent retry is exactly what the key is for.
        for attempt in range(5):
            connection = http.client.HTTPConnection("127.0.0.1", self.port)
            try:
                connection.request(method, path, payload, headers)
                response = connection.getresponse()
                data = response.read()
                return response.status, json.loads(data)
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(0.02)
            finally:
                connection.close()
        self.fail("unreachable")

    def test_duplicate_declaration_key_over_real_concurrent_connections(self):
        body = {"workflows": 4, "executions": 40,
                "growth": {"workflows": {"step": 2, "cap": 8}}}
        barrier = threading.Barrier(FANOUT)
        results = []

        def declare():
            barrier.wait()
            results.append(self.call("PUT", "/quotas", body, key="http-shared", tenant="http"))

        threads = [threading.Thread(target=declare) for _ in range(FANOUT)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertTrue(all(status == 200 for status, _ in results))
        payloads = {json.dumps(data, sort_keys=True) for _, data in results}
        self.assertEqual(1, len(payloads))
        status, data = self.call("GET", "/quotas", tenant="http")
        self.assertEqual(200, status)
        self.assertEqual(results[0][1], data)

    def test_cross_operation_key_reuse_over_http_conflicts_without_touching_quota(self):
        barrier = threading.Barrier(2)
        captured = {}

        def declare():
            barrier.wait()
            captured["declare"] = self.call(
                "PUT", "/quotas", {"workflows": 6, "executions": 60},
                key="http-cross", tenant="http-cross",
            )

        def delete():
            barrier.wait()
            captured["delete"] = self.call(
                "DELETE", "/quotas", {}, key="http-cross", tenant="http-cross",
            )

        t1 = threading.Thread(target=declare)
        t2 = threading.Thread(target=delete)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        codes = {name: result[0] for name, result in captured.items()}
        self.assertEqual(sorted(codes.values()), [200, 409])
        for result in captured.values():
            if result[0] == 409:
                self.assertEqual("conflict", result[1]["error"]["code"])
        status, data = self.call("GET", "/quotas", tenant="http-cross")
        self.assertEqual(200, status)
        if captured["declare"][0] == 200:
            self.assertEqual({"workflows": 6, "executions": 60}, data["quota"])
        else:
            self.assertIsNone(data["quota"])


if __name__ == "__main__":
    unittest.main()
