import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile

from chronicleflow.errors import ConflictError, NotFoundError, ValidationError
from chronicleflow.service import ChronicleFlow

TASK = [{"id": "a", "kind": "task", "depends_on": []}]


class QuotaConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "concurrency.db")
        self.service = ChronicleFlow(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def declare(self, body, key, tenant="t"):
        return self.service.declare_quota(body, key, tenant)

    def create_workflow(self, wid, key, tenant="t"):
        return self.service.create_workflow({"id": wid, "nodes": TASK}, key, tenant)

    def create_execution(self, eid, wf, key, tenant="t"):
        return self.service.create_execution(
            {"id": eid, "workflow_id": wf, "input": {}}, key, tenant
        )

    def _run(self, target, count):
        barrier = threading.Barrier(count)

        def wrapped(index):
            barrier.wait()
            return target(index)

        with ThreadPoolExecutor(max_workers=count) as pool:
            return list(pool.map(wrapped, range(count)))

    # --- growth under concurrency --------------------------------------

    def test_concurrent_growth_writes_lose_no_raise_and_meet_the_serial_bound(self):
        # limit 2, step 3, a cap far above the load: the final limit is the
        # serial result 2 + ceil((n-2)/3)*3 regardless of completion order.
        n = 20
        self.declare(
            {"workflows": 2, "executions": 1000,
             "growth": {"workflows": {"step": 3, "cap": 1000}}},
            "q",
        )

        def write(index):
            self.create_workflow(f"w{index}", f"k{index}")

        failures = [error for error in self._run(write, n) if isinstance(error, Exception)]
        self.assertEqual([], failures)
        status = self.service.quota_status("t")["status"]["workflows"]
        self.assertEqual(n, status["held"])
        self.assertEqual(2 + ((n - 2 + 2) // 3) * 3, status["limit"])
        self.assertEqual(status["limit"] - n, status["remaining"])
        # 20 holdings walk the whole steps 2 -> 5 -> ... -> 20 exactly.
        self.assertEqual(20, status["limit"])

    def test_concurrent_execution_growth_raises_in_whole_steps_within_the_cap(self):
        self.create_workflow("wf", "wf")
        self.declare(
            {"workflows": 1000, "executions": 2,
             "growth": {"executions": {"step": 4, "cap": 14}}},
            "q",
        )
        # Serial steps from 2 are 2 -> 6 -> 10 -> 14; twelve concurrent
        # executions all fit within the raised cap and none is rejected.
        outcomes = self._run(lambda i: self._attempt(lambda: self.create_execution(f"r{i}", "wf", f"k{i}")), 12)
        self.assertEqual(12, sum(1 for ok, _ in outcomes if ok))
        for ok, error in outcomes:
            if not ok:
                self.assertIn("quota", str(error))
        status = self.service.quota_status("t")["status"]["executions"]
        self.assertEqual({"limit": 14, "held": 12, "remaining": 2}, status)

    def test_concurrent_execution_growth_rejects_past_the_cap_without_drift(self):
        self.create_workflow("wf", "wf")
        self.declare(
            {"workflows": 1000, "executions": 2,
             "growth": {"executions": {"step": 4, "cap": 10}}},
            "q",
        )
        # Steps from 2 reach 6 then 10 (the cap); a 13th execution cannot be
        # admitted by any whole step within the cap, so it fails and the limit
        # stays at 10.
        outcomes = self._run(lambda i: self._attempt(lambda: self.create_execution(f"r{i}", "wf", f"k{i}")), 13)
        admitted = sum(1 for ok, _ in outcomes if ok)
        self.assertEqual(10, admitted)
        for ok, error in outcomes:
            if not ok:
                self.assertIn("quota", str(error))
        status = self.service.quota_status("t")["status"]["executions"]
        self.assertEqual({"limit": 10, "held": 10, "remaining": 0}, status)

    def test_concurrent_writes_past_the_cap_reject_without_drifting_the_limit(self):
        # Boundaries 2 -> 5 -> 8; the next whole step would be 11 past the
        # cap 9, so the 9th holding is rejected. Concurrency changes none of it.
        n = 14
        self.declare(
            {"workflows": 2, "executions": 1000,
             "growth": {"workflows": {"step": 3, "cap": 9}}},
            "q",
        )
        outcomes = self._run(lambda i: self._attempt(lambda: self.create_workflow(f"w{i}", f"k{i}")), n)
        admitted = sum(1 for ok, _ in outcomes if ok)
        self.assertEqual(8, admitted)
        for ok, error in outcomes:
            if not ok:
                self.assertIn("quota", str(error))
        status = self.service.quota_status("t")["status"]["workflows"]
        self.assertEqual({"limit": 8, "held": 8, "remaining": 0}, status)
        # Exactly the rejected requests inserted nothing; which indices lost
        # depends on completion order, so take them from the ordered outcomes.
        rejected_indices = [index for index, (ok, _) in enumerate(outcomes) if not ok]
        self.assertEqual(n - 8, len(rejected_indices))
        for index in rejected_indices:
            with self.assertRaises(NotFoundError):
                self.service.get_workflow(f"w{index}", "t")

    def test_concurrent_writes_without_a_policy_keep_a_hard_limit_and_zero_drift(self):
        n = 10
        self.declare({"workflows": 3, "executions": 1000}, "q")
        outcomes = self._run(lambda i: self._attempt(lambda: self.create_workflow(f"w{i}", f"k{i}")), n)
        self.assertEqual(3, sum(1 for ok, _ in outcomes if ok))
        for ok, error in outcomes:
            if not ok:
                self.assertIn("quota", str(error))
        self.assertEqual(
            {"limit": 3, "held": 3, "remaining": 0},
            self.service.quota_status("t")["status"]["workflows"],
        )

    def _attempt(self, action):
        try:
            return True, action()
        except ConflictError as error:
            return False, error

    # --- declaration and delete interleaving ---------------------------

    def test_concurrent_declarations_are_only_ever_read_complete(self):
        bodies = [
            {"workflows": 2, "executions": 200},
            {"workflows": 3, "executions": 300,
             "growth": {"workflows": {"step": 2, "cap": 9},
                        "executions": {"step": 10, "cap": 400}}},
            {"workflows": 4, "executions": 400,
             "growth": {"executions": {"step": 1, "cap": 400}}},
        ]
        stop = threading.Event()
        seen_bad = []

        def reader():
            while not stop.is_set():
                # Each read is its own request and must return one complete
                # declaration; two back-to-back reads may straddle a commit and
                # therefore name different complete declarations.
                for read in (lambda: self.service.get_quota("t"),
                             lambda: self.service.quota_status("t")):
                    try:
                        result = read()
                    except Exception as error:  # pragma: no cover - a read must never fail
                        seen_bad.append(("raised", repr(error)))
                        continue
                    if "quota" in result:
                        quota = result["quota"]
                        keys = set(quota) if quota else set()
                        if keys not in (set(), {"workflows", "executions"},
                                        {"workflows", "executions", "growth"}):
                            seen_bad.append(("shape", quota))
                        if quota and "growth" in quota:
                            for kind, entry in quota["growth"].items():
                                if list(entry) != ["step", "cap"] or entry["cap"] < quota[kind]:
                                    seen_bad.append(("policy", quota))
                    else:
                        status = result["status"]
                        if status is not None:
                            for kind in ("workflows", "executions"):
                                entry = status[kind]
                                if list(entry) != ["limit", "held", "remaining"]:
                                    seen_bad.append(("status-keys", status))
                                elif entry["remaining"] != entry["limit"] - entry["held"]:
                                    seen_bad.append(("remaining", status))

        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        rounds = 25

        def declare(index):
            body = bodies[index % len(bodies)]
            for round_index in range(rounds):
                try:
                    self.service.declare_quota(body, f"d{index}-{round_index}", "t")
                except ConflictError:
                    pass

        self._run(declare, len(bodies))
        stop.set()
        reader_thread.join()
        self.assertEqual([], seen_bad)
        # The surviving declaration is exactly one of the complete inputs.
        final = self.service.get_quota("t")["quota"]
        self.assertIn(final, bodies)

    def test_delete_versus_declare_ends_in_a_whole_declaration_or_a_whole_empty(self):
        self.create_workflow("kept", "kept")
        self.declare(
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 1, "cap": 5}}},
            "seed",
        )
        body = {"workflows": 7, "executions": 70,
                "growth": {"executions": {"step": 5, "cap": 90}}}

        def mix(index):
            for round_index in range(20):
                try:
                    if index % 2 == 0:
                        self.service.declare_quota(body, f"a-{index}-{round_index}", "t")
                    else:
                        self.service.delete_quota({}, f"x-{index}-{round_index}", "t")
                except ConflictError:
                    pass

        self._run(mix, 6)
        quota = self.service.get_quota("t")
        status = self.service.quota_status("t")
        if quota["quota"] is None:
            self.assertEqual({"status": None}, status)
        else:
            self.assertEqual(body, quota["quota"])
            self.assertIsNotNone(status["status"])
        # The delete never touched held workflows, which stay fully usable.
        self.service.get_workflow("kept", "t")
        self.create_execution("kept-run", "kept", "kept-run")

    def test_delete_leaves_definite_empty_reads_and_keeps_every_holding(self):
        self.create_workflow("w1", "w1")
        self.create_workflow("w2", "w2")
        self.declare(
            {"workflows": 1, "executions": 1,
             "growth": {"workflows": {"step": 2, "cap": 10},
                        "executions": {"step": 2, "cap": 10}}},
            "q",
        )
        # A delete and a re-declare of the same quota race; then delete wins the
        # last word on a second pass.
        self.service.delete_quota({}, "del", "t")
        self.assertEqual({"quota": None}, self.service.get_quota("t"))
        self.assertEqual({"status": None}, self.service.quota_status("t"))
        # Repeating the read and delete (same key) stays the definite empty
        # result and performs no second removal.
        self.assertEqual({"quota": None}, self.service.delete_quota({}, "del", "t"))
        self.assertEqual({"quota": None}, self.service.get_quota("t"))
        self.service.get_workflow("w1", "t")
        self.service.get_workflow("w2", "t")
        # Later writes run without a quota check and never restore a limit.
        self.create_workflow("w3", "w3")
        self.assertEqual({"quota": None}, self.service.get_quota("t"))
        self.assertEqual({"status": None}, self.service.quota_status("t"))

    # --- idempotency under concurrency ---------------------------------

    def test_concurrent_duplicate_key_grows_once_and_returns_one_result(self):
        self.declare(
            {"workflows": 1, "executions": 1000,
             "growth": {"workflows": {"step": 3, "cap": 90}}},
            "q", "u",
        )
        self.create_workflow("seed", "seed", "u")  # holding at the limit
        results = []
        lock = threading.Lock()

        def duplicate(_index):
            result = self.service.create_workflow(
                {"id": "once", "nodes": TASK}, "SAME", "u"
            )
            with lock:
                results.append(result)

        self._run(duplicate, 8)
        first = results[0]
        self.assertTrue(all(result == first for result in results))
        status = self.service.quota_status("u")["status"]["workflows"]
        # One holding added and one whole-step raise (1 -> 4), never more.
        self.assertEqual({"limit": 4, "held": 2, "remaining": 2}, status)

    def test_concurrent_duplicate_declaration_and_delete_keys_act_once(self):
        body = {"workflows": 5, "executions": 50}

        def declare(_index):
            return self.service.declare_quota(body, "DUP", "v")

        declarations = self._run(declare, 6)
        self.assertTrue(all(result == declarations[0] for result in declarations))
        self.assertEqual({"quota": body}, self.service.get_quota("v"))

        def delete(_index):
            return self.service.delete_quota({}, "DUP-DEL", "v")

        deletions = self._run(delete, 6)
        self.assertTrue(all(result == {"quota": None} for result in deletions))
        self.assertEqual({"quota": None}, self.service.get_quota("v"))

    def test_cross_operation_key_reuse_under_races_leaves_quota_data_intact(self):
        for round_index in range(20):
            tenant = f"r{round_index}"
            self.declare({"workflows": 1, "executions": 1000}, f"base-{round_index}", tenant)
            winner = {}
            start = threading.Barrier(2)

            def declare_with_key():
                start.wait()
                try:
                    self.service.declare_quota(
                        {"workflows": 9, "executions": 900}, f"KEY-{round_index}", tenant
                    )
                    winner["declaration"] = True
                except ConflictError:
                    winner.setdefault("conflicts", []).append("declare")

            def create_with_key():
                start.wait()
                try:
                    self.create_workflow("wf", f"KEY-{round_index}", tenant)
                    winner["creation"] = True
                except ConflictError:
                    winner.setdefault("conflicts", []).append("create")

            threads = [threading.Thread(target=declare_with_key),
                       threading.Thread(target=create_with_key)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(1, len(winner.get("conflicts", [])), winner)
            quota = self.service.get_quota(tenant)["quota"]
            if "creation" in winner:
                # The workflow write won: the declaration stayed at its limits.
                self.assertEqual({"workflows": 1, "executions": 1000}, quota)
                self.service.get_workflow("wf", tenant)
            else:
                # The declaration won: no workflow was created, limits replaced.
                self.assertEqual({"workflows": 9, "executions": 900}, quota)
                with self.assertRaises(NotFoundError):
                    self.service.get_workflow("wf", tenant)

    # --- validation, rejection, and isolation under concurrency --------

    def test_invalid_declarations_under_concurrency_write_nothing(self):
        self.declare({"workflows": 4, "executions": 40}, "good")
        good = {"workflows": 4, "executions": 40}
        bad_bodies = [
            {"workflows": 0, "executions": 40},
            {"workflows": 4, "executions": -1},
            {"workflows": 4, "executions": 40, "growth": {}},
            {"workflows": 4, "executions": 40, "growth": {"workflows": {"step": 1, "cap": 3}}},
            {"workflows": 4, "executions": 40, "extra": 1},
            {"workflows": 4.0, "executions": 40},
        ]

        def attempt(index):
            body = bad_bodies[index % len(bad_bodies)]
            try:
                self.service.declare_quota(body, f"bad-{index}", "t")
                return "accepted"
            except ValidationError:
                return "rejected"

        results = self._run(attempt, len(bad_bodies) * 3)
        self.assertTrue(all(result == "rejected" for result in results))
        self.assertEqual({"quota": good}, self.service.get_quota("t"))

    def test_concurrent_tenants_never_share_limits_or_expansions(self):
        self.declare(
            {"workflows": 1, "executions": 1000,
             "growth": {"workflows": {"step": 1, "cap": 50}}},
            "qa", "alpha",
        )

        def alpha_write(index):
            self.create_workflow(f"a{index}", f"ka{index}", "alpha")

        def beta_write(index):
            # Beta holds no quota: its concurrent writes run unchecked and
            # must never expand alpha's limit.
            self.create_workflow(f"b{index}", f"kb{index}", "beta")

        count = 12
        barrier = threading.Barrier(count * 2)

        def go(tenant, index):
            barrier.wait()
            if tenant == "alpha":
                alpha_write(index)
            else:
                beta_write(index)

        with ThreadPoolExecutor(max_workers=count * 2) as pool:
            futures = []
            for index in range(count):
                futures.append(pool.submit(go, "alpha", index))
                futures.append(pool.submit(go, "beta", index))
            for future in futures:
                future.result()

        # Alpha grew strictly from its own 12 holdings; beta still has no quota.
        self.assertEqual(count, self.service.quota_status("alpha")["status"]["workflows"]["held"])
        self.assertEqual(count, self.service.quota_status("alpha")["status"]["workflows"]["limit"])
        self.assertEqual({"quota": None}, self.service.get_quota("beta"))
        self.assertEqual({"status": None}, self.service.quota_status("beta"))


class QuotaConcurrencyMultiConnectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "multi.db")
        self.first = ChronicleFlow(self.path)
        # A second service opens its own connection against the same file.
        self.second = ChronicleFlow(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def test_growth_writes_across_two_connections_serialize_without_busy_errors(self):
        self.first.declare_quota(
            {"workflows": 1, "executions": 1000,
             "growth": {"workflows": {"step": 2, "cap": 100}}},
            "q", "t",
        )
        count = 24
        barrier = threading.Barrier(count)
        errors = []

        def write(index):
            barrier.wait()
            service = self.first if index % 2 == 0 else self.second
            try:
                service.create_workflow(
                    {"id": f"w{index}", "nodes": TASK}, f"k{index}", "t"
                )
            except Exception as error:  # pragma: no cover - SQLITE_BUSY would land here
                errors.append(repr(error))

        threads = [threading.Thread(target=write, args=(i,)) for i in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([], errors)
        status = self.first.quota_status("t")["status"]["workflows"]
        self.assertEqual(count, status["held"])
        # Serial whole-step ceiling from limit 1 with step 2 for 24 holdings
        # is 25; the concurrent result never exceeds that serial bound.
        self.assertEqual(25, status["limit"])
        self.assertEqual(1, status["remaining"])


if __name__ == "__main__":
    unittest.main()
