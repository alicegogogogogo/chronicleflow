from __future__ import annotations

import json
import math
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _finite_json, _identifier
from .notify import NOTIFY_EVENT_TYPES, parse_subscriptions
from .schedule import Cron, parse_schedule
from .store import Store


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return "other"


def _evaluate_condition(node: Node, input_data: Any) -> bool:
    current = input_data
    for segment in node.path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return False
        current = current[segment]
    return _json_type(current) == _json_type(node.equals) and current == node.equals


def _new_iteration() -> dict[str, Any]:
    return {"completed_nodes": [], "skipped_nodes": [], "condition_results": {}, "outputs": {}, "attempts": {}}


def _new_loop_state() -> dict[str, Any]:
    return {"status": "pending", "current_iteration": 0, "iterations": [], "end_reason": None}


DEFAULT_LEASE_SECONDS = 30.0

# How often the background scheduler looks for due schedules.
SCHEDULER_TICK_SECONDS = 0.05


class ChronicleFlow:
    def __init__(self, database: str):
        self.store = Store(database)
        # Per-thread notification state: events appended inside an operation
        # are buffered and delivered only after the operation commits.
        self._local = threading.local()
        self._scheduler = threading.Thread(target=self._scheduler_loop, daemon=True, name="chronicleflow-scheduler")
        self._scheduler.start()

    def _scheduler_loop(self) -> None:
        """Fire due schedules in the background; a failing pass never stops the loop."""
        while True:
            try:
                with self._operation():
                    with self.store.transaction():
                        self._process_schedules()
            except Exception:
                pass
            time.sleep(SCHEDULER_TICK_SECONDS)

    def _idempotent(self, key: str | None, operation: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        if not key:
            raise ValidationError("Idempotency-Key header is required")
        with self.store.transaction() as connection:
            existing = connection.execute("SELECT operation, response FROM idempotency WHERE key = ?", (key,)).fetchone()
            if existing:
                if existing["operation"] != operation:
                    raise ConflictError("idempotency key was already used for another operation")
                return self.store.decode(existing["response"])
            response = action()
            connection.execute(
                "INSERT INTO idempotency(key, operation, response) VALUES (?, ?, ?)",
                (key, operation, self.store.encode(response)),
            )
            return response

    @contextmanager
    def _operation(self) -> Iterator[None]:
        """Scope a public operation so buffered notifications drain once, after commit."""
        depth = getattr(self._local, "depth", 0)
        self._local.depth = depth + 1
        failed = True
        try:
            yield
            failed = False
        finally:
            self._local.depth = depth
            if depth == 0:
                pending = getattr(self._local, "pending", [])
                self._local.pending = []
                # A failed operation rolls its events back, so nothing is
                # delivered for it; a delivery problem never reaches the caller.
                if not failed:
                    for notice in pending:
                        try:
                            self._deliver_notice(notice)
                        except Exception:
                            pass

    def _subscriptions_for(self, execution_id: str) -> list[tuple[str, dict[str, Any]]]:
        """Return (label, subscription) pairs for an execution, workflow first."""
        row = self.store.connection.execute("SELECT workflow_id FROM executions WHERE id = ?", (execution_id,)).fetchone()
        if not row:
            return []
        pairs: list[tuple[str, dict[str, Any]]] = []
        for owner_type, owner_id in (("workflow", row["workflow_id"]), ("execution", execution_id)):
            rows = self.store.connection.execute(
                "SELECT position, document FROM subscriptions WHERE owner_type = ? AND owner_id = ? ORDER BY position",
                (owner_type, owner_id),
            ).fetchall()
            for sub_row in rows:
                pairs.append((f"{owner_type}:{sub_row['position']}", self.store.decode(sub_row["document"])))
        return pairs

    def _deliver_notice(self, notice: dict[str, Any]) -> None:
        for label, subscription in self._subscriptions_for(notice["execution_id"]):
            if notice["type"] not in subscription["events"]:
                continue
            # The idempotency key is deterministic per event and subscription:
            # retries of this delivery reuse it, other events never share it.
            key = f"{notice['execution_id']}:{notice['sequence']}:{label}"
            record = self._attempt_delivery(subscription, notice, key)
            with self.store.transaction() as connection:
                sequence_row = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM deliveries WHERE execution_id = ?",
                    (notice["execution_id"],),
                ).fetchone()
                connection.execute(
                    "INSERT INTO deliveries(execution_id, sequence, document) VALUES (?, ?, ?)",
                    (notice["execution_id"], sequence_row["sequence"], self.store.encode(record)),
                )

    def _attempt_delivery(self, subscription: dict[str, Any], notice: dict[str, Any], key: str) -> dict[str, Any]:
        message = {"event_type": notice["type"], "execution_id": notice["execution_id"], **notice["payload"]}
        body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
        tries: list[dict[str, Any]] = []
        status = "failed"
        for attempt in range(1, subscription["max_attempts"] + 1):
            if attempt > 1:
                # Increasing backoff between attempts of the same delivery.
                time.sleep(min(0.1 * (2 ** (attempt - 2)), 1.0))
            request = urllib.request.Request(
                subscription["url"],
                data=body,
                headers={"Content-Type": "application/json", "Idempotency-Key": key},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=subscription["timeout_seconds"]) as response:
                    tries.append({"attempt": attempt, "status_code": response.status})
                status = "delivered"
                break
            except urllib.error.HTTPError as error:
                tries.append({"attempt": attempt, "status_code": error.code})
            except Exception as error:
                reason = getattr(error, "reason", error)
                tries.append({"attempt": attempt, "error": str(reason)})
        return {
            "url": subscription["url"],
            "event_type": notice["type"],
            "event_sequence": notice["sequence"],
            "idempotency_key": key,
            "attempts": tries,
            "attempt_count": len(tries),
            "status": status,
            "occurred_at": self.store.now(),
        }

    def deliveries(self, execution_id: str) -> dict[str, Any]:
        self.get_execution(execution_id)
        rows = self.store.connection.execute(
            "SELECT sequence, document FROM deliveries WHERE execution_id = ? ORDER BY sequence",
            (execution_id,),
        ).fetchall()
        return {"deliveries": [{"sequence": row["sequence"], **self.store.decode(row["document"])} for row in rows]}

    def create_workflow(self, raw: Any, key: str | None) -> dict[str, Any]:
        subscriptions = None
        schedule = None
        if isinstance(raw, dict):
            # Subscriptions and the schedule are validated up front so an
            # invalid declaration rejects the whole request before anything
            # is written.
            if "subscriptions" in raw:
                subscriptions = parse_subscriptions(raw["subscriptions"])
                raw = {field: value for field, value in raw.items() if field != "subscriptions"}
            if "schedule" in raw:
                schedule = parse_schedule(raw["schedule"])
                raw = {field: value for field, value in raw.items() if field != "schedule"}
        workflow = Workflow.parse(raw)

        def create() -> dict[str, Any]:
            try:
                self.store.connection.execute(
                    "INSERT INTO workflows(id, document) VALUES (?, ?)",
                    (workflow.id, self.store.encode(workflow.as_dict())),
                )
            except Exception as error:
                if "UNIQUE constraint" in str(error):
                    raise ConflictError(f"workflow {workflow.id} already exists") from error
                raise
            for position, subscription in enumerate(subscriptions or []):
                self.store.connection.execute(
                    "INSERT INTO subscriptions(owner_type, owner_id, position, document) VALUES (?, ?, ?, ?)",
                    ("workflow", workflow.id, position, self.store.encode(subscription)),
                )
            if schedule is not None:
                self._insert_schedule(workflow.id, schedule)
            return workflow.as_dict()

        return self._idempotent(key, f"create-workflow:{workflow.id}", create)

    def _insert_schedule(self, workflow_id: str, schedule: dict[str, Any]) -> None:
        """Attach a fresh schedule declaration to a workflow; it is due from now on."""
        self.store.connection.execute(
            "INSERT INTO schedules(workflow_id, document, paused, anchor_at, cursor) VALUES (?, ?, 0, ?, '') "
            "ON CONFLICT(workflow_id) DO UPDATE SET document = excluded.document, "
            "anchor_at = excluded.anchor_at, cursor = '', last_triggered_at = NULL, last_execution_id = NULL",
            (workflow_id, self.store.encode(schedule), time.time()),
        )
        # A new declaration starts a fresh period lineage, so trigger records
        # of a previous plan never make a new period look already fired.
        self.store.connection.execute("DELETE FROM schedule_triggers WHERE workflow_id = ?", (workflow_id,))

    def create_execution(self, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or not {"id", "workflow_id", "input"} <= set(raw) <= {
            "id",
            "workflow_id",
            "input",
            "timeout_seconds",
            "subscriptions",
        }:
            raise ValidationError(
                "execution must contain exactly id, workflow_id, input, and optionally timeout_seconds and subscriptions"
            )
        execution_id = _identifier(raw["id"], "execution id")
        workflow_id = _identifier(raw["workflow_id"], "workflow id")
        if not isinstance(raw["input"], dict):
            raise ValidationError("input must be an object")
        _finite_json(raw["input"], "input")
        subscriptions = parse_subscriptions(raw["subscriptions"]) if "subscriptions" in raw else None
        timeout = raw.get("timeout_seconds")
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise ValidationError("timeout_seconds must be a positive number of seconds")
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValidationError("timeout_seconds must be a positive number of seconds")

        def create() -> dict[str, Any]:
            return self._insert_execution(execution_id, workflow_id, raw["input"], timeout, subscriptions)

        return self._idempotent(key, f"create-execution:{execution_id}", create)

    def _insert_execution(
        self,
        execution_id: str,
        workflow_id: str,
        input_data: dict[str, Any],
        timeout: float | None,
        subscriptions: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        """Insert a running execution and its start event; caller holds a transaction."""
        workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
        if not workflow_row:
            raise NotFoundError(f"workflow {workflow_id} was not found")
        workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
        loops = {node.id: _new_loop_state() for node in workflow.nodes if node.kind == "loop"}
        has_approvals = any(node.approval is not None for node in workflow.nodes)
        deadline = time.time() + timeout if timeout is not None else None
        state = {
            "id": execution_id,
            "workflow_id": workflow_id,
            "status": "running",
            "termination_reason": None,
            "timeout_seconds": timeout,
            "deadline_at": deadline,
            "input": input_data,
            "completed_nodes": [],
            "skipped_nodes": [],
            "failed_nodes": [],
            "condition_results": {},
            "outputs": {},
            "attempts": {},
            "loops": loops,
        }
        # Approval state exists only for workflows that declare approval
        # points, so every other execution keeps its baseline state shape.
        if has_approvals:
            state["waiting_approval"] = None
            state["approvals"] = []
        try:
            self.store.connection.execute(
                "INSERT INTO executions(id, workflow_id, state) VALUES (?, ?, ?)",
                (execution_id, workflow_id, self.store.encode(state)),
            )
        except Exception as error:
            if "UNIQUE constraint" in str(error):
                raise ConflictError(f"execution {execution_id} already exists") from error
            raise
        for position, subscription in enumerate(subscriptions or []):
            self.store.connection.execute(
                "INSERT INTO subscriptions(owner_type, owner_id, position, document) VALUES (?, ?, ?, ?)",
                ("execution", execution_id, position, self.store.encode(subscription)),
            )
        started_payload: dict[str, Any] = {
            "workflow_id": workflow_id,
            "input": input_data,
            "loops": loops,
            "timeout_seconds": timeout,
            "deadline_at": deadline,
        }
        if has_approvals:
            started_payload["waiting_approval"] = None
            started_payload["approvals"] = []
        self._append(
            execution_id,
            "execution_started",
            started_payload,
        )
        return state

    def get_execution(self, execution_id: str) -> dict[str, Any]:
        with self._operation():
            row = self.store.connection.execute("SELECT state FROM executions WHERE id = ?", (execution_id,)).fetchone()
            if not row:
                raise NotFoundError(f"execution {execution_id} was not found")
            state = self.store.decode(row["state"])
            self._maybe_timeout(execution_id, state)
            return state

    def _maybe_timeout(self, execution_id: str, state: dict[str, Any]) -> None:
        deadline = state.get("deadline_at")
        if state["status"] == "running" and deadline is not None and time.time() >= deadline:
            self._terminate(execution_id, state, "timeout")
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))

    def _terminate(self, execution_id: str, state: dict[str, Any], reason: str, extra: dict[str, Any] | None = None) -> None:
        state["status"] = "terminated"
        state["termination_reason"] = reason
        # A termination (rejection, timeout, or cancellation) dismisses any
        # approval point the execution was parked at; its request event remains
        # in the stream as the record of the wait.
        if "waiting_approval" in state:
            state["waiting_approval"] = None
        payload = {"reason": reason}
        if extra:
            payload.update(extra)
        self._append(execution_id, "execution_terminated", payload)

    def events(self, execution_id: str) -> list[dict[str, Any]]:
        self.get_execution(execution_id)
        rows = self.store.connection.execute(
            "SELECT sequence, type, payload, occurred_at FROM events WHERE execution_id = ? ORDER BY sequence",
            (execution_id,),
        ).fetchall()
        return [
            {"sequence": row["sequence"], "type": row["type"], "payload": self.store.decode(row["payload"]), "occurred_at": row["occurred_at"]}
            for row in rows
        ]

    def advance(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) not in (
            {"output"},
            {"failure"},
            {"output", "worker_id"},
            {"failure", "worker_id"},
        ):
            raise ValidationError("advance body must contain exactly an output object or a failure object")
        worker_id = raw.get("worker_id")
        if worker_id is not None:
            worker_id = _identifier(worker_id, "worker id")
        if "output" in raw:
            if not isinstance(raw["output"], dict):
                raise ValidationError("advance output must be an object")
            _finite_json(raw["output"], "output")
        else:
            failure = raw["failure"]
            if not isinstance(failure, dict) or set(failure) != {"reason"} or not isinstance(failure["reason"], str):
                raise ValidationError("advance failure must contain exactly a reason string")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            self._assert_submission_allowed(execution_id, worker_id)
            # Parked at an approval point: the decision operation is the only
            # way forward, so an advance absorbs its output or failure and
            # returns the current state unchanged. Lease ownership is still
            # enforced, exactly as for any other running-execution submission.
            if state.get("waiting_approval") is not None:
                return state
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            self._auto_process(execution_id, workflow, state)
            if state["status"] == "running":
                ready = self._ready_tasks(workflow, state)
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id = ready[0]
                node = next(node for node in workflow.nodes if node.id == node_id)
                if node.approval is not None:
                    self._request_approval(execution_id, workflow, state, node)
                elif "failure" in raw:
                    self._fail_node(execution_id, workflow, state, node_id, raw["failure"]["reason"])
                else:
                    self._complete_node(execution_id, workflow, state, node_id, raw["output"])
                if state["status"] == "running" and state.get("waiting_approval") is None:
                    self._auto_process(execution_id, workflow, state)
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            # Every path from a running start that reaches here settled at least
            # one node boundary (completed, failed, or parked at an approval),
            # so checkpoint it in the same transaction.
            self._write_checkpoint(execution_id, state)
            return state

        with self._operation():
            return self._idempotent(key, f"advance:{execution_id}", apply)

    def decision(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or not {"approver", "decision"} <= set(raw) <= {"approver", "decision", "output", "reason"}:
            raise ValidationError("decision body must contain approver and decision, plus output or reason")
        approver = raw["approver"]
        if not isinstance(approver, str):
            raise ValidationError("approver must be a string")
        verdict = raw["decision"]
        if verdict not in ("approved", "rejected"):
            raise ValidationError("decision must be \"approved\" or \"rejected\"")
        if verdict == "approved":
            if set(raw) != {"approver", "decision", "output"}:
                raise ValidationError("an approved decision must contain exactly approver, decision, and output")
            if not isinstance(raw["output"], dict):
                raise ValidationError("decision output must be an object")
            _finite_json(raw["output"], "output")
            output = raw["output"]
            reason = None
        else:
            if set(raw) != {"approver", "decision", "reason"}:
                raise ValidationError("a rejected decision must contain exactly approver, decision, and reason")
            if not isinstance(raw["reason"], str):
                raise ValidationError("decision reason must be a string")
            output = None
            reason = raw["reason"]

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            waiting = state.get("waiting_approval")
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            if waiting is None:
                # A duplicate of the decision that already resolved the most
                # recent approval point returns that first result; it neither
                # advances the node again nor appends another event. Any other
                # decision against an execution with no pending point conflicts.
                if self._is_repeated_decision(state, approver, verdict, output, reason):
                    return state
                raise ConflictError("execution has no pending approval decision")
            if approver not in waiting["approvers"]:
                raise ConflictError(f"approver {approver} is not allowed to decide this approval point")
            node_id = waiting["node_id"]
            context = self._approval_context(waiting)
            state["waiting_approval"] = None
            state["approvals"].append(
                {"node_id": node_id, "approver": approver, "decision": verdict, "reason": reason, **context}
            )
            payload: dict[str, Any] = {"node_id": node_id, "approver": approver, "decision": verdict, **context}
            if verdict == "rejected":
                payload["reason"] = reason
            self._append(execution_id, "approval_decided", payload)
            if verdict == "approved":
                self._complete_node(execution_id, workflow, state, node_id, output)
                if state["status"] == "running":
                    self._auto_process(execution_id, workflow, state)
            else:
                state["failed_nodes"].append(node_id)
                self._terminate(execution_id, state, "rejected", {"node_id": node_id})
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            # The decision settles the parked node boundary: completion on
            # approval, permanent failure on rejection.
            self._write_checkpoint(execution_id, state)
            return state

        with self._operation():
            return self._idempotent(key, f"decision:{execution_id}", apply)

    @staticmethod
    def _is_repeated_decision(
        state: dict[str, Any], approver: str, verdict: str, output: Any, reason: str | None
    ) -> bool:
        records = state.get("approvals") or []
        if not records:
            return False
        record = records[-1]
        if record["approver"] != approver or record["decision"] != verdict:
            return False
        if verdict == "rejected":
            return record["reason"] == reason
        return ChronicleFlow._recorded_output(state, record) == output

    @staticmethod
    def _recorded_output(state: dict[str, Any], record: dict[str, Any]) -> Any:
        if "loop_id" in record:
            iteration = state["loops"][record["loop_id"]]["iterations"][record["iteration"] - 1]
            return iteration["outputs"].get(record["node_id"])
        return state["outputs"].get(record["node_id"])

    @staticmethod
    def _approval_context(waiting: dict[str, Any]) -> dict[str, Any]:
        return {key: waiting[key] for key in ("loop_id", "iteration") if key in waiting}

    def _request_approval(self, execution_id: str, workflow: Workflow, state: dict[str, Any], node: Node) -> None:
        waiting: dict[str, Any] = {"node_id": node.id, "approvers": list(node.approval.approvers)}
        loop_id = self._active_loop(workflow, state, node.id)
        if loop_id is not None:
            waiting["loop_id"] = loop_id
            waiting["iteration"] = state["loops"][loop_id]["current_iteration"]
        state["waiting_approval"] = waiting
        self._append(
            execution_id,
            "approval_requested",
            {"node_id": node.id, "approvers": list(node.approval.approvers), **self._approval_context(waiting)},
        )

    def cancel(self, execution_id: str, key: str | None) -> dict[str, Any]:
        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            self._terminate(execution_id, state, "cancelled")
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        with self._operation():
            return self._idempotent(key, f"cancel:{execution_id}", apply)

    def _lease_row(self, execution_id: str) -> Any:
        return self.store.connection.execute(
            "SELECT worker_id, lease_seconds, expires_at, heartbeat_at FROM leases WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()

    @staticmethod
    def _lease_payload(worker_id: str, lease_seconds: float, expires_at: float, heartbeat_at: float) -> dict[str, Any]:
        return {
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
            "expires_at": expires_at,
            "heartbeat_at": heartbeat_at,
        }

    def _assert_submission_allowed(self, execution_id: str, worker_id: str | None) -> None:
        """Once a work item is claimed, only the active lease holder may submit results."""
        row = self._lease_row(execution_id)
        if row is None:
            return
        if time.time() >= row["expires_at"]:
            raise ConflictError(f"lease for execution {execution_id} has expired")
        if worker_id != row["worker_id"]:
            raise ConflictError(f"work item for execution {execution_id} is held by another worker")

    def claim(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or "worker_id" not in raw or not set(raw) <= {"worker_id", "lease_seconds"}:
            raise ValidationError("claim body must contain a worker_id and optionally lease_seconds")
        worker_id = _identifier(raw["worker_id"], "worker id")
        lease_seconds = raw.get("lease_seconds", DEFAULT_LEASE_SECONDS)
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float)):
            raise ValidationError("lease_seconds must be a positive number of seconds")
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValidationError("lease_seconds must be a positive number of seconds")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                # A finished execution has no claimable work item; the request
                # is a definite empty result and absorbs no input.
                return {"work_item": None, "lease": None}
            now = time.time()
            row = self._lease_row(execution_id)
            if row is not None and now < row["expires_at"]:
                raise ConflictError(f"work item for execution {execution_id} is already claimed")
            expires_at = now + lease_seconds
            self.store.connection.execute(
                "INSERT INTO leases(execution_id, worker_id, lease_seconds, expires_at, heartbeat_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(execution_id) DO UPDATE SET worker_id = excluded.worker_id, "
                "lease_seconds = excluded.lease_seconds, expires_at = excluded.expires_at, heartbeat_at = excluded.heartbeat_at",
                (execution_id, worker_id, lease_seconds, expires_at, now),
            )
            return {
                "work_item": {"execution_id": execution_id, "workflow_id": state["workflow_id"]},
                "lease": self._lease_payload(worker_id, lease_seconds, expires_at, now),
            }

        return self._idempotent(key, f"claim:{execution_id}", apply)

    def heartbeat(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"worker_id"}:
            raise ValidationError("heartbeat body must contain exactly a worker_id")
        worker_id = _identifier(raw["worker_id"], "worker id")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            row = self._lease_row(execution_id)
            if row is None:
                raise NotFoundError(f"execution {execution_id} has no claimed work item")
            now = time.time()
            if state["status"] != "running":
                raise ConflictError(f"execution {execution_id} is not running")
            if now >= row["expires_at"]:
                raise ConflictError(f"lease for execution {execution_id} has expired")
            if row["worker_id"] != worker_id:
                raise ConflictError(f"work item for execution {execution_id} is held by another worker")
            # A heartbeat only extends the lease and refreshes the active time;
            # it never advances nodes, writes outputs, or appends node events.
            expires_at = now + row["lease_seconds"]
            self.store.connection.execute(
                "UPDATE leases SET expires_at = ?, heartbeat_at = ? WHERE execution_id = ?",
                (expires_at, now, execution_id),
            )
            return {"lease": self._lease_payload(worker_id, row["lease_seconds"], expires_at, now)}

        return self._idempotent(key, f"heartbeat:{execution_id}", apply)

    def release(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"worker_id"}:
            raise ValidationError("release body must contain exactly a worker_id")
        worker_id = _identifier(raw["worker_id"], "worker id")

        def apply() -> dict[str, Any]:
            self.get_execution(execution_id)
            row = self._lease_row(execution_id)
            if row is None:
                raise NotFoundError(f"execution {execution_id} has no claimed work item")
            if time.time() >= row["expires_at"]:
                raise ConflictError(f"lease for execution {execution_id} has expired")
            if row["worker_id"] != worker_id:
                raise ConflictError(f"work item for execution {execution_id} is held by another worker")
            self.store.connection.execute("DELETE FROM leases WHERE execution_id = ?", (execution_id,))
            return {"released": True}

        return self._idempotent(key, f"release:{execution_id}", apply)

    def checkpoints(self, execution_id: str) -> dict[str, Any]:
        self.get_execution(execution_id)
        rows = self.store.connection.execute(
            "SELECT sequence, event_sequence, document, created_at FROM checkpoints WHERE execution_id = ? ORDER BY sequence",
            (execution_id,),
        ).fetchall()
        return {
            "checkpoints": [
                {
                    "sequence": row["sequence"],
                    "event_sequence": row["event_sequence"],
                    "state": self.store.decode(row["document"])["state"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def recover(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"from"} or not isinstance(raw["from"], str):
            raise ValidationError("recover body must contain exactly a from string")
        if raw["from"] != "latest_checkpoint":
            raise ValidationError("recover from must be \"latest_checkpoint\"")

        def apply() -> dict[str, Any]:
            # get_execution applies a due timeout first, so termination always
            # takes precedence over recovery.
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            row = self.store.connection.execute(
                "SELECT document FROM checkpoints WHERE execution_id = ? ORDER BY sequence DESC LIMIT 1",
                (execution_id,),
            ).fetchone()
            if not row:
                raise ConflictError("execution has no checkpoint to recover from")
            try:
                document = self.store.decode(row["document"])
                snapshot = document["state"]
                event_sequence = document["event_sequence"]
            except (ValueError, KeyError, TypeError) as error:
                raise ConflictError("latest checkpoint is not parseable") from error
            if not isinstance(snapshot, dict) or not isinstance(event_sequence, int):
                raise ConflictError("latest checkpoint is not parseable")
            if snapshot != state:
                raise ConflictError("latest checkpoint does not match the materialized state")
            return snapshot

        return self._idempotent(key, f"recover:{execution_id}", apply)

    # --- schedules ------------------------------------------------------

    def _schedule_row(self, workflow_id: str) -> Any:
        return self.store.connection.execute(
            "SELECT workflow_id, document, paused, anchor_at, cursor, last_triggered_at, last_execution_id "
            "FROM schedules WHERE workflow_id = ?",
            (workflow_id,),
        ).fetchone()

    def _assert_workflow_exists(self, workflow_id: str) -> None:
        row = self.store.connection.execute("SELECT 1 FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
        if not row:
            raise NotFoundError(f"workflow {workflow_id} was not found")

    def _schedule_status(self, row: Any) -> dict[str, Any]:
        if row is None:
            # A workflow without a declared schedule has a definite empty result.
            return {"schedule": None}
        return {
            "schedule": self.store.decode(row["document"]),
            "paused": bool(row["paused"]),
            "last_triggered_at": row["last_triggered_at"],
            "last_execution_id": row["last_execution_id"],
        }

    def schedule_status(self, workflow_id: str) -> dict[str, Any]:
        with self._operation():
            with self.store.transaction():
                self._assert_workflow_exists(workflow_id)
                # Settle any due periods first so the answer reflects the
                # schedule as of now, not as of the last background tick.
                self._process_schedules(workflow_id)
                return self._schedule_status(self._schedule_row(workflow_id))

    @staticmethod
    def _empty_body(raw: Any, operation: str) -> None:
        if not isinstance(raw, dict) or raw:
            raise ValidationError(f"{operation} body must be an empty object")

    def pause_schedule(self, workflow_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        self._empty_body(raw, "pause schedule")

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id)
            if self._schedule_row(workflow_id) is None:
                raise NotFoundError(f"workflow {workflow_id} has no schedule")
            self.store.connection.execute("UPDATE schedules SET paused = 1 WHERE workflow_id = ?", (workflow_id,))
            return self._schedule_status(self._schedule_row(workflow_id))

        with self._operation():
            return self._idempotent(key, f"pause-schedule:{workflow_id}", apply)

    def resume_schedule(self, workflow_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        self._empty_body(raw, "resume schedule")

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id)
            if self._schedule_row(workflow_id) is None:
                raise NotFoundError(f"workflow {workflow_id} has no schedule")
            self.store.connection.execute("UPDATE schedules SET paused = 0 WHERE workflow_id = ?", (workflow_id,))
            # Periods that came due while paused are settled immediately
            # according to the missed policy.
            self._process_schedules(workflow_id)
            return self._schedule_status(self._schedule_row(workflow_id))

        with self._operation():
            return self._idempotent(key, f"resume-schedule:{workflow_id}", apply)

    def update_schedule(self, workflow_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        # The whole plan is validated before anything is written, so an
        # invalid declaration never partially replaces the stored one.
        schedule = parse_schedule(raw)

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id)
            self._insert_schedule(workflow_id, schedule)
            return self._schedule_status(self._schedule_row(workflow_id))

        with self._operation():
            return self._idempotent(key, f"update-schedule:{workflow_id}", apply)

    def _set_schedule_cursor(self, workflow_id: str, cursor: str) -> None:
        self.store.connection.execute("UPDATE schedules SET cursor = ? WHERE workflow_id = ?", (cursor, workflow_id))

    def _process_schedules(self, workflow_id: str | None = None) -> None:
        """Settle every due schedule period once; caller holds a transaction."""
        now = time.time()
        if workflow_id is None:
            rows = self.store.connection.execute(
                "SELECT workflow_id, document, paused, anchor_at, cursor FROM schedules"
            ).fetchall()
        else:
            rows = self.store.connection.execute(
                "SELECT workflow_id, document, paused, anchor_at, cursor FROM schedules WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchall()
        for row in rows:
            self._process_schedule(row, now)

    def _process_schedule(self, row: Any, now: float) -> None:
        """Advance one schedule to the current time.

        Exactly one execution may exist per schedule period: the trigger row
        and the cursor together make a repeated pass for the same period a
        no-op. While paused nothing fires; a "skip" schedule consumes the
        periods it sleeps through, a "catch_up" schedule keeps them pending
        and fires only the most recent one when it is resumed.
        """
        workflow_id = row["workflow_id"]
        document = self.store.decode(row["document"])
        paused = bool(row["paused"])
        cursor = row["cursor"]
        if "interval_seconds" in document:
            interval = document["interval_seconds"]
            anchor = row["anchor_at"]
            index = int((now - anchor) // interval)
            if index < 1:
                return
            consumed = int(cursor) if cursor else 0
            if index <= consumed:
                return
            period_key = f"i:{index}"
            fire_at = anchor + index * interval
            granularity = float(interval)
            new_cursor = str(index)
        else:
            cron = Cron.parse(document["cron"])
            candidate = int(cursor) if cursor else cron.next_after(row["anchor_at"])
            horizon = int(now // 60) * 60
            latest = None
            following = candidate
            while following is not None and following <= horizon:
                latest = following
                following = cron.next_after(following)
            if latest is None:
                if candidate is not None and str(candidate) != cursor:
                    self._set_schedule_cursor(workflow_id, str(candidate))
                return
            period_key = f"c:{latest}"
            fire_at = float(latest)
            granularity = 60.0
            # When no further match exists (an unreachable expression), keep
            # scanning from just past the current horizon on later passes.
            new_cursor = str(following) if following is not None else str(horizon + 60)
        if paused:
            if document["missed_policy"] == "skip":
                self._set_schedule_cursor(workflow_id, new_cursor)
            return
        if document["missed_policy"] == "catch_up" or now - fire_at < granularity:
            self._fire_schedule(workflow_id, document, period_key)
        self._set_schedule_cursor(workflow_id, new_cursor)

    def _fire_schedule(self, workflow_id: str, document: dict[str, Any], period_key: str) -> str:
        """Create the execution for one schedule period, or return the existing one."""
        existing = self.store.connection.execute(
            "SELECT execution_id, triggered_at FROM schedule_triggers WHERE workflow_id = ? AND period_key = ?",
            (workflow_id, period_key),
        ).fetchone()
        if existing:
            execution_id = existing["execution_id"]
            triggered_at = existing["triggered_at"]
        else:
            execution_id = f"{workflow_id}-scheduled-{period_key}"
            claimed = self.store.connection.execute("SELECT 1 FROM executions WHERE id = ?", (execution_id,)).fetchone()
            if not claimed:
                # The created execution is exactly a manually created one:
                # same state shape and the usual execution_started event,
                # with nothing schedule-specific added to its stream.
                self._insert_execution(execution_id, workflow_id, document["input"], None, None)
            triggered_at = self.store.now()
            self.store.connection.execute(
                "INSERT INTO schedule_triggers(workflow_id, period_key, execution_id, triggered_at) VALUES (?, ?, ?, ?)",
                (workflow_id, period_key, execution_id, triggered_at),
            )
        self.store.connection.execute(
            "UPDATE schedules SET last_triggered_at = ?, last_execution_id = ? WHERE workflow_id = ?",
            (triggered_at, execution_id, workflow_id),
        )
        return execution_id

    def _node_container(self, workflow: Workflow, state: dict[str, Any], node_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return the state fragment holding the node's progress and its event context."""
        loop_id = self._active_loop(workflow, state, node_id)
        if loop_id is None:
            return state, {}
        loop_state = state["loops"][loop_id]
        return loop_state["iterations"][-1], {"loop_id": loop_id, "iteration": loop_state["current_iteration"]}

    def _complete_node(self, execution_id: str, workflow: Workflow, state: dict[str, Any], node_id: str, output: Any) -> None:
        container, context = self._node_container(workflow, state, node_id)
        container["completed_nodes"].append(node_id)
        container["outputs"][node_id] = output
        container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
        self._append(execution_id, "node_completed", {"node_id": node_id, "output": output, **context})

    def _fail_node(self, execution_id: str, workflow: Workflow, state: dict[str, Any], node_id: str, reason: str) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        retries = by_id[node_id].retries or 0
        container, context = self._node_container(workflow, state, node_id)
        entry = container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
        entry["failures"] += 1
        self._append(execution_id, "node_failed", {"node_id": node_id, "attempt": entry["attempt"], "reason": reason, **context})
        if entry["failures"] <= retries:
            entry["attempt"] += 1
            self._append(execution_id, "node_retried", {"node_id": node_id, "attempt": entry["attempt"], "reason": reason, **context})
        else:
            state["failed_nodes"].append(node_id)
            self._terminate(execution_id, state, "retries_exhausted", {"node_id": node_id})

    def _ready_tasks(self, workflow: Workflow, state: dict[str, Any]) -> list[str]:
        bodies = workflow.loop_bodies()
        body_members = set().union(*bodies.values()) if bodies else set()
        satisfied = set(state["completed_nodes"]) | set(state["skipped_nodes"])
        ready = [
            node.id
            for node in workflow.nodes
            if node.kind == "task"
            and node.id not in body_members
            and node.id not in satisfied
            and set(node.depends_on) <= satisfied
        ]
        by_id = {node.id: node for node in workflow.nodes}
        for loop_id, body in bodies.items():
            loop_state = state["loops"][loop_id]
            if loop_state["status"] != "running":
                continue
            iteration = loop_state["iterations"][-1]
            iteration_satisfied = set(iteration["completed_nodes"]) | set(iteration["skipped_nodes"])
            for node_id in body:
                node = by_id[node_id]
                if node.kind == "task" and node_id not in iteration_satisfied and set(node.depends_on) <= iteration_satisfied:
                    ready.append(node_id)
        return sorted(ready)

    def _active_loop(self, workflow: Workflow, state: dict[str, Any], node_id: str) -> str | None:
        for loop_id, body in workflow.loop_bodies().items():
            if node_id in body and state["loops"][loop_id]["status"] == "running":
                return loop_id
        return None

    def _auto_process(self, execution_id: str, workflow: Workflow, state: dict[str, Any]) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        bodies = workflow.loop_bodies()
        body_members = set().union(*bodies.values()) if bodies else set()
        completed = set(state["completed_nodes"])
        skipped = set(state["skipped_nodes"])
        changed = True
        while changed:
            changed = False
            for node in sorted(workflow.nodes, key=lambda item: item.id):
                if node.kind == "loop" or node.id in body_members:
                    continue
                if node.id in completed or node.id in skipped:
                    continue
                if not set(node.depends_on) <= completed | skipped:
                    continue
                if node.kind == "condition":
                    result = _evaluate_condition(node, state["input"])
                    state["condition_results"][node.id] = result
                    state["completed_nodes"].append(node.id)
                    completed.add(node.id)
                    self._append(execution_id, "condition_evaluated", {"node_id": node.id, "result": result})
                    changed = True
                elif node.run_if is not None and state["condition_results"][node.run_if.condition_id] != node.run_if.expected:
                    state["skipped_nodes"].append(node.id)
                    skipped.add(node.id)
                    self._append(execution_id, "node_skipped", {"node_id": node.id})
                    changed = True
            for loop_node in sorted((node for node in workflow.nodes if node.kind == "loop"), key=lambda item: item.id):
                loop_state = state["loops"][loop_node.id]
                if loop_state["status"] == "pending":
                    if not set(loop_node.depends_on) <= completed | skipped:
                        continue
                    result = _evaluate_condition(by_id[loop_node.condition], state["input"])
                    self._append(
                        execution_id,
                        "loop_condition_evaluated",
                        {"loop_id": loop_node.id, "node_id": loop_node.condition, "iteration": 0, "result": result},
                    )
                    if result:
                        loop_state["status"] = "running"
                        loop_state["current_iteration"] = 1
                        loop_state["iterations"].append(_new_iteration())
                        self._append(execution_id, "iteration_started", {"loop_id": loop_node.id, "iteration": 1})
                    else:
                        self._finish_loop(execution_id, state, loop_node.id, loop_state, "condition_false")
                        completed.add(loop_node.id)
                    changed = True
                elif loop_state["status"] == "running":
                    iteration = loop_state["iterations"][-1]
                    iteration_completed = set(iteration["completed_nodes"])
                    iteration_skipped = set(iteration["skipped_nodes"])
                    for node_id in sorted(bodies[loop_node.id]):
                        node = by_id[node_id]
                        if node_id in iteration_completed or node_id in iteration_skipped:
                            continue
                        if not set(node.depends_on) <= iteration_completed | iteration_skipped:
                            continue
                        if node.kind == "condition":
                            result = _evaluate_condition(node, state["input"])
                            iteration["condition_results"][node_id] = result
                            iteration["completed_nodes"].append(node_id)
                            iteration_completed.add(node_id)
                            self._append(
                                execution_id,
                                "condition_evaluated",
                                {
                                    "node_id": node_id,
                                    "result": result,
                                    "loop_id": loop_node.id,
                                    "iteration": loop_state["current_iteration"],
                                },
                            )
                            changed = True
                        elif node.run_if is not None and iteration["condition_results"][node.run_if.condition_id] != node.run_if.expected:
                            iteration["skipped_nodes"].append(node_id)
                            iteration_skipped.add(node_id)
                            self._append(
                                execution_id,
                                "node_skipped",
                                {"node_id": node_id, "loop_id": loop_node.id, "iteration": loop_state["current_iteration"]},
                            )
                            changed = True
                    if bodies[loop_node.id] <= iteration_completed | iteration_skipped:
                        current = loop_state["current_iteration"]
                        result = _evaluate_condition(by_id[loop_node.condition], state["input"])
                        self._append(
                            execution_id,
                            "loop_condition_evaluated",
                            {"loop_id": loop_node.id, "node_id": loop_node.condition, "iteration": current, "result": result},
                        )
                        if not result:
                            self._finish_loop(execution_id, state, loop_node.id, loop_state, "condition_false")
                            completed.add(loop_node.id)
                        elif current >= loop_node.max_iterations:
                            self._finish_loop(execution_id, state, loop_node.id, loop_state, "iteration_limit")
                            completed.add(loop_node.id)
                        else:
                            loop_state["current_iteration"] = current + 1
                            loop_state["iterations"].append(_new_iteration())
                            self._append(execution_id, "iteration_started", {"loop_id": loop_node.id, "iteration": current + 1})
                        changed = True
        finished = completed | skipped
        if all(node.id in finished for node in workflow.nodes if node.id not in body_members):
            state["status"] = "completed"
            self._append(execution_id, "execution_completed", {})

    def _finish_loop(self, execution_id: str, state: dict[str, Any], loop_id: str, loop_state: dict[str, Any], reason: str) -> None:
        loop_state["status"] = "completed"
        loop_state["end_reason"] = reason
        state["completed_nodes"].append(loop_id)
        self._append(
            execution_id,
            "loop_completed",
            {"loop_id": loop_id, "reason": reason, "iterations": loop_state["current_iteration"]},
        )

    def replay(self, execution_id: str) -> dict[str, Any]:
        stored = self.get_execution(execution_id)
        rebuilt: dict[str, Any] | None = None
        for event in self.events(execution_id):
            event_type = event["type"]
            payload = event["payload"]
            if event_type == "execution_started":
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": payload["workflow_id"],
                    "status": "running",
                    "termination_reason": None,
                    "timeout_seconds": payload.get("timeout_seconds"),
                    "deadline_at": payload.get("deadline_at"),
                    "input": payload["input"],
                    "completed_nodes": [],
                    "skipped_nodes": [],
                    "failed_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                    "attempts": {},
                    "loops": {loop_id: _new_loop_state() for loop_id in payload.get("loops", {})},
                }
                if "approvals" in payload:
                    rebuilt["waiting_approval"] = payload.get("waiting_approval")
                    rebuilt["approvals"] = []
            elif event_type == "condition_evaluated" and rebuilt is not None:
                node_id = payload["node_id"]
                if "loop_id" in payload:
                    iteration = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                    iteration["condition_results"][node_id] = payload["result"]
                    iteration["completed_nodes"].append(node_id)
                else:
                    rebuilt["condition_results"][node_id] = payload["result"]
                    rebuilt["completed_nodes"].append(node_id)
            elif event_type == "node_skipped" and rebuilt is not None:
                if "loop_id" in payload:
                    rebuilt["loops"][payload["loop_id"]]["iterations"][-1]["skipped_nodes"].append(payload["node_id"])
                else:
                    rebuilt["skipped_nodes"].append(payload["node_id"])
            elif event_type == "node_completed" and rebuilt is not None:
                node_id = payload["node_id"]
                if "loop_id" in payload:
                    iteration = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                    iteration["completed_nodes"].append(node_id)
                    iteration["outputs"][node_id] = payload["output"]
                    iteration["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
                else:
                    rebuilt["completed_nodes"].append(node_id)
                    rebuilt["outputs"][node_id] = payload["output"]
                    rebuilt["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
            elif event_type == "node_failed" and rebuilt is not None:
                node_id = payload["node_id"]
                if "loop_id" in payload:
                    container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                else:
                    container = rebuilt
                entry = container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
                entry["attempt"] = payload["attempt"]
                entry["failures"] += 1
            elif event_type == "node_retried" and rebuilt is not None:
                node_id = payload["node_id"]
                if "loop_id" in payload:
                    container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                else:
                    container = rebuilt
                container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})["attempt"] = payload["attempt"]
            elif event_type == "iteration_started" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["status"] = "running"
                loop_state["current_iteration"] = payload["iteration"]
                loop_state["iterations"].append(_new_iteration())
            elif event_type == "loop_completed" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["status"] = "completed"
                loop_state["end_reason"] = payload["reason"]
                rebuilt["completed_nodes"].append(payload["loop_id"])
            elif event_type == "approval_requested" and rebuilt is not None:
                rebuilt["waiting_approval"] = {
                    "node_id": payload["node_id"],
                    "approvers": list(payload["approvers"]),
                    **({"loop_id": payload["loop_id"], "iteration": payload["iteration"]} if "loop_id" in payload else {}),
                }
            elif event_type == "approval_decided" and rebuilt is not None:
                verdict = payload["decision"]
                record = {
                    "node_id": payload["node_id"],
                    "approver": payload["approver"],
                    "decision": verdict,
                    "reason": payload.get("reason"),
                }
                if "loop_id" in payload:
                    record["loop_id"] = payload["loop_id"]
                    record["iteration"] = payload["iteration"]
                rebuilt["waiting_approval"] = None
                rebuilt["approvals"].append(record)
            elif event_type == "execution_completed" and rebuilt is not None:
                rebuilt["status"] = "completed"
            elif event_type == "execution_terminated" and rebuilt is not None:
                rebuilt["status"] = "terminated"
                rebuilt["termination_reason"] = payload["reason"]
                if "waiting_approval" in rebuilt:
                    rebuilt["waiting_approval"] = None
                if payload["reason"] in ("retries_exhausted", "rejected") and "node_id" in payload:
                    rebuilt["failed_nodes"].append(payload["node_id"])
        if rebuilt is None:
            raise ConflictError("execution event stream has no start event")
        return {"consistent": rebuilt == stored, "execution": rebuilt}

    def _append(self, execution_id: str, event_type: str, payload: dict[str, Any]) -> None:
        row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM events WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        self.store.connection.execute(
            "INSERT INTO events(execution_id, sequence, type, payload, occurred_at) VALUES (?, ?, ?, ?, ?)",
            (execution_id, row["sequence"], event_type, self.store.encode(payload), self.store.now()),
        )
        if event_type in NOTIFY_EVENT_TYPES:
            # Buffered, not delivered: the surrounding operation may still roll
            # back. The outermost _operation scope delivers after the commit.
            self._local.pending = getattr(self._local, "pending", []) + [
                {"execution_id": execution_id, "sequence": row["sequence"], "type": event_type, "payload": payload}
            ]

    def _write_checkpoint(self, execution_id: str, state: dict[str, Any]) -> None:
        """Persist the state summary and event position at a node boundary."""
        event_row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) AS sequence FROM events WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        checkpoint_row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM checkpoints WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        document = {"state": state, "event_sequence": event_row["sequence"]}
        self.store.connection.execute(
            "INSERT INTO checkpoints(execution_id, sequence, event_sequence, document, created_at) VALUES (?, ?, ?, ?, ?)",
            (execution_id, checkpoint_row["sequence"], event_row["sequence"], self.store.encode(document), self.store.now()),
        )
