from __future__ import annotations

import math
import time
from typing import Any, Callable

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _finite_json, _identifier
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


class ChronicleFlow:
    def __init__(self, database: str):
        self.store = Store(database)

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

    def create_workflow(self, raw: Any, key: str | None) -> dict[str, Any]:
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
            return workflow.as_dict()

        return self._idempotent(key, f"create-workflow:{workflow.id}", create)

    def create_execution(self, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) not in ({"id", "workflow_id", "input"}, {"id", "workflow_id", "input", "timeout_seconds"}):
            raise ValidationError("execution must contain exactly id, workflow_id, input, and optionally timeout_seconds")
        execution_id = _identifier(raw["id"], "execution id")
        workflow_id = _identifier(raw["workflow_id"], "workflow id")
        if not isinstance(raw["input"], dict):
            raise ValidationError("input must be an object")
        _finite_json(raw["input"], "input")
        timeout = raw.get("timeout_seconds")
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise ValidationError("timeout_seconds must be a positive number of seconds")
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValidationError("timeout_seconds must be a positive number of seconds")

        def create() -> dict[str, Any]:
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
            if not workflow_row:
                raise NotFoundError(f"workflow {workflow_id} was not found")
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            loops = {node.id: _new_loop_state() for node in workflow.nodes if node.kind == "loop"}
            approvals_enabled = any(node.kind == "task" and node.approval is not None for node in workflow.nodes)
            deadline = time.time() + timeout if timeout is not None else None
            state = {
                "id": execution_id,
                "workflow_id": workflow_id,
                "status": "running",
                "termination_reason": None,
                "timeout_seconds": timeout,
                "deadline_at": deadline,
                "input": raw["input"],
                "completed_nodes": [],
                "skipped_nodes": [],
                "failed_nodes": [],
                "condition_results": {},
                "outputs": {},
                "attempts": {},
                "loops": loops,
            }
            if approvals_enabled:
                state["pending_approval"] = None
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
            started_payload = {
                "workflow_id": workflow_id,
                "input": raw["input"],
                "loops": loops,
                "timeout_seconds": timeout,
                "deadline_at": deadline,
            }
            if approvals_enabled:
                # The flag is only present for workflows that declare approval
                # points, so baseline event streams keep their exact shape.
                started_payload["approvals_enabled"] = True
            self._append(execution_id, "execution_started", started_payload)
            return state

        return self._idempotent(key, f"create-execution:{execution_id}", create)

    def get_execution(self, execution_id: str) -> dict[str, Any]:
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
        if state.get("pending_approval") is not None:
            state["pending_approval"] = None
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
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            if state.get("pending_approval") is not None:
                # A waiting approval point blocks advance: only a decision may
                # continue the task, so the submitted output is not consumed.
                return state
            self._auto_process(execution_id, workflow, state)
            parked = False
            if state["status"] == "running":
                ready = self._ready_tasks(workflow, state)
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id = ready[0]
                by_id = {node.id: node for node in workflow.nodes}
                if by_id[node_id].approval is not None:
                    # The task declares an approval point: park it without
                    # writing the output or completing the node, and wait for
                    # a decision. The submitted output is not consumed and no
                    # node boundary is settled, so no checkpoint is written.
                    _, context = self._node_container(workflow, state, node_id)
                    state["pending_approval"] = node_id
                    self._append(
                        execution_id,
                        "approval_requested",
                        {"node_id": node_id, "approvers": list(by_id[node_id].approval.approvers), **context},
                    )
                    parked = True
                elif "failure" in raw:
                    self._fail_node(execution_id, workflow, state, node_id, raw["failure"]["reason"])
                else:
                    self._complete_node(execution_id, workflow, state, node_id, raw["output"])
                if state["status"] == "running":
                    self._auto_process(execution_id, workflow, state)
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            # Every path from a running start that reaches here settled at
            # least one node boundary, so checkpoint it in the same
            # transaction — except parking at an approval point, which waits
            # for a decision instead of settling a boundary.
            if not parked:
                self._write_checkpoint(execution_id, state)
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    def cancel(self, execution_id: str, key: str | None) -> dict[str, Any]:
        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            self._terminate(execution_id, state, "cancelled")
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        return self._idempotent(key, f"cancel:{execution_id}", apply)

    def decide(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValidationError("decide body must be an object")
        decision = raw.get("decision")
        if decision not in ("approved", "rejected"):
            raise ValidationError("decision must be approved or rejected")
        approver = _identifier(raw.get("approver"), "approver")
        if decision == "approved":
            if set(raw) != {"approver", "decision", "output"}:
                raise ValidationError("an approval decision must contain exactly approver, decision, and output")
            if not isinstance(raw["output"], dict):
                raise ValidationError("decision output must be an object")
            _finite_json(raw["output"], "output")
            reason = None
        else:
            if set(raw) != {"approver", "decision", "reason"}:
                raise ValidationError("a rejection decision must contain exactly approver, decision, and reason")
            if not isinstance(raw["reason"], str):
                raise ValidationError("decision reason must be a string")
            reason = raw["reason"]

        def apply() -> dict[str, Any]:
            # get_execution applies a due timeout first, so a waiting approval
            # point that timed out no longer accepts a decision.
            state = self.get_execution(execution_id)
            node_id = state.get("pending_approval")
            if state["status"] != "running" or node_id is None:
                raise ConflictError(f"execution {execution_id} is not waiting for an approval")
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            by_id = {node.id: node for node in workflow.nodes}
            if approver not in by_id[node_id].approval.approvers:
                raise ConflictError(f"approver {approver} is not allowed to decide node {node_id}")
            _, context = self._node_container(workflow, state, node_id)
            record = {"node_id": node_id, "approver": approver, "decision": decision, "reason": reason, **context}
            state["approvals"].append(record)
            self._append(execution_id, "approval_decided", record)
            state["pending_approval"] = None
            if decision == "approved":
                self._complete_node(execution_id, workflow, state, node_id, raw["output"])
                self._auto_process(execution_id, workflow, state)
            else:
                state["failed_nodes"].append(node_id)
                self._terminate(execution_id, state, "rejected", {"node_id": node_id})
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            # A decision settles a node boundary, so checkpoint it in the same
            # transaction as the state update and event append.
            self._write_checkpoint(execution_id, state)
            return state

        return self._idempotent(key, f"decide:{execution_id}", apply)

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
            # Parking at an approval point writes no checkpoint, so events
            # after the checkpoint position may already be part of the
            # materialized state; fold them onto the checkpoint snapshot.
            rows = self.store.connection.execute(
                "SELECT type, payload FROM events WHERE execution_id = ? AND sequence > ? ORDER BY sequence",
                (execution_id, event_sequence),
            ).fetchall()
            rebuilt = self.store.decode(self.store.encode(snapshot))
            for row in rows:
                self._fold_event(execution_id, rebuilt, row["type"], self.store.decode(row["payload"]))
            if rebuilt != state:
                raise ConflictError("latest checkpoint does not match the materialized state")
            return rebuilt

        return self._idempotent(key, f"recover:{execution_id}", apply)

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
        rebuilt = self._rebuild_from_events(execution_id, self.events(execution_id))
        return {"consistent": rebuilt == stored, "execution": rebuilt}

    def _rebuild_from_events(self, execution_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
        rebuilt: dict[str, Any] | None = None
        for event in events:
            rebuilt = self._fold_event(execution_id, rebuilt, event["type"], event["payload"])
        if rebuilt is None:
            raise ConflictError("execution event stream has no start event")
        return rebuilt

    def _fold_event(self, execution_id: str, rebuilt: dict[str, Any] | None, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
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
            if payload.get("approvals_enabled"):
                rebuilt["pending_approval"] = None
                rebuilt["approvals"] = []
            return rebuilt
        if rebuilt is None:
            raise ConflictError("execution event stream has no start event")
        if event_type == "approval_requested":
            rebuilt["pending_approval"] = payload["node_id"]
        elif event_type == "approval_decided":
            rebuilt["pending_approval"] = None
            rebuilt["approvals"].append(payload)
        elif event_type == "condition_evaluated":
            node_id = payload["node_id"]
            if "loop_id" in payload:
                iteration = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                iteration["condition_results"][node_id] = payload["result"]
                iteration["completed_nodes"].append(node_id)
            else:
                rebuilt["condition_results"][node_id] = payload["result"]
                rebuilt["completed_nodes"].append(node_id)
        elif event_type == "node_skipped":
            if "loop_id" in payload:
                rebuilt["loops"][payload["loop_id"]]["iterations"][-1]["skipped_nodes"].append(payload["node_id"])
            else:
                rebuilt["skipped_nodes"].append(payload["node_id"])
        elif event_type == "node_completed":
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
        elif event_type == "node_failed":
            node_id = payload["node_id"]
            if "loop_id" in payload:
                container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
            else:
                container = rebuilt
            entry = container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
            entry["attempt"] = payload["attempt"]
            entry["failures"] += 1
        elif event_type == "node_retried":
            node_id = payload["node_id"]
            if "loop_id" in payload:
                container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
            else:
                container = rebuilt
            container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})["attempt"] = payload["attempt"]
        elif event_type == "iteration_started":
            loop_state = rebuilt["loops"][payload["loop_id"]]
            loop_state["status"] = "running"
            loop_state["current_iteration"] = payload["iteration"]
            loop_state["iterations"].append(_new_iteration())
        elif event_type == "loop_completed":
            loop_state = rebuilt["loops"][payload["loop_id"]]
            loop_state["status"] = "completed"
            loop_state["end_reason"] = payload["reason"]
            rebuilt["completed_nodes"].append(payload["loop_id"])
        elif event_type == "execution_completed":
            rebuilt["status"] = "completed"
        elif event_type == "execution_terminated":
            rebuilt["status"] = "terminated"
            rebuilt["termination_reason"] = payload["reason"]
            if "pending_approval" in rebuilt:
                rebuilt["pending_approval"] = None
            if payload["reason"] in ("retries_exhausted", "rejected") and "node_id" in payload:
                rebuilt["failed_nodes"].append(payload["node_id"])
        return rebuilt

    def _append(self, execution_id: str, event_type: str, payload: dict[str, Any]) -> None:
        row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM events WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        self.store.connection.execute(
            "INSERT INTO events(execution_id, sequence, type, payload, occurred_at) VALUES (?, ?, ?, ?, ?)",
            (execution_id, row["sequence"], event_type, self.store.encode(payload), self.store.now()),
        )

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
