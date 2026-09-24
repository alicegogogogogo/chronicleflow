from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Callable

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _finite_json, _identifier
from .store import Store

TERMINAL_REASONS = {"completed", "failed", "cancelled", "timed_out"}


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


def _new_iteration(retry_nodes: tuple[str, ...] = ()) -> dict[str, Any]:
    iteration: dict[str, Any] = {
        "completed_nodes": [],
        "skipped_nodes": [],
        "condition_results": {},
        "outputs": {},
    }
    if retry_nodes:
        iteration["nodes"] = {
            node_id: {"attempt": 1, "failures": 0, "status": "pending"} for node_id in retry_nodes
        }
    return iteration


def _body_retry_nodes(by_id: dict[str, Node], body: frozenset[str]) -> tuple[str, ...]:
    return tuple(sorted(node_id for node_id in body if by_id[node_id].kind == "task" and by_id[node_id].retries > 0))


def _new_loop_state() -> dict[str, Any]:
    return {"status": "pending", "current_iteration": 0, "iterations": [], "end_reason": None}


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ChronicleFlow:
    def __init__(self, database: str, clock: Callable[[], datetime] | None = None):
        self.store = Store(database, clock=clock)

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
        if not isinstance(raw, dict) or not {"id", "workflow_id", "input"} <= set(raw) <= {
            "id",
            "workflow_id",
            "input",
            "timeout_seconds",
        }:
            raise ValidationError("execution must contain id, workflow_id, input, and optionally timeout_seconds")
        execution_id = _identifier(raw["id"], "execution id")
        workflow_id = _identifier(raw["workflow_id"], "workflow id")
        if not isinstance(raw["input"], dict):
            raise ValidationError("input must be an object")
        _finite_json(raw["input"], "input")
        timeout_seconds = None
        if "timeout_seconds" in raw:
            timeout_seconds = self._parse_timeout(raw["timeout_seconds"])

        def create() -> dict[str, Any]:
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
            if not workflow_row:
                raise NotFoundError(f"workflow {workflow_id} was not found")
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            by_id = {node.id: node for node in workflow.nodes}
            loops = {node.id: _new_loop_state() for node in workflow.nodes if node.kind == "loop"}
            bodies = workflow.loop_bodies()
            body_members = set().union(*bodies.values()) if bodies else set()
            retry_nodes = sorted(
                node.id
                for node in workflow.nodes
                if node.kind == "task" and node.retries > 0 and node.id not in body_members
            )
            body_retry_nodes: dict[str, tuple[str, ...]] = {}
            for loop_id, body in bodies.items():
                retry_in_body = _body_retry_nodes(by_id, body)
                if retry_in_body:
                    body_retry_nodes[loop_id] = retry_in_body
            started_at = self.store.now()
            state: dict[str, Any] = {
                "id": execution_id,
                "workflow_id": workflow_id,
                "status": "running",
                "input": raw["input"],
                "completed_nodes": [],
                "skipped_nodes": [],
                "condition_results": {},
                "outputs": {},
                "loops": loops,
            }
            start_payload: dict[str, Any] = {"workflow_id": workflow_id, "input": raw["input"], "loops": loops}
            if retry_nodes:
                state["nodes"] = {
                    node_id: {"attempt": 1, "failures": 0, "status": "pending"} for node_id in retry_nodes
                }
                start_payload["retry_nodes"] = retry_nodes
            if body_retry_nodes:
                start_payload["body_retry_nodes"] = body_retry_nodes
            if timeout_seconds is not None:
                state["timeout_seconds"] = timeout_seconds
                state["started_at"] = started_at
                start_payload["timeout_seconds"] = timeout_seconds
                start_payload["started_at"] = started_at
            if retry_nodes or timeout_seconds is not None:
                state["termination_reason"] = None
            try:
                self.store.connection.execute(
                    "INSERT INTO executions(id, workflow_id, state) VALUES (?, ?, ?)",
                    (execution_id, workflow_id, self.store.encode(state)),
                )
            except Exception as error:
                if "UNIQUE constraint" in str(error):
                    raise ConflictError(f"execution {execution_id} already exists") from error
                raise
            self._append(execution_id, "execution_started", start_payload)
            return state

        return self._idempotent(key, f"create-execution:{execution_id}", create)

    @staticmethod
    def _parse_timeout(value: Any) -> int | float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError("timeout_seconds must be a positive number of seconds")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValidationError("timeout_seconds must be a finite positive number of seconds")
        if value <= 0:
            raise ValidationError("timeout_seconds must be a positive number of seconds")
        return value

    def get_execution(self, execution_id: str) -> dict[str, Any]:
        row = self.store.connection.execute("SELECT state FROM executions WHERE id = ?", (execution_id,)).fetchone()
        if not row:
            raise NotFoundError(f"execution {execution_id} was not found")
        return self.store.decode(row["state"])

    def inspect_execution(self, execution_id: str) -> dict[str, Any]:
        """Return the execution state, materializing a due timeout first."""
        with self.store.transaction():
            state = self.get_execution(execution_id)
            if state["status"] == "running" and self._timed_out(state):
                self._terminate(execution_id, state, "timed_out")
                self._persist(state, execution_id)
            return state

    def events(self, execution_id: str) -> list[dict[str, Any]]:
        # Materialize a due timeout so the stream stays consistent with state.
        self.inspect_execution(execution_id)
        rows = self.store.connection.execute(
            "SELECT sequence, type, payload, occurred_at FROM events WHERE execution_id = ? ORDER BY sequence",
            (execution_id,),
        ).fetchall()
        return [
            {"sequence": row["sequence"], "type": row["type"], "payload": self.store.decode(row["payload"]), "occurred_at": row["occurred_at"]}
            for row in rows
        ]

    def advance(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        failure_reason = self._parse_advance_body(raw)

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            if self._timed_out(state):
                self._terminate(execution_id, state, "timed_out")
                self._persist(state, execution_id)
                return state
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            self._auto_process(execution_id, workflow, state)
            if state["status"] == "running":
                ready = self._ready_tasks(workflow, state)
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id = ready[0]
                if failure_reason is not None:
                    self._record_failure(execution_id, workflow, state, node_id, failure_reason)
                else:
                    self._record_success(execution_id, workflow, state, node_id, raw["output"])
                if state["status"] == "running":
                    self._auto_process(execution_id, workflow, state)
            self._persist(state, execution_id)
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    @staticmethod
    def _parse_advance_body(raw: Any) -> str | None:
        if not isinstance(raw, dict) or set(raw) not in ({"output"}, {"failure"}):
            raise ValidationError("advance body must contain exactly an output object or a failure object")
        if "output" in raw:
            if not isinstance(raw["output"], dict):
                raise ValidationError("advance body must contain exactly an output object")
            _finite_json(raw["output"], "output")
            return None
        failure = raw["failure"]
        if not isinstance(failure, dict) or set(failure) != {"reason"}:
            raise ValidationError("failure must contain exactly a reason string")
        reason = failure["reason"]
        if not isinstance(reason, str) or not reason or len(reason) > 1000:
            raise ValidationError("failure reason must be a non-empty string of at most 1000 characters")
        return reason

    @staticmethod
    def _tracking_entry(state: dict[str, Any], loop_id: str | None, node_id: str) -> dict[str, Any] | None:
        if loop_id is None:
            container = state.get("nodes")
        else:
            iteration = state["loops"][loop_id]["iterations"][-1]
            container = iteration.get("nodes")
        return container.get(node_id) if container is not None else None

    @staticmethod
    def _mark_skipped(state: dict[str, Any], loop_id: str | None, node_id: str) -> None:
        entry = ChronicleFlow._tracking_entry(state, loop_id, node_id)
        if entry is not None:
            entry["status"] = "skipped"

    def _record_success(
        self, execution_id: str, workflow: Workflow, state: dict[str, Any], node_id: str, output: dict[str, Any]
    ) -> None:
        loop_id = self._active_loop(workflow, state, node_id)
        entry = self._tracking_entry(state, loop_id, node_id)
        attempt = entry["attempt"] if entry else 1
        if loop_id is None:
            state["completed_nodes"].append(node_id)
            state["outputs"][node_id] = output
            payload: dict[str, Any] = {"node_id": node_id, "output": output}
        else:
            loop_state = state["loops"][loop_id]
            iteration = loop_state["iterations"][-1]
            iteration["completed_nodes"].append(node_id)
            iteration["outputs"][node_id] = output
            payload = {
                "node_id": node_id,
                "output": output,
                "loop_id": loop_id,
                "iteration": loop_state["current_iteration"],
            }
        if attempt > 1:
            payload["attempt"] = attempt
        self._append(execution_id, "node_completed", payload)
        if entry is not None:
            entry["status"] = "completed"

    def _record_failure(
        self, execution_id: str, workflow: Workflow, state: dict[str, Any], node_id: str, reason: str
    ) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        retries = by_id[node_id].retries
        loop_id = self._active_loop(workflow, state, node_id)
        entry = self._tracking_entry(state, loop_id, node_id)
        attempt = entry["attempt"] if entry else 1
        loop_state = state["loops"][loop_id] if loop_id is not None else None
        failed_payload: dict[str, Any] = {"node_id": node_id, "attempt": attempt, "reason": reason}
        retry_payload: dict[str, Any] = {"node_id": node_id, "attempt": attempt, "next_attempt": attempt + 1}
        if loop_id is not None:
            failed_payload["loop_id"] = loop_id
            failed_payload["iteration"] = loop_state["current_iteration"]
            retry_payload["loop_id"] = loop_id
            retry_payload["iteration"] = loop_state["current_iteration"]
        self._append(execution_id, "node_failed", failed_payload)
        if attempt <= retries:
            # The node goes back to pending and becomes ready again for another attempt.
            self._append(execution_id, "node_retried", retry_payload)
            assert entry is not None
            entry["attempt"] = attempt + 1
            entry["failures"] = attempt
            entry["status"] = "pending"
        else:
            failed_entry = {"attempt": attempt, "failures": attempt, "status": "failed"}
            if loop_id is None:
                state.setdefault("nodes", {})[node_id] = failed_entry
            else:
                iteration = state["loops"][loop_id]["iterations"][-1]
                iteration.setdefault("nodes", {})[node_id] = failed_entry
            self._terminate(execution_id, state, "failed", failed_node=node_id)

    def cancel(self, execution_id: str, raw: Any, key: str | None) -> dict[str, Any]:
        if raw is not None and raw != {}:
            raise ValidationError("cancel body must be empty or an empty object")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                return state
            if self._timed_out(state):
                self._terminate(execution_id, state, "timed_out")
            else:
                self._terminate(execution_id, state, "cancelled")
            self._persist(state, execution_id)
            return state

        return self._idempotent(key, f"cancel:{execution_id}", apply)

    def _timed_out(self, state: dict[str, Any]) -> bool:
        timeout_seconds = state.get("timeout_seconds")
        if timeout_seconds is None:
            return False
        started_at = _parse_timestamp(state["started_at"])
        return self.store.current_time() >= started_at + timedelta(seconds=timeout_seconds)

    def _terminate(self, execution_id: str, state: dict[str, Any], reason: str, failed_node: str | None = None) -> None:
        payload: dict[str, Any] = {"reason": reason}
        if failed_node is not None:
            payload["node_id"] = failed_node
        state["status"] = reason
        state["termination_reason"] = reason
        self._append(execution_id, "execution_terminated", payload)

    def _persist(self, state: dict[str, Any], execution_id: str) -> None:
        self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))

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
                    self._mark_skipped(state, None, node.id)
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
                        loop_state["iterations"].append(_new_iteration(_body_retry_nodes(by_id, bodies[loop_node.id])))
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
                            self._mark_skipped(state, loop_node.id, node_id)
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
                            loop_state["iterations"].append(_new_iteration(_body_retry_nodes(by_id, bodies[loop_node.id])))
                            self._append(execution_id, "iteration_started", {"loop_id": loop_node.id, "iteration": current + 1})
                        changed = True
        finished = completed | skipped
        if all(node.id in finished for node in workflow.nodes if node.id not in body_members):
            state["status"] = "completed"
            if "termination_reason" in state or "nodes" in state:
                state["termination_reason"] = "completed"
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
        # Materialize a due timeout before comparing state with the stream.
        self.inspect_execution(execution_id)
        stored = self.get_execution(execution_id)
        rebuilt: dict[str, Any] | None = None
        body_retry_nodes: dict[str, Any] = {}

        def tracking_container(target: dict[str, Any], loop_id: str | None) -> dict[str, Any]:
            if loop_id is None:
                return target.setdefault("nodes", {})
            iteration = target["loops"][loop_id]["iterations"][-1]
            return iteration.setdefault("nodes", {})

        for event in self.events(execution_id):
            event_type = event["type"]
            payload = event["payload"]
            if event_type == "execution_started":
                body_retry_nodes = payload.get("body_retry_nodes", {})
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": payload["workflow_id"],
                    "status": "running",
                    "input": payload["input"],
                    "completed_nodes": [],
                    "skipped_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                    "loops": {loop_id: _new_loop_state() for loop_id in payload.get("loops", {})},
                }
                retry_nodes = payload.get("retry_nodes", [])
                if retry_nodes:
                    rebuilt["nodes"] = {
                        node_id: {"attempt": 1, "failures": 0, "status": "pending"} for node_id in retry_nodes
                    }
                if "timeout_seconds" in payload:
                    rebuilt["timeout_seconds"] = payload["timeout_seconds"]
                    rebuilt["started_at"] = payload["started_at"]
                if retry_nodes or "timeout_seconds" in payload:
                    rebuilt["termination_reason"] = None
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
                loop_id = payload.get("loop_id")
                if loop_id is not None:
                    iteration = rebuilt["loops"][loop_id]["iterations"][-1]
                    iteration["skipped_nodes"].append(payload["node_id"])
                    container = iteration.get("nodes")
                else:
                    rebuilt["skipped_nodes"].append(payload["node_id"])
                    container = rebuilt.get("nodes")
                if container is not None and payload["node_id"] in container:
                    container[payload["node_id"]]["status"] = "skipped"
            elif event_type == "node_completed" and rebuilt is not None:
                node_id = payload["node_id"]
                loop_id = payload.get("loop_id")
                if loop_id is not None:
                    iteration = rebuilt["loops"][loop_id]["iterations"][-1]
                    iteration["completed_nodes"].append(node_id)
                    iteration["outputs"][node_id] = payload["output"]
                    container = iteration.get("nodes")
                else:
                    rebuilt["completed_nodes"].append(node_id)
                    rebuilt["outputs"][node_id] = payload["output"]
                    container = rebuilt.get("nodes")
                if container is not None and node_id in container:
                    container[node_id]["status"] = "completed"
            elif event_type == "node_failed" and rebuilt is not None:
                loop_id = payload.get("loop_id")
                container = tracking_container(rebuilt, loop_id)
                container[payload["node_id"]] = {
                    "attempt": payload["attempt"],
                    "failures": payload["attempt"],
                    "status": "failed",
                }
            elif event_type == "node_retried" and rebuilt is not None:
                loop_id = payload.get("loop_id")
                container = tracking_container(rebuilt, loop_id)
                container[payload["node_id"]] = {
                    "attempt": payload["next_attempt"],
                    "failures": payload["attempt"],
                    "status": "pending",
                }
            elif event_type == "iteration_started" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["status"] = "running"
                loop_state["current_iteration"] = payload["iteration"]
                loop_state["iterations"].append(_new_iteration(tuple(body_retry_nodes.get(payload["loop_id"], []))))
            elif event_type == "loop_completed" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["status"] = "completed"
                loop_state["end_reason"] = payload["reason"]
                rebuilt["completed_nodes"].append(payload["loop_id"])
            elif event_type == "execution_completed" and rebuilt is not None:
                rebuilt["status"] = "completed"
                if "termination_reason" in rebuilt or "nodes" in rebuilt:
                    rebuilt["termination_reason"] = "completed"
            elif event_type == "execution_terminated" and rebuilt is not None:
                reason = payload["reason"]
                rebuilt["status"] = reason
                rebuilt["termination_reason"] = reason
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
