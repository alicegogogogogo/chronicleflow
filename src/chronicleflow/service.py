from __future__ import annotations

from typing import Any, Callable

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Workflow, _identifier, evaluate_condition
from .store import Store


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
        if not isinstance(raw, dict) or set(raw) != {"id", "workflow_id", "input"}:
            raise ValidationError("execution must contain exactly id, workflow_id, and input")
        execution_id = _identifier(raw["id"], "execution id")
        workflow_id = _identifier(raw["workflow_id"], "workflow id")
        if not isinstance(raw["input"], dict):
            raise ValidationError("input must be an object")

        def create() -> dict[str, Any]:
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (workflow_id,)).fetchone()
            if not workflow_row:
                raise NotFoundError(f"workflow {workflow_id} was not found")
            state = {
                "id": execution_id,
                "workflow_id": workflow_id,
                "status": "running",
                "input": raw["input"],
                "completed_nodes": [],
                "outputs": {},
                "condition_results": {},
                "skipped_nodes": [],
            }
            try:
                self.store.connection.execute(
                    "INSERT INTO executions(id, workflow_id, state) VALUES (?, ?, ?)",
                    (execution_id, workflow_id, self.store.encode(state)),
                )
            except Exception as error:
                if "UNIQUE constraint" in str(error):
                    raise ConflictError(f"execution {execution_id} already exists") from error
                raise
            self._append(execution_id, "execution_started", {"workflow_id": workflow_id, "input": raw["input"]})
            return state

        return self._idempotent(key, f"create-execution:{execution_id}", create)

    def get_execution(self, execution_id: str) -> dict[str, Any]:
        row = self.store.connection.execute("SELECT state FROM executions WHERE id = ?", (execution_id,)).fetchone()
        if not row:
            raise NotFoundError(f"execution {execution_id} was not found")
        return self.store.decode(row["state"])

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
        if not isinstance(raw, dict) or set(raw) != {"output"} or not isinstance(raw["output"], dict):
            raise ValidationError("advance body must contain exactly an output object")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id)
            if state["status"] != "running":
                raise ConflictError("execution is not running")
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            by_id = {node.id: node for node in workflow.nodes}

            completed = set(state["completed_nodes"])
            skipped = set(state["skipped_nodes"])
            resolved = completed | skipped

            # Auto-process ready conditions (deterministic id order) and skip
            # tasks whose run_if no longer matches, until a fixed point. A
            # skipped node satisfies downstream dependencies just like a
            # completed one, which may unlock further conditions.
            while True:
                progressed = False
                ready_conditions = sorted(
                    node.id
                    for node in workflow.nodes
                    if node.kind == "condition"
                    and node.id not in resolved
                    and set(node.depends_on) <= resolved
                )
                for node_id in ready_conditions:
                    node = by_id[node_id]
                    result = evaluate_condition(state["input"], node.path, node.equals)
                    state["condition_results"][node_id] = result
                    state["completed_nodes"].append(node_id)
                    completed.add(node_id)
                    resolved.add(node_id)
                    self._append(execution_id, "condition_evaluated", {"node_id": node_id, "result": result})
                    progressed = True

                skippable = sorted(
                    node.id
                    for node in workflow.nodes
                    if node.kind == "task"
                    and node.id not in resolved
                    and node.run_if is not None
                    and node.run_if[0] in state["condition_results"]
                    and state["condition_results"][node.run_if[0]] != node.run_if[1]
                )
                for node_id in skippable:
                    state["skipped_nodes"].append(node_id)
                    skipped.add(node_id)
                    resolved.add(node_id)
                    self._append(execution_id, "node_skipped", {"node_id": node_id})
                    progressed = True

                if not progressed:
                    break

            if len(resolved) == len(workflow.nodes):
                # Auto-processing finished the execution; the supplied output
                # is not consumed.
                state["status"] = "completed"
                self._append(execution_id, "execution_completed", {})
                self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
                return state

            ready = sorted(
                node.id
                for node in workflow.nodes
                if node.kind == "task" and node.id not in resolved and set(node.depends_on) <= resolved
            )
            if not ready:
                raise ConflictError("execution has no ready node")
            node_id = ready[0]
            state["completed_nodes"].append(node_id)
            state["outputs"][node_id] = raw["output"]
            completed.add(node_id)
            resolved.add(node_id)
            self._append(execution_id, "node_completed", {"node_id": node_id, "output": raw["output"]})
            if len(resolved) == len(workflow.nodes):
                state["status"] = "completed"
                self._append(execution_id, "execution_completed", {})
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    def replay(self, execution_id: str) -> dict[str, Any]:
        stored = self.get_execution(execution_id)
        rebuilt: dict[str, Any] | None = None
        for event in self.events(execution_id):
            if event["type"] == "execution_started":
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": event["payload"]["workflow_id"],
                    "status": "running",
                    "input": event["payload"]["input"],
                    "completed_nodes": [],
                    "outputs": {},
                    "condition_results": {},
                    "skipped_nodes": [],
                }
            elif rebuilt is None:
                continue
            elif event["type"] == "condition_evaluated":
                node_id = event["payload"]["node_id"]
                rebuilt["completed_nodes"].append(node_id)
                rebuilt["condition_results"][node_id] = event["payload"]["result"]
            elif event["type"] == "node_skipped":
                rebuilt["skipped_nodes"].append(event["payload"]["node_id"])
            elif event["type"] == "node_completed":
                node_id = event["payload"]["node_id"]
                rebuilt["completed_nodes"].append(node_id)
                rebuilt["outputs"][node_id] = event["payload"]["output"]
            elif event["type"] == "execution_completed":
                rebuilt["status"] = "completed"
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
