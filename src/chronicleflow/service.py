from __future__ import annotations

from typing import Any, Callable

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _identifier
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
                "skipped_nodes": [],
                "condition_results": {},
                "outputs": {},
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
                return state
            workflow_row = self.store.connection.execute("SELECT document FROM workflows WHERE id = ?", (state["workflow_id"],)).fetchone()
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            self._auto_process(execution_id, workflow, state)
            if state["status"] == "running":
                satisfied = set(state["completed_nodes"]) | set(state["skipped_nodes"])
                ready = sorted(
                    node.id
                    for node in workflow.nodes
                    if node.kind == "task" and node.id not in satisfied and set(node.depends_on) <= satisfied
                )
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id = ready[0]
                state["completed_nodes"].append(node_id)
                state["outputs"][node_id] = raw["output"]
                self._append(execution_id, "node_completed", {"node_id": node_id, "output": raw["output"]})
                if len(state["completed_nodes"]) + len(state["skipped_nodes"]) == len(workflow.nodes):
                    state["status"] = "completed"
                    self._append(execution_id, "execution_completed", {})
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    def _auto_process(self, execution_id: str, workflow: Workflow, state: dict[str, Any]) -> None:
        completed = set(state["completed_nodes"])
        skipped = set(state["skipped_nodes"])
        changed = True
        while changed:
            changed = False
            for node in sorted(workflow.nodes, key=lambda item: item.id):
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
        if len(completed) + len(skipped) == len(workflow.nodes):
            state["status"] = "completed"
            self._append(execution_id, "execution_completed", {})

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
                    "skipped_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                }
            elif event["type"] == "condition_evaluated" and rebuilt is not None:
                node_id = event["payload"]["node_id"]
                rebuilt["condition_results"][node_id] = event["payload"]["result"]
                rebuilt["completed_nodes"].append(node_id)
            elif event["type"] == "node_skipped" and rebuilt is not None:
                rebuilt["skipped_nodes"].append(event["payload"]["node_id"])
            elif event["type"] == "node_completed" and rebuilt is not None:
                node_id = event["payload"]["node_id"]
                rebuilt["completed_nodes"].append(node_id)
                rebuilt["outputs"][node_id] = event["payload"]["output"]
            elif event["type"] == "execution_completed" and rebuilt is not None:
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
