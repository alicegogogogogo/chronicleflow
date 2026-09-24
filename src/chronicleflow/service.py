from __future__ import annotations

from typing import Any, Callable

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _identifier, loop_bodies
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


def _initial_loop_state() -> dict[str, Any]:
    return {"iteration": 0, "iterations": [], "end_reason": None}


def _initial_iteration(number: int) -> dict[str, Any]:
    return {"number": number, "nodes": [], "skipped": [], "outputs": {}, "conditions": {}}


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
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            loop_ids = sorted(node.id for node in workflow.nodes if node.kind == "loop")
            state = {
                "id": execution_id,
                "workflow_id": workflow_id,
                "status": "running",
                "input": raw["input"],
                "completed_nodes": [],
                "skipped_nodes": [],
                "condition_results": {},
                "outputs": {},
                "loops": {loop_id: _initial_loop_state() for loop_id in loop_ids},
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
            self._append(execution_id, "execution_started", {"workflow_id": workflow_id, "input": raw["input"], "loops": loop_ids})
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
            bodies = loop_bodies(workflow.nodes)
            body_of = {member: loop_id for loop_id, body in bodies.items() for member in body}
            self._auto_process(execution_id, workflow, state, bodies, body_of)
            if state["status"] == "running":
                satisfied = set(state["completed_nodes"]) | set(state["skipped_nodes"])
                ready = []
                for node in workflow.nodes:
                    if node.kind != "task":
                        continue
                    loop_id = body_of.get(node.id)
                    if loop_id is None:
                        if node.id not in satisfied and set(node.depends_on) <= satisfied:
                            ready.append(node.id)
                        continue
                    loop_state = state["loops"][loop_id]
                    if loop_state["end_reason"] is not None or loop_state["iteration"] == 0:
                        continue
                    current = loop_state["iterations"][-1]
                    if node.id not in current["nodes"] and set(node.depends_on) <= set(current["nodes"]):
                        ready.append(node.id)
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id = sorted(ready)[0]
                loop_id = body_of.get(node_id)
                if loop_id is None:
                    state["completed_nodes"].append(node_id)
                    state["outputs"][node_id] = raw["output"]
                    self._append(execution_id, "node_completed", {"node_id": node_id, "output": raw["output"]})
                else:
                    current = state["loops"][loop_id]["iterations"][-1]
                    current["nodes"].append(node_id)
                    current["outputs"][node_id] = raw["output"]
                    self._append(
                        execution_id,
                        "node_completed",
                        {"node_id": node_id, "output": raw["output"], "loop_id": loop_id, "iteration": current["number"]},
                    )
                if len(state["completed_nodes"]) + len(state["skipped_nodes"]) == len(workflow.nodes) - len(body_of):
                    state["status"] = "completed"
                    self._append(execution_id, "execution_completed", {})
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    def _auto_process(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        bodies: dict[str, set[str]],
        body_of: dict[str, str],
    ) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        completed = set(state["completed_nodes"])
        skipped = set(state["skipped_nodes"])
        changed = True
        while changed:
            changed = False
            for node in sorted(workflow.nodes, key=lambda item: item.id):
                if node.id in body_of or node.id in completed or node.id in skipped:
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
                elif node.kind == "loop":
                    if self._process_loop(execution_id, node, by_id, bodies[node.id], state, completed):
                        changed = True
                elif node.run_if is not None and state["condition_results"][node.run_if.condition_id] != node.run_if.expected:
                    state["skipped_nodes"].append(node.id)
                    skipped.add(node.id)
                    self._append(execution_id, "node_skipped", {"node_id": node.id})
                    changed = True
        if len(completed) + len(skipped) == len(workflow.nodes) - len(body_of):
            state["status"] = "completed"
            self._append(execution_id, "execution_completed", {})

    def _process_loop(
        self,
        execution_id: str,
        loop: Node,
        by_id: dict[str, Node],
        body: set[str],
        state: dict[str, Any],
        completed: set[str],
    ) -> bool:
        loop_state = state["loops"][loop.id]
        if loop_state["end_reason"] is not None:
            return False
        changed = False
        condition = by_id[loop.condition_id]
        if loop_state["iteration"] == 0:
            result = _evaluate_condition(condition, state["input"])
            self._append(execution_id, "loop_judgment", {"loop_id": loop.id, "iteration": 0, "result": result})
            changed = True
            if result:
                self._start_iteration(execution_id, loop, loop_state, 1)
            else:
                self._end_loop(execution_id, loop, loop_state, state, completed, "condition_false")
                return True
        while loop_state["end_reason"] is None:
            current = loop_state["iterations"][-1]
            processed = set(current["nodes"])
            progressed = False
            for member_id in sorted(body):
                if member_id in processed:
                    continue
                member = by_id[member_id]
                if not set(member.depends_on) <= processed:
                    continue
                if member.kind == "condition":
                    result = _evaluate_condition(member, state["input"])
                    current["nodes"].append(member_id)
                    current["conditions"][member_id] = result
                    self._append(
                        execution_id,
                        "condition_evaluated",
                        {"node_id": member_id, "result": result, "loop_id": loop.id, "iteration": current["number"]},
                    )
                    progressed = True
                elif member.run_if is not None and current["conditions"][member.run_if.condition_id] != member.run_if.expected:
                    current["nodes"].append(member_id)
                    current["skipped"].append(member_id)
                    self._append(
                        execution_id,
                        "node_skipped",
                        {"node_id": member_id, "loop_id": loop.id, "iteration": current["number"]},
                    )
                    progressed = True
            if progressed:
                changed = True
                continue
            if len(current["nodes"]) < len(body):
                break  # waiting for a body task to be completed by advance
            changed = True
            if current["number"] >= loop.max_iterations:
                self._end_loop(execution_id, loop, loop_state, state, completed, "iteration_limit")
            else:
                result = _evaluate_condition(condition, state["input"])
                self._append(execution_id, "loop_judgment", {"loop_id": loop.id, "iteration": current["number"], "result": result})
                if result:
                    self._start_iteration(execution_id, loop, loop_state, current["number"] + 1)
                else:
                    self._end_loop(execution_id, loop, loop_state, state, completed, "condition_false")
        return changed

    def _start_iteration(self, execution_id: str, loop: Node, loop_state: dict[str, Any], number: int) -> None:
        loop_state["iteration"] = number
        loop_state["iterations"].append(_initial_iteration(number))
        self._append(execution_id, "loop_iteration_started", {"loop_id": loop.id, "iteration": number})

    def _end_loop(
        self,
        execution_id: str,
        loop: Node,
        loop_state: dict[str, Any],
        state: dict[str, Any],
        completed: set[str],
        reason: str,
    ) -> None:
        loop_state["end_reason"] = reason
        state["completed_nodes"].append(loop.id)
        completed.add(loop.id)
        self._append(execution_id, "loop_ended", {"loop_id": loop.id, "reason": reason, "iterations": loop_state["iteration"]})

    def replay(self, execution_id: str) -> dict[str, Any]:
        stored = self.get_execution(execution_id)
        rebuilt: dict[str, Any] | None = None
        for event in self.events(execution_id):
            payload = event["payload"]
            if event["type"] == "execution_started":
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": payload["workflow_id"],
                    "status": "running",
                    "input": payload["input"],
                    "completed_nodes": [],
                    "skipped_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                    "loops": {loop_id: _initial_loop_state() for loop_id in payload.get("loops", [])},
                }
            elif rebuilt is None:
                continue
            elif event["type"] == "loop_iteration_started":
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["iteration"] = payload["iteration"]
                loop_state["iterations"].append(_initial_iteration(payload["iteration"]))
            elif event["type"] == "loop_ended":
                rebuilt["loops"][payload["loop_id"]]["end_reason"] = payload["reason"]
                rebuilt["completed_nodes"].append(payload["loop_id"])
            elif event["type"] == "condition_evaluated":
                node_id = payload["node_id"]
                loop_id = payload.get("loop_id")
                if loop_id is None:
                    rebuilt["condition_results"][node_id] = payload["result"]
                    rebuilt["completed_nodes"].append(node_id)
                else:
                    current = rebuilt["loops"][loop_id]["iterations"][-1]
                    current["nodes"].append(node_id)
                    current["conditions"][node_id] = payload["result"]
            elif event["type"] == "node_skipped":
                node_id = payload["node_id"]
                loop_id = payload.get("loop_id")
                if loop_id is None:
                    rebuilt["skipped_nodes"].append(node_id)
                else:
                    current = rebuilt["loops"][loop_id]["iterations"][-1]
                    current["nodes"].append(node_id)
                    current["skipped"].append(node_id)
            elif event["type"] == "node_completed":
                node_id = payload["node_id"]
                loop_id = payload.get("loop_id")
                if loop_id is None:
                    rebuilt["completed_nodes"].append(node_id)
                    rebuilt["outputs"][node_id] = payload["output"]
                else:
                    current = rebuilt["loops"][loop_id]["iterations"][-1]
                    current["nodes"].append(node_id)
                    current["outputs"][node_id] = payload["output"]
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
