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
            workflow = Workflow.parse(self.store.decode(workflow_row["document"]))
            state = self._initial_state(execution_id, workflow_id, raw["input"], workflow)
            try:
                self.store.connection.execute(
                    "INSERT INTO executions(id, workflow_id, state) VALUES (?, ?, ?)",
                    (execution_id, workflow_id, self.store.encode(state)),
                )
            except Exception as error:
                if "UNIQUE constraint" in str(error):
                    raise ConflictError(f"execution {execution_id} already exists") from error
                raise
            self._append(
                execution_id,
                "execution_started",
                {
                    "workflow_id": workflow_id,
                    "input": raw["input"],
                    "loops": [loop.id for loop in workflow.nodes if loop.kind == "loop"],
                },
            )
            return state

        return self._idempotent(key, f"create-execution:{execution_id}", create)

    @staticmethod
    def _initial_state(execution_id: str, workflow_id: str, input_data: dict[str, Any], workflow: Workflow) -> dict[str, Any]:
        return {
            "id": execution_id,
            "workflow_id": workflow_id,
            "status": "running",
            "input": input_data,
            "completed_nodes": [],
            "skipped_nodes": [],
            "condition_results": {},
            "outputs": {},
            "loops": {
                loop.id: {
                    "status": "waiting",
                    "current_iteration": 0,
                    "reason": None,
                    "iterations": [],
                }
                for loop in workflow.nodes
                if loop.kind == "loop"
            },
        }

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
            engine = _Engine(self, execution_id, workflow, state)
            engine.auto_process()
            if state["status"] == "running":
                ready = engine.ready_tasks()
                if not ready:
                    raise ConflictError("execution has no ready node")
                node_id, iteration = ready[0]
                engine.complete_task(node_id, iteration, raw["output"])
                engine.auto_process()
            self.store.connection.execute("UPDATE executions SET state = ? WHERE id = ?", (self.store.encode(state), execution_id))
            return state

        return self._idempotent(key, f"advance:{execution_id}", apply)

    def replay(self, execution_id: str) -> dict[str, Any]:
        stored = self.get_execution(execution_id)
        rebuilt: dict[str, Any] | None = None
        loops_state: dict[str, Any] = {}

        def iteration_record(loop_id: str, iteration: int) -> dict[str, Any]:
            record = loops_state[loop_id]
            while len(record["iterations"]) < iteration:
                record["iterations"].append(
                    {
                        "iteration": len(record["iterations"]) + 1,
                        "completed_nodes": [],
                        "skipped_nodes": [],
                        "condition_results": {},
                    }
                )
            return record["iterations"][iteration - 1]

        for event in self.events(execution_id):
            payload = event["payload"]
            if event["type"] == "execution_started":
                loops_state = {
                    loop_id: {"status": "waiting", "current_iteration": 0, "reason": None, "iterations": []}
                    for loop_id in payload.get("loops", [])
                }
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": payload["workflow_id"],
                    "status": "running",
                    "input": payload["input"],
                    "completed_nodes": [],
                    "skipped_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                    "loops": loops_state,
                }
                continue
            if rebuilt is None:
                continue
            if event["type"] == "condition_evaluated":
                node_id = payload["node_id"]
                if "loop_id" in payload:
                    entry = iteration_record(payload["loop_id"], payload["iteration"])
                    entry["condition_results"][node_id] = payload["result"]
                    entry["completed_nodes"].append(node_id)
                    rebuilt["condition_results"].setdefault(node_id, []).append(payload["result"])
                else:
                    rebuilt["condition_results"][node_id] = payload["result"]
                    rebuilt["completed_nodes"].append(node_id)
            elif event["type"] == "node_skipped":
                if "loop_id" in payload:
                    iteration_record(payload["loop_id"], payload["iteration"])["skipped_nodes"].append(payload["node_id"])
                else:
                    rebuilt["skipped_nodes"].append(payload["node_id"])
            elif event["type"] == "node_completed":
                if "loop_id" in payload:
                    entry = iteration_record(payload["loop_id"], payload["iteration"])
                    entry["completed_nodes"].append(payload["node_id"])
                    rebuilt["outputs"].setdefault(payload["node_id"], []).append(payload["output"])
                else:
                    rebuilt["completed_nodes"].append(payload["node_id"])
                    rebuilt["outputs"][payload["node_id"]] = payload["output"]
            elif event["type"] == "loop_started":
                loops_state[payload["loop_id"]]["status"] = "running"
            elif event["type"] == "iteration_started":
                record = loops_state[payload["loop_id"]]
                record["current_iteration"] = payload["iteration"]
                iteration_record(payload["loop_id"], payload["iteration"])
            elif event["type"] == "loop_completed":
                record = loops_state[payload["loop_id"]]
                record["status"] = "completed"
                record["reason"] = payload["reason"]
                record["current_iteration"] = len(record["iterations"])
                rebuilt["completed_nodes"].append(payload["loop_id"])
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


class _Engine:
    """Materializes workflow state transitions and records them as events.

    Loop event protocol (all payloads carry loop_id):

    - loop_started {} then loop_condition_evaluated {phase:"entry",result}
    - on a true judgment: iteration_started {iteration}
    - body node events carry iteration; iteration_completed {iteration}
    - loop_condition_evaluated {phase:"between",iteration,result}
    - loop_completed {reason,iterations} where reason is condition_false
      or iteration_limit
    """

    def __init__(self, service: ChronicleFlow, execution_id: str, workflow: Workflow, state: dict[str, Any]):
        self.service = service
        self.execution_id = execution_id
        self.workflow = workflow
        self.state = state
        self.by_id = {node.id: node for node in workflow.nodes}
        self.loop_nodes = sorted(
            (node for node in workflow.nodes if node.kind == "loop"), key=lambda node: node.id
        )
        self.bodies = {loop.id: set(workflow.loops[loop.id].body) for loop in self.loop_nodes}
        self.loop_owners = {
            member_id: loop.id for loop in self.loop_nodes for member_id in self.bodies[loop.id]
        }

    def append(self, event_type: str, payload: dict[str, Any]) -> None:
        self.service._append(self.execution_id, event_type, payload)

    def loop_record(self, loop_id: str) -> dict[str, Any]:
        return self.state["loops"][loop_id]

    def iteration_entry(self, loop_id: str, iteration: int) -> dict[str, Any]:
        return self.loop_record(loop_id)["iterations"][iteration - 1]

    def is_finished(self, node_id: str, iteration: int | None = None) -> bool:
        owner = self.loop_owners.get(node_id)
        if owner is None:
            return node_id in self.state["completed_nodes"] or node_id in self.state["skipped_nodes"]
        record = self.loop_record(owner)
        if record["status"] != "running" or record["current_iteration"] != iteration:
            return False
        entry = record["iterations"][iteration - 1]
        return node_id in entry["completed_nodes"] or node_id in entry["skipped_nodes"]

    def dependencies_satisfied(self, node: Node, iteration: int | None) -> bool:
        for dependency_id in node.depends_on:
            if self.loop_owners.get(dependency_id) == self.loop_owners.get(node.id):
                if not self.is_finished(dependency_id, iteration):
                    return False
            elif dependency_id not in self.state["completed_nodes"] and dependency_id not in self.state["skipped_nodes"]:
                return False
        return True

    def condition_value(self, condition_id: str, iteration: int | None) -> bool:
        results = self.state["condition_results"][condition_id]
        return results if iteration is None else results[iteration - 1]

    # -- outer DAG --------------------------------------------------------

    def evaluate_outer_condition(self, node: Node) -> bool:
        if self.is_finished(node.id) or not self.dependencies_satisfied(node, None):
            return False
        result = _evaluate_condition(node, self.state["input"])
        self.state["condition_results"][node.id] = result
        self.state["completed_nodes"].append(node.id)
        self.append("condition_evaluated", {"node_id": node.id, "result": result})
        return True

    def skip_outer_task_if_guarded(self, node: Node) -> bool:
        if node.run_if is None or self.is_finished(node.id) or not self.dependencies_satisfied(node, None):
            return False
        if self.condition_value(node.run_if.condition_id, None) == node.run_if.expected:
            return False
        self.state["skipped_nodes"].append(node.id)
        self.append("node_skipped", {"node_id": node.id})
        return True

    # -- loops ------------------------------------------------------------

    def ready_loops(self) -> list[Node]:
        settled = set(self.state["completed_nodes"]) | set(self.state["skipped_nodes"])
        return [
            loop
            for loop in self.loop_nodes
            if self.loop_record(loop.id)["status"] == "waiting" and set(loop.depends_on) <= settled
        ]

    def start_loop(self, loop: Node) -> None:
        record = self.loop_record(loop.id)
        record["status"] = "running"
        self.append("loop_started", {"loop_id": loop.id})
        result = _evaluate_condition(self.by_id[loop.condition_id], self.state["input"])
        self.append(
            "loop_condition_evaluated",
            {"loop_id": loop.id, "condition_id": loop.condition_id, "phase": "entry", "result": result},
        )
        if result is False:
            self.finish_loop(loop, "condition_false")
        else:
            self.start_iteration(loop, 1)

    def start_iteration(self, loop: Node, iteration: int) -> None:
        record = self.loop_record(loop.id)
        record["current_iteration"] = iteration
        record["iterations"].append(
            {"iteration": iteration, "completed_nodes": [], "skipped_nodes": [], "condition_results": {}}
        )
        self.append("iteration_started", {"loop_id": loop.id, "iteration": iteration})

    def finish_loop(self, loop: Node, reason: str) -> None:
        record = self.loop_record(loop.id)
        record["status"] = "completed"
        record["reason"] = reason
        record["current_iteration"] = len(record["iterations"])
        self.state["completed_nodes"].append(loop.id)
        self.append(
            "loop_completed",
            {"loop_id": loop.id, "reason": reason, "iterations": len(record["iterations"])},
        )

    def evaluate_body_condition(self, loop: Node, iteration: int, node: Node) -> bool:
        if self.is_finished(node.id, iteration) or not self.dependencies_satisfied(node, iteration):
            return False
        result = _evaluate_condition(node, self.state["input"])
        entry = self.iteration_entry(loop.id, iteration)
        entry["condition_results"][node.id] = result
        self.state["condition_results"].setdefault(node.id, []).append(result)
        entry["completed_nodes"].append(node.id)
        self.append(
            "condition_evaluated",
            {"loop_id": loop.id, "iteration": iteration, "node_id": node.id, "result": result},
        )
        return True

    def skip_body_task_if_guarded(self, loop: Node, iteration: int, node: Node) -> bool:
        if node.run_if is None or self.is_finished(node.id, iteration) or not self.dependencies_satisfied(node, iteration):
            return False
        if self.condition_value(node.run_if.condition_id, iteration) == node.run_if.expected:
            return False
        self.iteration_entry(loop.id, iteration)["skipped_nodes"].append(node.id)
        self.append(
            "node_skipped",
            {"loop_id": loop.id, "iteration": iteration, "node_id": node.id},
        )
        return True

    def complete_task(self, node_id: str, iteration: int | None, output: dict[str, Any]) -> None:
        if iteration is None:
            self.state["completed_nodes"].append(node_id)
            self.state["outputs"][node_id] = output
            self.append("node_completed", {"node_id": node_id, "output": output})
            return
        owner = self.loop_owners[node_id]
        self.iteration_entry(owner, iteration)["completed_nodes"].append(node_id)
        self.state["outputs"].setdefault(node_id, []).append(output)
        self.append(
            "node_completed",
            {"loop_id": owner, "iteration": iteration, "node_id": node_id, "output": output},
        )

    def settle_iteration(self, loop: Node, iteration: int) -> bool:
        """Auto-evaluate conditions and skip guarded tasks inside one round."""
        progressed = False
        changed = True
        while changed:
            changed = False
            for member_id in sorted(self.bodies[loop.id]):
                node = self.by_id[member_id]
                if self.is_finished(member_id, iteration):
                    continue
                if node.kind == "condition":
                    if self.evaluate_body_condition(loop, iteration, node):
                        changed = True
                        progressed = True
                elif self.skip_body_task_if_guarded(loop, iteration, node):
                    changed = True
                    progressed = True
        return progressed

    def close_iteration_if_done(self, loop: Node) -> bool:
        """Finish a round whose whole body is settled. Returns progress flag."""
        record = self.loop_record(loop.id)
        if record["status"] != "running":
            return False
        iteration = record["current_iteration"]
        entry = record["iterations"][iteration - 1]
        done = set(entry["completed_nodes"]) | set(entry["skipped_nodes"])
        if not self.bodies[loop.id] <= done:
            return False
        self.append("iteration_completed", {"loop_id": loop.id, "iteration": iteration})
        # The control condition is a body node, so it was auto-evaluated while
        # the round was settled; the round-end judgment reuses that result.
        result = entry["condition_results"][loop.condition_id]
        self.append(
            "loop_condition_evaluated",
            {
                "loop_id": loop.id,
                "condition_id": loop.condition_id,
                "phase": "between",
                "iteration": iteration,
                "result": result,
            },
        )
        if result is False:
            self.finish_loop(loop, "condition_false")
        elif iteration >= loop.max_iterations:
            self.finish_loop(loop, "iteration_limit")
        else:
            self.start_iteration(loop, iteration + 1)
        return True

    # -- main fixed point -------------------------------------------------

    def auto_process(self) -> None:
        outer_conditions = sorted(
            (node for node in self.workflow.nodes if node.kind == "condition" and node.id not in self.loop_owners),
            key=lambda node: node.id,
        )
        outer_tasks = sorted(
            (node for node in self.workflow.nodes if node.kind == "task" and node.id not in self.loop_owners),
            key=lambda node: node.id,
        )
        progressed = True
        while progressed:
            progressed = False
            for loop in self.ready_loops():
                self.start_loop(loop)
                progressed = True
            for loop in self.loop_nodes:
                record = self.loop_record(loop.id)
                if record["status"] != "running":
                    continue
                iteration = record["current_iteration"]
                if self.settle_iteration(loop, iteration):
                    progressed = True
                if self.close_iteration_if_done(loop):
                    progressed = True
            for node in outer_conditions:
                if self.evaluate_outer_condition(node):
                    progressed = True
            for node in outer_tasks:
                if self.skip_outer_task_if_guarded(node):
                    progressed = True
        # Body nodes are encapsulated by their loop: they only appear in the
        # per-iteration records, never in the top-level lists.
        total = len(self.workflow.nodes) - len(self.loop_owners)
        if len(self.state["completed_nodes"]) + len(self.state["skipped_nodes"]) == total:
            if self.state["status"] == "running":
                self.state["status"] = "completed"
                self.append("execution_completed", {})

    def ready_tasks(self) -> list[tuple[str, int | None]]:
        """All ready tasks, lexicographically smallest node id first."""
        ready: list[tuple[str, int | None]] = []
        for loop in self.loop_nodes:
            record = self.loop_record(loop.id)
            if record["status"] != "running":
                continue
            iteration = record["current_iteration"]
            for member_id in self.bodies[loop.id]:
                node = self.by_id[member_id]
                if (
                    node.kind == "task"
                    and not self.is_finished(member_id, iteration)
                    and self.dependencies_satisfied(node, iteration)
                ):
                    ready.append((member_id, iteration))
        for node in self.workflow.nodes:
            if node.kind == "task" and node.id not in self.loop_owners:
                if not self.is_finished(node.id) and self.dependencies_satisfied(node, None):
                    ready.append((node.id, None))
        ready.sort(key=lambda item: item[0])
        return ready
