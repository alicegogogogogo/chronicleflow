from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .errors import ValidationError

MAX_LOOP_ITERATIONS = 100
MAX_RETRIES = 10


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 100:
        raise ValidationError(f"{field} must be a non-empty string of at most 100 characters")
    return value


def _input_path(value: Any) -> str:
    if not isinstance(value, str) or not value or any(not segment for segment in value.split(".")):
        raise ValidationError("path must be a non-empty dot-separated input path")
    return value


def _finite_json(value: Any, field: str) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValidationError(f"{field} must not contain non-finite numbers")
    if isinstance(value, dict):
        for item in value.values():
            _finite_json(item, field)
    elif isinstance(value, list):
        for item in value:
            _finite_json(item, field)
    return value


def _json_scalar(value: Any, field: str) -> Any:
    if isinstance(value, (dict, list)):
        raise ValidationError(f"{field} must be a JSON scalar")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValidationError(f"{field} must be a finite JSON scalar")
    return value


@dataclass(frozen=True)
class RunIf:
    condition_id: str
    expected: bool

    @classmethod
    def parse(cls, raw: Any) -> "RunIf":
        if not isinstance(raw, dict) or set(raw) != {"condition_id", "expected"}:
            raise ValidationError("run_if must contain exactly condition_id and expected")
        condition_id = _identifier(raw["condition_id"], "condition id")
        if not isinstance(raw["expected"], bool):
            raise ValidationError("run_if expected must be a boolean")
        return cls(condition_id, raw["expected"])

    def as_dict(self) -> dict[str, Any]:
        return {"condition_id": self.condition_id, "expected": self.expected}


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    depends_on: tuple[str, ...]
    path: str | None = None
    equals: Any = None
    run_if: RunIf | None = None
    retries: int | None = None
    entry: str | None = None
    condition: str | None = None
    max_iterations: int | None = None

    @classmethod
    def parse(cls, raw: Any) -> "Node":
        if not isinstance(raw, dict):
            raise ValidationError("each node must be an object")
        kind = raw.get("kind")
        if kind not in ("task", "condition", "loop"):
            raise ValidationError("node kind must be task, condition, or loop")
        base = {"id", "kind", "depends_on"}
        if not base <= set(raw):
            raise ValidationError("each node must contain id, kind, and depends_on")
        node_id = _identifier(raw["id"], "node id")
        dependencies = raw["depends_on"]
        if not isinstance(dependencies, list) or any(not isinstance(item, str) for item in dependencies):
            raise ValidationError("depends_on must be an array of node identifiers")
        if len(dependencies) != len(set(dependencies)):
            raise ValidationError("depends_on must not contain duplicates")
        depends_on = tuple(dependencies)
        if kind == "task":
            if not set(raw) - base <= {"run_if", "retries"}:
                raise ValidationError("task nodes may only contain id, kind, depends_on, run_if, and retries")
            run_if = RunIf.parse(raw["run_if"]) if "run_if" in raw else None
            retries = raw.get("retries")
            if retries is not None:
                if isinstance(retries, bool) or not isinstance(retries, int):
                    raise ValidationError("retries must be an integer")
                if not 0 <= retries <= MAX_RETRIES:
                    raise ValidationError(f"retries must be between 0 and {MAX_RETRIES}")
            return cls(node_id, "task", depends_on, run_if=run_if, retries=retries)
        if kind == "loop":
            if set(raw) != base | {"entry", "condition", "max_iterations"}:
                raise ValidationError("loop nodes must contain exactly id, kind, depends_on, entry, condition, and max_iterations")
            max_iterations = raw["max_iterations"]
            if isinstance(max_iterations, bool) or not isinstance(max_iterations, int):
                raise ValidationError("max_iterations must be an integer")
            if not 1 <= max_iterations <= MAX_LOOP_ITERATIONS:
                raise ValidationError(f"max_iterations must be between 1 and {MAX_LOOP_ITERATIONS}")
            return cls(
                node_id,
                "loop",
                depends_on,
                entry=_identifier(raw["entry"], "entry"),
                condition=_identifier(raw["condition"], "condition"),
                max_iterations=max_iterations,
            )
        if set(raw) != base | {"path", "equals"}:
            raise ValidationError("condition nodes must contain exactly id, kind, depends_on, path, and equals")
        return cls(
            node_id,
            "condition",
            depends_on,
            path=_input_path(raw["path"]),
            equals=_json_scalar(raw["equals"], "equals"),
        )

    def as_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {"id": self.id, "kind": self.kind, "depends_on": list(self.depends_on)}
        if self.kind == "condition":
            document["path"] = self.path
            document["equals"] = self.equals
        elif self.kind == "loop":
            document["entry"] = self.entry
            document["condition"] = self.condition
            document["max_iterations"] = self.max_iterations
        else:
            if self.run_if is not None:
                document["run_if"] = self.run_if.as_dict()
            if self.retries is not None:
                document["retries"] = self.retries
        return document


@dataclass(frozen=True)
class Workflow:
    id: str
    nodes: tuple[Node, ...]

    @classmethod
    def parse(cls, raw: Any) -> "Workflow":
        if not isinstance(raw, dict) or set(raw) != {"id", "nodes"}:
            raise ValidationError("workflow must contain exactly id and nodes")
        workflow_id = _identifier(raw["id"], "workflow id")
        if not isinstance(raw["nodes"], list) or not raw["nodes"]:
            raise ValidationError("nodes must be a non-empty array")
        nodes = tuple(Node.parse(node) for node in raw["nodes"])
        identifiers = {node.id for node in nodes}
        if len(identifiers) != len(nodes):
            raise ValidationError("node identifiers must be unique")
        for node in nodes:
            if node.id in node.depends_on:
                raise ValidationError(f"node {node.id} cannot depend on itself")
            missing = set(node.depends_on) - identifiers
            if missing:
                raise ValidationError(f"node {node.id} has unknown dependencies: {', '.join(sorted(missing))}")
        by_id = {node.id: node for node in nodes}
        for node in nodes:
            if node.run_if is None:
                continue
            target = by_id.get(node.run_if.condition_id)
            if target is None or target.kind != "condition":
                raise ValidationError(f"node {node.id} run_if must reference a condition node")
            if node.run_if.condition_id not in node.depends_on:
                raise ValidationError(f"node {node.id} must list condition {node.run_if.condition_id} in depends_on")
        _assert_acyclic(nodes)
        _assert_valid_loops(nodes, by_id)
        return cls(workflow_id, nodes)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "nodes": [node.as_dict() for node in self.nodes]}

    def loop_bodies(self) -> dict[str, frozenset[str]]:
        """Map each loop node id to the set of node ids forming its body."""
        by_id = {node.id: node for node in self.nodes}
        bodies: dict[str, frozenset[str]] = {}
        for node in self.nodes:
            if node.kind == "loop":
                bodies[node.id] = frozenset(_collect_loop_body(by_id, node.entry))
        return bodies


def _collect_loop_body(by_id: dict[str, Node], entry: str) -> set[str]:
    body: set[str] = set()
    stack = [entry]
    while stack:
        node_id = stack.pop()
        if node_id in body:
            continue
        body.add(node_id)
        stack.extend(by_id[node_id].depends_on)
    return body


def _assert_valid_loops(nodes: tuple[Node, ...], by_id: dict[str, Node]) -> None:
    bodies: dict[str, set[str]] = {}
    for node in nodes:
        if node.kind != "loop":
            continue
        entry = by_id.get(node.entry)
        if entry is None:
            raise ValidationError(f"loop {node.id} entry references an unknown node")
        if entry.kind != "task":
            raise ValidationError(f"loop {node.id} entry must reference a task node")
        body = _collect_loop_body(by_id, entry.id)
        if node.id in body:
            raise ValidationError(f"loop {node.id} entry must not depend on the loop itself")
        if any(by_id[member].kind == "loop" for member in body):
            raise ValidationError(f"loop {node.id} body must not contain another loop")
        judge = by_id.get(node.condition)
        if judge is None:
            raise ValidationError(f"loop {node.id} condition references an unknown node")
        if judge.kind != "condition":
            raise ValidationError(f"loop {node.id} condition must reference a condition node")
        if judge.id not in body:
            raise ValidationError(f"loop {node.id} condition must belong to the loop body")
        bodies[node.id] = body
    owner: dict[str, str] = {}
    for loop_id, body in bodies.items():
        for member in body:
            if member in owner:
                raise ValidationError(f"node {member} belongs to more than one loop body")
            owner[member] = loop_id
    for node in nodes:
        if node.id in owner:
            continue
        for dependency in node.depends_on:
            if dependency in owner:
                raise ValidationError(
                    f"node {node.id} must not depend on loop body node {dependency}; depend on loop {owner[dependency]} instead"
                )


def _assert_acyclic(nodes: tuple[Node, ...]) -> None:
    graph = {node.id: node.depends_on for node in nodes}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise ValidationError("workflow contains a dependency cycle")
        if node_id in visited:
            return
        visiting.add(node_id)
        for dependency in graph[node_id]:
            visit(dependency)
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in graph:
        visit(node_id)
