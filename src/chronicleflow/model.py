from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import ValidationError


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 100:
        raise ValidationError(f"{field} must be a non-empty string of at most 100 characters")
    return value


def _input_path(value: Any) -> str:
    if not isinstance(value, str) or not value or any(not segment for segment in value.split(".")):
        raise ValidationError("path must be a non-empty dot-separated input path")
    return value


def _json_scalar(value: Any, field: str) -> Any:
    if isinstance(value, (dict, list)):
        raise ValidationError(f"{field} must be a JSON scalar")
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

    @classmethod
    def parse(cls, raw: Any) -> "Node":
        if not isinstance(raw, dict):
            raise ValidationError("each node must be an object")
        kind = raw.get("kind")
        if kind not in ("task", "condition"):
            raise ValidationError("node kind must be task or condition")
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
            if not set(raw) - base <= {"run_if"}:
                raise ValidationError("task nodes may only contain id, kind, depends_on, and run_if")
            run_if = RunIf.parse(raw["run_if"]) if "run_if" in raw else None
            return cls(node_id, "task", depends_on, run_if=run_if)
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
        elif self.run_if is not None:
            document["run_if"] = self.run_if.as_dict()
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
        return cls(workflow_id, nodes)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "nodes": [node.as_dict() for node in self.nodes]}


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
