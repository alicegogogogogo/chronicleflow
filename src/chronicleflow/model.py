from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from .errors import ValidationError

_UNSET = object()


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 100:
        raise ValidationError(f"{field} must be a non-empty string of at most 100 characters")
    return value


def _input_path(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 1000:
        raise ValidationError("path must be a non-empty dot-separated string of at most 1000 characters")
    parts = value.split(".")
    if any(not part for part in parts):
        raise ValidationError("path must be a non-empty dot-separated string")
    return value


def _json_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("equals must be a JSON scalar")
        return value
    raise ValidationError("equals must be a JSON scalar")


def scalar_matches(value: Any, expected: Any) -> bool:
    """Compare a resolved input value with equals by JSON type and value."""
    if expected is None:
        return value is None
    if isinstance(expected, bool):
        return isinstance(value, bool) and value == expected
    if isinstance(expected, str):
        return isinstance(value, str) and value == expected
    if isinstance(expected, (int, float)):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and value == expected
    return False


def evaluate_condition(input_value: Any, path: str, expected: Any) -> bool:
    current = input_value
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return scalar_matches(current, expected)


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    depends_on: tuple[str, ...]
    run_if: tuple[str, bool] | None = None
    path: str | None = None
    equals: Any = field(default=_UNSET)

    @staticmethod
    def _parse_run_if(raw: Any) -> tuple[str, bool] | None:
        if raw is None:
            return None
        if not isinstance(raw, dict) or set(raw) != {"condition_id", "expected"}:
            raise ValidationError("run_if must contain exactly condition_id and expected")
        condition_id = _identifier(raw["condition_id"], "run_if condition_id")
        if not isinstance(raw["expected"], bool):
            raise ValidationError("run_if expected must be a boolean")
        return condition_id, raw["expected"]

    @classmethod
    def parse(cls, raw: Any) -> "Node":
        if not isinstance(raw, dict):
            raise ValidationError("each node must be an object")
        missing = {"id", "kind", "depends_on"} - set(raw)
        if missing:
            raise ValidationError(f"each node must contain {', '.join(sorted(missing))}")
        node_id = _identifier(raw["id"], "node id")
        kind = raw["kind"]
        if not isinstance(kind, str) or kind not in ("task", "condition"):
            raise ValidationError("node kind must be task or condition")
        dependencies = raw["depends_on"]
        if not isinstance(dependencies, list) or any(not isinstance(item, str) for item in dependencies):
            raise ValidationError("depends_on must be an array of node identifiers")
        if len(dependencies) != len(set(dependencies)):
            raise ValidationError("depends_on must not contain duplicates")

        if kind == "task":
            unknown = set(raw) - {"id", "kind", "depends_on", "run_if"}
            if unknown:
                raise ValidationError(f"task node {node_id} has unknown fields: {', '.join(sorted(unknown))}")
            return cls(node_id, "task", tuple(dependencies), run_if=cls._parse_run_if(raw.get("run_if")))

        unknown = set(raw) - {"id", "kind", "depends_on", "path", "equals"}
        if unknown:
            raise ValidationError(f"condition node {node_id} has unknown fields: {', '.join(sorted(unknown))}")
        if "path" not in raw or "equals" not in raw:
            raise ValidationError("condition node must contain path and equals")
        path = _input_path(raw["path"])
        equals = _json_scalar(raw["equals"])
        return cls(node_id, "condition", tuple(dependencies), path=path, equals=equals)

    def as_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {"id": self.id, "kind": self.kind, "depends_on": list(self.depends_on)}
        if self.kind == "task":
            if self.run_if is not None:
                document["run_if"] = {"condition_id": self.run_if[0], "expected": self.run_if[1]}
        else:
            document["path"] = self.path
            document["equals"] = self.equals
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
        by_id = {node.id: node for node in nodes}
        for node in nodes:
            if node.id in node.depends_on:
                raise ValidationError(f"node {node.id} cannot depend on itself")
            missing = set(node.depends_on) - identifiers
            if missing:
                raise ValidationError(f"node {node.id} has unknown dependencies: {', '.join(sorted(missing))}")
            if node.run_if is not None:
                condition_id, _expected = node.run_if
                target = by_id.get(condition_id)
                if target is None:
                    raise ValidationError(f"task {node.id} references unknown condition {condition_id}")
                if target.kind != "condition":
                    raise ValidationError(f"task {node.id} run_if must reference a condition node")
                if condition_id not in node.depends_on:
                    raise ValidationError(
                        f"task {node.id} must list run_if condition {condition_id} in depends_on"
                    )
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
