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
class Approval:
    approvers: tuple[str, ...]

    @classmethod
    def parse(cls, raw: Any) -> "Approval":
        if not isinstance(raw, dict) or set(raw) != {"approvers"}:
            raise ValidationError("approval must contain exactly approvers")
        approvers = raw["approvers"]
        if not isinstance(approvers, list) or not approvers:
            raise ValidationError("approval approvers must be a non-empty array")
        if any(not isinstance(item, str) for item in approvers):
            raise ValidationError("approval approvers must be strings")
        if len(approvers) != len(set(approvers)):
            raise ValidationError("approval approvers must not contain duplicates")
        return cls(tuple(approvers))

    def as_dict(self) -> dict[str, Any]:
        return {"approvers": list(self.approvers)}


@dataclass(frozen=True)
class MapTemplate:
    """The per-instance task a map node expands into.

    A template carries only a task identifier, a retry bound, and an optional
    approval point: instances are ready tasks like any other, distinguished
    only by their map node and element index.
    """

    task_id: str
    retries: int | None
    approval: Approval | None = None

    @classmethod
    def parse(cls, raw: Any) -> "MapTemplate":
        if not isinstance(raw, dict) or not set(raw) <= {"id", "retries", "approval"}:
            raise ValidationError("map template may only contain id, retries, and approval")
        if "id" not in raw:
            raise ValidationError("map template must contain an id")
        task_id = _identifier(raw["id"], "map template id")
        retries = raw["retries"] if "retries" in raw else None
        if retries is not None:
            if isinstance(retries, bool) or not isinstance(retries, int):
                raise ValidationError("map template retries must be an integer")
            if not 0 <= retries <= MAX_RETRIES:
                raise ValidationError(f"map template retries must be between 0 and {MAX_RETRIES}")
        approval = Approval.parse(raw["approval"]) if "approval" in raw else None
        return cls(task_id, retries, approval)

    def as_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {"id": self.task_id}
        if self.retries is not None:
            document["retries"] = self.retries
        if self.approval is not None:
            document["approval"] = self.approval.as_dict()
        return document


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    depends_on: tuple[str, ...]
    path: str | None = None
    equals: Any = None
    run_if: RunIf | None = None
    retries: int | None = None
    approval: Approval | None = None
    entry: str | None = None
    condition: str | None = None
    max_iterations: int | None = None
    source: str | None = None
    max_instances: int | None = None
    template: MapTemplate | None = None

    @classmethod
    def parse(cls, raw: Any) -> "Node":
        if not isinstance(raw, dict):
            raise ValidationError("each node must be an object")
        kind = raw.get("kind")
        if kind not in ("task", "condition", "loop", "map"):
            raise ValidationError("node kind must be task, condition, loop, or map")
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
            if not set(raw) - base <= {"run_if", "retries", "approval"}:
                raise ValidationError("task nodes may only contain id, kind, depends_on, run_if, retries, and approval")
            run_if = RunIf.parse(raw["run_if"]) if "run_if" in raw else None
            retries = raw.get("retries")
            if retries is not None:
                if isinstance(retries, bool) or not isinstance(retries, int):
                    raise ValidationError("retries must be an integer")
                if not 0 <= retries <= MAX_RETRIES:
                    raise ValidationError(f"retries must be between 0 and {MAX_RETRIES}")
            approval = Approval.parse(raw["approval"]) if "approval" in raw else None
            return cls(node_id, "task", depends_on, run_if=run_if, retries=retries, approval=approval)
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
        if kind == "map":
            required = {"source", "path", "max_instances", "template"}
            if set(raw) != base | required:
                raise ValidationError(
                    "map nodes must contain exactly id, kind, depends_on, source, path, max_instances, and template"
                )
            source = _identifier(raw["source"], "map source")
            element_path = _input_path(raw["path"])
            max_instances = raw["max_instances"]
            if isinstance(max_instances, bool) or not isinstance(max_instances, int):
                raise ValidationError("max_instances must be an integer")
            if max_instances < 1:
                raise ValidationError("max_instances must be a positive integer")
            template = MapTemplate.parse(raw["template"])
            return cls(
                node_id,
                "map",
                depends_on,
                path=element_path,
                source=source,
                max_instances=max_instances,
                template=template,
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
        elif self.kind == "map":
            document["source"] = self.source
            document["path"] = self.path
            document["max_instances"] = self.max_instances
            document["template"] = self.template.as_dict()
        else:
            if self.run_if is not None:
                document["run_if"] = self.run_if.as_dict()
            if self.retries is not None:
                document["retries"] = self.retries
            if self.approval is not None:
                document["approval"] = self.approval.as_dict()
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
        _assert_valid_maps(nodes, by_id)
        return cls(workflow_id, nodes)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "nodes": [node.as_dict() for node in self.nodes]}

    def loop_bodies(self) -> dict[str, frozenset[str]]:
        """Map each loop node id to the set of node ids forming its body."""
        by_id = {node.id: node for node in self.nodes}
        bodies: dict[str, frozenset[str]] = {}
        for node in self.nodes:
            if node.kind == "loop":
                bodies[node.id] = frozenset(_collect_loop_body(by_id, node.entry, node.condition))
        return bodies


def _ancestors(by_id: dict[str, Node], node_id: str) -> set[str]:
    """Every node reachable from node_id by following depends_on edges."""
    found: set[str] = set()
    stack = [node_id]
    while stack:
        current = stack.pop()
        if current in found:
            continue
        found.add(current)
        stack.extend(by_id[current].depends_on)
    return found


def _collect_loop_body(by_id: dict[str, Node], entry: str, condition: str) -> set[str]:
    """The loop body region anchored by its named entry and condition.

    The two anchors name the ends of the repeated segment: the entry task may
    run first with the condition evaluated after it (the condition depends on
    the entry), or the condition may gate the entry task (the entry depends on
    it). Either way the body is the union of what the anchors depend on; the
    anchors are additionally required to be connected (see
    ``_assert_valid_loops``).
    """
    return _ancestors(by_id, entry) | _ancestors(by_id, condition)


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
        judge = by_id.get(node.condition)
        if judge is None:
            raise ValidationError(f"loop {node.id} condition references an unknown node")
        if judge.kind != "condition":
            raise ValidationError(f"loop {node.id} condition must reference a condition node")
        # The condition is inside the repeated segment only when it is connected
        # to the entry: it gates the entry (entry depends on it) or it is
        # evaluated after the entry (it depends on the entry).
        entry_ancestors = _ancestors(by_id, node.entry)
        condition_ancestors = _ancestors(by_id, node.condition)
        if node.condition not in entry_ancestors and node.entry not in condition_ancestors:
            raise ValidationError(f"loop {node.id} condition must belong to the loop body")
        body = _collect_loop_body(by_id, node.entry, node.condition)
        if node.id in body:
            raise ValidationError(f"loop {node.id} entry must not depend on the loop itself")
        if any(by_id[member].kind == "loop" for member in body):
            raise ValidationError(f"loop {node.id} body must not contain another loop")
        if any(by_id[member].kind == "map" for member in body):
            raise ValidationError(f"loop {node.id} body must not contain a map node")
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


def _assert_valid_maps(nodes: tuple[Node, ...], by_id: dict[str, Node]) -> None:
    """Structural rules for dynamic map nodes.

    The expansion source must be a task explicitly listed as a dependency, the
    template's task identifier must not collide with any declared node (it
    names only the dynamically expanded instances), and a map node must not
    itself sit inside a loop body (nested dynamic expansion is out of scope).
    """
    bodies = {
        node.id: frozenset(_collect_loop_body(by_id, node.entry, node.condition))
        for node in nodes
        if node.kind == "loop"
    }
    body_members = set().union(*bodies.values()) if bodies else set()
    declared = {node.id for node in nodes}
    templates = {node.template.task_id for node in nodes if node.kind == "map"}
    for node in nodes:
        if node.kind != "map":
            continue
        if node.id in body_members:
            raise ValidationError(f"map {node.id} must not belong to a loop body")
        source = by_id.get(node.source)
        if source is None:
            raise ValidationError(f"map {node.id} source references an unknown node")
        if source.kind != "task":
            raise ValidationError(f"map {node.id} source must reference a task node")
        if node.source not in node.depends_on:
            raise ValidationError(f"map {node.id} must list its source task {node.source} in depends_on")
    if len(templates) != len([node for node in nodes if node.kind == "map"]):
        raise ValidationError("map template identifiers must be unique")
    collided = templates & declared
    if collided:
        raise ValidationError(
            f"map template id {next(iter(collided))} must not collide with a declared node identifier"
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
