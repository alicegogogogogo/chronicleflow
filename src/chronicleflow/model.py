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
    entry: str | None = None
    condition_id: str | None = None
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
            if not set(raw) - base <= {"run_if"}:
                raise ValidationError("task nodes may only contain id, kind, depends_on, and run_if")
            run_if = RunIf.parse(raw["run_if"]) if "run_if" in raw else None
            return cls(node_id, "task", depends_on, run_if=run_if)
        if kind == "condition":
            if set(raw) != base | {"path", "equals"}:
                raise ValidationError("condition nodes must contain exactly id, kind, depends_on, path, and equals")
            return cls(
                node_id,
                "condition",
                depends_on,
                path=_input_path(raw["path"]),
                equals=_json_scalar(raw["equals"], "equals"),
            )
        # loop
        if set(raw) != base | {"entry", "condition_id", "max_iterations"}:
            raise ValidationError(
                "loop nodes must contain exactly id, kind, depends_on, entry, condition_id, and max_iterations"
            )
        entry = _identifier(raw["entry"], "loop entry")
        condition_id = _identifier(raw["condition_id"], "loop condition id")
        max_iterations = raw["max_iterations"]
        if not isinstance(max_iterations, int) or isinstance(max_iterations, bool) or not 1 <= max_iterations <= 1000:
            raise ValidationError("max_iterations must be an integer between 1 and 1000")
        return cls(
            node_id,
            "loop",
            depends_on,
            entry=entry,
            condition_id=condition_id,
            max_iterations=max_iterations,
        )

    def as_dict(self) -> dict[str, Any]:
        document: dict[str, Any] = {"id": self.id, "kind": self.kind, "depends_on": list(self.depends_on)}
        if self.kind == "condition":
            document["path"] = self.path
            document["equals"] = self.equals
        elif self.kind == "loop":
            document["entry"] = self.entry
            document["condition_id"] = self.condition_id
            document["max_iterations"] = self.max_iterations
        elif self.run_if is not None:
            document["run_if"] = self.run_if.as_dict()
        return document


@dataclass(frozen=True)
class LoopSpec:
    node: Node
    body: frozenset[str]


@dataclass(frozen=True)
class Workflow:
    id: str
    nodes: tuple[Node, ...]
    loops: dict[str, LoopSpec]

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

        loop_nodes = [node for node in nodes if node.kind == "loop"]
        loops = _build_loops(nodes, by_id, loop_nodes)

        for node in nodes:
            if node.run_if is None:
                continue
            target = by_id.get(node.run_if.condition_id)
            if target is None or target.kind != "condition":
                raise ValidationError(f"node {node.id} run_if must reference a condition node")
            if node.run_if.condition_id not in node.depends_on:
                raise ValidationError(f"node {node.id} must list condition {node.run_if.condition_id} in depends_on")
            owner = _loop_owner(node.id, loops)
            if owner is not None and _loop_owner(node.run_if.condition_id, loops) != owner:
                raise ValidationError(
                    f"loop body node {node.id} run_if must reference a condition in the same loop body"
                )
        _assert_acyclic(by_id, nodes, loops)
        return cls(workflow_id, nodes, loops)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "nodes": [node.as_dict() for node in self.nodes]}

    @property
    def node_map(self) -> dict[str, Node]:
        return {node.id: node for node in self.nodes}


def _loop_owner(node_id: str, loops: dict[str, LoopSpec]) -> str | None:
    for loop_id, spec in loops.items():
        if node_id in spec.body:
            return loop_id
    return None


def _build_loops(
    nodes: tuple[Node, ...], by_id: dict[str, Node], loop_nodes: list[Node]
) -> dict[str, LoopSpec]:
    """Derive each loop body from its entry task.

    The body is the entry task plus every node reachable from it by following
    edges to nodes that depend on it (the entry's transitive dependents). A
    round therefore starts at the entry and flows forward through the sub-DAG.
    A body only contains task/condition nodes, never nests, stays
    self-contained, and is sealed from the outer graph.
    """
    dependents: dict[str, list[str]] = {node.id: [] for node in nodes}
    for node in nodes:
        for dependency in node.depends_on:
            dependents[dependency].append(node.id)

    loops: dict[str, LoopSpec] = {}
    for loop in loop_nodes:
        entry = by_id.get(loop.entry)
        if entry is None:
            raise ValidationError(f"loop {loop.id} entry references an unknown node")
        if entry.kind != "task":
            raise ValidationError(f"loop {loop.id} entry must reference a task node")

        body: set[str] = set()
        stack = [loop.entry]
        while stack:
            current = stack.pop()
            if current in body:
                continue
            node = by_id.get(current)
            if node is None:
                raise ValidationError(f"loop {loop.id} body references unknown node: {current}")
            if node.kind == "loop":
                raise ValidationError(f"loop {loop.id} body must not contain another loop")
            if current == loop.id:
                raise ValidationError(f"loop {loop.id} entry must not reach the loop node itself")
            body.add(current)
            stack.extend(dependents[current])

        # The continue judgment belongs to a condition that is part of the
        # body, i.e. one that observes the entry round through dependencies.
        if loop.condition_id not in body:
            raise ValidationError(
                f"loop {loop.id} condition_id must be reachable from its entry and belong to the loop body"
            )
        if by_id[loop.condition_id].kind != "condition":
            raise ValidationError(f"loop {loop.id} condition_id must reference a condition node")

        # Body members must only depend on nodes inside the body; the round
        # must not implicitly execute an out-of-body node. This also forces
        # the entry to be the round root: any prerequisite of the entry lies
        # outside the forward closure and is rejected here.
        for member_id in body:
            out_of_body = set(by_id[member_id].depends_on) - body
            if out_of_body:
                raise ValidationError(
                    f"loop {loop.id} body node {member_id} has out-of-body dependencies: "
                    f"{', '.join(sorted(out_of_body))}"
                )

        loops[loop.id] = LoopSpec(loop, frozenset(body))

    # Bodies must be disjoint.
    owners: dict[str, str] = {}
    for loop_id, spec in loops.items():
        for member_id in spec.body:
            if member_id in owners:
                raise ValidationError(f"node {member_id} is part of more than one loop body")
            owners[member_id] = loop_id

    # A loop may only depend on outer prerequisite nodes (or other loop
    # nodes), never on a body member -- its own or another loop's.
    for loop_id, spec in loops.items():
        for dependency in spec.node.depends_on:
            if dependency in owners:
                if owners[dependency] == loop_id:
                    raise ValidationError(f"loop {loop_id} cannot depend on one of its own body nodes")
                raise ValidationError(
                    f"loop {loop_id} cannot depend on loop body node {dependency}; "
                    f"depend on loop {owners[dependency]} instead"
                )

    # Nothing outside a body may depend on one of its members. Such a node is
    # normally absorbed into the forward closure; a surviving edge here means
    # the member was pulled in by another loop (overlap, rejected above) or the
    # dependency crosses a boundary through a loop node (rejected just above).
    for node in nodes:
        if node.kind == "loop" or node.id in owners:
            continue
        for dependency in node.depends_on:
            if dependency in owners:
                raise ValidationError(
                    f"node {node.id} cannot depend on loop body node {dependency}; "
                    f"depend on loop {owners[dependency]} instead"
                )

    # The body subgraph must be a DAG on its own.
    for loop_id, spec in loops.items():
        _assert_body_acyclic(by_id, loop_id, spec.body)
    return loops


def _assert_body_acyclic(by_id: dict[str, Node], loop_id: str, body: frozenset[str]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise ValidationError(f"loop {loop_id} body contains a dependency cycle")
        if node_id in visited:
            return
        visiting.add(node_id)
        for dependency in by_id[node_id].depends_on:
            if dependency in body:
                visit(dependency)
        visiting.remove(node_id)
        visited.add(node_id)

    for member_id in body:
        visit(member_id)


def _assert_acyclic(by_id: dict[str, Node], nodes: tuple[Node, ...], loops: dict[str, LoopSpec]) -> None:
    """Validate the outer graph.

    Loop bodies are sealed subgraphs validated separately, so edges into body
    members are not traversed here; the loop node stands in for the whole body.
    """
    owners: dict[str, str] = {}
    for loop_id, spec in loops.items():
        for member_id in spec.body:
            owners[member_id] = loop_id

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise ValidationError("workflow contains a dependency cycle")
        if node_id in visited:
            return
        visiting.add(node_id)
        node = by_id[node_id]
        if node.kind == "loop":
            for dependency in node.depends_on:
                visit(dependency)
            _assert_body_acyclic(by_id, node.id, loops[node.id].body)
        else:
            owner = owners.get(node_id)
            for dependency in node.depends_on:
                if owner is None or owners.get(dependency) == owner:
                    visit(dependency)
        visiting.remove(node_id)
        visited.add(node_id)

    for node in nodes:
        visit(node.id)
