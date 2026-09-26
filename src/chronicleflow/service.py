from __future__ import annotations

import json
import logging
import math
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator

from .errors import ConflictError, NotFoundError, ValidationError
from .model import Node, Workflow, _finite_json, _identifier
from .notify import NOTIFY_EVENT_TYPES, parse_subscriptions
from .schedule import Cron, parse_schedule
from .store import Store

logger = logging.getLogger("chronicleflow")


def _parse_timestamp(value: str | None, field: str) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp ending in Z; absent means no boundary."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValidationError(f"{field} must be an ISO-8601 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValidationError(f"{field} must be an ISO-8601 UTC timestamp ending in Z") from error
    return parsed


def _parse_stored_time(value: str) -> datetime:
    """Parse a timestamp the service itself stored (always a UTC value ending in Z)."""
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _prometheus_label(value: str) -> str:
    """Escape a label value for the Prometheus text exposition format."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


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


def _new_iteration(map_ids: tuple[str, ...] = ()) -> dict[str, Any]:
    iteration: dict[str, Any] = {
        "completed_nodes": [],
        "skipped_nodes": [],
        "condition_results": {},
        "outputs": {},
        "attempts": {},
    }
    if map_ids:
        # A loop body containing map nodes tracks each nested map per
        # iteration: instances belong to the round that expanded them.
        iteration["maps"] = {map_id: _new_map_state() for map_id in map_ids}
    return iteration


def _new_loop_state() -> dict[str, Any]:
    return {"status": "pending", "current_iteration": 0, "iterations": [], "end_reason": None}


def _new_map_state() -> dict[str, Any]:
    return {"status": "pending", "instances": [], "outputs": [], "failure_reason": None}


def _new_map_instance(index: int) -> dict[str, Any]:
    return {
        "index": index,
        "status": "ready",
        "output": None,
        "failure_reason": None,
    }


def _map_state_for(
    state: dict[str, Any], map_id: str, loop_id: str | None = None, iteration: int | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Locate a map node's state record and the container holding its conclusions.

    A map node inside a loop body keeps its record in the owning iteration
    (instances belong to that round alone); every other map keeps its record
    at the execution level.
    """
    if loop_id is not None:
        container = state["loops"][loop_id]["iterations"][iteration - 1]
        return container["maps"][map_id], container
    return state["maps"][map_id], state


def _iteration_started_payload(loop_id: str, iteration: int, map_ids: tuple[str, ...]) -> dict[str, Any]:
    """The iteration-start record; a nested map's skeleton rides along so
    replay can rebuild the iteration's map ownership from the event stream."""
    payload: dict[str, Any] = {"loop_id": loop_id, "iteration": iteration}
    if map_ids:
        payload["maps"] = {map_id: _new_map_state() for map_id in map_ids}
    return payload


def _resolve_path(value: Any, path: str) -> Any:
    """Follow a dot-separated path; return None when any segment is missing."""
    current = value
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def _has_approval_points(workflow: Workflow) -> bool:
    """A workflow declares an approval point on a task or on a map template."""
    for node in workflow.nodes:
        if node.kind == "task" and node.approval is not None:
            return True
        if node.kind == "map" and node.template.approval is not None:
            return True
    return False


def _declared_map_states(workflow: Workflow) -> dict[str, dict[str, Any]]:
    """Execution-level map records: maps inside a loop body are excluded —
    their state lives in the loop's per-iteration records instead."""
    bodies = workflow.loop_bodies()
    body_members = set().union(*bodies.values()) if bodies else set()
    return {
        node.id: _new_map_state()
        for node in workflow.nodes
        if node.kind == "map" and node.id not in body_members
    }


def _nested_map_ids(workflow: Workflow) -> dict[str, tuple[str, ...]]:
    """Map each loop id to the sorted ids of the map nodes inside its body."""
    by_id = {node.id: node for node in workflow.nodes}
    nested: dict[str, tuple[str, ...]] = {}
    for loop_id, body in workflow.loop_bodies().items():
        map_ids = tuple(sorted(member for member in body if by_id[member].kind == "map"))
        if map_ids:
            nested[loop_id] = map_ids
    return nested


# Requests without a tenant identifier live in the legacy namespace, whose
# behavior is unchanged from before multi-tenancy existed.
DEFAULT_TENANT = ""

DEFAULT_LEASE_SECONDS = 30.0

# How often the background scheduler looks for due schedules.
SCHEDULER_TICK_SECONDS = 0.05

# Metered action types, in ascending identifier order. Only requests that
# carry a tenant identifier are metered; the legacy namespace stays unmetered.
USAGE_TYPE_WORKFLOW_CREATED = "workflow_created"
USAGE_TYPE_EXECUTION_STARTED = "execution_started"
USAGE_TYPE_SCHEDULE_TRIGGERED = "schedule_triggered"
USAGE_TYPE_DELIVERY_ATTEMPTED = "delivery_attempted"
USAGE_TYPES = (
    USAGE_TYPE_DELIVERY_ATTEMPTED,
    USAGE_TYPE_EXECUTION_STARTED,
    USAGE_TYPE_SCHEDULE_TRIGGERED,
    USAGE_TYPE_WORKFLOW_CREATED,
)

# Unit price per metered action, in integer cents. Positive by contract and
# reported verbatim on every bill line.
USAGE_UNIT_PRICES = {
    USAGE_TYPE_WORKFLOW_CREATED: 1000,
    USAGE_TYPE_EXECUTION_STARTED: 100,
    USAGE_TYPE_SCHEDULE_TRIGGERED: 50,
    USAGE_TYPE_DELIVERY_ATTEMPTED: 10,
}

# Termination reasons in ascending identifier order. The metrics status
# distribution always reports every reason, zero when no execution ended for
# it, so the breakdown shape never depends on the recorded facts.
TERMINATION_REASONS = ("cancelled", "rejected", "retries_exhausted", "timeout")


class ChronicleFlow:
    def __init__(self, database: str):
        self.store = Store(database)
        # Per-thread notification state: events appended inside an operation
        # are buffered and delivered only after the operation commits.
        self._local = threading.local()
        self._scheduler = threading.Thread(target=self._scheduler_loop, daemon=True, name="chronicleflow-scheduler")
        self._scheduler.start()

    def _scheduler_loop(self) -> None:
        """Fire due schedules in the background; a failing pass never stops the loop."""
        while True:
            try:
                with self._operation():
                    with self.store.transaction():
                        self._process_schedules()
            except Exception:
                logger.exception("scheduled processing pass failed")
            time.sleep(SCHEDULER_TICK_SECONDS)

    def _idempotent(self, key: str | None, operation: str, action: Callable[[], dict[str, Any]], tenant: str) -> dict[str, Any]:
        if not key:
            raise ValidationError("Idempotency-Key header is required")
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT operation, response FROM idempotency WHERE tenant = ? AND key = ?",
                (tenant, key),
            ).fetchone()
            if existing:
                if existing["operation"] != operation:
                    raise ConflictError("idempotency key was already used for another operation")
                return self.store.decode(existing["response"])
            response = action()
            connection.execute(
                "INSERT INTO idempotency(tenant, key, operation, response) VALUES (?, ?, ?, ?)",
                (tenant, key, operation, self.store.encode(response)),
            )
            return response

    @contextmanager
    def _operation(self) -> Iterator[None]:
        """Scope a public operation so buffered notifications drain once, after commit."""
        depth = getattr(self._local, "depth", 0)
        self._local.depth = depth + 1
        failed = True
        try:
            yield
            failed = False
        finally:
            self._local.depth = depth
            if depth == 0:
                pending = getattr(self._local, "pending", [])
                self._local.pending = []
                # A failed operation rolls its events back, so nothing is
                # delivered for it; a delivery problem never reaches the caller.
                if not failed:
                    for notice in pending:
                        try:
                            self._deliver_notice(notice)
                        except Exception:
                            logger.exception("notification delivery failed")

    def _subscriptions_for(self, execution_id: str, tenant: str) -> list[tuple[str, dict[str, Any]]]:
        """Return (label, subscription) pairs for an execution.

        Workflow subscriptions are those declared on the exact revision the
        execution is bound to (the empty tag for an unversioned workflow);
        execution subscriptions apply on top.
        """
        row = self.store.connection.execute(
            "SELECT workflow_id, workflow_version FROM executions WHERE tenant = ? AND id = ?",
            (tenant, execution_id),
        ).fetchone()
        if not row:
            return []
        pairs: list[tuple[str, dict[str, Any]]] = []
        owners = [("workflow", row["workflow_id"], row["workflow_version"] or ""), ("execution", execution_id, "")]
        for owner_type, owner_id, owner_version in owners:
            rows = self.store.connection.execute(
                "SELECT version, position, document FROM subscriptions "
                "WHERE tenant = ? AND owner_type = ? AND owner_id = ? AND version = ? ORDER BY position",
                (tenant, owner_type, owner_id, owner_version),
            ).fetchall()
            for sub_row in rows:
                label = f"{owner_type}:{sub_row['version']}:{sub_row['position']}" if sub_row["version"] else f"{owner_type}:{sub_row['position']}"
                pairs.append((label, self.store.decode(sub_row["document"])))
        return pairs

    def _deliver_notice(self, notice: dict[str, Any]) -> None:
        tenant = notice["tenant"]
        for label, subscription in self._subscriptions_for(notice["execution_id"], tenant):
            if "url" not in subscription:
                # A queue target is fed synchronously when its event is
                # appended; there is no outbound HTTP delivery for it.
                continue
            if notice["type"] not in subscription["events"]:
                continue
            # The idempotency key is deterministic per event and subscription:
            # retries of this delivery reuse it, other events never share it.
            key = f"{notice['execution_id']}:{notice['sequence']}:{label}"
            record = self._attempt_delivery(subscription, notice, key)
            try:
                self._insert_delivery(tenant, notice["execution_id"], record)
            except Exception as error:
                # A history write failure must not make the attempt vanish:
                # record it explicitly as a failed delivery in a fresh
                # transaction rather than swallowing the exception silently.
                logger.exception("delivery history write failed")
                fallback = dict(record)
                fallback["status"] = "failed"
                fallback["persistence_error"] = str(error)
                try:
                    self._insert_delivery(tenant, notice["execution_id"], fallback)
                except Exception:
                    # The database itself cannot accept the record; leave a
                    # trace in the log instead of failing the caller.
                    logger.exception("failed delivery history record could not be written")

    def _insert_delivery(self, tenant: str, execution_id: str, record: dict[str, Any]) -> None:
        with self.store.transaction() as connection:
            sequence_row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM deliveries "
                "WHERE tenant = ? AND execution_id = ?",
                (tenant, execution_id),
            ).fetchone()
            connection.execute(
                "INSERT INTO deliveries(tenant, execution_id, sequence, document) VALUES (?, ?, ?, ?)",
                (tenant, execution_id, sequence_row["sequence"], self.store.encode(record)),
            )

    def _record_usage(self, tenant: str, usage_type: str) -> None:
        """Append one metered usage record for a tenant on the open transaction.

        Only tenant-scoped requests are metered: the legacy namespace (empty
        tenant) keeps no records at all. Callers run inside a store
        transaction so the record commits atomically with the action it meters
        and rolls back when that action is rejected.
        """
        if not tenant:
            return
        sequence_row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM usage_records WHERE tenant = ?",
            (tenant,),
        ).fetchone()
        self.store.connection.execute(
            "INSERT INTO usage_records(tenant, sequence, type, created_at) VALUES (?, ?, ?, ?)",
            (tenant, sequence_row["sequence"], usage_type, self.store.now()),
        )

    def _record_delivery_attempt_usage(self, tenant: str) -> None:
        """Meter a delivery attempt in its own transaction.

        Deliveries run after the triggering operation commits, so they hold no
        open transaction; every HTTP attempt is metered once regardless of its
        final result, including when the delivery history write fails.
        """
        if not tenant:
            return
        with self.store.transaction():
            self._record_usage(tenant, USAGE_TYPE_DELIVERY_ATTEMPTED)


    def _attempt_delivery(self, subscription: dict[str, Any], notice: dict[str, Any], key: str) -> dict[str, Any]:
        message = {"event_type": notice["type"], "execution_id": notice["execution_id"], **notice["payload"]}
        body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
        tries: list[dict[str, Any]] = []
        status = "failed"
        for attempt in range(1, subscription["max_attempts"] + 1):
            if attempt > 1:
                # Increasing backoff between attempts of the same delivery.
                time.sleep(min(0.1 * (2 ** (attempt - 2)), 1.0))
            # Every HTTP attempt is metered once, whether it eventually
            # succeeds or fails; it runs after the triggering operation
            # committed, in its own transaction.
            self._record_delivery_attempt_usage(notice["tenant"])
            request = urllib.request.Request(
                subscription["url"],
                data=body,
                headers={"Content-Type": "application/json", "Idempotency-Key": key},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=subscription["timeout_seconds"]) as response:
                    tries.append({"status_code": response.status})
                status = "delivered"
                break
            except urllib.error.HTTPError as error:
                tries.append({"status_code": error.code})
            except Exception as error:
                reason = getattr(error, "reason", error)
                tries.append({"error": str(reason)})
        return {
            "url": subscription["url"],
            "event_type": notice["type"],
            "event_sequence": notice["sequence"],
            "idempotency_key": key,
            "attempts": tries,
            "attempt_count": len(tries),
            "status": status,
            "occurred_at": self.store.now(),
        }

    def deliveries(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        self.get_execution(execution_id, tenant)
        rows = self.store.connection.execute(
            "SELECT sequence, document FROM deliveries WHERE tenant = ? AND execution_id = ? ORDER BY sequence",
            (tenant, execution_id),
        ).fetchall()
        return {"deliveries": [{"sequence": row["sequence"], **self.store.decode(row["document"])} for row in rows]}

    # --- outbound message queues -------------------------------------------

    def _register_queue_targets(
        self, owner_type: str, owner_id: str, subscriptions: list[dict[str, Any]] | None, tenant: str
    ) -> None:
        """Record the queue names a workflow or execution declares.

        Queue names are unique per tenant across declarations: a name already
        declared by a different workflow or execution is a conflict, exactly
        like a reused identifier. The same owner may re-declare its own names,
        so a workflow adding a version keeps its queue targets.
        """
        names = [subscription["queue"] for subscription in subscriptions or [] if "queue" in subscription]
        if len(set(names)) != len(names):
            raise ConflictError("queue names declared by one subscription list must be unique")
        for name in names:
            row = self.store.connection.execute(
                "SELECT owner_type, owner_id FROM queue_targets WHERE tenant = ? AND name = ?",
                (tenant, name),
            ).fetchone()
            if row is not None and (row["owner_type"], row["owner_id"]) != (owner_type, owner_id):
                raise ConflictError(f"queue {name} is already declared in this tenant")
            self.store.connection.execute(
                "INSERT OR IGNORE INTO queue_targets(tenant, name, owner_type, owner_id) VALUES (?, ?, ?, ?)",
                (tenant, name, owner_type, owner_id),
            )

    def _instantiate_queues(
        self,
        execution_id: str,
        workflow_id: str,
        bound_version: str | None,
        subscriptions: list[dict[str, Any]] | None,
        tenant: str,
    ) -> None:
        """Create the execution's queue instances from its effective declarations.

        The bound workflow revision's queue targets apply to every execution
        of that revision; the execution's own queue targets apply on top.
        """
        targets: list[dict[str, Any]] = []
        rows = self.store.connection.execute(
            "SELECT document FROM subscriptions WHERE tenant = ? AND owner_type = 'workflow' "
            "AND owner_id = ? AND version = ? ORDER BY position",
            (tenant, workflow_id, bound_version or ""),
        ).fetchall()
        for row in rows:
            subscription = self.store.decode(row["document"])
            if "queue" in subscription:
                targets.append(subscription)
        for subscription in subscriptions or []:
            if "queue" in subscription:
                targets.append(subscription)
        for position, subscription in enumerate(targets):
            self.store.connection.execute(
                "INSERT INTO queues(tenant, execution_id, name, position, document, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    tenant,
                    execution_id,
                    subscription["queue"],
                    position,
                    self.store.encode(subscription),
                    self.store.now(),
                ),
            )

    def _enqueue_queue_messages(
        self, execution_id: str, event_sequence: int, event_type: str, payload: dict[str, Any], tenant: str
    ) -> None:
        """Append a message to each of the execution's queues subscribed to the event type."""
        rows = self.store.connection.execute(
            "SELECT name, document FROM queues WHERE tenant = ? AND execution_id = ? ORDER BY position",
            (tenant, execution_id),
        ).fetchall()
        for row in rows:
            subscription = self.store.decode(row["document"])
            if event_type not in subscription["events"]:
                continue
            sequence_row = self.store.connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM queue_messages "
                "WHERE tenant = ? AND execution_id = ? AND queue = ?",
                (tenant, execution_id, row["name"]),
            ).fetchone()
            # The idempotency key is deterministic per event and queue: every
            # delivery of this message reuses it, and other events never share it.
            key = f"{execution_id}:{event_sequence}:queue:{row['name']}"
            message = {
                "event_type": event_type,
                "event_sequence": event_sequence,
                "idempotency_key": key,
                "payload": payload,
                "status": "pending",
                "delivery_count": 0,
                "deliveries": [],
                "invisible_until": None,
                "enqueued_at": self.store.now(),
            }
            self.store.connection.execute(
                "INSERT INTO queue_messages(tenant, execution_id, queue, sequence, document) VALUES (?, ?, ?, ?, ?)",
                (tenant, execution_id, row["name"], sequence_row["sequence"], self.store.encode(message)),
            )

    def _queue_row(self, execution_id: str, name: str, tenant: str) -> Any:
        return self.store.connection.execute(
            "SELECT document FROM queues WHERE tenant = ? AND execution_id = ? AND name = ?",
            (tenant, execution_id, name),
        ).fetchone()

    def _queue_message_rows(self, execution_id: str, name: str, tenant: str) -> list[Any]:
        return self.store.connection.execute(
            "SELECT sequence, document FROM queue_messages "
            "WHERE tenant = ? AND execution_id = ? AND queue = ? ORDER BY sequence",
            (tenant, execution_id, name),
        ).fetchall()

    @staticmethod
    def _effective_queue_status(message: dict[str, Any], now: float) -> str:
        """A delivered message whose visibility expired is pending again."""
        if (
            message["status"] == "delivered"
            and message["invisible_until"] is not None
            and now >= message["invisible_until"]
        ):
            return "pending"
        return message["status"]

    def _render_queue_message(self, sequence: int, message: dict[str, Any], now: float) -> dict[str, Any]:
        # The sequence leads; every other field follows in stable key order.
        return {
            "sequence": sequence,
            "deliveries": message["deliveries"],
            "delivery_count": message["delivery_count"],
            "enqueued_at": message["enqueued_at"],
            "event_sequence": message["event_sequence"],
            "event_type": message["event_type"],
            "idempotency_key": message["idempotency_key"],
            "payload": message["payload"],
            "status": self._effective_queue_status(message, now),
        }

    def queues(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        """Queue state and delivery history for one execution, in declaration order."""
        with self._operation():
            with self.store.transaction():
                self.get_execution(execution_id, tenant)
                now = time.time()
                rows = self.store.connection.execute(
                    "SELECT name, document FROM queues WHERE tenant = ? AND execution_id = ? ORDER BY position",
                    (tenant, execution_id),
                ).fetchall()
                result = []
                for row in rows:
                    subscription = self.store.decode(row["document"])
                    messages = [
                        self._render_queue_message(
                            message_row["sequence"], self.store.decode(message_row["document"]), now
                        )
                        for message_row in self._queue_message_rows(execution_id, row["name"], tenant)
                    ]
                    result.append(
                        {
                            "messages": messages,
                            "queue": row["name"],
                            "visibility_seconds": subscription["visibility_seconds"],
                        }
                    )
                return {"queues": result}

    def pull_queue(
        self, execution_id: str, name: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        self._empty_body(raw, "pull")

        def apply() -> dict[str, Any]:
            self.get_execution(execution_id, tenant)
            queue_row = self._queue_row(execution_id, name, tenant)
            if queue_row is None:
                raise NotFoundError(f"queue {name} was not found for execution {execution_id}")
            subscription = self.store.decode(queue_row["document"])
            visibility = subscription["visibility_seconds"]
            now = time.time()
            delivered = []
            for message_row in self._queue_message_rows(execution_id, name, tenant):
                message = self.store.decode(message_row["document"])
                if self._effective_queue_status(message, now) != "pending":
                    continue
                # The message leaves the pending set until the visibility
                # deadline; unacknowledged by then, it becomes pending again.
                message["status"] = "delivered"
                message["invisible_until"] = now + visibility
                message["delivery_count"] += 1
                message["deliveries"].append(
                    {"delivered_at": self.store.now(), "delivery": message["delivery_count"]}
                )
                self.store.connection.execute(
                    "UPDATE queue_messages SET document = ? "
                    "WHERE tenant = ? AND execution_id = ? AND queue = ? AND sequence = ?",
                    (self.store.encode(message), tenant, execution_id, name, message_row["sequence"]),
                )
                # Every delivery to the caller is metered as a delivery
                # attempt, atomically with the pull that made it.
                self._record_usage(tenant, USAGE_TYPE_DELIVERY_ATTEMPTED)
                delivered.append(self._render_queue_message(message_row["sequence"], message, now))
            # An empty queue (or one with nothing currently deliverable) is a
            # definite empty result, not an error.
            return {"messages": delivered}

        with self._operation():
            return self._idempotent(key, f"pull-queue:{execution_id}:{name}", apply, tenant)

    def ack_queue(
        self, execution_id: str, name: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"idempotency_key"}:
            raise ValidationError("ack body must contain exactly an idempotency_key string")
        idempotency_key = raw["idempotency_key"]
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValidationError("ack idempotency_key must be a non-empty string")

        def apply() -> dict[str, Any]:
            self.get_execution(execution_id, tenant)
            if self._queue_row(execution_id, name, tenant) is None:
                raise NotFoundError(f"queue {name} was not found for execution {execution_id}")
            for message_row in self._queue_message_rows(execution_id, name, tenant):
                message = self.store.decode(message_row["document"])
                if message["idempotency_key"] != idempotency_key:
                    continue
                if message["status"] == "acknowledged":
                    # A repeated acknowledgement names a message that already
                    # left the queue: it is missing, like an unknown key.
                    raise NotFoundError(f"queue message {idempotency_key} was not found")
                message["status"] = "acknowledged"
                message["invisible_until"] = None
                self.store.connection.execute(
                    "UPDATE queue_messages SET document = ? "
                    "WHERE tenant = ? AND execution_id = ? AND queue = ? AND sequence = ?",
                    (self.store.encode(message), tenant, execution_id, name, message_row["sequence"]),
                )
                return {"acknowledged": True}
            raise NotFoundError(f"queue message {idempotency_key} was not found")

        with self._operation():
            return self._idempotent(key, f"ack-queue:{execution_id}:{name}", apply, tenant)

    # --- quotas ---------------------------------------------------------

    def declare_quota(self, raw: Any, key: str | None, tenant: str) -> dict[str, Any]:
        if not tenant:
            raise ValidationError("tenant id must be a non-empty string")
        if not isinstance(raw, dict) or set(raw) != {"workflows", "executions"}:
            raise ValidationError("quota must contain exactly workflows and executions")
        limits = {}
        for field in ("workflows", "executions"):
            value = raw[field]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationError(f"quota {field} must be a positive integer")
            if value <= 0:
                raise ValidationError(f"quota {field} must be a positive integer")
            limits[field] = value

        def apply() -> dict[str, Any]:
            self.store.connection.execute(
                "INSERT INTO quotas(tenant, workflows, executions, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(tenant) DO UPDATE SET workflows = excluded.workflows, "
                "executions = excluded.executions, updated_at = excluded.updated_at",
                (tenant, limits["workflows"], limits["executions"], self.store.now()),
            )
            return {"quota": {"workflows": limits["workflows"], "executions": limits["executions"]}}

        with self._operation():
            return self._idempotent(key, "declare-quota", apply, tenant)

    def get_quota(self, tenant: str) -> dict[str, Any]:
        if not tenant:
            raise ValidationError("tenant id must be a non-empty string")
        with self._operation():
            with self.store.transaction():
                row = self.store.connection.execute(
                    "SELECT workflows, executions FROM quotas WHERE tenant = ?",
                    (tenant,),
                ).fetchone()
                if row is None:
                    return {"quota": None}
                return {"quota": {"workflows": row["workflows"], "executions": row["executions"]}}

    def _usage_counts(self, tenant: str) -> dict[str, int]:
        rows = self.store.connection.execute(
            "SELECT type, COUNT(*) AS count FROM usage_records WHERE tenant = ? GROUP BY type",
            (tenant,),
        ).fetchall()
        counts = {row["type"]: row["count"] for row in rows}
        return {usage_type: counts[usage_type] for usage_type in USAGE_TYPES if counts.get(usage_type)}

    def usage(self, tenant: str) -> dict[str, Any]:
        if not tenant:
            raise ValidationError("tenant id must be a non-empty string")
        with self._operation():
            with self.store.transaction():
                # Types are reported in ascending identifier order; a type
                # with no records is omitted, so a tenant with no usage gets a
                # definite empty list.
                entries = [
                    {"type": usage_type, "count": count}
                    for usage_type, count in sorted(self._usage_counts(tenant).items())
                ]
                return {"usage": entries}

    def bill(self, tenant: str) -> dict[str, Any]:
        if not tenant:
            raise ValidationError("tenant id must be a non-empty string")
        with self._operation():
            with self.store.transaction():
                items = []
                total = 0
                for usage_type, count in sorted(self._usage_counts(tenant).items()):
                    unit_price = USAGE_UNIT_PRICES[usage_type]
                    subtotal = count * unit_price
                    total += subtotal
                    items.append(
                        {
                            "type": usage_type,
                            "count": count,
                            "unit_price": unit_price,
                            "subtotal": subtotal,
                        }
                    )
                return {"bill": {"items": items, "total": total}}

    # --- operational metrics ---------------------------------------------

    def metrics(
        self,
        tenant: str,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> dict[str, Any]:
        """Aggregate the tenant's persisted business facts into run metrics.

        The query is read-only: it appends no events, writes no usage records,
        and settles nothing (a due timeout stays unsettled until an operation
        observes it), so the answer reflects exactly what is stored. Every
        dimension is always present — a tenant with no recorded facts gets a
        definite all-zero result rather than an error.

        ``since`` and ``until`` bound a closed interval on the facts'
        occurrence times: a fact whose time equals either endpoint is counted.
        A window with ``since`` later than ``until`` simply contains no facts,
        so every dimension reports zero.
        """
        if not tenant:
            raise ValidationError("tenant id must be a non-empty string")
        with self._operation():
            with self.store.transaction():
                return self._metrics_counts(tenant, since, until)

    def _metrics_counts(
        self,
        tenant: str,
        since: datetime | None,
        until: datetime | None,
    ) -> dict[str, Any]:
        """Compute the metrics document; the caller validates the tenant and holds a transaction."""
        # A window with since later than until contains no facts at all, so its
        # result is the same definite zero shape as a tenant with no facts.
        empty_window = since is not None and until is not None and since > until

        distribution: dict[str, Any] = {
            "running": 0,
            "completed": 0,
            "terminated": {reason: 0 for reason in TERMINATION_REASONS},
        }
        # Each execution contributes exactly one status fact: a running
        # execution's fact is its execution_started event, while a completed or
        # terminated execution's fact is the event that established that
        # status. With no window this counts every execution once, exactly as
        # the unfiltered baseline does. An execution has at most one terminal
        # event, so the join never multiplies rows.
        if not empty_window:
            status_rows = self.store.connection.execute(
                "SELECT e.state, COALESCE(t.occurred_at, s.occurred_at) AS at "
                "FROM executions e "
                "JOIN events s ON s.tenant = e.tenant AND s.execution_id = e.id AND s.type = 'execution_started' "
                "LEFT JOIN events t ON t.tenant = e.tenant AND t.execution_id = e.id "
                "AND t.type IN ('execution_completed', 'execution_terminated') "
                "WHERE e.tenant = ?",
                (tenant,),
            ).fetchall()
            for row in status_rows:
                if not self._within_window(_parse_stored_time(row["at"]), since, until):
                    continue
                state = self.store.decode(row["state"])
                status = state["status"]
                if status == "terminated":
                    reason = state.get("termination_reason")
                    if reason in distribution["terminated"]:
                        distribution["terminated"][reason] += 1
                elif status in ("running", "completed"):
                    distribution[status] += 1

        # Completion, failure, and retry consumption are counted from
        # the recorded node events: condition evaluations and skips
        # append their own event types and are never counted here, and
        # loop body nodes accumulate one event per iteration. A version
        # migration appends no node events, so facts recorded before
        # and after it accumulate under the same node identifiers.
        completions: dict[str, int] = {}
        failures: dict[str, int] = {}
        retries: dict[str, int] = {}
        buckets = {
            "node_completed": completions,
            "node_failed": failures,
            "node_retried": retries,
        }
        if not empty_window:
            rows = self.store.connection.execute(
                "SELECT type, payload, occurred_at FROM events WHERE tenant = ? "
                "AND type IN ('node_completed', 'node_failed', 'node_retried')",
                (tenant,),
            ).fetchall()
            for row in rows:
                if not self._within_window(_parse_stored_time(row["occurred_at"]), since, until):
                    continue
                node_id = self.store.decode(row["payload"])["node_id"]
                bucket = buckets[row["type"]]
                bucket[node_id] = bucket.get(node_id, 0) + 1

        # Delivery outcomes are counted per outbound HTTP attempt, so
        # a delivery that fails and then succeeds on a retry records
        # one of each; an attempt succeeded exactly when it received a
        # 2xx status code. A delivery document carries the single
        # occurred_at of the delivery record, shared by every attempt
        # recorded in it.
        succeeded = 0
        failed = 0
        if not empty_window:
            rows = self.store.connection.execute(
                "SELECT document FROM deliveries WHERE tenant = ?",
                (tenant,),
            ).fetchall()
            for row in rows:
                document = self.store.decode(row["document"])
                if since is not None or until is not None:
                    occurred_at = document.get("occurred_at")
                    if not isinstance(occurred_at, str) or not self._within_window(
                        _parse_stored_time(occurred_at), since, until
                    ):
                        continue
                attempts = document.get("attempts")
                if not isinstance(attempts, list):
                    continue
                for attempt in attempts:
                    status_code = attempt.get("status_code") if isinstance(attempt, dict) else None
                    if isinstance(status_code, int) and 200 <= status_code < 300:
                        succeeded += 1
                    else:
                        failed += 1

        # One trigger row exists per settled schedule period, so a
        # repeated settlement of the same period counts nothing more.
        triggers: dict[str, int] = {}
        if not empty_window:
            rows = self.store.connection.execute(
                "SELECT workflow_id, triggered_at, COUNT(*) AS count FROM schedule_triggers "
                "WHERE tenant = ? GROUP BY workflow_id, triggered_at ORDER BY workflow_id",
                (tenant,),
            ).fetchall()
            for row in rows:
                if not self._within_window(_parse_stored_time(row["triggered_at"]), since, until):
                    continue
                triggers[row["workflow_id"]] = triggers.get(row["workflow_id"], 0) + row["count"]

        return {
            "status_distribution": distribution,
            "node_completions": dict(sorted(completions.items())),
            "node_failures": dict(sorted(failures.items())),
            "retry_consumption": dict(sorted(retries.items())),
            "delivery_succeeded": succeeded,
            "delivery_failed": failed,
            "schedule_triggers": dict(sorted(triggers.items())),
        }

    @staticmethod
    def _within_window(occurred_at: datetime, since: datetime | None, until: datetime | None) -> bool:
        """Closed-interval membership: a fact equal to either endpoint counts."""
        if since is not None and occurred_at < since:
            return False
        if until is not None and occurred_at > until:
            return False
        return True

    def metrics_export(
        self,
        tenant: str,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> str:
        """Render the filtered metrics in the Prometheus text exposition format.

        Every top-level dimension is one metric family named with the
        ``chronicleflow_`` prefix and the dimension's public key. Families
        appear in the documented top-level key order; samples within a family
        are ordered by ascending label value. Every family carries at least
        one zero-valued sample when it has no facts, values are decimal
        integers, every line ends with a newline, and the whole document ends
        with a single newline.
        """
        counts = self.metrics(tenant, since, until)
        status = counts["status_distribution"]
        terminated = status["terminated"]
        # Samples order by their label values: status first (completed, running,
        # then the terminated samples by ascending reason), which is the
        # ascending tuple-of-label-values order.
        lines: list[str] = [
            'chronicleflow_status_distribution{status="completed"} ' + str(status["completed"]),
            'chronicleflow_status_distribution{status="running"} ' + str(status["running"]),
        ]
        for reason in TERMINATION_REASONS:
            lines.append(
                f'chronicleflow_status_distribution{{status="terminated",reason="{reason}"}} '
                + str(terminated[reason])
            )
        for key in ("node_completions", "node_failures", "retry_consumption"):
            groups = counts[key]
            if groups:
                for label_value, value in groups.items():
                    lines.append(
                        f'chronicleflow_{key}{{node="{_prometheus_label(label_value)}"}} {value}'
                    )
            else:
                # A family with no facts still exposes one zero-valued sample.
                lines.append(f"chronicleflow_{key} 0")
        lines.append("chronicleflow_delivery_succeeded " + str(counts["delivery_succeeded"]))
        lines.append("chronicleflow_delivery_failed " + str(counts["delivery_failed"]))
        triggers = counts["schedule_triggers"]
        if triggers:
            for label_value, value in triggers.items():
                lines.append(
                    f'chronicleflow_schedule_triggers{{workflow="{_prometheus_label(label_value)}"}} {value}'
                )
        else:
            lines.append("chronicleflow_schedule_triggers 0")
        return "\n".join(lines) + "\n"

    def _quota_limits(self, tenant: str) -> Any:
        return self.store.connection.execute(
            "SELECT workflows, executions FROM quotas WHERE tenant = ?",
            (tenant,),
        ).fetchone()

    def _assert_within_quota(self, tenant: str, kind: str) -> None:
        """Reject a write that would grow a tenant past its declared quota."""
        limits = self._quota_limits(tenant)
        if limits is None:
            return
        ceiling = limits[kind]
        used = self.store.connection.execute(
            f"SELECT COUNT(*) AS used FROM {kind} WHERE tenant = ?",
            (tenant,),
        ).fetchone()["used"]
        if used >= ceiling:
            raise ConflictError(f"quota exceeded: tenant already holds {used} {kind} (quota limit is {ceiling})")

    def create_workflow(self, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        subscriptions = None
        schedule = None
        version = None
        if isinstance(raw, dict):
            # The version tag, subscriptions, and the schedule are validated
            # up front so an invalid declaration rejects the whole request
            # before anything is written.
            if "version" in raw:
                version = _identifier(raw["version"], "version")
                raw = {field: value for field, value in raw.items() if field != "version"}
            if "subscriptions" in raw:
                subscriptions = parse_subscriptions(raw["subscriptions"])
                raw = {field: value for field, value in raw.items() if field != "subscriptions"}
            if "schedule" in raw:
                schedule = parse_schedule(raw["schedule"])
                raw = {field: value for field, value in raw.items() if field != "schedule"}
        workflow = Workflow.parse(raw)

        def create() -> dict[str, Any]:
            existing = self.store.connection.execute(
                "SELECT current_version FROM workflows WHERE tenant = ? AND id = ?",
                (tenant, workflow.id),
            ).fetchone()
            if existing is not None:
                # An untagged post to an existing workflow is always an
                # identifier conflict, whether or not the workflow is versioned.
                if version is None:
                    raise ConflictError(f"workflow {workflow.id} already exists")
                duplicate = self.store.connection.execute(
                    "SELECT 1 FROM workflow_versions WHERE tenant = ? AND workflow_id = ? AND version = ?",
                    (tenant, workflow.id, version),
                ).fetchone()
                if duplicate:
                    raise ConflictError(f"workflow {workflow.id} version {version} already exists")
                position_row = self.store.connection.execute(
                    "SELECT COALESCE(MAX(position), 0) + 1 AS position FROM workflow_versions "
                    "WHERE tenant = ? AND workflow_id = ?",
                    (tenant, workflow.id),
                ).fetchone()
                self._insert_workflow_version(workflow.id, version, position_row["position"], workflow, tenant)
                # The newest declared version becomes the current snapshot; the
                # versions table keeps every prior revision immutable.
                self.store.connection.execute(
                    "UPDATE workflows SET document = ?, current_version = ? WHERE tenant = ? AND id = ?",
                    (self.store.encode(workflow.as_dict()), version, tenant, workflow.id),
                )
                self._replace_workflow_subscriptions(workflow.id, version, subscriptions, tenant)
                self._register_queue_targets("workflow", workflow.id, subscriptions, tenant)
                if schedule is not None:
                    self._insert_schedule(workflow.id, schedule, tenant)
                # An added version is itself one workflow creation, so it is
                # metered exactly like the first definition.
                self._record_usage(tenant, USAGE_TYPE_WORKFLOW_CREATED)
                result = workflow.as_dict()
                result["version"] = version
                return result
            # A duplicate identifier does not grow the holding, so it keeps the
            # ordinary identifier conflict even when the tenant is at quota.
            self._assert_within_quota(tenant, "workflows")
            try:
                self.store.connection.execute(
                    "INSERT INTO workflows(tenant, id, document, current_version) VALUES (?, ?, ?, ?)",
                    (tenant, workflow.id, self.store.encode(workflow.as_dict()), version),
                )
            except Exception as error:
                if "UNIQUE constraint" in str(error):
                    raise ConflictError(f"workflow {workflow.id} already exists") from error
                raise
            # Every workflow keeps an immutable revision at position 0. It is
            # untagged for a versionless workflow (whose executions bind to it),
            # or tagged with the first declared version.
            self._insert_workflow_version(workflow.id, version or "", 0, workflow, tenant)
            self._replace_workflow_subscriptions(workflow.id, version or "", subscriptions, tenant)
            self._register_queue_targets("workflow", workflow.id, subscriptions, tenant)
            if schedule is not None:
                self._insert_schedule(workflow.id, schedule, tenant)
            self._record_usage(tenant, USAGE_TYPE_WORKFLOW_CREATED)
            result = workflow.as_dict()
            if version is not None:
                result["version"] = version
            return result

        operation = f"create-workflow:{workflow.id}"
        if version is not None:
            # Adding two versions with the same idempotency key would otherwise
            # look like one operation; scope the key to the declared version.
            operation = f"{operation}:{version}"
        with self._operation():
            return self._idempotent(key, operation, create, tenant)

    def _insert_workflow_version(
        self, workflow_id: str, version: str, position: int, workflow: Workflow, tenant: str
    ) -> None:
        """Persist one immutable workflow revision (the empty tag is the unversioned revision)."""
        self.store.connection.execute(
            "INSERT INTO workflow_versions(tenant, workflow_id, version, position, document, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                tenant,
                workflow_id,
                version,
                position,
                self.store.encode(workflow.as_dict()),
                self.store.now(),
            ),
        )

    def _replace_workflow_subscriptions(
        self,
        workflow_id: str,
        version: str,
        subscriptions: list[dict[str, Any]] | None,
        tenant: str,
    ) -> None:
        """Attach a revision's subscriptions; absent subscriptions replace nothing."""
        if subscriptions is None:
            return
        self.store.connection.execute(
            "DELETE FROM subscriptions WHERE tenant = ? AND owner_type = 'workflow' "
            "AND owner_id = ? AND version = ?",
            (tenant, workflow_id, version),
        )
        for position, subscription in enumerate(subscriptions):
            self.store.connection.execute(
                "INSERT INTO subscriptions(tenant, owner_type, owner_id, version, position, document) "
                "VALUES (?, 'workflow', ?, ?, ?, ?)",
                (tenant, workflow_id, version, position, self.store.encode(subscription)),
            )

    def _workflow_version_rows(self, workflow_id: str, tenant: str) -> list[Any]:
        return self.store.connection.execute(
            "SELECT version, position, document, created_at FROM workflow_versions "
            "WHERE tenant = ? AND workflow_id = ? ORDER BY position",
            (tenant, workflow_id),
        ).fetchall()

    def get_workflow(self, workflow_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        """Return every stored revision in declaration order plus the current one."""
        with self._operation():
            with self.store.transaction():
                row = self.store.connection.execute(
                    "SELECT current_version FROM workflows WHERE tenant = ? AND id = ?",
                    (tenant, workflow_id),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"workflow {workflow_id} was not found")
                versions = []
                for version_row in self._workflow_version_rows(workflow_id, tenant):
                    document = self.store.decode(version_row["document"])
                    # Each entry is the stored definition carrying its tag; the
                    # unversioned revision's entry stays untagged.
                    if version_row["version"]:
                        document["version"] = version_row["version"]
                    versions.append(document)
                return {"current_version": row["current_version"], "id": workflow_id, "versions": versions}

    def _load_workflow(
        self, workflow_id: str, tenant: str, version: str | None = None
    ) -> tuple[dict[str, Any], str | None]:
        """Return the document and stored tag for the current or a named revision."""
        workflow_row = self.store.connection.execute(
            "SELECT document, current_version FROM workflows WHERE tenant = ? AND id = ?",
            (tenant, workflow_id),
        ).fetchone()
        if workflow_row is None:
            raise NotFoundError(f"workflow {workflow_id} was not found")
        if version is None:
            return self.store.decode(workflow_row["document"]), workflow_row["current_version"]
        version_row = self.store.connection.execute(
            "SELECT document FROM workflow_versions WHERE tenant = ? AND workflow_id = ? AND version = ?",
            (tenant, workflow_id, version),
        ).fetchone()
        if version_row is None:
            raise NotFoundError(f"workflow {workflow_id} version {version} was not found")
        return self.store.decode(version_row["document"]), version

    def _bound_workflow(self, execution_id: str, tenant: str) -> Workflow:
        """Load the exact immutable revision an execution is bound to.

        The empty tag is the unversioned revision, which remains stored after a
        workflow is upgraded, so an upgrade never moves an old execution.
        """
        row = self.store.connection.execute(
            "SELECT v.document FROM executions e "
            "JOIN workflow_versions v "
            "ON v.tenant = e.tenant AND v.workflow_id = e.workflow_id "
            "AND v.version = COALESCE(e.workflow_version, '') "
            "WHERE e.tenant = ? AND e.id = ?",
            (tenant, execution_id),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"execution {execution_id} was not found")
        return Workflow.parse(self.store.decode(row["document"]))

    def _insert_schedule(self, workflow_id: str, schedule: dict[str, Any], tenant: str) -> None:
        """Attach a fresh schedule declaration to a workflow; it is due from now on."""
        self.store.connection.execute(
            "INSERT INTO schedules(tenant, workflow_id, document, paused, anchor_at, cursor) "
            "VALUES (?, ?, ?, 0, ?, '') "
            "ON CONFLICT(tenant, workflow_id) DO UPDATE SET document = excluded.document, "
            "anchor_at = excluded.anchor_at, cursor = '', last_triggered_at = NULL, last_execution_id = NULL",
            (tenant, workflow_id, self.store.encode(schedule), time.time()),
        )
        # A new declaration starts a fresh period lineage, so trigger records
        # of a previous plan never make a new period look already fired.
        self.store.connection.execute(
            "DELETE FROM schedule_triggers WHERE tenant = ? AND workflow_id = ?",
            (tenant, workflow_id),
        )

    def create_execution(self, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        if not isinstance(raw, dict) or not {"id", "workflow_id", "input"} <= set(raw) <= {
            "id",
            "workflow_id",
            "input",
            "version",
            "timeout_seconds",
            "subscriptions",
        }:
            raise ValidationError(
                "execution must contain exactly id, workflow_id, input, and optionally version, timeout_seconds and subscriptions"
            )
        execution_id = _identifier(raw["id"], "execution id")
        workflow_id = _identifier(raw["workflow_id"], "workflow id")
        version = _identifier(raw["version"], "version") if "version" in raw else None
        if not isinstance(raw["input"], dict):
            raise ValidationError("input must be an object")
        _finite_json(raw["input"], "input")
        subscriptions = parse_subscriptions(raw["subscriptions"]) if "subscriptions" in raw else None
        timeout = raw.get("timeout_seconds")
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise ValidationError("timeout_seconds must be a positive number of seconds")
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValidationError("timeout_seconds must be a positive number of seconds")

        def create() -> dict[str, Any]:
            return self._insert_execution(execution_id, workflow_id, raw["input"], timeout, subscriptions, tenant, version)

        return self._idempotent(key, f"create-execution:{execution_id}", create, tenant)

    def _insert_execution(
        self,
        execution_id: str,
        workflow_id: str,
        input_data: dict[str, Any],
        timeout: float | None,
        subscriptions: list[dict[str, Any]] | None,
        tenant: str = DEFAULT_TENANT,
        version: str | None = None,
    ) -> dict[str, Any]:
        """Insert a running execution and its start event; caller holds a transaction."""
        document, bound_version = self._load_workflow(workflow_id, tenant, version)
        workflow = Workflow.parse(document)
        # A duplicate identifier does not grow the holding, so it keeps the
        # ordinary identifier conflict even when the tenant is at quota.
        existing = self.store.connection.execute(
            "SELECT 1 FROM executions WHERE tenant = ? AND id = ?",
            (tenant, execution_id),
        ).fetchone()
        if existing:
            raise ConflictError(f"execution {execution_id} already exists")
        self._assert_within_quota(tenant, "executions")
        loops = {node.id: _new_loop_state() for node in workflow.nodes if node.kind == "loop"}
        maps = _declared_map_states(workflow)
        has_approvals = _has_approval_points(workflow)
        deadline = time.time() + timeout if timeout is not None else None
        state = {
            "id": execution_id,
            "workflow_id": workflow_id,
            "status": "running",
            "termination_reason": None,
            "timeout_seconds": timeout,
            "deadline_at": deadline,
            "input": input_data,
            "completed_nodes": [],
            "skipped_nodes": [],
            "failed_nodes": [],
            "condition_results": {},
            "outputs": {},
            "attempts": {},
            "loops": loops,
        }
        # Dynamic map state exists only for workflows that declare map nodes,
        # so every other execution keeps its baseline state shape.
        if maps:
            state["maps"] = maps
        # Approval state exists only for workflows that declare approval
        # points, so every other execution keeps its baseline state shape.
        if has_approvals:
            state["waiting_approval"] = None
            state["approvals"] = []
        # The binding tag is part of the state only for a named revision; an
        # execution of an unversioned workflow gains no new field.
        if bound_version:
            state["version"] = bound_version
        try:
            self.store.connection.execute(
                "INSERT INTO executions(tenant, id, workflow_id, state, workflow_version) VALUES (?, ?, ?, ?, ?)",
                (tenant, execution_id, workflow_id, self.store.encode(state), bound_version),
            )
        except Exception as error:
            if "UNIQUE constraint" in str(error):
                raise ConflictError(f"execution {execution_id} already exists") from error
            raise
        for position, subscription in enumerate(subscriptions or []):
            self.store.connection.execute(
                "INSERT INTO subscriptions(tenant, owner_type, owner_id, version, position, document) "
                "VALUES (?, 'execution', ?, '', ?, ?)",
                (tenant, execution_id, position, self.store.encode(subscription)),
            )
        # Queue targets declared on the bound workflow revision or the
        # execution itself become the execution's own queue instances.
        self._register_queue_targets("execution", execution_id, subscriptions, tenant)
        self._instantiate_queues(execution_id, workflow_id, bound_version, subscriptions, tenant)
        started_payload: dict[str, Any] = {
            "workflow_id": workflow_id,
            "input": input_data,
            "loops": loops,
            "timeout_seconds": timeout,
            "deadline_at": deadline,
        }
        if maps:
            started_payload["maps"] = maps
        if bound_version:
            started_payload["version"] = bound_version
        if has_approvals:
            started_payload["waiting_approval"] = None
            started_payload["approvals"] = []
        self._append(
            execution_id,
            "execution_started",
            started_payload,
            tenant,
        )
        # An execution is metered once at creation; the same insert path backs
        # scheduled runs, which additionally record a schedule_triggered usage.
        self._record_usage(tenant, USAGE_TYPE_EXECUTION_STARTED)
        return state

    def get_execution(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        with self._operation():
            row = self.store.connection.execute(
                "SELECT state FROM executions WHERE tenant = ? AND id = ?",
                (tenant, execution_id),
            ).fetchone()
            if not row:
                raise NotFoundError(f"execution {execution_id} was not found")
            state = self.store.decode(row["state"])
            self._maybe_timeout(execution_id, state, tenant)
            return state

    def _maybe_timeout(self, execution_id: str, state: dict[str, Any], tenant: str) -> None:
        deadline = state.get("deadline_at")
        if state["status"] == "running" and deadline is not None and time.time() >= deadline:
            self._terminate(execution_id, state, "timeout", tenant=tenant)
            self.store.connection.execute(
                "UPDATE executions SET state = ? WHERE tenant = ? AND id = ?",
                (self.store.encode(state), tenant, execution_id),
            )

    def _terminate(
        self,
        execution_id: str,
        state: dict[str, Any],
        reason: str,
        tenant: str = DEFAULT_TENANT,
        extra: dict[str, Any] | None = None,
    ) -> None:
        state["status"] = "terminated"
        state["termination_reason"] = reason
        # A termination (rejection, timeout, or cancellation) dismisses any
        # approval point the execution was parked at; its request event remains
        # in the stream as the record of the wait.
        if "waiting_approval" in state:
            state["waiting_approval"] = None
        payload = {"reason": reason}
        if extra:
            payload.update(extra)
        self._append(execution_id, "execution_terminated", payload, tenant)

    def events(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> list[dict[str, Any]]:
        self.get_execution(execution_id, tenant)
        rows = self.store.connection.execute(
            "SELECT sequence, type, payload, occurred_at FROM events "
            "WHERE tenant = ? AND execution_id = ? ORDER BY sequence",
            (tenant, execution_id),
        ).fetchall()
        return [
            {"sequence": row["sequence"], "type": row["type"], "payload": self.store.decode(row["payload"]), "occurred_at": row["occurred_at"]}
            for row in rows
        ]

    def advance(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) not in (
            {"output"},
            {"failure"},
            {"output", "worker_id"},
            {"failure", "worker_id"},
        ):
            raise ValidationError("advance body must contain exactly an output object or a failure object")
        worker_id = raw.get("worker_id")
        if worker_id is not None:
            worker_id = _identifier(worker_id, "worker id")
        if "output" in raw:
            if not isinstance(raw["output"], dict):
                raise ValidationError("advance output must be an object")
            _finite_json(raw["output"], "output")
        else:
            failure = raw["failure"]
            if not isinstance(failure, dict) or set(failure) != {"reason"} or not isinstance(failure["reason"], str):
                raise ValidationError("advance failure must contain exactly a reason string")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id, tenant)
            if state["status"] != "running":
                return state
            self._assert_submission_allowed(execution_id, worker_id, tenant)
            # Parked at an approval point: the decision operation is the only
            # way forward, so an advance absorbs its output or failure and
            # returns the current state unchanged. Lease ownership is still
            # enforced, exactly as for any other running-execution submission.
            if state.get("waiting_approval") is not None:
                return state
            workflow = self._bound_workflow(execution_id, tenant)
            self._auto_process(execution_id, workflow, state, tenant)
            if state["status"] == "running":
                ready = self._ready_targets(workflow, state)
                if not ready:
                    raise ConflictError("execution has no ready node")
                target = ready[0]
                if target.get("map_id") is not None:
                    map_node = next(node for node in workflow.nodes if node.id == target["map_id"])
                    approval = map_node.template.approval
                else:
                    node = next(node for node in workflow.nodes if node.id == target["node_id"])
                    approval = node.approval
                if approval is not None:
                    self._request_approval(execution_id, workflow, state, target, tenant)
                elif "failure" in raw:
                    self._fail_target(execution_id, workflow, state, target, raw["failure"]["reason"], tenant)
                else:
                    self._complete_target(execution_id, workflow, state, target, raw["output"], tenant)
                if state["status"] == "running" and state.get("waiting_approval") is None:
                    self._auto_process(execution_id, workflow, state, tenant)
            self.store.connection.execute(
                "UPDATE executions SET state = ? WHERE tenant = ? AND id = ?",
                (self.store.encode(state), tenant, execution_id),
            )
            # Every path from a running start that reaches here settled at least
            # one node boundary (completed, failed, or parked at an approval),
            # so checkpoint it in the same transaction.
            self._write_checkpoint(execution_id, state, tenant)
            return state

        with self._operation():
            return self._idempotent(key, f"advance:{execution_id}", apply, tenant)

    def decision(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or not {"approver", "decision"} <= set(raw) <= {"approver", "decision", "output", "reason"}:
            raise ValidationError("decision body must contain approver and decision, plus output or reason")
        approver = raw["approver"]
        if not isinstance(approver, str):
            raise ValidationError("approver must be a string")
        verdict = raw["decision"]
        if verdict not in ("approved", "rejected"):
            raise ValidationError("decision must be \"approved\" or \"rejected\"")
        if verdict == "approved":
            if set(raw) != {"approver", "decision", "output"}:
                raise ValidationError("an approved decision must contain exactly approver, decision, and output")
            if not isinstance(raw["output"], dict):
                raise ValidationError("decision output must be an object")
            _finite_json(raw["output"], "output")
            output = raw["output"]
            reason = None
        else:
            if set(raw) != {"approver", "decision", "reason"}:
                raise ValidationError("a rejected decision must contain exactly approver, decision, and reason")
            if not isinstance(raw["reason"], str):
                raise ValidationError("decision reason must be a string")
            output = None
            reason = raw["reason"]

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id, tenant)
            waiting = state.get("waiting_approval")
            workflow = self._bound_workflow(execution_id, tenant)
            if waiting is None:
                # A duplicate of the decision that already resolved the most
                # recent approval point returns that first result; it neither
                # advances the node again nor appends another event. Any other
                # decision against an execution with no pending point conflicts.
                if self._is_repeated_decision(state, approver, verdict, output, reason):
                    return state
                raise ConflictError("execution has no pending approval decision")
            if approver not in waiting["approvers"]:
                raise ConflictError(f"approver {approver} is not allowed to decide this approval point")
            node_id = waiting["node_id"]
            context = self._approval_context(waiting)
            state["waiting_approval"] = None
            state["approvals"].append(
                {"node_id": node_id, "approver": approver, "decision": verdict, "reason": reason, **context}
            )
            payload: dict[str, Any] = {"node_id": node_id, "approver": approver, "decision": verdict, **context}
            if verdict == "rejected":
                payload["reason"] = reason
            self._append(execution_id, "approval_decided", payload, tenant)
            if verdict == "approved":
                target: dict[str, Any] = {"node_id": node_id}
                if "map_id" in waiting:
                    target = {"map_id": waiting["map_id"], "node_id": node_id, "index": waiting["index"]}
                    if "loop_id" in waiting:
                        target["loop_id"] = waiting["loop_id"]
                        target["iteration"] = waiting["iteration"]
                self._complete_target(execution_id, workflow, state, target, output, tenant)
                if state["status"] == "running":
                    self._auto_process(execution_id, workflow, state, tenant)
            elif "map_id" in waiting:
                map_id = waiting["map_id"]
                index = waiting["index"]
                map_state, _ = _map_state_for(state, map_id, waiting.get("loop_id"), waiting.get("iteration"))
                instance = next(item for item in map_state["instances"] if item["index"] == index)
                instance["status"] = "failed"
                instance["failure_reason"] = reason
                map_state["status"] = "failed"
                map_state["failure_reason"] = reason
                state["failed_nodes"].append(map_id)
                extra = {"node_id": map_id, "map_id": map_id, "index": index}
                if "loop_id" in waiting:
                    extra["loop_id"] = waiting["loop_id"]
                    extra["iteration"] = waiting["iteration"]
                self._terminate(execution_id, state, "rejected", tenant, extra)
            else:
                state["failed_nodes"].append(node_id)
                self._terminate(execution_id, state, "rejected", tenant, {"node_id": node_id})
            self.store.connection.execute(
                "UPDATE executions SET state = ? WHERE tenant = ? AND id = ?",
                (self.store.encode(state), tenant, execution_id),
            )
            # The decision settles the parked node boundary: completion on
            # approval, permanent failure on rejection.
            self._write_checkpoint(execution_id, state, tenant)
            return state

        with self._operation():
            return self._idempotent(key, f"decision:{execution_id}", apply, tenant)

    @staticmethod
    def _is_repeated_decision(
        state: dict[str, Any], approver: str, verdict: str, output: Any, reason: str | None
    ) -> bool:
        records = state.get("approvals") or []
        if not records:
            return False
        record = records[-1]
        if record["approver"] != approver or record["decision"] != verdict:
            return False
        if verdict == "rejected":
            return record["reason"] == reason
        return ChronicleFlow._recorded_output(state, record) == output

    @staticmethod
    def _recorded_output(state: dict[str, Any], record: dict[str, Any]) -> Any:
        if "map_id" in record:
            map_state, _ = _map_state_for(state, record["map_id"], record.get("loop_id"), record.get("iteration"))
            instance = next(item for item in map_state["instances"] if item["index"] == record["index"])
            return instance["output"]
        if "loop_id" in record:
            iteration = state["loops"][record["loop_id"]]["iterations"][record["iteration"] - 1]
            return iteration["outputs"].get(record["node_id"])
        return state["outputs"].get(record["node_id"])

    @staticmethod
    def _approval_context(waiting: dict[str, Any]) -> dict[str, Any]:
        context = {key: waiting[key] for key in ("loop_id", "iteration") if key in waiting}
        if "map_id" in waiting:
            context["map_id"] = waiting["map_id"]
            context["index"] = waiting["index"]
        return context

    def _request_approval(
        self, execution_id: str, workflow: Workflow, state: dict[str, Any], target: dict[str, Any], tenant: str
    ) -> None:
        node_id = target["node_id"]
        if target.get("map_id") is not None:
            map_node = next(node for node in workflow.nodes if node.id == target["map_id"])
            approvers = list(map_node.template.approval.approvers)
        else:
            node = next(node for node in workflow.nodes if node.id == node_id)
            approvers = list(node.approval.approvers)
        waiting: dict[str, Any] = {"node_id": node_id, "approvers": approvers}
        if target.get("map_id") is not None:
            # The loop context travels with the target: a nested map's
            # template id is not a declared node, so it cannot be looked up.
            if "loop_id" in target:
                waiting["loop_id"] = target["loop_id"]
                waiting["iteration"] = target["iteration"]
            waiting["map_id"] = target["map_id"]
            waiting["index"] = target["index"]
            map_state, _ = _map_state_for(state, target["map_id"], target.get("loop_id"), target.get("iteration"))
            instance = next(item for item in map_state["instances"] if item["index"] == target["index"])
            instance["status"] = "waiting"
        else:
            loop_id = self._active_loop(workflow, state, node_id)
            if loop_id is not None:
                waiting["loop_id"] = loop_id
                waiting["iteration"] = state["loops"][loop_id]["current_iteration"]
        state["waiting_approval"] = waiting
        self._append(
            execution_id,
            "approval_requested",
            {"node_id": node_id, "approvers": approvers, **self._approval_context(waiting)},
            tenant,
        )

    def cancel(self, execution_id: str, key: str | None, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id, tenant)
            if state["status"] != "running":
                return state
            self._terminate(execution_id, state, "cancelled", tenant)
            self.store.connection.execute(
                "UPDATE executions SET state = ? WHERE tenant = ? AND id = ?",
                (self.store.encode(state), tenant, execution_id),
            )
            return state

        with self._operation():
            return self._idempotent(key, f"cancel:{execution_id}", apply, tenant)

    @staticmethod
    def _reconcile_state_for_definition(state: dict[str, Any], workflow: Workflow) -> None:
        """Align the materialized summary with a newly bound revision.

        Loops introduced by the new definition start in their initial pending
        state; loops only the previous definition knew keep their recorded
        history untouched, so pre-migration conclusions never change. The
        approval fields exist as soon as the bound definition declares an
        approval point; a parked waiting point is left exactly as recorded.
        """
        for node in workflow.nodes:
            if node.kind == "loop" and node.id not in state["loops"]:
                state["loops"][node.id] = _new_loop_state()
        declared_maps = _declared_map_states(workflow)
        if declared_maps and "maps" not in state:
            state["maps"] = {}
        for map_id, map_state in declared_maps.items():
            if map_id not in state.get("maps", {}):
                state["maps"][map_id] = map_state
        if _has_approval_points(workflow) and "approvals" not in state:
            state["waiting_approval"] = None
            state["approvals"] = []

    def migrate(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"version"}:
            raise ValidationError("migrate body must contain exactly a version string")
        target = _identifier(raw["version"], "version")

        def apply() -> dict[str, Any]:
            row = self.store.connection.execute(
                "SELECT workflow_id, workflow_version FROM executions WHERE tenant = ? AND id = ?",
                (tenant, execution_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"execution {execution_id} was not found")
            workflow_id = row["workflow_id"]
            target_row = self.store.connection.execute(
                "SELECT 1 FROM workflow_versions WHERE tenant = ? AND workflow_id = ? AND version = ?",
                (tenant, workflow_id, target),
            ).fetchone()
            if target_row is None:
                raise NotFoundError(f"workflow {workflow_id} version {target} was not found")
            # get_execution settles a due timeout first, so a deadline that has
            # elapsed takes precedence over the migration, as it does for every
            # other operation.
            state = self.get_execution(execution_id, tenant)
            if state["status"] != "running":
                raise ConflictError(f"execution {execution_id} is not running")
            current = row["workflow_version"]
            if target == current:
                # Already bound to the target: report the current state without
                # appending an event or writing a checkpoint.
                return state
            document, _ = self._load_workflow(workflow_id, tenant, target)
            workflow = Workflow.parse(document)
            self._reconcile_state_for_definition(state, workflow)
            # The payload carries the target revision's structural skeleton so
            # replay can rebuild the migration point from the event stream
            # alone, exactly as it does from the execution_started record.
            payload: dict[str, Any] = {"from_version": current, "to_version": target}
            payload["loops"] = {
                node.id: _new_loop_state() for node in workflow.nodes if node.kind == "loop"
            }
            declared_maps = _declared_map_states(workflow)
            if declared_maps:
                payload["maps"] = declared_maps
            if _has_approval_points(workflow):
                payload["waiting_approval"] = None
                payload["approvals"] = []
            self._append(execution_id, "version_migrated", payload, tenant)
            state["version"] = target
            self.store.connection.execute(
                "UPDATE executions SET state = ?, workflow_version = ? WHERE tenant = ? AND id = ?",
                (self.store.encode(state), target, tenant, execution_id),
            )
            # The migration is recorded at the same node boundary: checkpoint
            # the rebound summary at the migration event's position so recovery
            # resumes on the new version without replaying it.
            self._write_checkpoint(execution_id, state, tenant)
            return state

        with self._operation():
            # Scope the operation to the target tag so two migrations of the
            # same execution to different versions sharing one key conflict,
            # exactly like two version declarations do.
            return self._idempotent(key, f"migrate:{execution_id}:{target}", apply, tenant)

    def _lease_row(self, execution_id: str, tenant: str) -> Any:
        return self.store.connection.execute(
            "SELECT worker_id, lease_seconds, expires_at, heartbeat_at FROM leases "
            "WHERE tenant = ? AND execution_id = ?",
            (tenant, execution_id),
        ).fetchone()

    @staticmethod
    def _lease_payload(worker_id: str, lease_seconds: float, expires_at: float, heartbeat_at: float) -> dict[str, Any]:
        return {
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
            "expires_at": expires_at,
            "heartbeat_at": heartbeat_at,
        }

    def _assert_submission_allowed(self, execution_id: str, worker_id: str | None, tenant: str) -> None:
        """Once a work item is claimed, only the active lease holder may submit results."""
        row = self._lease_row(execution_id, tenant)
        if row is None:
            return
        if time.time() >= row["expires_at"]:
            raise ConflictError(f"lease for execution {execution_id} has expired")
        if worker_id != row["worker_id"]:
            raise ConflictError(f"work item for execution {execution_id} is held by another worker")

    def claim(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or "worker_id" not in raw or not set(raw) <= {"worker_id", "lease_seconds"}:
            raise ValidationError("claim body must contain a worker_id and optionally lease_seconds")
        worker_id = _identifier(raw["worker_id"], "worker id")
        lease_seconds = raw.get("lease_seconds", DEFAULT_LEASE_SECONDS)
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float)):
            raise ValidationError("lease_seconds must be a positive number of seconds")
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValidationError("lease_seconds must be a positive number of seconds")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id, tenant)
            if state["status"] != "running":
                # A finished execution has no claimable work item; the request
                # is a definite empty result and absorbs no input.
                return {"work_item": None, "lease": None}
            now = time.time()
            row = self._lease_row(execution_id, tenant)
            if row is not None and now < row["expires_at"]:
                raise ConflictError(f"work item for execution {execution_id} is already claimed")
            expires_at = now + lease_seconds
            self.store.connection.execute(
                "INSERT INTO leases(tenant, execution_id, worker_id, lease_seconds, expires_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(tenant, execution_id) DO UPDATE SET worker_id = excluded.worker_id, "
                "lease_seconds = excluded.lease_seconds, expires_at = excluded.expires_at, "
                "heartbeat_at = excluded.heartbeat_at",
                (tenant, execution_id, worker_id, lease_seconds, expires_at, now),
            )
            return {
                "work_item": {"execution_id": execution_id, "workflow_id": state["workflow_id"]},
                "lease": self._lease_payload(worker_id, lease_seconds, expires_at, now),
            }

        return self._idempotent(key, f"claim:{execution_id}", apply, tenant)

    def heartbeat(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"worker_id"}:
            raise ValidationError("heartbeat body must contain exactly a worker_id")
        worker_id = _identifier(raw["worker_id"], "worker id")

        def apply() -> dict[str, Any]:
            state = self.get_execution(execution_id, tenant)
            row = self._lease_row(execution_id, tenant)
            if row is None:
                raise NotFoundError(f"execution {execution_id} has no claimed work item")
            now = time.time()
            if state["status"] != "running":
                raise ConflictError(f"execution {execution_id} is not running")
            if now >= row["expires_at"]:
                raise ConflictError(f"lease for execution {execution_id} has expired")
            if row["worker_id"] != worker_id:
                raise ConflictError(f"work item for execution {execution_id} is held by another worker")
            # A heartbeat only extends the lease and refreshes the active time;
            # it never advances nodes, writes outputs, or appends node events.
            expires_at = now + row["lease_seconds"]
            self.store.connection.execute(
                "UPDATE leases SET expires_at = ?, heartbeat_at = ? WHERE tenant = ? AND execution_id = ?",
                (expires_at, now, tenant, execution_id),
            )
            return {"lease": self._lease_payload(worker_id, row["lease_seconds"], expires_at, now)}

        return self._idempotent(key, f"heartbeat:{execution_id}", apply, tenant)

    def release(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"worker_id"}:
            raise ValidationError("release body must contain exactly a worker_id")
        worker_id = _identifier(raw["worker_id"], "worker id")

        def apply() -> dict[str, Any]:
            self.get_execution(execution_id, tenant)
            row = self._lease_row(execution_id, tenant)
            if row is None:
                raise NotFoundError(f"execution {execution_id} has no claimed work item")
            if time.time() >= row["expires_at"]:
                raise ConflictError(f"lease for execution {execution_id} has expired")
            if row["worker_id"] != worker_id:
                raise ConflictError(f"work item for execution {execution_id} is held by another worker")
            self.store.connection.execute(
                "DELETE FROM leases WHERE tenant = ? AND execution_id = ?",
                (tenant, execution_id),
            )
            return {"released": True}

        return self._idempotent(key, f"release:{execution_id}", apply, tenant)

    def checkpoints(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        self.get_execution(execution_id, tenant)
        rows = self.store.connection.execute(
            "SELECT sequence, event_sequence, document, created_at FROM checkpoints "
            "WHERE tenant = ? AND execution_id = ? ORDER BY sequence",
            (tenant, execution_id),
        ).fetchall()
        return {
            "checkpoints": [
                {
                    "sequence": row["sequence"],
                    "event_sequence": row["event_sequence"],
                    "state": self.store.decode(row["document"])["state"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def recover(
        self, execution_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {"from"} or not isinstance(raw["from"], str):
            raise ValidationError("recover body must contain exactly a from string")
        if raw["from"] != "latest_checkpoint":
            raise ValidationError("recover from must be \"latest_checkpoint\"")

        def apply() -> dict[str, Any]:
            # get_execution applies a due timeout first, so termination always
            # takes precedence over recovery.
            state = self.get_execution(execution_id, tenant)
            if state["status"] != "running":
                return state
            row = self.store.connection.execute(
                "SELECT document FROM checkpoints WHERE tenant = ? AND execution_id = ? ORDER BY sequence DESC LIMIT 1",
                (tenant, execution_id),
            ).fetchone()
            if not row:
                raise ConflictError("execution has no checkpoint to recover from")
            try:
                document = self.store.decode(row["document"])
                snapshot = document["state"]
                event_sequence = document["event_sequence"]
            except (ValueError, KeyError, TypeError) as error:
                raise ConflictError("latest checkpoint is not parseable") from error
            if not isinstance(snapshot, dict) or not isinstance(event_sequence, int):
                raise ConflictError("latest checkpoint is not parseable")
            if snapshot != state:
                raise ConflictError("latest checkpoint does not match the materialized state")
            return snapshot

        return self._idempotent(key, f"recover:{execution_id}", apply, tenant)

    # --- schedules ------------------------------------------------------

    def _schedule_row(self, workflow_id: str, tenant: str = DEFAULT_TENANT) -> Any:
        return self.store.connection.execute(
            "SELECT workflow_id, document, paused, anchor_at, cursor, last_triggered_at, last_execution_id "
            "FROM schedules WHERE tenant = ? AND workflow_id = ?",
            (tenant, workflow_id),
        ).fetchone()

    def _assert_workflow_exists(self, workflow_id: str, tenant: str = DEFAULT_TENANT) -> None:
        row = self.store.connection.execute(
            "SELECT 1 FROM workflows WHERE tenant = ? AND id = ?",
            (tenant, workflow_id),
        ).fetchone()
        if not row:
            raise NotFoundError(f"workflow {workflow_id} was not found")

    def _schedule_status(self, row: Any) -> dict[str, Any]:
        if row is None:
            # A workflow without a declared schedule has a definite empty result.
            return {"schedule": None}
        return {
            "schedule": self.store.decode(row["document"]),
            "paused": bool(row["paused"]),
            "last_triggered_at": row["last_triggered_at"],
            "last_execution_id": row["last_execution_id"],
        }

    def schedule_status(self, workflow_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        with self._operation():
            with self.store.transaction():
                self._assert_workflow_exists(workflow_id, tenant)
                # Settle any due periods first so the answer reflects the
                # schedule as of now, not as of the last background tick.
                self._process_schedules(workflow_id, tenant)
                return self._schedule_status(self._schedule_row(workflow_id, tenant))

    @staticmethod
    def _empty_body(raw: Any, operation: str) -> None:
        if not isinstance(raw, dict) or raw:
            raise ValidationError(f"{operation} body must be an empty object")

    def pause_schedule(
        self, workflow_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        self._empty_body(raw, "pause schedule")

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id, tenant)
            if self._schedule_row(workflow_id, tenant) is None:
                raise NotFoundError(f"workflow {workflow_id} has no schedule")
            self.store.connection.execute(
                "UPDATE schedules SET paused = 1 WHERE tenant = ? AND workflow_id = ?",
                (tenant, workflow_id),
            )
            return self._schedule_status(self._schedule_row(workflow_id, tenant))

        with self._operation():
            return self._idempotent(key, f"pause-schedule:{workflow_id}", apply, tenant)

    def resume_schedule(
        self, workflow_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        self._empty_body(raw, "resume schedule")

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id, tenant)
            if self._schedule_row(workflow_id, tenant) is None:
                raise NotFoundError(f"workflow {workflow_id} has no schedule")
            self.store.connection.execute(
                "UPDATE schedules SET paused = 0 WHERE tenant = ? AND workflow_id = ?",
                (tenant, workflow_id),
            )
            # Periods that came due while paused are settled immediately
            # according to the missed policy.
            self._process_schedules(workflow_id, tenant)
            return self._schedule_status(self._schedule_row(workflow_id, tenant))

        with self._operation():
            return self._idempotent(key, f"resume-schedule:{workflow_id}", apply, tenant)

    def update_schedule(
        self, workflow_id: str, raw: Any, key: str | None, tenant: str = DEFAULT_TENANT
    ) -> dict[str, Any]:
        # The whole plan is validated before anything is written, so an
        # invalid declaration never partially replaces the stored one.
        schedule = parse_schedule(raw)

        def apply() -> dict[str, Any]:
            self._assert_workflow_exists(workflow_id, tenant)
            self._insert_schedule(workflow_id, schedule, tenant)
            return self._schedule_status(self._schedule_row(workflow_id, tenant))

        with self._operation():
            return self._idempotent(key, f"update-schedule:{workflow_id}", apply, tenant)

    def _set_schedule_cursor(self, tenant: str, workflow_id: str, cursor: str) -> None:
        self.store.connection.execute(
            "UPDATE schedules SET cursor = ? WHERE tenant = ? AND workflow_id = ?",
            (cursor, tenant, workflow_id),
        )

    def _process_schedules(self, workflow_id: str | None = None, tenant: str | None = None) -> None:
        """Settle every due schedule period once; caller holds a transaction."""
        now = time.time()
        if workflow_id is None:
            rows = self.store.connection.execute(
                "SELECT tenant, workflow_id, document, paused, anchor_at, cursor FROM schedules"
            ).fetchall()
        else:
            rows = self.store.connection.execute(
                "SELECT tenant, workflow_id, document, paused, anchor_at, cursor FROM schedules "
                "WHERE tenant = ? AND workflow_id = ?",
                (tenant or DEFAULT_TENANT, workflow_id),
            ).fetchall()
        for row in rows:
            self._process_schedule(row, now)

    def _process_schedule(self, row: Any, now: float) -> None:
        """Advance one schedule to the current time.

        Exactly one execution may exist per schedule period: the trigger row
        and the cursor together make a repeated pass for the same period a
        no-op. While paused nothing fires; a "skip" schedule consumes the
        periods it sleeps through, a "catch_up" schedule keeps them pending
        and fires only the most recent one when it is resumed.
        """
        tenant = row["tenant"]
        workflow_id = row["workflow_id"]
        document = self.store.decode(row["document"])
        paused = bool(row["paused"])
        cursor = row["cursor"]
        if "interval_seconds" in document:
            interval = document["interval_seconds"]
            anchor = row["anchor_at"]
            index = int((now - anchor) // interval)
            if index < 1:
                return
            consumed = int(cursor) if cursor else 0
            if index <= consumed:
                return
            period_key = f"i:{index}"
            fire_at = anchor + index * interval
            granularity = float(interval)
            new_cursor = str(index)
        else:
            cron = Cron.parse(document["cron"])
            candidate = int(cursor) if cursor else cron.next_after(row["anchor_at"])
            horizon = int(now // 60) * 60
            latest = None
            following = candidate
            while following is not None and following <= horizon:
                latest = following
                following = cron.next_after(following)
            if latest is None:
                if candidate is not None and str(candidate) != cursor:
                    self._set_schedule_cursor(tenant, workflow_id, str(candidate))
                return
            period_key = f"c:{latest}"
            fire_at = float(latest)
            granularity = 60.0
            # When no further match exists (an unreachable expression), keep
            # scanning from just past the current horizon on later passes.
            new_cursor = str(following) if following is not None else str(horizon + 60)
        if paused:
            if document["missed_policy"] == "skip":
                self._set_schedule_cursor(tenant, workflow_id, new_cursor)
            return
        if document["missed_policy"] == "catch_up" or now - fire_at < granularity:
            fired = self._fire_schedule(tenant, workflow_id, document, period_key)
            if fired is None:
                # The execution quota blocks this period: create nothing and
                # leave the cursor and schedule status untouched so the period
                # is retried on a later pass once capacity exists.
                return
        self._set_schedule_cursor(tenant, workflow_id, new_cursor)

    def _fire_schedule(
        self, tenant: str, workflow_id: str, document: dict[str, Any], period_key: str
    ) -> str | None:
        """Create the execution for one schedule period, or return the existing one.

        Returns None when the tenant's execution quota leaves no room; the
        period then stays pending and the schedule is left unchanged.
        """
        existing = self.store.connection.execute(
            "SELECT execution_id, triggered_at FROM schedule_triggers "
            "WHERE tenant = ? AND workflow_id = ? AND period_key = ?",
            (tenant, workflow_id, period_key),
        ).fetchone()
        if existing:
            execution_id = existing["execution_id"]
            triggered_at = existing["triggered_at"]
        else:
            execution_id = f"{workflow_id}-scheduled-{period_key}"
            claimed = self.store.connection.execute(
                "SELECT 1 FROM executions WHERE tenant = ? AND id = ?",
                (tenant, execution_id),
            ).fetchone()
            if not claimed:
                limits = self._quota_limits(tenant)
                if limits is not None:
                    used = self.store.connection.execute(
                        "SELECT COUNT(*) AS used FROM executions WHERE tenant = ?",
                        (tenant,),
                    ).fetchone()["used"]
                    if used >= limits["executions"]:
                        return None
                # A scheduled run binds the revision that is current at fire
                # time (the unversioned revision for a workflow without
                # versions); later upgrades never move it.
                # The created execution is exactly a manually created one:
                # same state shape and the usual execution_started event,
                # with nothing schedule-specific added to its stream.
                self._insert_execution(execution_id, workflow_id, document["input"], None, None, tenant)
                # A scheduled run counts both as an execution start (recorded
                # by _insert_execution) and as one schedule trigger. A
                # repeated pass for the same period takes the existing-row or
                # already-claimed path and is not metered again.
                self._record_usage(tenant, USAGE_TYPE_SCHEDULE_TRIGGERED)
            triggered_at = self.store.now()
            self.store.connection.execute(
                "INSERT INTO schedule_triggers(tenant, workflow_id, period_key, execution_id, triggered_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (tenant, workflow_id, period_key, execution_id, triggered_at),
            )
        self.store.connection.execute(
            "UPDATE schedules SET last_triggered_at = ?, last_execution_id = ? WHERE tenant = ? AND workflow_id = ?",
            (triggered_at, execution_id, tenant, workflow_id),
        )
        return execution_id

    def _node_container(self, workflow: Workflow, state: dict[str, Any], node_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return the state fragment holding the node's progress and its event context."""
        loop_id = self._active_loop(workflow, state, node_id)
        if loop_id is None:
            return state, {}
        loop_state = state["loops"][loop_id]
        return loop_state["iterations"][-1], {"loop_id": loop_id, "iteration": loop_state["current_iteration"]}

    def _complete_node(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        node_id: str,
        output: Any,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        container, context = self._node_container(workflow, state, node_id)
        container["completed_nodes"].append(node_id)
        container["outputs"][node_id] = output
        container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
        self._append(execution_id, "node_completed", {"node_id": node_id, "output": output, **context}, tenant)

    def _fail_node(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        node_id: str,
        reason: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        retries = by_id[node_id].retries or 0
        container, context = self._node_container(workflow, state, node_id)
        entry = container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
        entry["failures"] += 1
        self._append(execution_id, "node_failed", {"node_id": node_id, "attempt": entry["attempt"], "reason": reason, **context}, tenant)
        if entry["failures"] <= retries:
            entry["attempt"] += 1
            self._append(execution_id, "node_retried", {"node_id": node_id, "attempt": entry["attempt"], "reason": reason, **context}, tenant)
        else:
            state["failed_nodes"].append(node_id)
            self._terminate(execution_id, state, "retries_exhausted", tenant, {"node_id": node_id})

    def _complete_target(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        target: dict[str, Any],
        output: Any,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        if target.get("map_id") is not None:
            self._complete_map_instance(execution_id, workflow, state, target, output, tenant)
        else:
            self._complete_node(execution_id, workflow, state, target["node_id"], output, tenant)

    def _fail_target(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        target: dict[str, Any],
        reason: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        if target.get("map_id") is not None:
            self._fail_map_instance(execution_id, workflow, state, target, reason, tenant)
        else:
            self._fail_node(execution_id, workflow, state, target["node_id"], reason, tenant)

    @staticmethod
    def _map_context(target: dict[str, Any]) -> dict[str, Any]:
        """The loop ownership of a nested map target; empty for a top-level map."""
        if target.get("loop_id") is not None:
            return {"loop_id": target["loop_id"], "iteration": target["iteration"]}
        return {}

    def _complete_map_instance(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        target: dict[str, Any],
        output: Any,
        tenant: str,
    ) -> None:
        map_id = target["map_id"]
        index = target["index"]
        context = self._map_context(target)
        map_state, container = _map_state_for(state, map_id, target.get("loop_id"), target.get("iteration"))
        instance = next(item for item in map_state["instances"] if item["index"] == index)
        instance["status"] = "completed"
        instance["output"] = output
        self._append(
            execution_id,
            "node_completed",
            {"node_id": target["node_id"], "output": output, "map_id": map_id, "index": index, **context},
            tenant,
        )
        if all(item["status"] == "completed" for item in map_state["instances"]):
            self._finish_map(execution_id, workflow, state, map_id, map_state, tenant, container, context)

    def _map_instance_attempt(
        self,
        execution_id: str,
        map_id: str,
        index: int,
        tenant: str,
        loop_id: str | None = None,
        iteration: int | None = None,
    ) -> int:
        """Count node_failed events already recorded for one map instance.

        Instance records carry only index, status, output, and failure reason,
        so the attempt number is derived from the event stream (its durable
        source) rather than kept as extra state. The next failure is attempt
        ``count + 1``. A nested instance is matched by its loop and iteration
        as well, since the same map id and index recur every round.
        """
        rows = self.store.connection.execute(
            "SELECT payload FROM events WHERE tenant = ? AND execution_id = ? AND type = 'node_failed'",
            (tenant, execution_id),
        ).fetchall()
        failures = 0
        for row in rows:
            payload = self.store.decode(row["payload"])
            if (
                payload.get("map_id") == map_id
                and payload.get("index") == index
                and payload.get("loop_id") == loop_id
                and payload.get("iteration") == iteration
            ):
                failures += 1
        return failures + 1

    def _fail_map_instance(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        target: dict[str, Any],
        reason: str,
        tenant: str,
    ) -> None:
        map_id = target["map_id"]
        index = target["index"]
        context = self._map_context(target)
        map_node = next(node for node in workflow.nodes if node.id == map_id)
        retries = map_node.template.retries or 0
        map_state, _ = _map_state_for(state, map_id, target.get("loop_id"), target.get("iteration"))
        instance = next(item for item in map_state["instances"] if item["index"] == index)
        attempt = self._map_instance_attempt(
            execution_id, map_id, index, tenant, target.get("loop_id"), target.get("iteration")
        )
        self._append(
            execution_id,
            "node_failed",
            {
                "node_id": target["node_id"],
                "attempt": attempt,
                "reason": reason,
                "map_id": map_id,
                "index": index,
                **context,
            },
            tenant,
        )
        if attempt <= retries:
            self._append(
                execution_id,
                "node_retried",
                {
                    "node_id": target["node_id"],
                    "attempt": attempt + 1,
                    "reason": reason,
                    "map_id": map_id,
                    "index": index,
                    **context,
                },
                tenant,
            )
        else:
            instance["status"] = "failed"
            instance["failure_reason"] = reason
            map_state["status"] = "failed"
            map_state["failure_reason"] = reason
            state["failed_nodes"].append(map_id)
            self._terminate(
                execution_id,
                state,
                "retries_exhausted",
                tenant,
                {"node_id": map_id, "map_id": map_id, "index": index, **context},
            )

    def _expand_map(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        map_node: Node,
        map_state: dict[str, Any],
        tenant: str,
        container: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Expand a pending map once every dependency is completed or skipped.

        The element list is a dot-separated path into the recorded output of
        the source task — for a nested map, the output the source recorded in
        the owning iteration. A missing path or a value that is not an array
        expands to zero instances and completes the map with an empty output
        list. More elements than the declared bound create no instances: the
        map fails permanently and the execution terminates.
        """
        container = container if container is not None else state
        context = context if context is not None else {}
        source_output = container["outputs"].get(map_node.source)
        if map_node.source in container["skipped_nodes"]:
            # A skipped source task never recorded an output; expand zero.
            elements: Any = None
        else:
            elements = _resolve_path(source_output, map_node.path) if source_output is not None else None
        if not isinstance(elements, list):
            elements = []
        if len(elements) > map_node.max_instances:
            # The bound is exceeded before anything is created, so no instance
            # exists and no instance event is appended. The expansion event
            # records zero created instances and the observed element count.
            element_count = len(elements)
            reason = (
                f"map {map_node.id} expanded to {element_count} instances, "
                f"exceeding its max_instances bound of {map_node.max_instances}"
            )
            map_state["status"] = "failed"
            map_state["failure_reason"] = reason
            state["failed_nodes"].append(map_node.id)
            self._append(
                execution_id,
                "map_expanded",
                {
                    "map_id": map_node.id,
                    "instance_count": 0,
                    "element_count": element_count,
                    "max_instances": map_node.max_instances,
                    "exceeded": True,
                    "reason": reason,
                    **context,
                },
                tenant,
            )
            self._terminate(
                execution_id,
                state,
                "retries_exhausted",
                tenant,
                {"node_id": map_node.id, "map_id": map_node.id, **context},
            )
            return
        map_state["instances"] = [_new_map_instance(index) for index in range(len(elements))]
        map_state["status"] = "running"
        self._append(
            execution_id,
            "map_expanded",
            {"map_id": map_node.id, "instance_count": len(elements), "exceeded": False, **context},
            tenant,
        )
        if not elements:
            self._finish_map(execution_id, workflow, state, map_node.id, map_state, tenant, container, context)

    def _finish_map(
        self,
        execution_id: str,
        workflow: Workflow,
        state: dict[str, Any],
        map_id: str,
        map_state: dict[str, Any],
        tenant: str,
        container: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Complete a map node; its output is the ordered list of instance outputs.

        The conclusion lands in the owning container: the execution state for
        a top-level map, the owning iteration record for a nested one.
        """
        container = container if container is not None else state
        context = context if context is not None else {}
        map_state["status"] = "completed"
        map_state["outputs"] = [instance["output"] for instance in sorted(map_state["instances"], key=lambda item: item["index"])]
        container["outputs"][map_id] = map_state["outputs"]
        container["completed_nodes"].append(map_id)
        self._append(
            execution_id,
            "map_completed",
            {"map_id": map_id, "instance_count": len(map_state["instances"]), **context},
            tenant,
        )

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

    @staticmethod
    def _target_sort_key(target: dict[str, Any]) -> tuple[str, str, int]:
        """Lexicographic ordering of ready work by task identifier.

        Regular tasks key on their node id; map instances key on the template
        task id, tie-broken by map node id and element index so instances of
        one map stay in ascending index order. Template ids never collide with
        declared node ids, so the two spaces never interleave ambiguously.
        """
        if target.get("map_id") is not None:
            return target["node_id"], target["map_id"], target["index"]
        return target["node_id"], "", -1

    def _ready_targets(self, workflow: Workflow, state: dict[str, Any]) -> list[dict[str, Any]]:
        """All ready work items: declared tasks and expanded map instances.

        Map instances queue in ascending element index once their map is
        expanded; every advance still settles exactly the first target. A
        nested map's instances belong to the iteration that expanded them and
        carry its loop id and iteration number.
        """
        targets: list[dict[str, Any]] = [{"node_id": node_id} for node_id in self._ready_tasks(workflow, state)]
        map_nodes = {node.id: node for node in workflow.nodes if node.kind == "map"}
        nested: dict[str, tuple[str, int]] = {}
        for loop_id, loop_state in state["loops"].items():
            if loop_state["status"] != "running":
                continue
            iteration = loop_state["iterations"][-1]
            for map_id in iteration.get("maps", {}):
                nested[map_id] = (loop_id, loop_state["current_iteration"])
        for map_id, map_node in map_nodes.items():
            context: dict[str, Any] = {}
            if map_id in nested:
                loop_id, iteration_no = nested[map_id]
                map_state = state["loops"][loop_id]["iterations"][iteration_no - 1]["maps"][map_id]
                context = {"loop_id": loop_id, "iteration": iteration_no}
            else:
                map_state = state.get("maps", {}).get(map_id)
            if map_state is None or map_state["status"] != "running":
                continue
            for instance in map_state["instances"]:
                if instance["status"] == "ready":
                    targets.append(
                        {"map_id": map_id, "node_id": map_node.template.task_id, "index": instance["index"], **context}
                    )
        return sorted(targets, key=self._target_sort_key)

    def _active_loop(self, workflow: Workflow, state: dict[str, Any], node_id: str) -> str | None:
        for loop_id, body in workflow.loop_bodies().items():
            if node_id in body and state["loops"][loop_id]["status"] == "running":
                return loop_id
        return None

    def _auto_process(
        self, execution_id: str, workflow: Workflow, state: dict[str, Any], tenant: str = DEFAULT_TENANT
    ) -> None:
        by_id = {node.id: node for node in workflow.nodes}
        bodies = workflow.loop_bodies()
        body_members = set().union(*bodies.values()) if bodies else set()
        nested = _nested_map_ids(workflow)
        completed = set(state["completed_nodes"])
        skipped = set(state["skipped_nodes"])
        changed = True
        while changed:
            changed = False
            if state["status"] != "running":
                # A map expansion may terminate the execution mid-pass; nothing
                # may be evaluated or appended after that termination.
                break
            for node in sorted(workflow.nodes, key=lambda item: item.id):
                if node.kind == "loop" or node.kind == "map" or node.id in body_members:
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
                    self._append(execution_id, "condition_evaluated", {"node_id": node.id, "result": result}, tenant)
                    changed = True
                elif node.run_if is not None and state["condition_results"][node.run_if.condition_id] != node.run_if.expected:
                    state["skipped_nodes"].append(node.id)
                    skipped.add(node.id)
                    self._append(execution_id, "node_skipped", {"node_id": node.id}, tenant)
                    changed = True
            for loop_node in sorted((node for node in workflow.nodes if node.kind == "loop"), key=lambda item: item.id):
                if state["status"] != "running":
                    break
                loop_state = state["loops"][loop_node.id]
                if loop_state["status"] == "pending":
                    if not set(loop_node.depends_on) <= completed | skipped:
                        continue
                    result = _evaluate_condition(by_id[loop_node.condition], state["input"])
                    self._append(
                        execution_id,
                        "loop_condition_evaluated",
                        {"loop_id": loop_node.id, "node_id": loop_node.condition, "iteration": 0, "result": result},
                        tenant,
                    )
                    if result:
                        loop_state["status"] = "running"
                        loop_state["current_iteration"] = 1
                        loop_state["iterations"].append(_new_iteration(nested.get(loop_node.id, ())))
                        self._append(
                            execution_id,
                            "iteration_started",
                            _iteration_started_payload(loop_node.id, 1, nested.get(loop_node.id, ())),
                            tenant,
                        )
                    else:
                        self._finish_loop(execution_id, state, loop_node.id, loop_state, "condition_false", tenant)
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
                                tenant,
                            )
                            changed = True
                        elif node.run_if is not None and iteration["condition_results"][node.run_if.condition_id] != node.run_if.expected:
                            iteration["skipped_nodes"].append(node_id)
                            iteration_skipped.add(node_id)
                            self._append(
                                execution_id,
                                "node_skipped",
                                {"node_id": node_id, "loop_id": loop_node.id, "iteration": loop_state["current_iteration"]},
                                tenant,
                            )
                            changed = True
                    for map_id in nested.get(loop_node.id, ()):
                        # A nested map expands once the iteration's own
                        # dependencies settle, reading the source task's output
                        # recorded in this iteration. Iterations started before
                        # a migration introduced the map carry no skeleton and
                        # are left to their recorded conclusions.
                        if state["status"] != "running":
                            break
                        map_state = iteration.get("maps", {}).get(map_id)
                        if map_state is None or map_state["status"] != "pending":
                            continue
                        map_node = by_id[map_id]
                        if not set(map_node.depends_on) <= iteration_completed | iteration_skipped:
                            continue
                        self._expand_map(
                            execution_id,
                            workflow,
                            state,
                            map_node,
                            map_state,
                            tenant,
                            container=iteration,
                            context={"loop_id": loop_node.id, "iteration": loop_state["current_iteration"]},
                        )
                        if map_state["status"] == "completed":
                            # A zero-instance expansion finishes at once,
                            # releasing the iteration's successors.
                            iteration_completed.add(map_id)
                        changed = True
                    if state["status"] == "running" and (
                        bodies[loop_node.id] - {mid for mid in nested.get(loop_node.id, ()) if mid not in iteration.get("maps", {})}
                        <= iteration_completed | iteration_skipped
                    ):
                        current = loop_state["current_iteration"]
                        result = _evaluate_condition(by_id[loop_node.condition], state["input"])
                        self._append(
                            execution_id,
                            "loop_condition_evaluated",
                            {"loop_id": loop_node.id, "node_id": loop_node.condition, "iteration": current, "result": result},
                            tenant,
                        )
                        if not result:
                            self._finish_loop(execution_id, state, loop_node.id, loop_state, "condition_false", tenant)
                            completed.add(loop_node.id)
                        elif current >= loop_node.max_iterations:
                            self._finish_loop(execution_id, state, loop_node.id, loop_state, "iteration_limit", tenant)
                            completed.add(loop_node.id)
                        else:
                            loop_state["current_iteration"] = current + 1
                            loop_state["iterations"].append(_new_iteration(nested.get(loop_node.id, ())))
                            self._append(
                                execution_id,
                                "iteration_started",
                                _iteration_started_payload(loop_node.id, current + 1, nested.get(loop_node.id, ())),
                                tenant,
                            )
                        changed = True
            for map_node in sorted((node for node in workflow.nodes if node.kind == "map"), key=lambda item: item.id):
                if state["status"] != "running":
                    break
                if map_node.id in body_members:
                    # A nested map expands inside its loop's current iteration.
                    continue
                map_state = state["maps"][map_node.id]
                if map_state["status"] != "pending":
                    continue
                if not set(map_node.depends_on) <= completed | skipped:
                    continue
                self._expand_map(execution_id, workflow, state, map_node, map_state, tenant)
                if map_state["status"] == "completed":
                    # A zero-instance expansion finishes at once, immediately
                    # releasing its successors just like any finished node.
                    completed.add(map_node.id)
                changed = True
        finished = completed | skipped
        if state["status"] == "running" and all(
            node.id in finished for node in workflow.nodes if node.id not in body_members
        ):
            state["status"] = "completed"
            self._append(execution_id, "execution_completed", {}, tenant)

    def _finish_loop(
        self,
        execution_id: str,
        state: dict[str, Any],
        loop_id: str,
        loop_state: dict[str, Any],
        reason: str,
        tenant: str = DEFAULT_TENANT,
    ) -> None:
        # Once the loop is finished, its body conclusions are also visible in
        # the execution-level completed list. Include every body node completed
        # in any iteration, preserving first completion order and listing a
        # node only once, followed by the loop node itself. Skipped body nodes
        # remain per-iteration skips rather than completions.
        listed = set(state["completed_nodes"])
        for iteration in loop_state["iterations"]:
            for node_id in iteration["completed_nodes"]:
                if node_id not in listed:
                    state["completed_nodes"].append(node_id)
                    listed.add(node_id)
        loop_state["status"] = "completed"
        loop_state["end_reason"] = reason
        state["completed_nodes"].append(loop_id)
        self._append(
            execution_id,
            "loop_completed",
            {"loop_id": loop_id, "reason": reason, "iterations": loop_state["current_iteration"]},
            tenant,
        )

    @staticmethod
    def _replay_map_instance(
        rebuilt: dict[str, Any], map_id: str, index: int, loop_id: str | None = None, iteration: int | None = None
    ) -> dict[str, Any]:
        map_state, _ = _map_state_for(rebuilt, map_id, loop_id, iteration)
        return next(item for item in map_state["instances"] if item["index"] == index)

    def replay(self, execution_id: str, tenant: str = DEFAULT_TENANT) -> dict[str, Any]:
        stored = self.get_execution(execution_id, tenant)
        rebuilt: dict[str, Any] | None = None
        for event in self.events(execution_id, tenant):
            event_type = event["type"]
            payload = event["payload"]
            if event_type == "execution_started":
                rebuilt = {
                    "id": execution_id,
                    "workflow_id": payload["workflow_id"],
                    "status": "running",
                    "termination_reason": None,
                    "timeout_seconds": payload.get("timeout_seconds"),
                    "deadline_at": payload.get("deadline_at"),
                    "input": payload["input"],
                    "completed_nodes": [],
                    "skipped_nodes": [],
                    "failed_nodes": [],
                    "condition_results": {},
                    "outputs": {},
                    "attempts": {},
                    "loops": {loop_id: _new_loop_state() for loop_id in payload.get("loops", {})},
                }
                # The version binding is rebuilt solely from the start record.
                if payload.get("version"):
                    rebuilt["version"] = payload["version"]
                if payload.get("maps"):
                    rebuilt["maps"] = {map_id: _new_map_state() for map_id in payload["maps"]}
                if "approvals" in payload:
                    rebuilt["waiting_approval"] = payload.get("waiting_approval")
                    rebuilt["approvals"] = []
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
                if "loop_id" in payload:
                    rebuilt["loops"][payload["loop_id"]]["iterations"][-1]["skipped_nodes"].append(payload["node_id"])
                else:
                    rebuilt["skipped_nodes"].append(payload["node_id"])
            elif event_type == "node_completed" and rebuilt is not None:
                node_id = payload["node_id"]
                if "map_id" in payload:
                    instance = self._replay_map_instance(
                        rebuilt, payload["map_id"], payload["index"], payload.get("loop_id"), payload.get("iteration")
                    )
                    instance["status"] = "completed"
                    instance["output"] = payload["output"]
                    instance["failure_reason"] = None
                elif "loop_id" in payload:
                    iteration = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                    iteration["completed_nodes"].append(node_id)
                    iteration["outputs"][node_id] = payload["output"]
                    iteration["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
                else:
                    rebuilt["completed_nodes"].append(node_id)
                    rebuilt["outputs"][node_id] = payload["output"]
                    rebuilt["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
            elif event_type == "node_failed" and rebuilt is not None:
                node_id = payload["node_id"]
                if "map_id" in payload:
                    instance = self._replay_map_instance(
                        rebuilt, payload["map_id"], payload["index"], payload.get("loop_id"), payload.get("iteration")
                    )
                    instance["status"] = "failed"
                    instance["failure_reason"] = payload["reason"]
                elif "loop_id" in payload:
                    container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                    entry = container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
                    entry["attempt"] = payload["attempt"]
                    entry["failures"] += 1
                else:
                    entry = rebuilt["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})
                    entry["attempt"] = payload["attempt"]
                    entry["failures"] += 1
            elif event_type == "node_retried" and rebuilt is not None:
                node_id = payload["node_id"]
                if "map_id" in payload:
                    instance = self._replay_map_instance(
                        rebuilt, payload["map_id"], payload["index"], payload.get("loop_id"), payload.get("iteration")
                    )
                    # A consumed failure with retries left re-queues it; the
                    # transient failure reason is not part of its final record.
                    instance["status"] = "ready"
                    instance["failure_reason"] = None
                elif "loop_id" in payload:
                    container = rebuilt["loops"][payload["loop_id"]]["iterations"][-1]
                    container["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})["attempt"] = payload["attempt"]
                else:
                    rebuilt["attempts"].setdefault(node_id, {"attempt": 1, "failures": 0})["attempt"] = payload["attempt"]
            elif event_type == "map_expanded" and rebuilt is not None:
                map_state, _ = _map_state_for(
                    rebuilt, payload["map_id"], payload.get("loop_id"), payload.get("iteration")
                )
                if payload.get("exceeded"):
                    # Over-limit expansion creates no instances; the map fails
                    # permanently and the termination event ends the execution.
                    map_state["status"] = "failed"
                    map_state["failure_reason"] = payload.get("reason")
                else:
                    map_state["instances"] = [_new_map_instance(index) for index in range(payload["instance_count"])]
                    map_state["status"] = "running"
            elif event_type == "map_completed" and rebuilt is not None:
                map_state, container = _map_state_for(
                    rebuilt, payload["map_id"], payload.get("loop_id"), payload.get("iteration")
                )
                map_state["status"] = "completed"
                map_state["outputs"] = [
                    instance["output"]
                    for instance in sorted(map_state["instances"], key=lambda item: item["index"])
                ]
                container["outputs"][payload["map_id"]] = map_state["outputs"]
                container["completed_nodes"].append(payload["map_id"])
            elif event_type == "iteration_started" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                loop_state["status"] = "running"
                loop_state["current_iteration"] = payload["iteration"]
                iteration = _new_iteration()
                if "maps" in payload:
                    # The nested-map skeleton rides on the start record so the
                    # iteration's map ownership is rebuilt exactly.
                    iteration["maps"] = {map_id: _new_map_state() for map_id in payload["maps"]}
                loop_state["iterations"].append(iteration)
            elif event_type == "loop_completed" and rebuilt is not None:
                loop_state = rebuilt["loops"][payload["loop_id"]]
                # Mirror the materialized roll-up: body nodes completed in any
                # iteration enter the execution-level completed list once, in
                # first completion order, followed by the loop node.
                listed = set(rebuilt["completed_nodes"])
                for iteration in loop_state["iterations"]:
                    for node_id in iteration["completed_nodes"]:
                        if node_id not in listed:
                            rebuilt["completed_nodes"].append(node_id)
                            listed.add(node_id)
                loop_state["status"] = "completed"
                loop_state["end_reason"] = payload["reason"]
                rebuilt["completed_nodes"].append(payload["loop_id"])
            elif event_type == "approval_requested" and rebuilt is not None:
                waiting_record = {
                    "node_id": payload["node_id"],
                    "approvers": list(payload["approvers"]),
                    **({"loop_id": payload["loop_id"], "iteration": payload["iteration"]} if "loop_id" in payload else {}),
                }
                if "map_id" in payload:
                    waiting_record["map_id"] = payload["map_id"]
                    waiting_record["index"] = payload["index"]
                    instance = self._replay_map_instance(
                        rebuilt, payload["map_id"], payload["index"], payload.get("loop_id"), payload.get("iteration")
                    )
                    instance["status"] = "waiting"
                rebuilt["waiting_approval"] = waiting_record
            elif event_type == "version_migrated" and rebuilt is not None:
                # Rebuild the migration point from the record alone: add the
                # structure the target revision introduces and rebind, while
                # keeping every pre-migration conclusion (and a parked approval
                # point) exactly as the earlier events rebuilt it.
                for loop_id in payload.get("loops", {}):
                    if loop_id not in rebuilt["loops"]:
                        rebuilt["loops"][loop_id] = _new_loop_state()
                for map_id, map_state in payload.get("maps", {}).items():
                    if map_id not in rebuilt.get("maps", {}):
                        rebuilt.setdefault("maps", {})[map_id] = map_state
                if "approvals" in payload and "approvals" not in rebuilt:
                    rebuilt["waiting_approval"] = None
                    rebuilt["approvals"] = []
                rebuilt["version"] = payload["to_version"]
            elif event_type == "approval_decided" and rebuilt is not None:
                verdict = payload["decision"]
                record = {
                    "node_id": payload["node_id"],
                    "approver": payload["approver"],
                    "decision": verdict,
                    "reason": payload.get("reason"),
                }
                if "loop_id" in payload:
                    record["loop_id"] = payload["loop_id"]
                    record["iteration"] = payload["iteration"]
                if "map_id" in payload:
                    record["map_id"] = payload["map_id"]
                    record["index"] = payload["index"]
                rebuilt["waiting_approval"] = None
                rebuilt["approvals"].append(record)
            elif event_type == "execution_completed" and rebuilt is not None:
                rebuilt["status"] = "completed"
            elif event_type == "execution_terminated" and rebuilt is not None:
                rebuilt["status"] = "terminated"
                rebuilt["termination_reason"] = payload["reason"]
                if "waiting_approval" in rebuilt:
                    rebuilt["waiting_approval"] = None
                if "map_id" in payload:
                    map_state, _ = _map_state_for(
                        rebuilt, payload["map_id"], payload.get("loop_id"), payload.get("iteration")
                    )
                    if "index" in payload:
                        instance = self._replay_map_instance(
                            rebuilt, payload["map_id"], payload["index"], payload.get("loop_id"), payload.get("iteration")
                        )
                        instance["status"] = "failed"
                        if payload["reason"] == "rejected":
                            # A rejected instance writes no node_failed event;
                            # its reason is the approval_decided record the
                            # replay already rebuilt for that instance.
                            text = next(
                                record.get("reason")
                                for record in reversed(rebuilt.get("approvals", []))
                                if record.get("map_id") == payload["map_id"]
                                and record.get("index") == payload["index"]
                                and record.get("loop_id") == payload.get("loop_id")
                                and record.get("iteration") == payload.get("iteration")
                            )
                            instance["failure_reason"] = text
                            map_state["failure_reason"] = text
                        else:
                            # Retry exhaustion: the reason was already recorded
                            # by the instance's final node_failed event.
                            map_state["failure_reason"] = instance["failure_reason"]
                    map_state["status"] = "failed"
                if payload["reason"] in ("retries_exhausted", "rejected") and "node_id" in payload:
                    rebuilt["failed_nodes"].append(payload["node_id"])
        if rebuilt is None:
            raise ConflictError("execution event stream has no start event")
        return {"consistent": rebuilt == stored, "execution": rebuilt}

    def _append(
        self, execution_id: str, event_type: str, payload: dict[str, Any], tenant: str = DEFAULT_TENANT
    ) -> None:
        row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM events "
            "WHERE tenant = ? AND execution_id = ?",
            (tenant, execution_id),
        ).fetchone()
        self.store.connection.execute(
            "INSERT INTO events(tenant, execution_id, sequence, type, payload, occurred_at) VALUES (?, ?, ?, ?, ?, ?)",
            (tenant, execution_id, row["sequence"], event_type, self.store.encode(payload), self.store.now()),
        )
        if event_type in NOTIFY_EVENT_TYPES:
            # Queue targets are fed in the same transaction as the event, so a
            # rolled-back operation never leaves a queued message behind.
            self._enqueue_queue_messages(execution_id, row["sequence"], event_type, payload, tenant)
            # Buffered, not delivered: the surrounding operation may still roll
            # back. The outermost _operation scope delivers after the commit.
            self._local.pending = getattr(self._local, "pending", []) + [
                {
                    "tenant": tenant,
                    "execution_id": execution_id,
                    "sequence": row["sequence"],
                    "type": event_type,
                    "payload": payload,
                }
            ]

    def _write_checkpoint(
        self, execution_id: str, state: dict[str, Any], tenant: str = DEFAULT_TENANT
    ) -> None:
        """Persist the state summary and event position at a node boundary."""
        event_row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) AS sequence FROM events WHERE tenant = ? AND execution_id = ?",
            (tenant, execution_id),
        ).fetchone()
        checkpoint_row = self.store.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM checkpoints "
            "WHERE tenant = ? AND execution_id = ?",
            (tenant, execution_id),
        ).fetchone()
        document = {"state": state, "event_sequence": event_row["sequence"]}
        self.store.connection.execute(
            "INSERT INTO checkpoints(tenant, execution_id, sequence, event_sequence, document, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tenant, execution_id, checkpoint_row["sequence"], event_row["sequence"], self.store.encode(document), self.store.now()),
        )
