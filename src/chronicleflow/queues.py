from __future__ import annotations

import math
from typing import Any

from .errors import ValidationError

# Business events a queue target may declare interest in. It is the same set a
# webhook subscription may watch: a termination is a single event type
# whatever its reason.
QUEUE_EVENT_TYPES = ("node_completed", "execution_completed", "execution_terminated", "approval_decided")

DEFAULT_VISIBILITY_SECONDS = 30.0


def parse_queues(raw: Any) -> list[dict[str, Any]]:
    """Validate a declared queue target list and return it normalized.

    Each target carries exactly a queue ``name`` and the event types it wants,
    plus an optional visibility timeout; the default is filled in so enqueue
    and pull never re-check for a missing field. A repeated name is not a
    malformed declaration: the service rejects it as an ordinary same-tenant
    identifier conflict, so this parser only normalizes the entries.
    """
    if not isinstance(raw, list):
        raise ValidationError("queues must be an array")
    queues: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict) or not {"name", "events"} <= set(item) <= {
            "name",
            "events",
            "visibility_seconds",
        }:
            raise ValidationError(
                "each queue must contain name and events, and optionally visibility_seconds"
            )
        name = item["name"]
        if not isinstance(name, str) or not name:
            raise ValidationError("queue name must be a non-empty string")
        events = item["events"]
        if not isinstance(events, list) or not events:
            raise ValidationError("queue events must be a non-empty array")
        if any(not isinstance(event, str) or event not in QUEUE_EVENT_TYPES for event in events):
            raise ValidationError("queue events must only contain known event types")
        if len(set(events)) != len(events):
            raise ValidationError("queue events must not contain duplicates")
        visibility = item.get("visibility_seconds", DEFAULT_VISIBILITY_SECONDS)
        if isinstance(visibility, bool) or not isinstance(visibility, (int, float)):
            raise ValidationError("queue visibility_seconds must be a positive number of seconds")
        if not math.isfinite(visibility) or visibility <= 0:
            raise ValidationError("queue visibility_seconds must be a positive number of seconds")
        queues.append({"name": name, "events": list(events), "visibility_seconds": visibility})
    return queues
