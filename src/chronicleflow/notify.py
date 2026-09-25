from __future__ import annotations

import math
from typing import Any
from urllib.parse import urlsplit

from .errors import ValidationError

# The only business events a subscription may declare interest in. A
# termination is a single event type regardless of its reason.
NOTIFY_EVENT_TYPES = ("node_completed", "execution_completed", "execution_terminated", "approval_decided")

MAX_DELIVERY_ATTEMPTS = 10
DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_MAX_ATTEMPTS = 1


def parse_subscriptions(raw: Any) -> list[dict[str, Any]]:
    """Validate a declared subscription list and return it normalized.

    Every entry carries exactly a target url, the event types it cares about,
    and an optional delivery timeout and attempt count; defaults are filled in
    so delivery never re-checks for missing fields.
    """
    if not isinstance(raw, list):
        raise ValidationError("subscriptions must be an array")
    subscriptions: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict) or not {"url", "events"} <= set(item) <= {
            "url",
            "events",
            "timeout_seconds",
            "max_attempts",
        }:
            raise ValidationError(
                "each subscription must contain url and events, and optionally timeout_seconds and max_attempts"
            )
        url = item["url"]
        if not isinstance(url, str) or not url:
            raise ValidationError("subscription url must be a non-empty string")
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValidationError("subscription url must be an http or https address")
        events = item["events"]
        if not isinstance(events, list) or not events:
            raise ValidationError("subscription events must be a non-empty array")
        if any(not isinstance(event, str) or event not in NOTIFY_EVENT_TYPES for event in events):
            raise ValidationError("subscription events must only contain known event types")
        if len(set(events)) != len(events):
            raise ValidationError("subscription events must not contain duplicates")
        timeout = item.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValidationError("subscription timeout_seconds must be a positive number of seconds")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValidationError("subscription timeout_seconds must be a positive number of seconds")
        max_attempts = item.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise ValidationError(f"subscription max_attempts must be an integer between 1 and {MAX_DELIVERY_ATTEMPTS}")
        if not 1 <= max_attempts <= MAX_DELIVERY_ATTEMPTS:
            raise ValidationError(f"subscription max_attempts must be an integer between 1 and {MAX_DELIVERY_ATTEMPTS}")
        subscriptions.append(
            {"url": url, "events": list(events), "timeout_seconds": timeout, "max_attempts": max_attempts}
        )
    return subscriptions
