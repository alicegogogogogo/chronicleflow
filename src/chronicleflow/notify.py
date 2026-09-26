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

# A queue target hides a pulled message for this many seconds unless the
# caller acknowledges it first; the declaration may override it with any
# positive number of seconds.
DEFAULT_VISIBILITY_SECONDS = 30


def _parse_events(raw: Any) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise ValidationError("subscription events must be a non-empty array")
    if any(not isinstance(event, str) or event not in NOTIFY_EVENT_TYPES for event in raw):
        raise ValidationError("subscription events must only contain known event types")
    if len(set(raw)) != len(raw):
        raise ValidationError("subscription events must not contain duplicates")
    return list(raw)


def parse_subscriptions(raw: Any) -> list[dict[str, Any]]:
    """Validate a declared subscription list and return it normalized.

    Every entry is either a webhook target (a url with an optional delivery
    timeout and attempt count) or a queue target (a queue name with an
    optional visibility timeout); exactly one of the two shapes must be
    declared. Defaults are filled in so delivery never re-checks for missing
    fields.
    """
    if not isinstance(raw, list):
        raise ValidationError("subscriptions must be an array")
    subscriptions: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValidationError("each subscription must be an object")
        has_url = "url" in item
        has_queue = "queue" in item
        if has_url == has_queue:
            raise ValidationError("each subscription must declare exactly one of url or queue")
        if "events" not in item:
            raise ValidationError("each subscription must contain url and events, or queue and events")
        events = _parse_events(item["events"])
        if has_url:
            if not set(item) <= {"url", "events", "timeout_seconds", "max_attempts"}:
                raise ValidationError(
                    "each subscription must contain url and events, and optionally timeout_seconds and max_attempts"
                )
            url = item["url"]
            if not isinstance(url, str) or not url:
                raise ValidationError("subscription url must be a non-empty string")
            parts = urlsplit(url)
            if parts.scheme not in ("http", "https") or not parts.netloc:
                raise ValidationError("subscription url must be an http or https address")
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
                {"url": url, "events": events, "timeout_seconds": timeout, "max_attempts": max_attempts}
            )
        else:
            if not set(item) <= {"queue", "events", "visibility_seconds"}:
                raise ValidationError(
                    "each queue subscription must contain queue and events, and optionally visibility_seconds"
                )
            name = item["queue"]
            if not isinstance(name, str) or not name or len(name) > 100:
                raise ValidationError("subscription queue must be a non-empty string of at most 100 characters")
            visibility = item.get("visibility_seconds", DEFAULT_VISIBILITY_SECONDS)
            if isinstance(visibility, bool) or not isinstance(visibility, (int, float)):
                raise ValidationError("subscription visibility_seconds must be a positive number of seconds")
            if not math.isfinite(visibility) or visibility <= 0:
                raise ValidationError("subscription visibility_seconds must be a positive number of seconds")
            subscriptions.append({"queue": name, "events": events, "visibility_seconds": visibility})
    return subscriptions
