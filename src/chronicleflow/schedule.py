from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ValidationError
from .model import _finite_json

# A schedule declaration carries exactly one plan: either a fixed number of
# seconds between firings or a five-field cron expression, plus the input
# every created execution starts with and the missed-period policy.
MISFIRE_POLICIES = ("catch_up", "skip")

# Five fields: minute, hour, day of month, month, day of week. Days of week
# follow cron numbering: 0 is Sunday and 6 is Saturday.
CRON_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day_of_month", 1, 31),
    ("month", 1, 12),
    ("day_of_week", 0, 6),
)


def parse_schedule(raw: Any) -> dict[str, Any]:
    """Validate a schedule declaration and return it normalized.

    The declaration carries exactly one plan (``interval_seconds`` or
    ``cron``), the execution ``input`` and the ``misfire_policy``.
    """
    if not isinstance(raw, dict) or not {"input", "misfire_policy"} <= set(raw) <= {
        "interval_seconds",
        "cron",
        "input",
        "misfire_policy",
    }:
        raise ValidationError(
            "schedule must contain input and misfire_policy, plus exactly one of interval_seconds or cron"
        )
    has_interval = "interval_seconds" in raw
    has_cron = "cron" in raw
    if has_interval == has_cron:
        raise ValidationError("schedule must declare exactly one of interval_seconds or cron")
    plan: dict[str, Any] = {}
    if has_interval:
        interval = raw["interval_seconds"]
        if isinstance(interval, bool) or not isinstance(interval, int):
            raise ValidationError("interval_seconds must be a positive integer number of seconds")
        if interval <= 0:
            raise ValidationError("interval_seconds must be a positive integer number of seconds")
        plan["interval_seconds"] = interval
    else:
        expression = raw["cron"]
        if not isinstance(expression, str) or not expression:
            raise ValidationError("cron must be a non-empty five-field expression")
        parse_cron(expression)
        plan["cron"] = expression
    input_data = raw["input"]
    if not isinstance(input_data, dict):
        raise ValidationError("schedule input must be an object")
    _finite_json(input_data, "input")
    policy = raw["misfire_policy"]
    if policy not in MISFIRE_POLICIES:
        raise ValidationError("misfire_policy must be \"catch_up\" or \"skip\"")
    plan["input"] = input_data
    plan["misfire_policy"] = policy
    return plan


def _parse_number(token: str, low: int, high: int, field: str) -> int:
    # isdigit() intentionally rejects signs, spaces, and non-digit characters.
    if not token.isdigit():
        raise ValidationError(f"cron {field} contains an unparseable fragment: {token}")
    value = int(token)
    if not low <= value <= high:
        raise ValidationError(f"cron {field} value must be between {low} and {high}")
    return value


def _parse_field(token: str, low: int, high: int, field: str) -> set[int]:
    values: set[int] = set()
    for fragment in token.split(","):
        if not fragment:
            raise ValidationError(f"cron {field} contains an empty fragment")
        step: int | None = None
        if "/" in fragment:
            base, step_token = fragment.split("/", 1)
            if "/" in step_token or not step_token.isdigit() or int(step_token) <= 0:
                raise ValidationError(f"cron {field} contains an unparseable fragment: {fragment}")
            step = int(step_token)
        else:
            base = fragment
        if base == "*":
            start, end = low, high
        elif "-" in base:
            parts = base.split("-")
            if len(parts) != 2:
                raise ValidationError(f"cron {field} contains an unparseable fragment: {fragment}")
            start = _parse_number(parts[0], low, high, field)
            end = _parse_number(parts[1], low, high, field)
            if start > end:
                raise ValidationError(f"cron {field} range start must not exceed its end")
        else:
            start = _parse_number(base, low, high, field)
            end = high if step is not None else start
        if start > end:
            raise ValidationError(f"cron {field} range start must not exceed its end")
        if step is None:
            values.add(start) if end == start else values.update(range(start, end + 1))
        else:
            values.update(range(start, end + 1, step))
    return values


def parse_cron(expression: str) -> dict[str, Any]:
    """Parse a five-field cron expression into matched value sets."""
    fields = expression.split()
    if len(fields) != 5:
        raise ValidationError("cron expression must contain exactly five fields")
    (minute_token, hour_token, dom_token, month_token, dow_token) = fields
    minutes = _parse_field(minute_token, *CRON_FIELDS[0][1:], "minute")
    hours = _parse_field(hour_token, *CRON_FIELDS[1][1:], "hour")
    days_of_month = _parse_field(dom_token, *CRON_FIELDS[2][1:], "day of month")
    months = _parse_field(month_token, *CRON_FIELDS[3][1:], "month")
    days_of_week = _parse_field(dow_token, *CRON_FIELDS[4][1:], "day of week")
    return {
        "minutes": minutes,
        "hours": hours,
        "days_of_month": days_of_month,
        "months": months,
        "days_of_week": days_of_week,
        "dom_restricted": dom_token != "*",
        "dow_restricted": dow_token != "*",
    }


def _day_matches(parsed: dict[str, Any], moment: datetime) -> bool:
    if moment.month not in parsed["months"]:
        return False
    # When only one of the day fields is restricted it decides; when both are
    # restricted a day matches on either rule, matching cron convention.
    if not parsed["dom_restricted"] and not parsed["dow_restricted"]:
        return True
    dom_match = moment.day in parsed["days_of_month"]
    # isoweekday runs Monday=1..Sunday=7; cron numbers Sunday=0..Saturday=6.
    dow_match = (moment.isoweekday() % 7) in parsed["days_of_week"]
    if not parsed["dom_restricted"]:
        return dow_match
    if not parsed["dow_restricted"]:
        return dom_match
    return dom_match or dow_match


def cron_previous(expression: str, now: float) -> float:
    """Return the most recent scheduled period start not later than ``now``.

    Cron periods are minute aligned in UTC. The search walks candidate days
    backwards (at most a few hundred iterations) rather than every missed
    minute so an annually matching expression stays cheap.
    """
    parsed = parse_cron(expression)
    current = datetime.fromtimestamp(math.floor(now), tz=timezone.utc)
    candidate = current.replace(second=0, microsecond=0)
    minute_choices = sorted(parsed["minutes"])
    hour_choices = sorted(parsed["hours"])

    def latest_minute_on(day: datetime, not_later: datetime) -> datetime | None:
        for hour in reversed(hour_choices):
            if hour > not_later.hour:
                continue
            for minute in reversed(minute_choices):
                if hour == not_later.hour and minute > not_later.minute:
                    continue
                moment = day.replace(hour=hour, minute=minute)
                if moment <= not_later and _day_matches(parsed, moment):
                    return moment
        return None

    found = latest_minute_on(candidate, candidate)
    day = candidate
    for _ in range(366 * 5):
        if found is not None:
            return found.timestamp()
        day = (day - timedelta(days=1)).replace(hour=23, minute=59)
        found = latest_minute_on(day, day)
    raise ValidationError("cron expression has no matching time within the search horizon")
