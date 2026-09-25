from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ValidationError
from .model import _finite_json

MISSED_POLICIES = ("catch_up", "skip")

# minute, hour, day-of-month, month, day-of-week (0 and 7 are Sunday)
_CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def _parse_cron_field(text: str, low: int, high: int) -> frozenset[int]:
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise ValidationError("cron field contains an empty list item")
        base = part
        step = 1
        if "/" in part:
            base, _, step_text = part.rpartition("/")
            if not step_text.isdigit() or int(step_text) <= 0:
                raise ValidationError("cron step must be a positive integer")
            step = int(step_text)
        if base == "*":
            start, end = low, high
        elif "-" in base:
            start_text, _, end_text = base.partition("-")
            if not start_text.isdigit() or not end_text.isdigit():
                raise ValidationError("cron range bounds must be non-negative integers")
            start, end = int(start_text), int(end_text)
            if start > end:
                raise ValidationError("cron range start must not exceed its end")
        else:
            if not base.isdigit():
                raise ValidationError("cron field contains an unparseable item")
            start = end = int(base)
        if start < low or end > high:
            raise ValidationError(f"cron values must be between {low} and {high}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class Cron:
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    any_day_of_month: bool
    any_day_of_week: bool

    @classmethod
    def parse(cls, raw: Any) -> "Cron":
        if not isinstance(raw, str) or not raw.strip():
            raise ValidationError("cron must be a non-empty string of five fields")
        fields = raw.split()
        if len(fields) != 5:
            raise ValidationError("cron must contain exactly five fields")
        parsed = [_parse_cron_field(text, low, high) for text, (low, high) in zip(fields, _CRON_RANGES)]
        return cls(
            minutes=parsed[0],
            hours=parsed[1],
            days_of_month=parsed[2],
            months=parsed[3],
            days_of_week=parsed[4],
            any_day_of_month=fields[2] == "*",
            any_day_of_week=fields[4] == "*",
        )

    def _day_matches(self, moment: datetime) -> bool:
        day_of_month = moment.day in self.days_of_month
        weekday = moment.isoweekday() % 7  # Sunday is 0, as in cron
        day_of_week = weekday in self.days_of_week or (weekday == 0 and 7 in self.days_of_week)
        if not self.any_day_of_month and not self.any_day_of_week:
            # Standard cron: restricted day-of-month and day-of-week are OR-ed.
            return day_of_month or day_of_week
        return day_of_month and day_of_week

    def matches(self, moment: datetime) -> bool:
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.month in self.months
            and self._day_matches(moment)
        )

    def next_after(self, epoch_seconds: float) -> int | None:
        """Return the epoch seconds of the first matching minute starting strictly
        after epoch_seconds, or None when nothing matches within five years."""
        moment = datetime.fromtimestamp(epoch_seconds, timezone.utc).replace(second=0, microsecond=0)
        moment += timedelta(minutes=1)
        limit = moment + timedelta(days=366 * 5 + 2)
        while moment < limit:
            if moment.month not in self.months:
                moment = (moment.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0)
            elif not self._day_matches(moment):
                moment = (moment + timedelta(days=1)).replace(hour=0, minute=0)
            elif moment.hour not in self.hours:
                moment = (moment + timedelta(hours=1)).replace(minute=0)
            elif moment.minute not in self.minutes:
                moment = moment + timedelta(minutes=1)
            else:
                return int(moment.timestamp())
        return None


def parse_schedule(raw: Any) -> dict[str, Any]:
    """Validate a declared schedule and return it normalized.

    A schedule carries exactly an interval in seconds or a five-field cron
    expression (never both), the input used for each created execution, and
    the missed-period policy.
    """
    if not isinstance(raw, dict):
        raise ValidationError("schedule must be an object")
    unknown = set(raw) - {"interval_seconds", "cron", "input", "missed_policy"}
    if unknown:
        raise ValidationError(f"schedule contains unknown fields: {', '.join(sorted(unknown))}")
    missing = {"input", "missed_policy"} - set(raw)
    if missing:
        raise ValidationError(f"schedule is missing fields: {', '.join(sorted(missing))}")
    has_interval = "interval_seconds" in raw
    has_cron = "cron" in raw
    if has_interval and has_cron:
        raise ValidationError("schedule must declare either interval_seconds or cron, not both")
    if not has_interval and not has_cron:
        raise ValidationError("schedule must declare interval_seconds or cron")
    schedule: dict[str, Any] = {}
    if has_interval:
        interval = raw["interval_seconds"]
        if isinstance(interval, bool) or not isinstance(interval, int):
            raise ValidationError("schedule interval_seconds must be a positive integer")
        if interval <= 0:
            raise ValidationError("schedule interval_seconds must be a positive integer")
        schedule["interval_seconds"] = interval
    else:
        # Parsed here so an unparseable or out-of-range expression rejects the
        # whole request; the original text is kept as the declared plan.
        Cron.parse(raw["cron"])
        schedule["cron"] = raw["cron"]
    if not isinstance(raw["input"], dict):
        raise ValidationError("schedule input must be an object")
    _finite_json(raw["input"], "schedule input")
    schedule["input"] = raw["input"]
    policy = raw["missed_policy"]
    if policy not in MISSED_POLICIES:
        raise ValidationError('schedule missed_policy must be "catch_up" or "skip"')
    schedule["missed_policy"] = policy
    return schedule
