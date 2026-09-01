"""Validated cron expressions for periodic schedules (REQUIREMENTS.md §6).

Standard five-field cron (minute hour day-of-month month day-of-week) plus the
usual ``@hourly``/``@daily``/... macros. Written here rather than taken as a
dependency because §2 makes ``asyncpg`` the only required runtime dependency.

Occurrence instants are computed in the schedule's own timezone, so a "03:00
local" schedule stays at 03:00 across a daylight-saving change.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, tzinfo
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from rqueue.errors import ValidationError

__all__ = ["CronExpression", "resolve_timezone"]

# Deliberately generous: 8 years covers the worst realistic gap, which is
# "Feb 29 in a century year that is not a leap year" (1896 -> 1904).
_MAX_DAY_SCAN: Final = 366 * 8

_MONTH_NAMES: Final = {
    name: index
    for index, name in enumerate(
        [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ],
        start=1,
    )
}
_DOW_NAMES: Final = {
    name: index
    for index, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])
}

_MACROS: Final = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
    "@minutely": "* * * * *",
}

_FIELD_RE: Final = re.compile(
    r"^(?P<start>\*|[a-z0-9]+)(?:-(?P<end>[a-z0-9]+))?(?:/(?P<step>\d+))?$"
)


def resolve_timezone(name: str) -> tzinfo:
    """Look up an IANA timezone, turning a bad name into a ValidationError."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ValidationError(f"unknown timezone {name!r}") from exc


def _parse_field(
    spec: str,
    *,
    low: int,
    high: int,
    names: dict[str, int] | None,
    field_name: str,
) -> tuple[frozenset[int], bool]:
    """Parse one cron field into its value set and whether it is restricted."""
    spec = spec.strip().lower()
    if not spec:
        raise ValidationError(f"cron {field_name} field is empty")
    restricted = spec != "*"
    values: set[int] = set()

    for part in spec.split(","):
        match = _FIELD_RE.match(part)
        if match is None:
            raise ValidationError(f"cron {field_name} field {part!r} is malformed")
        step = int(match.group("step") or 1)
        if step < 1:
            raise ValidationError(f"cron {field_name} step must be >= 1")

        if match.group("start") == "*":
            if match.group("end") is not None:
                raise ValidationError(f"cron {field_name} field {part!r} is malformed")
            start, end = low, high
        else:
            start = _parse_value(match.group("start"), names, field_name, low, high)
            raw_end = match.group("end")
            end = (
                start
                if raw_end is None
                else _parse_value(raw_end, names, field_name, low, high)
            )
            if match.group("step") and raw_end is None:
                # "5/15" means "from 5 to the end of the range, every 15".
                end = high
        if start > end:
            raise ValidationError(
                f"cron {field_name} range {part!r} runs backwards ({start} > {end})"
            )
        values.update(range(start, end + 1, step))

    if not values:
        raise ValidationError(f"cron {field_name} field {spec!r} matches nothing")
    return frozenset(values), restricted


def _parse_value(
    token: str, names: dict[str, int] | None, field_name: str, low: int, high: int
) -> int:
    if names is not None and token in names:
        value = names[token]
    else:
        try:
            value = int(token)
        except ValueError as exc:
            raise ValidationError(
                f"cron {field_name} value {token!r} is not a number"
            ) from exc
    if field_name == "day-of-week" and value == 7:
        value = 0  # both 0 and 7 mean Sunday
    if not low <= value <= high:
        raise ValidationError(
            f"cron {field_name} value {value} is outside {low}-{high}"
        )
    return value


@dataclass(frozen=True, slots=True)
class CronExpression:
    """A parsed, validated cron expression."""

    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    dom_restricted: bool
    dow_restricted: bool

    @classmethod
    def parse(cls, expression: str) -> CronExpression:
        if not isinstance(expression, str) or not expression.strip():
            raise ValidationError("cron expression must be a non-empty string")
        original = expression.strip()
        normalized = _MACROS.get(original.lower(), original)
        fields = normalized.split()
        if len(fields) != 5:
            raise ValidationError(
                "cron expression must have 5 fields "
                f"(minute hour day-of-month month day-of-week); got {len(fields)}"
            )
        minutes, _ = _parse_field(
            fields[0], low=0, high=59, names=None, field_name="minute"
        )
        hours, _ = _parse_field(
            fields[1], low=0, high=23, names=None, field_name="hour"
        )
        dom, dom_restricted = _parse_field(
            fields[2], low=1, high=31, names=None, field_name="day-of-month"
        )
        months, _ = _parse_field(
            fields[3], low=1, high=12, names=_MONTH_NAMES, field_name="month"
        )
        dow, dow_restricted = _parse_field(
            fields[4], low=0, high=7, names=_DOW_NAMES, field_name="day-of-week"
        )
        return cls(
            expression=original,
            minutes=minutes,
            hours=hours,
            days_of_month=dom,
            months=months,
            days_of_week=frozenset(0 if value == 7 else value for value in dow),
            dom_restricted=dom_restricted,
            dow_restricted=dow_restricted,
        )

    def matches_day(self, day: date) -> bool:
        """Classic cron day matching.

        When both day-of-month and day-of-week are restricted the day matches
        if *either* does -- the long-standing Vixie cron rule, which surprises
        people but is what every other scheduler implements.
        """
        if day.month not in self.months:
            return False
        dom_match = day.day in self.days_of_month
        dow_match = (day.weekday() + 1) % 7 in self.days_of_week
        if self.dom_restricted and self.dow_restricted:
            return dom_match or dow_match
        return dom_match and dow_match

    def previous(self, moment: datetime, *, tz: tzinfo) -> datetime | None:
        """The latest occurrence at or before ``moment``."""
        local = moment.astimezone(tz).replace(second=0, microsecond=0)
        day = local.date()
        ceiling: tuple[int, int] | None = (local.hour, local.minute)
        for _ in range(_MAX_DAY_SCAN):
            if self.matches_day(day):
                for candidate in self._times_on(day, tz, descending=True):
                    if ceiling is not None:
                        naive = (candidate.hour, candidate.minute)
                        if naive > ceiling:
                            continue
                    # Re-check against the real instant: a DST transition can
                    # move a local wall time to an instant on the wrong side.
                    if candidate <= moment:
                        return candidate
            day -= timedelta(days=1)
            ceiling = None
        return None

    def next(self, moment: datetime, *, tz: tzinfo) -> datetime | None:
        """The earliest occurrence strictly after ``moment``."""
        local = moment.astimezone(tz).replace(second=0, microsecond=0)
        day = local.date()
        floor: tuple[int, int] | None = (local.hour, local.minute)
        for _ in range(_MAX_DAY_SCAN):
            if self.matches_day(day):
                for candidate in self._times_on(day, tz, descending=False):
                    if floor is not None:
                        naive = (candidate.hour, candidate.minute)
                        if naive < floor:
                            continue
                    if candidate > moment:
                        return candidate
            day += timedelta(days=1)
            floor = None
        return None

    def occurrences_between(
        self, start: datetime, end: datetime, *, tz: tzinfo, limit: int
    ) -> list[datetime]:
        """Occurrences in ``(start, end]``, newest first, capped at ``limit``."""
        found: list[datetime] = []
        cursor = end
        while len(found) < limit:
            candidate = self.previous(cursor, tz=tz)
            if candidate is None or candidate <= start:
                break
            found.append(candidate)
            cursor = candidate - timedelta(minutes=1)
        return found

    def _times_on(self, day: date, tz: tzinfo, *, descending: bool) -> list[datetime]:
        hours = sorted(self.hours, reverse=descending)
        minutes = sorted(self.minutes, reverse=descending)
        return [
            datetime.combine(day, time(hour, minute), tzinfo=tz)
            for hour in hours
            for minute in minutes
        ]
