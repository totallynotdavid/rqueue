"""Cron parsing and occurrence arithmetic."""

from __future__ import annotations

from datetime import datetime

import pytest

from rqueue.cron import CronExpression, resolve_timezone
from rqueue.errors import ValidationError

UTC = resolve_timezone("UTC")
LIMA = resolve_timezone("America/Lima")


def at(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("*/5 * * * *", at(2026, 9, 1, 12, 5)),
        ("0 * * * *", at(2026, 9, 1, 12, 0)),
        ("@daily", at(2026, 9, 1, 0, 0)),
        ("30 11 * * *", at(2026, 9, 1, 11, 30)),
        ("0 0 1 * *", at(2026, 9, 1, 0, 0)),
    ],
)
def test_previous_occurrence(expression: str, expected: datetime) -> None:
    cron = CronExpression.parse(expression)
    assert cron.previous(at(2026, 9, 1, 12, 7), tz=UTC) == expected


def test_next_is_strictly_after() -> None:
    cron = CronExpression.parse("*/5 * * * *")
    moment = at(2026, 9, 1, 12, 5)
    assert cron.next(moment, tz=UTC) == at(2026, 9, 1, 12, 10)


def test_previous_is_inclusive() -> None:
    cron = CronExpression.parse("*/5 * * * *")
    moment = at(2026, 9, 1, 12, 5)
    assert cron.previous(moment, tz=UTC) == moment


def test_named_month_and_weekday() -> None:
    cron = CronExpression.parse("0 3 * jan mon")
    following = cron.next(at(2026, 1, 1, 0, 0), tz=UTC)
    assert following is not None
    assert following.month == 1
    assert following.weekday() == 0
    assert (following.hour, following.minute) == (3, 0)


def test_dom_and_dow_both_restricted_is_a_union() -> None:
    # Vixie cron's rule: with both fields restricted, either one matching is
    # enough. 2026-09-13 is a Sunday, and the 15th is a Tuesday.
    cron = CronExpression.parse("0 0 15 9 sun")
    days = {
        occurrence.day
        for occurrence in cron.occurrences_between(
            at(2026, 9, 1), at(2026, 9, 20), tz=UTC, limit=20
        )
    }
    assert 15 in days
    assert 13 in days


def test_timezone_is_respected() -> None:
    cron = CronExpression.parse("0 3 * * *")
    occurrence = cron.previous(at(2026, 9, 1, 12, 0), tz=LIMA)
    assert occurrence is not None
    assert occurrence.astimezone(LIMA).hour == 3
    assert occurrence.astimezone(UTC).hour == 8


def test_occurrences_between_is_bounded_and_ordered() -> None:
    cron = CronExpression.parse("*/5 * * * *")
    found = cron.occurrences_between(
        at(2026, 9, 1, 11, 40), at(2026, 9, 1, 12, 7), tz=UTC, limit=3
    )
    assert len(found) == 3
    assert found == sorted(found, reverse=True)


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "5-1 * * * *",
        "*/0 * * * *",
        "banana * * * *",
        "*-3 * * * *",
    ],
)
def test_invalid_expressions_are_rejected(expression: str) -> None:
    with pytest.raises(ValidationError):
        CronExpression.parse(expression)


def test_unknown_timezone_is_rejected() -> None:
    with pytest.raises(ValidationError):
        resolve_timezone("Mars/Olympus_Mons")


def test_dst_spring_forward_still_fires_once_a_day() -> None:
    # Lima has no DST; use a zone that does. 2026-03-08 is the US change.
    tz = resolve_timezone("America/New_York")
    cron = CronExpression.parse("30 2 * * *")
    occurrences = cron.occurrences_between(
        datetime(2026, 3, 6, tzinfo=UTC),
        datetime(2026, 3, 11, tzinfo=UTC),
        tz=tz,
        limit=10,
    )
    assert len(occurrences) == len(set(occurrences))
