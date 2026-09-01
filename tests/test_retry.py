"""Retry policy arithmetic and predicates."""

from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from rqueue.errors import PermanentFailure, ValidationError
from rqueue.retry import RetryPolicy


def test_backoff_is_exponential_and_capped() -> None:
    policy = RetryPolicy(
        initial_backoff=1.0, multiplier=2.0, max_backoff=10.0, jitter=0.0
    )
    assert [policy.backoff_seconds(n) for n in (1, 2, 3, 4, 5)] == [
        1.0,
        2.0,
        4.0,
        8.0,
        10.0,
    ]


def test_backoff_never_goes_negative_with_jitter() -> None:
    policy = RetryPolicy(
        initial_backoff=0.01,
        jitter=1.0,
        _random=random.Random(1),
    )
    assert all(policy.backoff_seconds(n) >= 0 for n in range(1, 50))


def test_jitter_spreads_retries() -> None:
    policy = RetryPolicy(initial_backoff=10.0, jitter=0.5, _random=random.Random(7))
    delays = {policy.backoff_seconds(1) for _ in range(20)}
    assert len(delays) > 1
    assert all(5.0 <= delay <= 15.0 for delay in delays)


def test_next_attempt_at_is_in_the_future() -> None:
    policy = RetryPolicy(initial_backoff=5.0, jitter=0.0)
    now = datetime(2026, 9, 1, tzinfo=UTC)
    assert policy.next_attempt_at(1, now=now) == now.replace(second=5)


def test_attempt_budget_is_the_authority() -> None:
    policy = RetryPolicy(max_attempts=3)
    assert policy.should_retry(RuntimeError(), attempt=2)
    assert not policy.should_retry(RuntimeError(), attempt=3)
    assert not policy.should_retry(RuntimeError(), attempt=4)


def test_permanent_failure_is_never_retried() -> None:
    policy = RetryPolicy(max_attempts=10)
    assert not policy.should_retry(PermanentFailure("nope"), attempt=1)


def test_retry_on_narrows_the_exception_classes() -> None:
    policy = RetryPolicy(max_attempts=5, retry_on=(TimeoutError,))
    assert policy.should_retry(TimeoutError(), attempt=1)
    assert not policy.should_retry(ValueError(), attempt=1)


def test_retry_if_predicate_wins_over_classes() -> None:
    policy = RetryPolicy(
        max_attempts=5,
        retry_on=(TimeoutError,),
        retry_if=lambda exc: "transient" in str(exc),
    )
    assert policy.should_retry(ValueError("transient blip"), attempt=1)
    assert not policy.should_retry(TimeoutError("hard stop"), attempt=1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": 10_000},
        {"initial_backoff": -1.0},
        {"initial_backoff": 10.0, "max_backoff": 1.0},
        {"multiplier": 0.5},
        {"jitter": 2.0},
    ],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
