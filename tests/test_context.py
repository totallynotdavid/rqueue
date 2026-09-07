"""Task handler context behavior."""

from __future__ import annotations

import asyncio
import logging
import uuid

import pytest

from rqueue.context import TaskContext
from rqueue.errors import CancelJob, PermanentFailure, Retry
from rqueue.retry import RetryPolicy


@pytest.mark.parametrize(
    ("attempt", "exc", "expected"),
    [
        (1, RuntimeError("transient"), True),
        (2, RuntimeError("last attempt"), False),
        (1, PermanentFailure("terminal"), False),
    ],
)
def test_will_retry_matches_worker_retry_decision(
    attempt: int, exc: BaseException, expected: bool
) -> None:
    policy = RetryPolicy(max_attempts=2, jitter=0.0)
    context = _make_context(
        attempt=attempt,
        max_attempts=policy.max_attempts,
        retry=policy,
    )

    assert context.will_retry(exc) is expected


@pytest.mark.parametrize(
    ("exc", "expected"),
    [(Retry(), True), (CancelJob(), False)],
)
def test_will_retry_matches_control_flow_signals(
    exc: BaseException, expected: bool
) -> None:
    context = _make_context(
        attempt=1,
        max_attempts=1,
        retry=RetryPolicy(max_attempts=5),
    )

    assert context.will_retry(exc) is expected


def test_will_retry_uses_the_persisted_job_attempt_limit() -> None:
    context = _make_context(
        attempt=1,
        max_attempts=1,
        retry=RetryPolicy(max_attempts=5),
    )

    assert context.is_last_attempt
    assert not context.will_retry(RuntimeError("last attempt"))


def test_will_retry_reuses_a_decision_for_the_worker() -> None:
    calls = 0

    def retry_if(exc: BaseException) -> bool:
        nonlocal calls
        calls += 1
        return calls == 1

    context = _make_context(
        attempt=1,
        max_attempts=2,
        retry=RetryPolicy(max_attempts=5, retry_if=retry_if),
    )
    exc = RuntimeError("stateful predicate")

    assert context.will_retry(exc)
    assert context.will_retry(exc)
    assert calls == 1


def _make_context(
    *, attempt: int, max_attempts: int, retry: RetryPolicy
) -> TaskContext:
    return TaskContext(
        job_id=uuid.uuid4(),
        queue="test",
        task="example",
        attempt=attempt,
        max_attempts=max_attempts,
        retry=retry,
        metadata={},
        heartbeat=_heartbeat,
        cancel_event=asyncio.Event(),
        logger=logging.getLogger("test"),
    )


async def _heartbeat() -> bool:
    return False
