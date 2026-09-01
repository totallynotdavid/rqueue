"""Per-task retry policy (REQUIREMENTS.md §5).

The persisted ``attempt`` column is the sole authority for how many attempts a
job has had; nothing here keeps in-process state, so a worker restart or a
crash mid-attempt does not reset a job's budget.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from rqueue.errors import PermanentFailure, ValidationError
from rqueue.limits import validate_max_attempts

__all__ = ["RetryPolicy"]

_JITTER_SOURCE = random.Random()  # noqa: S311 - jitter spreads retries, not secrets


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to retry a task, and how long to wait between attempts.

    ``jitter`` is a fraction of the computed delay, applied symmetrically and
    clamped at zero, so a fleet that fails together does not retry in lockstep.
    """

    max_attempts: int = 3
    initial_backoff: float = 1.0
    max_backoff: float = 3600.0
    multiplier: float = 2.0
    jitter: float = 0.1
    retry_on: Sequence[type[BaseException]] = (Exception,)
    retry_if: Callable[[BaseException], bool] | None = None
    _random: random.Random = field(default=_JITTER_SOURCE, repr=False, compare=False)

    def __post_init__(self) -> None:
        validate_max_attempts(self.max_attempts)
        if self.initial_backoff < 0:
            raise ValidationError("initial_backoff must be >= 0")
        if self.max_backoff < self.initial_backoff:
            raise ValidationError("max_backoff must be >= initial_backoff")
        if self.multiplier < 1:
            raise ValidationError("multiplier must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValidationError("jitter must be a fraction between 0 and 1")

    def backoff_seconds(self, attempt: int) -> float:
        """Delay before attempt ``attempt + 1``, given ``attempt`` has failed."""
        if attempt < 1:
            attempt = 1
        exponent = min(attempt - 1, 32)  # 2**32 already saturates max_backoff
        delay = min(self.initial_backoff * self.multiplier**exponent, self.max_backoff)
        if self.jitter:
            spread = delay * self.jitter
            delay += self._random.uniform(-spread, spread)
        return max(delay, 0.0)

    def next_attempt_at(self, attempt: int, *, now: datetime | None = None) -> datetime:
        reference = now or datetime.now(UTC)
        return reference + timedelta(seconds=self.backoff_seconds(attempt))

    def should_retry(self, exc: BaseException, *, attempt: int) -> bool:
        """Whether ``exc`` on ``attempt`` earns another attempt.

        A :class:`~rqueue.errors.PermanentFailure` is never retried regardless
        of the exception classes configured -- it is the handler stating that
        this job cannot succeed.
        """
        if attempt >= self.max_attempts:
            return False
        if isinstance(exc, PermanentFailure):
            return False
        if self.retry_if is not None:
            return bool(self.retry_if(exc))
        return isinstance(exc, tuple(self.retry_on))
