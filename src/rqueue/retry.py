"""Per-task retry policy (REQUIREMENTS.md §5).

The persisted ``attempt`` column is the sole authority for how many attempts a
job has had; nothing here keeps in-process state, so a worker restart or a
crash mid-attempt does not reset a job's budget.
"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from rqueue.errors import ConfigurationError, PermanentFailure, ValidationError
from rqueue.limits import validate_max_attempts

__all__ = ["RetryPolicy", "RetryPolicyData"]

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
        _validate_numeric_settings(
            max_attempts=self.max_attempts,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            multiplier=self.multiplier,
            jitter=self.jitter,
        )

    def backoff_seconds(self, attempt: int) -> float:
        """Delay before attempt ``attempt + 1``, given ``attempt`` has failed."""
        if attempt < 1:
            attempt = 1
        exponent = min(attempt - 1, 32)  # 2**32 already saturates max_backoff
        if self.initial_backoff == 0:
            delay = 0.0
        else:
            try:
                delay = min(
                    self.initial_backoff * self.multiplier**exponent,
                    self.max_backoff,
                )
            except OverflowError:
                # A finite multiplier can still overflow while raising it to
                # the bounded exponent, or while converting a huge integer
                # result for multiplication. The configured cap is the
                # correct result once the uncapped exponential has exceeded it.
                delay = self.max_backoff
        if self.jitter:
            spread = delay * self.jitter
            delay += self._random.uniform(-spread, spread)
        return max(delay, 0.0)

    def next_attempt_at(self, attempt: int, *, now: datetime | None = None) -> datetime:
        reference = now or datetime.now(UTC)
        delay = self.backoff_seconds(attempt)
        # Bound the delay before constructing the timedelta. Using whole
        # seconds avoids a float rounding step crossing datetime.max by one
        # microsecond at the upper edge of the representable range.
        maximum = datetime.max.replace(tzinfo=reference.tzinfo) - reference
        maximum_seconds = maximum.days * 86_400 + maximum.seconds
        return reference + timedelta(seconds=min(delay, maximum_seconds))

    def should_retry(
        self,
        exc: BaseException,
        *,
        attempt: int,
        max_attempts: int | None = None,
    ) -> bool:
        """Whether ``exc`` on ``attempt`` earns another attempt.

        ``max_attempts`` overrides the policy default for a job whose persisted
        attempt budget differs from the task registration.
        A :class:`~rqueue.errors.PermanentFailure` is never retried regardless
        of the exception classes configured -- it is the handler stating that
        this job cannot succeed.
        """
        attempt_limit = (
            self.max_attempts
            if max_attempts is None
            else validate_max_attempts(max_attempts)
        )
        if attempt >= attempt_limit:
            return False
        if isinstance(exc, PermanentFailure):
            return False
        if self.retry_if is not None:
            return bool(self.retry_if(exc))
        return isinstance(exc, tuple(self.retry_on))


@dataclass(frozen=True, slots=True)
class RetryPolicyData:
    """The pure-data part of a retry policy that is safe to persist.

    Exception classes and ``retry_if`` are deliberately absent. They are
    executable registration-time choices and remain owned by the worker that
    registered the handler. This value contains only the numeric settings that
    an independent producer and worker need to agree on.
    """

    max_attempts: int
    initial_backoff: float
    max_backoff: float
    multiplier: float
    jitter: float

    def __post_init__(self) -> None:
        _validate_numeric_settings(
            max_attempts=self.max_attempts,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            multiplier=self.multiplier,
            jitter=self.jitter,
        )

    @classmethod
    def from_policy(cls, policy: RetryPolicy) -> RetryPolicyData:
        """Take only the safe, numeric settings from a runtime policy."""
        return cls(
            max_attempts=policy.max_attempts,
            initial_backoff=policy.initial_backoff,
            max_backoff=policy.max_backoff,
            multiplier=policy.multiplier,
            jitter=policy.jitter,
        )

    def to_json(self) -> str:
        """Serialize only numeric retry settings; never executable objects."""
        return json.dumps(
            {
                "version": 1,
                "max_attempts": self.max_attempts,
                "initial_backoff": self.initial_backoff,
                "max_backoff": self.max_backoff,
                "multiplier": self.multiplier,
                "jitter": self.jitter,
            },
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(
        cls, value: str | bytes | bytearray | Mapping[str, Any]
    ) -> RetryPolicyData:
        """Read and validate the numeric settings stored on a job row.

        The exact key set is intentional. A database value containing
        ``retry_if`` or exception references is rejected as malformed data;
        it is never interpreted as Python.
        """
        try:
            data: object = (
                dict(value) if isinstance(value, Mapping) else json.loads(value)
            )
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(
                "persisted retry policy is not valid JSON"
            ) from exc
        if not isinstance(data, dict) or set(data) != _PERSISTED_POLICY_KEYS:
            raise ConfigurationError("persisted retry policy has an invalid shape")
        version = data.get("version")
        if not isinstance(version, int) or isinstance(version, bool) or version != 1:
            raise ConfigurationError(
                "persisted retry policy has an unsupported version"
            )
        try:
            return cls(
                max_attempts=data["max_attempts"],
                initial_backoff=data["initial_backoff"],
                max_backoff=data["max_backoff"],
                multiplier=data["multiplier"],
                jitter=data["jitter"],
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ConfigurationError(
                "persisted retry policy has an invalid shape"
            ) from exc

    def apply_to(self, policy: RetryPolicy) -> RetryPolicy:
        """Apply persisted numeric settings while retaining local code hooks."""
        return replace(
            policy,
            max_attempts=self.max_attempts,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            multiplier=self.multiplier,
            jitter=self.jitter,
        )

    def to_policy(self) -> RetryPolicy:
        """Build a hook-free runtime policy from these numeric settings."""
        return RetryPolicy(
            max_attempts=self.max_attempts,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            multiplier=self.multiplier,
            jitter=self.jitter,
        )


_PERSISTED_POLICY_KEYS = frozenset(
    {
        "version",
        "max_attempts",
        "initial_backoff",
        "max_backoff",
        "multiplier",
        "jitter",
    }
)


def _validate_numeric_settings(
    *,
    max_attempts: int,
    initial_backoff: float,
    max_backoff: float,
    multiplier: float,
    jitter: float,
) -> None:
    validate_max_attempts(max_attempts)
    numbers = {
        "initial_backoff": initial_backoff,
        "max_backoff": max_backoff,
        "multiplier": multiplier,
        "jitter": jitter,
    }
    for name, value in numbers.items():
        if not isinstance(value, int | float) or isinstance(value, bool):
            raise ValidationError(f"{name} must be a finite number")
        try:
            finite = math.isfinite(float(value))
        except (OverflowError, ValueError):
            finite = False
        if not finite:
            raise ValidationError(f"{name} must be a finite number")
    if initial_backoff < 0:
        raise ValidationError("initial_backoff must be >= 0")
    if max_backoff < initial_backoff:
        raise ValidationError("max_backoff must be >= initial_backoff")
    if multiplier < 1:
        raise ValidationError("multiplier must be >= 1")
    if not 0 <= jitter <= 1:
        raise ValidationError("jitter must be a fraction between 0 and 1")
