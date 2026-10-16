"""The task registry: explicit names, explicit decoders, async handlers only."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any

from rqueue.context import TaskContext
from rqueue.errors import ConfigurationError, ValidationError
from rqueue.limits import (
    MAX_TASK_NAME_LENGTH,
    validate_name,
    validate_timeout,
)
from rqueue.retry import RetryPolicy, RetryPolicyData

__all__ = [
    "PayloadDecoder",
    "TaskDeclaration",
    "TaskHandler",
    "TaskRegistration",
    "identity_decoder",
]

#: A handler takes the decoded payload and its context, and returns nothing.
type TaskHandler = Callable[[Any, TaskContext], Awaitable[None]]

#: Turns the JSON payload into whatever the handler wants. A Pydantic
#: ``TypeAdapter(...).validate_python`` and a plain function both satisfy it.
type PayloadDecoder = Callable[[Any], Any]


def identity_decoder(payload: Any) -> Any:
    """The default decoder: hand the parsed JSON to the handler unchanged."""
    return payload


def _require_retry_policy(value: object, *, task_name: str) -> RetryPolicy:
    if not isinstance(value, RetryPolicy):
        raise ConfigurationError(
            f"retry for task {task_name!r} must be a RetryPolicy instance"
        )
    return value


@dataclass(frozen=True, slots=True)
class TaskDeclaration:
    """Enqueue-relevant metadata for one task name.

    It has no handler or decoder, so a producer can set retry and timeout
    defaults without importing worker code.
    """

    name: str
    retry: RetryPolicy
    timeout: float | None

    def __post_init__(self) -> None:
        validate_name(self.name, kind="task name", max_length=MAX_TASK_NAME_LENGTH)
        _require_retry_policy(self.retry, task_name=self.name)
        validate_timeout(self.timeout)


@dataclass(frozen=True, slots=True)
class TaskRegistration:
    """One declared task name plus everything the worker needs to run it."""

    declaration: TaskDeclaration
    handler: TaskHandler
    decoder: PayloadDecoder = identity_decoder

    @property
    def name(self) -> str:
        return self.declaration.name

    @property
    def retry(self) -> RetryPolicy:
        return self.declaration.retry

    @property
    def timeout(self) -> float | None:
        return self.declaration.timeout

    def for_job(
        self,
        *,
        retry_policy: RetryPolicyData | None,
        max_attempts: int,
        timeout: float | None,
    ) -> TaskRegistration:
        """Bind the persisted enqueue defaults for a job to this handler.

        A job is durable and may have been inserted by a different process
        with a different ``Queue`` instance. Its numeric retry settings are
        therefore authoritative over the worker process's local registration.
        Retryable exception classes and ``retry_if`` remain local registration
        choices and are never read from the job row. A NULL timeout means the
        job did not specify one, so the registration timeout remains the
        fallback.
        """
        effective_retry = (
            retry_policy.apply_to(self.retry)
            if retry_policy is not None
            else self.retry
        )
        # The database column is the authoritative attempt ceiling.
        effective_retry = replace(effective_retry, max_attempts=max_attempts)
        declaration = TaskDeclaration(
            name=self.name,
            retry=effective_retry,
            timeout=timeout if timeout is not None else self.timeout,
        )
        return type(self)(
            declaration=declaration,
            handler=self.handler,
            decoder=self.decoder,
        )

    def __post_init__(self) -> None:
        if not callable(self.handler):
            raise ConfigurationError(f"handler for task {self.name!r} is not callable")
        if not inspect.iscoroutinefunction(self.handler):
            raise ConfigurationError(
                f"handler for task {self.name!r} must be an 'async def' function; "
                "wrap blocking work with 'await asyncio.to_thread(fn, ...)' inside "
                "an async handler instead"
            )
        if not callable(self.decoder):
            raise ValidationError(f"decoder for task {self.name!r} is not callable")
