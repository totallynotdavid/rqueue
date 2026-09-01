"""The task registry: explicit names, explicit decoders, async handlers only."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from rqueue.context import TaskContext
from rqueue.errors import ConfigurationError, ValidationError
from rqueue.limits import (
    MAX_TASK_NAME_LENGTH,
    validate_name,
    validate_timeout,
)
from rqueue.retry import RetryPolicy

__all__ = ["PayloadDecoder", "TaskHandler", "TaskRegistration", "identity_decoder"]

#: A handler takes the decoded payload and its context, and returns nothing.
type TaskHandler = Callable[[Any, TaskContext], Awaitable[None]]

#: Turns the JSON payload into whatever the handler wants. A Pydantic
#: ``TypeAdapter(...).validate_python`` and a plain function both satisfy it.
type PayloadDecoder = Callable[[Any], Any]


def identity_decoder(payload: Any) -> Any:
    """The default decoder: hand the parsed JSON to the handler unchanged."""
    return payload


@dataclass(frozen=True, slots=True)
class TaskRegistration:
    """One registered task name and everything the worker needs to run it."""

    name: str
    handler: TaskHandler
    decoder: PayloadDecoder
    retry: RetryPolicy
    timeout: float | None

    def __post_init__(self) -> None:
        validate_name(self.name, kind="task name", max_length=MAX_TASK_NAME_LENGTH)
        validate_timeout(self.timeout)
        if not callable(self.handler):
            raise ConfigurationError(f"handler for task {self.name!r} is not callable")
        # §4 is async-only on purpose: pgqueuer shipped sync-handler support and
        # removed it again. One execution path, and blocking work is wrapped by
        # the handler with asyncio.to_thread, which the Worker's bounded default
        # executor then caps.
        if not inspect.iscoroutinefunction(self.handler):
            raise ConfigurationError(
                f"handler for task {self.name!r} must be an 'async def' function; "
                "wrap blocking work with 'await asyncio.to_thread(fn, ...)' inside "
                "an async handler instead"
            )
        if not callable(self.decoder):
            raise ValidationError(f"decoder for task {self.name!r} is not callable")
