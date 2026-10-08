"""Structured metrics and logging hooks.

No metrics vendor is required or assumed. The queue calls a small sink
protocol; the default drops everything, and :class:`LoggingMetricsSink` turns
the same calls into structured log records for deployments that scrape logs.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "LoggingMetricsSink",
    "MetricsSink",
    "NullMetricsSink",
]


@runtime_checkable
class MetricsSink(Protocol):
    """Where the worker and scheduler report what they observed.

    The emitted series are:

    ``rqueue.queue.depth``            gauge, jobs pending or leased
    ``rqueue.queue.ready``            gauge, jobs eligible right now
    ``rqueue.queue.oldest_ready_age`` gauge, seconds
    ``rqueue.claim.latency``          timing, seconds for one claim round trip
    ``rqueue.claim.jobs``             counter, jobs actually leased
    ``rqueue.handler.duration``       timing, seconds of user code
    ``rqueue.job.succeeded``          counter
    ``rqueue.job.retried``            counter
    ``rqueue.job.failed``             counter
    ``rqueue.job.cancelled``          counter
    ``rqueue.lease.expired``          counter, leases recovered by the sweep
    ``rqueue.wakeup``                 counter, tagged source=notify|poll
    ``rqueue.schedule.fired``         counter
    """

    def counter(self, name: str, value: int = 1, **fields: Any) -> None: ...

    def gauge(self, name: str, value: float, **fields: Any) -> None: ...

    def timing(self, name: str, seconds: float, **fields: Any) -> None: ...


class NullMetricsSink:
    """The default sink: correct, free, and reports nothing."""

    def counter(self, name: str, value: int = 1, **fields: Any) -> None:
        return None

    def gauge(self, name: str, value: float, **fields: Any) -> None:
        return None

    def timing(self, name: str, seconds: float, **fields: Any) -> None:
        return None


class LoggingMetricsSink:
    """Emit each measurement as one structured DEBUG record."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("rqueue.metrics")

    def _emit(self, kind: str, name: str, value: float, fields: dict[str, Any]) -> None:
        self._logger.debug(
            "%s %s=%s",
            kind,
            name,
            value,
            extra={"metric": name, "metric_kind": kind, "value": value, **fields},
        )

    def counter(self, name: str, value: int = 1, **fields: Any) -> None:
        self._emit("counter", name, value, fields)

    def gauge(self, name: str, value: float, **fields: Any) -> None:
        self._emit("gauge", name, value, fields)

    def timing(self, name: str, seconds: float, **fields: Any) -> None:
        self._emit("timing", name, seconds, fields)
