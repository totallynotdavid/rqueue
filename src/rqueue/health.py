"""Readiness checks that distinguish their failure modes (§7).

A single "ready: false" is useless during an incident. This reports
PostgreSQL connectivity, migration version, worker availability, and scheduler
availability separately, so a probe can tell "the database is down" from "the
schema is behind" from "nobody is consuming this queue".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from rqueue import migrations
from rqueue.storage import Storage

if TYPE_CHECKING:
    import asyncpg

__all__ = ["Readiness", "check_readiness"]


@dataclass(frozen=True, slots=True)
class Readiness:
    """The result of one readiness probe."""

    connected: bool
    schema_version: int | None
    expected_schema_version: int
    workers: tuple[str, ...] = ()
    schedulers: tuple[str, ...] = ()
    problems: tuple[str, ...] = field(default_factory=tuple)
    require_worker: bool = True
    require_scheduler: bool = False

    @property
    def migrations_up_to_date(self) -> bool:
        return self.schema_version == self.expected_schema_version

    @property
    def worker_available(self) -> bool:
        return bool(self.workers)

    @property
    def scheduler_available(self) -> bool:
        return bool(self.schedulers)

    @property
    def ready(self) -> bool:
        if not self.connected or not self.migrations_up_to_date:
            return False
        if self.require_worker and not self.worker_available:
            return False
        return not (self.require_scheduler and not self.scheduler_available)


async def check_readiness(
    pool: asyncpg.Pool,
    *,
    schema: str = "task_queue",
    queue: str | None = None,
    worker_staleness: timedelta = timedelta(seconds=60),
    scheduler_staleness: timedelta = timedelta(seconds=300),
    require_worker: bool = True,
    require_scheduler: bool = False,
) -> Readiness:
    """Probe every dependency independently and report each one."""
    storage = Storage(schema)
    expected = max((m.version for m in migrations.load_migrations()), default=0)
    problems: list[str] = []
    try:
        async with pool.acquire() as connection:
            await connection.execute("SELECT 1")
            try:
                version = await migrations.current_version(connection, schema=schema)
            except Exception as exc:
                problems.append(f"migration state unreadable: {exc}")
                version = None
            now = datetime.now(UTC)
            workers = tuple(
                await storage.live_instances(
                    connection,
                    kind="worker",
                    since=now - worker_staleness,
                    queue=queue,
                )
            )
            schedulers = tuple(
                await storage.live_instances(
                    connection,
                    kind="scheduler",
                    since=now - scheduler_staleness,
                    queue=queue,
                )
            )
    except Exception as exc:
        return Readiness(
            connected=False,
            schema_version=None,
            expected_schema_version=expected,
            problems=(f"cannot reach PostgreSQL: {exc}",),
            require_worker=require_worker,
            require_scheduler=require_scheduler,
        )

    if version is not None and version != expected:
        problems.append(f"schema is at migration {version}, package expects {expected}")
    if require_worker and not workers:
        problems.append("no worker heartbeat within the staleness window")
    if require_scheduler and not schedulers:
        problems.append("no scheduler heartbeat within the staleness window")

    return Readiness(
        connected=True,
        schema_version=version,
        expected_schema_version=expected,
        workers=workers,
        schedulers=schedulers,
        problems=tuple(problems),
        require_worker=require_worker,
        require_scheduler=require_scheduler,
    )
