"""The only module that reads or writes job and schedule rows (REQUIREMENTS.md §8).

Every statement here is a module-level constant built once from a fixed tuple
of column names and a schema identifier that has been through
:func:`rqueue.limits.validate_identifier`. No caller value is ever
interpolated: values reach PostgreSQL exclusively as bind parameters.
Schema DDL and role grants are separate concerns handled by
:mod:`rqueue.migrations` and :mod:`rqueue.roles`; this module is only the
runtime read/write path that :class:`~rqueue.Queue`, :class:`~rqueue.Worker`,
:class:`~rqueue.Admin`, and :class:`~rqueue.Scheduler` share.

JSON columns are always read as ``::text`` and written as ``$n::text::jsonb``.
The connection belongs to the application, which may have installed its own
``jsonb`` codec on it; casting on both sides makes rqueue's behaviour identical
either way.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import asyncpg

from rqueue.errors import (
    ConfigurationError,
    JobNotFound,
    LeaseLost,
    ScheduleNotFound,
    ValidationError,
)
from rqueue.limits import (
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_QUEUE_NAME_LENGTH,
    truncate,
    validate_identifier,
    validate_name,
    validate_purge_limit,
)
from rqueue.models import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    Attempt,
    Job,
    JobState,
    QueuePause,
    QueueStats,
    Schedule,
)
from rqueue.retry import RetryPolicyData

__all__ = ["ClaimedJob", "JobInsert", "Storage"]

_JOB_FIELDS: Final = (
    "id",
    "queue",
    "task",
    "payload",
    "state",
    "priority",
    "attempt",
    "max_attempts",
    "scheduled_at",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "dedupe_key",
    "concurrency_key",
    "worker_id",
    "lease_token",
    "leased_until",
    "heartbeat_at",
    "cancel_requested",
    "timeout_seconds",
    "error_type",
    "error_message",
    "metadata",
    "retry_policy",
)
_JSON_FIELDS: Final = frozenset({"payload", "metadata", "retry_policy"})

_SCHEDULE_FIELDS: Final = (
    "id",
    "name",
    "queue",
    "task",
    "payload",
    "cron",
    "timezone",
    "enabled",
    "priority",
    "max_attempts",
    "concurrency_key",
    "created_at",
    "updated_at",
)

#: ``pg_notify`` channels are identifiers, capped at 63 bytes. The channel is
#: ``rqueue_<schema>``, so the schema has a tighter cap than PostgreSQL's own.
_MAX_SCHEMA_LENGTH_FOR_CHANNEL: Final = 63 - len("rqueue_")


def _columns(fields: Sequence[str], prefix: str = "") -> str:
    qualifier = f"{prefix}." if prefix else ""
    return ", ".join(
        f"{qualifier}{name}::text AS {name}"
        if name in _JSON_FIELDS
        else f"{qualifier}{name}"
        for name in fields
    )


def _invalid_policy_message(value: Any) -> str:
    message = truncate(
        f"invalid persisted retry policy: {value}",
        MAX_ERROR_MESSAGE_LENGTH,
    )
    assert message is not None  # noqa: S101 - the input is always rendered
    return message


@dataclass(frozen=True, slots=True)
class JobInsert:
    """A fully validated job row, ready to be written.

    Validation happens in :mod:`rqueue.queue` before any statement is sent, so
    that ``enqueue_many`` can check an entire batch before writing a row.
    """

    id: uuid.UUID
    queue: str
    task: str
    payload_json: str
    priority: int
    max_attempts: int
    scheduled_at: datetime | None
    dedupe_key: str | None
    concurrency_key: str | None
    timeout_seconds: float | None
    metadata_json: str
    retry_policy_json: str | None
    raise_on_conflict: bool


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """A job plus the lease token that authorizes writes to this attempt."""

    job: Job
    lease_token: uuid.UUID
    leased_until: datetime


class Storage:
    """Static, parameterized SQL for one schema."""

    def __init__(self, schema: str = "task_queue") -> None:
        schema = validate_identifier(schema, kind="schema")
        if len(schema) > _MAX_SCHEMA_LENGTH_FOR_CHANNEL:
            raise ValidationError(
                f"schema must be at most {_MAX_SCHEMA_LENGTH_FOR_CHANNEL} characters "
                "so the rqueue_<schema> NOTIFY channel fits in an identifier"
            )
        self.schema = schema
        self.notify_channel = f"rqueue_{schema}"
        self._sql = _Statements(schema)

    # ---------------------------------------------------------------- enqueue

    async def insert_job(
        self, connection: asyncpg.Connection, spec: JobInsert
    ) -> tuple[Job, bool]:
        """Insert one job; return it and whether this call created it.

        The dedupe collision path is an ``ON CONFLICT ... DO UPDATE`` that
        writes nothing, rather than ``DO NOTHING``. ``DO NOTHING`` does not
        wait on a concurrent inserter, so a racing producer would get neither
        an insert nor a row to return -- exactly the "skip mode drops silently"
        problem §1 calls out in pgqueuer. ``DO UPDATE`` blocks on the other
        transaction and then returns the row that won.
        """
        row = await connection.fetchrow(
            self._sql.insert_job,
            spec.id,
            spec.queue,
            spec.task,
            spec.payload_json,
            spec.priority,
            spec.max_attempts,
            spec.scheduled_at,
            spec.dedupe_key,
            spec.concurrency_key,
            spec.timeout_seconds,
            spec.retry_policy_json,
            spec.metadata_json,
        )
        assert row is not None  # noqa: S101 - INSERT ... RETURNING always yields a row
        return Job.from_row(row), row["id"] == spec.id

    # ------------------------------------------------------------------ claim

    async def claim(
        self,
        connection: asyncpg.Connection,
        *,
        queue: str,
        worker_id: str,
        tasks: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[ClaimedJob]:
        """Lease up to ``limit`` due jobs for ``worker_id``.

        One short transaction, never held across user code (§3). Candidate rows
        are taken with ``FOR UPDATE SKIP LOCKED``; a candidate that carries a
        named concurrency key must additionally win that key's slot, which is
        where two workers racing for one business resource are serialized.
        """
        if not tasks:
            return []
        async with connection.transaction():
            candidates = await connection.fetch(
                self._sql.claim_candidates, queue, list(tasks), limit
            )
            if not candidates:
                return []

            ids: list[uuid.UUID] = []
            tokens: list[uuid.UUID] = []
            malformed: dict[uuid.UUID, str] = {}
            for candidate in candidates:
                raw_policy = candidate["retry_policy"]
                policy_error: str | None = None
                if raw_policy is not None:
                    try:
                        RetryPolicyData.from_json(raw_policy)
                    except ConfigurationError:
                        policy_error = _invalid_policy_message(raw_policy)
                token = uuid.uuid4()
                key = candidate["concurrency_key"]
                if key is not None:
                    acquired = await connection.fetchval(
                        self._sql.acquire_slot,
                        queue,
                        key,
                        candidate["id"],
                        token,
                        worker_id,
                        lease_seconds,
                    )
                    if acquired is None:
                        continue
                if policy_error is not None:
                    await connection.execute(
                        self._sql.quarantine_pending_policy,
                        candidate["id"],
                        policy_error,
                    )
                    malformed[candidate["id"]] = policy_error
                ids.append(candidate["id"])
                tokens.append(token)

            if not ids:
                return []

            rows = await connection.fetch(
                self._sql.lease_jobs, ids, tokens, worker_id, lease_seconds
            )
            claimed: list[ClaimedJob] = []
            malformed_leased: list[tuple[uuid.UUID, uuid.UUID, str]] = []
            for row in rows:
                leased_policy_error = malformed.get(row["id"])
                if leased_policy_error is not None:
                    malformed_leased.append(
                        (row["id"], row["lease_token"], leased_policy_error)
                    )
                else:
                    claimed.append(
                        ClaimedJob(
                            job=Job.from_row(row),
                            lease_token=row["lease_token"],
                            leased_until=row["leased_until"],
                        )
                    )
            await connection.execute(
                self._sql.open_attempts, [row["id"] for row in rows]
            )
            for job_id, lease_token, policy_error in malformed_leased:
                await connection.fetchrow(
                    self._sql.fail_invalid_policy,
                    job_id,
                    lease_token,
                    "ConfigurationError",
                    policy_error,
                )
                await connection.execute(
                    self._sql.close_attempt,
                    job_id,
                    lease_token,
                    "failed",
                    "ConfigurationError",
                    policy_error,
                )
                await connection.execute(self._sql.release_slot, job_id, lease_token)

        return claimed

    # ---------------------------------------------------- lease-fenced writes

    async def heartbeat(
        self,
        connection: asyncpg.Connection,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        lease_seconds: float,
    ) -> bool:
        """Extend a lease; return whether cancellation has been requested.

        Raises :class:`LeaseLost` if this token is no longer the current one --
        the fencing check, made by SQL predicate rather than by reading the row
        and deciding in Python.
        """
        async with connection.transaction():
            row = await connection.fetchrow(
                self._sql.heartbeat, job_id, lease_token, lease_seconds
            )
            if row is None:
                raise LeaseLost(
                    f"job {job_id} is no longer leased under token {lease_token}"
                )
            await connection.execute(
                self._sql.extend_slot, job_id, lease_token, lease_seconds
            )
        return bool(row["cancel_requested"])

    async def complete(
        self,
        connection: asyncpg.Connection,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
    ) -> Job:
        return await self._finalize(
            connection,
            self._sql.complete,
            job_id=job_id,
            lease_token=lease_token,
            outcome="succeeded",
        )

    async def fail_terminal(
        self,
        connection: asyncpg.Connection,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        error_type: str,
        error_message: str,
    ) -> Job:
        return await self._finalize(
            connection,
            self._sql.fail_terminal,
            job_id=job_id,
            lease_token=lease_token,
            outcome="failed",
            error_type=error_type,
            error_message=error_message,
        )

    async def cancel_leased(
        self,
        connection: asyncpg.Connection,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        reason: str,
    ) -> Job:
        return await self._finalize(
            connection,
            self._sql.cancel_leased,
            job_id=job_id,
            lease_token=lease_token,
            outcome="cancelled",
            error_type="Cancelled",
            error_message=reason,
        )

    async def reschedule(
        self,
        connection: asyncpg.Connection,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        retry_at: datetime,
        error_type: str,
        error_message: str,
    ) -> Job:
        """Return a leased job to ``pending`` for a later attempt."""
        return await self._finalize(
            connection,
            self._sql.reschedule,
            job_id=job_id,
            lease_token=lease_token,
            outcome="retry",
            error_type=error_type,
            error_message=error_message,
            extra=(retry_at,),
        )

    async def _finalize(
        self,
        connection: asyncpg.Connection,
        statement: str,
        *,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        outcome: str,
        error_type: str | None = None,
        error_message: str | None = None,
        extra: tuple[Any, ...] = (),
    ) -> Job:
        """Apply one lease-fenced terminal or retry transition.

        Job row, attempt record, and concurrency slot move together in one
        transaction so an observer never sees a finished job still holding a
        concurrency slot.
        """
        args: tuple[Any, ...] = (job_id, lease_token, *extra)
        if error_type is not None or error_message is not None:
            args = (*args, error_type, error_message)
        async with connection.transaction():
            row = await connection.fetchrow(statement, *args)
            if row is None:
                raise LeaseLost(
                    f"job {job_id} is no longer leased under token {lease_token}; "
                    "another attempt has taken over"
                )
            await connection.execute(
                self._sql.close_attempt,
                job_id,
                lease_token,
                outcome,
                error_type,
                error_message,
            )
            await connection.execute(self._sql.release_slot, job_id, lease_token)
        return Job.from_row(row)

    # -------------------------------------------------------------- recovery

    async def recover_expired_leases(
        self,
        connection: asyncpg.Connection,
        *,
        queue: str,
        limit: int = 100,
    ) -> list[uuid.UUID]:
        """Make jobs whose lease expired eligible again, or fail them.

        Clearing ``lease_token`` is what makes the stale holder's next write
        fail: its token no longer matches any row, so ``complete``/``fail``
        raise :class:`LeaseLost` (§10.5). A job that has already used its last
        attempt becomes a durable failure instead of looping forever, and a job
        cancelled while leased is finalized here if its holder never came back.
        """
        rows = await connection.fetch(self._sql.recover_expired, queue, limit)
        return [row["id"] for row in rows]

    # ------------------------------------------------------------ inspection

    async def get_job(
        self, connection: asyncpg.Connection, job_id: uuid.UUID
    ) -> Job | None:
        row = await connection.fetchrow(self._sql.get_job, job_id)
        return Job.from_row(row) if row is not None else None

    async def list_jobs(
        self,
        connection: asyncpg.Connection,
        *,
        queue: str | None = None,
        states: Sequence[JobState | str] | None = None,
        task: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        state_values = [str(state) for state in states] if states else None
        rows = await connection.fetch(
            self._sql.list_jobs, queue, state_values, task, limit, offset
        )
        return [Job.from_row(row) for row in rows]

    async def attempts(
        self, connection: asyncpg.Connection, job_id: uuid.UUID
    ) -> list[Attempt]:
        rows = await connection.fetch(self._sql.list_attempts, job_id)
        return [Attempt.from_row(row) for row in rows]

    async def stats(self, connection: asyncpg.Connection, *, queue: str) -> QueueStats:
        row = await connection.fetchrow(self._sql.stats, queue)
        assert row is not None  # noqa: S101 - aggregate query always returns a row
        return QueueStats(
            queue=queue,
            pending=row["pending"],
            leased=row["leased"],
            succeeded=row["succeeded"],
            failed=row["failed"],
            cancelled=row["cancelled"],
            ready=row["ready"],
            oldest_ready_age_seconds=row["oldest_ready_age_seconds"],
            expired_leases=row["expired_leases"],
        )

    async def distinct_pending_tasks(
        self, connection: asyncpg.Connection, *, queue: str
    ) -> list[str]:
        rows = await connection.fetch(self._sql.distinct_active_tasks, queue)
        return [row["task"] for row in rows]

    # ---------------------------------------------------------- administration

    async def cancel(self, connection: asyncpg.Connection, job_id: uuid.UUID) -> Job:
        """Cancel a job.

        A pending job is cancelled outright. A leased job only gets
        ``cancel_requested``; §5 makes cancellation cooperative, so the lease
        holder -- or lease expiry -- finalizes it.
        """
        row = await connection.fetchrow(self._sql.cancel, job_id)
        if row is None:
            raise JobNotFound(f"no job with id {job_id}")
        return Job.from_row(row)

    async def retry_terminal(
        self,
        connection: asyncpg.Connection,
        job_id: uuid.UUID,
        *,
        scheduled_at: datetime | None = None,
        additional_attempts: int = 1,
    ) -> Job:
        """Operator retry: the only path from a terminal state back to pending."""
        row = await connection.fetchrow(
            self._sql.retry_terminal, job_id, scheduled_at, additional_attempts
        )
        if row is None:
            existing = await self.get_job(connection, job_id)
            if existing is None:
                raise JobNotFound(f"no job with id {job_id}")
            raise LeaseLost(
                f"job {job_id} is {existing.state}, not terminal; only a terminal "
                "job can be retried by an operator"
            )
        return Job.from_row(row)

    async def purge(
        self,
        connection: asyncpg.Connection,
        *,
        queue: str | None,
        older_than: datetime,
        states: Sequence[str] = TERMINAL_STATES,
        limit: int = 10000,
    ) -> int:
        """Delete terminal jobs through the routine that bounds the delete.

        Not a ``DELETE`` statement: the deleting is done by
        ``purge_terminal_jobs`` (migration 0006), which re-derives queue,
        terminal state, cutoff, and batch size for itself. That is what lets a
        role holding nothing but :attr:`~rqueue.Capability.PURGE` run this --
        it has no ``DELETE`` on ``jobs`` and is not meant to.

        The routine takes one queue, because "every queue" is precisely the
        unbounded delete it exists to refuse. ``queue=None`` therefore fans out
        over the queues that have something to purge -- but a fan-out is only
        oldest-first if something stops the first queue swallowing the budget,
        and visiting queues in age order is not that something. So the cutoff
        is tightened first: find the age of the ``limit``-th oldest candidate
        in the schema, and hand *that* to every queue instead of the caller's
        cutoff. Each queue then deletes only rows older than everything the
        budget cannot reach, in any visiting order, and the invariant holds by
        construction rather than by luck.

        That bound is also what keeps the cost proportional to ``limit``. The
        statement it replaced stopped after ``limit`` index entries; a fan-out
        that asks "which queues have anything at all?" against the caller's
        cutoff reads the entire retention backlog, which for a nightly cron
        against a multi-million-row table is the whole table, every pass, to
        delete ten thousand rows.

        The arguments are checked here as well as inside the routine. The
        routine is still the boundary -- it is what a hand-written call has to
        get past -- but a fan-out that happens to match no queue would never
        reach it, and an invalid cutoff or a live state would come back as a
        quiet ``0`` instead of an error.

        The queue goes through :func:`~rqueue.limits.validate_name` rather than
        :func:`~rqueue.limits.validate_queue_target`, so ``'*'`` is a name that
        does not exist rather than the wildcard pause and resume accept. "Every
        queue" is spelled ``queue=None`` here, and spelling it ``'*'`` has to
        fail as the typed error every other bad name gives -- the routine's own
        refusal arrives as a raw ``asyncpg`` exception, which is a traceback out
        of ``rqueue purge --queue '*'`` rather than a message.
        """
        chosen = validate_purge_limit(limit)
        wanted = _validate_purge_states(states)
        older_than = await _validate_purge_cutoff(connection, older_than)
        if queue is not None:
            named = validate_name(
                queue, kind="queue name", max_length=MAX_QUEUE_NAME_LENGTH
            )
            return await self._purge_one(
                connection,
                queue=named,
                older_than=older_than,
                states=wanted,
                limit=chosen,
            )

        horizon = await self._purge_horizon(
            connection, states=wanted, older_than=older_than, limit=chosen
        )
        removed = 0
        queues = await connection.fetch(self._sql.purge_queues, wanted, horizon, chosen)
        for row in queues:
            if removed >= chosen:
                break
            removed += await self._purge_one(
                connection,
                queue=row["queue"],
                older_than=horizon,
                states=wanted,
                limit=chosen - removed,
            )
        return removed

    async def _purge_horizon(
        self,
        connection: asyncpg.Connection,
        *,
        states: list[str],
        older_than: datetime,
        limit: int,
    ) -> datetime:
        """The cutoff that bounds a whole-schema purge to its oldest rows.

        Everything at or before the ``limit``-th oldest candidate is, by
        definition, among the ``limit`` oldest -- so purging each queue at that
        cutoff spends the budget on those rows and no others, whatever order
        the queues are visited in. One row further and the cutoff would admit a
        row outside the budget, which the fan-out could then delete in place of
        an older one on a queue it has not reached yet.

        *At or before*, inclusive, which is the whole subtlety. Jobs finished
        in one transaction share a ``finished_at``, so a tie group routinely
        straddles the boundary: excluding it drops rows that are inside the
        budget and just as old as rows being deleted, and a purge sized to keep
        up quietly stops keeping up. The statement therefore returns the
        boundary instant plus one microsecond, which the routine's strict
        ``<`` turns into "every row of that age, and nothing newer". Rows of
        identical age have no oldest-first order between them, so which of them
        the budget reaches does not matter -- only how many.

        With no boundary row there are fewer candidates than the budget and all
        of them are eligible, so the caller's own cutoff is returned. The queue
        scan that follows carries the budget as its own limit either way, so it
        stays bounded here and, more to the point, stays bounded when the tie
        group this cutoff admits is larger than the budget by any margin.
        """
        boundary: datetime | None = await connection.fetchval(
            self._sql.purge_horizon, states, older_than, limit
        )
        return older_than if boundary is None else boundary

    async def _purge_one(
        self,
        connection: asyncpg.Connection,
        *,
        queue: str,
        older_than: datetime,
        states: list[str],
        limit: int,
    ) -> int:
        """One queue's bounded delete, with the routine's refusals typed.

        The routine authorizes its caller, and a role that is neither the
        schema owner, a superuser, nor granted the queue is refused. That is a
        configuration answer -- add the grant, or run as an identity that has
        it -- so it reaches the caller as one, instead of as a driver error the
        CLI has no branch for and prints as a traceback.
        """
        try:
            return int(
                await connection.fetchval(
                    self._sql.purge, queue, states, older_than, limit
                )
                or 0
            )
        except asyncpg.InsufficientPrivilegeError as exc:
            raise ConfigurationError(str(exc)) from exc

    # -------------------------------------------------------------- schedules

    async def upsert_schedule(
        self,
        connection: asyncpg.Connection,
        *,
        name: str,
        queue: str,
        task: str,
        payload_json: str,
        cron: str,
        timezone: str,
        enabled: bool,
        priority: int,
        max_attempts: int,
        concurrency_key: str | None,
    ) -> Schedule:
        row = await connection.fetchrow(
            self._sql.upsert_schedule,
            name,
            queue,
            task,
            payload_json,
            cron,
            timezone,
            enabled,
            priority,
            max_attempts,
            concurrency_key,
        )
        assert row is not None  # noqa: S101 - upsert always returns a row
        return Schedule.from_row(row)

    async def list_schedules(
        self, connection: asyncpg.Connection, *, enabled_only: bool = True
    ) -> list[Schedule]:
        rows = await connection.fetch(self._sql.list_schedules, enabled_only)
        return [Schedule.from_row(row) for row in rows]

    async def get_schedule(self, connection: asyncpg.Connection, name: str) -> Schedule:
        row = await connection.fetchrow(self._sql.get_schedule, name)
        if row is None:
            raise ScheduleNotFound(f"no schedule named {name!r}")
        return Schedule.from_row(row)

    async def delete_schedule(self, connection: asyncpg.Connection, name: str) -> bool:
        return bool(await connection.fetchval(self._sql.delete_schedule, name))

    async def last_occurrence(
        self, connection: asyncpg.Connection, schedule_id: uuid.UUID
    ) -> datetime | None:
        value: datetime | None = await connection.fetchval(
            self._sql.last_occurrence, schedule_id
        )
        return value

    async def fire_occurrence(
        self,
        connection: asyncpg.Connection,
        *,
        schedule_id: uuid.UUID,
        occurrence_at: datetime,
        spec: JobInsert,
        fired_by: str | None,
    ) -> Job | None:
        """Record one occurrence and the job it produces, atomically.

        The job row and the occurrence row are written in a single transaction
        (§6) -- a savepoint, when the caller already has one open. If the
        occurrence key is already taken, the unique constraint rejects the
        insert, this scheduler's job is rolled back with it, and the caller
        learns it lost the race. If the process dies part way through, neither
        row survives and the next tick retries the same occurrence -- which is
        exactly why this design needs no leader election and no schedule-row
        reclaim.

        A plain INSERT rather than ``ON CONFLICT DO NOTHING``: the plain form
        waits on a competing uncommitted occurrence and then proceeds if that
        transaction aborted, where DO NOTHING would give up on an occurrence
        nobody ended up firing.
        """
        try:
            async with connection.transaction():
                job, _ = await self.insert_job(connection, spec)
                await connection.execute(
                    self._sql.insert_occurrence,
                    schedule_id,
                    occurrence_at,
                    spec.queue,
                    spec.id,
                    fired_by,
                )
                return job
        except asyncpg.UniqueViolationError:
            return None

    async def occurrence_count(
        self, connection: asyncpg.Connection, schedule_id: uuid.UUID
    ) -> int:
        return int(
            await connection.fetchval(self._sql.occurrence_count, schedule_id) or 0
        )

    # ------------------------------------------------------------- heartbeats

    async def record_runtime_heartbeat(
        self,
        connection: asyncpg.Connection,
        *,
        kind: str,
        instance: str,
        queue: str,
        metadata_json: str = "{}",
    ) -> None:
        """Record that ``instance`` is alive on ``queue``.

        The queue is required, not merely recorded: it is part of the row's
        key, because one instance id can serve different queues in different
        processes and each is its own liveness fact.
        """
        await connection.execute(
            self._sql.record_runtime_heartbeat, kind, instance, queue, metadata_json
        )

    async def clear_runtime_heartbeats(
        self,
        connection: asyncpg.Connection,
        *,
        kind: str,
        instance: str,
        keep: Sequence[str],
    ) -> int:
        """Drop this instance's heartbeats for every queue outside ``keep``.

        Liveness is a claim about the present, and the staleness window is
        there for a process that *died* -- it cannot retract anything, so time
        has to. A process that is still running and has merely stopped serving
        a queue can retract it, and until it does, a readiness probe for that
        queue keeps answering "yes, and this is the instance feeding it" for
        the width of that window.
        """
        status = await connection.execute(
            self._sql.clear_runtime_heartbeats, kind, instance, list(keep)
        )
        return int(status.rsplit(" ", 1)[-1])

    async def stale_heartbeat_queues(
        self,
        connection: asyncpg.Connection,
        *,
        kind: str,
        instance: str,
        keep: Sequence[str],
    ) -> list[str]:
        """The queues :meth:`clear_runtime_heartbeats` would retract.

        Asking first is what makes the retraction cost a privilege only when
        there is something to retract. A component that never changes the set
        of queues it serves -- the overwhelmingly common case, every tick of
        every deployment -- then issues no ``DELETE`` at all.
        """
        rows = await connection.fetch(
            self._sql.stale_heartbeat_queues, kind, instance, list(keep)
        )
        return [row["queue"] for row in rows]

    async def live_instances(
        self,
        connection: asyncpg.Connection,
        *,
        kind: str,
        since: datetime,
        queue: str | None = None,
    ) -> list[str]:
        rows = await connection.fetch(self._sql.live_instances, kind, since, queue)
        return [row["instance"] for row in rows]

    # ------------------------------------------------------------ queue pause

    async def pause_queue(
        self, connection: asyncpg.Connection, *, queue: str
    ) -> QueuePause:
        """Pause ``queue``, or every queue when given the ``'*'`` wildcard.

        Idempotent: a second pause leaves the first one's timestamp alone.
        """
        row = await connection.fetchrow(self._sql.pause_queue, queue)
        assert row is not None  # noqa: S101 - the upsert always returns its row
        return QueuePause.from_row(row)

    async def resume_queue(
        self, connection: asyncpg.Connection, *, queue: str
    ) -> list[str]:
        """Resume ``queue``, or every paused queue for ``'*'``.

        Returns the queues that were actually paused before this call, so a
        caller can tell a real resume from a no-op.
        """
        rows = await connection.fetch(self._sql.resume_queue, queue)
        return [row["queue"] for row in rows]

    async def paused_queues(self, connection: asyncpg.Connection) -> list[QueuePause]:
        rows = await connection.fetch(self._sql.queue_pauses)
        return [QueuePause.from_row(row) for row in rows]

    async def is_queue_paused(
        self, connection: asyncpg.Connection, *, queue: str
    ) -> bool:
        """Whether ``queue`` is paused, by its own row or by the wildcard."""
        return bool(await connection.fetchval(self._sql.is_queue_paused, queue))


def _validate_purge_states(states: Sequence[str]) -> list[str]:
    """Terminal states only, and at least one.

    ``purge_terminal_jobs`` refuses anything else, but a fan-out over zero
    queues never calls it, so the same rule is applied before the first
    statement rather than only inside the last one.
    """
    wanted = [str(state) for state in states]
    if not wanted:
        raise ValidationError("purge needs at least one terminal state")
    live = sorted(set(wanted) - set(TERMINAL_STATES))
    if live:
        raise ValidationError(
            f"purge may only delete terminal jobs; {', '.join(live)} "
            f"{'is' if len(live) == 1 else 'are'} not terminal"
        )
    return wanted


async def _validate_purge_cutoff(
    connection: asyncpg.Connection, older_than: datetime
) -> datetime:
    """Reject a cutoff the routine would reject, using the routine's clock.

    ``purge_terminal_jobs`` compares against PostgreSQL's ``now()``. Checking
    here against the client's clock instead means the two disagree by exactly
    the skew between them, and a caller inside that window -- ``retention`` of
    zero or near it is the realistic way in -- passes this check and then takes
    a raw ``PostgresError`` from the routine, which is the failure this
    validator exists to prevent.

    ``now()`` is the transaction timestamp, so inside a transaction this reads
    the same instant the routine will. Outside one it reads slightly earlier,
    which can only make this check stricter than the one it stands in for.

    A naive cutoff is refused rather than assumed to be UTC, which is the
    convention :func:`~rqueue.limits.validate_scheduled_at` already sets for
    every caller-supplied instant. Guessing the offset would silently move the
    cutoff by the caller's own offset and delete rows they did not mean to
    delete; without the check it is a raw ``TypeError`` from comparing it
    against an aware ``now()``.
    """
    if older_than.tzinfo is None or older_than.utcoffset() is None:
        raise ValidationError("purge needs a timezone-aware cutoff")
    server_now: datetime = await connection.fetchval("SELECT now()")
    if older_than > server_now:
        raise ValidationError(
            f"purge needs a cutoff in the past (got {older_than.isoformat()}, "
            f"database clock reads {server_now.isoformat()})"
        )
    return older_than


class _Statements:
    """Every statement rqueue issues, built once per schema."""

    def __init__(self, schema: str) -> None:
        jobs = f"{schema}.jobs"
        attempts = f"{schema}.job_attempts"
        slots = f"{schema}.concurrency_slots"
        schedules = f"{schema}.schedules"
        occurrences = f"{schema}.schedule_occurrences"
        beats = f"{schema}.runtime_heartbeats"
        pauses = f"{schema}.queue_pauses"
        returning = _columns(_JOB_FIELDS)
        job_cols = _columns(_JOB_FIELDS, "j")
        schedule_cols = _columns(_SCHEDULE_FIELDS)
        active = ", ".join(f"'{state}'" for state in ACTIVE_STATES)
        terminal = ", ".join(f"'{state}'" for state in TERMINAL_STATES)

        self.insert_job = f"""
            INSERT INTO {jobs} (
                id, queue, task, payload, state, priority, attempt, max_attempts,
                scheduled_at, dedupe_key, concurrency_key, timeout_seconds,
                retry_policy, metadata
            )
            VALUES (
                $1, $2, $3, $4::text::jsonb, 'pending', $5, 0, $6,
                COALESCE($7::timestamptz, now()), $8, $9, $10,
                $11::text::jsonb, $12::text::jsonb
            )
            ON CONFLICT (queue, dedupe_key)
                WHERE dedupe_key IS NOT NULL AND state IN ({active})
            DO UPDATE SET updated_at = {jobs}.updated_at
            RETURNING {returning}
        """

        # The pause check is a queue-wide gate, not a per-row one: it names no
        # column of the jobs table, so PostgreSQL hoists it into an InitPlan
        # evaluated once per claim and hangs a one-time filter off the index
        # scan. A paused queue therefore returns zero rows with the claim index
        # never executed at all; an unpaused one pays one read of a table that
        # holds a row per queue ever paused. Enforcing it here rather than in
        # Worker is what makes the pause hold for a worker that has not yet
        # heard about it (0003_queue_pause.sql), which is stronger than either
        # Oban's or River's in-process check.
        self.claim_candidates = f"""
            SELECT id, concurrency_key, retry_policy::text AS retry_policy
            FROM {jobs}
            WHERE queue = $1
              AND state = 'pending'
              AND scheduled_at <= now()
              AND NOT cancel_requested
              AND attempt < max_attempts
              AND task = ANY($2::text[])
              AND NOT EXISTS (
                  SELECT 1 FROM {pauses} AS p
                  WHERE p.queue IN ($1, '*') AND p.paused_at IS NOT NULL
              )
            ORDER BY priority DESC, scheduled_at, seq
            FOR UPDATE SKIP LOCKED
            LIMIT $3
        """

        self.quarantine_pending_policy = f"""
            UPDATE {jobs}
            SET retry_policy = NULL,
                error_type = 'ConfigurationError',
                error_message = $2,
                updated_at = now()
            WHERE id = $1 AND state = 'pending'
        """

        # The slot key is scoped to its queue (0005_queue_scoped_runtime.sql),
        # the same scoping dedupe_key has always had, so the queue is part of
        # both the inserted row and the conflict arbiter.
        self.acquire_slot = f"""
            INSERT INTO {slots} (
                queue, key, job_id, lease_token, worker_id, leased_until
            )
            VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
            ON CONFLICT (queue, key) DO UPDATE
            SET job_id = EXCLUDED.job_id,
                lease_token = EXCLUDED.lease_token,
                worker_id = EXCLUDED.worker_id,
                acquired_at = now(),
                leased_until = EXCLUDED.leased_until,
                -- Taking over an expired slot takes over the ownership with
                -- it (0010_slot_ownership.sql). Left alone, the row would
                -- still name the holder this one displaced, and the new
                -- holder could not release the slot it is holding.
                role_name = current_user
            WHERE concurrency_slots.leased_until <= now()
            RETURNING key
        """

        self.lease_jobs = f"""
            UPDATE {jobs} AS j
            SET state = 'leased',
                attempt = j.attempt + 1,
                worker_id = $3,
                lease_token = v.token,
                leased_until = now() + make_interval(secs => $4),
                heartbeat_at = now(),
                started_at = COALESCE(j.started_at, now()),
                updated_at = now()
            FROM unnest($1::uuid[], $2::uuid[]) AS v(id, token)
            WHERE j.id = v.id AND j.state = 'pending'
            RETURNING {job_cols}
        """

        self.open_attempts = f"""
            INSERT INTO {attempts} (
                job_id, queue, task, attempt, worker_id, lease_token, started_at
            )
            SELECT j.id, j.queue, j.task, j.attempt, j.worker_id, j.lease_token, now()
            FROM {jobs} AS j
            WHERE j.id = ANY($1::uuid[])
        """

        self.heartbeat = f"""
            UPDATE {jobs}
            SET heartbeat_at = now(),
                leased_until = now() + make_interval(secs => $3),
                updated_at = now()
            WHERE id = $1 AND lease_token = $2 AND state = 'leased'
            RETURNING cancel_requested
        """

        self.extend_slot = f"""
            UPDATE {slots}
            SET leased_until = now() + make_interval(secs => $3)
            WHERE job_id = $1 AND lease_token = $2
        """

        finalize_head = f"UPDATE {jobs} AS j SET"
        finalize_tail = (
            "WHERE j.id = $1 AND j.lease_token = $2 AND j.state = 'leased' "
            f"RETURNING {job_cols}"
        )

        self.complete = f"""
            {finalize_head}
                state = 'succeeded',
                lease_token = NULL,
                leased_until = NULL,
                finished_at = now(),
                error_type = NULL,
                error_message = NULL,
                updated_at = now()
            {finalize_tail}
        """

        self.fail_terminal = f"""
            {finalize_head}
                state = 'failed',
                lease_token = NULL,
                leased_until = NULL,
                finished_at = now(),
                error_type = $3,
                error_message = $4,
                updated_at = now()
            {finalize_tail}
        """

        self.fail_invalid_policy = f"""
            {finalize_head}
                state = 'failed',
                retry_policy = NULL,
                lease_token = NULL,
                leased_until = NULL,
                finished_at = now(),
                error_type = $3,
                error_message = $4,
                updated_at = now()
            {finalize_tail}
        """

        self.cancel_leased = f"""
            {finalize_head}
                state = 'cancelled',
                lease_token = NULL,
                leased_until = NULL,
                finished_at = now(),
                error_type = $3,
                error_message = $4,
                updated_at = now()
            {finalize_tail}
        """

        self.reschedule = f"""
            {finalize_head}
                state = 'pending',
                lease_token = NULL,
                leased_until = NULL,
                worker_id = NULL,
                heartbeat_at = NULL,
                scheduled_at = $3,
                error_type = $4,
                error_message = $5,
                updated_at = now()
            {finalize_tail}
        """

        self.close_attempt = f"""
            UPDATE {attempts}
            SET finished_at = now(),
                outcome = $3,
                error_type = $4,
                error_message = $5
            WHERE job_id = $1 AND lease_token = $2 AND finished_at IS NULL
        """

        self.release_slot = f"""
            DELETE FROM {slots} WHERE job_id = $1 AND lease_token = $2
        """

        # One statement so recovery is atomic per job: the attempt record is
        # closed, the concurrency slot released, and the job row re-opened or
        # failed together. Data-modifying CTEs run exactly once each, against
        # the same snapshot, so `expired` names the same rows throughout.
        self.recover_expired = f"""
            WITH expired AS (
                SELECT id, attempt, max_attempts, cancel_requested, lease_token
                FROM {jobs}
                WHERE queue = $1 AND state = 'leased' AND leased_until <= now()
                ORDER BY leased_until
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            ),
            closed AS (
                UPDATE {attempts} AS a
                SET finished_at = now(),
                    outcome = 'lease_expired',
                    error_type = 'LeaseExpired',
                    error_message = 'lease expired before the attempt finished'
                FROM expired AS e
                WHERE a.job_id = e.id
                  AND a.lease_token = e.lease_token
                  AND a.finished_at IS NULL
                RETURNING a.job_id
            ),
            released AS (
                DELETE FROM {slots} AS s
                USING expired AS e
                WHERE s.job_id = e.id AND s.lease_token = e.lease_token
                RETURNING s.job_id
            )
            UPDATE {jobs} AS j
            SET state = CASE
                    WHEN e.cancel_requested THEN 'cancelled'
                    WHEN e.attempt >= e.max_attempts THEN 'failed'
                    ELSE 'pending'
                END,
                lease_token = NULL,
                leased_until = NULL,
                worker_id = NULL,
                heartbeat_at = NULL,
                finished_at = CASE
                    WHEN e.cancel_requested OR e.attempt >= e.max_attempts THEN now()
                    ELSE NULL
                END,
                error_type = CASE
                    WHEN e.cancel_requested THEN 'Cancelled'
                    WHEN e.attempt >= e.max_attempts THEN 'LeaseExpired'
                    ELSE j.error_type
                END,
                error_message = CASE
                    WHEN e.cancel_requested
                        THEN 'cancelled while leased; lease expired unfinalized'
                    WHEN e.attempt >= e.max_attempts
                        THEN 'lease expired before the attempt finished'
                    ELSE j.error_message
                END,
                updated_at = now()
            FROM expired AS e
            WHERE j.id = e.id
            RETURNING j.id
        """

        self.get_job = f"SELECT {returning} FROM {jobs} WHERE id = $1"

        self.list_jobs = f"""
            SELECT {job_cols}
            FROM {jobs} AS j
            WHERE ($1::text IS NULL OR j.queue = $1)
              AND ($2::text[] IS NULL OR j.state = ANY($2))
              AND ($3::text IS NULL OR j.task = $3)
            ORDER BY j.seq DESC
            LIMIT $4 OFFSET $5
        """

        self.list_attempts = f"""
            SELECT id, job_id, attempt, worker_id, lease_token, started_at,
                   finished_at, outcome, error_type, error_message
            FROM {attempts}
            WHERE job_id = $1
            ORDER BY attempt
        """

        self.stats = f"""
            SELECT
                count(*) FILTER (WHERE state = 'pending')::int   AS pending,
                count(*) FILTER (WHERE state = 'leased')::int    AS leased,
                count(*) FILTER (WHERE state = 'succeeded')::int AS succeeded,
                count(*) FILTER (WHERE state = 'failed')::int    AS failed,
                count(*) FILTER (WHERE state = 'cancelled')::int AS cancelled,
                count(*) FILTER (
                    WHERE state = 'pending' AND scheduled_at <= now()
                )::int AS ready,
                EXTRACT(EPOCH FROM (now() - min(scheduled_at) FILTER (
                    WHERE state = 'pending' AND scheduled_at <= now()
                )))::float8 AS oldest_ready_age_seconds,
                count(*) FILTER (
                    WHERE state = 'leased' AND leased_until <= now()
                )::int AS expired_leases
            FROM {jobs}
            WHERE queue = $1
        """

        self.distinct_active_tasks = f"""
            SELECT DISTINCT task FROM {jobs}
            WHERE queue = $1 AND state IN ({active})
        """

        self.cancel = f"""
            UPDATE {jobs} AS j
            SET state = CASE WHEN j.state = 'pending' THEN 'cancelled' ELSE j.state END,
                cancel_requested = true,
                finished_at = CASE
                    WHEN j.state = 'pending' THEN now() ELSE j.finished_at
                END,
                error_type = CASE
                    WHEN j.state = 'pending' THEN 'Cancelled' ELSE j.error_type
                END,
                error_message = CASE
                    WHEN j.state = 'pending' THEN 'cancelled before execution'
                    ELSE j.error_message
                END,
                updated_at = now()
            WHERE j.id = $1
            RETURNING {job_cols}
        """

        # The attempt counter is deliberately *not* reset: attempt records are
        # immutable and keyed by (job_id, attempt), so a reset would collide
        # with the history it is supposed to preserve. An operator retry grants
        # a fresh budget by raising the ceiling instead.
        self.retry_terminal = f"""
            UPDATE {jobs} AS j
            SET state = 'pending',
                max_attempts = LEAST(
                    1000, GREATEST(j.max_attempts, j.attempt + $3::int)
                ),
                retry_policy = CASE
                    WHEN j.retry_policy IS NULL THEN NULL
                    ELSE jsonb_set(
                        j.retry_policy,
                        ARRAY['max_attempts'],
                        to_jsonb(
                            LEAST(
                                1000, GREATEST(j.max_attempts, j.attempt + $3::int)
                            )
                        ),
                        false
                    )
                END,
                scheduled_at = COALESCE($2::timestamptz, now()),
                started_at = NULL,
                finished_at = NULL,
                worker_id = NULL,
                lease_token = NULL,
                leased_until = NULL,
                heartbeat_at = NULL,
                cancel_requested = false,
                error_type = NULL,
                error_message = NULL,
                updated_at = now()
            WHERE j.id = $1 AND j.state IN ({terminal})
            RETURNING {job_cols}
        """

        # Retention deletes through the SECURITY DEFINER routine from
        # migration 0006, never through a DELETE here. The routine is the
        # safety boundary -- it re-checks queue, terminal state, cutoff, and
        # batch size -- so this statement carries no predicates of its own to
        # get wrong, and a PURGE-only role can run it with no DELETE grant.
        self.purge = f"""
            SELECT {schema}.purge_terminal_jobs($1, $2::text[], $3, $4)
        """

        # The cutoff a whole-schema purge hands every queue: one tick past the
        # age of the budget's last row -- `OFFSET $3 - 1` is that row, since
        # the offset is zero-based. `Storage._purge_horizon` explains why it is
        # inclusive; the arithmetic is here because the tick is a property of
        # the column, not of the caller -- timestamptz resolves to the
        # microsecond, so `+ 1us` is the smallest value that admits every row
        # sharing that instant and no row after it. The routine compares with a
        # strict `<`, and that comparison is fixed: migration 0006 is
        # checksummed and forward-only.
        #
        # A LIMIT-ed index scan on jobs_retention_idx, so the work is
        # proportional to the budget rather than to the backlog behind it --
        # the property the single ordered DELETE had, and the reason this is
        # not simply `SELECT min(finished_at) ... GROUP BY queue`.
        self.purge_horizon = f"""
            SELECT finished_at + interval '1 microsecond'
            FROM {jobs}
            WHERE state = ANY($1::text[])
              AND finished_at IS NOT NULL
              AND finished_at < $2
            ORDER BY finished_at
            OFFSET $3 - 1 LIMIT 1
        """

        # Which queues hold rows inside that horizon, oldest queue first.
        #
        # The grouping runs over a LIMIT-ed scan rather than over the horizon
        # alone, because the horizon is inclusive of the tie group that
        # straddles it and a tie group has no size bound -- one transaction
        # finishing a hundred thousand jobs gives them all one `finished_at`,
        # and `finished_at < $2` then matches every one of them. Grouping the
        # whole tie group to learn which queues it touches is the full-backlog
        # read the horizon exists to prevent.
        #
        # Reading only the `limit` oldest rows finds every queue the budget can
        # reach and no others: those rows are themselves under the horizon, so
        # the queues holding them hold at least `limit` deletable rows between
        # them, and a queue absent from them has nothing the budget could get
        # to before something older. The tie group stays fully eligible -- that
        # is the horizon's job, and `purge_terminal_jobs` still deletes from it
        # up to the budget; this statement only decides where to look.
        self.purge_queues = f"""
            SELECT queue FROM (
                SELECT queue, finished_at FROM {jobs}
                WHERE state = ANY($1::text[])
                  AND finished_at IS NOT NULL
                  AND finished_at < $2
                ORDER BY finished_at
                LIMIT $3
            ) AS budget
            GROUP BY queue
            ORDER BY min(finished_at)
        """

        self.upsert_schedule = f"""
            INSERT INTO {schedules} (
                name, queue, task, payload, cron, timezone, enabled, priority,
                max_attempts, concurrency_key
            )
            VALUES ($1, $2, $3, $4::text::jsonb, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (name) DO UPDATE
            SET queue = EXCLUDED.queue,
                task = EXCLUDED.task,
                payload = EXCLUDED.payload,
                cron = EXCLUDED.cron,
                timezone = EXCLUDED.timezone,
                enabled = EXCLUDED.enabled,
                priority = EXCLUDED.priority,
                max_attempts = EXCLUDED.max_attempts,
                concurrency_key = EXCLUDED.concurrency_key,
                updated_at = now()
            RETURNING {schedule_cols}
        """

        self.list_schedules = f"""
            SELECT {schedule_cols} FROM {schedules}
            WHERE NOT $1::boolean OR enabled
            ORDER BY name
        """

        self.get_schedule = f"SELECT {schedule_cols} FROM {schedules} WHERE name = $1"

        self.delete_schedule = f"""
            WITH removed AS (
                DELETE FROM {schedules} WHERE name = $1 RETURNING 1
            )
            SELECT count(*)::int > 0 FROM removed
        """

        self.last_occurrence = f"""
            SELECT max(occurrence_at) FROM {occurrences} WHERE schedule_id = $1
        """

        self.insert_occurrence = f"""
            INSERT INTO {occurrences} (
                schedule_id, occurrence_at, queue, job_id, fired_by
            )
            VALUES ($1, $2, $3, $4, $5)
        """

        self.occurrence_count = f"""
            SELECT count(*)::int FROM {occurrences} WHERE schedule_id = $1
        """

        # The queue is part of the key (0005_queue_scoped_runtime.sql), not a
        # payload column: a heartbeat is one component's liveness on one queue,
        # and an instance id -- a pod name, a hostname, a container ordinal --
        # says nothing about which. With (kind, instance) alone, a queue-scoped
        # role's upsert would conflict with a row its own policy hides.
        # `role_name` is never named here: it defaults to `current_user`
        # (0008_heartbeat_ownership.sql), which is the point -- a liveness claim
        # records the identity its writer authenticated as, not one it chose.
        # The arbiter has to match the unique index, so it names the column even
        # though the statement does not supply it.
        self.record_runtime_heartbeat = f"""
            INSERT INTO {beats} (kind, instance, queue, updated_at, metadata)
            VALUES ($1, $2, $3, now(), $4::text::jsonb)
            ON CONFLICT (kind, instance, queue, role_name) DO UPDATE
            SET updated_at = now(),
                metadata = EXCLUDED.metadata
        """

        # The other half of the upsert above: a component's heartbeats are the
        # queues it is serving *now*, so the ones it has stopped serving have
        # to go, not merely age out. Scoped to one instance, because a row for
        # another instance of the same kind is another process's liveness and
        # not this one's to retract. An empty `keep` deletes every row this
        # instance holds -- `queue <> ALL('{}')` is true of all of them --
        # which is exactly right for a component that is now serving nothing.
        self.clear_runtime_heartbeats = f"""
            DELETE FROM {beats}
            WHERE kind = $1 AND instance = $2 AND role_name = current_user
              AND queue <> ALL($3::text[])
        """

        # Exactly the rows the DELETE above would remove, so a caller can find
        # out whether it has anything to retract before asking for the
        # privilege to retract it. Needs SELECT and nothing more, which every
        # role that writes a heartbeat already holds -- which is what keeps a
        # role provisioned before DELETE joined the grant set from failing on a
        # tick with nothing to do. The `role_name` predicate is repeated rather
        # than left to the policy so the two statements have the same reach for
        # the schema owner, who bypasses it.
        self.stale_heartbeat_queues = f"""
            SELECT queue FROM {beats}
            WHERE kind = $1 AND instance = $2 AND role_name = current_user
              AND queue <> ALL($3::text[])
            ORDER BY queue
        """

        # DISTINCT because the key is per queue (0005_queue_scoped_runtime.sql)
        # and one instance can be alive on several of them: an unfiltered probe
        # asks "which schedulers are alive?", and answering with the same id
        # once per queue it serves would make a readiness report count
        # deployments that do not exist.
        self.live_instances = f"""
            SELECT DISTINCT instance FROM {beats}
            WHERE kind = $1 AND updated_at >= $2
              AND ($3::text IS NULL OR queue = $3)
            ORDER BY instance
        """

        # Pausing an already-paused queue keeps the original paused_at and
        # updated_at, so "paused since" survives a repeated call -- River's
        # CASE-guarded UPDATE, expressed as an upsert because rqueue has no
        # queue registry to UPDATE against.
        self.pause_queue = f"""
            INSERT INTO {pauses} (queue, paused_at, updated_at)
            VALUES ($1, now(), now())
            ON CONFLICT (queue) DO UPDATE
            SET paused_at = COALESCE(queue_pauses.paused_at, EXCLUDED.paused_at),
                updated_at = CASE
                    WHEN queue_pauses.paused_at IS NULL THEN EXCLUDED.updated_at
                    ELSE queue_pauses.updated_at
                END
            RETURNING queue, paused_at, updated_at
        """

        # '*' resumes everything, including any individually paused queue --
        # "resume all" that left a queue paused because someone had paused it
        # by name would be a trap. Resuming by name clears only that queue's
        # row, so it cannot punch a hole in a global pause.
        self.resume_queue = f"""
            UPDATE {pauses}
            SET paused_at = NULL, updated_at = now()
            WHERE paused_at IS NOT NULL
              AND CASE WHEN $1 = '*' THEN true ELSE queue = $1 END
            RETURNING queue
        """

        self.queue_pauses = f"""
            SELECT queue, paused_at, updated_at FROM {pauses}
            WHERE paused_at IS NOT NULL
            ORDER BY queue
        """

        self.is_queue_paused = f"""
            SELECT EXISTS (
                SELECT 1 FROM {pauses}
                WHERE queue IN ($1, '*') AND paused_at IS NOT NULL
            )
        """
