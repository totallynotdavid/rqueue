"""Keep the claim query index-backed with a long-lived queue backlog.

The second test guards the other half of the hot path: the queue-pause gate
added in 0003 must stay a once-per-claim check, not a per-candidate-row one.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from rqueue import Admin, Queue
from rqueue.models import TERMINAL_STATES
from rqueue.storage import Storage

JOBS_TABLE = "task_queue.jobs"
JOBS_RELATION = "jobs"
PAUSE_RELATION = "queue_pauses"
CLAIM_INDEX = "jobs_claim_idx"
TERMINAL_ROWS = 30_000
PENDING_ROWS = 300
BATCH_SIZE = 10


def _flatten(plan: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [plan]
    for child in plan.get("Plans", []):
        nodes.extend(_flatten(child))
    return nodes


async def _analyze_claim_plan(
    connection: asyncpg.Connection, queue: Queue, queue_name: str
) -> list[dict[str, Any]]:
    """Analyze claiming in a transaction that is rolled back afterwards."""
    sql = queue.storage._sql.claim_candidates
    await connection.execute("BEGIN")
    try:
        rows = await connection.fetch(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql,
            queue_name,
            ["plan-task"],
            BATCH_SIZE,
        )
    finally:
        await connection.execute("ROLLBACK")

    raw = rows[0]["QUERY PLAN"]
    document = json.loads(raw) if isinstance(raw, str) else raw
    return _flatten(document[0]["Plan"])


async def _seed(
    connection: asyncpg.Connection, queue_name: str, *, terminal: int, pending: int
) -> None:
    if terminal:
        await connection.execute(
            f"""
            INSERT INTO {JOBS_TABLE}
                (queue, task, payload, state, max_attempts, attempt, finished_at)
            SELECT $1, 'historical-task', '{{}}'::jsonb,
                   CASE WHEN g % 2 = 0 THEN 'succeeded' ELSE 'failed' END,
                   3, 1, now()
            FROM generate_series(1, $2) AS g
            """,
            queue_name,
            terminal,
        )
    await connection.execute(
        f"""
        INSERT INTO {JOBS_TABLE} (queue, task, payload, state, max_attempts)
        SELECT $1, 'plan-task', '{{}}'::jsonb, 'pending', 3
        FROM generate_series(1, $2)
        """,
        queue_name,
        pending,
    )
    await connection.execute(f"ANALYZE {JOBS_TABLE}")


async def test_claim_plan_uses_index_with_terminal_backlog(
    queue: Queue, pool: asyncpg.Pool, queue_name: str
) -> None:
    """Claiming should inspect the batch, not the table's historical backlog."""
    async with pool.acquire() as connection:
        await _seed(
            connection, queue_name, terminal=TERMINAL_ROWS, pending=PENDING_ROWS
        )

        nodes = await _analyze_claim_plan(connection, queue, queue_name)

    job_scans = [node for node in nodes if node.get("Relation Name") == JOBS_RELATION]
    assert any(node.get("Index Name") == CLAIM_INDEX for node in job_scans), (
        f"claim plan no longer uses {CLAIM_INDEX}; "
        f"nodes={[(n.get('Node Type'), n.get('Index Name')) for n in job_scans]}"
    )
    assert not any(
        (node.get("Node Type") or "").endswith("Seq Scan") for node in job_scans
    ), f"claim plan fell back to a sequential scan: {job_scans}"

    scanned = sum(
        round((node.get("Actual Rows") or 0) * (node.get("Actual Loops") or 1))
        for node in job_scans
    )
    assert scanned <= BATCH_SIZE * 4, (
        f"claim plan scanned {scanned} rows for a batch of {BATCH_SIZE}; "
        f"nodes={job_scans}"
    )


async def test_the_pause_gate_is_checked_once_per_claim_not_once_per_row(
    queue: Queue, pool: asyncpg.Pool, queue_name: str, admin: Admin
) -> None:
    """The pause predicate names no column of `jobs`, and must stay that way.

    PostgreSQL therefore hoists it into an InitPlan and hangs a one-time filter
    off the claim's index scan: one execution per claim whatever the batch size,
    and when the queue is paused the claim index is not touched at all. A
    correlated formulation would cost one lookup per candidate row instead.
    """
    async with pool.acquire() as connection:
        await _seed(connection, queue_name, terminal=0, pending=PENDING_ROWS)

        running = await _analyze_claim_plan(connection, queue, queue_name)
        pause_nodes = [
            node for node in running if node.get("Relation Name") == PAUSE_RELATION
        ]
        assert pause_nodes, f"the claim plan no longer reads {PAUSE_RELATION}"
        assert all(node.get("Actual Loops") == 1 for node in pause_nodes), (
            f"the pause check ran per row rather than once: {pause_nodes}"
        )
        # That the gate does not displace jobs_claim_idx is the backlog test
        # above: it explains the very same statement, index and all.

        await admin.pause_queue(queue_name)
        try:
            paused = await _analyze_claim_plan(connection, queue, queue_name)
        finally:
            await admin.resume_queue(queue_name)

        job_scans = [
            node for node in paused if node.get("Relation Name") == JOBS_RELATION
        ]
        assert job_scans, "the paused plan stopped mentioning the jobs table"
        # "never executed" is reported as zero loops: a paused queue costs one
        # read of a one-page table and nothing else.
        assert all(not node.get("Actual Loops") for node in job_scans), (
            f"a paused queue still scanned jobs: {job_scans}"
        )


PURGE_BACKLOG = 30_000
PURGE_BUDGET = 500
# Either retention index is a correct answer for the whole-schema probes:
# one is ordered by age alone, the other by (queue, age), and which the
# planner prefers depends on statistics. The property under test is that
# an index bounds the read, not which of the two does.
RETENTION_INDEXES = {"jobs_retention_idx", "jobs_retention_queue_idx"}
INDEX_SCANS = {"Index Scan", "Index Only Scan", "Bitmap Index Scan"}


async def _analyze(
    connection: asyncpg.Connection, sql: str, *args: Any
) -> list[dict[str, Any]]:
    rows = await connection.fetch("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, *args)
    raw = rows[0]["QUERY PLAN"]
    document = json.loads(raw) if isinstance(raw, str) else raw
    return _flatten(document[0]["Plan"])


async def test_a_bounded_purge_reads_the_budget_not_the_backlog(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """Retention cost must follow the limit, not the rows behind it.

    A whole-schema purge asks two questions before deleting: how old is the
    budget's last row, and which queues hold rows that old. Both have to stop
    after roughly `limit` index entries -- which is what the single ordered
    DELETE they replaced did. Asked against the caller's own cutoff instead,
    the second one reads every terminal row in the schema: a nightly cron
    trimming ten thousand rows pays for the whole backlog, every pass.
    """
    from datetime import UTC, datetime, timedelta

    storage = Storage("task_queue")
    older_than = datetime.now(UTC) - timedelta(days=1)
    states = list(TERMINAL_STATES)

    async with pool.acquire() as connection:
        for suffix in ("a", "b", "c"):
            await connection.execute(
                f"""
                INSERT INTO {JOBS_TABLE}
                    (queue, task, payload, state, max_attempts, finished_at)
                SELECT $1, 'plan-task', '{{}}'::jsonb, 'succeeded', 3,
                       now() - interval '30 days' - (g * interval '1 second')
                FROM generate_series(1, $2) AS g
                """,
                f"{queue_name}{suffix}",
                PURGE_BACKLOG // 3,
            )
        # A tie group far larger than the budget, all of it newer than the
        # backlog above. This is the shape that used to fall back to the
        # caller's own cutoff and aggregate everything behind it.
        await connection.execute(
            f"""
            INSERT INTO {JOBS_TABLE}
                (queue, task, payload, state, max_attempts, finished_at)
            SELECT $1, 'plan-task', '{{}}'::jsonb, 'succeeded', 3,
                   now() - interval '2 days'
            FROM generate_series(1, $2)
            """,
            f"{queue_name}tied",
            PURGE_BUDGET * 4,
        )
        await connection.execute(f"ANALYZE {JOBS_TABLE}")
        try:
            horizon = await connection.fetchval(
                storage._sql.purge_horizon, states, older_than, PURGE_BUDGET
            )
            assert horizon is not None, "the backlog is larger than the budget"

            for sql, args in (
                (storage._sql.purge_horizon, (states, older_than, PURGE_BUDGET)),
                (storage._sql.purge_queues, (states, horizon, PURGE_BUDGET)),
            ):
                nodes = await _analyze(connection, sql, *args)
                assert not [
                    node for node in nodes if "Seq Scan" in node["Node Type"]
                ], [node["Node Type"] for node in nodes]
                # A bitmap plan splits the work between a Bitmap Index Scan and
                # a Bitmap Heap Scan, and only the former names its index, so
                # rows are counted from the index side.
                indexed = [node for node in nodes if node["Node Type"] in INDEX_SCANS]
                assert indexed, [node["Node Type"] for node in nodes]
                assert all(
                    node.get("Index Name") in RETENTION_INDEXES for node in indexed
                ), [node.get("Index Name") for node in indexed]
                read = sum(
                    node["Actual Rows"] * node["Actual Loops"] for node in indexed
                )
                # Slack for the boundary row; the point is that it is neither
                # the 30,000-row backlog nor the tie group behind it.
                assert read <= PURGE_BUDGET * 2, read
        finally:
            await connection.execute(
                f"DELETE FROM {JOBS_TABLE} WHERE queue LIKE $1", f"{queue_name}%"
            )


TIED_AT_THE_BOUNDARY = 20_000


async def test_a_tie_group_at_the_boundary_does_not_defeat_the_cost_bound(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """The horizon admits the whole tie group; the scan must not read it.

    The two are separate promises and they pull against each other. The horizon
    is deliberately inclusive of the rows sharing the budget's last instant --
    otherwise a purge sized to keep up drops rows that are inside the budget --
    and a tie group has no size bound, because one transaction finishing a
    hundred thousand jobs gives every one of them the same `finished_at`.

    So `finished_at < horizon` alone is not a bound on anything. Asking which
    queues hold rows under it, with nothing else to stop the scan, reads the
    entire tie group: a budget of ten against twenty thousand tied rows is a
    sequential scan of all twenty thousand, which is the whole-backlog read the
    horizon exists to prevent, arrived at by the horizon's own inclusiveness.

    The rows still have to be *eligible* -- the delete may take any of them --
    so the fix cannot be to narrow the horizon. Only the queue-discovery scan
    is bounded, and that is enough: the `limit` oldest rows name every queue
    the budget can reach.
    """
    from datetime import UTC, datetime, timedelta

    storage = Storage("task_queue")
    older_than = datetime.now(UTC) - timedelta(days=1)
    states = list(TERMINAL_STATES)
    budget = 10

    async with pool.acquire() as connection:
        # The oldest rows in the schema, all sharing one instant, spread over
        # four queues. The horizon therefore lands inside the tie group.
        await connection.execute(
            f"""
            INSERT INTO {JOBS_TABLE}
                (queue, task, payload, state, max_attempts, finished_at)
            SELECT $1 || (g % 4), 'plan-task', '{{}}'::jsonb, 'succeeded', 3,
                   now() - interval '90 days'
            FROM generate_series(1, $2) AS g
            """,
            f"{queue_name}tied",
            TIED_AT_THE_BOUNDARY,
        )
        await connection.execute(f"ANALYZE {JOBS_TABLE}")
        try:
            horizon = await connection.fetchval(
                storage._sql.purge_horizon, states, older_than, budget
            )
            assert horizon is not None

            nodes = await _analyze(
                connection, storage._sql.purge_queues, states, horizon, budget
            )
            assert not [node for node in nodes if "Seq Scan" in node["Node Type"]], [
                node["Node Type"] for node in nodes
            ]
            indexed = [node for node in nodes if node["Node Type"] in INDEX_SCANS]
            assert indexed, [node["Node Type"] for node in nodes]
            read = sum(node["Actual Rows"] * node["Actual Loops"] for node in indexed)
            assert read <= budget * 2, (
                f"the queue scan read {read} rows for a budget of {budget}; "
                f"the tie group holds {TIED_AT_THE_BOUNDARY}"
            )

            # And the budget is still delivered in full out of that tie group,
            # which is the property the inclusive horizon is there for.
            deleted = await storage.purge(
                connection, queue=None, older_than=older_than, limit=budget
            )
            assert deleted == budget, deleted
        finally:
            await connection.execute(
                f"DELETE FROM {JOBS_TABLE} WHERE queue LIKE $1", f"{queue_name}%"
            )


PURGE_QUEUES = 40
PURGE_PER_QUEUE = 50
RETENTION_QUEUE_INDEX = "jobs_retention_queue_idx"


async def test_a_fanned_out_purge_scans_only_what_it_deletes(
    pool: asyncpg.Pool, queue_name: str
) -> None:
    """Whole-schema retention costs its limit, not queues x limit.

    The routine picks candidates by queue *and* age. Served by an index on
    (queue, state, seq) it finds the queue's whole terminal history and then
    throws away everything outside the cutoff -- invisible on one queue, and
    the dominant cost across many, since a fan-out visits every queue holding
    rows below its horizon. Migration 0007 orders an index by (queue,
    finished_at) so each of those scans reads the rows it is about to delete
    and stops.
    """
    from datetime import UTC, datetime, timedelta

    storage = Storage("task_queue")
    older_than = datetime.now(UTC) - timedelta(days=1)
    states = list(TERMINAL_STATES)
    budget = PURGE_QUEUES * PURGE_PER_QUEUE // 2

    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {JOBS_TABLE}
                (queue, task, payload, state, max_attempts, finished_at)
            SELECT $1 || q, 'plan-task', '{{}}'::jsonb, 'succeeded', 3,
                   now() - interval '30 days' - (r * interval '1 second')
            FROM generate_series(1, $2) AS q, generate_series(1, $3) AS r
            """,
            queue_name,
            PURGE_QUEUES,
            PURGE_PER_QUEUE,
        )
        await connection.execute(f"ANALYZE {JOBS_TABLE}")
        try:
            horizon = await connection.fetchval(
                storage._sql.purge_horizon, states, older_than, budget
            )
            assert horizon is not None

            # The routine's candidate scan, as it runs for one queue of the
            # fan-out. Half this queue's rows are below the shared horizon.
            nodes = await _analyze(
                connection,
                f"""
                SELECT j.id FROM {JOBS_TABLE} AS j
                WHERE j.queue = $1 AND j.state = ANY($2::text[])
                  AND j.finished_at IS NOT NULL AND j.finished_at < $3
                ORDER BY j.finished_at LIMIT $4
                """,
                f"{queue_name}1",
                states,
                horizon,
                budget,
            )
            indexed = [node for node in nodes if node["Node Type"] in INDEX_SCANS]
            assert indexed, [node["Node Type"] for node in nodes]
            assert all(
                node.get("Index Name") == RETENTION_QUEUE_INDEX for node in indexed
            ), [node.get("Index Name") for node in indexed]
            read = sum(node["Actual Rows"] * node["Actual Loops"] for node in indexed)
            # Its own eligible rows, not its whole terminal history.
            assert read <= PURGE_PER_QUEUE // 2 + 1, read
        finally:
            await connection.execute(
                f"DELETE FROM {JOBS_TABLE} WHERE queue LIKE $1", f"{queue_name}%"
            )
