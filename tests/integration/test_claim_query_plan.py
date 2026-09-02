"""Keep the claim query index-backed with a long-lived queue backlog.

The second test guards the other half of the hot path: the queue-pause gate
added in 0003 must stay a once-per-claim check, not a per-candidate-row one.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from rqueue import Admin, Queue

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
