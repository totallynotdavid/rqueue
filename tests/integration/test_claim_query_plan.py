"""Keep the claim query index-backed with a long-lived queue backlog."""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from rqueue import Queue

JOBS_TABLE = "task_queue.jobs"
JOBS_RELATION = "jobs"
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


async def test_claim_plan_uses_index_with_terminal_backlog(
    queue: Queue, pool: asyncpg.Pool, queue_name: str
) -> None:
    """Claiming should inspect the batch, not the table's historical backlog."""
    async with pool.acquire() as connection:
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
            TERMINAL_ROWS,
        )
        await connection.execute(
            f"""
            INSERT INTO {JOBS_TABLE} (queue, task, payload, state, max_attempts)
            SELECT $1, 'plan-task', '{{}}'::jsonb, 'pending', 3
            FROM generate_series(1, $2)
            """,
            queue_name,
            PENDING_ROWS,
        )
        await connection.execute(f"ANALYZE {JOBS_TABLE}")

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
