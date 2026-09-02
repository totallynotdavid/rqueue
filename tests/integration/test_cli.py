"""The ``rqueue`` command line, against a real database.

These are synchronous on purpose: ``main`` owns its event loop, exactly as it
does when an operator runs it.
"""

from __future__ import annotations

import json
import uuid

import asyncpg
import pytest

from rqueue.cli import main


def test_migrate_status_and_readiness_round_trip(
    admin_dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    schema = f"cli_{uuid.uuid4().hex[:10]}"
    try:
        assert main(["--database-url", admin_dsn, "--schema", schema, "migrate"]) == 0
        assert "applied 0001_core" in capsys.readouterr().out

        # Re-running is a no-op and still succeeds.
        assert main(["--database-url", admin_dsn, "--schema", schema, "migrate"]) == 0
        assert "already up to date" in capsys.readouterr().out

        assert main(["--database-url", admin_dsn, "--schema", schema, "status"]) == 0
        state = json.loads(capsys.readouterr().out)
        assert state["up_to_date"] is True
        assert state["pending"] == []
        assert state["current_version"] == state["latest_version"]

        # No worker has ever checked in, so readiness fails and says why.
        code = main(["--database-url", admin_dsn, "--schema", schema, "readiness"])
        report = json.loads(capsys.readouterr().out)
        assert code == 1
        assert report["connected"] is True
        assert report["migrations_up_to_date"] is True
        assert any("no worker heartbeat" in p for p in report["problems"])

        code = main(
            [
                "--database-url",
                admin_dsn,
                "--schema",
                schema,
                "readiness",
                "--no-require-worker",
            ]
        )
        assert code == 0
        assert json.loads(capsys.readouterr().out)["ready"] is True
    finally:
        _drop_schema(admin_dsn, schema)


def test_status_reports_pending_migrations_with_a_nonzero_exit(
    admin_dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    schema = f"cli_{uuid.uuid4().hex[:10]}"
    try:
        assert (
            main(
                [
                    "--database-url",
                    admin_dsn,
                    "--schema",
                    schema,
                    "migrate",
                    "--target",
                    "1",
                ]
            )
            == 0
        )
        capsys.readouterr()
        assert main(["--database-url", admin_dsn, "--schema", schema, "status"]) == 1
        state = json.loads(capsys.readouterr().out)
        assert state["up_to_date"] is False
        assert state["pending"] == ["0002_scheduling", "0003_queue_pause"]
    finally:
        _drop_schema(admin_dsn, schema)


def test_purge_removes_old_terminal_jobs(
    admin_dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    schema = f"cli_{uuid.uuid4().hex[:10]}"
    try:
        assert main(["--database-url", admin_dsn, "--schema", schema, "migrate"]) == 0
        capsys.readouterr()
        _execute(
            admin_dsn,
            f"""
            INSERT INTO {schema}.jobs
                (queue, task, payload, state, max_attempts, finished_at)
            VALUES ('q', 't', '{{}}'::jsonb, 'succeeded', 3, now() - interval '40 days')
            """,
        )
        assert (
            main(
                [
                    "--database-url",
                    admin_dsn,
                    "--schema",
                    schema,
                    "purge",
                    "--retention-days",
                    "30",
                ]
            )
            == 0
        )
        assert "purged 1 job(s)" in capsys.readouterr().out
    finally:
        _drop_schema(admin_dsn, schema)


def test_a_missing_database_url_is_a_usage_error(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RQUEUE_DATABASE_URL", raising=False)
    assert main(["status"]) == 2
    assert "--database-url" in capsys.readouterr().err


def _execute(dsn: str, statement: str) -> None:
    import asyncio

    async def run() -> None:
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(run())


def _drop_schema(dsn: str, schema: str) -> None:
    _execute(dsn, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
