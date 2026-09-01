"""The packaged migration files, checked without a database."""

from __future__ import annotations

import re

from rqueue import migrations
from rqueue.storage import Storage


def test_migrations_are_contiguous_and_ordered() -> None:
    loaded = migrations.load_migrations()
    assert [m.version for m in loaded] == list(range(1, len(loaded) + 1))
    assert len(loaded) >= 2, "an upgrade test needs a previous release to upgrade from"


def test_every_migration_is_transactional() -> None:
    # PostgreSQL runs DDL transactionally; nothing shipped today needs the
    # no-transaction escape hatch.
    assert all(m.transactional for m in migrations.load_migrations())


def test_checksums_are_stable_and_distinct() -> None:
    loaded = migrations.load_migrations()
    again = migrations.load_migrations()
    assert [m.checksum for m in loaded] == [m.checksum for m in again]
    assert len({m.checksum for m in loaded}) == len(loaded)


def test_schema_placeholder_is_the_only_interpolation() -> None:
    # Anything else in braces would be an accidental format field.
    pattern = re.compile(r"\{([^}]*)\}")
    for migration in migrations.load_migrations():
        for match in pattern.findall(migration.sql):
            assert match in {"schema", ""}, f"{migration.name} interpolates {{{match}}}"


def test_advisory_lock_key_is_stable_and_schema_specific() -> None:
    first = migrations._advisory_lock_key("task_queue")
    assert first == migrations._advisory_lock_key("task_queue")
    assert first != migrations._advisory_lock_key("other_queue")
    assert -(2**63) <= first < 2**63


def test_substituted_sql_names_the_configured_schema() -> None:
    sql = migrations.load_migrations()[0].sql.replace("{schema}", "jobs_alt")
    assert "jobs_alt.jobs" in sql
    assert "{schema}" not in sql
    assert Storage("jobs_alt").notify_channel == "rqueue_jobs_alt"
