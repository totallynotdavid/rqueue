# Contributing to rqueue

For a change that is not small, open an issue first and describe what you want
to change.

[docs/architecture.md](../docs/architecture.md) is the map of the code.

## Set up

[mise](https://mise.jdx.dev) installs the toolchain that `mise.toml` pins:
Python, uv, and PostgreSQL. PostgreSQL runs as a project-local cluster in
`.data/postgres`. Docker is not needed.

```console
$ mise run install    # uv sync --group dev
$ mise run db:start   # initialize .data/postgres on first run, then start it
```

## Checks

```console
$ mise run test              # fast suite, no database
$ mise run test-integration  # integration suite on a disposable database
$ mise run test-pipeline     # the master pipeline test only
$ mise run lint              # ruff check, ruff format --check, mypy --strict
$ mise run fix               # ruff check --fix, ruff format
```

CI runs the same checks. The fast suite runs on Python 3.13 and 3.14. The
integration suite runs against PostgreSQL 18.

Other database tasks:

```console
$ mise run db:stop    # stop the local cluster
$ mise run db:reset   # stop it and delete .data/postgres
```

## Tests

The fast suite needs no database. Integration tests are marked
`pytest.mark.integration` and are excluded from the default run.

`scripts/integration.sh` runs the integration suite:

1. Creates a database named `rqueue_integration_<timestamp>_<pid>`.
2. Runs `rqueue migrate` against it as the database owner.
3. Provisions a role with the `produce` and `consume` capabilities on the queue
   `scoped`. The tests that exercise least-privilege behavior connect as this
   role.
4. Runs `pytest -m integration`.
5. Drops the database and the role on exit, also when the tests fail.

Paths after the script name go to pytest. Put other pytest options after `--`:

```console
$ bash scripts/integration.sh tests/integration/test_master_pipeline.py
$ bash scripts/integration.sh -- tests/integration/test_operations.py -k 0010
```

By default the script starts the local cluster with `mise run db:start`. To use
another server, set `RQUEUE_DATABASE_URL` to a URL whose user can create
databases and roles. The script then skips `db:start`. CI does this with a
PostgreSQL service container.

The master pipeline test
([`tests/integration/test_master_pipeline.py`](../tests/integration/test_master_pipeline.py))
walks one job lifecycle in a single run and checks the stored state after each
step. Run it after a change to claiming, leases, or the scheduler.

## Migrations

A migration file cannot be edited once released, because its checksum is
recorded. Add a new numbered file instead.

A migration that breaks a statement the previous release's workers still run
must refuse while they run. 0005, 0008, and 0010 do this in SQL. Test it with
the previous release's heartbeat statement, as the tests for those migrations in
[`tests/integration/test_operations.py`](../tests/integration/test_operations.py)
do. [Migrations](../docs/migrations.md#running-processes) describes what an
operator does when one refuses.
