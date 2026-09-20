# Development

PostgreSQL is managed by [mise](https://mise.jdx.dev) and is project-local. It
does not use Docker. The layout matches the sibling picv-2025 repository, so one
mental model covers both.

```console
$ mise run install           # sync the virtualenv
$ mise run db:start          # start .data/postgres (init on first run)
$ mise run test              # fast suite, no database
$ mise run test-integration  # disposable database + scoped role, then teardown
$ mise run test-pipeline     # just the §11 master pipeline test, for the inner loop
$ mise run lint              # ruff check, ruff format --check, mypy --strict
$ mise run db:reset          # delete the local cluster
```

`scripts/integration.sh` creates `rqueue_integration_<timestamp>_<pid>`, runs
rqueue's own migrations against it with an admin connection, provisions a scoped
least-privilege role, runs `pytest -m integration`, and drops both the database
and the role in a `trap ... EXIT`. It never runs against a shared database. Set
`RQUEUE_DATABASE_URL` and it skips `mise run db:start`, which is how CI supplies
its own service container.

The fast suite is database-free and stays that way. Integration tests are marked
`pytest.mark.integration` and excluded from the default run.

The "§11" above refers to [Requirements §11](requirements.md).
