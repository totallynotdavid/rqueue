#!/usr/bin/env bash
set -euo pipefail

# These tests own a disposable database on the project-local cluster and never
# accept a production URL.
base_url="${RQUEUE_DATABASE_URL:-postgresql://rqueue@127.0.0.1:5432/rqueue}"
database_name="rqueue_integration_$(date +%s)_$$"
app_role="${database_name}_role"
app_password="rqueue-integration-test-password"

# Anything after the flags is passed through to pytest, so a single test file
# can be run in a loop during development:
#   bash scripts/integration.sh tests/integration/test_master_pipeline.py
pytest_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --) shift; pytest_args+=("$@"); break ;;
        -*) echo "usage: $0 [pytest args...]" >&2; exit 2 ;;
        *) pytest_args+=("$1"); shift ;;
    esac
done

# CI supplies its own service container through RQUEUE_DATABASE_URL, so
# db:start is skipped when it is set.
if [ -z "${RQUEUE_DATABASE_URL:-}" ]; then
    mise run db:start
    export RQUEUE_LOCAL_DATABASE_OWNER=1
fi

admin_url="$(
    uv run python -m scripts.database create \
        --base-url "$base_url" \
        --name "$database_name"
)"
app_url="$(
    uv run python -m scripts.database url \
        --base-url "$admin_url" \
        --name "$database_name" \
        --user "$app_role" \
        --password "$app_password"
)"

cleanup() {
    uv run python -m scripts.database drop \
        --base-url "$base_url" \
        --name "$database_name" \
        --role "$app_role" >/dev/null
}
trap cleanup EXIT

# Schema changes run with the database-owner connection. The app role below is
# deliberately limited to runtime DML on one queue.
uv run rqueue --database-url "$admin_url" migrate

RQUEUE_ROLE_PASSWORD="$app_password" \
uv run rqueue --database-url "$admin_url" grant-role \
    --role "$app_role" \
    --capability produce \
    --capability consume \
    --queue scoped

RQUEUE_ADMIN_DATABASE_URL="$admin_url" \
RQUEUE_APP_DATABASE_URL="$app_url" \
RQUEUE_APP_ROLE="$app_role" \
    uv run pytest -m integration "${pytest_args[@]}"
