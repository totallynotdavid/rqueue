#!/usr/bin/env bash
set -euo pipefail

# Without RQUEUE_DATABASE_URL, each run owns a temporary PostgreSQL cluster on
# its own port and removes it on exit. CI supplies a shared service container
# through RQUEUE_DATABASE_URL instead. The tests borrow a database there and
# do not manage that server's lifecycle.
cluster="$(dirname "$0")/cluster.sh"
base_url="${RQUEUE_DATABASE_URL:-}"
cluster_dir=""
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

cleanup() {
    if [ -n "$cluster_dir" ]; then
        bash "$cluster" stop "$cluster_dir"
        rm -rf "$cluster_dir"
    else
        uv run python -m scripts.database drop \
            --base-url "$base_url" \
            --name "$database_name" \
            --role "$app_role" >/dev/null
    fi
}
trap cleanup EXIT

if [ -z "$base_url" ]; then
    cluster_dir="$(mktemp -d "${TMPDIR:-/tmp}/rqueue-integration.XXXXXX")"
    base_url="$(bash "$cluster" start "$cluster_dir")"
    RQUEUE_LOCAL_CLUSTER_PORT="$(cat "$cluster_dir/rqueue.port")"
    export RQUEUE_LOCAL_CLUSTER_DIR="$cluster_dir" RQUEUE_LOCAL_CLUSTER_PORT
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
