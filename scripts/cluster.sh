#!/usr/bin/env bash
# cluster.sh start|url|stop <data_dir> [port]
#
# Each cluster uses its own data directory and TCP port, so clusters do not
# collide. `start` records an automatically chosen port for later restarts.
# The server uses TCP only.
set -euo pipefail

command="${1:-}"
data_dir="${2:-}"
if [ -z "$command" ] || [ -z "$data_dir" ]; then
    echo "usage: $0 start|url|stop <data_dir> [port]" >&2
    exit 2
fi
port_file="$data_dir/rqueue.port"

pg() { mise x postgres -- "$@"; }

free_port() {
    python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])'
}

url() { echo "postgresql://rqueue@127.0.0.1:$(cat "$port_file")/rqueue"; }

start_on() {
    pg pg_ctl -D "$data_dir" -l "$data_dir/server.log" -w start \
        -o "-h 127.0.0.1 -p $1 -c unix_socket_directories= -c fsync=off -c synchronous_commit=off -c full_page_writes=off" \
        >/dev/null
}

case "$command" in
    start)
        requested="${3:-}"
        if [ ! -f "$data_dir/PG_VERSION" ]; then
            mkdir -p "$data_dir"
            pg initdb -D "$data_dir" -U rqueue \
                --auth-host=trust --auth-local=trust --no-instructions >/dev/null
        fi
        if pg pg_ctl -D "$data_dir" status >/dev/null 2>&1; then
            :
        elif [ -n "$requested" ]; then
            echo "$requested" >"$port_file"
            start_on "$requested"
        else
            # A free port can be taken between choosing it and binding it, so a
            # failed start picks another.
            port="$(cat "$port_file" 2>/dev/null || free_port)"
            for _ in 1 2 3 4 5; do
                echo "$port" >"$port_file"
                if start_on "$port"; then
                    break
                fi
                port="$(free_port)"
            done
            pg pg_isready -h 127.0.0.1 -p "$(cat "$port_file")" -U rqueue -q
        fi
        port="$(cat "$port_file")"
        if ! pg psql -h 127.0.0.1 -p "$port" -U rqueue -d rqueue -qAtc "SELECT 1" \
            >/dev/null 2>&1; then
            pg createdb -h 127.0.0.1 -p "$port" -U rqueue rqueue
        fi
        url
        ;;
    url)
        url
        ;;
    stop)
        pg pg_ctl -D "$data_dir" stop -m fast -w >/dev/null 2>&1 || true
        ;;
    *)
        echo "usage: $0 start|url|stop <data_dir> [port]" >&2
        exit 2
        ;;
esac
