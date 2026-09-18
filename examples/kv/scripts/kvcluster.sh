#!/usr/bin/env bash
# kvcluster.sh — a three-node kv cluster on this host, from the 2.12.0 release
# binaries plus this crate's kv-service. Modelled on the release's
# packaging/quickstart-local.sh, but it leaves the cluster running and lets
# you stop/start/kill individual processes, which is what the tests need.
#
#   scripts/kvcluster.sh up [--fresh]      start 3 nodes + 3 kv-services + 3 gateways
#   scripts/kvcluster.sh down              stop everything (services, gateways, then nodes)
#   scripts/kvcluster.sh status            uc2ctl status on every node
#   scripts/kvcluster.sh leader            print the serving leader's id
#   scripts/kvcluster.sh stop|start|kill <node|service|gateway> N
#   scripts/kvcluster.sh snapshot          uc2ctl snapshot against the leader
#   scripts/kvcluster.sh snapshot-show N   uc2ctl snapshot show on node N
#   scripts/kvcluster.sh settings <file>   uc2ctl settings apply against the leader
#   scripts/kvcluster.sh ctl N <args...>   uc2ctl <args> against node N
#
# Environment:
#   KV_ROOT     cluster state (default $HOME/uc2-kv). NOT /tmp: nodes refuse
#               RAM-backed filesystems.
#   UC2_BIN_DIR release binaries (default ../release/uc2-2.12.0-x86_64-unknown-linux-gnu/bin)
#   KV_BIN_DIR  kv-service / kv (default target/release of this crate)
#   KV_SNAPSHOT_INTERVAL  genesis snapshot_interval_bytes (default 0 = on demand)
#   KV_PORT_OFFSET  added to every port below (default 0)
#
# Ports: nodes UDP 9300-9302, gateways TCP 9400-9402, metrics TCP 9500-9502.

set -euo pipefail

APP="kv"
# KV_PORT_OFFSET shifts every port, so a test cluster can coexist with a
# manual one (the tests use 10).
OFF="${KV_PORT_OFFSET:-0}"
NODE_PORT_BASE=$((9300 + OFF))
GW_PORT_BASE=$((9400 + OFF))
METRICS_PORT_BASE=$((9500 + OFF))
GATEWAYS="127.0.0.1:$GW_PORT_BASE,127.0.0.1:$((GW_PORT_BASE + 1)),127.0.0.1:$((GW_PORT_BASE + 2))"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname -- "$SCRIPT_DIR")"
SANDBOX="$(dirname -- "$APP_DIR")"
UC2_BIN_DIR="${UC2_BIN_DIR:-$SANDBOX/release/uc2-2.12.0-x86_64-unknown-linux-gnu/bin}"
# cargo may be configured with a shared target dir (this host is), so ask it.
if [ -z "${KV_BIN_DIR:-}" ]; then
    KV_BIN_DIR="$APP_DIR/target/release"
    [ -x "$KV_BIN_DIR/kv-service" ] || KV_BIN_DIR="$(cd "$APP_DIR" && cargo metadata --format-version=1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')/release"
fi
ROOT="${KV_ROOT:-$HOME/uc2-kv}"
LOGS="$ROOT/logs"
PIDS="$ROOT/pids"
SNAPSHOT_INTERVAL="${KV_SNAPSHOT_INTERVAL:-0}"

say() { printf '%s\n' "$*"; }
die() { printf 'kvcluster: %s\n' "$*" >&2; exit 3; }
tcp_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

need_bins() {
    for b in uc2-node uc2ctl uc2-gateway; do
        [ -x "$UC2_BIN_DIR/$b" ] || die "$UC2_BIN_DIR/$b is missing or not executable"
    done
    [ -x "$KV_BIN_DIR/kv-service" ] || die "$KV_BIN_DIR/kv-service missing — cargo build --release first"
}

# ---------------------------------------------------------------- process bookkeeping

pidfile() { echo "$PIDS/$1$2.pid"; }   # kind, index

alive() { # kind N
    local f; f="$(pidfile "$1" "$2")"
    [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null
}

spawn() { # kind N cmd...
    local kind="$1" n="$2"; shift 2
    if alive "$kind" "$n"; then say "   $kind$n already running (pid $(cat "$(pidfile "$kind" "$n")"))"; return 0; fi
    # Append to the log: a restarted process's history stays in one file.
    setsid "$@" >>"$LOGS/$kind$n.log" 2>&1 </dev/null &
    echo $! >"$(pidfile "$kind" "$n")"
    say "   started $kind$n (pid $!)"
}

stop_one() { # kind N [signal]
    local kind="$1" n="$2" sig="${3:-TERM}" f pid i
    f="$(pidfile "$kind" "$n")"
    [ -f "$f" ] || return 0
    pid="$(cat "$f")"
    if kill -0 "$pid" 2>/dev/null; then
        kill "-$sig" "$pid" 2>/dev/null || true
        for i in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
        if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; sleep 0.2; fi
    fi
    rm -f "$f"
    say "   stopped $kind$n (pid $pid, SIG$sig)"
}

start_node()    { spawn node "$1" "$UC2_BIN_DIR/uc2-node" --config "$ROOT/n$1/node.toml"; }
start_service() { spawn service "$1" "$KV_BIN_DIR/kv-service" --instance-dir "$ROOT/n$1" --app-id "$APP"; }
start_gateway() { spawn gateway "$1" "$UC2_BIN_DIR/uc2-gateway" --config "$ROOT/gw$1.toml"; }

ctl() { local n="$1"; shift; "$UC2_BIN_DIR/uc2ctl" "$@" --instance-dir "$ROOT/n$n" --app-id "$APP"; }

leader() {
    # A SIGKILLed node leaves its control page frozen with the leader and
    # CAN_SERVE bits still set (run-a-gateway.md, "When the node underneath
    # dies"), and `uc2ctl status` reads that page verbatim — so check the
    # process is alive before believing the page.
    local i out
    for i in 0 1 2; do
        alive node "$i" || continue
        if out=$(ctl "$i" status 2>/dev/null); then
            case "$out" in *"leader=true can_serve=true"*) echo "$i"; return 0 ;; esac
        fi
    done
    return 1
}

wait_leader() {
    local deadline=$(( $(date +%s) + "${1:-30}" )) l
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if l=$(leader); then echo "$l"; return 0; fi
        sleep 0.2
    done
    return 1
}

# ---------------------------------------------------------------- config

write_config() {
    mkdir -p "$ROOT" "$LOGS" "$PIDS"
    : >"$ROOT/.uc2-kv"
    for i in 0 1 2; do mkdir -p "$ROOT/n$i"; done
    if [ ! -f "$ROOT/admin.key" ]; then
        "$UC2_BIN_DIR/uc2ctl" gen-admin-key "$ROOT/admin.key" >"$LOGS/gen-admin-key.log" 2>&1 || die "gen-admin-key failed: see $LOGS/gen-admin-key.log"
    fi
    local members="" gw_members="" i
    for i in 0 1 2; do
        members+="[[members]]
id = $i
addr = \"127.0.0.1:$((NODE_PORT_BASE + i))\"

"
        gw_members+="[[members]]
node_id = $i
gateway = \"127.0.0.1:$((GW_PORT_BASE + i))\"

"
    done
    for i in 0 1 2; do
        cat >"$ROOT/n$i/node.toml" <<EOT
id = $i
bind = "127.0.0.1:$((NODE_PORT_BASE + i))"
instance_dir = "$ROOT/n$i"
app_id = "$APP"

# Small geometry so journal purge is observable on a laptop-sized write
# volume: purge drops whole non-active segments, so with 4 MiB segments a few
# MiB of writes after a snapshot is enough to see archive_first_base move.
# (Top-level keys MUST precede [[members]]: after it they parse as a member's.)
buffer_bytes = 16777216
journal_segment_bytes = 4194304

$members# The kv state machine implements SnapshotStateMachine and kv-service starts
# with start_with_snapshots(); this is the other half of bounding the log.
[purge]
below_snapshot_slack_bytes = 1048576

[services]
names = ["kv"]

# Genesis seed only. snapshot_interval_bytes = 0 means instants are
# operator-commanded (uc2ctl snapshot); set KV_SNAPSHOT_INTERVAL for a cadence.
[settings]
snapshot_interval_bytes = $SNAPSHOT_INTERVAL
snapshot_target = "all"

[log]
level = "info"

[metrics]
bind = "127.0.0.1:$((METRICS_PORT_BASE + i))"

[crypto]
enabled = false

[admin]
auth = "hmac"
keys = [{ name = "admin", key_path = "$ROOT/admin.key" }]
EOT
        cat >"$ROOT/gw$i.toml" <<EOT
[local]
instance_dir = "$ROOT/n$i"
app_id = "$APP"
listen = "127.0.0.1:$((GW_PORT_BASE + i))"

$gw_members[limits]
# The client's exposure window to a node that died under this gateway.
request_timeout_ms = 2000

# kv-service runs Sessioned<KvSm>: the envelope is what makes a re-sent write
# answer "replayed" instead of applying twice.
[session]
envelope = true
EOT
    done
}

# ---------------------------------------------------------------- commands

cmd_up() {
    local fresh=0
    [ "${1:-}" = "--fresh" ] && fresh=1
    need_bins
    case "$ROOT" in /tmp|/tmp/*|/dev/shm|/dev/shm/*) die "KV_ROOT=$ROOT is RAM-backed; nodes refuse it" ;; esac
    if [ -d "$ROOT" ] && [ -n "$(ls -A "$ROOT" 2>/dev/null)" ] && [ ! -e "$ROOT/.uc2-kv" ]; then
        die "$ROOT is not empty and was not created by this script"
    fi
    if [ "$fresh" = 1 ]; then
        cmd_down >/dev/null 2>&1 || true
        rm -rf "$ROOT/n0" "$ROOT/n1" "$ROOT/n2" "$LOGS" "$PIDS"
    fi
    for i in 0 1 2; do
        tcp_open 127.0.0.1 $((GW_PORT_BASE + i)) && ! alive gateway "$i" && die "TCP port $((GW_PORT_BASE + i)) is in use by something else"
    done
    write_config
    say "kv cluster: root=$ROOT"
    say "1. nodes"
    for i in 0 1 2; do start_node "$i"; done
    say "   waiting for a serving leader"
    local l; l=$(wait_leader 30) || { say "no serving leader after 30s"; tail -n 20 "$LOGS"/node*.log; exit 1; }
    say "   node $l is the serving leader"
    say "2. kv-services"
    for i in 0 1 2; do start_service "$i"; done
    say "3. gateways"
    for i in 0 1 2; do start_gateway "$i"; done
    local deadline=$(( $(date +%s) + 30 ))
    for i in 0 1 2; do
        until tcp_open 127.0.0.1 $((GW_PORT_BASE + i)); do
            [ "$(date +%s)" -lt "$deadline" ] || { say "gateway $i not listening"; exit 1; }
            sleep 0.2
        done
    done
    say "   gateways: $GATEWAYS"
    say "up. try: $KV_BIN_DIR/kv --gateways $GATEWAYS put hello world"
}

cmd_down() {
    # Reader before writer: services and gateways first, then nodes.
    for i in 0 1 2; do stop_one service "$i"; stop_one gateway "$i"; done
    for i in 0 1 2; do stop_one node "$i"; done
}

cmd_status() {
    local i
    for i in 0 1 2; do
        say "--- node $i ($(alive node "$i" && echo running || echo down); service $(alive service "$i" && echo running || echo down); gateway $(alive gateway "$i" && echo running || echo down))"
        ctl "$i" status 2>&1 || true
    done
}

case "${1:-}" in
    up) shift; cmd_up "$@" ;;
    down) cmd_down ;;
    status) cmd_status ;;
    leader) leader ;;
    wait-leader) wait_leader "${2:-30}" ;;
    # Stopping or killing a node also takes its gateway down, emulating the
    # packaged units' BindsTo=uc2-node.service: a gateway left running over a
    # dead node keeps accepting writes into a ring nobody drains and answers
    # UNKNOWN after request_timeout (documented residual window). Pass
    # KV_NO_BINDSTO=1 to reproduce that window deliberately.
    stop) stop_one "$2" "$3" TERM; [ "$2" = node ] && [ -z "${KV_NO_BINDSTO:-}" ] && stop_one gateway "$3" TERM; true ;;
    kill) stop_one "$2" "$3" KILL; [ "$2" = node ] && [ -z "${KV_NO_BINDSTO:-}" ] && stop_one gateway "$3" KILL; true ;;
    start) need_bins; case "$2" in node) start_node "$3" ;; service) start_service "$3" ;; gateway) start_gateway "$3" ;; *) die "start what?" ;; esac ;;
    snapshot) l=$(leader) || die "no leader"; ctl "$l" snapshot --admin-key "$ROOT/admin.key" ;;
    snapshot-show) ctl "$2" snapshot show ;;
    settings) l=$(leader) || die "no leader"; ctl "$l" settings apply "$2" --admin-key "$ROOT/admin.key" ;;
    ctl) n="$2"; shift 2; ctl "$n" "$@" ;;
    wipe) stop_one service "$2" TERM; stop_one gateway "$2" TERM; stop_one node "$2" TERM; rm -rf "$ROOT/n$2"; mkdir -p "$ROOT/n$2"; write_config; say "   wiped n$2" ;;
    metrics) curl -sf "http://127.0.0.1:$((METRICS_PORT_BASE + $2))/metrics" ;;
    gateways) echo "$GATEWAYS" ;;
    root) echo "$ROOT" ;;
    *) sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
