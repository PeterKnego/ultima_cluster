#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# ultima_cluster quickstart: a real three-node cluster on one host, from
# binaries only — no Rust toolchain, no source tree, no container runtime.
#
# Unpack a release tarball and run it:
#
#     tar xzf uc2-<ver>-<target>.tar.gz
#     cd uc2-<ver>-<target>
#     packaging/quickstart-local.sh
#
# It writes a config file per node, starts three `uc2-node` daemons, waits for
# one of them to become a serving leader, attaches a `counter-service` to each,
# starts a `uc2-gateway` in front of each, and then drives the whole thing from
# the outside with `counter-remote`: reset, add 5, add 5, read back
# linearizably, expect 10. Then it kills everything it started.
#
#   Usage: quickstart-local.sh [--bin-dir DIR] [--root DIR] [--secs N] [--keep] [--full]
#     --bin-dir   directory holding uc2-node, uc2ctl, uc2-gateway,
#                 counter-service and counter-remote. Default: the `bin`
#                 sibling of this script's directory (i.e. the layout of an
#                 extracted release tarball, uc2-<ver>-<target>/{bin,packaging}),
#                 or $UC2_BIN_DIR if that is set.
#     --root      where the cluster's state lives. Default $HOME/uc2-quickstart.
#                 EVERY RUN STARTS A FRESH CLUSTER: $ROOT/n0,n1,n2 are deleted
#                 first. A non-empty root this script did not create is refused
#                 rather than deleted, so pointing it at a real instance
#                 directory cannot destroy anything.
#                 A root under /tmp or /dev/shm is REFUSED: those are usually
#                 RAM-backed, every fsync there is a silent no-op, and a node
#                 refuses to start on one anyway.
#     --secs N    keep the cluster up N more seconds after the demo, so you can
#                 poke at it in another terminal. Default 0.
#     --keep      leave the cluster running and print the PIDs and how to stop
#                 it. Without this, everything started here is killed on exit —
#                 including on failure and on Ctrl-C.
#     --full      accepted for compatibility; three gateways is the only mode.
#                 (One gateway per node is what makes REDIRECT work: a client
#                 that dials a follower's gateway is told where the leader is.
#                 With a single gateway there is nowhere to redirect to.)
#
#   Exit codes: 0 = PASS, 1 = a step failed (named, with log tails),
#               3 = a precondition (missing binary, bad root, port in use).
#
# Ports: nodes take UDP 9100-9102, gateways take TCP 9200-9202. Disk: each node
# RESERVES its log buffer and IPC rings up front (~78 MiB), so budget ~250 MiB
# under --root plus room for the journals.
#
# This script depends on bash, coreutils and sed only — no curl, no jq, no ss —
# so it runs in a bare `ubuntu:24.04` container with nothing else installed.

set -euo pipefail

APP="quickstart"
NODE_PORT_BASE=9100
GW_PORT_BASE=9200
GATEWAYS="127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9202"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${UC2_BIN_DIR:-$(dirname -- "$SCRIPT_DIR")/bin}"
ROOT="${HOME:-/root}/uc2-quickstart"
SECS=0
KEEP=0

PIDS=()
NAMES=()

usage() {
    sed -n '/^#   Usage:/,/^#               3 = a precondition/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# ---------------------------------------------------------------- helpers

say()  { printf '%s\n' "$*"; }
precond() { printf 'quickstart-local.sh: %s\n' "$*" >&2; exit 3; }

dump_logs() {
    local f
    for f in "$LOGS"/*.log; do
        [ -e "$f" ] || continue
        printf '\n----- %s (last 20 lines) -----\n' "$f"
        tail -n 20 "$f" 2>/dev/null || true
    done
}

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    dump_logs >&2
    exit 1
}

# Something is accepting TCP connections there. Bash's /dev/tcp does the whole
# job; `ss`/`nc`/`curl` are not installed in a minimal container.
tcp_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

start_bg() { # start_bg NAME CMD...
    local name="$1"; shift
    "$@" >"$LOGS/$name.log" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    NAMES+=("$name")
    say "   started $name (pid $pid)"
}

# A child that has already exited is never going to satisfy the condition we
# are polling for — say so now, with its log, instead of at the timeout.
assert_children_alive() {
    local i
    for i in "${!PIDS[@]}"; do
        if ! kill -0 "${PIDS[i]}" 2>/dev/null; then
            fail "$1: ${NAMES[i]} (pid ${PIDS[i]}) exited early — see $LOGS/${NAMES[i]}.log"
        fi
    done
}

# Every exit path lands here: success, a named failure, `set -e`, Ctrl-C.
# shellcheck disable=SC2317  # reached via `trap`, which shellcheck cannot see
cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    if [ ${#PIDS[@]} -eq 0 ]; then exit "$rc"; fi
    if [ "$KEEP" = 1 ]; then
        say ""
        say "cluster left running (--keep). Stop it with:"
        say "   kill ${PIDS[*]}"
        exit "$rc"
    fi
    local i pid waited alive
    for ((i = ${#PIDS[@]} - 1; i >= 0; i--)); do
        kill -TERM "${PIDS[i]}" 2>/dev/null || true
    done
    waited=0
    while [ "$waited" -lt 50 ]; do
        alive=0
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then alive=1; fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 0.1
        waited=$((waited + 1))
    done
    for pid in "${PIDS[@]}"; do
        kill -KILL "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    exit "$rc"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- arguments

while [ $# -gt 0 ]; do
    case "$1" in
        --bin-dir) BIN_DIR="${2:?--bin-dir needs a directory}"; shift 2 ;;
        --root)    ROOT="${2:?--root needs a directory}"; shift 2 ;;
        --secs)    SECS="${2:?--secs needs a number}"; shift 2 ;;
        --keep)    KEEP=1; shift ;;
        --full)    say "note: --full is the default; flag accepted for compatibility"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'quickstart-local.sh: unknown argument %s\n\n' "$1" >&2; usage >&2; exit 3 ;;
    esac
done

case "$ROOT" in /*) ;; *) ROOT="$PWD/$ROOT" ;; esac
case "$BIN_DIR" in /*) ;; *) BIN_DIR="$PWD/$BIN_DIR" ;; esac
LOGS="$ROOT/logs"

# ---------------------------------------------------------------- 0. preconditions

case "$ROOT" in
    /tmp|/tmp/*|/dev/shm|/dev/shm/*)
        precond "--root $ROOT is under a RAM-backed filesystem: every fsync there is a \
silent no-op, so a node refuses to start on one. Pick a directory on a real disk." ;;
esac

case "$SECS" in
    ''|*[!0-9]*) precond "--secs must be a whole number of seconds, got '$SECS'" ;;
esac

[ -d "$BIN_DIR" ] || precond "--bin-dir $BIN_DIR is not a directory"
for b in uc2-node uc2ctl uc2-gateway counter-service counter-remote; do
    [ -x "$BIN_DIR/$b" ] || precond "$BIN_DIR/$b is missing or not executable"
done

# The marker file is the safety catch: this only ever deletes state under a
# root this script created, never an operator's real instance directory.
if [ -d "$ROOT" ] && [ -n "$(ls -A "$ROOT" 2>/dev/null)" ] && [ ! -e "$ROOT/.uc2-quickstart" ]
then
    precond "--root $ROOT is not empty and was not created by this script. Point --root at \
an empty or new directory; this script starts a fresh cluster every run and will not delete \
state it does not recognise."
fi

for i in 0 1 2; do
    p=$((GW_PORT_BASE + i))
    if tcp_open 127.0.0.1 "$p"; then
        precond "TCP port $p is already in use — stop whatever holds it (an earlier \
quickstart left running?) and try again"
    fi
done

say "ultima_cluster quickstart"
say "   binaries: $BIN_DIR"
say "   root:     $ROOT"
say ""

# ---------------------------------------------------------------- 1. config

say "1. writing configuration"

# Every run starts a FRESH cluster. Not tidiness: a node re-creates its
# control page on startup, and anything that attached to the previous run's
# page (a service, `uc2ctl status`, a gateway) would be reading a stale
# instance — which is exactly the case the instance_id contract fail-stops on.
# A demo with no supervisor to restart those processes must not walk into it.
#
# The marker file (checked in the preconditions above) is the safety catch:
# this only ever deletes state under a root this script created, never an
# operator's real instance directory.
mkdir -p "$ROOT"
: >"$ROOT/.uc2-quickstart"
rm -rf "$ROOT/n0" "$ROOT/n1" "$ROOT/n2" "$LOGS"
mkdir -p "$LOGS"
for i in 0 1 2; do mkdir -p "$ROOT/n$i"; done

# The admin key authenticates membership changes (uc2ctl add/promote/remove).
# It is generated once and reused: gen-admin-key refuses to overwrite, on
# purpose — rotating a key is a deliberate act, not a rerun side effect.
if [ ! -f "$ROOT/admin.key" ]; then
    "$BIN_DIR/uc2ctl" gen-admin-key "$ROOT/admin.key" >"$LOGS/gen-admin-key.log" 2>&1 ||
        fail "step 1 (gen-admin-key): see $LOGS/gen-admin-key.log"
fi

members_toml=""
for i in 0 1 2; do
    members_toml+="[[members]]
id = $i
addr = \"127.0.0.1:$((NODE_PORT_BASE + i))\"

"
done

gw_members_toml=""
for i in 0 1 2; do
    gw_members_toml+="[[members]]
node_id = $i
gateway = \"127.0.0.1:$((GW_PORT_BASE + i))\"

"
done

for i in 0 1 2; do
    # This is a real node.toml — the same shape you would install at
    # /etc/uc2/node.toml. `bind` and this node's own [[members]] entry must be
    # the identical address; the node refuses to start if they disagree.
    cat >"$ROOT/n$i/node.toml" <<EOF
id = $i
bind = "127.0.0.1:$((NODE_PORT_BASE + i))"
instance_dir = "$ROOT/n$i"
app_id = "$APP"

$members_toml# Cleartext node-to-node traffic. Both sections are REQUIRED: an absent one
# is a startup refusal naming it, never a silent default. See
# docs/how-to/encrypt-node-traffic.md before running this over a real network.
[crypto]
enabled = false

# Membership changes must be signed with a named key. auth = "none" is the
# pre-v2.6.0 posture, where filesystem permissions are the only boundary.
[admin]
auth = "hmac"
keys = [{ name = "admin", key_path = "$ROOT/admin.key" }]
EOF

    # One gateway per node, each attached to its own local node over shmem.
    # The [[members]] table is how a gateway that is NOT on the leader tells a
    # client where to go — it is the only place gateway addresses exist, so it
    # must be identical in all three files.
    cat >"$ROOT/gw$i.toml" <<EOF
[local]
instance_dir = "$ROOT/n$i"
app_id = "$APP"
listen = "127.0.0.1:$((GW_PORT_BASE + i))"

$gw_members_toml# counter-service runs a plain CounterSm, not a uc_service::Sessioned wrapper
# around one, so there is nothing on the far end to strip a session envelope:
# raw pass-through. A production service wraps its state machine in Sessioned
# and turns this on, which is what makes a re-sent write answer "replayed"
# instead of applying twice.
[session]
envelope = false
EOF
done
say "   3 node.toml + 3 gateway.toml under $ROOT"

# ---------------------------------------------------------------- 2. nodes

say "2. starting three nodes"
for i in 0 1 2; do
    start_bg "node$i" "$BIN_DIR/uc2-node" --config "$ROOT/n$i/node.toml"
done

say "   waiting for a serving leader (up to 30s)"
LEADER=""
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    assert_children_alive "step 2 (election)"
    for i in 0 1 2; do
        if out=$("$BIN_DIR/uc2ctl" status --instance-dir "$ROOT/n$i" --app-id "$APP" 2>/dev/null)
        then
            case "$out" in *"leader=true can_serve=true"*) LEADER="$i"; break 2 ;; esac
        fi
    done
    sleep 0.2
done
[ -n "$LEADER" ] || fail "step 2 (election): no serving leader after 30s"
say "   node $LEADER is the serving leader"

# ---------------------------------------------------------------- 3. services

say "3. attaching a counter-service to each node"
for i in 0 1 2; do
    start_bg "service$i" "$BIN_DIR/counter-service" --instance-dir "$ROOT/n$i" --app-id "$APP"
done

# ---------------------------------------------------------------- 4. gateways

say "4. starting a gateway in front of each node"
for i in 0 1 2; do
    start_bg "gateway$i" "$BIN_DIR/uc2-gateway" --config "$ROOT/gw$i.toml"
done

say "   waiting for the gateways to accept (up to 30s)"
deadline=$(( $(date +%s) + 30 ))
for i in 0 1 2; do
    p=$((GW_PORT_BASE + i))
    until tcp_open 127.0.0.1 "$p"; do
        assert_children_alive "step 4 (gateway listen)"
        [ "$(date +%s)" -lt "$deadline" ] ||
            fail "step 4 (gateway listen): nothing accepting on 127.0.0.1:$p after 30s"
        sleep 0.2
    done
done
say "   listening on $GATEWAYS"

# ---------------------------------------------------------------- 5. the demo

remote() { "$BIN_DIR/counter-remote" --gateways "$GATEWAYS" --app-id "$APP" "$@" 2>>"$LOGS/counter-remote.log"; }

say "5. driving the cluster from outside, through the gateways"

# The first request doubles as the readiness probe: the services still have to
# attach and the leader still has to commit its NewTerm frame before anything
# can be applied. `reset` is idempotent, so retrying it is free — and it makes
# a rerun against an existing --root start from zero.
ok=0
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    assert_children_alive "step 5 (first request)"
    if out=$(remote reset); then ok=1; break; fi
    sleep 0.5
done
[ "$ok" = 1 ] || fail "step 5 (first request): the cluster never answered a remote reset"
say "   reset            -> $out"

out=$(remote add 5) || fail "step 5 (add 5): counter-remote failed"
say "   add 5            -> $out"
out=$(remote add 5) || fail "step 5 (add 5, again): counter-remote failed"
say "   add 5            -> $out"
out=$(remote get --linearizable) || fail "step 5 (linearizable read): counter-remote failed"
say "   get              -> $out"

[ "$out" = "value=10" ] || fail "step 5 (assertion): expected value=10, got '$out'"

say ""
say "PASS"
say ""
say "Two writes were acknowledged only after a MAJORITY of the three nodes had"
say "fsync'd them, and the read went through the cluster's read barrier — via a"
say "gateway that may not even have been the leader's."
say ""
say "Try it yourself while the cluster is up (--keep or --secs N):"
say "   $BIN_DIR/counter-remote --gateways $GATEWAYS --app-id $APP add 7"
say "   $BIN_DIR/uc2ctl status --instance-dir $ROOT/n0 --app-id $APP"

if [ "$SECS" -gt 0 ]; then
    say ""
    say "holding the cluster up for ${SECS}s"
    sleep "$SECS"
fi

exit 0
