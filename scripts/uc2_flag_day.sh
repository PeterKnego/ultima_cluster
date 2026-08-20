#!/usr/bin/env bash
# UC v2 M11 Task 7 — scripted flag-day cluster upgrade with measured downtime.
#
# Composes M9's clean drain (SIGTERM/`systemctl stop`) and `uc2ctl status`'s
# counters into: preflight -> confirm traffic stopped -> stop ALL nodes ->
# verify every stopped node's durable position agrees -> run the operator's
# upgrade hook on every host -> start ALL nodes -> wait for one serving
# leader -> print measured downtime.
#
# A cluster runs all-encrypted or all-cleartext, and (wire protocol 0.5.0) a
# mixed-version cluster STALLS commits rather than making unsound ones — so
# this is a flag day: every node stops, upgrades, and restarts together.
# There is no rolling/partial mode.
#
# The ABORT PATH IS LOAD-BEARING: once nodes are stopped, any failure this
# script detects on the way back up (durable mismatch at step 3, a failed
# upgrade-cmd at step 4, or any other unexpected error) restarts every node
# on whatever binary is currently in place and exits 1 — the cluster is
# never left down. This is the "un-upgrade" path; it does not revert a
# partially-applied `--upgrade-cmd`, so an abort after a partial upgrade may
# leave the cluster on a MIXED version (which self-stalls commits per the
# 0.5.0 posture above rather than doing anything unsound) — the abort output
# says this explicitly so an operator knows to check `uc2ctl status`
# everywhere before assuming the cluster is healthy again.
#
# Usage (ssh mode — one systemd unit per host, real fleet):
#   uc2_flag_day.sh --hosts "user@h1,user@h2,user@h3" --ssh-key K \
#                   --unit uc2-node --uc2ctl /usr/local/bin/uc2ctl \
#                   --instance-dir /srv/uc2/nX --app-id myapp \
#                   --upgrade-cmd 'sudo install -m755 /tmp/uc2-node.new /usr/local/bin/uc2-node' \
#                   --yes-traffic-stopped
#
#   `--instance-dir` is a TEMPLATE: the literal substring `nX` is replaced
#   with `n0`, `n1`, `n2`, ... per host, in host order. `--unit` is the SAME
#   systemd unit name on every host (each host runs exactly one node).
#
# Usage (--local mode — plain `uc2-node --config` processes, SIGTERM instead
# of `systemctl`, same verify logic; for dev boxes and the m11 gate harness):
#   uc2_flag_day.sh --hosts "local:/path/n0.toml,/path/n1.toml,/path/n2.toml" \
#                   --uc2ctl target/release/uc2ctl \
#                   --uc2-node-bin target/release/uc2-node \
#                   --app-id myapp --upgrade-cmd 'true' --yes-traffic-stopped
#
#   Each `local:` entry is a node's TOML config file (its `instance_dir` is
#   read from the file itself, so `--instance-dir` is not needed/used in this
#   mode). `--upgrade-cmd` runs ONCE, locally, not once per node — there is
#   one `uc2-node` binary path on this box, so "upgrade" locally is a no-op
#   hook (e.g. `true`); a real local upgrade would swap the binary the
#   `--uc2-node-bin` path points at before this script's step 5 starts it.
#
# Exit codes: 0 = upgraded, cluster serving, `DOWNTIME: <secs>s` printed.
#             1 = refused/aborted (see the printed reason) — for a failure
#                 after nodes were stopped, everything was restarted first.
set -euo pipefail

# ------------------------------------------------------------------- style
# Same house conventions as scripts/elle_check.sh: set -euo pipefail, probe
# tools up front, ${VAR:-default} for tunables, scratch under $HOME/.cache
# (never /tmp — RAM-backed tmpfs with no swap on the dev box, see CLAUDE.md).

iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }
epoch_now() { date +%s.%N; }
log() { printf '[%s] %s\n' "$(iso_now)" "$*"; }
step() { printf '\n[%s] STEP %s: %s\n' "$(iso_now)" "$1" "$2"; }
die() { log "ERROR: $*"; exit 1; }

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o BatchMode=yes)

# ------------------------------------------------------------------- args

HOSTS_RAW=""
SSH_KEY="${HOME}/.ssh/id_ed25519"
UNIT="uc2-node"
UC2CTL=""
INSTANCE_DIR=""
APP_ID=""
UPGRADE_CMD=""
UC2_NODE_BIN=""
YES_TRAFFIC_STOPPED=0
STOP_TIMEOUT_SECS="${UC2_FLAGDAY_STOP_TIMEOUT_SECS:-15}"
SERVE_WAIT_SECS="${UC2_FLAGDAY_SERVE_WAIT_SECS:-60}"
POLL_SECS="${UC2_FLAGDAY_POLL_SECS:-1}"

usage() {
    sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-1}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --hosts) HOSTS_RAW="$2"; shift 2 ;;
        --ssh-key) SSH_KEY="$2"; shift 2 ;;
        --unit) UNIT="$2"; shift 2 ;;
        --uc2ctl) UC2CTL="$2"; shift 2 ;;
        --instance-dir) INSTANCE_DIR="$2"; shift 2 ;;
        --app-id) APP_ID="$2"; shift 2 ;;
        --upgrade-cmd) UPGRADE_CMD="$2"; shift 2 ;;
        --uc2-node-bin) UC2_NODE_BIN="$2"; shift 2 ;;
        --yes-traffic-stopped) YES_TRAFFIC_STOPPED=1; shift ;;
        -h|--help) usage 0 ;;
        *) die "unknown argument: $1 (see --help)" ;;
    esac
done

[ -n "$HOSTS_RAW" ] || die "--hosts is required"
[ -n "$UC2CTL" ] || die "--uc2ctl is required"
[ -n "$APP_ID" ] || die "--app-id is required"
[ -n "$UPGRADE_CMD" ] || die "--upgrade-cmd is required"

MODE="ssh"
declare -a HOSTS=()
declare -a CFGS=()
case "$HOSTS_RAW" in
    local:*)
        MODE="local"
        IFS=',' read -ra CFGS <<< "${HOSTS_RAW#local:}"
        [ -n "$UC2_NODE_BIN" ] || die "--uc2-node-bin is required in --local mode"
        [ -x "$UC2_NODE_BIN" ] || die "--uc2-node-bin not found/executable: $UC2_NODE_BIN"
        [ -x "$UC2CTL" ] || die "--uc2ctl not found/executable (local mode needs a local path): $UC2CTL"
        for c in "${CFGS[@]}"; do
            [ -f "$c" ] || die "config file not found: $c"
        done
        ;;
    *)
        IFS=',' read -ra HOSTS <<< "$HOSTS_RAW"
        [ -n "$INSTANCE_DIR" ] || die "--instance-dir is required in ssh mode"
        command -v ssh >/dev/null 2>&1 || die "ssh not found (ssh mode needs it)"
        ;;
esac

if [ "$MODE" = "local" ]; then N=${#CFGS[@]}; else N=${#HOSTS[@]}; fi
[ "$N" -ge 1 ] || die "--hosts named zero nodes"

for t in date awk grep; do
    command -v "$t" >/dev/null 2>&1 || die "$t not found (required)"
done

SCRATCH="$HOME/.cache/uc2-flagday/run-$$"
mkdir -p "$SCRATCH"
# shellcheck disable=SC2317  # invoked indirectly via `trap`
cleanup() { rm -rf "$SCRATCH"; }
trap cleanup EXIT

# ------------------------------------------------------------ node helpers

# ssh mode: --instance-dir is a template; the literal "nX" becomes "n<i>".
instance_dir_for() { # <i>
    if [ "${INSTANCE_DIR#*nX}" != "$INSTANCE_DIR" ]; then
        printf '%s' "${INSTANCE_DIR//nX/n$1}"
    else
        printf '%s' "$INSTANCE_DIR"
    fi
}

# local mode: instance_dir lives in the node's own TOML — the file is the
# source of truth, not a flag (Task brief: "local mode uses ONE uc2-node
# binary path arg").
local_instance_dir_for() { # <i>
    grep -E '^[[:space:]]*instance_dir[[:space:]]*=' "${CFGS[$1]}" | head -1 \
        | sed -E 's/^[^"]*"([^"]*)".*/\1/'
}

local_cfg_abs() { # <i>
    readlink -f "${CFGS[$1]}" 2>/dev/null || printf '%s' "${CFGS[$1]}"
}

ssh_run() { # <host> <remote-cmd>
    ssh "${SSH_OPTS[@]}" -i "$SSH_KEY" "$1" "$2"
}

# Prints the raw `uc2ctl status` text for node <i> on stdout; caller checks $?.
node_status() { # <i>
    if [ "$MODE" = "local" ]; then
        "$UC2CTL" status --instance-dir "$(local_instance_dir_for "$1")" --app-id "$APP_ID"
    else
        ssh_run "${HOSTS[$1]}" "$UC2CTL status --instance-dir '$(instance_dir_for "$1")' --app-id '$APP_ID'"
    fi
}

status_field() { # <status-text> <line-prefix> <field-name>
    printf '%s\n' "$1" | grep "^$2" | grep -oE "( |^)$3=[^ ]*" | head -1 | cut -d= -f2
}
cfg_version_of()  { status_field "$1" "config:" "version"; }
durable_of()      { status_field "$1" "log:" "durable"; }
leader_of()       { status_field "$1" "role:" "leader"; }
can_serve_of()    { status_field "$1" "role:" "can_serve"; }

# ------------------------------------------------------------ start/stop

start_node() { # <i> -- best-effort (never trips set -e); prints "ok"/"fail"
    if [ "$MODE" = "local" ]; then
        local cfg log
        cfg="$(local_cfg_abs "$1")"
        if pgrep -f -- "uc2-node --config $cfg" >/dev/null 2>&1; then
            echo ok; return   # already running -- idempotent
        fi
        log="$SCRATCH/node$1.log"
        # shellcheck disable=SC2024
        nohup "$UC2_NODE_BIN" --config "$cfg" >"$log" 2>&1 &
        disown
        echo ok
    else
        if ssh_run "${HOSTS[$1]}" "sudo systemctl start $UNIT"; then
            echo ok
        else
            echo fail
        fi
    fi
}

start_all() { # best-effort, parallel; used both by step 5 and the abort path
    local i
    for i in $(seq 0 $((N - 1))); do
        start_node "$i" > "$SCRATCH/start.$i" 2>&1 &
    done
    wait
    local failed=()
    for i in $(seq 0 $((N - 1))); do
        [ "$(cat "$SCRATCH/start.$i" 2>/dev/null)" = "ok" ] || failed+=("$i")
    done
    if [ "${#failed[@]}" -gt 0 ]; then
        log "WARNING: start failed for node(s): ${failed[*]} -- check manually"
    fi
}

# --------------------------------------------------------------- abort path

ABORTING=0
NODES_STOPPED=0

abort_restart() { # <reason...>
    if [ "$ABORTING" -eq 1 ]; then return; fi
    ABORTING=1
    log "ABORT: $*"
    log "un-upgrade path: restarting every node now, on whatever binary is currently"
    log "in place (the upgrade-cmd may have applied on SOME hosts and not others --"
    log "if so this restart brings the cluster up on a MIXED version, which the"
    log "0.5.0 content-attested wire protocol self-stalls commits on rather than"
    log "doing anything unsound; check 'uc2ctl status' on every node before trusting"
    log "the cluster is healthy, and re-run the upgrade only after every node agrees)."
    start_all
    log "ABORT: restart issued. Exiting 1 -- the upgrade did NOT complete."
    exit 1
}

# shellcheck disable=SC2317  # invoked indirectly via `trap`
on_err() {
    local line=$1
    if [ "$ABORTING" -eq 1 ]; then return; fi
    log "ERROR: unexpected failure at line $line"
    if [ "$NODES_STOPPED" -eq 1 ]; then
        abort_restart "unexpected script failure while nodes were stopped"
    fi
    exit 1
}
trap 'on_err $LINENO' ERR

# ============================================================== step 0

step 0 "preflight -- every host reachable, every node healthy, config versions agree"
declare -a PRE_ISSUES=()
BASE_VERSION=""
for i in $(seq 0 $((N - 1))); do
    label="node $i"
    if [ "$MODE" = "ssh" ]; then
        label="node $i (${HOSTS[$i]})"
        if ! ssh_run "${HOSTS[$i]}" "true"; then
            PRE_ISSUES+=("$label: unreachable over ssh")
            continue
        fi
    fi
    if text="$(node_status "$i" 2>&1)"; then
        v="$(cfg_version_of "$text")"
        if [ -z "$v" ]; then
            PRE_ISSUES+=("$label: 'uc2ctl status' output did not parse (no config: version=)")
            continue
        fi
        if [ -z "$BASE_VERSION" ]; then
            BASE_VERSION="$v"
        elif [ "$v" != "$BASE_VERSION" ]; then
            PRE_ISSUES+=("$label: config version=$v disagrees with node 0's $BASE_VERSION")
        fi
    else
        PRE_ISSUES+=("$label: 'uc2ctl status' failed: $text")
    fi
done
if [ "${#PRE_ISSUES[@]}" -gt 0 ]; then
    log "PREFLIGHT FAILED -- nothing was stopped:"
    for issue in "${PRE_ISSUES[@]}"; do log "  - $issue"; done
    exit 1
fi
log "preflight OK: $N node(s) healthy, config version=$BASE_VERSION"

# ============================================================== step 1

step 1 "operator confirms client traffic is stopped"
if [ "$YES_TRAFFIC_STOPPED" -ne 1 ]; then
    log "REFUSED: --yes-traffic-stopped was not given."
    log "This script cannot verify that client traffic has actually stopped --"
    log "it can only see the cluster's own counters, not who is submitting to it."
    log "Stop client traffic, then re-run with --yes-traffic-stopped. Nothing was touched."
    exit 1
fi
log "operator confirmed traffic stopped; proceeding"

# ============================================================== step 2

step 2 "stop ALL nodes (parallel)"
for i in $(seq 0 $((N - 1))); do
    (
        if [ "$MODE" = "local" ]; then
            cfg="$(local_cfg_abs "$i")"
            pat="uc2-node --config $cfg"
            if pgrep -f -- "$pat" >/dev/null 2>&1; then
                pkill -TERM -f -- "$pat" || true
                deadline=$(( $(date +%s) + STOP_TIMEOUT_SECS ))
                while pgrep -f -- "$pat" >/dev/null 2>&1; do
                    if [ "$(date +%s)" -ge "$deadline" ]; then
                        echo "fail" > "$SCRATCH/stop.$i"
                        epoch_now > "$SCRATCH/stopts.$i"
                        exit 0
                    fi
                    sleep 0.2
                done
            fi
            echo "ok" > "$SCRATCH/stop.$i"
            epoch_now > "$SCRATCH/stopts.$i"
        else
            if ssh_run "${HOSTS[$i]}" "sudo systemctl stop $UNIT"; then
                echo "ok" > "$SCRATCH/stop.$i"
            else
                echo "fail" > "$SCRATCH/stop.$i"
            fi
            epoch_now > "$SCRATCH/stopts.$i"
        fi
    ) &
done
wait

declare -a STOP_FAILS=()
LAST_STOP_TS=""
for i in $(seq 0 $((N - 1))); do
    rc="$(cat "$SCRATCH/stop.$i" 2>/dev/null || echo fail)"
    ts="$(cat "$SCRATCH/stopts.$i" 2>/dev/null || epoch_now)"
    log "node $i stop: $rc @ $ts"
    [ "$rc" = "ok" ] || STOP_FAILS+=("$i")
    if [ -z "$LAST_STOP_TS" ] || awk -v a="$ts" -v b="$LAST_STOP_TS" 'BEGIN{exit !(a>b)}'; then
        LAST_STOP_TS="$ts"
    fi
done
NODES_STOPPED=1
if [ "${#STOP_FAILS[@]}" -gt 0 ]; then
    abort_restart "node(s) ${STOP_FAILS[*]} did not confirm stop within ${STOP_TIMEOUT_SECS}s"
fi
log "all $N node(s) stopped; last-stop timestamp $LAST_STOP_TS"

# ============================================================== step 3

step 3 "verify -- every stopped node's durable position agrees"
declare -a DURABLES=()
declare -a VERIFY_ISSUES=()
for i in $(seq 0 $((N - 1))); do
    if text="$(node_status "$i" 2>&1)"; then
        d="$(durable_of "$text")"
        if [ -z "$d" ]; then
            VERIFY_ISSUES+=("node $i: status did not parse a durable= value")
        else
            DURABLES+=("$d")
            log "node $i durable=$d"
        fi
    else
        VERIFY_ISSUES+=("node $i: 'uc2ctl status' against the stopped leftover cnc failed: $text")
    fi
done
if [ "${#VERIFY_ISSUES[@]}" -gt 0 ]; then
    for issue in "${VERIFY_ISSUES[@]}"; do log "  - $issue"; done
    abort_restart "could not read every stopped node's durable position"
fi
FIRST_DURABLE="${DURABLES[0]}"
MISMATCH=0
for d in "${DURABLES[@]}"; do
    [ "$d" = "$FIRST_DURABLE" ] || MISMATCH=1
done
if [ "$MISMATCH" -eq 1 ]; then
    abort_restart "durable positions disagree across stopped nodes: ${DURABLES[*]} -- the cluster did not fully drain before stopping (or a node was already behind); upgrading now risks losing acked writes on the lagging node(s)"
fi
log "verify OK: all $N node(s) durable=$FIRST_DURABLE"

# ============================================================== step 4

step 4 "run --upgrade-cmd on every host"
if [ "$MODE" = "local" ]; then
    log "local mode: running the upgrade hook ONCE (one uc2-node binary path on this box)"
    up_rc=0
    bash -c "$UPGRADE_CMD" || up_rc=$?
    if [ "$up_rc" -eq 0 ]; then
        log "upgrade-cmd OK"
    else
        abort_restart "upgrade-cmd failed (local, rc=$up_rc)"
    fi
else
    for i in $(seq 0 $((N - 1))); do
        ( ssh_run "${HOSTS[$i]}" "$UPGRADE_CMD" > "$SCRATCH/up.$i.log" 2>&1; echo $? > "$SCRATCH/up.$i" ) &
    done
    wait
    declare -a UP_FAILS=()
    for i in $(seq 0 $((N - 1))); do
        rc="$(cat "$SCRATCH/up.$i" 2>/dev/null || echo 1)"
        log "node $i upgrade-cmd: exit $rc"
        if [ "$rc" != "0" ]; then
            UP_FAILS+=("$i")
            log "  output: $(cat "$SCRATCH/up.$i.log" 2>/dev/null)"
        fi
    done
    if [ "${#UP_FAILS[@]}" -gt 0 ]; then
        abort_restart "upgrade-cmd failed on host(s) ${UP_FAILS[*]}"
    fi
    log "upgrade-cmd OK on all $N host(s)"
fi

# ============================================================== step 5

step 5 "start ALL nodes"
NODES_STOPPED=0   # from here, "restart" IS what we're doing -- no auto-abort needed
start_all
log "start issued for all $N node(s)"

# ============================================================== step 6

step 6 "wait for a single serving leader and matching config version (bounded ${SERVE_WAIT_SECS}s)"
DEADLINE=$(( $(date +%s) + SERVE_WAIT_SECS ))
FIRST_SERVE_TS=""
while true; do
    declare -a VERS=() LDR_OK=()
    all_parsed=1
    for i in $(seq 0 $((N - 1))); do
        if text="$(node_status "$i" 2>&1)"; then
            v="$(cfg_version_of "$text")"
            l="$(leader_of "$text")"
            c="$(can_serve_of "$text")"
            [ -n "$v" ] || all_parsed=0
            VERS+=("$v")
            if [ "$l" = "true" ] && [ "$c" = "true" ]; then LDR_OK+=("$i"); fi
        else
            all_parsed=0
        fi
    done
    if [ "$all_parsed" -eq 1 ]; then
        same=1
        for v in "${VERS[@]}"; do [ "$v" = "${VERS[0]}" ] || same=0; done
        if [ "$same" -eq 1 ] && [ "${#LDR_OK[@]}" -eq 1 ]; then
            FIRST_SERVE_TS="$(epoch_now)"
            log "converged: config version=${VERS[0]}, leader=node ${LDR_OK[0]}"
            break
        fi
    fi
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        log "TIMEOUT after ${SERVE_WAIT_SECS}s: did not converge to one serving leader with matching config version"
        log "last observation: versions=${VERS[*]:-<none>} leader-ok-nodes=${LDR_OK[*]:-<none>}"
        exit 1
    fi
    sleep "$POLL_SECS"
done

# ============================================================== step 7

DOWNTIME="$(awk -v a="$LAST_STOP_TS" -v b="$FIRST_SERVE_TS" 'BEGIN{printf "%.3f", b-a}')"
step 7 "done"
log "DOWNTIME: ${DOWNTIME}s"
echo "DOWNTIME: ${DOWNTIME}s"
exit 0
