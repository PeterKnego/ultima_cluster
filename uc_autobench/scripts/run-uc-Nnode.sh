#!/usr/bin/env bash
# Launch a real N-node uc cluster on loopback, then drive the commit-path
# load bench against the leader. Journal dirs on real disk by default; set
# DATA_ROOT=/dev/shm (Linux) for a tmpfs variant.
#
# N uc-node-launch processes are started in a tight loop (so all N come up
# inside the bootstrapper's add_learner / wait_for_service_ready windows).
# node_id=1 is the bootstrapper (it runs initialize + add_learner +
# change_membership) and is therefore the cluster's initial leader.
#
# Transport is selectable via UC_TRANSPORT env var:
#   UC_TRANSPORT=udp  → each node uses UDP transport
#   UC_TRANSPORT=quic (or unset) → each node uses QUIC (default)
#
# `commit-path-load --connect` attaches to ONE node's instance dir; uc_client
# is leader-aware and a follower rejects submit with NotLeader, so we MUST
# point --connect at the leader. We probe each node dir with a tiny single
# submit and connect the full ladder to whichever node is leader.
#
# Env knobs (all optional):
#   N          number of nodes (default 3)
#   UC_TRANSPORT  transport: "udp" or "quic" (default quic)
#   APP_ID     raft app identifier (default uc-bench-Nnode)
#   DATA_ROOT  journal / raft log dir root (default /tmp/ucN)
#   IPC_ROOT   shmem ring dir root (default /tmp/ucN-ipc)
#   OUT_DIR    CSV output dir (default bench-out)
#   RATES      comma-separated throughput ladder (default 20,40,60,80,100,150,250)
#   INFLIGHT   comma-separated inflight ladder (default 1,4,16,64)
#   PAYLOAD    payload bytes per request (default 64)
#   SLEEP      seconds to wait for leader election (default 15)
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

# Resolve cargo's target dir (honors $CARGO_TARGET_DIR and .cargo/config.toml's
# build.target-dir), falling back to ./target if cargo/jq are unavailable.
resolve_target_dir() {
  local d="${CARGO_TARGET_DIR:-}"
  if [[ -z "$d" ]] && command -v cargo >/dev/null && command -v jq >/dev/null; then
    d="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | jq -r '.target_directory // empty')"
  fi
  printf '%s' "${d:-./target}"
}
TARGET_DIR="$(resolve_target_dir)"

N="${N:-3}"
APP_ID="${APP_ID:-uc-bench-${N}node}"
DATA_ROOT="${DATA_ROOT:-/tmp/uc${N}}"        # journal (raft log)
IPC_ROOT="${IPC_ROOT:-/tmp/uc${N}-ipc}"      # shmem rings
OUT_DIR="${OUT_DIR:-bench-out}"
RATES="${RATES:-20,40,60,80,100,150,250}"
INFLIGHT="${INFLIGHT:-1,4,16,64}"
PAYLOAD="${PAYLOAD:-64}"
SLEEP="${SLEEP:-15}"                          # leader-election settle time
export UC_TRANSPORT="${UC_TRANSPORT:-}"       # pass through to uc-node-launch processes

rm -rf "$DATA_ROOT" "$IPC_ROOT"; mkdir -p "$DATA_ROOT" "$IPC_ROOT" "$OUT_DIR"

cargo build -p uc_autobench --bin uc-node-launch --bin commit-path-load --release

NODE_BIN="$TARGET_DIR/release/uc-node-launch"
LOAD_BIN="$TARGET_DIR/release/commit-path-load"

# Build the peer list: node n listens on 127.0.0.1:$((7000+n))
PEERS=()
for n in $(seq 1 "$N"); do
  PEERS+=(--peer "${n}@127.0.0.1:$((7000 + n))")
done

pids=()
for n in $(seq 1 "$N"); do
  addr="127.0.0.1:$((7000 + n))"
  "$NODE_BIN" \
    --node-id "$n" --listen "$addr" "${PEERS[@]}" \
    --app-id "$APP_ID" \
    --instance-dir "$IPC_ROOT/node$n" \
    --data-dir "$DATA_ROOT/node$n" > "/tmp/uc${N}-node${n}.log" 2>&1 &
  pids+=($!)
done

# Hardened teardown: SIGINT for an ordered shutdown (uc-node-launch catches
# ctrl_c → service-then-node), wait up to ~5s, SIGKILL any survivor, then
# `wait` to reap zombies. Runs once (the EXIT path clears its own + signal
# traps).
cleanup() {
  trap - EXIT INT TERM
  local pid
  for pid in "${pids[@]}"; do kill -INT "$pid" 2>/dev/null || true; done
  for _ in $(seq 1 10); do
    local alive=0
    for pid in "${pids[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [[ "$alive" -eq 0 ]] && break
    sleep 0.5
  done
  for pid in "${pids[@]}"; do kill -KILL "$pid" 2>/dev/null || true; done
  wait "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

# Wait for the cluster to elect a leader & accept clients.
echo "waiting ${SLEEP}s for leader election (N=${N}, transport=${UC_TRANSPORT:-quic})..." >&2
sleep "$SLEEP"

# Detect the leader: probe each node dir with a 1-request ladder and pick the
# one that actually commits (followers commit nothing -> count==0). Default to
# node1 (the bootstrapper / initial leader) if probing is inconclusive.
leader_dir=""
probe_csv="$(mktemp)"
for n in $(seq 1 "$N"); do
  dir="$IPC_ROOT/node$n"
  if "$LOAD_BIN" --connect "$dir" --app-id "$APP_ID" \
       --config probe --rates 5 --inflight 1 --payload-bytes "$PAYLOAD" \
       --out "$probe_csv" >/dev/null 2>&1; then
    # data row 2, "count" is column 5; >0 means this node accepted submits.
    count="$(awk -F, 'NR==2{print $5}' "$probe_csv" 2>/dev/null || echo 0)"
    if [[ -n "${count:-}" && "$count" -gt 0 ]]; then
      leader_dir="$dir"
      echo "leader detected at node${n} ($dir), probe count=$count" >&2
      break
    fi
  fi
done
rm -f "$probe_csv"
if [[ -z "$leader_dir" ]]; then
  echo "WARNING: leader probe inconclusive; defaulting to node1" >&2
  leader_dir="$IPC_ROOT/node1"
fi

"$LOAD_BIN" \
  --connect "$leader_dir" --app-id "$APP_ID" \
  --config "${N}node_loopback" --rates "$RATES" --inflight "$INFLIGHT" \
  --payload-bytes "$PAYLOAD" --out "$OUT_DIR/uc_${N}node_loopback.csv"

echo "wrote $OUT_DIR/uc_${N}node_loopback.csv" >&2
