#!/usr/bin/env bash
# Local validation of the replication-RPC probe: confirm the leader emits
# REPL_RPC_STATS with n>0 on a 3-node launch (real QUIC replication between 3
# localhost processes) and n=0 on a 1-node launch (no replication). Sandbox
# absolutes are inflated — this checks the probe FIRES, not the value.
set -uo pipefail
BIN_DIR="${BIN_DIR:-/home/claude/.cache/cargo-target/release}"
LAUNCH="$BIN_DIR/uc-node-launch"; LOAD="$BIN_DIR/commit-path-load"
APP_ID="uc-repl-probe"; OUTD=bench-out/floor-decomp
cleanup(){ pkill -9 -f '[u]c-node-launch' 2>/dev/null||true; rm -rf /dev/shm/uc-rp-node* /tmp/uc-rp-data* 2>/dev/null||true; }
trap cleanup EXIT
launch(){ local id=$1 port=$2; shift 2
  UC_API_BATCH_LINGER_MS=0 UC_DURABILITY=eventual UC_TRANSPORT=quic \
  setsid "$LAUNCH" --node-id "$id" --listen "127.0.0.1:$port" "$@" \
    --app-id "$APP_ID" --with-service --instance-dir "/dev/shm/uc-rp-node$id" \
    --data-dir "/tmp/uc-rp-data$id" > "$OUTD/rp-node$id.out" 2>&1 </dev/null & }

for mode in 3node 1node; do
  cleanup
  echo "=== $mode ==="
  if [ "$mode" = 3node ]; then
    P="--peer 0@127.0.0.1:7001 --peer 1@127.0.0.1:7002 --peer 2@127.0.0.1:7003"
    launch 0 7001 $P; launch 1 7002 $P; launch 2 7003 $P
  else
    launch 0 7001 --peer 0@127.0.0.1:7001
  fi
  sleep 12
  "$LOAD" --connect /dev/shm/uc-rp-node0 --app-id "$APP_ID" --config "$mode" \
    --rates 500 --inflight 1 --payload-bytes 64 --window-secs 6 --warmup-secs 2 \
    --out "$OUTD/rp_$mode.csv" >/dev/null 2>&1
  sleep 4   # let the 3s probe ticker emit a post-load line
  echo "node0 last REPL_RPC_STATS:"; grep 'REPL_RPC_STATS' "$OUTD/rp-node0.out" | tail -1
  cleanup; sleep 1
done
