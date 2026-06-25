#!/usr/bin/env bash
# Floor-decomposition driver: layered e2e-delta at linger=0.
# Runs the 2x2 {node_count in 1,3} x {durability in eventual,consistent} and
# records UNLOADED (inflight=1) per-commit e2e p50/p99 so the buckets subtract:
#   base = 1node_eventual
#   fsync       = 1node_consistent - 1node_eventual
#   replication = 3node_eventual   - 1node_eventual
#   cross-check = 3node_consistent ~= base + fsync + replication
# Absolutes are inflated in the 4-vCPU sandbox; this is a HARNESS dry-run — the
# ordering/subtraction sanity is the signal, authoritative numbers come from the c6id fleet.
set -uo pipefail

BIN_DIR="${BIN_DIR:-target/release}"
LAUNCH="$BIN_DIR/uc-node-launch"
LOAD="$BIN_DIR/commit-path-load"
APP_ID="uc-floor-decomp"
# Floor = UNLOADED. At inflight=1, max rate = 1/latency (~1000/s at a 1ms floor),
# so rates must stay BELOW that or the driver backlogs and p50 explodes.
RATES="${RATES:-200,500}"
INFLIGHT="${INFLIGHT:-1}"
PAYLOAD="${PAYLOAD:-64}"
WINDOW="${WINDOW:-5}"
WARMUP="${WARMUP:-2}"
OUT="${OUT:-bench-out/floor-decomp/floor.csv}"
PEERS3="--peer 0@127.0.0.1:7001 --peer 1@127.0.0.1:7002 --peer 2@127.0.0.1:7003"
PEERS1="--peer 0@127.0.0.1:7001"

mkdir -p "$(dirname "$OUT")"
: > "$OUT"   # truncate; commit-path-load appends a header+rows per invocation

cleanup() {
  pkill -9 -f '[u]c-node-launch' 2>/dev/null || true
  rm -rf /dev/shm/uc-fd-node* /tmp/uc-fd-data* 2>/dev/null || true
}
trap cleanup EXIT

launch_node() {  # $1=node_id $2=listen_port $3=peers
  local id="$1" port="$2"; shift 2
  UC_API_BATCH_LINGER_MS=0 UC_DURABILITY="$DURA" UC_TRANSPORT=quic \
  setsid "$LAUNCH" --node-id "$id" --listen "127.0.0.1:$port" "$@" \
    --app-id "$APP_ID" --with-service \
    --instance-dir "/dev/shm/uc-fd-node$id" \
    --data-dir "/tmp/uc-fd-data$id" \
    > "bench-out/floor-decomp/node$id.out" 2>&1 < /dev/null &
}

run_arm() {  # $1=ncount $2=durability
  local ncount="$1"; DURA="$2"
  local label="${ncount}node_${DURA}"
  echo "=== ARM $label (linger=0, inflight=$INFLIGHT, rates=$RATES) ==="
  cleanup
  if [ "$ncount" = "1" ]; then
    launch_node 0 7001 $PEERS1
  else
    launch_node 0 7001 $PEERS3
    launch_node 1 7002 $PEERS3
    launch_node 2 7003 $PEERS3
  fi
  # wait for election + service ready
  sleep 12
  # commit-path-load TRUNCATES --out, so write per-arm files and merge after.
  "$LOAD" --connect /dev/shm/uc-fd-node0 --app-id "$APP_ID" \
    --config "$label" --rates "$RATES" --inflight "$INFLIGHT" \
    --payload-bytes "$PAYLOAD" --window-secs "$WINDOW" --warmup-secs "$WARMUP" \
    --out "bench-out/floor-decomp/arm_${label}.csv"
  local rc=$?
  cleanup
  sleep 2
  return $rc
}

for nc in 1 3; do
  for d in eventual consistent; do
    run_arm "$nc" "$d" || echo "ARM ${nc}node_${d} FAILED (rc=$?)"
  done
done

# Merge per-arm CSVs (header once) into $OUT.
head -1 bench-out/floor-decomp/arm_1node_eventual.csv > "$OUT" 2>/dev/null
for f in bench-out/floor-decomp/arm_*.csv; do tail -n +2 "$f" 2>/dev/null; done >> "$OUT"

echo "=== RESULTS ($OUT) ==="
cat "$OUT"
