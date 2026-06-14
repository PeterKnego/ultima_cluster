#!/usr/bin/env bash
# A/B sweep of openraft max_payload_entries on the 3-node loopback commit path.
# For each value, spin a fresh 3-node cluster (real-disk journal) and drive the
# same open-loop ladder against the leader. High INFLIGHT so >300 entries can be
# pending replication — only then does max_payload_entries=300 actually bind.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

OUT=bench-out/maxpayload
mkdir -p "$OUT"

export DATA_ROOT=/home/claude/uc3-disk      # real-disk ext4 journal (fsync has cost)
export IPC_ROOT=/dev/shm/uc3-ipc            # shmem rings (tmpfs, correct)
export RATES="${RATES:-500,1000,2000,5000,10000}"
export INFLIGHT="${INFLIGHT:-64,512}"
export PAYLOAD=64
export SLEEP=15

for MPE in 300 1024 4096; do
  echo "######## max_payload_entries=$MPE ########"
  export UC_MAX_PAYLOAD_ENTRIES=$MPE
  export OUT_DIR="$OUT/tmp_$MPE"
  mkdir -p "$OUT_DIR"
  if UC_MAX_PAYLOAD_ENTRIES=$MPE bash uc_autobench/scripts/run-uc-3node.sh; then
    cp "$OUT_DIR/uc_3node_loopback.csv" "$OUT/mpe_$MPE.csv"
    # tag the config column with the mpe value for easy joining
    echo "OK mpe=$MPE -> $OUT/mpe_$MPE.csv"
  else
    echo "FAILED mpe=$MPE" >&2
  fi
done
echo "==== DONE. CSVs in $OUT ===="
ls -la "$OUT"/mpe_*.csv
