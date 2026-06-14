#!/usr/bin/env bash
# Runs on node0. Drives commit-path-load against the local UC node's shmem instance.
# Args: BIN INSTANCE_DIR APP_ID RATES PAYLOAD INFLIGHT MEASURE WARMUP OUT
set -uxo pipefail
BIN="$1"; shift          # path to commit-path-load
INSTANCE_DIR="$1"; shift
APP_ID="$1"; shift
RATES="$1"; shift
PAYLOAD="$1"; shift
INFLIGHT="$1"; shift
MEASURE="$1"; shift
WARMUP="$1"; shift
OUT="$1"; shift
"$BIN" --connect "$INSTANCE_DIR" --app-id "$APP_ID" \
  --config dist_3node --rates "$RATES" --inflight "$INFLIGHT" \
  --payload-bytes "$PAYLOAD" --window-secs "$MEASURE" \
  --warmup-secs "$WARMUP" \
  --out "$OUT"
