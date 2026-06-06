#!/usr/bin/env bash
# Drive the UC single-node commit-path load bench for both fsync targets.
# Disk run: journal on the default real-disk TMPDIR.
# tmpfs run: journal on a RAM disk (Linux: /dev/shm; macOS: hdiutil RAM disk).
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

OUT_DIR="${OUT_DIR:-bench-out}"
mkdir -p "$OUT_DIR"
RATES="${RATES:-100,500,1000,2000,5000,10000,20000}"
INFLIGHT="${INFLIGHT:-1,8,32,128}"
PAYLOAD="${PAYLOAD:-64}"

build() { cargo build -p uc_autobench --bin commit-path-load --release; }
run() { # $1=config-label $2=tmpdir
  TMPDIR="$2" ./target/release/commit-path-load \
    --config "$1" --rates "$RATES" --inflight "$INFLIGHT" \
    --payload-bytes "$PAYLOAD" --out "$OUT_DIR/uc_$1.csv"
}

build

# --- real disk ---
run single_disk "${TMPDIR:-/tmp}"

# --- tmpfs / RAM disk ---
if [[ "$(uname)" == "Linux" && -d /dev/shm ]]; then
  run single_tmpfs /dev/shm
elif [[ "$(uname)" == "Darwin" ]]; then
  # 512MB RAM disk; skip gracefully if hdiutil unavailable or RAM disk fails.
  # The whole RAM-disk dance is wrapped so a failure can't abort the (already
  # completed) disk run, and the detach always fires to avoid leaking the disk.
  if command -v hdiutil >/dev/null; then
    # `hdiutil attach -nomount` pads the device path with trailing whitespace on
    # macOS; `xargs` trims it so `detach` can find the disk.
    DEV=$(hdiutil attach -nomount ram://1048576 | xargs)
    if [[ -n "$DEV" ]] && diskutil erasevolume HFS+ ucbenchram "$DEV" >/dev/null; then
      run single_tmpfs /Volumes/ucbenchram || echo "tmpfs run failed (disk run unaffected)" >&2
      hdiutil detach "$DEV" >/dev/null || echo "WARN: could not detach RAM disk $DEV" >&2
    else
      echo "SKIP tmpfs run: RAM disk setup failed" >&2
      [[ -n "$DEV" ]] && hdiutil detach "$DEV" >/dev/null 2>&1 || true
    fi
  else
    echo "SKIP tmpfs run: no hdiutil" >&2
  fi
else
  echo "SKIP tmpfs run: unsupported platform" >&2
fi
echo "UC CSVs in $OUT_DIR/" >&2
