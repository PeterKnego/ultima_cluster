#!/usr/bin/env bash
# Read-your-writes capstone teeth (spec 2026-10-08, planning erratum 3).
# T1: node skips the wait, client guard off -> the capstone must FIND a violation.
# T2: node skips the wait, client guard on  -> clean, and the guard must fire.
set -euo pipefail
cd "$(dirname "$0")/.."
run() {
    local tooth="$1"; shift
    echo "== tooth $tooth =="
    env "$@" UC2_RYW_TOOTH="$tooth" \
        cargo test -p uc_node --features mutation-testing \
        --test read_your_writes_capstone -- --nocapture
}
run T1 UC2_MUTATION=skip-min-position-wait UC2_CLIENT_MUTATION=skip-min-position-guard
run T2 UC2_MUTATION=skip-min-position-wait
echo "both teeth caught"
