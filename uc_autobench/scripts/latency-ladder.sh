#!/usr/bin/env bash
# latency-ladder.sh — run the 4-rung loopback latency ladder and print derived taxes.
#
# Runs internode-rpc-bench in --mode ping on loopback for each of:
#   bare-udp  busyspin-udp  udp  quic
# Appends each result row (prefixed with rung name) to the TSV, then computes:
#   tax_async_vs_busyspin = p50(bare-udp) − p50(busyspin-udp)
#   tax_uc_bookkeeping    = p50(udp)      − p50(bare-udp)
# Both values are printed signed — a negative value is expected on shared/virtual hosts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TSV="${REPO_ROOT}/uc_autobench/tasks/latency-ladder/results.tsv"

DURATION="${DURATION:-20}"
PAYLOAD="${PAYLOAD:-64}"

TRANSPORTS=(bare-udp busyspin-udp udp quic)

# ---------------------------------------------------------------------------
# Build once; resolve target directory via cargo metadata
# ---------------------------------------------------------------------------
echo "==> Building internode-rpc-bench (release)…" >&2
cargo build -p uc_autobench --bin internode-rpc-bench --release \
    --manifest-path "${REPO_ROOT}/Cargo.toml" >&2

TARGET_DIR="$(cargo metadata --manifest-path "${REPO_ROOT}/Cargo.toml" \
    --no-deps --format-version 1 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['target_directory'])")"
BENCH="${TARGET_DIR}/release/internode-rpc-bench"

# ---------------------------------------------------------------------------
# Reset TSV to header-only before collecting new results
# ---------------------------------------------------------------------------
printf 'rung\tsystem\tconfig\tworkload\tpayload_bytes\tinflight\ttarget_rate\tachieved_rate\tp50_ns\tp99_ns\tp99_9_ns\tp99_99_ns\tmax_ns\tcount\n' \
    > "${TSV}"

# ---------------------------------------------------------------------------
# Associative array to capture p50 values per rung
# ---------------------------------------------------------------------------
declare -A P50

for transport in "${TRANSPORTS[@]}"; do
    echo "==> Running rung: ${transport}  (duration=${DURATION}s, payload=${PAYLOAD}B)" >&2

    # The binary prints one header line then one data row to stdout.
    # We capture only the data row (line 2).
    csv_row="$("${BENCH}" \
        --role both \
        --transport "${transport}" \
        --mode ping \
        --duration "${DURATION}" \
        --payload "${PAYLOAD}" \
        2>/dev/null \
        | tail -n 1)"

    # Prepend the rung name and convert commas to tabs for the TSV.
    printf '%s\t%s\n' "${transport}" "$(echo "${csv_row}" | tr ',' '\t')" >> "${TSV}"

    # Extract p50_ns (column 8 of the original CSV = field 9 after prepending rung).
    # Original CSV columns (1-indexed): system(1) config(2) workload(3) payload_bytes(4)
    #   inflight(5) target_rate(6) achieved_rate(7) p50_ns(8) …
    p50="$(echo "${csv_row}" | cut -d',' -f8)"
    P50["${transport}"]="${p50}"

    echo "    p50=${p50} ns" >&2
done

# ---------------------------------------------------------------------------
# Derived taxes (signed integers)
# ---------------------------------------------------------------------------
p50_bare="${P50[bare-udp]}"
p50_busyspin="${P50[busyspin-udp]}"
p50_udp="${P50[udp]}"

tax_async=$(( p50_bare - p50_busyspin ))
tax_bookkeeping=$(( p50_udp - p50_bare ))

echo ""
echo "=== Latency taxes (loopback, payload=${PAYLOAD}B, duration=${DURATION}s) ==="
printf 'tax_async_vs_busyspin  = %d ns   [p50(bare-udp) − p50(busyspin-udp)]\n' "${tax_async}"
printf 'tax_uc_bookkeeping     = %d ns   [p50(udp) − p50(bare-udp)]\n' "${tax_bookkeeping}"
echo ""
echo "Full results written to: ${TSV}"
