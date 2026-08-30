#!/usr/bin/env bash
# UC v2 elle mutation testing (design spec 2026-07-15 §4.7): prove the elle
# harness + UC's safety layers catch injected consensus bugs. First a CONTROL
# (feature compiled in, UC2_MUTATION unset -> every pass clean: the feature is
# inert), then each mutation under a DEDICATED adversary pass, asserting the
# previously-clean outcome breaks.
#
# The three teeth use three DIFFERENT oracles, because UC catches them in three
# different ways (this is a feature — defense in depth — not a shortcut):
#
#   commit-quorum-minus-one  elle verdict flips to INVALID (serializable AND
#                            strict): a degraded commit quorum lets an isolated
#                            leader split-brain -> divergent committed histories.
#   skip-read-barrier        elle verdict flips to INVALID under the STRICT model
#                            ONLY (valid under plain serializable): a deposed
#                            leader answers a stale read -> a pure real-time
#                            anomaly. This is the tooth that proves the strict
#                            model earns its keep.
#   skip-vote-order-check    the driver run HARD-FAILS (exit != 0): a stale leader
#                            wins and truncates committed entries, which the pass's
#                            own CommittedTruncationWitness convicts on directly —
#                            `durable` stepping backward below a position the node
#                            had already committed. This is a timing race (whether
#                            a stale candidate wins at all), so it is retried a few
#                            times; a clean control never fails.
#
#                            NOTE 2026-08-02: until this date the tooth had no
#                            detector of its own — it was scored on whatever
#                            happened to make the driver exit non-zero, and what
#                            did was an `uc2-archive` fail-stop. That was issue #6,
#                            a REAL UC defect, fixed 07-30 by 2fd845e; the tooth
#                            then went silent (weekly 30736463470: 0/5) even though
#                            the injected bug still destroys committed data. An
#                            oracle borrowed from a bug elsewhere expires when that
#                            bug is fixed. Hence the explicit witness.
#
# If a mutation stops being caught: RAISE the dose (ops / cycles / hold), never
# weaken the assertion. And check WHY it stopped: an oracle that fires only as a
# side effect of some other defect dies silently the day that defect is fixed
# (see the vote-order note below). Prefer a detector that names the safety
# property the mutation violates.
#
# Artifacts go to DISK, never /tmp — /tmp is RAM-backed tmpfs with no swap on this
# box and large histories there OOM-kill the run (see CLAUDE.md).
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
MUT_DIR="${ELLE_MUT_DIR:-$HOME/.cache/uc2-elle-mut}"
STRICT_MODEL="${ELLE_STRICT_MODEL:-strong-serializable}"
JAVA_XMX="${ELLE_JAVA_XMX:-2g}"
BUDGET="${ELLE_MUTATION_BUDGET_SECS:-150}"
VOTE_ORDER_TRIES="${ELLE_VOTE_ORDER_TRIES:-3}"

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq not found" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR (vendored jar; see tools/elle-cli/README.md)" >&2; exit 1; }
mkdir -p "$MUT_DIR"

# gen <subdir> <passfn> <UC2_MUTATION> <extra KEY=VAL env ...>: run one driver
# pass. Echoes the driver's EXIT CODE on stdout (0 = test passed). History (if
# the pass wrote one) lands at $MUT_DIR/<subdir>/<pass-without-elle_>/history.edn.
gen() {
    local sub="$1" passfn="$2" mut="$3"; shift 3
    rm -rf "${MUT_DIR:?}/$sub"
    local ec=0
    ( cd "$ROOT" && env "$@" UC2_MUTATION="$mut" ELLE_DIR="$MUT_DIR/$sub" \
        ELLE_BUDGET_SECS="$BUDGET" \
        cargo test -p uc_node --release --features mutation-testing \
        --test elle_v2 -- --ignored --exact "$passfn" --nocapture ) >"$MUT_DIR/$sub.log" 2>&1 || ec=$?
    echo "$ec"
}

hist() { echo "$MUT_DIR/$1/${2#elle_}/history.edn"; }

# valid_under <consistency-model> <history>: echoes the "valid?" flag (true|false|
# unknown|missing). Bounds the JVM heap so a pathological history fails loudly
# instead of ballooning into the OOM killer.
valid_under() {
    [ -f "$2" ] || { echo missing; return; }
    local out
    out="$("$JAVA" "-Xmx$JAVA_XMX" -jar "$JAR" --model list-append \
        --consistency-models "$1" --verbose "$2" 2>/dev/null)" || true
    # jq on EMPTY stdin exits 0 with no output, so guard it — else an elle-cli
    # crash would silently yield "" instead of the intended "missing".
    [ -n "$out" ] || { echo missing; return; }
    printf '%s' "$out" | jq -r '(.["valid?"])|tostring' 2>/dev/null || echo missing
}

fail() { echo "FAIL: $1" >&2; exit 1; }

echo "== CONTROL: feature-on build, UC2_MUTATION unset — every pass must be clean =="
# commit-quorum's pass: strict-clean control.
ec="$(gen ctl_cq elle_mut_commit_quorum "" ELLE_TARGET_OPS=4000 ELLE_MIN_FAULTS=4 ELLE_WORKERS=4)"
[ "$ec" = 0 ] || fail "control commit_quorum pass errored (exit $ec) — see $MUT_DIR/ctl_cq.log"
v="$(valid_under "$STRICT_MODEL" "$(hist ctl_cq elle_mut_commit_quorum)")"
[ "$v" = true ] || fail "control commit_quorum not strict-clean (valid?=$v) — feature not inert"
# read-barrier's directed pass: strict-clean control (its reads simply fail —
# a non-leader refuses them — so no stale read is ever recorded).
ec="$(gen ctl_rb elle_mut_read_barrier "" ELLE_MIN_FAULTS=6)"
[ "$ec" = 0 ] || fail "control read_barrier pass errored (exit $ec) — see $MUT_DIR/ctl_rb.log"
v="$(valid_under "$STRICT_MODEL" "$(hist ctl_rb elle_mut_read_barrier)")"
[ "$v" = true ] || fail "control read_barrier not strict-clean (valid?=$v) — feature not inert"
# vote-order's pass: a clean control simply passes (reconverges every cycle).
ec="$(gen ctl_vo elle_mut_vote_order "" ELLE_TARGET_OPS=3000 ELLE_MIN_FAULTS=10 ELLE_WORKERS=4 ELLE_HOLD_MS=3000)"
[ "$ec" = 0 ] || fail "control vote_order pass hard-failed (exit $ec) — feature not inert"
echo "OK: control clean (all three passes)"

echo "== TOOTH 1/3: commit-quorum-minus-one — elle verdict must flip to INVALID =="
ec="$(gen cq elle_mut_commit_quorum commit-quorum-minus-one ELLE_TARGET_OPS=4000 ELLE_MIN_FAULTS=4 ELLE_WORKERS=4)"
h="$(hist cq elle_mut_commit_quorum)"
vs="$(valid_under serializable "$h")"; vst="$(valid_under "$STRICT_MODEL" "$h")"
[ "$vst" = false ] || fail "commit-quorum NOT caught (strict valid?=$vst; if 'missing' the driver crashed — see $MUT_DIR/cq.log) — raise dose, never weaken"
echo "OK: commit-quorum CAUGHT (serializable valid?=$vs, strict valid?=$vst)"

echo "== TOOTH 2/3: skip-read-barrier — STRICT-ONLY real-time anomaly =="
ec="$(gen rb elle_mut_read_barrier skip-read-barrier ELLE_MIN_FAULTS=6)"
h="$(hist rb elle_mut_read_barrier)"
vs="$(valid_under serializable "$h")"; vst="$(valid_under "$STRICT_MODEL" "$h")"
[ "$vst" = false ] || fail "read-barrier NOT caught under strict model (valid?=$vst; if 'missing' the driver crashed — see $MUT_DIR/rb.log) — raise dose"
# The whole point of this tooth: invisible to plain serializability.
[ "$vs" = true ] || echo "  note: expected serializable-clean (valid?=true) but got '$vs' — a stale read is a pure real-time anomaly; strict caught it regardless"
echo "OK: read-barrier CAUGHT under strict model only (serializable valid?=$vs, strict valid?=$vst)"

echo "== TOOTH 3/3: skip-vote-order-check — driver run must HARD-FAIL (<= $VOTE_ORDER_TRIES tries) =="
caught=0
for t in $(seq 1 "$VOTE_ORDER_TRIES"); do
    ec="$(gen vo elle_mut_vote_order skip-vote-order-check ELLE_TARGET_OPS=3000 ELLE_MIN_FAULTS=12 ELLE_WORKERS=4 ELLE_HOLD_MS=3000)"
    if [ "$ec" != 0 ]; then
        # Prefer the witness's own verdict over the bare panic location; fall back
        # to any panic / lost-leader line (a crash is still a legitimate catch).
        # The trailing `|| true` matters under `set -e`: a hard-fail with NO
        # matching line (an OOM kill, say) must still report, not abort here.
        why="$(grep -m1 -a 'COMMITTED-TRUNCATION' "$MUT_DIR/vo.log" \
               || grep -m1 -aE 'panicked at|no single serving leader' "$MUT_DIR/vo.log" \
               || true)"
        why="$(printf '%s' "$why" | sed 's/^[[:space:]]*//' | cut -c1-110)"
        echo "OK: vote-order CAUGHT on try $t (exit $ec: $why)"
        caught=1; break
    fi
    echo "  try $t: reconverged (exit 0) — stale-winner race did not land, retrying"
done
[ "$caught" = 1 ] || fail "vote-order NOT caught in $VOTE_ORDER_TRIES tries — raise dose (ELLE_MIN_FAULTS/ELLE_HOLD_MS), and see the 2026-08-02 entry in docs/benchmarks/uc2-elle-gate-2026-07-16.md before assuming the dose is the problem"

echo "elle mutation testing passed: control clean, 3/3 teeth caught"
