#!/usr/bin/env bash
# Morning read of the UC nightly. Written for whoever opens it cold.
#
# Context: main badd703 (2026-08-05) fixed group-key activation waiting on peers
# it could never deliver to — the crypto-under-reconfiguration failure that made
# `crashtest-crypto` fail ~25%/run. Local A/B put the fixed rate at 2/40 (5.0%),
# statistically indistinguishable from the crypto-OFF floor of 3.3%. So:
#   * crashtest-crypto GREEN  -> consistent with the fix (one clean run is weak
#                                evidence on its own; the local n=40 is the real
#                                number, this is the deployment check)
#   * crashtest-crypto RED    -> read the assertion. `sigkill_mid_config_window`
#                                failing on the LIVENESS bar at ~5%/run is the
#                                known residue = the test's own flakiness, NOT a
#                                crypto regression. Anything else is new.
set -u
cd "$(dirname "$0")/.." || exit 1
OUT="$HOME/.cache/uc2-nightly/report.txt"
mkdir -p "$(dirname "$OUT")"
{
  echo "=============================================================="
  echo " UC nightly — $(date -Is)"
  echo "=============================================================="
  echo
  gh run list --workflow=nightly.yml --limit 4 \
     --json createdAt,conclusion,headSha,databaseId \
     -q '.[] | "\(.createdAt[0:16])  \(.conclusion // "running")  \(.headSha[0:7])  id=\(.databaseId)"'
  echo
  id=$(gh run list --workflow=nightly.yml --limit 1 --json databaseId -q '.[0].databaseId')
  concl=$(gh run view "$id" --json conclusion -q '.conclusion')
  echo "--- latest run $id: ${concl:-running}"
  if [ "$concl" = "success" ]; then
    echo "ALL GREEN. crashtest-crypto passed — consistent with badd703."
  else
    echo "FAILED JOBS:"
    gh run view "$id" --json jobs \
       -q '.jobs[] | select(.conclusion=="failure") | "  " + .name + "  (id " + (.databaseId|tostring) + ")"'
    for jid in $(gh run view "$id" --json jobs -q '.jobs[] | select(.conclusion=="failure") | .databaseId'); do
      echo
      echo "--- job $jid, failing test + assertion:"
      gh api "repos/PeterKnego/ultima_cluster/actions/jobs/$jid/logs" 2>/dev/null \
        | grep -aE "^.*(test .* FAILED|liveness:|panicked at)" | head -6 | cut -c1-160
    done
    echo
    echo "TRIAGE: 'sigkill_mid_config_window' + 'liveness: only N ops' is the"
    echo "KNOWN ~5%/run residue (test flakiness, present with crypto OFF too)."
    echo "Any other test, or a different assertion, is something new."
  fi
} > "$OUT" 2>&1
