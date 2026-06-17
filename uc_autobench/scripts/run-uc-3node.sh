#!/usr/bin/env bash
# Thin shim: delegates to run-uc-Nnode.sh with N=3.
# All env knobs (RATES, INFLIGHT, PAYLOAD, SLEEP, DATA_ROOT, IPC_ROOT,
# OUT_DIR, APP_ID, UC_TRANSPORT) are passed through automatically since they
# are read by run-uc-Nnode.sh from the environment.
exec env N=3 "$(dirname "$0")/run-uc-Nnode.sh" "$@"
