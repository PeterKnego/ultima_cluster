#!/usr/bin/env bash
# Runs on node0. Assumes media drivers + cluster nodes already started cluster-wide
# (the run role starts them per-host). Drives the rate ladder via LoadTestRig.
# Args: HOME_DIR RATES PAYLOAD WARMUP MEASURE BATCH
set -uxo pipefail
HOME_DIR="$1"; shift
RATES="$1"; shift          # comma-separated
PAYLOAD="$1"; shift
WARMUP="$1"; shift
MEASURE="$1"; shift
BATCH="$1"; shift
export JAVA_HOME; JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
export AERON_SCRIPT_HOME="${HOME_DIR}/aeron-deploy/scripts/aeron"
CFG="${HOME_DIR}/aeron-cfg"
mkdir -p "${HOME_DIR}/results"
IFS=',' read -ra RUNGS <<< "$RATES"
for r in "${RUNGS[@]}"; do
  export JVM_OPTS="-Xms64M \
-Dio.aeron.benchmarks.output.directory=${HOME_DIR}/results \
-Dio.aeron.benchmarks.message.rate=${r} \
-Dio.aeron.benchmarks.message.length=${PAYLOAD} \
-Dio.aeron.benchmarks.batch.size=${BATCH} \
-Dio.aeron.benchmarks.warmup.iterations=${WARMUP} \
-Dio.aeron.benchmarks.warmup.message.rate=${r} \
-Dio.aeron.benchmarks.iterations=${MEASURE} \
-Dio.aeron.benchmarks.output.file=aeron_rung_${r}"
  echo "=== aeron rung ${r} ==="
  timeout 180 "${AERON_SCRIPT_HOME}/cluster-client" "${CFG}/cluster.properties" "${CFG}/client.properties" || true
done
