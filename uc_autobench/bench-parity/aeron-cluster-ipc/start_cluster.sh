#!/usr/bin/env bash
# Start a 3-node Aeron cluster with IPC client edge (matched to ultima_cluster).
#
# DIFF vs shipped scripts/samples/cluster_localhost/start_cluster.sh:
#   - The 4th media driver (md_client, /dev/shm/client-driver) is NOT started.
#     The client shares node0's driver, so the client<->node hop is shared memory.
#   - Everything else (3 node drivers + 3 cluster-node processes) is unchanged.
#
# Point AERON_SCRIPT_HOME at the aeron-benchmarks `scripts/aeron` dir (the one
# holding the `media-driver` and `cluster-node` launchers).
set -euxo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# Required: the aeron-benchmarks deployTar `scripts/aeron` dir (holds the
# `media-driver` / `cluster-node` / `cluster-client` launchers). No default —
# the aeron-benchmarks checkout lives outside this repo.
if [[ -z "${AERON_SCRIPT_HOME:-}" ]]; then
  echo "ERROR: set AERON_SCRIPT_HOME to the aeron-benchmarks scripts/aeron dir" >&2
  exit 2
fi

function startNode() {
  node=$1
  JVM_OPTS="-Xms16M"
  JVM_OPTS="${JVM_OPTS} -Daeron.event.cluster.log=all -Daeron.event.cluster.log.disable=APPEND_POSITION,COMMIT_POSITION"
  JVM_OPTS="${JVM_OPTS} -Daeron.event.log.filename=log_${node}.log"
  export JVM_OPTS
  "${AERON_SCRIPT_HOME}/cluster-node" "${DIR}/cluster.properties" "${DIR}/node${node}.properties" > "node${node}.out" &
}

echo "Starting media drivers (one per node; NO separate client driver — client shares node0's)"
"${AERON_SCRIPT_HOME}/media-driver" "${DIR}/cluster.properties" "${DIR}/node0.properties" > md_node0.out &
"${AERON_SCRIPT_HOME}/media-driver" "${DIR}/cluster.properties" "${DIR}/node1.properties" > md_node1.out &
"${AERON_SCRIPT_HOME}/media-driver" "${DIR}/cluster.properties" "${DIR}/node2.properties" > md_node2.out &

echo "Cluster nodes (node0 is appointed leader, co-located with client)"
startNode 0
startNode 1
startNode 2

echo "Start client (shares /dev/shm/node0-driver -> ingress/egress over aeron:ipc)"
export JVM_OPTS="-Xms16M"
"${AERON_SCRIPT_HOME}/cluster-client" "${DIR}/cluster.properties" "${DIR}/client.properties"
