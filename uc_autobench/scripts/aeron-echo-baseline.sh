#!/usr/bin/env bash
# aeron-echo-baseline.sh — turnkey per-link Aeron RTT floor (control-side).
#
# Wraps the CANONICAL aeron-io/benchmarks orchestrator
#   {{ aeron_deploy_dir }}/scripts/aeron/remote-echo-benchmarks
# (built by the build_aeron role into /opt/bench/aeron-deploy/) so that a
# point-to-point Aeron echo RTT floor is reproducible on the next provisioned
# fleet with a single command. This is the Aeron half of the netping A/B —
# the UC side lives in netping-sweep.sh; both shape the SAME node0<->node1 link.
#
# SSH / invocation model (confirmed from upstream master, 2026-06-18):
#   remote-echo-benchmarks sources remote-benchmarks-helper, which in turn
#   sources ../remote-benchmarks-runner. The orchestrator runs ON THE CONTROL
#   BOX and SSHes BOTH hosts itself via execute_remote_command:
#       ssh -i $SSH_{CLIENT,SERVER}_KEY_FILE \
#           $SSH_{CLIENT,SERVER}_USER@$SSH_{CLIENT,SERVER}_NODE  "<remote cmd>"
#   It starts the server (EchoNode) and client (LoadTestRig) each behind their
#   own media driver, pins driver conductor/sender/receiver + the app thread to
#   isolated cores, then scps the HDR results tarball back to --download-dir.
#   => There is NO client<->server SSH requirement; the control box (this host,
#      where `make up-fanout` already runs Ansible) reaches both nodes. The two
#      hosts share the same deployed SSH key, so they CAN reach each other if a
#      future upstream change needs it, but the current contract does not.
#
# required_vars the orchestrator enforces (we set ALL of them):
#   from remote-echo-benchmarks:
#     CLIENT_JAVA_HOME CLIENT_BENCHMARKS_PATH
#     CLIENT_DRIVER_CONDUCTOR_CPU_CORE CLIENT_DRIVER_SENDER_CPU_CORE
#     CLIENT_DRIVER_RECEIVER_CPU_CORE CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE
#     CLIENT_NON_ISOLATED_CPU_CORES CLIENT_CPU_NODE
#     CLIENT_DESTINATION_CHANNEL CLIENT_SOURCE_CHANNEL
#     SERVER_JAVA_HOME SERVER_BENCHMARKS_PATH
#     SERVER_DRIVER_CONDUCTOR_CPU_CORE SERVER_DRIVER_SENDER_CPU_CORE
#     SERVER_DRIVER_RECEIVER_CPU_CORE SERVER_ECHO_CPU_CORE
#     SERVER_NON_ISOLATED_CPU_CORES SERVER_CPU_NODE
#     SERVER_DESTINATION_CHANNEL SERVER_SOURCE_CHANNEL
#   from remote-benchmarks-runner (NOT validated by remote-echo-benchmarks
#   itself, but required by the runner it sources — we set them too):
#     SSH_CLIENT_USER SSH_CLIENT_KEY_FILE SSH_CLIENT_NODE
#     SSH_SERVER_USER SSH_SERVER_KEY_FILE SSH_SERVER_NODE
#
# Run-matrix env (consumed by the runner; we pass single-point defaults that
# match the ad-hoc run we did): RUNS ITERATIONS WARMUP_ITERATIONS
#   WARMUP_MESSAGE_RATE MESSAGE_RATE(csv) MESSAGE_LENGTH(csv, same count)
#   BURST_SIZE(csv = batch.size). MESSAGE_RATE and MESSAGE_LENGTH MUST have the
#   same number of CSV elements (runner asserts this).
#
# Channel semantics (confirmed; orchestrator passes these to BOTH sides):
#   DESTINATION_CHANNEL = where the client publishes / the server subscribes
#                         => the SERVER's endpoint (node1).
#   SOURCE_CHANNEL      = where the server echoes / the client subscribes
#                         => the CLIENT's endpoint (node0).
#   Both CLIENT_* and SERVER_* destination/source are set to the SAME pair so
#   the two media drivers agree on the link. Form: aeron:udp?endpoint=IP:PORT|mtu=MTU
#
# CPU rationale (ccx33 = 8 dedicated vCPU, cores 0-7):
#   A FAIR Aeron baseline needs >=4 busy-spin cores/host: driver conductor (1),
#   sender (2), receiver (3) + the app thread (4: LoadTestRig on the client /
#   EchoNode on the server). Cores 0,5,6,7 stay NON-isolated for the JVM,
#   numactl, GC, and OS. CPU_NODE=0 (single NUMA node on ccx33). ccx13 (2 vCPU)
#   is too small for this — provision ccx33 (see group_vars all.yml note).
#
# This is a PER-LINK floor: Aeron echo is point-to-point (one client, one
# server). It is NOT a quorum/commit number. For the K-of-N quorum model use
# netping-sweep.sh EXPERIMENT=fanout.
#
# DRY_RUN=1 (or --dry-run) prints the FULL orchestrator invocation with every
# env var exported and the resolved private-IP channels + CPU cores, WITHOUT
# SSHing or executing anything. Primary local validation path.
#
# Env knobs (all optional; defaults match group_vars/all.yml + our ad-hoc run):
#   INVENTORY        path to Ansible hosts.yml (default: auto-discovered)
#   SSH_USER         SSH login user (default: inventory ansible_user, else root)
#   SSH_KEY          SSH private key path (default: inventory key, else
#                    ~/.ssh/id_ed25519)
#   AERON_DEPLOY_DIR aeron-deploy dir on BOTH hosts (default /opt/bench/aeron-deploy)
#   JAVA_HOME_REMOTE JAVA_HOME on BOTH hosts.   # VERIFY on first provision:
#                    default /usr/lib/jvm/java-21-openjdk-amd64 (jdk_version 21,
#                    toolchains role). If the detected path differs, override.
#   DEST_PORT        server echo endpoint port (default 13333)
#   SRC_PORT         client subscribe endpoint port (default 13334)
#   MTU              channel mtu (default 1408; matches the ad-hoc run + dist default)
#   CLIENT_DRIVERS   --client-drivers csv (default "java")
#   SERVER_DRIVERS   --server-drivers csv (default "java")
#   CONTEXT          --context label (default "uc-perlink")
#   MESSAGE_RATE     csv (default 10000)        # LoadTestRig io...message.rate
#   MESSAGE_LENGTH   csv (default 64)           # io...message.length (bytes)
#   ITERATIONS       (default 5)                # io...iterations
#   WARMUP_ITERATIONS(default 2)                # io...warmup.iterations
#   WARMUP_MESSAGE_RATE (default 10000)         # io...warmup.message.rate
#   BURST_SIZE       csv (default 1)            # io...batch.size
#   RUNS             repeats per cell (default 1)
#   OUT_DIR          local results dir (default: bench-out/aeron-echo)
#   DRY_RUN          1 to print without executing
#
# CPU-core overrides (rarely needed; defaults below assume cores 0-7):
#   {CLIENT,SERVER}_CONDUCTOR_CORE {CLIENT,SERVER}_SENDER_CORE
#   {CLIENT,SERVER}_RECEIVER_CORE CLIENT_RIG_CORE SERVER_ECHO_CORE
#   {CLIENT,SERVER}_NONISO_CORES {CLIENT,SERVER}_CPU_NODE_OVR

set -euo pipefail

# ---------------------------------------------------------------------------
# Dry-run flag: accept --dry-run CLI arg OR DRY_RUN env var.
# ---------------------------------------------------------------------------
DRY_RUN="${DRY_RUN:-0}"
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    DRY_RUN=1
  fi
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Inventory resolution (mirrors netping-sweep.sh).
# ---------------------------------------------------------------------------
DEFAULT_INVENTORY="${REPO_ROOT}/bench-infra/inventory/hosts.yml"
INVENTORY="${INVENTORY:-$DEFAULT_INVENTORY}"

# parse_inventory <file>: sets NODE0_IP/NODE1_IP (public/SSH),
# NODE0_PRIVATE_IP/NODE1_PRIVATE_IP (channels), INVENTORY_SSH_USER/KEY.
parse_inventory() {
  local inv="$1"
  # Map node_role -> ansible_host/private_ip by host BLOCK, robust to field
  # order within a block. The old `grep -A5 node_role` read fields from the
  # NEXT block (the generator emits ansible_host/private_ip BEFORE node_role),
  # so node0 got node1's IP and node1 came up empty — and the empty grep tripped
  # set -e/pipefail, killing the script with zero output. (Same fix as
  # netping-sweep.sh.) The trailing `nodeN: {}` group sections carry no fields.
  NODE0_IP="" NODE1_IP="" NODE0_PRIVATE_IP="" NODE1_PRIVATE_IP=""
  eval "$(awk '
    /^        [^ ][^:]*:/ { host=$1; sub(/:$/,"",host) }
    /ansible_host:/        { ah[host]=$2 }
    /private_ip:/          { pip[host]=$2 }
    /node_role:/           { role[host]=$2 }
    END {
      for (h in role) {
        if (role[h]=="node0") { print "NODE0_IP=" ah[h]; print "NODE0_PRIVATE_IP=" pip[h] }
        if (role[h]=="node1") { print "NODE1_IP=" ah[h]; print "NODE1_PRIVATE_IP=" pip[h] }
      }
    }' "$inv")"
  INVENTORY_SSH_USER="$(grep 'ansible_user:' "$inv" | head -1 | awk '{print $2}')"
  INVENTORY_SSH_KEY="$(grep 'ansible_ssh_private_key_file:' "$inv" | head -1 | awk '{print $2}')"
}

if [[ "$DRY_RUN" -eq 1 && ! -f "$INVENTORY" ]]; then
  echo "[dry-run] Inventory not found at $INVENTORY — using FAKE addresses for expansion." >&2
  NODE0_IP="203.0.113.10"
  NODE1_IP="203.0.113.11"
  NODE0_PRIVATE_IP="10.10.1.10"
  NODE1_PRIVATE_IP="10.10.1.11"
  INVENTORY_SSH_USER="root"
  INVENTORY_SSH_KEY="/home/claude/.ssh/id_ed25519"
else
  if [[ ! -f "$INVENTORY" ]]; then
    echo "ERROR: inventory not found at $INVENTORY" >&2
    echo "  Run: make -C bench-infra up-fanout FANOUT_INSTANCE_TYPE=ccx33" >&2
    echo "  (the Aeron baseline needs >=4 isolated cores/host — use ccx33, not ccx13)" >&2
    echo "  Or set INVENTORY=<path/to/hosts.yml>" >&2
    exit 1
  fi
  parse_inventory "$INVENTORY"
fi

# Validate node0 (client/leader) + node1 (server/follower) public IPs.
if [[ -z "${NODE0_IP:-}" || -z "${NODE1_IP:-}" ]]; then
  echo "ERROR: could not parse node0/node1 public IPs from inventory at $INVENTORY" >&2
  echo "  Expected YAML fields: node_role: node0 / node1, ansible_host: <ip>" >&2
  exit 1
fi

# Channels use the PRIVATE inter-node IP (realistic link, e.g. enp7s0 on
# Hetzner); fall back to public with a warning.
if [[ -n "${NODE0_PRIVATE_IP:-}" ]]; then
  CLIENT_CONNECT_IP="$NODE0_PRIVATE_IP"
else
  echo "[warn] no private_ip for node0 — using public ansible_host for channels" >&2
  CLIENT_CONNECT_IP="$NODE0_IP"
fi
if [[ -n "${NODE1_PRIVATE_IP:-}" ]]; then
  SERVER_CONNECT_IP="$NODE1_PRIVATE_IP"
else
  echo "[warn] no private_ip for node1 — using public ansible_host for channels" >&2
  SERVER_CONNECT_IP="$NODE1_IP"
fi

# ---------------------------------------------------------------------------
# SSH credentials: explicit env wins, else inventory, else sane fleet default.
# ---------------------------------------------------------------------------
SSH_USER="${SSH_USER:-${INVENTORY_SSH_USER:-root}}"
SSH_KEY="${SSH_KEY:-${INVENTORY_SSH_KEY:-/home/claude/.ssh/id_ed25519}}"

# ---------------------------------------------------------------------------
# Dist layout + JVM (match group_vars/all.yml + build_aeron / toolchains roles).
# ---------------------------------------------------------------------------
AERON_DEPLOY_DIR="${AERON_DEPLOY_DIR:-/opt/bench/aeron-deploy}"
# VERIFY on first provision: the toolchains role detects JAVA_HOME via
# `dirname dirname readlink javac`; for openjdk-21 on Ubuntu this is the path
# below. If `ssh node0 'dirname $(dirname $(readlink -f $(which javac)))'`
# differs, set JAVA_HOME_REMOTE explicitly.
JAVA_HOME_REMOTE="${JAVA_HOME_REMOTE:-/usr/lib/jvm/java-21-openjdk-amd64}"
ORCHESTRATOR="${AERON_DEPLOY_DIR}/scripts/aeron/remote-echo-benchmarks"

# ---------------------------------------------------------------------------
# Channels: aeron:udp?endpoint=IP:PORT|mtu=MTU
#   DESTINATION = server endpoint (node1) — client publishes here.
#   SOURCE      = client endpoint (node0) — server echoes here.
# Both sides get the SAME pair so the drivers agree.
# ---------------------------------------------------------------------------
DEST_PORT="${DEST_PORT:-13333}"
SRC_PORT="${SRC_PORT:-13334}"
MTU="${MTU:-1408}"
DEST_CHANNEL="aeron:udp?endpoint=${SERVER_CONNECT_IP}:${DEST_PORT}|mtu=${MTU}"
SRC_CHANNEL="aeron:udp?endpoint=${CLIENT_CONNECT_IP}:${SRC_PORT}|mtu=${MTU}"

# ---------------------------------------------------------------------------
# CPU cores (ccx33: 0-7). 1/2/3 = driver conductor/sender/receiver;
# 4 = app (LoadTestRig on client / EchoNode on server); 0,5,6,7 non-isolated.
# ---------------------------------------------------------------------------
CLIENT_CONDUCTOR_CORE="${CLIENT_CONDUCTOR_CORE:-1}"
CLIENT_SENDER_CORE="${CLIENT_SENDER_CORE:-2}"
CLIENT_RECEIVER_CORE="${CLIENT_RECEIVER_CORE:-3}"
CLIENT_RIG_CORE="${CLIENT_RIG_CORE:-4}"
CLIENT_NONISO_CORES="${CLIENT_NONISO_CORES:-0,5,6,7}"
CLIENT_CPU_NODE_OVR="${CLIENT_CPU_NODE_OVR:-0}"

SERVER_CONDUCTOR_CORE="${SERVER_CONDUCTOR_CORE:-1}"
SERVER_SENDER_CORE="${SERVER_SENDER_CORE:-2}"
SERVER_RECEIVER_CORE="${SERVER_RECEIVER_CORE:-3}"
SERVER_ECHO_CORE="${SERVER_ECHO_CORE:-4}"
SERVER_NONISO_CORES="${SERVER_NONISO_CORES:-0,5,6,7}"
SERVER_CPU_NODE_OVR="${SERVER_CPU_NODE_OVR:-0}"

# ---------------------------------------------------------------------------
# Run matrix (LoadTestRig). MESSAGE_RATE/MESSAGE_LENGTH must be equal-length csv.
# ---------------------------------------------------------------------------
MESSAGE_RATE_KNOB="${MESSAGE_RATE:-10000}"
MESSAGE_LENGTH_KNOB="${MESSAGE_LENGTH:-64}"
ITERATIONS_KNOB="${ITERATIONS:-5}"
WARMUP_ITERATIONS_KNOB="${WARMUP_ITERATIONS:-2}"
WARMUP_MESSAGE_RATE_KNOB="${WARMUP_MESSAGE_RATE:-10000}"
BURST_SIZE_KNOB="${BURST_SIZE:-1}"
RUNS_KNOB="${RUNS:-1}"

CLIENT_DRIVERS="${CLIENT_DRIVERS:-java}"
SERVER_DRIVERS="${SERVER_DRIVERS:-java}"
CONTEXT="${CONTEXT:-uc-perlink}"

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/bench-out/aeron-echo}"

# ---------------------------------------------------------------------------
# Assemble the full environment for the orchestrator.
# ---------------------------------------------------------------------------
build_env() {
  # remote-echo-benchmarks required_vars (client side)
  ORCH_ENV=(
    "CLIENT_JAVA_HOME=${JAVA_HOME_REMOTE}"
    "CLIENT_BENCHMARKS_PATH=${AERON_DEPLOY_DIR}"
    "CLIENT_DRIVER_CONDUCTOR_CPU_CORE=${CLIENT_CONDUCTOR_CORE}"
    "CLIENT_DRIVER_SENDER_CPU_CORE=${CLIENT_SENDER_CORE}"
    "CLIENT_DRIVER_RECEIVER_CPU_CORE=${CLIENT_RECEIVER_CORE}"
    "CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE=${CLIENT_RIG_CORE}"
    "CLIENT_NON_ISOLATED_CPU_CORES=${CLIENT_NONISO_CORES}"
    "CLIENT_CPU_NODE=${CLIENT_CPU_NODE_OVR}"
    "CLIENT_DESTINATION_CHANNEL=${DEST_CHANNEL}"
    "CLIENT_SOURCE_CHANNEL=${SRC_CHANNEL}"
    # server side
    "SERVER_JAVA_HOME=${JAVA_HOME_REMOTE}"
    "SERVER_BENCHMARKS_PATH=${AERON_DEPLOY_DIR}"
    "SERVER_DRIVER_CONDUCTOR_CPU_CORE=${SERVER_CONDUCTOR_CORE}"
    "SERVER_DRIVER_SENDER_CPU_CORE=${SERVER_SENDER_CORE}"
    "SERVER_DRIVER_RECEIVER_CPU_CORE=${SERVER_RECEIVER_CORE}"
    "SERVER_ECHO_CPU_CORE=${SERVER_ECHO_CORE}"
    "SERVER_NON_ISOLATED_CPU_CORES=${SERVER_NONISO_CORES}"
    "SERVER_CPU_NODE=${SERVER_CPU_NODE_OVR}"
    "SERVER_DESTINATION_CHANNEL=${DEST_CHANNEL}"
    "SERVER_SOURCE_CHANNEL=${SRC_CHANNEL}"
    # DPDK driver vars: the orchestrator substitutes driver_aeron_dpdk_*_var
    # UNCONDITIONALLY for every driver (line ~273) and dereferences these under
    # `set -u`, so they must merely EXIST even when --client/server-drivers=java
    # (which contains no DPDK placeholder, so the values are never used). Set
    # them so a non-DPDK host doesn't abort with an unbound-variable error.
    "CLIENT_AERON_DPDK_GATEWAY_IPV4_ADDRESS="
    "CLIENT_AERON_DPDK_LOCAL_IPV4_ADDRESS="
    "SERVER_AERON_DPDK_GATEWAY_IPV4_ADDRESS="
    "SERVER_AERON_DPDK_LOCAL_IPV4_ADDRESS="
    # remote-benchmarks-runner required_vars (SSH targets). node0 = client,
    # node1 = server. SSH uses the PUBLIC ansible_host IP.
    # These SSH targets are used BY THE ORCHESTRATOR, which runs ON node0 — not
    # from the control box. So they must be reachable node->node: use the
    # PRIVATE IPs (allowed by the intra-cluster SG rule; the public IPs are only
    # open to the control box) and a key path that exists ON node0
    # (ORCH_REMOTE_KEY — the control box's SSH_KEY path generally does not exist
    # on the node). Overridable; default to private IPs with a public fallback.
    "SSH_CLIENT_USER=${SSH_USER}"
    "SSH_CLIENT_KEY_FILE=${ORCH_REMOTE_KEY:-${SSH_KEY}}"
    "SSH_CLIENT_NODE=${ORCH_CLIENT_NODE:-${NODE0_PRIVATE_IP:-${NODE0_IP}}}"
    "SSH_SERVER_USER=${SSH_USER}"
    "SSH_SERVER_KEY_FILE=${ORCH_REMOTE_KEY:-${SSH_KEY}}"
    "SSH_SERVER_NODE=${ORCH_SERVER_NODE:-${NODE1_PRIVATE_IP:-${NODE1_IP}}}"
    # run matrix consumed by the runner
    "RUNS=${RUNS_KNOB}"
    "ITERATIONS=${ITERATIONS_KNOB}"
    "WARMUP_ITERATIONS=${WARMUP_ITERATIONS_KNOB}"
    "WARMUP_MESSAGE_RATE=${WARMUP_MESSAGE_RATE_KNOB}"
    "MESSAGE_RATE=${MESSAGE_RATE_KNOB}"
    "MESSAGE_LENGTH=${MESSAGE_LENGTH_KNOB}"
    "BURST_SIZE=${BURST_SIZE_KNOB}"
  )
  # Orchestrator positional args.
  ORCH_ARGS=(
    --client-drivers "${CLIENT_DRIVERS}"
    --server-drivers "${SERVER_DRIVERS}"
    --mtu "${MTU}"
    --context "${CONTEXT}"
    --download-dir "${OUT_DIR}"
  )
}

build_env

print_invocation() {
  echo "# ---- Aeron remote-echo-benchmarks orchestrator invocation ----"
  echo "# control box SSHes node0 (client/leader) + node1 (server/follower)"
  echo "# client SSH target : ${SSH_USER}@${NODE0_IP}  (channels via ${CLIENT_CONNECT_IP})"
  echo "# server SSH target : ${SSH_USER}@${NODE1_IP}  (channels via ${SERVER_CONNECT_IP})"
  echo "# DESTINATION_CHANNEL=${DEST_CHANNEL}"
  echo "# SOURCE_CHANNEL     =${SRC_CHANNEL}"
  echo "env \\"
  local kv
  for kv in "${ORCH_ENV[@]}"; do
    printf "  %q \\\\\n" "${kv}"
  done
  printf "  %q \\\\\n" "${ORCHESTRATOR}"
  local a
  for a in "${ORCH_ARGS[@]}"; do
    printf "  %q" "${a}"
  done
  printf "\n"
}

mkdir -p "${OUT_DIR}"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[dry-run] not executing — printing full orchestrator invocation:" >&2
  print_invocation
  echo "" >&2
  echo "[dry-run] result tarballs would land in ${OUT_DIR}/ (scp'd by download_results)" >&2
  echo "[dry-run] HDR percentile files: aeron-echo-*_mtu=${MTU}_length=*_rate=*/run-*/  (parse p50/p99 with uc_autobench/scripts/aeron_hdr_to_csv.py)" >&2
  exit 0
fi

# ---------------------------------------------------------------------------
# Real run: orchestrator must exist on the control box's view? No — it lives on
# the dist under AERON_DEPLOY_DIR, which is on the HOSTS, not the control box.
# remote-echo-benchmarks is part of the same aeron-deploy tree; it is intended
# to be invoked from a control box that has a COPY of the scripts dir. Since the
# build_aeron role only deploys to the hosts, we run the orchestrator on node0
# (which has the dist) over SSH, exporting the env there. node0 then SSHes
# itself (as client) + node1 (as server). node0 must therefore reach node1 over
# SSH with the deployed key — see program.md "first-provision checklist".
#
# VERIFY on first provision: confirm node0 can `ssh -i <deployed key> root@node1`.
# The fleet deploys the same key to all hosts; if the PRIVATE key is not present
# on node0, either (a) push it, or (b) run the orchestrator from this control
# box directly if the aeron scripts dir is also available locally.
# ---------------------------------------------------------------------------
echo "[run] launching orchestrator on node0 (${NODE0_IP}) — it SSHes client(self)+server(node1)" >&2
echo "[run] results -> ${OUT_DIR}/" >&2

# Build a single remote command: export every env var, then exec the orchestrator.
REMOTE_CMD=""
for kv in "${ORCH_ENV[@]}"; do
  REMOTE_CMD+="export $(printf '%q' "${kv}"); "
done
REMOTE_CMD+="${ORCHESTRATOR}"
for a in "${ORCH_ARGS[@]}"; do
  REMOTE_CMD+=" $(printf '%q' "${a}")"
done

# shellcheck disable=SC2029  # intentional client-side expansion of REMOTE_CMD
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new -o BatchMode=yes \
  "${SSH_USER}@${NODE0_IP}" "${REMOTE_CMD}"

# The orchestrator writes HDR results under node0:<benchmarks_path>/scripts/results.
# Its own download step targets a control-box path that doesn't exist on node0,
# so fetch the tarball back ourselves.
echo "[fetch] pulling aeron results from node0 -> ${OUT_DIR}/" >&2
mkdir -p "${OUT_DIR}"
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new -o BatchMode=yes "${SSH_USER}@${NODE0_IP}" \
  "cd ${AERON_DEPLOY_DIR}/scripts/results 2>/dev/null && tar -czf /tmp/aeron-results.tgz . 2>/dev/null && echo ok" \
  && scp -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${NODE0_IP}:/tmp/aeron-results.tgz" "${OUT_DIR}/aeron-results.tgz" \
  && tar -xzf "${OUT_DIR}/aeron-results.tgz" -C "${OUT_DIR}" \
  && echo "[fetch] aeron results in ${OUT_DIR}/" >&2 \
  || echo "[fetch] WARN: no aeron results fetched (check node0 ${AERON_DEPLOY_DIR}/scripts/results)" >&2

echo "[run] done. HDR results under ${OUT_DIR}/ — normalize with aeron_hdr_to_csv.py" >&2
