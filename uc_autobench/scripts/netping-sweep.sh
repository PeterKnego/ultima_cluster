#!/usr/bin/env bash
# netping-sweep.sh — cross-host RTT experiment driver (control-side).
#
# Two experiment modes controlled by EXPERIMENT (default: ping):
#
#   ping    (2-node fleet): node1 (client) → node0 (server). Reads node0 +
#           node1 from inventory. Symmetric netem on node0 + node1. Sequential
#           single-inflight RTT per (transport × payload × netem) cell.
#
#   fanout  (3-node fleet): node0 (leader/client) fans out concurrent pings to
#           node1 + node2 (followers/servers). Measures K-of-N quorum latency —
#           i.e. the latency to receive K replies out of 2 followers. This
#           models the Raft commit path: QUORUM=1 (default) = faster follower
#           wins (3-node majority commit); QUORUM=2 = all-acks / slower
#           follower. Symmetric netem on all 3 nodes.
#
# For every cell of the experiment matrix (transport × payload × netem_delay ×
# netem_loss) the driver:
#   1. Applies tc-netem SYMMETRICALLY on all relevant nodes via SSH (idempotent
#      del-first). delay=D ms adds ~D to each leg (≈2D to RTT); loss=L% is
#      applied per-direction. Shaping is identical across all transports so
#      the A/B comparison is apples-to-apples. Applied to NETEM_IFACE, which is
#      auto-detected per cloud (the iface owning the private inter-node IP —
#      enp7s0 on Hetzner, ens5 on AWS) unless explicitly overridden.
#   2. SSHs the client node to run the internode-rpc-bench binary:
#      - ping:   node1 → node0's PRIVATE IP (NODE0_CONNECT).
#      - fanout: node0 → node1's + node2's PRIVATE IPs (comma-separated
#                --connect), with --mode fanout --quorum ${QUORUM:-1}.
#      SSH itself still uses the public ansible_host IPs.
#   3. Captures the CSV row from stdout; the `system` column distinguishes
#      ping rows (e.g. `udp-ping`) from fanout rows (e.g. `udp-fanout`).
#   4. Appends a TSV row (with netem columns prepended) to results.tsv.
#   5. Removes netem from all shaped nodes (also in a cleanup trap).
#
# DRY_RUN=1 (or --dry-run) prints every command fully-expanded without
# executing any SSH — the primary local validation path.
#
# Env knobs (all optional):
#   EXPERIMENT       ping|fanout (default: ping)
#   QUORUM           K in K-of-N for fanout mode (default: 1; models 3-node
#                    Raft majority commit — faster follower wins; use 2 for
#                    all-acks / slower-follower model)
#   INVENTORY        path to the Ansible hosts.yml (default: auto-discovered)
#   TRANSPORTS       space-separated list: "udp quic aeron" (default)
#   PAYLOADS         space-separated payload sizes in bytes (default: "64 1024")
#   MODE             ping|ladder (default: ping)
#   DURATION         measurement window in seconds (default: 10)
#   RATE             open-loop rate for ladder mode, RPCs/s (default: 20000)
#   INFLIGHT         inflight cap for ladder mode (default: 128)
#   NETEM_DELAYS     space-separated one-way delay values in ms (default: "0 1 5")
#   NETEM_LOSS       space-separated loss values in pct (default: "0 1")
#   NETEM_IFACE      NIC to shape on all nodes. Default: auto-detected as the
#                    iface owning the private inter-node IP (enp7s0 on Hetzner,
#                    ens5 on AWS); set explicitly to override.
#   SSH_USER         SSH login user (default: read from inventory ansible_user)
#   SSH_KEY          path to SSH private key (default: read from inventory)
#   SSH_OPTS         extra SSH options (default: -o StrictHostKeyChecking=accept-new)
#   UC_TARGET_BIN    directory of the internode-rpc-bench binary on the hosts
#                    (default: /opt/bench/uc/target/release — matches group_vars)
#   AERON_PING_CMD   full path to the Aeron ping/client launcher on node1
#                    (default: /opt/bench/aeron-deploy/scripts/aeron/ping)
#                    *** VERIFY against the built dist on first provision ***
#   AERON_DEPLOY_DIR Aeron deploy dir on node0 (default: /opt/bench/aeron-deploy)
#   OUT_DIR          local results directory (default: uc_autobench/tasks/netping)
#   DRY_RUN          set to 1 to print commands without executing

set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

# ---------------------------------------------------------------------------
# Dry-run flag: accept --dry-run CLI arg OR DRY_RUN env var.
# ---------------------------------------------------------------------------
DRY_RUN="${DRY_RUN:-0}"
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    DRY_RUN=1
  fi
done

# ---------------------------------------------------------------------------
# Experiment mode
# ---------------------------------------------------------------------------
EXPERIMENT="${EXPERIMENT:-ping}"
QUORUM="${QUORUM:-1}"

# ---------------------------------------------------------------------------
# Inventory resolution
# ---------------------------------------------------------------------------
# Default: bench-infra/ansible/inventory/hosts.yml (generated by
# terraform_to_inventory.sh).  Override with INVENTORY=<path>.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEFAULT_INVENTORY="${REPO_ROOT}/bench-infra/inventory/hosts.yml"
INVENTORY="${INVENTORY:-$DEFAULT_INVENTORY}"

# parse_inventory <file>
# Parses ansible_host (public), private_ip (private), ansible_user, and key
# from hosts.yml.  Sets NODE0_IP, NODE1_IP, NODE2_IP (public/SSH),
# NODE0_PRIVATE_IP, NODE1_PRIVATE_IP, NODE2_PRIVATE_IP (private; empty if not
# present), INVENTORY_SSH_USER, INVENTORY_SSH_KEY.
parse_inventory() {
  local inv="$1"
  # Map node_role -> ansible_host/private_ip by host BLOCK, robust to field
  # order within a block. (The old `grep -A5 node_role` read fields from the
  # NEXT block, since the generator emits ansible_host/private_ip BEFORE
  # node_role — node0 picked up node1's IP and node1 came up empty, killing the
  # script under `set -e`/pipefail.) Host entries are 8-space-indented names;
  # the trailing `nodeN: {}` group sections carry no fields, so they don't
  # clobber. Defaults keep `set -u` happy when a role is absent (2-node fleet).
  NODE0_IP="" NODE1_IP="" NODE2_IP=""
  NODE0_PRIVATE_IP="" NODE1_PRIVATE_IP="" NODE2_PRIVATE_IP=""
  eval "$(awk '
    /^        [^ ][^:]*:/ { host=$1; sub(/:$/,"",host) }
    /ansible_host:/        { ah[host]=$2 }
    /private_ip:/          { pip[host]=$2 }
    /node_role:/           { role[host]=$2 }
    END {
      for (h in role) {
        if (role[h]=="node0") { print "NODE0_IP=" ah[h]; print "NODE0_PRIVATE_IP=" pip[h] }
        if (role[h]=="node1") { print "NODE1_IP=" ah[h]; print "NODE1_PRIVATE_IP=" pip[h] }
        if (role[h]=="node2") { print "NODE2_IP=" ah[h]; print "NODE2_PRIVATE_IP=" pip[h] }
      }
    }' "$inv")"
  INVENTORY_SSH_USER="$(grep 'ansible_user:' "$inv" | head -1 | awk '{print $2}')"
  INVENTORY_SSH_KEY="$(grep 'ansible_ssh_private_key_file:' "$inv" | head -1 | awk '{print $2}')"
}

if [[ "$DRY_RUN" -eq 1 ]]; then
  # Dry-run: use a synthetic inventory if the real one is absent.
  if [[ ! -f "$INVENTORY" ]]; then
    echo "[dry-run] Inventory not found at $INVENTORY — using FAKE addresses for expansion." >&2
    NODE0_IP="203.0.113.10"
    NODE1_IP="203.0.113.11"
    NODE2_IP="203.0.113.12"
    NODE0_PRIVATE_IP="10.10.1.10"
    NODE1_PRIVATE_IP="10.10.1.11"
    NODE2_PRIVATE_IP="10.10.1.12"
    INVENTORY_SSH_USER="bench"
    INVENTORY_SSH_KEY="/dev/null"
  else
    # Parse the real inventory even in dry-run.
    parse_inventory "$INVENTORY"
  fi
else
  # Real run: inventory MUST exist (fleet not up → clear error).
  if [[ ! -f "$INVENTORY" ]]; then
    echo "ERROR: inventory not found at $INVENTORY" >&2
    if [[ "$EXPERIMENT" == "fanout" ]]; then
      echo "  Run: make -C bench-infra up-fanout  (provisions 3-node fleet + persistent responders)" >&2
    else
      echo "  Run: make -C bench-infra up-ping  (provisions fleet + persistent responders)" >&2
    fi
    echo "  Or set INVENTORY=<path/to/hosts.yml>" >&2
    exit 1
  fi
  parse_inventory "$INVENTORY"
fi

# Validate we got both node0+node1 public IPs (required for both modes).
if [[ -z "${NODE0_IP:-}" || -z "${NODE1_IP:-}" ]]; then
  echo "ERROR: could not parse node0/node1 IPs from inventory at $INVENTORY" >&2
  echo "  Expected YAML fields: node_role: node0 / node1, ansible_host: <ip>" >&2
  exit 1
fi

# fanout mode also requires node2.
if [[ "$EXPERIMENT" == "fanout" ]]; then
  if [[ -z "${NODE2_IP:-}" ]]; then
    echo "ERROR: EXPERIMENT=fanout requires node2 in inventory — provision with 'make up-fanout'" >&2
    exit 1
  fi
fi

# NODE0_CONNECT / NODE1_CONNECT / NODE2_CONNECT: the address clients use for
# --connect. Use private_ip (the realistic inter-node path, e.g. enp7s0 on
# Hetzner) when available; fall back to ansible_host (public) with a warning.
if [[ -n "${NODE0_PRIVATE_IP:-}" ]]; then
  NODE0_CONNECT="$NODE0_PRIVATE_IP"
else
  echo "[warn] no private_ip for node0 in inventory — using public ansible_host for --connect" >&2
  NODE0_CONNECT="$NODE0_IP"
fi
if [[ -n "${NODE1_PRIVATE_IP:-}" ]]; then
  NODE1_CONNECT="$NODE1_PRIVATE_IP"
else
  echo "[warn] no private_ip for node1 in inventory — using public ansible_host for --connect" >&2
  NODE1_CONNECT="$NODE1_IP"
fi
if [[ -n "${NODE2_PRIVATE_IP:-}" ]]; then
  NODE2_CONNECT="$NODE2_PRIVATE_IP"
else
  # node2 may be absent for 2-node ping experiment — only warn in fanout mode.
  if [[ "$EXPERIMENT" == "fanout" ]]; then
    echo "[warn] no private_ip for node2 in inventory — using public ansible_host for --connect" >&2
  fi
  NODE2_CONNECT="${NODE2_IP:-}"
fi

# ---------------------------------------------------------------------------
# SSH credentials: prefer explicit env overrides, fall back to inventory.
# ---------------------------------------------------------------------------
SSH_USER="${SSH_USER:-${INVENTORY_SSH_USER:-bench}}"
SSH_KEY="${SSH_KEY:-${INVENTORY_SSH_KEY:-}}"
# SSH_ARGS is a bash array of ssh flags (no user/host — those are added at
# call-sites).  We use an array so shellcheck is satisfied and we get correct
# word-splitting on paths with spaces.
SSH_ARGS=( -o StrictHostKeyChecking=accept-new -o BatchMode=yes )
if [[ -n "${SSH_KEY:-}" ]]; then
  SSH_ARGS+=( -i "${SSH_KEY}" )
fi
# ssh_run <host> <remote-cmd>  — runs one SSH command (or prints it in dry-run).
# Declared here so netem_apply / run_* functions can use it directly.
ssh_run() {
  local host="$1" cmd="$2"
  # SC2029: command string is intentionally expanded on the client side so that
  # variables (UC_TARGET_BIN, port, etc.) are resolved before SSH transmission.
  # shellcheck disable=SC2029
  ssh "${SSH_ARGS[@]}" "${SSH_USER}@${host}" "${cmd}"
}

# ---------------------------------------------------------------------------
# Experiment matrix knobs
# ---------------------------------------------------------------------------
TRANSPORTS="${TRANSPORTS:-udp quic aeron}"
PAYLOADS="${PAYLOADS:-64 1024}"
MODE="${MODE:-ping}"
DURATION="${DURATION:-10}"
RATE="${RATE:-20000}"
INFLIGHT="${INFLIGHT:-128}"
NETEM_DELAYS="${NETEM_DELAYS:-0 1 5}"    # one-way delay ms; 0 = no shaping
NETEM_LOSS="${NETEM_LOSS:-0 1}"          # packet loss pct; 0 = no shaping
# NIC to shape with netem on every node in the experiment.  Cloud-agnostic:
# an explicit NETEM_IFACE wins; otherwise we auto-detect the iface that OWNS the
# private inter-node IP (enp7s0 on Hetzner, ens5 on AWS Nitro, eth0 elsewhere).
# Detection SSHes node0 — all nodes share one image, so one answer applies to
# all.  In dry-run (no SSH) we fall back to the Hetzner default.
detect_netem_iface() {
  local priv="${NODE0_PRIVATE_IP:-}"
  [[ -z "$priv" ]] && return 1
  # `ip -o -4 addr show` field $4 is "<addr>/<prefix>"; match the one starting
  # with the private IP and print its iface (field $2).  index()==1 avoids
  # treating the dots in the IP as regex wildcards.
  ssh_run "${NODE0_IP}" \
    "ip -o -4 addr show | awk -v ip='${priv}' 'index(\$4, ip\"/\")==1 {print \$2; exit}'" 2>/dev/null
}
if [[ -n "${NETEM_IFACE:-}" ]]; then
  :  # explicit override — trust it as-is
elif [[ "$DRY_RUN" -eq 1 ]]; then
  NETEM_IFACE="enp7s0"  # cannot SSH in dry-run; assume Hetzner default
else
  NETEM_IFACE="$(detect_netem_iface || true)"
  if [[ -z "${NETEM_IFACE:-}" ]]; then
    echo "[warn] could not auto-detect netem iface from node0 (private IP ${NODE0_PRIVATE_IP:-<none>}); falling back to enp7s0 — override with NETEM_IFACE=<iface>" >&2
    NETEM_IFACE="enp7s0"
  else
    echo "[info] auto-detected NETEM_IFACE=${NETEM_IFACE} (owns private IP ${NODE0_PRIVATE_IP})" >&2
  fi
fi

# ---------------------------------------------------------------------------
# Remote binary paths (match group_vars/all.yml defaults)
# ---------------------------------------------------------------------------
UC_TARGET_BIN="${UC_TARGET_BIN:-/opt/bench/uc/target/release}"
# Aeron ping client launcher on node1.
# *** VERIFY on first provision: list /opt/bench/aeron-deploy/scripts/aeron/ and
#     confirm the client launcher name.  Common names: 'ping', 'client',
#     'cluster-client', 'remote'.  Adjust AERON_PING_CMD accordingly. ***
AERON_PING_CMD="${AERON_PING_CMD:-/opt/bench/aeron-deploy/scripts/aeron/ping}"
# Aeron deploy dir on node0 (for its config files).
AERON_DEPLOY_DIR="${AERON_DEPLOY_DIR:-/opt/bench/aeron-deploy}"

# ---------------------------------------------------------------------------
# Port constants (match group_vars/all.yml: netping_udp_port / netping_quic_port)
# ---------------------------------------------------------------------------
UDP_PORT=9100
QUIC_PORT=9101

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/uc_autobench/tasks/netping}"
RESULTS_TSV="${OUT_DIR}/results.tsv"
TSV_HEADER="netem_delay_ms	netem_loss_pct	system	config	workload	payload_bytes	inflight	target_rate	achieved_rate	p50_ns	p99_ns	p99_9_ns	p99_99_ns	max_ns	count"
mkdir -p "$OUT_DIR"
# Write header if file does not exist or is empty.
if [[ ! -s "$RESULTS_TSV" ]]; then
  printf '%s\n' "$TSV_HEADER" > "$RESULTS_TSV"
fi

# ---------------------------------------------------------------------------
# netem helpers
# ---------------------------------------------------------------------------
# netem_apply_one <host> <delay_ms> <loss_pct>
# Idempotent on a single host: del-first (|| true), then add only when needed.
# (Private helper; call netem_apply to shape all relevant hosts symmetrically.)
netem_apply_one() {
  local host="$1" delay="$2" loss="$3"
  # Always del first (idempotent; ignore if there's no existing qdisc).
  local del_cmd="sudo tc qdisc del dev ${NETEM_IFACE} root 2>/dev/null || true"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] SSH ${SSH_USER}@${host}: ${del_cmd}" >&2
  else
    ssh_run "${host}" "${del_cmd}"
  fi

  if [[ "$delay" -gt 0 || "$loss" -gt 0 ]]; then
    # Build netem params: delay and/or loss.
    local netem_args=""
    [[ "$delay" -gt 0 ]] && netem_args+=" delay ${delay}ms"
    [[ "$loss"  -gt 0 ]] && netem_args+=" loss ${loss}%"
    local add_cmd="sudo tc qdisc add dev ${NETEM_IFACE} root netem${netem_args}"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "[dry-run] SSH ${SSH_USER}@${host}: ${add_cmd}" >&2
    else
      ssh_run "${host}" "${add_cmd}"
    fi
  fi
}

# netem_apply <delay_ms> <loss_pct>
# Applies netem SYMMETRICALLY on all nodes relevant to the current experiment:
#   ping:   node0 + node1
#   fanout: node0 + node1 + node2
# delay=D ms adds ~D to each leg (≈2D to RTT); loss=L% is per-direction.
# When delay==0 AND loss==0 no shaping rule is added (baseline cell).
netem_apply() {
  local delay="$1" loss="$2"
  netem_apply_one "${NODE0_IP}" "$delay" "$loss"
  netem_apply_one "${NODE1_IP}" "$delay" "$loss"
  if [[ "$EXPERIMENT" == "fanout" ]]; then
    netem_apply_one "${NODE2_IP}" "$delay" "$loss"
  fi
}

# netem_remove_one <host>
# Tears down netem on a single host (|| true — safe when no qdisc installed).
netem_remove_one() {
  local host="$1"
  local del_cmd="sudo tc qdisc del dev ${NETEM_IFACE} root 2>/dev/null || true"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] SSH ${SSH_USER}@${host} [cleanup]: ${del_cmd}" >&2
  else
    ssh_run "${host}" "${del_cmd}" || true
  fi
}

# netem_remove
# Tears down netem on all nodes relevant to the current experiment so no
# host is left shaped on crash/interrupt.
netem_remove() {
  netem_remove_one "${NODE0_IP}"
  netem_remove_one "${NODE1_IP}"
  if [[ "$EXPERIMENT" == "fanout" ]]; then
    netem_remove_one "${NODE2_IP}"
  fi
}

# ---------------------------------------------------------------------------
# Cleanup trap: always remove netem from all nodes on EXIT/INT/TERM.
# (The per-experiment finally-block also removes it, but this is the belt.)
# ---------------------------------------------------------------------------
cleanup() {
  if [[ "${_NETEM_ACTIVE:-0}" -eq 1 ]]; then
    if [[ "$EXPERIMENT" == "fanout" ]]; then
      echo "[cleanup] removing netem from node0 (${NODE0_IP}), node1 (${NODE1_IP}), node2 (${NODE2_IP})" >&2
    else
      echo "[cleanup] removing netem from node0 (${NODE0_IP}) and node1 (${NODE1_IP})" >&2
    fi
    netem_remove
    _NETEM_ACTIVE=0
  fi
}
trap 'cleanup' EXIT
trap 'cleanup; exit 130' INT TERM
_NETEM_ACTIVE=0

# ---------------------------------------------------------------------------
# Run one UC ping experiment (udp or quic) on node1 → node0.
# Returns: sets global _CSV_ROW to the raw CSV row from the binary.
# ---------------------------------------------------------------------------
run_uc_experiment() {
  local transport="$1" payload="$2" config_label="$3"
  local port
  case "$transport" in
    udp)  port="$UDP_PORT"  ;;
    quic) port="$QUIC_PORT" ;;
    *) echo "ERROR: unknown UC transport $transport" >&2; return 1 ;;
  esac

  # NODE0_CONNECT = private_ip (or public fallback) — the realistic inter-node
  # path on Hetzner.  netem shapes enp7s0, so --connect must target that iface.
  # SSH (node1 runner + node0 netem) still uses the public NODE1_IP / NODE0_IP.
  local client_cmd
  client_cmd="${UC_TARGET_BIN}/internode-rpc-bench \
    --role client \
    --transport ${transport} \
    --connect ${NODE0_CONNECT}:${port} \
    --mode ${MODE} \
    --payload ${payload} \
    --duration ${DURATION} \
    --config ${config_label}"
  # Append rate/inflight only in ladder mode (they're ignored by ping mode but
  # are still valid flags — include them always for traceability).
  client_cmd+=" --rate ${RATE} --inflight ${INFLIGHT}"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] SSH ${SSH_USER}@${NODE1_IP}: ${client_cmd}" >&2
    # Synthetic CSV row for dry-run validation.
    _CSV_ROW="${transport}-ping,${config_label},rpc-ping,${payload},1,${RATE},0,0,0,0,0,0,0"
  else
    # Capture CSV (stdout); logs go to stderr on the remote side.
    local raw
    raw="$(ssh_run "${NODE1_IP}" "${client_cmd}" 2>/dev/null)"
    # The binary always emits a header line first; take the second (data) line.
    _CSV_ROW="$(printf '%s\n' "$raw" | awk 'NR==2 && NF>0 {print; exit}')"
    if [[ -z "$_CSV_ROW" ]]; then
      echo "WARNING: no CSV row received from internode-rpc-bench (transport=${transport})" >&2
      _CSV_ROW="${transport}-ping,${config_label},rpc-ping,${payload},1,${RATE},0,0,0,0,0,0,0"
    fi
  fi
}

# ---------------------------------------------------------------------------
# Run one UC fanout experiment (udp or quic) on node0 → node1 + node2.
# node0 is the leader/client; node1 + node2 are followers/servers.
# QUORUM=K controls the K-of-N threshold:
#   K=1 (default): faster follower wins (models 3-node Raft majority commit).
#   K=2:           both followers must reply (all-acks / slower-follower model).
# Returns: sets global _CSV_ROW to the raw CSV row from the binary.
# ---------------------------------------------------------------------------
run_uc_fanout_experiment() {
  local transport="$1" payload="$2" config_label="$3"
  local port
  case "$transport" in
    udp)  port="$UDP_PORT"  ;;
    quic) port="$QUIC_PORT" ;;
    *) echo "ERROR: unknown UC transport $transport" >&2; return 1 ;;
  esac

  # --connect takes a comma-separated list of follower addresses (private IPs).
  local connect_list="${NODE1_CONNECT}:${port},${NODE2_CONNECT}:${port}"

  local client_cmd
  client_cmd="${UC_TARGET_BIN}/internode-rpc-bench \
    --role client \
    --transport ${transport} \
    --mode fanout \
    --connect ${connect_list} \
    --quorum ${QUORUM} \
    --payload ${payload} \
    --duration ${DURATION} \
    --config ${config_label}"
  client_cmd+=" --rate ${RATE} --inflight ${INFLIGHT}"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] SSH ${SSH_USER}@${NODE0_IP}: ${client_cmd}" >&2
    # Synthetic CSV row for dry-run validation.
    _CSV_ROW="${transport}-fanout,${config_label},rpc-fanout,${payload},1,${RATE},0,0,0,0,0,0,0"
  else
    local raw
    raw="$(ssh_run "${NODE0_IP}" "${client_cmd}" 2>/dev/null)"
    _CSV_ROW="$(printf '%s\n' "$raw" | awk 'NR==2 && NF>0 {print; exit}')"
    if [[ -z "$_CSV_ROW" ]]; then
      echo "WARNING: no CSV row received from internode-rpc-bench (fanout transport=${transport})" >&2
      _CSV_ROW="${transport}-fanout,${config_label},rpc-fanout,${payload},1,${RATE},0,0,0,0,0,0,0"
    fi
  fi
}

# ---------------------------------------------------------------------------
# Run one Aeron ping experiment on node1.
#
# Aeron output format is UNCERTAIN without the live aeron-io/benchmarks dist.
# The launcher (AERON_PING_CMD, default: .../scripts/aeron/ping) writes
# HDR histogram stats to stdout/a file.  The awk parser below targets the
# canonical aeron-benchmarks output format:
#   Value(ns)  Percentile  TotalCount  ...
# where lines look like:
#   12345  0.500000  100  ...
#   98765  0.990000  198  ...
#
# *** VERIFY on first provision: capture the actual Aeron ping stdout/output
#     file and adjust the awk block, field indices, and AERON_PING_CMD to
#     match the real launcher name and output location. ***
#
# If a field is unavailable, 0 is emitted and noted in a comment.
# ---------------------------------------------------------------------------
run_aeron_experiment() {
  local payload="$1" config_label="$2"
  # NODE0_CONNECT (global) = private_ip of node0 — pass to the Aeron launcher
  # when wiring in the remote address (see VERIFY comment below).

  # Aeron ping launcher typically needs the target cluster/media-driver address.
  # Adjust these JVM_OPTS if the launcher takes a properties file instead.
  # *** Verify the correct options for your aeron-io/benchmarks build ***
  local aeron_cmd
  # shellcheck disable=SC2089
  aeron_cmd="JAVA_HOME=\"\$(dirname \$(dirname \$(readlink -f \$(which javac))))\" \
    JVM_OPTS=\"-Xms16M \
    -Dio.aeron.benchmarks.message.length=${payload} \
    -Dio.aeron.benchmarks.iterations=${DURATION} \
    -Dio.aeron.benchmarks.output.directory=/tmp/aeron-netping-\$\$ \
    -Dio.aeron.benchmarks.output.file=aeron_netping\" \
    ${AERON_PING_CMD}"
  # *** VERIFY: some launchers accept the remote address as an arg or via a
  #     properties file (e.g. ${AERON_DEPLOY_DIR}/cfg/ping.properties).
  #     Use ${NODE0_CONNECT} (private ip) as the remote node0 address. ***

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] SSH ${SSH_USER}@${NODE1_IP}: ${aeron_cmd}" >&2
    # Synthetic normalized row for dry-run.
    _CSV_ROW="aeron-ping,${config_label},rpc-ping,${payload},1,0,0,0,0,0,0,0,0"
    return
  fi

  # --- Run the launcher; capture stdout (HDR histogram) ---
  local raw_output
  raw_output="$(ssh_run "${NODE1_IP}" "${aeron_cmd}" 2>/dev/null)" || true

  # --- Parse HDR histogram output into p50/p99/p99.9/max/count (ns) ---
  # *** VERIFY: adjust field indices against the real aeron-benchmarks output ***
  # Expected canonical format (per hdrhistogram report):
  #   #[Mean    =     1234, StdDeviation   =      56]
  #   #[Max     =     9876, Total count    =    1000]
  #   #[Buckets =      ..., SubBuckets     =      ..]
  #        Value  Percentile  TotalCount  1/(1-Percentile)
  #         1234  0.500000         500     2.00
  #         5678  0.990000         990   100.00
  #         8901  0.999000         999  1000.00
  #         9876  0.999900         ...
  #         9876  1.000000        1000     Infinity
  # Field 1 = value (ns), field 2 = percentile, field 3 = total count.
  # Last "1.0" line gives max.  Count from "[Max ... Total count = N]" line.
  local p50=0 p99=0 p999=0 p9999=0 maxv=0 count=0 achieved=0
  if [[ -n "$raw_output" ]]; then
    p50="$(printf '%s\n' "$raw_output"   | awk '$2~/^0\.5/{v=$1} END{print (v+0)}')"
    p99="$(printf '%s\n' "$raw_output"   | awk '$2~/^0\.99/{v=$1} END{print (v+0)}')"
    p999="$(printf '%s\n' "$raw_output"  | awk '$2~/^0\.999[^9]/{v=$1} END{print (v+0)}')"
    p9999="$(printf '%s\n' "$raw_output" | awk '$2~/^0\.9999/{v=$1} END{print (v+0)}')"
    maxv="$(printf '%s\n' "$raw_output"  | awk '/\[Max/{match($0,/Max[[:space:]]*=[[:space:]]*([0-9]+)/,a); if(a[1]>0) print a[1]+0}' | tail -1)"
    count="$(printf '%s\n' "$raw_output" | awk '/Total count/{match($0,/Total count[[:space:]]*=[[:space:]]*([0-9]+)/,a); if(a[1]>0) print a[1]+0}' | tail -1)"
    # Achieved rate: count / duration (approximate; Aeron doesn't emit this).
    if [[ -n "$count" && "$count" -gt 0 && "$DURATION" -gt 0 ]]; then
      achieved="$(awk "BEGIN{printf \"%.1f\", ${count}/${DURATION}}")"
    fi
  fi

  # Normalize: system=aeron-ping, workload=rpc-ping, inflight=1, target_rate=0
  # (Aeron ping is sequential; rate is not separately targeted).
  _CSV_ROW="aeron-ping,${config_label},rpc-ping,${payload},1,0,${achieved:-0},${p50:-0},${p99:-0},${p999:-0},${p9999:-0},${maxv:-0},${count:-0}"
}

# ---------------------------------------------------------------------------
# Append one result row to the TSV.
# csv_to_tsv <netem_delay_ms> <netem_loss_pct> <csv_row>
# Prepends the two netem columns and converts comma-delimiters to tabs.
# ---------------------------------------------------------------------------
append_tsv_row() {
  local delay="$1" loss="$2" csv_row="$3"
  local tsv_row
  tsv_row="$(printf '%s\t%s\t%s\n' "$delay" "$loss" "$csv_row" | tr ',' '\t')"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] TSV row → ${RESULTS_TSV}:" >&2
    printf '  %s\n' "$tsv_row" >&2
  else
    printf '%s\n' "$tsv_row" >> "$RESULTS_TSV"
  fi
}

# ---------------------------------------------------------------------------
# Build the netem config label suffix for the CSV `config` field.
# e.g. delay=1ms + loss=0pct → "d1ms_l0"; delay=0 + loss=0 → "baseline"
# ---------------------------------------------------------------------------
netem_label() {
  local delay="$1" loss="$2"
  if [[ "$delay" -eq 0 && "$loss" -eq 0 ]]; then
    echo "baseline"
  else
    echo "d${delay}ms_l${loss}pct"
  fi
}

# ---------------------------------------------------------------------------
# Main sweep loop
# ---------------------------------------------------------------------------
if [[ "$EXPERIMENT" == "fanout" ]]; then
  echo "=== netping-sweep [EXPERIMENT=fanout QUORUM=${QUORUM}] ===" >&2
  echo "=== node0/leader ssh=${NODE0_IP} connect=${NODE0_CONNECT} ===" >&2
  echo "=== node1/follower ssh=${NODE1_IP} connect=${NODE1_CONNECT} ===" >&2
  echo "=== node2/follower ssh=${NODE2_IP} connect=${NODE2_CONNECT} ===" >&2
  echo "    leader fans out to node1+node2; quorum=${QUORUM} (1=faster-wins, 2=all-acks)" >&2
else
  echo "=== netping-sweep [EXPERIMENT=ping] ===" >&2
  echo "=== node0 ssh=${NODE0_IP} connect=${NODE0_CONNECT} ===" >&2
  echo "=== node1 ssh=${NODE1_IP} connect=${NODE1_CONNECT} ===" >&2
fi
echo "    transports=${TRANSPORTS}" >&2
echo "    payloads=${PAYLOADS}  mode=${MODE}  duration=${DURATION}s" >&2
echo "    netem_delays=${NETEM_DELAYS}  netem_loss=${NETEM_LOSS}  iface=${NETEM_IFACE}" >&2
[[ "$DRY_RUN" -eq 1 ]] && echo "    *** DRY RUN — no SSH will be executed ***" >&2

total=0
for t in $TRANSPORTS; do
  for p in $PAYLOADS; do
    for d in $NETEM_DELAYS; do
      for l in $NETEM_LOSS; do
        total=$(( total + 1 ))
      done
    done
  done
done
current=0

for t in $TRANSPORTS; do
  for p in $PAYLOADS; do
    for d in $NETEM_DELAYS; do
      for l in $NETEM_LOSS; do
        current=$(( current + 1 ))
        label="$(netem_label "$d" "$l")"
        echo "[${current}/${total}] transport=${t} payload=${p}B netem=d${d}ms_l${l}pct config=${label}" >&2

        # --- 1. Apply netem symmetrically on all relevant nodes ---
        _NETEM_ACTIVE=1
        netem_apply "$d" "$l"

        # --- 2. Run experiment ---
        _CSV_ROW=""
        if [[ "$EXPERIMENT" == "fanout" ]]; then
          case "$t" in
            udp|quic)
              run_uc_fanout_experiment "$t" "$p" "$label"
              ;;
            aeron)
              echo "[fanout] Aeron fanout not implemented — skipping transport=${t}" >&2
              _CSV_ROW=""
              ;;
            *)
              echo "WARNING: unknown transport '${t}', skipping" >&2
              _CSV_ROW=""
              ;;
          esac
        else
          case "$t" in
            udp|quic)
              run_uc_experiment "$t" "$p" "$label"
              ;;
            aeron)
              run_aeron_experiment "$p" "$label"
              ;;
            *)
              echo "WARNING: unknown transport '${t}', skipping" >&2
              _CSV_ROW=""
              ;;
          esac
        fi

        # --- 3. Remove netem from all shaped nodes ---
        netem_remove
        _NETEM_ACTIVE=0

        # --- 4. Append result ---
        if [[ -n "${_CSV_ROW:-}" ]]; then
          append_tsv_row "$d" "$l" "$_CSV_ROW"
        fi

      done  # loss
    done    # delay
  done      # payload
done        # transport

echo "=== sweep complete: ${current} experiments ===" >&2
if [[ "$DRY_RUN" -eq 0 ]]; then echo "    results: ${RESULTS_TSV}" >&2; fi
