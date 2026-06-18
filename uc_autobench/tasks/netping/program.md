# Task: netping — cross-host transport RTT (UC UDP / QUIC / Aeron baseline)

Measures raw transport round-trip latency across bare-metal hosts in isolation
from Raft consensus — no journal, no openraft, no shared memory. Puts absolute
numbers on the "network floor" that every inter-node RPC pays, and lets us
compare the UC UDP mux, UC QUIC, and Aeron as a reference baseline on identical
hardware under identical netem conditions.

Two experiment modes controlled by `EXPERIMENT`:

| Mode     | Fleet | What it measures |
|----------|-------|-----------------|
| `ping`   | 2-node (node0=server, node1=client) | Sequential single-inflight RTT per transport |
| `fanout` | 3-node (node0=leader/client, node1+node2=followers/servers) | K-of-N quorum latency — leader fans out concurrent pings to both followers, measures time to receive K replies |

The `fanout` mode directly models the Raft commit path:
- `QUORUM=1` (default): faster follower wins — models 3-node Raft majority commit latency.
- `QUORUM=2`: both followers must reply — models all-acks or slower-follower latency.

## What this task measures

| Transport   | System label    | Workload label  | Notes                                            |
|-------------|-----------------|-----------------|--------------------------------------------------|
| UC UDP      | `udp-ping`      | `rpc-ping`      | `UdpMux` echo, sequential RTT (ping mode)        |
| UC QUIC     | `quic-ping`     | `rpc-ping`      | `quinn` echo, sequential RTT (ping mode)         |
| Aeron       | `aeron-ping`    | `rpc-ping`      | aeron-io/benchmarks echo, HDR stats (ping mode)  |
| UC UDP      | `udp-fanout`    | `rpc-fanout`    | leader→2-follower fan-out, K-of-N quorum latency |
| UC QUIC     | `quic-fanout`   | `rpc-fanout`    | leader→2-follower fan-out, K-of-N quorum latency |

Mode `ping` (default `MODE`): sequential single-inflight RTT — strictly one
request at a time, as fast as possible, for `DURATION` seconds. No open-loop
pacing; measures raw serial round-trip.

Mode `ladder`: open-loop CO-free rate sweep at `RATE` RPS with `INFLIGHT` cap.
Use `MODE=ladder` for throughput-stress RTT curves.

netem dimensions: each (transport, payload) cell is repeated across the
cartesian product of `NETEM_DELAYS` × `NETEM_LOSS` — giving RTT-vs-loss and
RTT-vs-delay curves with a single run.

## Fleet lifecycle

### 2-node ping experiment

```
make -C bench-infra up-ping         # provisions 2-host ccx13 fleet
                                     # + runs netping.yml (persistent responders on all nodes)
                                     # node0: UC-UDP :9100, UC-QUIC :9101, Aeron echo
                                     # node1: UC-UDP :9100, UC-QUIC :9101 (idle)

bash uc_autobench/scripts/netping-sweep.sh   # EXPERIMENT=ping (default)

make -C bench-infra destroy         # tear down hosts
```

### 3-node fan-out experiment

```
make -C bench-infra up-fanout       # provisions 3-host ccx13 fleet
                                     # + runs netping.yml (persistent responders on ALL nodes)
                                     # node0: leader — runs the fanout client
                                     # node1: follower — UC-UDP :9100, UC-QUIC :9101
                                     # node2: follower — UC-UDP :9100, UC-QUIC :9101

EXPERIMENT=fanout QUORUM=1 bash uc_autobench/scripts/netping-sweep.sh

make -C bench-infra destroy         # tear down hosts
```

The responders are started once by `netping.yml` (via the `netping_serve` role)
and stay up for the entire sweep. UC responders now start on **all nodes** (the
`node_role == "node0"` gate was dropped for UC — idle echo servers on the leader
are harmless). The driver does NOT re-provision between experiments.

## How to run

```bash
# Dry-run (no fleet required — validates command expansion):
DRY_RUN=1 bash uc_autobench/scripts/netping-sweep.sh

# Dry-run fanout mode:
DRY_RUN=1 EXPERIMENT=fanout QUORUM=1 bash uc_autobench/scripts/netping-sweep.sh

# Minimal ping sweep (UC transports only, baseline only):
TRANSPORTS="udp quic" NETEM_DELAYS=0 NETEM_LOSS=0 \
  bash uc_autobench/scripts/netping-sweep.sh

# Full default ping sweep (udp + quic + aeron × 64B + 1024B × delays 0/1/5ms × loss 0/1%):
bash uc_autobench/scripts/netping-sweep.sh

# Fan-out sweep (udp + quic, QUORUM=1 = faster follower / 3-node majority commit model):
EXPERIMENT=fanout QUORUM=1 TRANSPORTS="udp quic" \
  bash uc_autobench/scripts/netping-sweep.sh

# Fan-out sweep (QUORUM=2 = all-acks / slower-follower model):
EXPERIMENT=fanout QUORUM=2 TRANSPORTS="udp quic" \
  bash uc_autobench/scripts/netping-sweep.sh

# Custom matrix:
TRANSPORTS="udp quic" PAYLOADS="64 512 4096" \
  NETEM_DELAYS="0 5 20" NETEM_LOSS="0 1 5" \
  DURATION=30 \
  bash uc_autobench/scripts/netping-sweep.sh
```

All results append to `uc_autobench/tasks/netping/results.tsv`.

## Env knobs

| Variable        | Default                            | Description                         |
|-----------------|------------------------------------|-------------------------------------|
| `EXPERIMENT`    | `ping`                             | `ping` (2-node) or `fanout` (3-node leader→2-follower quorum latency) |
| `QUORUM`        | `1`                                | K in K-of-N for fanout: 1=faster-follower (Raft majority), 2=all-acks |
| `INVENTORY`     | `bench-infra/inventory/hosts.yml`  | Ansible hosts.yml from terraform    |
| `TRANSPORTS`    | `udp quic aeron`                   | Space-separated transport list      |
| `PAYLOADS`      | `64 1024`                          | Echo payload sizes (bytes)          |
| `MODE`          | `ping`                             | `ping` or `ladder`                  |
| `DURATION`      | `10`                               | Measurement window (seconds)        |
| `RATE`          | `20000`                            | Open-loop rate for ladder mode      |
| `INFLIGHT`      | `128`                              | Inflight cap for ladder mode        |
| `NETEM_DELAYS`  | `0 1 5`                            | One-way delay values (ms)           |
| `NETEM_LOSS`    | `0 1`                              | Packet loss values (pct)            |
| `NETEM_IFACE`   | _auto-detected_                    | NIC to shape on all nodes. Auto-detected as the iface owning the private inter-node IP (`enp7s0` on Hetzner, `ens5` on AWS); set explicitly to override |
| `SSH_USER`      | from inventory `ansible_user`      | SSH login user                      |
| `SSH_KEY`       | from inventory key file            | SSH private key path                |
| `UC_TARGET_BIN` | `/opt/bench/uc/target/release`     | Binary dir on both hosts            |
| `AERON_PING_CMD`| `/opt/bench/aeron-deploy/scripts/aeron/ping` | Aeron client launcher on node1 |
| `DRY_RUN`       | `0`                                | Set to `1` to print without SSH     |

## TSV schema

`uc_autobench/tasks/netping/results.tsv`, tab-separated, 15 columns:

```
netem_delay_ms  netem_loss_pct  system  config  workload  payload_bytes  inflight
target_rate  achieved_rate  p50_ns  p99_ns  p99_9_ns  p99_99_ns  max_ns  count
```

The `config` column encodes the netem condition as a label:
- `baseline` — no shaping (delay=0, loss=0)
- `d1ms_l0pct` — 1ms one-way delay, no loss
- `d5ms_l1pct` — 5ms delay, 1% loss
- etc.

The first two columns (`netem_delay_ms`, `netem_loss_pct`) repeat the netem
values in numeric form for easy pandas/ggplot pivoting.

The `system` column distinguishes ping rows (`udp-ping`, `quic-ping`,
`aeron-ping`) from fanout rows (`udp-fanout`, `quic-fanout`). Filter on
`system` to separate experiment modes in analysis.

UC columns map 1-to-1 from `internode-rpc-bench`'s 13-col CSV output.
Aeron columns are normalized into the same schema (unknown fields → 0).

## Aeron parser caveat

The Aeron client launcher path (`AERON_PING_CMD`) and its output format
**must be verified on first provision**.  The parser in `netping-sweep.sh`
targets the canonical `hdrhistogram` text format emitted by
`aeron-io/benchmarks` tools.  If the real output differs:

1. SSH to node1 and run the launcher manually against node0 to capture output.
2. Adjust the `awk` parser block in `netping-sweep.sh` (clearly marked with
   `*** VERIFY ***` comments) to match the real field layout.
3. Update `AERON_PING_CMD` (default: `.../scripts/aeron/ping`) if the script
   name differs — common alternatives: `client`, `cluster-client`, `remote`.

The `netping_serve` role on node0 also parameterizes `aeron_echo_launcher`
(group_vars, default: `echo-server`).  Confirm both the server-side and
client-side launcher names match the built `aeron-io/benchmarks` dist.

Aeron fanout is not implemented (the `aeron-io/benchmarks` echo launcher is
designed for sequential ping, not fan-out). Aeron transport is silently skipped
when `EXPERIMENT=fanout`.

## Aeron per-link RTT floor (canonical orchestrator)

`uc_autobench/scripts/aeron-echo-baseline.sh` wires the **canonical
aeron-io/benchmarks orchestrator** (`remote-echo-benchmarks`, built into
`/opt/bench/aeron-deploy/` by the `build_aeron` role, ref pinned by
`aeron_benchmarks_ref` in `group_vars/all.yml`) to produce a turnkey
point-to-point Aeron echo RTT floor over the same node0<->node1 link.

This replaces the ad-hoc `echo-server` + `echo-client` launch that FAILED
cross-host (`awaitConnected` 60s timeout) even with channels + all LoadTestRig
params set: the orchestrator manages the media-driver lifecycle, channels, CPU
pinning, SSH, and result collection, which the ad-hoc launch did not.

### How to run

```bash
make -C bench-infra up-fanout FANOUT_INSTANCE_TYPE=ccx33   # 3x ccx33 (8 vCPU)
bash uc_autobench/scripts/aeron-echo-baseline.sh           # control-side driver
# inspect: DRY_RUN=1 bash uc_autobench/scripts/aeron-echo-baseline.sh
```

It resolves node0 = client/leader and node1 = server/follower from
`bench-infra/inventory/hosts.yml` (public `ansible_host` for SSH, `private_ip`
for the Aeron channels), exports every orchestrator env var, and runs the
orchestrator. HDR result tarballs land in `bench-out/aeron-echo/`; normalize
p50/p99 with `uc_autobench/scripts/aeron_hdr_to_csv.py`.

### Why ccx33 (>=4 isolated cores)

A FAIR Aeron baseline pins the media-driver **conductor / sender / receiver**
threads (3 busy-spin cores) plus the **app thread** (LoadTestRig on the client,
EchoNode on the server) = 4 isolated cores/host. The script assigns cores
1/2/3 to the driver threads and core 4 to the app; cores 0,5,6,7 stay
non-isolated for the JVM/numactl/GC/OS. `CPU_NODE=0` (single NUMA node on
ccx33). ccx13 (2 vCPU) is too small — provision ccx33.

### PER-LINK, not a quorum number

Aeron echo is point-to-point (one client, one server). This is a per-link RTT
floor, NOT a Raft commit/quorum latency. For the K-of-N quorum model use the UC
`EXPERIMENT=fanout` path above.

### Upstream contract confirmed (aeron-io/benchmarks @ master, 2026-06-18)

- **SSH / invocation model**: `remote-echo-benchmarks` sources
  `remote-benchmarks-helper`, which sources `../remote-benchmarks-runner`. The
  orchestrator runs from a **control box** and SSHes BOTH hosts itself via
  `execute_remote_command` (`ssh -i $SSH_*_KEY_FILE $SSH_*_USER@$SSH_*_NODE`).
  It starts the server (EchoNode) then the client (LoadTestRig), each behind its
  own media driver, pins threads with `taskset`, then `scp`s an HDR results
  tarball back to `--download-dir`. **There is NO client<->server SSH
  requirement** — only control->each-host.
- **Required env** (the script's own `required_vars` array): the 20
  `CLIENT_*`/`SERVER_*` vars (JAVA_HOME, BENCHMARKS_PATH, the 3 driver cores,
  the app core [`LOAD_TEST_RIG_MAIN`/`ECHO`], NON_ISOLATED_CPU_CORES, CPU_NODE,
  DESTINATION_CHANNEL, SOURCE_CHANNEL). The sourced runner adds 6 more it
  validates itself: `SSH_{CLIENT,SERVER}_{USER,KEY_FILE,NODE}`.
- **Channels**: `DESTINATION_CHANNEL` = where the client publishes / the server
  subscribes (the SERVER's endpoint, node1). `SOURCE_CHANNEL` = where the server
  echoes / the client subscribes (the CLIENT's endpoint, node0). Form
  `aeron:udp?endpoint=IP:PORT|mtu=MTU`.
- **Run matrix** (runner env): `RUNS`, `ITERATIONS`, `WARMUP_ITERATIONS`,
  `WARMUP_MESSAGE_RATE`, `MESSAGE_RATE` (csv), `MESSAGE_LENGTH` (csv, MUST be the
  same length as MESSAGE_RATE — runner asserts), `BURST_SIZE` (csv = batch.size).
- **Invocation args**: `remote-echo-benchmarks --client-drivers <csv>
  --server-drivers <csv> [--mtu csv] [--context label] [--download-dir dir]`.
  Supported drivers include `java` (used here), `c`, `c-ef-vi`, `c-dpdk`, etc.

### VERIFY on first provision

1. **`node0 -> node1` SSH**: the script runs the orchestrator ON node0 over SSH
   (node0 has the dist), and the orchestrator then SSHes node1 as the server.
   So node0 needs the deployed PRIVATE key to reach node1. The fleet deploys the
   same key to all hosts; confirm the private key is present on node0 (or push
   it / run the orchestrator from a control box that has the scripts dir
   locally). The bench key is `/home/claude/.ssh/id_ed25519`.
2. **`JAVA_HOME_REMOTE`**: defaults to `/usr/lib/jvm/java-21-openjdk-amd64`
   (jdk_version 21). Confirm with
   `ssh node0 'dirname $(dirname $(readlink -f $(which javac)))'`; override
   `JAVA_HOME_REMOTE=` if it differs.
3. **Driver list**: `--client-drivers java --server-drivers java` is the safe
   default; switch to `c` only if the C media driver was built into the dist.
4. **HDR parse schema**: confirm `aeron_hdr_to_csv.py`'s percentile-table regex
   matches the LoadTestRig HDR output layout (it was written for the cping
   classic table); adjust if the columns differ.

## Frozen paths (never edit)

- `uc_autobench/tasks/netping/results.tsv`  (append-only; owned by the driver)

## Notes

- **Responders on all nodes**: the `netping_serve` Ansible role starts UC UDP +
  QUIC echo responders on every node (not just node0). This enables fan-out
  experiments where node1 + node2 act as follower echo servers. Aeron remains
  node0-only (deferred; ccx13 too small for a fair Aeron baseline).
- The driver connects clients to nodes' **private IPs** (`private_ip` fields in
  the inventory, e.g. `10.10.1.10` on Hetzner) rather than public
  `ansible_host`. SSH for running commands and applying netem still uses public
  IPs. If `private_ip` is absent the driver falls back to `ansible_host` with
  a warning.
- **Fan-out netem**: in `EXPERIMENT=fanout`, netem is applied symmetrically on
  all three nodes (node0 + node1 + node2) on `NETEM_IFACE`. This ensures all
  legs of the fan-out are uniformly impaired, giving an apples-to-apples
  comparison with the ping experiment.
- netem is applied **symmetrically** on `NETEM_IFACE` (auto-detected per cloud).
  `delay=D ms` adds ~D to each leg (≈2D to RTT); `loss=L%` is applied
  per-direction. The baseline cell (delay=0, loss=0) applies no shaping.
- The cleanup trap ensures netem is always removed from **all** shaped nodes
  on EXIT/INT/TERM, so a failed run never leaves any host shaped.
- The driver is idempotent: re-running appends new rows; it does not overwrite.
  To reset, truncate `results.tsv` to the header row.
