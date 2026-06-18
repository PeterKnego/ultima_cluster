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
| `NETEM_IFACE`   | `enp7s0`                           | NIC to shape on all nodes (Hetzner private-network iface; override per cloud, e.g. `NETEM_IFACE=eth0`) |
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
- netem is applied **symmetrically** on `NETEM_IFACE` (default `enp7s0`).
  `delay=D ms` adds ~D to each leg (≈2D to RTT); `loss=L%` is applied
  per-direction. The baseline cell (delay=0, loss=0) applies no shaping.
- The cleanup trap ensures netem is always removed from **all** shaped nodes
  on EXIT/INT/TERM, so a failed run never leaves any host shaped.
- The driver is idempotent: re-running appends new rows; it does not overwrite.
  To reset, truncate `results.tsv` to the header row.
