# Task: netping — cross-host transport RTT (UC UDP / QUIC / Aeron baseline)

Measures raw transport round-trip latency across two bare-metal hosts in
isolation from Raft consensus — no journal, no openraft, no shared memory.
Puts absolute numbers on the "network floor" that every inter-node RPC pays,
and lets us compare the UC UDP mux, UC QUIC, and Aeron as a reference baseline
on identical hardware under identical netem conditions.

## What this task measures

| Transport   | System label  | Workload label | Notes                               |
|-------------|---------------|----------------|-------------------------------------|
| UC UDP      | `udp-ping`    | `rpc-ping`     | `UdpMux` echo, sequential RTT       |
| UC QUIC     | `quic-ping`   | `rpc-ping`     | `quinn` echo, sequential RTT        |
| Aeron       | `aeron-ping`  | `rpc-ping`     | aeron-io/benchmarks echo, HDR stats |

Mode `ping` (default): sequential single-inflight RTT — strictly one request
at a time, as fast as possible, for `DURATION` seconds.  No open-loop pacing;
measures raw serial round-trip.

Mode `ladder`: open-loop CO-free rate sweep at `RATE` RPS with `INFLIGHT` cap.
Use `MODE=ladder` for throughput-stress RTT curves.

netem dimensions: each (transport, payload) cell is repeated across the
cartesian product of `NETEM_DELAYS` × `NETEM_LOSS` — giving RTT-vs-loss and
RTT-vs-delay curves with a single run.

## Fleet lifecycle

```
make -C bench-infra up-ping       # provisions 2-host ccx13 fleet
                                   # + runs netping.yml (persistent responders)
                                   # node0: UC-UDP :9100, UC-QUIC :9101, Aeron echo
                                   # node1: runs the driver's client per-experiment

bash uc_autobench/scripts/netping-sweep.sh   # the experiment driver (this task)

make -C bench-infra destroy       # tear down hosts (responders die with hosts)
```

The responders are started once by `netping.yml` (via the `netping_serve` role)
and stay up for the entire sweep.  The driver does NOT re-provision between
experiments — it only SSHs node1 to run the client and SSHs node0 to apply /
remove netem.

## How to run

```bash
# Dry-run (no fleet required — validates command expansion):
DRY_RUN=1 bash uc_autobench/scripts/netping-sweep.sh

# Minimal sweep (UC transports only, baseline only):
TRANSPORTS="udp quic" NETEM_DELAYS=0 NETEM_LOSS=0 \
  bash uc_autobench/scripts/netping-sweep.sh

# Full default sweep (udp + quic + aeron × 64B + 1024B × delays 0/1/5ms × loss 0/1%):
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
| `INVENTORY`     | `bench-infra/inventory/hosts.yml`  | Ansible hosts.yml from terraform    |
| `TRANSPORTS`    | `udp quic aeron`                   | Space-separated transport list      |
| `PAYLOADS`      | `64 1024`                          | Echo payload sizes (bytes)          |
| `MODE`          | `ping`                             | `ping` or `ladder`                  |
| `DURATION`      | `10`                               | Measurement window (seconds)        |
| `RATE`          | `20000`                            | Open-loop rate for ladder mode      |
| `INFLIGHT`      | `128`                              | Inflight cap for ladder mode        |
| `NETEM_DELAYS`  | `0 1 5`                            | One-way delay values (ms)           |
| `NETEM_LOSS`    | `0 1`                              | Packet loss values (pct)            |
| `NETEM_IFACE`   | `eth0`                             | NIC to shape on both node0 and node1|
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
(group_vars, default: `echo`).  Confirm both the server-side and client-side
launcher names match the built `aeron-io/benchmarks` dist.

## Frozen paths (never edit)

- `uc_autobench/tasks/netping/results.tsv`  (append-only; owned by the driver)

## Notes

- netem is applied **symmetrically on both node0 and node1** (`NETEM_IFACE`,
  default eth0).  `delay=D ms` adds ~D to each leg (≈2D to RTT); `loss=L%` is
  applied per-direction.  This means "delay=5ms" produces a clean ≈10ms RTT
  increase, and the impairment is identical across all transports so the A/B
  comparison is apples-to-apples.  The baseline cell (delay=0, loss=0) applies
  no shaping on either host.
- The cleanup trap in the driver ensures netem is always removed from **both**
  node0 and node1 on EXIT/INT/TERM, so a failed run never leaves either host
  shaped.
- The driver is idempotent: re-running appends new rows; it does not overwrite.
  To reset, truncate `results.tsv` to the header row.
