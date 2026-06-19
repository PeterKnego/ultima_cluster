# Cross-Host Pipeline-Depth Commit A/B Run-Book

**Goal:** Measure the latency and throughput impact of pipelining (depth 8) vs. sequential
commit (depth 1) on a live 3-node UC cluster over a real cross-host network, using the
existing `bench-infra` run role and `commit-path-load` open-loop harness.

This document is the procedure for Task 7 (billable cloud run). Do **not** start it without
an explicit go-ahead — it provisions real hosts (~$2/hr on Hetzner ccx13 or ~$0.70/hr on
AWS c7i.xlarge).

---

## Background

`UC_PIPELINE_DEPTH` is read at node startup by `pipeline_depth()` in `uc_node` (Phase A).
The `run` Ansible role sets it via the `uc_pipeline_depth` group var (default 8) and passes
it to every `uc-node-launch` process:

```
setsid UC_PIPELINE_DEPTH={{ uc_pipeline_depth }} {{ uc_target_bin }}/uc-node-launch ...
```

Passing `-e uc_pipeline_depth=1` on the `ansible-playbook` (or `make`) command line
overrides the group-var default and forces sequential commits across the cluster.

---

## Prerequisites

- `bench-infra/.env` with `HCLOUD_TOKEN` (Hetzner) or `AWS_ACCESS_KEY_ID` +
  `AWS_SECRET_ACCESS_KEY` for the target cloud.
- `bench-infra/terraform.tfvars` with `ssh_private_key_file` set.
- Terraform state is empty (no fleet running). Run `make -C bench-infra destroy` if needed.
- The working tree is clean and the `uc_pipeline_depth` var is committed (this task).

---

## Pass 1 — depth=1 (sequential, baseline)

### 1a. Provision + run

```bash
# From the repo root:
make -C bench-infra up-uc FANOUT_INSTANCE_TYPE=ccx13
cd bench-infra && ansible-playbook ansible/bench.yml \
  -e aeron_enabled=false \
  -e uc_pipeline_depth=1
```

`make up-uc` does:
1. `terraform apply` — 3 hosts (node0/node1/node2)
2. `make inventory` — writes `bench-infra/inventory/hosts.yml`
3. `ansible-playbook provision.yml -e aeron_enabled=false` — OS tuning, toolchain, UC
   build (rsync from local tree), peer config (`uc-peers.env` per host)

`bench.yml` then runs the `run` role (start cluster, sweep) and the `collect` role (fetch
CSVs to `bench-out/dist/<timestamp>/`).

The UC sweep runs `commit-path-load --config dist_3node` on node0 against
`/dev/shm/uc-node0` with `--inflight {{ inflight }}` (default 128) open-loop in-flight
commits, stepping through `rate_ladder` (default 100–20 000 msgs/s).

### 1b. Save the results

```bash
cp bench-out/dist/<TIMESTAMP>/node0/uc_sweep.csv bench-out/ab-depth1/uc_sweep.csv
```

(Replace `<TIMESTAMP>` with the actual `YYYYMMDDTHHMMSSz` directory produced by the
collect role.)

---

## Pass 2 — depth=8 (pipelined)

### 2a. Reprovision with depth=8

The fleet is still up from Pass 1. Restart the cluster with the default depth:

```bash
cd bench-infra && ansible-playbook ansible/bench.yml \
  -e aeron_enabled=false \
  -e uc_pipeline_depth=8
```

This re-runs the `run` role (kills old nodes, starts fresh with depth=8) and the `collect`
role (fetches a new timestamped directory).

### 2b. Save the results

```bash
cp bench-out/dist/<TIMESTAMP2>/node0/uc_sweep.csv bench-out/ab-depth8/uc_sweep.csv
```

---

## Lagging-Follower Catch-Up Variant

This variant checks that a restarted follower reconstructs state correctly under sustained
load and catches up without stalling the leader.

Run this during Pass 2 (depth=8), after the election settles but while `commit-path-load`
is still applying load. You can trigger it manually from a third terminal:

```bash
# 1. Start the cluster (already done by bench.yml above) and confirm election:
#    SSH into node0 and tail /opt/bench/uc-node.out until you see "leader elected".

# 2. Start sustained open-loop load on node0:
SSH_KEY=$(awk -F'"' '/ssh_private_key_file/{print $2}' bench-infra/terraform.tfvars)
NODE0_IP=$(cd bench-infra && terraform -chdir=terraform output -json nodes \
  | jq -r '.[]|select(.role=="node0").public_ip')

ssh -i "$SSH_KEY" root@"$NODE0_IP" \
  "/opt/bench/uc/target/release/commit-path-load \
    --connect /dev/shm/uc-node0 --app-id uc-bench-dist \
    --config dist_3node --rates 5000 --inflight 128 \
    --payload-bytes 64 --window-secs 60 --warmup-secs 5 \
    --out /opt/bench/results/catchup_under_load.csv" &

# 3. While load is running, SSH into node1 and kill the UC node:
NODE1_IP=$(cd bench-infra && terraform -chdir=terraform output -json nodes \
  | jq -r '.[]|select(.role=="node1").public_ip')
ssh -i "$SSH_KEY" root@"$NODE1_IP" "pkill -9 -f '[u]c-node-launch'; pkill -9 -f '[k]v_service'"

# 4. Restart node1 (it will reconstruct from journal, then rejoin):
ssh -i "$SSH_KEY" root@"$NODE1_IP" bash << 'REMOTE'
  . /opt/bench/uc-peers.env
  export UC_DURABILITY=consistent
  export UC_TRANSPORT=quic
  export UC_PIPELINE_DEPTH=8
  setsid /opt/bench/uc/target/release/uc-node-launch \
    --node-id $UC_NODE_ID --listen $UC_LISTEN $UC_PEERS \
    --app-id $UC_APP_ID --with-service \
    --instance-dir /dev/shm/uc-node1 \
    --data-dir /opt/bench/uc-data \
    > /opt/bench/uc-node.out 2>&1 < /dev/null &
REMOTE

# 5. Wait for node1 to catch up (watch /opt/bench/uc-node.out on node1 for
#    "reconstruction complete" / "follower applied up to ...").
# 6. Retrieve the load-run CSV:
scp -i "$SSH_KEY" root@"$NODE0_IP":/opt/bench/results/catchup_under_load.csv \
  bench-out/ab-depth8/catchup_under_load.csv
```

Observe that:
- Commit latency on node0 does **not** spike to the quorum-loss timeout (no leader transfer
  occurred, node1 is just a non-quorum follower during the gap).
- After node1 rejoins, `commit-path-load` throughput and latency return to the pre-kill
  baseline — confirming the pipeline depth setting is consistent across the reconstructed
  follower.

---

## Comparing the Two Passes

The CSV produced by `commit-path-load` (and `run_uc_sweep.sh`) has these columns:

| Column | Meaning |
|---|---|
| `system` | Always `uc` for this harness |
| `config` | Always `dist_3node` for cross-host runs |
| `workload` | Always `kv` |
| `payload_bytes` | Payload size per command (bytes) |
| `inflight` | Open-loop in-flight window (128 by default) |
| `target_rate` | Requested msgs/s for this ladder rung |
| `achieved_rate` | Measured throughput (msgs/s) — the system's actual capacity at that rung |
| `p50_ns` | Commit-path median latency (ns): from client `submit` to `SubmitResponse` |
| `p99_ns` | 99th-percentile commit latency (ns) |
| `p99_9_ns` | 99.9th-percentile commit latency (ns) |
| `p99_99_ns` | 99.99th-percentile commit latency (ns) |
| `max_ns` | Maximum observed commit latency (ns) in the measurement window |
| `count` | Total commits measured in the window |

**Key comparisons:**

- At rates below saturation (e.g. 1 000 msg/s): compare `p50_ns` and `p99_ns` between
  depth=1 and depth=8. Pipelining should lower p50 because the leader can coalesce
  multiple in-flight AppendEntries rather than waiting for each ack before dispatching
  the next.
- At saturation: compare `achieved_rate` — depth=8 should push more throughput before
  the queue backs up.
- At high inflight under high rate: watch `p99_9_ns` and `p99_99_ns` for tail inflation
  (depth=8 adds pipeline latency to the tail in the congested regime).

A quick shell diff:

```bash
paste -d, \
  <(awk -F, 'NR>1{print $7,$8,$9}' bench-out/ab-depth1/uc_sweep.csv) \
  <(awk -F, 'NR>1{print $7,$8,$9}' bench-out/ab-depth8/uc_sweep.csv) \
  | column -t -s,
# columns: depth1_achieved  depth1_p50  depth1_p99  depth8_achieved  depth8_p50  depth8_p99
```

---

## Tear Down

Always destroy the fleet when done to avoid ongoing charges:

```bash
make -C bench-infra destroy
```

---

## Notes

- The cross-host run uses the `dist_3node` `--config` flag in `commit-path-load`. This
  config exercises the full 3-node quorum path (leader → 2 followers → ack), unlike the
  loopback variants which run against a single-node cluster.
- The `run` role kills and restarts UC nodes on every `bench.yml` invocation, so the
  cluster starts with a clean election on each pass — no state leaks between depth=1
  and depth=8 runs.
- If the fleet is idle between passes, run `make -C bench-infra status` to check hosts are
  still reachable before starting Pass 2.
- For AWS, set `FANOUT_INSTANCE_TYPE=c7i.xlarge` (or larger) and ensure `netem_iface` is
  overridden to the correct NIC (e.g., `enp39s0` on c7i; check with `ip link` on the host).
