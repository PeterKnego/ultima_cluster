# Design — Terraform + Ansible bench provisioning for Aeron vs ultima_cluster

**Date:** 2026-06-14
**Status:** Design approved; pending spec review → implementation plan.
**Topic:** A provisioning + configuration rig that stands up real dedicated-vCPU
servers and prepares them to run the Aeron-Cluster vs `ultima_cluster` (UC)
parity benchmark on a properly-provisioned host fleet.

## 1. Motivation

`docs/tasks/task13_aeron_vs_uc_commit_path.md` §11 established that the parity
benchmark is **runnable end-to-end but cannot produce valid numbers on the local
4-vCPU shared VM**: Aeron busy-spins every agent thread, so on too few cores the
cluster starves (load avg ~28 on 4 vCPU → a flat ~100 ms latency floor independent
of rate, durability, and heartbeat). A valid run needs a quiet machine with
**physical/dedicated cores ≥ the busy-spin thread count**, which `RUN-PARAMS.md`
already stipulates ("a quiet dedicated bench machine").

This rig automates getting there: provision dedicated-vCPU hosts, configure
toolchains/builds/configs/OS-tuning, and run + collect the sweep — reproducibly,
cheaply, and across clouds.

## 2. Goals / Non-goals

**Goals**
- One command to stand up a 3-host fleet, configure it, run the parity sweep, and
  pull results back — plus a persistent mode for iterative debugging.
- **AWS and GCP are the primary targets** — both bill **per-second (60 s minimum)**,
  which makes the ephemeral one-shot mode (`up → run → destroy`) cheap. **Hetzner is
  a secondary target** — it bills a **1-hour minimum per VPS**, so it only amortizes
  in persistent mode; wasteful for short one-shot runs. All three sit behind a common
  interface; **no bare-metal.**
- Topology **B**: one cluster node per host; client co-located with node0 (the
  leader). Real NIC between nodes (UDP for Aeron, QUIC for UC), shmem client edge
  on node0 (UC always; Aeron gated — see §9).
- UC and Aeron run on the **same 3 hosts, sequentially, never concurrently** —
  identical hardware, "change only the system."
- Low-latency OS tuning sufficient that Aeron busy-spin is not starved.
- Provenance: every result set records what produced it.

**Non-goals**
- Fixing the Aeron IPC-ingress follower-crash config bug (§11) — tracked separately;
  the rig works around it (§9).
- Failover / leader-change scenarios (steady-state on a fixed leader only).
- A general-purpose multi-cloud platform. This rig serves this benchmark.
- CI integration (the one-shot mode is CI-friendly, but wiring CI is out of scope).

## 3. Architecture — two layers, one handoff

**Terraform owns infrastructure** (hosts, private network, SSH, firewall) and emits
an inventory. **Ansible owns everything on the box** (toolchains, builds, configs,
tuning, run, collect). The handoff is a single artifact: `terraform output -json`
→ a generated Ansible inventory. **Ansible never knows which cloud it is on** — it
sees host IPs + a `node0` marker. That decoupling is what makes adding a cloud cheap.

```
local machine ──terraform apply──▶ 3 dedicated-vCPU hosts (AWS/GCP primary)
      │                                  │  public IP = SSH/control
      │  terraform output -json          │  private IP = intra-node UDP/QUIC
      ▼                                  ▼
 inventory/hosts.yml ──ansible──▶ provision (tune/build/config) → bench (run/collect)
      ▲                                                                   │
      └────────────────────── results pulled to bench-out/dist/<ts>/ ◀────┘
```

## 4. Repo layout

```
bench-infra/
  Makefile                       # orchestration entrypoints (§8)
  terraform/
    main.tf                      # selects provider via var.cloud; calls matching module
    variables.tf                 # cloud, node_count(=3), instance_type, region, ssh_public_key, ttl_hours, allow_ssh_cidr
    outputs.tf                   # nodes = [{name, role, public_ip, private_ip}] — IDENTICAL across clouds
    modules/
      aws/                       # primary: EC2 + VPC + SG + placement group (per-second billing)
      gcp/                       # primary: GCE + VPC + compact placement (per-second billing)
      hetzner/ (later)           # secondary: hcloud servers + private network (1-hour-min billing)
  inventory/
    hosts.yml                    # GENERATED from terraform output (gitignored)
    terraform_to_inventory.sh    # terraform output -json → hosts.yml
  ansible/
    provision.yml                # heavy, idempotent, run once per `make up`
    bench.yml                    # re-invokable: run sweep + collect
    group_vars/all.yml           # matched run params (single source of truth, mirrors RUN-PARAMS.md)
    roles/
      os_tune/  toolchains/  build_aeron/  build_uc/  config/  run/  collect/
    templates/                   # *.properties.j2 (Aeron), uc node args (UC)
```

Results land outside the rig in the repo's existing `bench-out/dist/<timestamp>/`.

## 5. Terraform layer

**Provider-pluggable via a fixed module contract.** Each per-cloud module takes the
same inputs and produces the same `nodes[]` output; the root module and Ansible are
untouched when a cloud is added.

- **Module inputs:** `{ node_count, instance_type, region, ssh_public_key, ttl_hours, allow_ssh_cidr }`
- **Module output:** `nodes = [{ name, role, public_ip, private_ip }]`
  where `role ∈ {node0, node1, node2}`; **node0 = leader + co-located client host**.
- **Default `var.cloud = aws`** (per-second billing). Each module sets a sane
  per-cloud default `instance_type`; common cross-cloud invariants below.

Common to every module: a VPC/private network so intra-node traffic uses private IPs
(no public-internet hop); SSH (22) open only from `allow_ssh_cidr`, private subnet
open among the 3; SSH key uploaded by Terraform; instances tagged `owner` +
`ttl_hours` for the cost guard (§8). Sizing must keep cores ≥ Aeron busy-spin thread
count so the cluster is not starved (the local failure).

**AWS module (primary):**
- 3× compute-optimized instances in one **placement group** (low intra-node latency),
  default `c7i.4xlarge` (16 vCPU). For zero hyperthread contention prefer a `.metal`
  size; `instance_type` is a variable. VPC + subnet + security group.
- Auth via the standard AWS provider chain (env/profile); per-second billing.

**GCP module (primary):**
- 3× `c3-highcpu` (or `c4`) instances with a **compact placement policy**, default
  `c3-highcpu-8`; VPC + firewall. `instance_type`/`machine_type` is a variable.
- Auth via `GOOGLE_APPLICATION_CREDENTIALS` / gcloud ADC; per-second billing.

**Hetzner module (secondary, added later):**
- 3× **CCX dedicated-vCPU** instances (default `ccx33`: 8 dedicated vCPU / 32 GB) +
  a Hetzner private network; `HCLOUD_TOKEN` via environment.
- Billed at a **1-hour minimum per VPS** — acceptable for persistent mode, wasteful
  for short one-shot runs, hence secondary.

## 6. Ansible layer

**Inventory:** generated from `terraform output -json` into `inventory/hosts.yml`,
with groups `[cluster]` (all 3) and `[node0]` (leader/client host). Provider-agnostic.

**`provision.yml`** — heavy, idempotent, once per `make up`:
- `os_tune` — cpufreq governor → `performance`; disable transparent hugepages;
  `tuned-adm latency-performance` (if available); `vm.swappiness=0`; raise
  `net.core.{r,w}mem_max` so the configs' 128 k socket buffers take effect; raise
  `LimitNOFILE`/ulimits; optional explicit hugepages for Aeron (`aeron_hugepages`
  var); best-effort IRQ affinity.
- `toolchains` — Temurin **JDK 21** (apt or tarball), Rust via rustup, build deps
  (`git`, `unzip`, `protobuf-compiler`, `build-essential`).
- `build_aeron` — clone `aeron-io/benchmarks`, `./gradlew deployTar`, extract →
  `AERON_SCRIPT_HOME` (all 3 hosts).
- `build_uc` — sync UC source (§7), `cargo build --release` of `uc-node-launch` +
  `commit-path-load` (all 3 hosts).
- `config` — template Aeron `cluster.properties` (members list), `node{N}.properties`,
  `client.properties`, and UC peer args using the **private IPs** from inventory.
  node0 is appointed leader + co-located client. Matched durability + rate ladder
  come from `group_vars/all.yml`.

**`bench.yml`** — re-invokable, fast (build already done):
- For each system in `[uc, aeron]` (sequential, same hosts, never concurrent):
  start the node on each host (private-IP members), run the client on node0 across
  the rung ladder, capture the HdrHistogram, tear the cluster down.
- `collect` — fetch `.hgrm`/CSV to local `bench-out/dist/<timestamp>/` plus a
  **provenance manifest**: UC git SHA (+dirty flag), aeron-benchmarks SHA, instance
  type, kernel, and the exact run params used.

The `provision` / `bench` split is what gives the hybrid its iteration speed: build
once, bench many.

## 7. UC source delivery

Default: `rsync` the local working tree (excluding `target/`) so the current branch
and uncommitted work get tested; the manifest records the local git SHA + dirty flag.
A `uc_git_ref` var instead pins a committed SHA (clone + checkout) for canonical
runs. Aeron always builds from a pinned `aeron-io/benchmarks` ref recorded in vars.

## 8. Orchestration & lifecycle (the hybrid)

`Makefile` targets at `bench-infra/`:

| Target | Action |
|---|---|
| `make up` | `terraform apply` → generate inventory → `ansible-playbook provision.yml` |
| `make bench` | `ansible-playbook bench.yml` (run sweep + collect) against persistent hosts |
| `make bench-oneshot` | `up` → `bench` → `destroy` (clean-room canonical run = end-state "2") |
| `make status` | list instances + uptime; **warn if up > `ttl_hours`** |
| `make destroy` | `terraform destroy` |
| `make ssh-node0` | convenience SSH to the leader/client host |

- **Persistent mode (end-state "3"):** `make up` once, `make bench` repeatedly,
  SSH in to investigate anomalies (as we had to for the 100 ms floor), `make destroy`
  when done. Fast iteration.
- **One-shot mode (end-state "2"):** `make bench-oneshot` for a reproducible
  clean-room run with auto-teardown. Cheapest on **AWS/GCP** (per-second billing);
  on Hetzner each one-shot still costs a full hour minimum, so prefer persistent mode
  there.
- **Cost guard for the persistence risk:** TTL label + `make status` warning, plus an
  optional on-host self-`poweroff` timer at `ttl_hours` as a backstop against
  forgotten dedicated-vCPU instances.

## 9. Known dependency — Aeron IPC-ingress (the honest asymmetry)

Topology B's *shmem client edge* on node0 requires Aeron **IPC ingress**, which is
the config that crashes followers at election (`UdpChannel only supports UDP media:
aeron:ipc?endpoint=...`, §11). Therefore:

- `bench.yml` defaults to the **proven UDP-ingress** Aeron config — client edge is
  UDP-loopback on node0's host (the §3/§11 caveat: transport is a rounding error vs
  ms-scale consensus, so this is an acceptable first comparison).
- An `aeron_ingress: ipc` variant is templated but **gated behind the §11 fix**; it
  is not used until that config bug is resolved.
- **UC always gets its real shmem client edge.** The rig is explicit about this
  asymmetry in the manifest and run notes until IPC-ingress is fixed.

## 10. Matched run parameters (single source of truth)

`group_vars/all.yml` holds the matched sweep so both systems are driven identically,
mirroring `RUN-PARAMS.md`: rate ladder, measurement/warmup windows, payload bytes,
batch/per-message pacing, in-flight policy, idle strategy, **one durability posture**
(both durable or both non-durable, never mixed), and the histogram unit. `bench.yml`
asserts `achieved_rate == target_rate` (no Aeron `.FAIL`); a rung that doesn't keep
up is flagged invalid, not compared.

## 11. Success criteria

1. `make up` on a clean AWS (or GCP) account → 3 hosts (cores ≥ spin-thread count),
   tuned, both systems built, configs templated with real private IPs. Idempotent on
   re-run.
2. `make bench` → valid sweeps for UC and Aeron (no `.FAIL` rungs), results +
   provenance manifest in `bench-out/dist/<ts>/`.
3. `make bench-oneshot` → same, then hosts destroyed; no lingering instances.
4. `make destroy` and the TTL guard leave no forgotten billable resources.
5. Switching `var.cloud` between `aws` and `gcp` changes only the module; root +
   Ansible unchanged. Adding Hetzner later = one new `modules/hetzner/` satisfying the
   same contract.
6. The Aeron latency floor seen locally (busy-spin starvation) is **absent** on the
   provisioned dedicated-vCPU fleet — numbers are rate-responsive and comparable.

## 12. Future / out of scope

- `modules/hetzner/` (secondary cloud; the contract is designed for this).
- Adopting the IPC-ingress shmem client edge for Aeron once §11 is fixed.
- Cross-region / real-WAN topology; failover scenarios; CI wiring.
