# Benchmarks

An index of every measured result, what it was measured on, and where the full
record lives. The records themselves are the authority; this page exists so you
do not have to guess which of fifteen dated files is current.

For correctness rather than performance, see
[VERIFICATION.md](/docs/VERIFICATION.md).

---

## What "gate" means here

Every gate commits its **pass/fail rule to this repository before the run**, in
its own commit. From the M8 record:

> **Status: RULE PRE-COMMITTED, NOTHING MEASURED YET.** This file is committed
> *before* either arm runs. Everything below the "Pre-committed decide rule"
> heading is a promise made in ignorance of the data; the result sections are
> empty placeholders and are filled in afterwards, whatever they say.
>
> A bar chosen after seeing the number is not a bar, it is a description.

If an arm misses, the record says FAIL and keeps the bar. Git history is the
audit trail: the decide rule and the result are separate commits, in that order.

**Provenance is stated per result, never inherited.** A sandbox smoke run is
labelled as one — including where that makes a number look worse.

---

## Headline

| | |
|---|---|
| **3.79 M responses/s @ p50 ~0.3 ms, p99 ≤ 1.0 ms** | End to end through the SDK via the **local shmem client**, quorum-durable acks; 2025-generation hardware (`c9gd.4xlarge`), unpinned, n=8. Not a gate — a characterization sweep. Remote clients through a gateway pay a toll on top: ~0.5× per connection, ~0.9× aggregate (M13 rows below) |
| **1.64 M responses/s @ p99 0.771 ms** | The M5 *gate*, on the 2020-generation fleet (`c6id.2xlarge`): quorum-fsync'd, reads linearizable. p50 0.600 ms, p90 0.682 ms |
| **Failover p50 202 ms, 10/10 zero committed loss** | Sandbox/loopback; fleet confirmation outstanding |

CPU generation alone moves these headline numbers ~2× in rate and ~4× in p50
— the [architecture sweep](/docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)
measured it directly. Every row below names its hardware; compare within a
generation only.

---

## Performance results

| Record | Result | Measured on |
|---|---|---|
| [Architecture sweep — Intel vs Graviton](/docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md) | **3.79 M resp/s unpinned, p50 0.27–0.35 ms, p99 ≤ 1.0 ms** on Graviton; pinned throughput equal across arches at 3.04 M; pinning trades throughput for variance at a topology-dependent price; first full correctness pass on aarch64 | 3-host fleets, `c8id.4xlarge` + `c9gd.4xlarge` |
| [Core-count sweep](/docs/benchmarks/uc2-node-core-count-sweep-2026-08-31.md) | a node needs **4 cores on SMT x86, 2–3 on no-SMT ARM**; flat past that | 3-host fleet, `c8id.4xlarge` |
| [M13 rows on Graviton](/docs/benchmarks/uc2-m13-remote-on-arm-2026-09-01.md) | direct 3.41 M/s; one remote conn **0.50×** direct, aggregate **0.91×** (once measured above 1× — the direct arm shares the leader's cores); per-connection toll grew, aggregate toll shrank vs c6id. Characterization, not a gate | 3× `c9gd.2xlarge` + 2 client hosts |
| [M13 — remote path through the gateway](/docs/benchmarks/uc2-m13-gate-2026-08-24.md) | one TCP connection **0.617× direct** (1.08 M vs 1.75 M resp/s); N=16 aggregate **0.836× direct** (1.46 M); ladder monotone, 0 lost, no collapse | 4-host fleet, `c6id.2xlarge` |
| [M5 — end-to-end SDK](/docs/benchmarks/uc2-m5-gate-2026-07-12.md) | **1,639,187 responses/s** @ p50 0.600 / p90 0.682 / p99 0.771 ms · 4.1× the ≥400 k bar | 3-host fleet |
| [M3 — commit pipeline](/docs/benchmarks/uc2-m3-gate-2026-07-10.md) | **2,881,511 committed/s** @ p50 0.946 / p99 1.132 ms · 7.2× the bar | 3-host fleet |
| [M2 — replication stream](/docs/benchmarks/uc2-m2-gate-2026-07-10.md) | **235–323 MB/s durable per follower** · 2.3–3.2× the ≥ 100 MB/s bar — clean *and* under 0.5 % injected loss (34,870 NAKs served, `overruns 0`) | 3-host fleet |
| [M6 — learner join + purge](/docs/benchmarks/uc2-m6-gate-2026-07-12.md) | commit-rate dip **0.9 %** (gate < 10 %); below-floor reconstruction worst **2.80 s** over 5 purge cycles, zero read divergence | 4-host fleet |
| [M7 — live reconfiguration](/docs/benchmarks/uc2-m7-gate-2026-07-13.md) | per-transition dip **0.0–4.7 %** (gate < 10 %); leader self-removal handoff **3.22 s** (gate < 10 s), zero committed loss | 5-host fleet |
| [Read profile — before](/docs/benchmarks/uc2-read-profile-2026-07-26.md) | linearizable 244,052 reads/s vs snapshot 585,414 — **the barrier costs ~58 %** | 3-host fleet |
| [Read profile — after Rung A](/docs/benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md) | **542,065 reads/s (2.2×)** — barrier cost now ≤ 0.0 % read-only, 3.3 % mixed | 3-host fleet |
| [M4 — leader failover](/docs/benchmarks/uc2-m4-gate-2026-07-11.md) | p50 **202.1 ms**, p90 279.3 ms, max 394.1 ms · **10/10 zero committed loss** | 4-vCPU sandbox, loopback — *fleet not yet run* |
| [M8 — opt-in wire crypto](/docs/benchmarks/uc2-m8-gate-2026-07-29.md) | encrypted throughput **94.1 % of cleartext**, AES-NI confirmed | 4-vCPU dev box — *ratio only; fleet open* |
| [M1 — solo append+fsync](/docs/benchmarks/uc2-m1-gate-2026-07-09.md) | sandbox smoke on **tmpfs**, where `fdatasync` is nearly free — an upper bound, explicitly **not** the gate | 4-vCPU sandbox |

M1 has no fleet result. Its record says so in a banner at the top rather than
quietly presenting the sandbox number as the gate — as did M2's, until the
3-host fleet arm was run on 2026-07-11 and appended to the same record.

---

## The read-barrier arc

The most instructive sequence here, because it shows the method rather than a
number.

Linearizable reads run a quorum read-barrier, and nobody knew what it cost. A
fleet measurement answered it: linearizable reads plateaued at 244,052/s against
585,414/s for snapshot reads — a **41.7 % ratio, so the barrier was eating ~58 %
of read capacity.** A decide rule was committed beforehand stating what result
would justify building the optimization ("Rung A", batch-probe coalescing). The
measurement met it.

After Rung A landed, the *same* pre-committed rule was re-run against the same
fleet class and the same sweep — and returned **"Rung A NOT JUSTIFIED — the
barrier costs at most 0.0 % (read-only) / 3.3 % (mixed)."**

That is the rule working exactly as intended: it said *build this* when the cost
was 58 %, and *this is no longer worth building* once the cost was gone.
Linearizable reads went from 244,052/s to **542,065/s (2.2×)**, reaching 100.2 %
of snapshot-read capacity — the barrier is now free.

---

## Hardware

**Fleet** — `c6id.2xlarge` (8 vCPU, local instance-store NVMe) for the M2–M7
gate era; `c8id.4xlarge` (Intel Xeon 6, 8c×2SMT) and `c9gd.4xlarge` (Graviton,
16c no-SMT) for the 2026-08-31 sweeps — all `us-east-1`,
single AZ, cluster placement group, private-IP binding, journals on the NVMe
mount, `Durability::Consistent` (fdatasync per block), 64 B payloads. Host count
varies by gate (3 for M2/M3/M5/read-profile/sweeps, 4 for M6, 5 for M7). Driven by
`bench-infra/scripts/m6_fleet_gate.py`, which `stat -f`s every instance-dir
parent and **refuses to run on tmpfs or ramfs** — a journal on RAM makes every
fsync a silent no-op and would void the durability claim while everything
appeared to work.

**Sandbox** — 4 vCPU, shared development box, journals on ext4, nodes over
loopback. Used for smoke runs and for gates whose fleet arm has not been bought
yet.

---

## Reproducing

Fleet runs cost real AWS money and are a deliberate, separately-approved step —
which is why several records carry a sandbox arm and an outstanding fleet arm.

```bash
# Fleet (terraform + ansible under bench-infra/ provision the hosts first)
bench-infra/scripts/m6_fleet_gate.py --fleet                  # M6
bench-infra/scripts/m6_fleet_gate.py --fleet --m7             # M7
bench-infra/scripts/m6_fleet_gate.py --fleet --read-profile   # read profile
bench-infra/scripts/m14_core_sweep.py --fleet --topology 8x2  # core/arch sweep (16x1 on Graviton)

# Local, in-process smoke — 3 nodes + 3 services + 1 client in one process.
# The harness itself calls this "NOT the gate".
cargo run --release -p uc_node --example m5_gate -- all
```

The gate harnesses are `uc_node/examples/{m4,m5,m6,m7}_gate.rs` and
`read_profile.rs`; each carries its own `node` / `service` / `client` roles so
the fleet orchestrator can start one process per host.

---

## Open work

- **M4's fleet arm.** Failover is timeout-dominated and real NVMe fsync is faster
  than the sandbox's ext4, so the sandbox p50 is a conservative upper bound. What
  the fleet run would confirm is real-LAN detection timing, which loopback cannot
  speak to.
- **M8's fleet ratio.** The 94.1 % is a dev-box measurement; the record lists a
  cross-host confirmation as open.
- **M1** has never had a fleet arm. It is superseded in practice by M3 and M5,
  which measure the same path end to end on real hardware.
