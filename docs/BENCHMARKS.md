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

|||
|-|-|
| **2 M committed writes/s alongside 15.1 M read-your-writes reads/s, p99 0.34 ms** | Quorum-durable writes, reads spread over 3 nodes; every read type at every write rate to 2 M/s at p99 0.21–0.47 ms; unpaced ceiling 4.6–5.3 M committed writes/s with reads alongside. 2025-generation hardware (`c9gd.4xlarge`), 2 reps per cell. Not a gate — an instrument run ([record](/docs/benchmarks/uc2-read-types-under-writes-c9gd-2026-10-11.md)) |


CPU generation alone moves these headline numbers ~2× in rate and ~4× in p50
— the [architecture sweep](/docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)
measured it directly. Every row below names its hardware; compare within a
generation only.

---

## Performance results

| Record | Result | Measured on |
|---|---|---|
| [Service time — closed loop at inflight 1](/docs/benchmarks/uc2-service-time-2026-09-16.md) | the commit path's own round trip, first ever measured for v2: **shipped posture p50 123 µs / mean 166 µs** (fsync on, unpinned); pinned 109; + spinning apply agent 105 (p90 219 → 117: the 50 µs idle sleep was a second mode); + fsync off the path **77 µs**; raw UDP RTT on the same fleet 33.5 µs, so the wire is ≤ 29 % of the shipped p50 — kernel bypass declined by the bound. **Re-run 2026-09-17 with the apply agent's new ladder default (`2be3d6f`): shipped, unpinned p50 105 / p90 118 / mean 114 µs — the second mode gone with no tuning.** Measurement row, no bar | 4-host fleet, `c6id.2xlarge` |
| [Architecture sweep — Intel vs Graviton](/docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md) | **3.79 M resp/s unpinned, p50 0.27–0.35 ms, p99 ≤ 1.0 ms** on Graviton; pinned throughput equal across arches at 3.04 M; pinning trades throughput for variance at a topology-dependent price; first full correctness pass on aarch64 | 3-host fleets, `c8id.4xlarge` + `c9gd.4xlarge` |
| [Core-count sweep](/docs/benchmarks/uc2-node-core-count-sweep-2026-08-31.md) | a node needs **4 cores on SMT x86, 2–3 on no-SMT ARM**; flat past that | 3-host fleet, `c8id.4xlarge` |
| [M13 rows on Graviton](/docs/benchmarks/uc2-m13-remote-on-arm-2026-09-01.md) | direct 3.41 M/s; one remote conn **0.50×** direct, aggregate **0.91×** (once measured above 1× — the direct arm shares the leader's cores); per-connection toll grew, aggregate toll shrank vs c6id. Characterization, not a gate | 3× `c9gd.2xlarge` + 2 client hosts |
| [M13 — remote path through the gateway](/docs/benchmarks/uc2-m13-gate-2026-08-24.md) | one TCP connection **0.617× direct** (1.08 M vs 1.75 M resp/s); N=16 aggregate **0.836× direct** (1.46 M); ladder monotone, 0 lost, no collapse | 4-host fleet, `c6id.2xlarge` |
| [M5 — end-to-end SDK](/docs/benchmarks/uc2-m5-gate-2026-07-12.md) | **1,639,187 responses/s** @ p50 0.600 / p90 0.682 / p99 0.771 ms · 4.1× the ≥400 k bar | 3-host fleet |
| [M3 — commit pipeline](/docs/benchmarks/uc2-m3-gate-2026-07-10.md) | **2,881,511 committed/s** @ p50 0.946 / p99 1.132 ms · 7.2× the bar | 3-host fleet |
| [M2 — replication stream](/docs/benchmarks/uc2-m2-gate-2026-07-10.md) | **235–323 MB/s durable per follower** · 2.3–3.2× the ≥ 100 MB/s bar — clean *and* under 0.5 % injected loss (34,870 NAKs served, `overruns 0`) | 3-host fleet |
| [M6 — learner join + purge](/docs/benchmarks/uc2-m6-gate-2026-07-12.md) | commit-rate dip **0.9 %** (gate < 10 %); below-floor reconstruction worst **2.80 s** over 5 purge cycles, zero read divergence | 4-host fleet |
| [M7 — live reconfiguration](/docs/benchmarks/uc2-m7-gate-2026-07-13.md) | per-transition dip **0.0–4.7 %** (gate < 10 %); leader self-removal handoff **3.22 s** (gate < 10 s), zero committed loss | 5-host fleet |
| [Read types under writes — Graviton, writes to the ceiling](/docs/benchmarks/uc2-read-types-under-writes-c9gd-2026-10-11.md) | **15.1 M read-your-writes reads/s across 3 nodes beside 2 M committed writes/s, p99 0.34 ms**; 20.7 M snapshot reads/s read-only; write ceiling with reads alongside **4.6–5.3 M committed/s**; no read type starved to 2 M; linearizable = 0.55× snapshot on the leader; ryw/leader ÷ snap/leader **0.83 — FAIL** (≥ 0.90 bar kept) | 3-host fleet, `c9gd.4xlarge` |
| Superseded read records: [read profile](/docs/benchmarks/uc2-read-profile-2026-07-26.md) · [after Rung A](/docs/benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md) · [read spread](/docs/benchmarks/uc2-read-spread-2026-10-07.md) · [read types under writes](/docs/benchmarks/uc2-read-types-under-writes-2026-10-10.md) · [query-drain fix](/docs/benchmarks/uc2-query-drain-progress-2026-10-10.md) | earlier fleets and, for the two July records, both arms on the read-only idle cap found 2026-10-07; their numbers stand in their own files and are superseded by the row above | `c6id.2xlarge` |
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
fleet measurement in July found it eating more than half of read capacity
because every read ran its own probe round; a decide rule committed beforehand
said what result would justify batching the rounds ("Rung A"), and the
measurement met it. After Rung A the *same* rule was re-run and reported the
barrier as free — which later turned out to be the instrument, not the
barrier: both arms were sitting on the apply agent's read-only idle-sleep cap,
found in the read-spread run of 2026-10-07 and removed on 2026-10-10.

With the cap gone and writes pushed to the ceiling on Graviton (2026-10-11):
**the barrier costs about 45 % of the leader's read capacity** at 1,024 reads
in flight (3.88 M linearizable vs 7.05 M snapshot reads/s read-only, 2.89 M vs
5.07 M beside 2 M committed writes/s). The lever that moves reads is placement,
not the barrier: read-your-writes reads spread over the followers give
**4.4–5.8×** the linearizable path, 15.1 M/s beside 2 M committed writes/s
([record](/docs/benchmarks/uc2-read-types-under-writes-c9gd-2026-10-11.md)).

The rule did its job both times — it said *build this* when the cost was real,
and it reported an artifact faithfully when the instrument was capped. What it
could not do was see the cap. That is why a result's provenance is stated per
record, and why a superseded record stays in its file instead of being edited.

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

