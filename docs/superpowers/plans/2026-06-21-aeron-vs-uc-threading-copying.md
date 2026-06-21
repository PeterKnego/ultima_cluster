# Aeron-vs-UC Threading & Copying Investigation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a prioritized, microbenchmark-validated list of threading-handoff and data-copying optimization opportunities that would bring UC's commit path closer to Aeron's.

**Architecture:** Hybrid cost-anchored investigation. Frame the gap with existing cluster parity numbers → census UC's commit hot path for thread hops and payload copies → catalog Aeron's threading/copying patterns from core + cluster → validate each surfaced candidate with a microbenchmark → synthesize a two-tier (actionable / architectural) findings doc. Analysis only — no fixes implemented.

**Tech Stack:** Rust (UC: `uc_protocol`/`uc_node`/`uc_service`/`uc_client`/`ultima_journal`, Criterion benches), Java (Aeron: `aeron-client`/`aeron-driver`/`aeron-cluster` at `/home/claude/ultima/aeron`), `bench-parity/aeron-cluster-ipc` harness.

## Global Constraints

- Deliverable is the findings doc `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` — analysis only; **implement no fixes**.
- Every finding tagged with **confidence** (`sandbox-validated` / `needs-fleet-confirmation` / `hypothesis`) and **horizon** (`in-place tweak` / `refactor` / `long-horizon rewrite`).
- **Do not re-litigate settled work:** cross-host busy-poll is settled negative (task17 Phase B — any busy-poll claim must target an *intra-host* hop and say why it differs); the storage handoff is already documented (`docs/wal-journal-handoff-tax-2026-06-21.md` — cite/extend, don't rediscover); group-commit already amortizes the tax under load (every finding must state which regime — serial/shallow vs loaded — it helps).
- Microbenchmarks that need real NVMe / cross-host / `perf` are tagged `needs-fleet-confirmation`, not run in-sandbox (sandbox has `perf_event_paranoid=4`, no `perf` binary, NVMe fleet torn down).
- Spec: `docs/superpowers/specs/2026-06-21-aeron-vs-uc-threading-copying-design.md`.

---

## File Structure

- `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` — the findings doc (created Task 1, filled through Task 8).
- `uc_node/benches/handoff_wakeup_bench.rs` — futex-wakeup vs busy-spin per-hop latency microbench (Task 5).
- `uc_protocol/benches/payload_copy_bench.rs` — copy vs `Bytes` refcount-clone microbench (Task 6).
- Census/catalog artifacts (Tasks 2–4) are written directly into sections of the findings doc, not separate files.

---

### Task 1: Scaffold findings doc + frame the gap

**Files:**
- Create: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`

**Interfaces:**
- Produces: the doc skeleton with section anchors `## 1 Gap framing`, `## 2 UC commit-path census`, `## 3 Aeron core pattern catalog`, `## 4 Aeron Cluster census`, `## 5 Microbenchmark results`, `## 6 Prioritized opportunities`, `## 7 Synthesis`, that later tasks fill in.

- [ ] **Step 1: Gather the existing parity numbers**

Read `uc_autobench/tasks/netping/results.tsv` and the bench notes referenced by commit `e10a648` (Aeron-vs-UC parity, AWS c6id non-durable). Confirm the headline pair: Aeron ~80 µs p50 / 20k+ ops; UC ~8 ms p50 / ~10k ops. Note the run conditions (instance type, durable vs non-durable, payload size).

Run: `git show e10a648 --stat && sed -n '1,40p' uc_autobench/tasks/netping/results.tsv`

- [ ] **Step 2: Write the doc skeleton + gap-framing section**

Create the file with the 7 section headers above. Fill `## 1 Gap framing` with: the headline pair, run conditions, the prior-work map (network task16/17 done; storage handoff documented; group-commit amortization), and a one-paragraph statement that the suspected gap is a *shallow-pipeline latency* story (per-commit handoff + copies), not a throughput one — to be confirmed by the census.

- [ ] **Step 3: Commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): scaffold Aeron-vs-UC threading/copying findings + gap framing"
```

---

### Task 2: UC commit-path census (hops + copies)

**Files:**
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 2` section)

**Interfaces:**
- Consumes: doc skeleton from Task 1.
- Produces: a per-commit table of **thread hops** (each row: from-thread → to-thread, process boundary crossed? yes/no, wakeup mechanism futex/busy-spin/syscall, classification inherent/removable) and **payload copies** (each row: source → dest buffer, bytes copied vs refcount-handoff, necessary/removable, with `file:line` citation). The exact hop/copy count UC pays per commit.

- [ ] **Step 1: Dispatch a read-only Explore agent for the census**

Dispatch an `Explore` agent (read-only) with this task: trace one client write end-to-end and enumerate every thread hop and every payload copy, citing `file:line`. Reading targets, in order:
- `uc_client` submit → `clients/submit.ring` write (MPSC)
- `uc_node` client_dispatcher → `openraft.client_write` → replication → `RaftStateMachine::apply`
- `service/apply.ring` (SPSC) → `uc_service` apply_loop → `apply_resp.ring` → node → `clients/response.broadcast`
- ring impls in `uc_protocol` (SPSC/MPSC/Broadcast, atomic-after-write length prefix, task11 futex wakeups)
- `ultima_journal` writer thread + `Notifier`/`SeqWatermark` handoff

Require the agent to return two markdown tables (hops, copies) with the column schema in the Interfaces block above, plus the total hop count and total copy count.

- [ ] **Step 2: Verify the census against the code**

Spot-check 3 of the agent's `file:line` citations by reading them directly (one ring write, one openraft boundary, one apply-publish). Confirm the wakeup mechanism (futex vs busy-spin) and the copy-vs-refcount claim match the code. Cross-check the `AppCommand = bytes::Bytes` claim from CLAUDE.md against the actual submit→apply path — is it really refcounted all the way, or is there a copy at the ring boundary?

- [ ] **Step 3: Write the `## 2` section**

Paste the two verified tables. Add a summary line: "UC pays N wakeups and M payload copies per commit; of those, X wakeups and Y copies are classified removable." Flag each removable item with a candidate-ID (`T-1`, `T-2`… for threading; `C-1`, `C-2`… for copying) used by Tasks 5–8.

- [ ] **Step 4: Commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): UC commit-path hop/copy census"
```

---

### Task 3: Aeron core pattern catalog

**Files:**
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 3` section)

**Interfaces:**
- Consumes: doc skeleton from Task 1.
- Produces: a catalog of Aeron core threading/copying patterns, each with: pattern name, what it does, the UC candidate-ID(s) it maps to (or "no UC analog"), and a `file:line` citation in `/home/claude/ultima/aeron`.

- [ ] **Step 1: Dispatch a read-only Explore agent for the Aeron catalog**

Dispatch an `Explore` agent (read-only) over `/home/claude/ultima/aeron` to catalog these patterns with `file:line` citations:
- duty-cycle threading: `Agent`, `AgentRunner`, `IdleStrategy` (busy-spin → yield → park backoff) — `aeron-client`/`aeron-driver`
- lock-free `RingBuffer` (`ManyToOneRingBuffer`, `OneToOneRingBuffer`) and `BroadcastTransmitter` — how a consumer waits (busy-spin, never futex?)
- zero-copy publication: `tryClaim` / `BufferClaim`, flyweight buffers — how the payload reaches the log without a copy

Require: for each pattern, how many thread wakeups / payload copies it incurs per message, so it's directly comparable to the UC census.

- [ ] **Step 2: Map patterns onto UC candidates**

For each Aeron pattern, fill the "maps to UC candidate-ID" column by matching against the `T-*`/`C-*` IDs from Task 2. Note any Aeron pattern with no UC analog (these become architectural-tier findings in Task 8).

- [ ] **Step 3: Write the `## 3` section + commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): Aeron core threading/copying pattern catalog"
```

---

### Task 4: Aeron Cluster commit-path census

**Files:**
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 4` section)

**Interfaces:**
- Consumes: doc skeleton (Task 1); UC hop count (Task 2) for side-by-side.
- Produces: Aeron Cluster per-commit hop/copy count, in the same schema as the UC census, plus a one-row side-by-side: "Aeron Cluster: N wakeups / M copies; UC: N' / M'."

- [ ] **Step 1: Dispatch a read-only Explore agent over aeron-cluster**

Dispatch an `Explore` agent (read-only) over `/home/claude/ultima/aeron/aeron-cluster` and `bench-parity/aeron-cluster-ipc` to trace one cluster commit: ingress → consensus module → log append → commit → service (clustered service) → response. Count thread wakeups and payload copies per commit, same schema as Task 2. Note which threads are co-located in one agent vs separate.

- [ ] **Step 2: Write the `## 4` section with the side-by-side + commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): Aeron Cluster commit-path census + UC side-by-side"
```

---

### Task 5: Microbench — futex wakeup vs busy-spin per hop

**Files:**
- Create: `uc_node/benches/handoff_wakeup_bench.rs`
- Modify: `uc_node/Cargo.toml` (register the bench)
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 5` section)

**Interfaces:**
- Consumes: the threading candidate-IDs (`T-*`) from Task 2 — specifically the futex-wakeup hops flagged removable.
- Produces: measured per-hop latency for (a) futex round-trip wakeup, (b) busy-spin spin-wait on the same flag, in `## 5`, keyed to the `T-*` IDs. Validates the threading-axis mechanism cost. Tagged `sandbox-validated`.

- [ ] **Step 1: Write the bench**

```rust
// uc_node/benches/handoff_wakeup_bench.rs
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

// (a) futex-style: producer sets flag + wakes; consumer parks until woken.
fn bench_futex_handoff(c: &mut Criterion) {
    c.bench_function("handoff/futex_roundtrip", |b| {
        let flag = Arc::new(AtomicU32::new(0));
        let f2 = flag.clone();
        let consumer = thread::spawn(move || loop {
            // park until flag != 0; sentinel u32::MAX => stop
            while f2.load(Ordering::Acquire) == 0 {
                atomic_wait::wait(&f2, 0);
            }
            let v = f2.swap(0, Ordering::AcqRel);
            if v == u32::MAX { break; }
        });
        b.iter(|| {
            flag.store(1, Ordering::Release);
            atomic_wait::wake_one(&*flag);
            // spin until consumer drains, so we measure a full round trip
            while flag.load(Ordering::Acquire) != 0 {}
        });
        flag.store(u32::MAX, Ordering::Release);
        atomic_wait::wake_one(&*flag);
        consumer.join().unwrap();
    });
}

// (b) busy-spin: consumer never parks, spins on the flag.
fn bench_busyspin_handoff(c: &mut Criterion) {
    c.bench_function("handoff/busyspin_roundtrip", |b| {
        let flag = Arc::new(AtomicU64::new(0));
        let f2 = flag.clone();
        let consumer = thread::spawn(move || loop {
            let v = loop {
                let x = f2.load(Ordering::Acquire);
                if x != 0 { break x; }
                std::hint::spin_loop();
            };
            f2.store(0, Ordering::Release);
            if v == u64::MAX { break; }
        });
        b.iter(|| {
            flag.store(1, Ordering::Release);
            while flag.load(Ordering::Acquire) != 0 { std::hint::spin_loop(); }
        });
        flag.store(u64::MAX, Ordering::Release);
        consumer.join().unwrap();
    });
}

criterion_group!(benches, bench_futex_handoff, bench_busyspin_handoff);
criterion_main!(benches);
```

Add to `uc_node/Cargo.toml` (use `atomic-wait` if not already a dep; it is a tiny crate):

```toml
[dev-dependencies]
atomic-wait = "1"

[[bench]]
name = "handoff_wakeup_bench"
harness = false
```

- [ ] **Step 2: Run the bench**

Run: `cargo bench -p uc_node --bench handoff_wakeup_bench`
Expected: two reported times; `busyspin_roundtrip` materially lower than `futex_roundtrip` (the futex path pays the syscall + scheduler wakeup; busy-spin should be sub-µs to low-µs vs futex's several µs). Record both numbers.

- [ ] **Step 3: Write `## 5` futex-vs-busyspin subsection**

Record both numbers and the delta. State the implication for each `T-*` candidate: "removing the futex round-trip on hop T-k saves ≈ (futex − busyspin) per commit, at the cost of a core spinning." Tag `sandbox-validated`. Note the tradeoff: busy-spin burns a core, only viable on dedicated hops (cite that this is the intra-host analog Aeron uses, distinct from the settled-negative cross-host busy-poll).

- [ ] **Step 4: Commit**

```bash
git add uc_node/benches/handoff_wakeup_bench.rs uc_node/Cargo.toml docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "bench(uc_node): futex-wakeup vs busy-spin per-hop handoff latency"
```

---

### Task 6: Microbench — payload copy vs Bytes refcount handoff

**Files:**
- Create: `uc_protocol/benches/payload_copy_bench.rs`
- Modify: `uc_protocol/Cargo.toml` (register the bench; `bytes` + `criterion` as dev-deps if absent)
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 5` section)

**Interfaces:**
- Consumes: the copying candidate-IDs (`C-*`) from Task 2 and the payload sizes observed in the census.
- Produces: measured cost of `memcpy` vs `Bytes::clone` (refcount bump) across the census payload sizes, in `## 5`, keyed to `C-*`. Tagged `sandbox-validated`.

- [ ] **Step 1: Write the bench**

```rust
// uc_protocol/benches/payload_copy_bench.rs
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use bytes::Bytes;

// Sizes: replace with the actual payload sizes surfaced by the Task 2 census
// before running (e.g. 64, 256, 4096). Keep at least small/medium/large.
const SIZES: &[usize] = &[64, 256, 4096];

fn bench_copy_vs_refcount(c: &mut Criterion) {
    let mut g = c.benchmark_group("payload");
    for &n in SIZES {
        let src = vec![0u8; n];
        g.throughput(Throughput::Bytes(n as u64));
        g.bench_with_input(BenchmarkId::new("memcpy", n), &src, |b, src| {
            b.iter(|| {
                let mut dst = vec![0u8; src.len()];
                dst.copy_from_slice(src);
                criterion::black_box(dst);
            });
        });
        let bytes = Bytes::from(src.clone());
        g.bench_with_input(BenchmarkId::new("bytes_clone", n), &bytes, |b, bytes| {
            b.iter(|| {
                let c = bytes.clone(); // refcount bump, no data copy
                criterion::black_box(c);
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench_copy_vs_refcount);
criterion_main!(benches);
```

Add to `uc_protocol/Cargo.toml`:

```toml
[dev-dependencies]
criterion = "0.5"
bytes = "1"

[[bench]]
name = "payload_copy_bench"
harness = false
```

> Note: `uc_protocol` is `no_std`-friendly, but a `dev-dependency` bench is a separate `std` build target and does NOT violate the `no_std` posture of the library crate itself. Confirm the bench compiles without adding `bytes`/`criterion` to the library `[dependencies]`.

- [ ] **Step 2: Patch SIZES from the census, then run**

Replace `SIZES` with the payload sizes recorded in Task 2's census.

Run: `cargo bench -p uc_protocol --bench payload_copy_bench`
Expected: `bytes_clone` flat (~ns, size-independent — it's a refcount bump); `memcpy` grows with size. Record the crossover where copy cost becomes non-trivial vs the per-commit budget.

- [ ] **Step 3: Write `## 5` copy-vs-refcount subsection**

Record the numbers per size. For each `C-*` removable-copy candidate, state the per-commit saving = `memcpy(size)` at the census payload size. Tag `sandbox-validated`. Note where the copy is small enough to be irrelevant (e.g. <100 ns) so we don't over-prioritize it.

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/benches/payload_copy_bench.rs uc_protocol/Cargo.toml docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "bench(uc_protocol): payload memcpy vs Bytes refcount-clone"
```

---

### Task 7: Confirm/extend the isolated-handoff number

**Files:**
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 5` section)

**Interfaces:**
- Consumes: the existing handoff-tax measurement (tmpfs, ~35 µs async vs ~3 µs inline) from `docs/wal-journal-handoff-tax-2026-06-21.md`.
- Produces: a `## 5` subsection tying the Task-5 raw futex-roundtrip number to the full storage handoff (~32 µs), confirming the two-wakeup model, and stating what remains `needs-fleet-confirmation`.

- [ ] **Step 1: Reproduce the isolated handoff on tmpfs**

Run (per the handoff-tax doc):
```bash
cd ../ultima_db
TMPDIR=/tmp cargo bench --bench singlewriter_persistence_bench --features persistence -- standalone_consistent
TMPDIR=/tmp ULTIMA_WAL_INLINE=1 cargo bench --bench singlewriter_persistence_bench --features persistence -- standalone_consistent
```
Expected: async ~35 µs/commit, inline ~3 µs/commit → handoff ≈ 32 µs. Record actuals (sandbox numbers may differ).

- [ ] **Step 2: Reconcile with the Task-5 raw number**

Compare the isolated storage handoff (~32 µs) to the raw futex round-trip from Task 5. Confirm the "two scheduler wakeups per commit" model: storage handoff ≈ 2 × (futex round-trip) + small constant. Note any discrepancy as a hypothesis to chase on a real host.

- [ ] **Step 3: Write the `## 5` reconciliation subsection + list fleet-only items**

State explicitly which numbers need the fleet: depth-1 p99 (5.2 ms tail), end-to-end cluster commit attribution, `perf sched`/off-CPU. Tag those `needs-fleet-confirmation`.

- [ ] **Step 4: Commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): reconcile isolated handoff with raw futex roundtrip; list fleet-only items"
```

---

### Task 8: Synthesis — prioritized opportunities + two-tier writeup

**Files:**
- Modify: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md` (`## 6`, `## 7` sections)

**Interfaces:**
- Consumes: all of `## 2`–`## 5` (UC census + candidate-IDs, Aeron catalogs, microbench numbers).
- Produces: the prioritized opportunity table (`## 6`) and the two-tier synthesis + closing paragraph (`## 7`). This is the investigation's payload.

- [ ] **Step 1: Build the prioritized opportunity table (`## 6`)**

One row per candidate-ID (`T-*`, `C-*`) plus any Aeron-pattern-with-no-UC-analog from Task 3. Columns: candidate-ID | UC cost targeted | Aeron pattern borrowed | microbench evidence (number + which `## 5` subsection) | estimated headline impact (µs and % of the ~800µs/~8ms commit) | confidence | horizon | regime helped (serial/shallow | loaded | both). Sort by (high impact, high confidence, low horizon).

- [ ] **Step 2: Write the two-tier synthesis (`## 7`)**

- **Actionable tier:** opportunities inside the current architecture (busy-spin a named ring consumer; eliminate a named copy; collapse a named intra-process hop), each with its microbench-backed saving and ship-able horizon.
- **Architectural tier:** where the census shows the gap is structural (e.g. "Aeron pays N wakeups, UC pays ~3N due to the 3-process split + openraft round-trip"), name the structural cost and sketch what questioning it means (co-locate node+service threads; bypass openraft internal handoffs; single duty-cycle node agent). Flag `long-horizon rewrite`, rough upside, no pretense of cheap.

- [ ] **Step 3: Write the closing "what would close the gap" paragraph**

Rank actionable-tier total achievable saving vs the headline ~80µs-vs-8ms gap. State honestly: can UC approach Aeron with the actionable tier, or is parity fundamentally architectural? This is the bottom line.

- [ ] **Step 4: Final self-check against guardrails**

Verify: no busy-poll claim targets the wire; storage handoff findings cite the handoff-tax doc rather than rediscovering; every finding states its regime; every finding carries confidence + horizon tags. Fix inline.

- [ ] **Step 5: Commit**

```bash
git add docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md
git commit -m "docs(bench): Aeron-vs-UC prioritized opportunities + two-tier synthesis"
```

---

## Self-Review

**Spec coverage:**
- Deliverable findings doc → Tasks 1–8. ✓
- Two axes (threading, copying) → census Task 2 classifies both; benches Task 5 (threading) + Task 6 (copying). ✓
- Both anchors (cluster framing + core patterns) → Task 1 + Task 4 (cluster), Task 3 (core). ✓
- Hybrid methodology → census-first (Task 2), Aeron-as-oracle (Tasks 3–4), microbench-validate (Tasks 5–7), synthesize (Task 8). ✓
- Microbench split sandbox vs fleet → Tasks 5/6/7 tagged; fleet-only items listed Task 7. ✓
- Guardrails (busy-poll/handoff/group-commit) → Global Constraints + Task 5 Step 3 + Task 7 + Task 8 Step 4. ✓
- Two-tier synthesis + "everything on the table" → Task 8 Steps 2–3. ✓

**Placeholder scan:** SIZES in Task 6 is intentionally patched from the Task 2 census (explicit Step 2), not a placeholder. No TBD/TODO. ✓

**Type consistency:** candidate-ID scheme (`T-*`/`C-*`) defined Task 2 Step 3, consumed Tasks 5/6/8. Section anchors `## 1`–`## 7` defined Task 1, consumed throughout. ✓
