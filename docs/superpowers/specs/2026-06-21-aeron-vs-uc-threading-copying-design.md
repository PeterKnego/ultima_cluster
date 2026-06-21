# Aeron-vs-UC: threading-handoff & data-copying investigation — design

**Date:** 2026-06-21
**Type:** comparative investigation (analysis + microbenchmark-validated opportunities)
**Status:** design approved; ready for implementation plan

## Motivation

Cluster parity benchmarks put Aeron ~10–100× ahead of UC on commit latency/throughput
(latest AWS c6id non-durable run: **Aeron ~80 µs p50 / 20k+ ops**, **UC ~8 ms p50 / ~10k ops**;
commit `e10a648`). The network transport (task16/task17) and log storage (journal
preallocation, fdatasync, fill strategy) have already been worked. Two unexamined,
potentially high-leverage axes remain:

1. **Threading mode / thread-handoff** (incl. busy-poll vs futex wakeups).
2. **Data copying on the hot path** (minimize payload copies).

This investigation inspects both Aeron and UC along these two axes, validates where UC's
cost actually is with targeted microbenchmarks, and produces a prioritized list of
optimization opportunities to bring UC closer to Aeron.

## Deliverable

A findings doc: `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`, structured
as a **prioritized table of validated optimization opportunities**. Each entry carries:

- the UC cost it targets,
- the Aeron pattern it borrows,
- a microbenchmark result confirming/refuting it,
- an estimated headline impact (µs or % of the commit),
- a confidence tag (**sandbox-validated** / **needs-fleet-confirmation** / **hypothesis**),
- a horizon tag (**in-place tweak** / **refactor** / **long-horizon rewrite**).

This investigation stops short of implementing fixes. (A follow-on plan may prototype the
top opportunity.)

## Methodology — hybrid, cost-anchored (approach C)

Frame the gap with the existing cluster parity numbers → run the **UC hop/copy census**
(that's where *our* cost is, per the handoff-tax doc and the "IPC/fsync dwarf RTT"
finding) → for each identified cost, pull in the matching **Aeron pattern** as the target
state and **verify with a microbenchmark**. This spends the expensive work (deep code
reading + benchmarks) only on costs we've localized, while using Aeron as the design
oracle. The parity numbers keep us honest about whether a fix would actually move the
headline gap.

(Rejected alternatives: pure bottom-up census discovers Aeron's patterns only reactively;
pure top-down Aeron pattern-catalog risks cataloging patterns that don't matter for UC's
real costs.)

## Comparison anchor — both levels

- **Cluster-to-cluster** framing supplies the headline gap (Aeron Cluster commit path vs
  UC commit path — where the ~80µs/~8ms numbers came from).
- **Aeron core/IPC** supplies the threading/copying *patterns* (the duty-cycle `Agent`
  model, lock-free `RingBuffer`/`BroadcastTransmitter`, zero-copy `tryClaim`/flyweight
  buffers, `IdleStrategy` backoff). These transfer regardless of the consensus layer.

## The two axes

Run as parallel tracks over the same end-to-end commit trace.

### Axis 1 — threading / handoff

Count scheduler wakeups per commit on UC's path; classify each hop as **inherent**
(crosses a process boundary) vs **removable** (intra-process, or futex-where-busy-spin
would do). Compare to Aeron's wakeup count for an equivalent commit.

### Axis 2 — data copying

Count byte copies of the payload from client submit → journal → apply → response;
classify each as **necessary** (durability / format boundary) vs **removable** (could be a
`bytes::Bytes` refcount handoff or a flyweight view). The CLAUDE.md `AppCommand =
bytes::Bytes` zero-copy claim and the inter-node `write_chunks(&[Bytes])` scatter-gather
are the existing baseline to *verify* (the claim may already hold on some hops).

## Code inspection targets

### UC (commit hot path, in order)

- `uc_client` submit → `clients/submit.ring` (MPSC) write.
- `uc_node` client_dispatcher → `openraft.client_write` → replication → `RaftStateMachine::apply`.
- `service/apply.ring` (SPSC) → `uc_service` apply_loop → `apply_resp.ring` → node →
  `clients/response.broadcast`.
- ring buffer impls in `uc_protocol` (SPSC / MPSC / Broadcast; atomic-after-write length
  prefix; futex wakeups from task11).
- `ultima_journal` writer thread + `Notifier` / `SeqWatermark` handoff (partly mapped in
  the handoff-tax doc).

### Aeron

- `aeron-client` / `aeron-driver`: `Agent`, `AgentRunner`, `IdleStrategy`.
- IPC `RingBuffer` / `BroadcastTransmitter`; `tryClaim` / `BufferClaim` zero-copy publication.
- `aeron-cluster` consensus module commit path (cluster-to-cluster framing).

Aeron source is local at `/home/claude/ultima/aeron`; an apples-to-apples harness exists at
`bench-parity/aeron-cluster-ipc`.

## Microbenchmarks — split by where they can run

**In-sandbox (works today) — validates *mechanism* costs:**

- per-hop wakeup latency: futex round-trip vs busy-spin spin-wait.
- the handoff isolated on tmpfs (fsync ≈ 0; already measured ~35 µs async vs ~3 µs inline).
- a copy-vs-refcount microbench on the payload sizes UC uses.

**Needs the NVMe / cross-host fleet (re-provision `c6id.4xlarge`):**

- anything where the disk fsync tail or real RTT interacts with the handoff — the depth-1
  p99 (5.2 ms tail), end-to-end cluster commit attribution.
- `perf sched` / off-CPU profiling (sandbox has `perf_event_paranoid=4`, no perf binary).

Every finding is tagged **sandbox-validated** vs **needs-fleet-confirmation** so nothing
masquerades as proven.

## Execution shape

Four reading passes feed one synthesis:

1. UC commit-path census (hops + copies, with measured per-item cost).
2. Aeron core pattern catalog (Agent / IdleStrategy, RingBuffer, tryClaim / flyweight).
3. Aeron Cluster commit-path census (cluster-to-cluster framing).
4. Microbenchmark validation of each candidate surfaced by 1–3.

Passes 1–3 are largely independent read-only work → dispatch as parallel exploration
agents, each returning a structured census/catalog. Microbenchmark + synthesis done on the
main thread.

## Guardrails — do not re-litigate settled work

The doc must explicitly account for these prior results so they aren't resurfaced as "new":

- **Cross-host busy-poll is settled negative** (task17 Phase B): "network was never the
  bottleneck — fsync/IPC dwarf RTT." Any busy-poll recommendation must target an
  *intra-host* hop, not the wire, and say why this is different.
- **The storage handoff is already documented** (`docs/wal-journal-handoff-tax-2026-06-21.md`):
  the WAL inline-fsync spike (`spike/wal-inline-fsync`) and the journal `SeqWatermark`
  route already exist as proposals. The threading axis **cites and extends** these; its
  novel contribution is the **IPC ring hops** (client↔node↔service), which the handoff
  doc does not cover.
- **Group-commit already amortizes the tax under load** (2.9 µs/entry at depth). Every
  finding states whether it helps the *serial / shallow-pipeline* regime, the *loaded*
  regime, or both — the headline ~80µs-vs-8ms gap is likely a shallow-pipeline-latency
  story, not a throughput one.

## Scoring rubric (final prioritized table)

Each opportunity gets: estimated headline impact (µs or % of the ~800µs–8ms commit);
confidence (sandbox-validated / fleet-needed / hypothesis); horizon (in-place tweak /
refactor / long-horizon). Lead with **high-impact + high-confidence + low-horizon**.

## Synthesis — two tiers (constraints fully on the table)

Foundational constraints (3-process split, openraft, sync-deterministic-no-I/O apply) are
in scope to question, but findings are kept in two separate tiers so the actionable work
isn't buried under the speculative:

- **Actionable tier** — opportunities *inside* the current architecture (busy-spin a
  specific ring consumer, eliminate a specific copy, collapse a specific intra-process
  hop). Carry microbenchmark evidence and ship-able horizons.
- **Architectural tier** — where the census shows the gap is *structural* (e.g. "Aeron pays
  N wakeups, UC pays 3N because of the process split + openraft round-trip, and no in-place
  tweak closes that"), name the structural cost honestly and sketch what questioning it
  would mean (co-locating node+service threads, bypassing openraft's internal handoffs, an
  Aeron-style single duty-cycle node agent). Explicitly **long-horizon / rewrite-class**,
  with a rough upside estimate and no pretense of being cheap.

The doc closes with a **"what would actually close the ~80µs-vs-8ms gap"** paragraph that
ranks the tiers against the headline number — honest about whether UC can approach Aeron
with tweaks or whether parity is fundamentally a rewrite.

## Out of scope

- Implementing any fix (this is analysis only).
- Re-running the network/busy-poll-on-the-wire experiments (settled negative).
- Rediscovering the storage handoff proposals (cite, don't redo).
