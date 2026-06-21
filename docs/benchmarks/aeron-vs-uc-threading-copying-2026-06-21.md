# Aeron vs UC — threading-handoff & data-copying investigation

**Date:** 2026-06-21
**Type:** comparative investigation — analysis + microbenchmark-validated opportunities (no fixes implemented)
**Spec:** `docs/superpowers/specs/2026-06-21-aeron-vs-uc-threading-copying-design.md`
**Plan:** `docs/superpowers/plans/2026-06-21-aeron-vs-uc-threading-copying.md`

Each finding is tagged **confidence** (`sandbox-validated` / `needs-fleet-confirmation` / `hypothesis`)
and **horizon** (`in-place tweak` / `refactor` / `long-horizon rewrite`).

---

## 1 Gap framing

### Headline numbers (AWS 3× c6id.4xlarge, placement group, durability=none, 64 B payload)

Source: `docs/benchmarks/aeron-vs-uc-parity-2026-06-21.md` (commit `e10a648`).

| metric | Aeron | UC | ratio |
|---|---|---|---|
| p50 latency (rate 100, zero queueing) | ~0.11 ms (~80 µs) | ~8 ms | ~100× |
| p99.9 latency (steady) | sub-ms | seconds past the knee | — |
| sustained throughput | 20k+ msg/s (flat) | ~10k msg/s (saturates) | ~2× |

### What the gap actually is (critical — sets the whole investigation's target)

The parity doc is explicit, and the census must respect it:

1. **The ~100× p50 gap is dominated by the deliberate 5 ms `UC_API_BATCH_LINGER_MS`, not the wire.**
   At rate 100 (one msg / 10 ms, no queueing) UC is still ~8 ms ≈ 5 ms linger + ~2.7 ms Raft
   replication + shmem IPC. This is a throughput-batching tradeoff, not a transport deficit. A
   `linger=0` run (not yet done) is the honest latency floor.
2. **The comparison is not like-for-like:** Aeron's `LoadTestRig` measures raw open-loop cluster-message
   RTT; UC's `commit-path-load` measures the full client→submit→linger-batch→replicate→apply→response
   path. The fair, transport-independent targets are the **throughput ceiling** (Aeron ~2×) and the
   **per-commit pipeline cost beneath the linger**.
3. **QUIC ≈ UDP on this fleet** — transport is not the lever (consistent with task16/task17).

**Therefore this investigation targets the per-commit pipeline cost and the throughput ceiling, not the
linger and not the wire.** The two axes — thread-handoff wakeups and payload copies — are exactly the
per-commit pipeline costs that sit *under* the linger and that *cap throughput* (every wakeup and copy is
work done per commit that limits how many commits/sec one core can push). This is a **shallow-pipeline
latency + throughput-ceiling** story, to be confirmed by the census in §2.

### Prior-work map (settled — do not re-litigate; see §guardrails)

- **Network transport** (task16 UDP, task17 pipelined append + Phase B busy-poll): worked; cross-host
  busy-poll concluded **negative** ("network was never the bottleneck — fsync/IPC dwarf RTT"). Any
  busy-poll finding here must target an **intra-host** hop and say why it differs.
- **Log storage** (journal preallocation / fdatasync / fill-strategy; `docs/wal-journal-handoff-tax-2026-06-21.md`):
  the storage cross-thread handoff (~32 µs, two wakeups/commit) is already documented, with the WAL
  inline-fsync spike and the journal `SeqWatermark` route as existing proposals. This investigation
  **cites and extends** those; its novel contribution is the **IPC ring hops** (client↔node↔service).
- **Group-commit** already amortizes the handoff under load (~2.9 µs/entry at depth). Every finding states
  which regime it helps: **serial/shallow**, **loaded**, or **both**.

---

## 2 UC commit-path census (hops + copies)

One client write, leader steady-state. Citations spot-verified against the code (the journal
double-copy, the client `copy_from_slice`, the ring memcpy in/out, the spin-then-park).

### Hops (scheduler/wakeup boundaries)

| # | from-thread → to-thread | process boundary? | wakeup mechanism | class | file:line |
|---|---|---|---|---|---|
| 1 | uc_client task → uc_node client_dispatcher | **yes** | inter-proc **futex** (waiters-guarded) | inherent | `uc_protocol/src/ring/mpsc.rs:205`, `ring/futex.rs:22-44` |
| 2 | NotifyBridge parker thread → client_dispatcher task | no | futex-wait → tokio `Notify` | **removable T-1** | `uc_node/src/ipc/ring_bridge.rs:48-50`; `client_dispatcher.rs:144` |
| 3 | client_dispatcher → openraft RaftCore | no | tokio channel | **removable T-2** (openraft-internal) | `client_dispatcher.rs:107` |
| 4 | RaftCore → openraft SM worker | no | tokio channel | **removable T-3** (openraft-internal) | `raft/state_machine_shmem.rs:512-530` |
| 5 | node apply() → uc_service apply thread | **yes** | inter-proc **futex** | inherent | `uc_protocol/src/ring/spsc.rs:196`, `ring/futex.rs:35-44` |
| 6 | apply.ring publish → service consumer | (service side of #5) | **spin 64× then futex park** | inherent | `ring/spsc.rs:217-233`; `ring/common.rs:67` (`SPIN_TRIES`) |
| 7 | uc_service apply thread → node await_apply_resp | **yes** | inter-proc futex + Notify bridge | inherent | `uc_service/src/runtime/apply_loop.rs:124-138`; `state_machine_shmem.rs:949` |
| 8 | NotifyBridge parker → node SM worker | no | tokio `Notify` | **removable T-4** | `ring_bridge.rs:48-50`; `state_machine_shmem.rs:949` |
| 9 | node SM worker → client_dispatcher (responder) | no | tokio channel | **removable T-5** (openraft-internal) | `state_machine_shmem.rs:650-653` |
| 10 | client_dispatcher → uc_client broadcast_reader | **yes** | inter-proc futex (`all=true`) + Notify | inherent | `client_dispatcher.rs:321-335`; `ring/broadcast.rs:123` |
| 11 | broadcast_reader → caller's `submit().await` | no | tokio oneshot | **removable T-6** | `uc_client/src/rings.rs:115-116`; `client.rs:252` |
| J1 | openraft append → journal writer thread | no | `std::sync::mpsc` send | **removable T-7** | `ultima_journal/src/journal/mod.rs:334`; `writer.rs:330` |
| J2 | journal writer → append caller | no | **Condvar** notify (or inline cb) | **removable T-8** | `writer.rs:423`; `notifier.rs:115-128`; `mod.rs:441-448` |

**Total: 13 wakeup boundaries.** **4 are inherent** (cross-process IPC: #1, #5/#6, #7, #10);
**8 are removable** intra-process hops (T-1…T-8). Of the removable ones, **T-2/T-3/T-5 are
inside openraft** (would require forking it); **T-1/T-4/T-6** are UC's own bridge/oneshot hops;
**T-7/T-8** are the journal handoff already covered by `docs/wal-journal-handoff-tax-2026-06-21.md`.

### Copies (payload)

17 copy events; 13 necessary, 2 already zero-copy refcount handoffs, **2 removable**.

| key removable / handoff | what | file:line |
|---|---|---|
| **C-1** | journal serializes once, then **copies the bytes twice** — `payload.to_vec()` → `payload_vec.clone()` into `AppendRequest` **and** `pending.insert(…, payload_vec)`. An `Arc<[u8]>`/`Arc<Vec<u8>>` would drop one. | `ultima_journal/src/journal/mod.rs:324-351` |
| **C-2** | client `Bytes::copy_from_slice(&buf)` on the response read — could `Bytes::from(std::mem::take(&mut buf))` to avoid the extra heap copy. | `uc_client/src/rings.rs:114` |
| C4, C13 (already zero-copy) | `Bytes::from(Vec)` ownership transfer at the submit-read and apply-resp-read — no copy. | `client_dispatcher.rs:97`; `state_machine_shmem.rs:939` |

**The 4 unavoidable ring memcpys** (C2/C3 submit, C8/C9 apply) plus the bincode
encode/decode at each format boundary (C1/C5/C10/C11) are the bulk. Each ring hop is a copy
*into* the fixed shmem slot and a copy *out* — inherent to a fixed-region SPSC/MPSC ring.

**Verdict on the CLAUDE.md `AppCommand = bytes::Bytes` "no intermediate copy" claim:**
**partially true, overstated.** `Bytes` does eliminate intra-process heap copies (C4/C13 are
genuine refcount handoffs), but **every ring boundary is a forced memcpy** (the slot is a fixed
shmem region) — so the payload is copied ≥4× through the rings regardless of `Bytes`, plus the
serialize/deserialize passes.

**Payload-size context (for the copy microbench):** hot-path `payload_buf` preallocates **4096 B**
(`client_dispatcher.rs:64`); frame header is **20 B** (`ring/common.rs:276`); ring caps are
16 MiB (client) / 64 MiB (apply) with 4/16 MiB max frame — i.e. typical KV payloads are tens–hundreds
of bytes, large frames are outliers. Microbench sizes: **64 / 256 / 4096 B**.

---

## 3 Aeron core pattern catalog

**Note:** the threading/buffer primitives (`Agent`, `AgentRunner`, `IdleStrategy`,
`*RingBuffer`, `BroadcastTransmitter`, `UnsafeBuffer`) live in **agrona** (external jar, no
source in-tree); citations below are aeron's *call sites* into that API.

| pattern | what it does | wakeups/msg | copies/msg | file:line |
|---|---|---|---|---|
| **Agent duty-cycle** | each role (Conductor/Sender/Receiver) is a dedicated thread looping `doWork()`; never parks for work — polls | **0** | 0 | `aeron-driver/.../MediaDriver.java:279-283`; `DriverConductor.java:118`; `Sender.java:60`; `Receiver.java:41` |
| **BackoffIdleStrategy** (default) | on no-work: spin 10× → yield 20× → park 1µs↑1ms | 0 hot; 1 park only when fully idle | 0 | `Configuration.java:482,487-504` |
| **BusySpinIdleStrategy** (low-lat) | on no-work: `onSpinWait()` only, never parks (burns a core) | **0 always** | 0 | `aeron-samples/.../LowLatencyMediaDriver.java:46-47` |
| **ManyToOneRingBuffer** (client→driver) | MPSC; consumer (Conductor) **polls** every duty cycle | **0** | 0 (read in place) | `Aeron.java:1237`; `DriverProxy.java:83`; `ClientCommandAdapter.java:101` |
| **BroadcastTransmitter / CopyBroadcastReceiver** (driver→client) | one-to-many; client **polls** | **0** | 1 (CopyReceiver scratch copy vs fast transmitter) | `ClientProxy.java:218`; `Aeron.java:1243-1245`; `DriverEventsAdapter.java:70-71` |
| **Publication.offer** | claim term-buffer space (atomic `getAndAddLong`), then `putBytes` payload in | 0 (non-blocking) | **1** | `ConcurrentPublication.java:359,372,380` |
| **Publication.tryClaim + BufferClaim** | reserve a log range, hand the producer a **pointer into the log buffer**; write in place; `commit()`=`putIntRelease` | 0 | **0** | `Publication.java:557`; `ConcurrentPublication.java:311,713-736`; `logbuffer/BufferClaim.java:56-58,185-193` |
| **Image.poll / FragmentHandler** | consumer reads its position counter, volatile-loads frame length, calls handler with a **raw pointer into the mmap term buffer** | **0** | **0** | `Image.java:340,358-374`; `logbuffer/FrameDescriptor.java:297-306` |
| **Flyweight over DirectBuffer** | headers/messages read/written in place by byte-offset; no deserialize copy | 0 | **0** | `protocol/HeaderFlyweight.java:40`; `command/CorrelatedMessageFlyweight.java:37,65-71` |
| **mmap'd log term buffers** | log file `mmap`'d + wrapped by `UnsafeBuffer`; producer & consumer share the same physical pages → true OS-level zero-copy IPC | 0 | **0** | `LogBuffers.java:48,74,84-103,165-171` |

**Philosophy (single-process publish→poll, tryClaim path): 0 thread wakeups, 0 payload copies.**
The *entire* producer↔consumer coordination is one `putIntRelease` (publish) + one `getIntVolatile`
(poll) on the frame-length word. No queue, no condvar, no futex, no lock handoff between stages.
With `offer` instead of `tryClaim` it's 1 copy; the read side is always 0. Default Agents back off
to a park when fully idle; the low-latency profile busy-spins on dedicated cores (3 cores burned).

---

## 4 Aeron Cluster commit-path census

One cluster commit, leader. Same schema as §2 so it sits side-by-side.

| # | from → to | separate thread? | wait mechanism | file:line |
|---|---|---|---|---|
| 1 | client → ConsensusModuleAgent (ingress) | yes (proc) | UDP→driver→IPC term buffer; CM **polls** `controlledPoll()` | `IngressAdapter.java:223-231`; `ConsensusModuleAgent.java:2427` |
| 2 | CM → log publication / Archive | yes (driver) | `offer`/`tryClaim` to log term buffer; Archive **polls** RecordingPos | `LogPublisher.java:132`; `ConsensusModuleAgent.java:1627` |
| 3 | leader → followers (replicate) | yes (**network**) | MDC UDP | `LogPublisher.java:111`; `ConsensusModuleAgent.java:1633-1637` |
| 4 | follower → leader (appendPosition ACK) | yes (**network**) | follower polls counter, sends via consensus channel; leader **polls** `ConsensusAdapter` | `ConsensusModuleAgent.java:2686-2704`; `ConsensusAdapter.java:69-77` |
| 5 | CM `commitPosition` counter → ClusteredServiceAgent | yes (thread) | leader `setRelease()` atomic store; service **spins** on `commitPosition.get()` each duty cycle | `ConsensusModuleAgent.java:2863`; `ClusteredServiceAgent.java:262` |
| 6 | ServiceAgent → user `onSessionMessage()` | **no** (same stack) | in-thread call | `BoundedLogAdapter.java:130-163`; `ClusteredServiceAgent.java:482-496` |
| 7 | service response → client (egress) | yes (net/IPC) | `offer` to egress publication; client polls | `ClusteredServiceAgent.java:680`; `ContainerClientSession.java:86-89` |

**Copies:** 4 real byte-copies (ingress→log term buffer, log→archive file, network replicate,
egress→client). **All intra-process buffer reads are zero-copy flyweights** over mapped memory
(ingress dispatch, the bounded-log→service handoff). The leader→service commit notification is a
**single atomic counter write** read by the service via a shared `CountersReader` slab — **no
queue, no futex, no condvar**.

**Side-by-side (per commit):**

| | thread hops | of which park/futex on hot path | payload copies |
|---|---|---|---|
| **Aeron Cluster** | 5 agent boundaries (3 poll-based no-wakeup; 2 are network send/recv) | **0** (Agents poll/spin, never park for work) | **4** (all in-process reads are flyweight zero-copy) |
| **UC** | 13 (4 cross-process futex, 8 intra-proc; J1/J2 journal) | up to several **futex/condvar** wakeups + the spin-then-park | 17 events (4 forced ring memcpys + serialize/deserialize each boundary) |

**Structural difference.** Aeron Cluster keeps the consensus module and the clustered service as
two **polling Agents in one JVM** sharing `/dev/shm` term buffers and a counter slab — a commit
crosses zero queues/locks/futexes; the only blocking is the two network hops (intrinsic to Raft).
UC splits node and service into **separate OS processes** bridged by shmem rings, and routes
consensus through a **general-purpose Raft library (openraft) built on async tasks** — so each
intra-host stage transition that Aeron does as a counter-poll, UC does as a futex wake + park
(≈1–10 µs/round-trip on Linux) or a tokio channel/`Notify` hop. And where Aeron reads log entries
as flyweights in place, UC serializes to the journal, deserializes for apply, and copies through
each ring slot. **The threading and copy gap is structural, not a tuning miss** — it follows
directly from process-separation + a futex/async-channel handoff model vs. a single-process
poll-everything model.

**LoadTestRig measurement model** (`bench-parity/aeron-cluster-ipc/README.md:16-17`): open-loop
fixed-rate send, latency = `now − intendedSendTime` (coordinated-omission-safe). UC's harness
matches this `now − intended_send` model, so the §1 percentiles are apples-to-apples on the
*measurement*, even though the *workload* (raw RTT vs full batched SMR commit) is not like-for-like.

---

## 5 Microbenchmark results

Run in **this sandbox** (4 vCPU, virtualized — absolute numbers are inflated vs a
dedicated host; ratios and orders-of-magnitude are the signal). Both are dependency-free
`std` examples in `uc_protocol/examples/` (criterion/`atomic-wait` are not vendored and the
sandbox is offline). `release` build, 100k / 5M iterations.

### 5.1 Threading — futex park/wake vs busy-spin  *(sandbox-validated)*

`cargo run -p uc_protocol --release --example handoff_wakeup_bench` — ping-pong round trip,
futex arm uses the same `FUTEX_WAIT`/`FUTEX_WAKE` syscall as `uc_protocol/src/ring/futex.rs`.

| arm | ns / round-trip | ns / wakeup |
|---|---|---|
| busy-spin | **132** | **66** |
| futex park/wake | **23,516** | **11,758** |

**A futex wakeup costs ~11.7 µs here; a busy-spin handoff costs ~66 ns — a ~175× gap.**
Implication for each removable hop that currently parks: removing the park (busy-spin or
poll the ring) saves **≈ the futex cost per wakeup**. The inherent cross-process hops
(#1/#5/#7/#10) each pay this when the consumer has parked; the spin-then-park (`SPIN_TRIES=64`,
§2 hop #6) already dodges it *when the producer is hot*, but pays full price when the consumer
has gone to sleep — exactly the shallow-pipeline / low-rate regime where UC's p50 is measured.
Tradeoff: busy-spin burns a core (Aeron's low-latency profile burns 3) — only viable on a
bounded number of dedicated hot hops, **intra-host** (distinct from the settled-negative
cross-host busy-poll: this targets a local ring consumer, not the wire).

### 5.2 Copying — memcpy vs refcount-clone  *(sandbox-validated)*

`cargo run -p uc_protocol --release --example payload_copy_bench` — `copy_from_slice` vs
`Arc<[u8]>::clone` (same mechanism as `Bytes::clone`) at the §2 census payload sizes.

| size (B) | memcpy ns | Arc-clone ns |
|---|---|---|
| 64 | ~6 | ~5 |
| 256 | ~4 | ~4 |
| 4096 | **~40** | **~4** |

**A copy at KV payload sizes is single-digit-to-~40 ns; a refcount clone is flat ~4 ns.**
Both are **~300–2000× smaller than one futex wakeup (~11,700 ns).** So the removable copies
**C-1** (journal double-copy) and **C-2** (client `copy_from_slice`) save only **nanoseconds**
at typical payloads — **low priority.** Copying only becomes commit-relevant for **large frames**
(the 4 MiB / 16 MiB ring max): a 4 MiB memcpy ≈ tens of µs, then comparable to a wakeup.

### 5.3 Reconciliation with the documented storage handoff  *(sandbox-validated + cites prior)*

The handoff-tax doc (`docs/wal-journal-handoff-tax-2026-06-21.md`) independently measured the
*storage* cross-thread handoff at **~32 µs (store WAL, tmpfs)** and **~22 µs (journal, over the
raw-write floor)** — each is **two scheduler wakeups per commit** (enqueue→writer, writer→signal).
Dividing by two gives **~11–16 µs/wakeup**, which **matches this session's measured ~11.7 µs/wakeup**.
The two-wakeup model is therefore confirmed from both directions (a generic futex ping-pong here,
and the end-to-end WAL/journal handoff there). This *is* the journal `Notifier`/`SeqWatermark`
hops J1/J2 (T-7/T-8) in §2.

### 5.4 What still needs the fleet  *(needs-fleet-confirmation)*

- **Absolute wakeup cost on the real c6id host** — sandbox numbers are inflated; the headline
  attribution needs a dedicated-host re-run.
- **Depth-1 p99 (5.2 ms tail)** and **end-to-end cluster-commit attribution** (how many of the
  ~800 µs–8 ms a real commit spends parked vs replicating vs fsync).
- **`perf sched` / off-CPU profile** to confirm wakeups are the scheduler cost (sandbox
  `perf_event_paranoid=4`, no `perf`).
- **Deferred (not skipped):** re-running ultima_db's `singlewriter_persistence_bench` to
  re-confirm the ~32 µs / ~3 µs handoff live. Skipped this pass because (a) §5.3 already
  triangulates it, and (b) `../ultima_db` is a shared checkout that may have a concurrent
  session — avoided to not disturb it. Re-run on the fleet host alongside the items above.

---

## 6 Prioritized opportunities

Sorted by (impact × confidence ÷ horizon). "Regime" = which operating point it helps:
**serial/shallow** (parked consumers, low rate), **loaded** (hot producers, the ~10k/s ceiling),
or **both**. Headline impact is judged against the two real targets from §1 — the **throughput
ceiling** and a **linger=0 latency floor** — *not* the linger-bound 8 ms p50 (see §7).

| ID | opportunity | Aeron pattern | evidence | impact | confidence | horizon | regime |
|---|---|---|---|---|---|---|---|
| **O1** | **Busy-spin / poll the intra-host ring consumers UC owns** instead of futex-parking — the node→service & service→node bridge hops (T-1, T-4) and tuning the `SPIN_TRIES` window (hop #6). Eliminates the ~11.7 µs park per hop *when consumers idle*. | Agent duty-cycle + `BusySpinIdleStrategy`; consumers poll, never park | §5.1 (175× futex vs spin); §2 hops #5–8 | raises throughput ceiling; cuts ~tens-of-µs/commit at low rate | sandbox-validated (mechanism); **needs-fleet-confirmation** (ceiling delta) | **in-place tweak** (bounded cores) | **both** (esp. loaded) |
| **O2** | **Journal `SeqWatermark` route** for `append().wait()` (T-7/T-8) — drop the per-append `Notifier` alloc + condvar fan-out. | poll a watermark counter, not a per-op condvar | handoff-tax doc §B; §5.3 | collapses depth-1 p99 (hypothesis); ~1 wakeup/commit | hypothesis (needs `perf`) | refactor | serial/shallow |
| **O3** | **Adaptive spin window** on the cross-process hops (#1/#5/#7/#10): widen `SPIN_TRIES` under sustained load so the consumer rarely parks at the knee, fall back to park when idle. | `BackoffIdleStrategy` ladder (spin→yield→park) | §5.1; §3 | raises ceiling without burning a core when idle | hypothesis | in-place tweak | loaded |
| **C-1** | journal double-copy → `Arc<[u8]>` | flyweight / no redundant copy | §2; §5.2 | **~ns** (hygiene) | sandbox-validated | in-place tweak | both |
| **C-2** | client `copy_from_slice` → `Bytes::from(mem::take)` | zero-copy receive | §2; §5.2 | **~ns** (hygiene) | sandbox-validated | in-place tweak | both |
| **A1** | **Co-locate node+service in one process** with polling agents — removes 2 of the 4 IPC boundaries (#5/#6/#7) and their futex hops; service apply becomes an in-process poll like Aeron's ClusteredServiceAgent. | single-process polling Agents over shared term buffers | §4 side-by-side | removes ~2 inherent wakeups/commit + 2 ring copy-pairs | hypothesis | **long-horizon rewrite** | both |
| **A2** | **Bypass openraft's internal async hops** (T-2/T-3/T-5) — a duty-cycle consensus loop instead of task+channel handoffs. | `ConsensusModuleAgent` poll loop | §2 hops #3/#4/#9; §4 | removes ~3 intra-proc wakeups/commit | hypothesis | **long-horizon rewrite** (fork/replace openraft) | both |

---

## 7 Synthesis

### Actionable tier (inside the current architecture)

- **O1 is the highest-leverage shippable change**: busy-spin (or a wider spin window) the
  intra-host ring consumers the node already owns, so a commit's stage transitions don't pay the
  ~11.7 µs futex park. It is bounded (a handful of hot hops, not the whole system), it is the
  intra-host analog of Aeron's polling consumers, and it is explicitly *not* the settled-negative
  cross-host busy-poll. **O3** makes it idle-safe (adaptive spin→park) so cores aren't burned at
  rest. **O2** is the journal half, already designed in the handoff-tax doc.
- **The copying axis is a near-non-issue** (C-1/C-2): §5.2 shows copies are ~4–40 ns at KV sizes
  vs ~11,700 ns per wakeup. Do them for hygiene, not throughput. They'd only matter for MB-scale
  frames. **This refutes "data copying on the hot path" as a meaningful lever at typical payloads**
  — the CLAUDE.md `Bytes` zero-copy posture is already good enough; the forced ring memcpys are
  cheap.

### Architectural tier (structural gap — long-horizon)

§4 is the core result: **the Aeron gap on these two axes is structural, not a tuning miss.** Aeron
Cluster runs consensus + service as **polling Agents in one process** — a commit crosses **0
parks** intra-host and reads log entries as **flyweights (0 copies)**. UC splits node and service
into **separate OS processes** bridged by futex rings, and routes consensus through **openraft's
async task/channel model** — so each intra-host transition Aeron does as a counter-poll, UC does as
a futex wake+park (~11.7 µs here) or a tokio hop. **A1** (co-locate node+service, poll instead of
park) and **A2** (duty-cycle consensus vs openraft async) are what it would take to truly match
Aeron — both rewrite-class, both giving up correctness-proven seams (the process isolation that
makes service-crash reconstruction and the lincheck story work; the openraft consensus core). Not
recommended unless the low-latency floor becomes a primary product goal.

### What would actually close the ~80 µs-vs-8 ms gap

**Honest bottom line, in order:**

1. **The 8 ms p50 is ~5 ms linger + ~2.7 ms replication + IPC.** At this operating point, *every*
   threading/copying finding here is **<1% of p50** — the ~4 inherent intra-host wakeups total
   ~tens of µs against an 8 ms commit. **Nothing in this investigation moves the headline p50** at
   linger=5ms; the linger knob does (already known, §1).
2. **In a `linger=0` world** (the honest low-latency floor, not yet measured), p50 collapses toward
   **replication RTT (~2.7 ms) + IPC wakeups (~tens of µs) + fsync**. There, replication dominates;
   the intra-host wakeups (O1/O3) are the *second* term and worth reclaiming — but UC stays ~ms
   (replication-bound) vs Aeron's ~80 µs regardless. **Raft replication RTT, not threading, is the
   next floor** — and Aeron Cluster pays that too (its ~80 µs is a tuned single-host/IPC config; its
   cross-host cluster latency is also ms-range).
3. **The ~2× throughput ceiling (~10k/s) is the most tractable real target**, and it *is* partly a
   threading story: at ~100 µs/commit budget, several futex wakeups/commit + openraft's serialized
   apply cap single-core throughput, where Aeron's poll-everything model does not. **O1 + O3 (raise
   the ceiling by not parking under load) are the recommended next experiment**, fleet-confirmed.

**Recommendation:** pursue **O1/O3** (intra-host busy-spin/adaptive-spin, fleet-measured against the
throughput ceiling) and **O2** (journal watermark). Treat copying (C-1/C-2) as hygiene. Shelve the
architectural tier (A1/A2) unless a linger=0, sub-ms latency floor becomes a product requirement —
and even then, recognize replication RTT, not the handoff, is the wall after that.
