# UC v2 — Aeron-shaped Rust-native SMR (design spec)

**Date:** 2026-07-09
**Status:** approved design, pre-plan
**Supersedes (as direction):** the openraft-based v1 node core. v1 stays in-tree and green until v2 passes its gates (§9).

## 1. Why v2 (context, one page)

v1 (`uc_node` on openraft) is correct — lincheck/hard-crash/partition gates green — but
structurally capped ~13×/14× behind Aeron Cluster on matched hardware and durability:
Aeron ≥800 k msg/s @ p50 0.38 ms *with fsync* vs UC ~56 k @ floor 1.48 ms
(`docs/benchmarks/aeron-parity-scorecard-2026-07-02.md`). Three weeks of systematic
elimination proved the residual gap is architecture, not tuning:

- Floor decomposition: ~73 % of the commit floor is openraft async choreography +
  3-proc IPC; only ~27 % physical (fsync + wire RTT).
- Throughput plateau identical on 8 and 16 vCPU — the single consensus thread touching
  each entry ~5–7 times is the ceiling.
- SyncCore (openraft fork, Model B): well-executed, suite-green, latency-positive in its
  regime — and fleet-throughput NULL. The shape, not the scheduler, is the limit.

`docs/aeron-hot-path-anatomy.md` (3f675f4) is the source-level map of why Aeron wins and
**the reference document for every v2 design decision** ("port the design, not the code";
the Aeron Java tree is the tiebreaker). The three moves v2 adopts wholesale:

1. **Consensus is a control plane.** Replication is a byte-stream fan-out of the log
   itself; acks are coalesced durable-position gossip; the consensus thread touches each
   message once.
2. **Batching is structural, never timer-based.** Batches form from backlog at every
   stage; no linger anywhere.
3. **The node is a pipeline of single-writer polling agents** coordinated exclusively by
   shared-memory position counters.

## 2. Decisions locked (brainstorm 2026-07-09)

| decision | choice |
|---|---|
| openraft | **dropped entirely** — v2 owns replication, commit, elections, membership |
| transport | **own UDP + NAK**, position-addressed; log buffer = retransmit buffer |
| compatibility | **keep the uc_service/uc_client trait contracts; new shmem/wire protocol (v2)** |
| success bar | **≥400 k msg/s, p50 ≤1 ms, fsync on, linearizable, 3×c6id fleet** (stretch: 800 k parity); correctness gates non-negotiable |
| membership | **static voting set in v2.0**; learners (non-voting catch-up) supported; joint-consensus reconfig deferred to v2.x |
| approach | greenfield core in the same workspace ("Approach A"), v1 kept green alongside; Aeron design fidelity as discipline ("C's discipline") |
| encryption/auth | **explicit v2.0 non-goal** — trusted private network posture (same as stock Aeron); a PSK-MAC slot is reserved in the datagram header for later |

## 3. System shape

Three process roles as in v1 (client / node / service; shared instance dir; cnc +
rings + `instance.lock`). The v2 protocol makes the split free: the service polls the
shared log buffer directly, so no hop is added by process separation.

### 3.1 Node = four single-writer polling agents

Plain `std::thread`s, configurable idle strategy (busy-spin / yield / backoff). All
coordination via mmap'd position counters in cnc v2 — no channels, locks, or wakeups on
the hot path.

- **consensus agent** (the only "brain"): polls ingress ring → validates session →
  **one append** to the log buffer per message; drains control messages from the
  receiver's SPSC ring; per duty cycle ranks quorum durable positions → advances commit;
  runs the election SM when needed.
- **sender agent**: scans the log buffer from `sent_position`, packs complete frames
  MTU-full, fans out to followers (one scan, N sends, `sendmmsg`); serves NAKs by
  re-reading the buffer; transmits control messages queued by consensus.
- **receiver agent**: receives datagrams; log frames → position-addressed writes into
  the local log buffer (follower role); control frames → SPSC ring to consensus.
- **archive agent**: polls the buffer from `durable_position`, block-writes ≤1 MiB to
  `ultima_journal`, one `fdatasync` per block, then advances the durable counter.
  **The only fsync site.**

Service process: apply agent (polls committed log in place, calls user `apply`) + the
async output/query machinery. Clients: v1 model (MPSC ingress ring in, broadcast ring
out).

**Embedded mode is a config flag, not an architecture:** apply coordination is entirely
counters over shared memory, so running the service agent as a thread inside the node
process is the same code with a different mmap.

### 3.2 Crates

| crate | contents |
|---|---|
| `uc_protocol` (extended) | v2 layouts as a new module family: cnc v2, log-buffer frame format, position counters, ingress/egress rings, control-frame formats. Stays `no_std`, stays the multi-language gate. |
| `uc2_log` | log-buffer runtime (appender, position-addressed writer, validated readers) + archive agent (journal recording, durable position, replay-from-position). |
| `uc2_net` | UDP transport: sender/receiver agents, fan-out, NAK/retransmit, status/flow control, replay + bulk (snapshot) sessions, built-in fault-injection layer. |
| `uc2_consensus` | **pure, sync, deterministic** consensus SM (commit ranking, elections, truncation, learner admission) — no I/O, no threads, no clock (time injected) — plus the thin agent driving it. |
| `uc2_sim` | deterministic simulation harness for `uc2_consensus` (may live as `uc2_consensus` dev-deps/tests if small). |
| `uc2_node` | composition: agent wiring, discovery dir, instance lock, service/client attach, snapshot/reconstruction orchestration, admission window. |
| `uc_service` / `uc_client` | v2 backends behind the unchanged trait contracts. |
| reused as-is | `ultima_journal` (archive medium + StableValues), `uc-lincheck`, `uc-crashtest` (v2 bins), bench infra + Aeron reference arm. |

## 4. The log buffer, positions, and the archive

**Buffer.** One mmap'd power-of-2 ring per node (default ~512 MiB, configurable) in the
instance dir; byte offset = `position & (size−1)`. Exactly one writer per node, by role:
consensus agent (leader) appends; receiver agent (follower) writes frames at their
position offset — duplicates and reordering are idempotent by construction. Single
writer ⇒ plain stores + one release-store commit; no atomics beyond the commit word.

**Frame format** (`uc_protocol` v2, no_std): 32 B header — `length` (atomic-after-write
commit word; v1 discipline), type/flags, `leadership_term_id`, `session_id`,
`correlation_id` — then payload, 32-byte aligned. Position implicit from offset. Padding
frames absorb the wrap; no frame straddles the buffer end. No CRC in the buffer
(single-host memory); CRC is per journal block.

**Positions.** Absolute u64 byte positions, monotonic forever. A leadership term is
`(term_id, base_position)`; the fsync'd **term map** StableValue records term history —
the RecordingLog analog; vote comparison, divergence detection, and replay reason over
it. cnc v2 counters (cache-line separated, multi-language readable): append, durable,
sent, commit, service_applied, service epoch/seqlock, output_completed, per-follower
observability stats.

**Archive = ultima_journal recording blocks, not messages.** One journal record per
block (≤1 MiB, frame-aligned): `seq` = block index, `meta` = block base position, CRC
per block (amortized — removes v1's per-record CRC cost at the root), one `fdatasync`
per block, durable counter advances after. fsync frequency scales with block rate; the
archive agent's poll batching *is* the group commit — the journal's linger machinery is
bypassed (structural batching, P3). Replay-from-position: binary search block base
positions (via `meta` / block headers).

**Overrun rule — one hard gate, all else degrades.** The appender may never overwrite
bytes the archive hasn't recorded: the single hard backpressure point, surfaced at the
ingress door as admission control. Every other lagging reader degrades: a follower
NAKing below the buffer tail gets a journal replay session; a lagging service switches
to journal replay and rejoins the live buffer. The ring is a fast-path cache over the
journal, never a correctness dependency for readers.

**Truncation** (election-only): `truncate_after` the last fully-valid journal block,
re-append the surviving partial block, reset the buffer append position, update the term
map. Rare-path; correctness gated by the sim.

**Node restart:** prime counters from journal + StableValues; prefill the buffer tail
from the journal (bounded by the retransmit window); rejoin as follower.

## 5. Replication data plane (UDP + NAK)

**Send.** Sender packs complete frames MTU-full (1408 B default; jumbo-frame knob) and
sends the identical datagram to every follower (MDC-style). Datagrams are
**self-locating**: header carries the stream position of the first byte +
`leadership_term_id`. Idle → low-rate heartbeat datagrams carrying the append position
(liveness + progress).

**Receive.** Frames land at `position & mask` in the follower's buffer. Stale
`leadership_term_id` ⇒ dropped. The archive records only the contiguous prefix, so
durability is never fooled by holes.

**Loss → NAK.** A gap persisting past a short randomized delay (~1 RTT) triggers
`NAK(position, length)`; the sender retransmits by re-reading the log buffer at that
position — the buffer is the retransmit buffer. A NAK below the buffer tail upgrades to
a replay session.

**Flow control is quorum-paced, matching commit semantics.** Followers send status
messages (~every quarter-window) advertising contiguous-rebuilt position + receive
window. The sender's limit is the **quorum-th order statistic** over follower windows
(3 nodes: the faster of the two followers). A slow follower never stalls commit the
quorum could legally advance; it recovers via NAK or is demoted to replay.
(Deliberately not min/lockstep.)

**Replay sessions — one mechanism, three uses:** bounded, separately paced journal-read
stream (block-aligned, self-locating, own session id) for (a) a follower below the
buffer, (b) learner/new-node join, (c) post-election catch-up; hands off to the live
stream once within buffer range. The only replication path that reads storage —
steady state never does (P2).

**Control plane rides the same socket** (one UDP socket per node): `AppendPosition`,
`CommitPosition`, `RequestVote`/`Vote`, NAK, status — fixed-size little-endian frames in
`uc_protocol` v2, demuxed by the receiver to the consensus SPSC ring.

**Security posture (v2.0):** no wire encryption/auth; trusted private network assumed.
Reserved header slot for a per-datagram PSK-MAC later. This is a stated decision, not an
omission.

## 6. Control plane: gossip, commit, elections

**Steady state — two message types, per duty cycle, never per message.**
Follower: on durable-position advance (100 ms heartbeat floor) send
`AppendPosition(term_id, durable_pos)`. Leader, once per duty cycle: commit = majority-th
highest of {own durable, reported durables}, bounded by own durable; on advance, store
the cnc commit counter (that *is* the apply notification) and gossip `CommitPosition`
(same floor). Followers apply up to `min(commit, local contiguous durable)`.
**Commit means quorum-fsync'd** — v1 consistent mode semantics. Control traffic is
single-digit kHz regardless of message rate.

**Elections — Raft's safety core over positions, entirely inside `uc2_consensus`.**
Inputs: control messages, injected time ticks, local counter snapshots. Outputs:
messages + actions (persist vote, truncate to X, open term). The agent performs I/O;
the SM never does.

- Liveness: `CommitPosition`/heartbeats double as leader liveness; randomized timeout →
  candidacy.
- Vote rule: `RequestVote(new_term_id, last_leadership_term_id, last_durable_position)`;
  grant iff the term is new, no conflicting vote this term (vote persisted to StableValue
  **before** answering), and the candidate's `(last_term, durable_pos)` ≥ ours
  lexicographically. Only durable positions count — a crash discards the non-durable
  tail anyway.
- New leader: `term_id++`, `base_position` = own **durable** position (any local bytes
  beyond durable are discarded when opening the term — only durable bytes are the log);
  immediately
  appends a **NewTerm no-op frame and waits for it to commit before serving anything**
  (Raft §5.4.2 leader completeness).
- Reconciliation: leader ships its term map; a diverged follower truncates to the last
  common `(term, base)` prefix (only ever uncommitted bytes, by vote/commit safety),
  then catches up via NAK/replay.

**Linearizable reads (ReadIndex analog):** capture commit position C; confirm leadership
with one nonce'd heartbeat round (nonce echoed in `AppendPosition`); wait
`service_applied ≥ C`; query; validate with v1's service seqlock (epoch unchanged across
the query). Leader leases: v2.x.

**Membership:** static voting set from config (v2.0). Learners attach via replay +
live stream, no vote — covers "replace a box." Joint-consensus reconfiguration: v2.x.
(Temporary capability regression vs v1; accepted 2026-07-09.)

**Failure modes (same theorems as v1):** minority partitions can't advance commit; a
stale leader fails read confirmation (no stale reads); vote safety forbids split-brain;
quorum loss fails cleanly (submissions error/timeout, no phantom commits).

## 7. Apply path, service & client SDK (protocol v2)

**Apply = the service polls the log.** The service mmaps the buffer (read-only) + cnc.
Apply agent: `while service_applied < min(commit, contiguous_durable)` → read frame →
user's unchanged `fn apply(&mut self, position, cmd)` → advance counter. No apply ring,
no per-entry handoff.

- **One deliberate copy at the apply boundary:** payload copied out of the mapped buffer
  into `Bytes`, then validated against the append position (seqlock-style; on wrap-over,
  re-read — from the journal if overrun). Borrowed views into an overwritable ring would
  be unsound; copying at KV sizes measured as noise (2026-06-21 investigation).
- A lagging service degrades to direct read-only journal replay (shared instance dir)
  and rejoins the live buffer when caught up — same mechanism as reconstruction.

**Responses bypass the node.** Client stamps `(session_id, correlation_id)`; consensus
carries them in the frame header; the service echoes them + position onto the **egress
broadcast ring** directly (leader-only; follower offers are no-ops). No per-message
oneshots; the client matcher correlates off the ring.

**Ingress & admission:** v1-style MPSC ring; admission control becomes a **position
window** — submissions stall when `append − commit` exceeds the inflight-bytes budget
(v1's overload lesson in native coordinates).

**Queries:** typed `Query`/`QueryResponse` unchanged; linearizable path per §6 over a
small SPSC query ring; snapshot-consistency reads skip the barrier.

**OutputHandler — v1 contract verbatim** (async, leader-only, at-least-once, position as
idempotency key): service advances an `output_completed` cnc counter; the node persists
it periodically to the `output_progress` StableValue; leader transition replays
`(marker, commit]`. Monotonic persistence can only widen replay — at-least-once holds.

**Reconstruction (task14 semantics, simplified):** service restart bumps the epoch; the
service finds its own frontier (`store.latest_version()` / snapshot position), replays
the journal directly to commit, joins the live buffer. Snapshots: service-built
(`ultima_db::snapshot_stream` wire format kept) to a file at position S; marker
registered; journal purge gated below it; learner install = bulk snapshot session + tail
replay. `build_snapshot`/`install_snapshot` still return the position `u64`.
`app_id`/`instance_id`/`protocol_version` checks kept at every IPC entry.

**Unchanged non-negotiables:** apply is sync, deterministic, no I/O, no clock;
`AppCommand = Bytes`; reads typed, not closures.

## 8. Testing strategy

**L1 — deterministic simulation** (`uc2_sim`, exists before the first networked
election): N SM instances, one thread, virtual time; seeded-random delay/drop/dup/
reorder; injected crashes/restarts (StableValue state survives); archive progress as
events. Invariants after every step: election safety (≤1 leader/term), term-map prefix
consistency, commit monotonicity, committed-never-truncated, leader completeness. Modes:
seeded fuzz (thousands of seeds in CI), pinned regression seeds, scripted nasties (split
votes, asymmetric partitions, crash-during-truncate, stale-leader reads).

**L2 — component:** `uc2_log` wrap/padding, overrun gate, block boundaries, truncate +
partial re-append, replay seek, torn-tail recovery; `uc2_net` localhost harness with the
fault layer built in from day one (native to own-UDP), NAK under sustained loss, window
pacing, replay→live handoff. **loom** on the frame-commit word + counter visibility;
**miri** over unsafe mmap code (v1's cnc-align UB is the precedent).

**L3 — v1 correctness harness, ported not rebuilt:** uc-lincheck WGL capstone under
churn + leader kills; `lin_partition`'s four scenarios re-driven through the UDP fault
layer; uc-crashtest SIGKILL (service and node) mid-load on v2 bins. Same theorems v1
proved; the harness carrying over is the biggest de-risker in the plan.

**L4 — measurement gates** (bench infra + Aeron reference arm, same fleet; every
milestone lands with its measurement doc). Default `cargo test` = L1–L3 in-process;
crashtest/fault behind features; clippy `-D warnings`.

## 9. Milestones (build order = risk order)

| # | deliverable | gate |
|---|---|---|
| M1 | `uc2_log`: buffer + archive, single node | ≥1 M/s 64 B append+record+fsync solo |
| M2 | `uc2_net`: replication stream, 3 hosts | ≥100 MB/s per follower, durable positions keeping pace, resilient to 0.1–1 % injected loss |
| M3 | static-leader commit pipeline, 3 nodes | **go/no-go: ≥400 k committed/s, p50 ≤1 ms, fsync on** — before elections or SDK exist |
| M4 | elections + term map + truncation | sim + partition lincheck green; sub-second LAN failover |
| M5 | SDK/apply/protocol v2 e2e | full lincheck + crashtest green; client→apply→response ≥400 k @ p50 ≤1 ms (headline bar) |
| M6 | snapshots, learners, purge, ops | learner join under load; purge safety; reconstruction under load |

If M3 misses badly, stop and re-diagnose before building M4–M6 — the exact inversion of
v1, where the ceiling was discovered after everything was built. Each milestone gets its
own implementation plan (`docs/superpowers/plans/`); v1 retires only after M5+M6 hold
the bar.

## 10. Risks & open questions

- **Elections/truncation are the hardest 20 %** (leadership terms over positions,
  reconciliation, crash-during-truncate). Mitigation: pure-sync SM + L1 sim before any
  networked election; Aeron's Election/RecordingLog and the Raft paper as references.
- **Quorum-paced flow control** interacting with replay demotion needs careful liveness
  reasoning (no oscillation between live/replay) — sim + L2 scenarios.
- **Position-seek in the journal** (block base-position index) is a small new journal
  requirement; if `meta`-based binary search is awkward, add a sparse index sidecar —
  decide in the M1 plan.
- **Buffer prefill on restart** (how much tail to rehydrate) and learner snapshot
  transfer pacing: sized during M4/M6 plans.
- Multi-language client/service bindings (uc_protocol v2 gate) are a stated eventual
  goal; v2.0 ships Rust only.
