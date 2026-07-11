# UC v2 — M5 & M6 Design Sketches

> **Status: DESIGN SKETCHES, not implementation plans.** These are forward-visibility outlines written after M3 merged (2026-07-11), deliberately BEFORE M4 exists. Interfaces named here that M4 will create are provisional; every "open question" section lists what must be resolved by the preceding milestone before the full plan (M1–M3 style: complete code, TDD tasks) can be written. Expect the full plans to supersede details here without ceremony.
>
> Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` §7 (apply/SDK), §9 (gates), plus §6 (reads), task14/task12 in `docs/tasks/` (the v1 semantics being ported). Milestone gates: **M5 = full lincheck + crashtest green; client→apply→response ≥400k msgs/s @ p50 ≤1 ms — THE HEADLINE BAR. M6 = learner join under load; purge safety; reconstruction under load.**

---

## M5 sketch — SDK, apply path, protocol v2 IPC (spec §7)

**One sentence:** the service polls the shared log buffer in place (no apply ring), responses bypass the node via an egress broadcast ring, and the v1 trait contracts (`StateMachine`, `OutputHandler`, typed queries) get a v2 backend — proven by the ported v1 correctness harness and the headline throughput bar.

### Scope (what lands)

1. **cnc v2 + shared mappings.** The M1–M4 heap-side `LogCounters` becomes the mmap'd cnc page: `repr(C)` layout frozen (append, durable, sent, commit — M3 order — plus new: `service_applied`, `service_epoch`/seqlock word, `output_completed`, per-follower observability slots per spec §4). `app_id + instance_id + protocol_version` header checked at every attach (v1 rule, kept verbatim). The log-buffer FILE (M1's `create_file`/`open_file`) is mapped read-only by the service; instance dir layout + `instance.lock` (exclusive flock on node, shared probe on service/clients — v1 mechanics) land here, completing what `uc2_node` M4 deliberately omitted.
2. **Apply agent (service side).** Sync loop: `while service_applied < min(commit, contiguous_durable)`: read frame → **one deliberate copy** out of the mapped ring into `Bytes` → seqlock-validate against `append` (the M2 `read_frame_validated`/`read_run_validated` discipline — on `Overrun`, degrade to direct read-only journal replay, shared instance dir, and rejoin the live buffer when caught up) → dispatch by frame type: MESSAGE → user's unchanged `fn apply(&mut self, position, cmd) -> Response`; NEW_TERM/PADDING → skip; advance `service_applied` (Release). **The M3/M4 restart contract binds here:** commit is monotonic within-run only — after a node restart the apply agent must tolerate transient `commit < service_applied` (wait, never error), and the clamp `min(commit, contiguous durable)` is enforced HERE, not at the counter.
3. **Egress: responses bypass the node.** Client stamps `(session_id, correlation_id)` (already in every frame header since M1); the service echoes them + position onto a broadcast ring (v1's Broadcast ring type from `uc_protocol`, ported to the v2 cnc layout); leader-only — follower services compute but don't publish (spec: "follower offers are no-ops"). No per-message oneshots anywhere.
4. **Ingress + admission.** v1-style MPSC ring clients→node; the consensus agent (M4's ingress channel replaced by the ring) applies the admission window vs commit — the M3 knob (`admission_kib`), now enforced at the ring door as backpressure (v1's overload lesson in native coordinates).
5. **Queries.** Typed `Query`/`QueryResponse` (never closures). Snapshot-consistency reads: served directly by the service. Linearizable reads (spec §6): capture commit C → nonce'd heartbeat round (nonce echoed in AppendPosition — a small M4-frame extension) confirms leadership → wait `service_applied ≥ C` → query → validate with the service seqlock (epoch unchanged across the query — v1 task14's TOCTOU close, kept).
6. **OutputHandler, v1 contract verbatim.** Async, leader-only, at-least-once; service advances `output_completed` (cnc counter); node persists it periodically to an `output_progress` StableValue; leader transition replays `(marker, commit]`; position = the idempotency key. Monotonic persistence can only widen replay.
7. **L3 harness, ported not rebuilt.** `uc-lincheck` WGL capstone (in-memory RegisterSm) under churn + leader kills over the v2 stack; `lin_partition`'s four scenarios re-driven through the M4 partition handles; `uc-crashtest` SIGKILL (service and node) mid-load on v2 reference bins (`kv_node`/`kv_service`/`kv_client` examples get v2 backends). **This carried harness is the single biggest de-risker in the whole v2 program** (spec §8).
8. **`m5_gate`:** client→apply→response round trip, 3×c6id, ≥400k msgs/s @ p50 ≤1 ms fsync-on — the project's headline. Local smoke will be core-starved (M2/M3 precedent); the doc discipline (drain-inclusive clock, honest FAILs, admission sweep) carries over wholesale.

### Likely task shape (10–12 tasks)

cnc v2 layout + attach checks (core-only, pinned bytes) → instance dir + locks → apply agent + journal-degrade path → egress broadcast + client matcher → ingress ring + admission-at-the-door → queries (snapshot then linearizable + nonce frame) → OutputHandler + output_progress + transition replay → uc_service/uc_client v2 backends behind the unchanged traits → lincheck port → crashtest port → m5_gate + doc.

### Open questions (must be answered by M4-as-built)

- **Nonce plumbing for ReadIndex:** does the nonce ride AppendPosition (header reserved bytes?) or a new kind? Depends on M4's final control-frame shapes.
- **Who owns `service_applied` visibility to consensus** (the reconcile-driver analog: node notices a lagging/restarted service) — consensus agent polls it, or a dedicated watcher? Decide against M4's real duty-cycle budget.
- **Legacy-path deletion:** M4 keeps the M3-mode sender/receiver paths for old gates; M5 deletes them — confirm nothing but the retired m2/m3 gates still uses them.
- **Embedded mode** (spec §3: "a config flag, not an architecture") — same code, service agent as an in-process thread; needs the cnc mapping to be equally happy backed by a private mapping. Cheap if designed in from the cnc task; confirm no fs-only assumption creeps in.
- **`FollowerConfig`/addressing residue:** M4's addr-keyed membership at the demux edge — M5's client/service attach needs the instance-dir identity story (app_id/instance_id) unified with it.

### Risks

The headline bar is the whole project's bet — M3's evidence (734k committed/s core-starved, latency = queueing not floor) is encouraging but the apply+egress round trip adds two more hops and the service process. The v1 lincheck harness port is the schedule risk (task14-era subtleties: seqlock reads, service epochs, reconstruction interplay) — budget it as the largest task, not an afterthought.

---

## M6 sketch — snapshots, learners, purge, ops (spec §7/§9)

**One sentence:** service-built snapshots make journal purge safe and learner join possible; the ring's degradation story gets its final backstop; the system becomes operable.

### Scope (what lands)

1. **Snapshots, service-built.** `build_snapshot` streams the SM state (default `StoreStateMachine` adapter: `ultima_db::snapshot_stream` wire format kept end-to-end — the multi-version compatibility story from v1) to a file tagged with position S; returns S (the v1 u64-return rule, kept: resolves the race between decision and call). `install_snapshot` returns the post-install position. Snapshot markers registered with the node (cnc slot + StableValue).
2. **Purge, gated below the snapshot.** `Journal::purge_before` (exists) driven by the node once a snapshot at S is durable: purge blocks entirely below S. `PositionPurged` (M1's error, so far untestable) finally gets real: replay below the purge point → snapshot install + tail replay. The M1-triaged `blocks_recorded` rename lands here (`next_block_seq`).
3. **Learner join** (spec §6: static VOTING set; learners attach without vote — "covers replace-a-box"). Bulk snapshot session (the replay-session mechanism's third use: block-aligned, self-locating, own session id, separately paced) + tail replay + live-stream handoff. Learner = a Node role that never votes and never counts for quorum/flow.
4. **Reconstruction under load** (task14 semantics, simplified by v2's shape): service restart bumps the epoch; the service finds its own frontier (`store.latest_version()` / snapshot position), replays the journal directly to commit, rejoins the live buffer. The M4 `NoCommonPrefix` fatal and the M4-cut buffer prefill both resolve here: prefill sized (or permanently rejected — measure whether replay-on-demand suffices) and snapshot-install becomes the no-common-prefix answer.
5. **Ops.** Observability counters into the cnc stats section (per-follower durable/reported/naks — spec §4 lists them); the M2-triaged operational polish (m3/m4 gate admission-loop deadlines etc.); runbook notes (the fleet bind-IP footgun and friends promoted from benchmark docs into an ops doc).
6. **Gates:** learner join under load (no quorum stall, bounded catch-up); purge safety (a purged follower recovers via snapshot+tail, lincheck stays green); reconstruction under load (v1 task14's capstone equivalent on v2).

### Likely task shape (8–10 tasks)

snapshot wire plumbing (service-side build/install behind the trait, position-tagged) → snapshot marker + purge driver on the node → PositionPurged → snapshot-session fallback in the sender → learner Node role (no vote, no quorum, no flow membership) → learner join flow (bulk session + handoff) → service reconstruction path (epoch bump + frontier find + journal replay + rejoin) → prefill decision (measure, then implement-or-reject) → ops/observability sweep → m6 gates + docs → v1 retirement checklist (spec §9: v1 retires only after M5+M6 hold the bar).

### Open questions (must be answered by M4/M5-as-built)

- **Snapshot transport pacing** vs live-stream flow control sharing one socket (spec §5 says separately paced — concretely how the sender arbitrates duty-cycle budget between live, replay, and bulk sessions).
- **Purge coordination across roles:** leader purges below its snapshot, but followers' journals purge independently — the term map + snapshot markers must agree on the lowest replayable position cluster-wide (this is where M4's `NoCommonPrefix` fallback becomes reachable and must flip from Fatal to snapshot-install).
- **Learner in `CommitTracker`/flow:** excluded from quorum by construction (not in `members`) but the sender must still fan out + flow-account it — likely a second follower list ("replicated-to, not counted"), decide against M4's real FlowControl shape.
- **Epoch/seqlock final form** for the service (M5 defines it; M6's reconstruction leans on it).
- **How much of v1's task14 reconcile-driver is still needed** when the service polls the log itself (v2 removes the apply ring that caused most of task14's races) — re-derive the TOCTOU inventory rather than porting assumptions.

### Risks

Purge is the one operation that destroys information — every M6 bug class is "purged something someone still needed." The mitigations are the gates themselves (purge safety under lincheck) plus keeping purge OFF by default until the M6 gate holds. Learner pacing interacting with quorum flow control is the liveness-reasoning hotspot the spec already flags (§10).

---

## Sequencing reminder

Fleet runs first (M2 gate + **M3 go/no-go admission sweep** — if M3 misses badly on c6id, stop and re-diagnose before ANY of this), then M4 (plan exists: `2026-07-11-uc2-m4-elections.md`), then the full M5 plan written against M4-as-built, then M6 against M5-as-built. v1 stays green and retires only after M5+M6 hold the bar (spec §9).
