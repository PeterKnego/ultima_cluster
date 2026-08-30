# UC v2 — Linearizable-read profile harness: design spec

**Date:** 2026-07-25
**Status:** Design spec, approved for implementation.
**Motivation:** Decide, with measurement rather than argument, whether the
ReadIndex barrier costs read *capacity* — the gating open question (§6.1) of the
leader-lease brief, `docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md`.
**Branch:** `worktree-uc2-read-profile`, based on `main` @ `1b31e91`.
**Relationship to other work:** independent of the Veil V2 forward hunt running
in a separate session (that work lives in `proofs-veil/` and the separate
`veil-2.0-preview` checkout; this touches neither).

---

## 1. The question, and why it is asked first

The leader-lease brief proposes two optimizations to the linearizable read path:
**Rung A** (batch-probe coalescing, clock-free) and **Rung B** (a time-based
leader lease, which puts a bounded-clock-drift assumption into read safety). The
brief's own §6.1 says to measure before building either: *is per-read probe
traffic actually a throughput bottleneck, or are the single-writer agents
bottlenecked elsewhere (apply frontier, egress broadcast)?*

Nothing in the repo answers that today. `m5_gate` is write-path only (`type Query
= ()`, never driven under load); `m6_gate` and `m7_gate` issue linearizable reads
only as an occasional *serial* divergence guard. There is no concurrent read load
anywhere, so there is no read-throughput number to reason about.

This matters more than usual here. UC has a documented history of optimizations
that were locally real and end-to-end **NULL** — journal `fdatasync`, segment
preallocation, the SyncCore Model-B latency work. Each was built before the
bottleneck was established. This harness exists so Rung A does not join that
list.

**Explicit non-goal: this is an instrument, not a gate.** It has no pass/fail
exit code and claims no bar.

## 2. Decision rule (pre-committed)

Fixed before any run, so the outcome cannot be rationalized after the fact:

> **Build Rung A iff both hold:**
> **(a)** the linearizable-read throughput plateau is **≤ 70%** of the
> snapshot-read plateau at matched concurrency; **and**
> **(b)** the gap is present in the **read-only arm** (that arm's lin/snap ratio
> independently satisfies clause (a)'s ≤ 70% threshold), AND at that plateau the
> client sustained **≥ 90% of target concurrency**, AND neither arm is degraded
> per the >5% retries/redirects guard.

The 70% figure is a pre-commitment, not a target to be adjusted once the numbers
are in. One tie-break rule, also fixed in advance:

- **Borderline (65–75%)** does not license building on local smoke. Resolve it
  with a fleet run, or treat it as not justified.

**Why (b) is a real second criterion and not (a) restated:**

- The **read-only** requirement is the substantive part. With no writes in
  flight the service frontier is already caught up when a read is admitted, so
  the lin-vs-snap delta there is the barrier alone. A gap that appears *only* in
  the mixed arm is frontier-wait cost, not barrier cost — exactly the
  misattribution clause (b) exists to catch, and a case where Rung A would move
  nothing.
- The **sustained-concurrency** requirement uses the in-flight depth sampling of
  §6 threat 3: if the client was the ceiling, the plateau describes the harness,
  not the node.

The concurrency floor is gated on the **mean** sampled depth, not the minimum: a
minimum is a single 10 ms sample and is dominated by scheduler noise (one
descheduling of the single send thread drives it to zero), whereas "sustained the
target concurrency" over a throughput plateau is a claim about the window. The
minimum is reported alongside as context.

A verdict of "clause (b) unmet" always names **which** sub-condition failed
(read-only gap absent / concurrency not sustained / read-only arm degraded).

A third guard, added around — not inside — the pre-committed thresholds:

- **A degraded arm cannot certify a plateau.** A linearizable rung that collapses
  into `MSG_V2_RETRY` (read-barrier timeout, momentary `!can_serve`) resolves few
  genuine reads across the full elapsed window, which is a *low* ratio — the same
  shape as the result that justifies building. An arm whose
  `(retried + not_leader) / (reads + retried + not_leader)` exceeds **5%** yields
  `INCONCLUSIVE` instead of any verdict, so the failure mode and the build signal
  stay distinguishable.

Why both clauses:

- **(a) alone is not enough.** A throughput gap at low concurrency is latency
  cost, which pipelining already hides; Rung A amortizes coordination, it does
  not remove a round-trip from a serial dependency chain. Only a gap that
  survives to the *plateau* is a capacity gap.
- **(b) is what predicts the fix works.** If the gap is really the frontier wait,
  or if the ceiling was the load generator, deleting probe traffic moves nothing.

**Outcomes:**

| Result | Disposition |
| --- | --- |
| (a) and (b) | Rung A is justified; proceed to a Rung-A implementation plan. |
| (a) only, read-only gap absent | The gap is frontier-wait cost, not barrier cost. Rung A moves nothing; record why. |
| (a) only, concurrency not sustained | The plateau is the harness's, not the node's. Re-run with a stronger load generator before ruling. |
| neither | Both rungs null for this workload. The brief gets a "measured, declined" section and the read path is left alone. |

Rung B is out of scope for this decision regardless of outcome — it stays
sequenced behind the Veil V2 coherence-window result, per the brief's §5.

### 2.1 Amendment, 2026-07-25 — clause (b) reformulated

**This amendment was made before any measurement data existed.** No `client`,
`all`, or `ladder` run had produced a number that was recorded anywhere, and the
fleet run had not been scheduled; the only runs executed were local wiring
verification whose numbers §3.1 forbids from reaching the report. That provenance
is the point of a pre-commitment: the record must show the rule was changed
because its instrument was broken, not because a result was unwelcome.

**What changed and why.** The original clause (b) — "`uc2-consensus` or
`uc2-receiver` is the top-occupancy agent" — depended on the §4.3 yield-rate
proxy, which was **measured non-functional** (see §4.3). A yield-idling agent is
indistinguishable from a busy one at the OS level, so *no* external proxy can
rank these agents; the clause was unanswerable as written, not merely
noisy.

**What did not change.** Clause (a)'s 70% threshold, the 65–75% borderline band,
the borderline-before-justified ordering, and the >5% degraded guard are all
untouched. Only clause (b)'s content was replaced, with a formulation that
discharges the same job — *rule out that something other than the barrier
explains the gap* — from data the harness already collects.

**What was dropped.** The "ties do not count" tie-break went with the ranking it
broke ties in; there is no ranking left for it to apply to.

## 3. Harness shape

New example: `uc_node/examples/read_profile.rs`, following the established
`m5_gate` role split so the same binary serves both the local box and a fleet.

```text
cargo run -p uc_node --release --example read_profile -- node    --id N --bind A --members … --instance-dir D
cargo run -p uc_node --release --example read_profile -- service --instance-dir D
cargo run -p uc_node --release --example read_profile -- client  --instance-dir D --secs S --readers K [--mode lin|snap] [--write-rate W] [--node-pid P] [--service-pid Q]
cargo run -p uc_node --release --example read_profile -- all     --secs S   # local smoke, NOT a fleet number
cargo run -p uc_node --release --example read_profile -- decide  --rungs FILE [--write-rate W]
```

- `node` / `service` are thin fleet-role wrappers over the real SDK stack
  (`Node::start`, `ServiceBuilder` over a trivial counter SM), parked forever;
  the harness owns their lifecycle.
- `all` boots 3 nodes + 3 services in-process against real file-backed shmem,
  under a tempdir **guarded off `/tmp`** (RAM-backed tmpfs, no swap — the
  `m4_gate`/`m5_gate` precedent and the standing box rule in CLAUDE.md).
- Env caps `UC2_RP_MAX_SECS`, `UC2_RP_MAX_READERS` clip from above when set
  nonzero; unset is a no-op (the fleet's mode).
- Each `client` run prints one **rung JSON line** to stdout alongside its human
  report. On the fleet the ladder is one `client` process per rung under external
  orchestration, so `decide --rungs FILE` replays those lines through the *same*
  `evaluate_decision_rule` the unit tests pin. The orchestrator never
  re-implements the rule — a rule re-implemented outside its tests is no longer a
  pre-commitment.

### 3.1 Where measurement happens — AWS, not this box

**The AWS fleet run is the measurement. Local runs verify wiring and produce no
numbers.** This is stronger than the usual "local smoke is directional"
disclaimer, for two reasons:

1. **The box is shared.** A concurrent session runs the Veil V2 model-check;
   during this spec's authoring `lean` held 384% CPU and 7.2 GB RSS at load
   average 4.2 with ~7 GB available. A busy-spin/yield cluster measured
   alongside that is measuring the neighbour.
2. **The structure is wrong anyway.** One box shares its cores across 3 nodes'
   worth of polling agents plus 3 services plus the load generator; the M5 gate
   doc's disclaimer applies verbatim.

Consequences, binding on the implementation:

- Local runs exist to prove the harness resolves reads, guards monotonicity, and
  tears down cleanly. **No local run produces a row in the report, and no local
  run evaluates the decision rule for the record.**
- Local runs stay small and short (seconds, few rungs, reduced log-buffer size)
  to avoid contending with — or OOM-killing — the neighbouring session. The box
  has no swap; an OOM SIGKILLs the largest process and can take a session with
  it.
- The 3-host AWS fleet run is where the ladder is swept for real. It costs money
  and requires explicit user approval before `terraform apply`.

### 3.2 The client role must bypass `uc_client`

`uc_client::query_linearizable` routes through `send_and_await`
(`uc_client/src/client.rs:154-184`), which **blocks on a channel per call**.
Read concurrency is the independent variable of this entire experiment, so a
one-read-per-thread API would cap the very axis being swept.

The client role therefore does what `m5_gate`'s already does for writes: opens
the same cnc page, `query.ring`, and egress broadcasts a real client would, but
stamps its own `local_seq` and correlates responses through a preallocated slot
array. This is legitimate because the query ring carries the same correlation
shape as ingress — `(client_id, local_seq)` in `header_extra`
(`node.rs:1955`) — so pipelined reads are a supported use of the ring, not a
hack around a limitation.

`--readers K` sets the number of concurrent in-flight reads (the ladder axis),
independent of OS threads.

## 4. Measurement design

### 4.1 Primary signal — the A/B

Run the identical workload twice, toggling only `FLAG_V2_LINEARIZABLE`.
`node.rs:1956` is literally the fork: with the flag clear the query is forwarded
straight to the service; with it set the read takes the nonce + `send_read_probe`
+ `AwaitQuorum` path. Same admission, same drain cap, same service, same egress.
**The delta between the two arms is the barrier's end-to-end cost** — no
instrumentation required, and nothing in production code is touched.

### 4.2 The ladder

Sweep `--readers` until **both** arms plateau. A single concurrency point cannot
distinguish latency cost from capacity cost, which is exactly the distinction
clause (a) of the decision rule turns on. Report the full curve, not just the
peak.

### 4.3 Agent occupancy — MEASURED NON-FUNCTIONAL, retained as diagnostic only

**Status (2026-07-25): this section's metric does not work, and clause (b) no
longer depends on it.** The text below records what was tried, what was measured,
and why no external proxy can succeed — kept rather than deleted so the failure
is not re-attempted.

Every agent thread is already named by `AgentRunner::spawn` via
`thread::Builder::name` (`uc_log/src/agent.rs:53`): `uc2-consensus`,
`uc2-sender`, `uc2-receiver`, `uc2-archive`, `uc2-apply`. All are ≤15 chars, so
they survive intact in `/proc/<pid>/task/<tid>/comm`. Attribution is free — the
*attribution* was never the problem.

**The original plan.** CPU time is unusable: the node agents idle on
`IdleStrategy::Yield` (`agent.rs:28` → `std::thread::yield_now()`), so an *idle*
agent still burns a core in a yield loop and CPU% is saturated by construction.
The intended substitute was the yield RATE — `voluntary_ctxt_switches` from
`/proc/<pid>/task/<tid>/status`, differenced across the measurement window — on
the premise that each `sched_yield` from an empty duty cycle increments it, so a
busy agent yields rarely and an idle one yields at its loop rate.

**The premise is false.** Measured twice, independently:

| Probe | `sched_yield()` calls | Δ `voluntary_ctxt_switches` | Δ `nonvoluntary` |
| --- | --- | --- | --- |
| Rust/Python, 2 s | 1,483,000 | **+1** | +34 |
| C, direct | 2,000,000 | **+0** | +677 |

`sched_yield` leaves the task `TASK_RUNNING`, so the kernel accounts any
resulting switch as *non*voluntary — and with no other runnable task on the CPU
it often performs no switch at all. The nonvoluntary deltas are ordinary
preemption noise, not duty-cycle signal. A local `ladder` run shows the
consequence directly: every node agent reports ~0 yields/s, and the ranking
degenerates into the sampler's `(pid, tid)` tie-break.

**No external proxy can work here, and this is structural.** A yield-idling agent
and a fully busy agent are indistinguishable from outside the process: both burn
100% of a core, neither ever blocks, and neither reaches a scheduler state the
kernel exposes differently. `nonvoluntary_ctxt_switches` measures how often the
scheduler preempted the thread — a function of system load, not of the agent's
duty cycle. There is no third field that separates them.

**The only true occupancy metric would be internal.** Feature-gated duty-cycle
counters in `AgentRunner` — work-done vs. empty-poll counts per duty cycle,
dumped by the node role at exit — would measure occupancy directly and correctly.
That means changing `uc_node`, which this instrument deliberately does not do
(the whole design is "attach, never participate"), so it remains available if
occupancy is ever wanted for its own sake. It is **not** needed for the decision:
clause (b) was reformulated (§2.1) to be answerable from data the harness already
collects.

**What the harness still does.** The sampler is retained and is correct as far as
it goes — keyed by `(pid, tid)` rather than by thread name, and sampling the union
of the node and service PIDs. Two things it must get right, kept because they are
real bugs if regressed:

- **Samples are keyed by `(pid, tid)`, never by thread name.** Agent names are
  static, so any process running more than one node has several threads called
  `uc2-consensus`. A name-keyed before/after join differences unrelated threads;
  the mis-paired rows saturate to zero and then sort to the *front* of an
  ascending ranking, impersonating the busiest agent.
- **The service's PID must be sampled too** (`--node-pid` *and* `--service-pid`).
  On a fleet the service is a separate process, so a node-only sample contains no
  `uc2-apply` at all.

Its output is printed under an explicit caveat stating that near-zero rows are
the expected reading and that it does **not** feed the decision rule. The
`profiling`-gated escalation described above remains the fallback if a future
question genuinely needs per-agent occupancy.

## 5. Workload arms

Four runs: {read-only, mixed} × {linearizable, snapshot}.

- **Read-only** — the clean isolation. With no writes in flight,
  `service_applied >= commit_at` already holds when the read is admitted, so the
  frontier wait is free and the A/B delta is the barrier and nothing else.
- **Mixed** — reads plus a fixed background write rate (`--write-rate W`). The
  realistic case: the frontier wait becomes live, and the probe path now
  contends with replication traffic on the same agents.

## 6. Threats to validity

To be stated in the report, not discovered afterwards:

1. **A snapshot read skips more than the barrier.** `node.rs:1958` forwards it
   immediately via `forward_svc_query`, bypassing *both* the probe round and the
   frontier wait. The delta is therefore attributable to the barrier alone only
   in the read-only arm; in the mixed arm it is barrier + frontier wait, and the
   report must say so rather than quoting one number.
2. **`QUERY_DRAIN_PER_CYCLE = 64`** (`node.rs:186`) caps query admission per
   duty cycle and is itself a candidate ceiling. The A/B controls for it — both
   arms cross the same drain — but **if both arms plateau at the same number,
   that cap is the suspect, not the probe**, and the finding is "read admission
   is drain-capped", which is a different (and cheaper) fix than either rung.
3. **The load generator may be the bottleneck.** Report client-side in-flight
   depth and confirm the target concurrency is actually sustained; a plateau
   caused by the harness proves nothing about the node. Implemented as a 10 ms
   sampler over `sent - resolved` across the send window (the first 100 ms
   discarded as pipeline fill), reporting **mean and minimum** depth against the
   target per rung — a mean at target hides a stall, and "sustained" is the word
   the threat uses. This matters concretely: the client's send path is a single
   thread and its matcher is a single thread taking a histogram lock per
   response, so at high `--readers` the *client* is a plausible ceiling — and if
   both arms hit it the ratio goes to 100% and the barrier reads as free (a false
   negative). `evaluate_decision_rule`'s equal-plateau note names the load
   generator alongside `QUERY_DRAIN_PER_CYCLE` for this reason.
4. **Shared-core smoke is not evidence at all here** (§3.1) — the box carries a
   concurrent model-checking session. Local runs verify wiring; the fleet run
   measures. No local number reaches the report.
5. **The yield-rate proxy measures nothing** (§4.3, measured 2026-07-25:
   2,000,000 `sched_yield()` calls → +0 `voluntary_ctxt_switches`). Its output is
   printed as a labelled diagnostic and must not be quoted as occupancy or used
   to attribute a bottleneck to an agent. Clause (b) does not read it (§2.1).
6. **The two arms are not symmetric under back-pressure: a snapshot read can be
   DROPPED where a linearizable read is RETRIED.** This is production behaviour,
   not a harness defect, and it biases the A/B in the direction that makes the
   barrier look free.

   `drain_query_ring` forwards a snapshot read immediately and **discards
   `forward_svc_query`'s return value** (`node.rs:1956-1959`): if the `svc_query`
   ring is full at that instant, the query is gone — no retry, no answer, no
   `MSG_V2_RETRY`. The linearizable path does the opposite. A pending read whose
   forward fails **restores the query bytes and retries on the next duty cycle**
   (`node.rs:2101-2111`), so a full ring costs it latency, never the query.

   Consequences for the measurement:

   - The snapshot arm can lose queries permanently under saturation. Because the
     client's send governor paces on `sent - resolved`, a lost query never
     reopens its slot: the governor's window closes by exactly the number lost,
     the snapshot arm's offered load decays, and its measured rate falls. The
     lin/snap ratio then rises — potentially **past 100%**, i.e. "the barrier is
     free" or better — as an artifact of the snapshot arm being throttled by its
     own losses.
   - **A snapshot rung with `inflight_at_end != 0` is INVALID, not merely
     suspicious.** For the linearizable arm an unresolved tail can be a slow
     drain; for the snapshot arm it is the signature of exactly this drop, and
     the rung must be discarded rather than recorded with a caveat.
   - **Local wiring runs cannot surface this.** Filling `svc_query` requires the
     service to fall roughly 30k reads behind the node's drain — reachable at
     fleet read rates, not at the rates a shared dev box produces. Absence of the
     symptom locally is not evidence of its absence on the fleet.

   The harness does not paper over it: the client emits `inflight_at_end` per
   rung and `evaluate_decision_rule` prints an explicit note whenever the ratio
   exceeds 100%. Fixing the asymmetry would mean changing `src/`, which this work
   does not do — it is recorded here so the fleet report reads a ratio > 100% as
   a measurement artifact rather than as a result.

## 7. Output

A report under `docs/benchmarks/uc2-read-profile-<date>.md`, gate-doc-shaped:

- the concurrency ladder, both arms, both workload mixes (throughput + p50/p99);
- per-rung health: retries, redirects, unresolved-at-end, and the sustained
  in-flight depth (mean and minimum) against target — a rung that fails these is
  not a data point;
- the decision rule evaluated clause by clause, with an explicit verdict line;
  clause (b) names which sub-condition decided it;
- every threat in §6 addressed with what the run actually showed;
- the yield-rate diagnostic may be included only if labelled non-functional per
  §4.3; it is not evidence about any agent.

No exit code, no bar, no PASS/FAIL — the verdict is a disposition on Rung A.

## 8. Testing

The harness is test scaffolding, so the bar is that it does not lie:

- `all`-mode smoke must complete with zero unresolved reads (an unresolved-read
  tail silently inflates throughput — the `m5_gate` in-flight-at-end lesson).
- A correctness assertion in the linearizable arm: reads must be monotonic
  against the concurrent write stream (the `m6_gate` divergence-guard pattern),
  so a mis-wired harness that reads stale state fails loudly rather than
  reporting a flattering number.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- No change to any existing test's behavior; production code is untouched.

## 9. Out of scope

- Implementing Rung A or Rung B. This spec produces a decision, not a change to
  the read path.
- Rung B's clock-drift analysis and its Veil/Lean interaction — sequenced behind
  the V2 forward hunt.
- The two Veil §5 directed Rust checks (self-removed-leader step-down window;
  adopt-requires-committed-prefix). Related to the read path and worth doing, but
  a separate piece of work.
- Cross-region / WAN reads, DR, and any stale-read mode — out of scope in the
  leader-lease brief and out of scope here.

## Appendix — code anchors

| Concern | Location |
| --- | --- |
| Query-ring drain + linearizable fork | `uc_node/src/node.rs:1948-1956` |
| Snapshot read forwarded immediately | `node.rs:1958` (`forward_svc_query`) |
| Per-cycle query admission cap (64) | `node.rs:186` |
| Capture read index `commit_at` | `node.rs:1983` |
| Send probe / follower ack / count acks | `node.rs:1895-1903`, `1910-1918`, `1924-1943` |
| `PendingRead` / `ReadPhase` | `node.rs:204-235` |
| Service publishes `service_applied` | `uc_service/src/apply.rs:186` |
| Agent thread naming | `uc_log/src/agent.rs:53` |
| Idle strategy (`Yield` → `sched_yield`) | `uc_log/src/agent.rs:28` |
| Blocking client query API | `uc_client/src/client.rs:154-184` |
| Role-split + smoke-mode precedent | `uc_node/examples/m5_gate.rs` |
| Monotonic-read divergence guard precedent | `uc_node/examples/m6_gate.rs:418-426` |
