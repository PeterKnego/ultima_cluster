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
> **(b)** `uc2-consensus` or `uc2-receiver` is the **top-occupancy agent** at
> that plateau.

The 70% figure is a pre-commitment, not a target to be adjusted once the numbers
are in. Two tie-break rules, also fixed in advance:

- **Borderline (65–75%)** does not license building on local smoke. Resolve it
  with a fleet run, or treat it as not justified.
- **(b) is a ranking, and ties do not count.** If the top two agents are within
  the run-to-run spread of the yield-rate proxy, clause (b) is unmet and the
  escalation in §4.3 decides it.

Why both clauses:

- **(a) alone is not enough.** A throughput gap at low concurrency is latency
  cost, which pipelining already hides; Rung A amortizes coordination, it does
  not remove a round-trip from a serial dependency chain. Only a gap that
  survives to the *plateau* is a capacity gap.
- **(b) is what predicts the fix works.** If the apply frontier or the egress
  broadcast saturates first, deleting probe traffic entirely moves nothing.

**Outcomes:**

| Result | Disposition |
| --- | --- |
| (a) and (b) | Rung A is justified; proceed to a Rung-A implementation plan. |
| (a) only | Latency-shaped, not capacity-shaped. Rung A shelved; record why. |
| (b) only | The agent is busy with something other than probes — profile *that*. |
| neither | Both rungs null for this workload. The brief gets a "measured, declined" section and the read path is left alone. |

Rung B is out of scope for this decision regardless of outcome — it stays
sequenced behind the Veil V2 coherence-window result, per the brief's §5.

## 3. Harness shape

New example: `uc2_node/examples/read_profile.rs`, following the established
`m5_gate` role split so the same binary serves both the local box and a fleet.

```text
cargo run -p uc2_node --release --example read_profile -- node    --id N --bind A --members … --instance-dir D
cargo run -p uc2_node --release --example read_profile -- service --instance-dir D
cargo run -p uc2_node --release --example read_profile -- client  --instance-dir D --secs S --readers K [--mode lin|snap] [--write-rate W]
cargo run -p uc2_node --release --example read_profile -- all     --secs S   # local smoke, NOT a fleet number
```

- `node` / `service` are thin fleet-role wrappers over the real SDK stack
  (`Node::start`, `ServiceBuilder` over a trivial counter SM), parked forever;
  the harness owns their lifecycle.
- `all` boots 3 nodes + 3 services in-process against real file-backed shmem,
  under a tempdir **guarded off `/tmp`** (RAM-backed tmpfs, no swap — the
  `m4_gate`/`m5_gate` precedent and the standing box rule in CLAUDE.md).
- Env caps `UC2_RP_MAX_SECS`, `UC2_RP_MAX_READERS` clip from above when set
  nonzero; unset is a no-op (the fleet's mode).

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

### 3.2 The client role must bypass `uc2_client`

`uc2_client::query_linearizable` routes through `send_and_await`
(`uc2_client/src/client.rs:154-184`), which **blocks on a channel per call**.
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

### 4.3 Secondary signal — agent occupancy

Every agent thread is already named by `AgentRunner::spawn` via
`thread::Builder::name` (`uc2_log/src/agent.rs:53`): `uc2-consensus`,
`uc2-sender`, `uc2-receiver`, `uc2-archive`, `uc2-apply`. All are ≤15 chars, so
they survive intact in `/proc/<pid>/task/<tid>/comm`. Attribution is free.

**CPU time is the wrong metric here.** The node agents idle on
`IdleStrategy::Yield` (`agent.rs:28` → `std::thread::yield_now()`), so an *idle*
agent still burns a core in a yield loop; CPU% is near-saturated by construction
and carries almost no signal.

**The usable proxy is the yield rate:** `voluntary_ctxt_switches` from
`/proc/<pid>/task/<tid>/status`, sampled at the start and end of the measurement
window. Each `sched_yield` from an empty duty cycle increments it, so

```text
occupancy(agent) ≈ 1 − normalized(voluntary_ctxt_switches per second)
```

A busy agent yields rarely; an idle one yields at its loop rate. Ranking agents
by this is enough to discharge clause (b), which asks only which agent is
*top*-occupancy, not for an absolute duty-cycle percentage.

**Escalation, if the proxy is ambiguous** (e.g. two agents rank within noise):
fall back to a `profiling`-feature-gated counter set in `uc2_node` — probe
sends, acks processed, `ReadPhase` transitions — dumped by the node role at
exit. Deliberately held in reserve: it perturbs the hot path it measures and
enlarges the diff, and the A/B may well settle the question without it.

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
   caused by the harness proves nothing about the node.
4. **Shared-core smoke is not evidence at all here** (§3.1) — the box carries a
   concurrent model-checking session. Local runs verify wiring; the fleet run
   measures. No local number reaches the report.
5. **Yield-rate proxy is ordinal, not absolute.** It ranks agents; it does not
   measure duty-cycle occupancy. Clause (b) is written to need only the ranking.

## 7. Output

A report under `docs/benchmarks/uc2-read-profile-<date>.md`, gate-doc-shaped:

- the concurrency ladder, both arms, both workload mixes (throughput + p50/p99);
- per-agent yield-rate ranking at each plateau;
- the decision rule evaluated clause by clause, with an explicit verdict line;
- every threat in §6 addressed with what the run actually showed.

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
| Query-ring drain + linearizable fork | `uc2_node/src/node.rs:1948-1956` |
| Snapshot read forwarded immediately | `node.rs:1958` (`forward_svc_query`) |
| Per-cycle query admission cap (64) | `node.rs:186` |
| Capture read index `commit_at` | `node.rs:1983` |
| Send probe / follower ack / count acks | `node.rs:1895-1903`, `1910-1918`, `1924-1943` |
| `PendingRead` / `ReadPhase` | `node.rs:204-235` |
| Service publishes `service_applied` | `uc2_service/src/apply.rs:186` |
| Agent thread naming | `uc2_log/src/agent.rs:53` |
| Idle strategy (`Yield` → `sched_yield`) | `uc2_log/src/agent.rs:28` |
| Blocking client query API | `uc2_client/src/client.rs:154-184` |
| Role-split + smoke-mode precedent | `uc2_node/examples/m5_gate.rs` |
| Monotonic-read divergence guard precedent | `uc2_node/examples/m6_gate.rs:418-426` |
