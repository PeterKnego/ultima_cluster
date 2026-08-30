# UC v2 — Commit-path network-share decomposition: measurement brief

**Date:** 2026-08-02
**Status:** Draft brief, written as a session handoff — **not yet reviewed, no
measurement approved or run.** The §2 thresholds become a pre-commitment when
this file is committed unchanged ahead of the first fleet run; they may be
adjusted in review before that run, never after seeing data.
**Motivation:** Bound, with measurement rather than argument, what any faster
network technology could possibly buy UC — before building any of it. The
triggering question was "would DPDK or AWS EFA help?"; this brief exists so
that question gets a number instead of an opinion.
**Relationship to other work:** independent of the formal-verification track.
The isolated *technology* comparison (mmsg/GSO/AF_XDP/EFA RTT cells) belongs in
the sibling `hi-perf-cmp` repo's `network-rtt` grid and is explicitly **not**
this work; this brief produces the decide rule that says whether those cells
are worth funding for UC's sake (§7).

---

## 1. The question, and what is already known

At M5 the write path does 1.64 M responses/s at p50 0.600 ms, end to end,
quorum-fsync'd, on a 3-host `c6id.2xlarge` fleet. The commit path crosses the
wire twice per position: leader→follower DATA fan-out, and the follower's
`APPEND_POSITION` report back. Nobody has measured what share of the 600 µs —
or of the throughput ceiling — the network (wire propagation + kernel UDP
stack + syscalls) actually is.

What the repo already knows, and why the prior is "small":

- **The transport is one blocking-free `send_to`/`recv_from` syscall per
  datagram** (`uc_net/src/sender.rs:703`, `receiver.rs:874`) — no
  `sendmmsg`/`recvmmsg`, no UDP GSO/GRO, no `SO_BUSY_POLL`. But datagrams are
  packed MTU-full (`MTU_DEFAULT = 1408`, `uc_protocol/src/v2/datagram.rs:18`),
  so at M5's 64 B payloads a datagram carries ~14 frames and the sender pushes
  only ~120 k datagrams/s per follower. Syscall cost at that rate is real
  (~0.3–0.5 of a core, naively) but not obviously a ceiling.
- **v1-era evidence points away from the wire.** task17 Phase B (busy-polling
  the network datapaths) was a measured NULL — the network was never the
  bottleneck; fsync and IPC dwarfed RTT. The v1 floor decomposition put the
  commit floor at ~73 % software/structural vs ~27 % physical. v2 rebuilt the
  software side; the physics carries.
- **UC has a documented history of locally-real, end-to-end-NULL
  optimizations** (journal `fdatasync`, segment preallocation, SyncCore
  Model-B). Each was built before the bottleneck was established. Kernel-bypass
  networking is precisely the kind of high-glamour, high-cost change that
  produces the next entry in that list if adopted unmeasured.
- **The read-profile arc is the method precedent** (`2026-07-25` spec →
  baseline fleet → Rung A → re-measure): pre-committed decide rule, fleet-only
  numbers, threats stated in advance. Its §4.3 lesson binds this brief's shape
  (§3).

**Explicit non-goal: this is an instrument, not a gate.** No pass/fail exit
code, no bar. The output is a disposition on three pieces of possible work
(§7), any or all of which may be "declined, with the number that declined it".

## 2. Decision rule (pre-committed at approval time)

Two measured quantities, defined so that cross-host clock skew cannot touch
them (§4.1):

- **WIRE(P)** — for a sampled position `P`: the leader-clock interval from
  handing the covering DATA datagram to `send_to` until `recv_from` returns
  the commit-triggering `APPEND_POSITION` covering `P`, **minus** the
  follower-clock interval from that datagram's `recv_from` return until that
  report's `send_to` entry. A difference of two same-clock durations — skew
  cancels. WIRE is both kernel stacks + NIC + propagation for one full
  round trip: exactly and only what a faster transport could remove.
- **SYSCALL occupancy** — directly-timed `send_to`/`recv_from` durations in the
  sender and receiver agents, reported (i) as µs per datagram and (ii) as a
  share of the agent's duty cycle from internal counters (§3).

> **Clause (K) — kernel-bypass class (AF_XDP, DPDK, EFA/SRD) enters a design
> conversation iff both hold on the fleet:**
> **(K-lat)** WIRE p50 share ≥ **20 %** of the leader-observed append→commit
> p50 at the low-load point, **and** WIRE p50 ≥ **80 µs** absolute; **and**
> **(K-cpu)** sender **or** receiver socket-syscall occupancy ≥ **25 %** of
> that agent's duty cycle at the M5-plateau load point.
>
> **Clause (L) — the cheap ladder (`sendmmsg`/`recvmmsg`, UDP GSO/GRO,
> `SO_BUSY_POLL`, jumbo MTU) is justified iff:**
> SYSCALL share ≥ **10 %** of append→commit p50 at low load, **or** socket
> occupancy ≥ **15 %** at plateau.
>
> **Neither → the transport is left alone for LAN.** The verdict is recorded
> as "measured, declined", and any `hi-perf-cmp` network cells proceed on
> standalone comparison value only, not on a UC justification.

Fixed in advance, so the outcome cannot be rationalized afterwards:

- **Borderline (K-lat in 15–25 %, or L within ±3 points):** resolve only with
  a fleet re-run, or treat as not justified. Never resolved on local smoke.
- **Why (K) needs both legs:** (K-lat) alone is a latency claim — but at
  plateau the pipeline hides latency, so a wire share that never shows up as
  agent CPU cannot move throughput, and a p50 win of < 80 µs absolute is under
  the run-to-run noise of the M5 record. (K-cpu) alone is a CPU claim the
  cheap ladder (L) can usually buy back for two orders of magnitude less
  effort — kernel bypass must beat the ladder, not just beat zero.
- **The EFA/DPDK payoff is bounded above by WIRE.** Whatever those
  technologies' relative improvement, they cannot recover more of the commit
  p50 than WIRE's share — fsync, MTU-fill wait, report cadence, and
  duty-cycle latency are all outside their reach. The report states this
  bound as a number.
- **Perturbation guard:** the instrumentation feature itself is A/B'd
  (§4.4). If feature-on throughput deviates from feature-off by more than
  **3 %** at either load point, the run is **INCONCLUSIVE** — the instrument
  measured itself.
- **Contamination guard:** sampled positions whose window saw NAK service or
  retransmit are excluded (counted and reported); a run where exclusions
  exceed **5 %** of samples is INCONCLUSIVE.

## 3. Instrumentation shape — and the departure it requires

The read-profile spec's rule was "attach, never participate": production code
untouched. **This brief cannot honour that rule, and says so up front.** Its
§4.3 measured why: a yield-idling busy-spin agent is externally
indistinguishable from a busy one (2,000,000 `sched_yield()` calls → +0
`voluntary_ctxt_switches`), so *no external proxy* can measure agent occupancy
or in-agent stage timings. That section's own escalation path — "feature-gated
duty-cycle counters in `AgentRunner` … remain available if occupancy is ever
wanted for its own sake" — is exactly this brief. Occupancy is now wanted for
its own sake.

Therefore: a cargo feature (working name **`net-decomp`**), **off by default,
zero-cost when off** — every counter and timestamp behind `#[cfg(feature)]`,
compiled out of every normal build, never enabled by any default test or gate.

What it adds, sampled 1-in-N positions (N an env knob, default in the
implementation plan; sampling bounds observer cost):

- **Leader** (`uc_net::Sender` + the consensus agent in `uc_node`):
  `t_append` (frame published to the log buffer), `t_send` (entry to the
  `send_to` carrying the covering datagram, per follower), `t_report`
  (`recv_from` return of the commit-triggering `APPEND_POSITION`), `t_commit`
  (commit counter advance past `P` — the `rank_leader` crossing).
- **Follower** (`uc_net::Receiver` + archive agent): durations only —
  `d_recv→d_durable` (datagram in → covering fdatasync returned; the archive
  records one fdatasync per block, `uc_log/src/archive.rs` header) and
  `d_durable→d_report` (durable → `APPEND_POSITION` `send_to` entry; the
  report cadence and its `ap_reported` cursor live in
  `uc_net/src/receiver.rs:850`, re-send discipline at `:611`).
- **Syscall timing:** `Instant` pairs around `send_to`/`recv_from` at the
  sampled sites (µs/datagram distribution).
- **Duty-cycle counters in `AgentRunner`** (`uc_log/src/agent.rs`): per-agent
  work-vs-empty poll counts and time-in-socket vs time-in-pack/seal buckets,
  dumped at exit. This is the §4.3 internal-counter escalation, verbatim.

Each role dumps its samples as JSON lines at exit (result-contract style: one
object per line, stderr for logs). Correlation key is the **position**. A
`decide` subcommand replays the collected files through a unit-tested
`evaluate_decision_rule` — the orchestrator never re-implements the rule (the
read-profile precedent: a rule re-implemented outside its tests is no longer a
pre-commitment).

**Harness placement** is an implementation-plan decision, with a default: a
thin `uc_node/examples/net_decomp.rs` reusing the `m5_gate` role split
(`node`/`service`/`client`/`all`/`decide`), because the load whose 600 µs is
being decomposed *is* the M5 load shape and the fleet orchestration already
knows how to drive that shape. Extending `m5_gate` itself with a `--decompose`
flag is the alternative if the example would duplicate too much; either way
the gate's own numbers are never produced with the feature on.

## 4. Measurement design

### 4.1 The skew-free decomposition

Absolute timestamps are never compared across hosts. The commit path
decomposes into named same-clock stages:

| Stage | Clock | What it is | What could shrink it |
| --- | --- | --- | --- |
| `t_append→t_send` | leader | MTU-fill / pack wait + sender duty cycle | batching policy — **not** NIC tech |
| WIRE (defined §2) | derived, skew-free | 2× (kernel stack + NIC + propagation) | mmsg/GSO/busy-poll (partly); AF_XDP/DPDK/EFA (mostly) |
| `d_recv→d_durable` | follower | receiver processing + fdatasync | storage, block sizing — not NIC tech |
| `d_durable→d_report` | follower | report cadence wait | a cadence knob — not NIC tech |
| `t_report→t_commit` | leader | consensus duty-cycle + rank | polling cadence — not NIC tech |

Note the subtraction that defines WIRE automatically *excludes* the follower's
fsync and report-cadence wait — they sit inside the follower-clock interval
being subtracted. This is the load-bearing design decision of the whole brief:
it is what stops the report-cadence knob or a slow fsync from being
misattributed to the network.

Clock drift over a sub-millisecond window is nanoseconds; ignored, stated.

### 4.2 Attribution rules (fixed in advance)

- A position `P` is anchored to the **first** DATA datagram whose byte range
  covers it, and to the **first** `APPEND_POSITION` whose reported position is
  ≥ `P` from the follower whose report **triggered the commit advance** (3-node
  quorum = 2 and the leader's own durable counts in-memory, so the *faster*
  follower gates commit — the M8 gate doc records that the leader's
  self-report never crosses the wire). The slower follower's stages are
  recorded as context, never mixed into WIRE.
- The leader's self-addressed report send (`receiver.rs`
  `seal_and_send(self.cfg.leader, …)`, the M8 `seal_failures` finding) is a
  real syscall on the leader; it is **counted but labelled** in syscall
  occupancy, so the numbers stay explainable if the planned self-send
  suppression follow-up lands between runs.

### 4.3 Load points and arms

Two load points, crypto **OFF** (the default posture; an optional crypto-ON
arm may be recorded as a labelled diagnostic but never feeds the rule):

- **Low-load** — a paced rate well under plateau (latency-clean; queueing ~0).
  This is where (K-lat)/(L)'s *share of p50* is read: at plateau, queueing
  delay dominates every stage and shares are meaningless.
- **Plateau** — the M5 saturation point. This is where occupancy legs
  (K-cpu, L) are read: CPU shares only matter where CPU is the constraint.

### 4.4 The perturbation A/B

Same fleet, same rungs, feature-off vs feature-on, throughput compared. The
3 % guard (§2) is evaluated *before* any decomposition number is read. The
feature-off arm is also this run's tie back to the M5 record: if feature-off
plateau is not within noise of 1.64 M/s on the same hardware class, something
else changed and the run does not speak for the M5 number.

### 4.5 Where measurement happens

The read-profile §3.1 discipline applies verbatim: **the AWS fleet run is the
measurement; local runs verify wiring and produce no numbers.** The dev box is
shared, has no swap, and busy-spin clusters measured beside a neighbour
measure the neighbour. Fleet cost note: this run wants the same 3-host
`c6id.2xlarge` class as M5 and can share a provisioning session with the other
outstanding fleet arms (M8 encrypted/cleartext A/B, M4 failover) to amortize
the spend — one session, separately user-approved, as always.

## 5. Threats to validity

To be answered in the report, not discovered after it:

1. **Observer effect.** `Instant::now()` is ~20–25 ns via vDSO, but the hot
   path is nanosecond-scale ring operations; hence sampling 1-in-N *and* the
   §4.4 A/B guard. If the guard fails, no number survives.
2. **MTU-fill wait masquerading as latency.** At low load a frame may wait for
   its datagram to fill (or for the sender's flush-on-idle). That wait is
   structural batching policy, reported as its own stage
   (`t_append→t_send`), and is *outside* WIRE by construction. A large value
   here is a batching-policy finding, not a network finding.
3. **Report cadence inflating the apparent round trip.** Excluded from WIRE by
   the subtraction (§4.1). If `d_durable→d_report` turns out large, the cheap
   fix is a cadence knob in `uc_net`, and the report must say so rather than
   let the raw leader-observed RTT be quoted as "network".
4. **NAK/retransmit contamination.** A sampled position swept into loss
   recovery measures the repair path, not the steady state. Excluded per the
   §2 guard, with counts reported.
5. **Quorum masking.** Commit is gated by the *faster* follower; a
   decomposition read off the slower one overstates every stage. Fixed by the
   §4.2 attribution rule; both followers reported.
6. **The 64 B regime is the only regime measured.** All shares are claims
   about M5's ops-bound workload. The bytes-bound regime (large payloads) has
   different arithmetic — the 2xlarge's 12.5 Gbps NIC binds first there — and
   is out of scope (§8); the report must scope its verdict accordingly.
7. **Shared-box smoke is not evidence** (§4.5). No local number reaches the
   report or the rule.
8. **The rule's thresholds are drafts until ratified.** This file is a
   handoff; §2 binds only once re-committed (or explicitly approved) before
   the first fleet run. Changing a threshold after data exists voids the
   pre-commitment and the record must say so.

## 6. Output

A report under `docs/benchmarks/uc2-net-decomp-<date>.md`, gate-doc-shaped:

- the full stage table (§4.1) at both load points, p50/p90/p99 per stage,
  both followers;
- WIRE and SYSCALL stated as shares *and* absolute µs, with the EFA/DPDK
  upper-bound sentence written out;
- syscall occupancy per agent from the duty-cycle counters;
- the §2 rule evaluated clause by clause, with an explicit verdict line per
  clause (K, L) naming which leg decided it;
- every threat in §5 addressed with what the run actually showed;
- the perturbation A/B and contamination counts, before any other number.

Index entry added to `docs/BENCHMARKS.md` per house convention.

## 7. What the verdict feeds

Three dispositions, one measurement:

1. **`uc_net` cheap ladder** (mmsg/GSO/busy-poll/jumbo): build iff clause
   (L); otherwise declined with the number.
2. **`hi-perf-cmp` `network-rtt` ladder cells** (`udp_mmsg`, `udp_gso`,
   `udp_busypoll`, `io_uring`, `af_xdp`, `efa_srd`): funded *as UC work* iff
   clause (K) or (L); otherwise they remain available on that repo's own
   standalone-comparison merits. (Note for that repo regardless: its
   network-rtt methodology is one-outstanding ping-pong — a batched-pps mode
   is a methodology addition, and an EFA cell needs an EFA-capable instance
   type in its bench-infra, which `c6id.2xlarge` is not.)
3. **EFA/DPDK/AF_XDP for UC** — a design conversation iff clause (K), and even
   then bounded by the recorded WIRE ceiling and by the standing constraint
   that UC's log-buffer-as-retransmit-buffer + NAK repair is load-bearing,
   proven machinery (SRD's built-in reliable delivery overlaps it
   destructively; any backend would sit behind the sender/receiver seam and
   keep the NAK path for below-floor snapshot upgrade).

## 8. Out of scope

- Any change to transport behavior. This brief produces numbers and a
  disposition, not a faster `uc_net`.
- Implementing any `hi-perf-cmp` cell (separate repo, separate brief, its own
  methodology decisions).
- The bytes-bound / large-payload regime (a separate workload-envelope
  question), WAN/cross-region anything, and the crypto-ON decomposition
  beyond the optional labelled arm.
- The read-path shared ceiling (~540 k reads/s) and its read-only-vs-mixed
  inversion — the other open perf lead, unrelated instrument, separate brief
  if pursued.

## 9. Handoff state (for the next session)

Conclusions already reached in the originating conversation, so they are not
re-derived:

- Packet-rate arithmetic: ~120 k datagrams/s per follower at M5 (64 B
  payloads, MTU-full packing) → syscall cost is sub-ceiling on priors; kernel
  UDP placement-group RTT ~50–100 µs vs EFA ~15–25 µs → naive best-case p50
  win ~50–80 µs of 600 µs. The priors say (K) fails; the priors are exactly
  what the rule exists to check.
- EFA is unavailable on `c6id.2xlarge` (16xlarge/32xlarge/metal-class only),
  so a (K) outcome also multiplies fleet cost before any build.
- The `hi-perf-cmp` routing decision (technology cells there, UC-share
  measurement here) is recorded in this brief's §7 and in project memory.

Open decisions deliberately left to the implementing session:

1. Ratify or adjust the §2 thresholds (then commit before the fleet run —
   that commit is the pre-commitment).
2. Harness placement: new `net_decomp.rs` example (default) vs `m5_gate
   --decompose`.
3. Sampling N, the feature name, and the JSON-line field set.
4. Whether the fleet run shares a provisioning session with the M8 A/B and
   M4 arms (recommended for cost).

Next step per house workflow: review this brief, then
`superpowers:writing-plans` for the instrumented-harness implementation.

## Appendix — code anchors

| Concern | Location |
| --- | --- |
| One `send_to` per datagram, MTU-full packing | `uc_net/src/sender.rs:5`, `:703`, `:833` |
| Sender duty cycle | `uc_net/src/sender.rs:516` (`do_work`) |
| Receiver recv loop | `uc_net/src/receiver.rs:874` |
| `APPEND_POSITION` handler (leader side) | `uc_net/src/receiver.rs:231` |
| Report re-send cadence / `ap_reported` cursor | `uc_net/src/receiver.rs:611`, `:850` |
| Leader self-addressed report (M8 `seal_failures`) | `uc_net/src/receiver.rs:1087` region; M8 gate doc |
| MTU constant (jumbo knob) | `uc_protocol/src/v2/datagram.rs:18` |
| fdatasync per archive block | `uc_log/src/archive.rs` (module header) |
| Commit ranking on the leader | `uc_consensus/src/election.rs:277` (`rank_leader`) |
| Agent runner (duty-cycle counter site) | `uc_log/src/agent.rs` |
| External-proxy impossibility (do not re-attempt) | read-profile spec §4.3 (`2026-07-25-uc2-read-profile-design.md`) |
| Role-split harness precedent | `uc_node/examples/m5_gate.rs`, `read_profile.rs` |
| Fleet orchestration | `bench-infra/scripts/m6_fleet_gate.py` |
