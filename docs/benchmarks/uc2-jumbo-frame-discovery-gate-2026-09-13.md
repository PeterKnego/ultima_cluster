# uc2 jumbo-frame discovery gate — run 2026-09-13

**Bars committed:** 2026-09-12 (as `…-gate-2026-09-12.md`, the pre-commitment
plan 2 Task 5 wrote). **Fleet run: 2026-09-13**, 4 × c6id.2xlarge, us-east-1a
(the 2.11.0 gates' shape), driver `bench-infra/scripts/jumbo_gate.py` at
`cf2ab77` (rows b and d implemented for the run; the pre-commitment's versions
were print-only stubs). The file was renamed to the run date and its links
repointed, as the pre-commitment said to do. Every result cell below now
points into [Results](#results); the bar column is byte-identical to the
pre-commitment.

**Verdicts in one line:** rows **a** and **d** PASS; row **b**'s three
functional clauses hold and its throughput clause is **inconclusive** (the
pre-committed pair rule asked for 29 pairs, 12 were run, the paired mean sits
0.8 pp below the bar inside a ±15 pp band) — recorded as the driver's FAIL,
bar unmoved; row **c** NOT RUN (its blackhole probe cleared; the soak's
instrument was never built — the maintainer's call, 2026-09-12); rows **e**
and **f** reported, no bar. The driver's own exit verdicts: `--arms a,c,d`
→ `RESULT: NOT RUN (exit 3)` (row c's stub outranks the two passes);
`--arms b` → `RESULT: FAIL (exit 1)`.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell **UNRUN**, before any fleet run against
> it — the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13/M14/M14c2 and every 2.11.0/2.12.0 gate skeleton
> since (FSM identity, time-and-timers, the log-clock gate). Nothing in the
> bar table may be edited to match a result: a run that misses a bar is
> recorded as a **FAIL** and the bar is **kept, unmoved**. This document
> itself is a PRE-COMMITMENT, not a record — its own commit message says so —
> and must not be read as "gated" until a fleet run fills in the results table
> below.

## What the gate measures

Spec:
[`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md`](../superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md)
§10 ("Proof") — this document reproduces its fleet-gate table (rows a-f)
**verbatim**, plus its "Errata (plan 1, as built)" section, which changes
what two of those rows must expect. Plan 1
([`2026-09-10-uc2-jumbo-frame-discovery-plan1.md`](../superpowers/plans/2026-09-10-uc2-jumbo-frame-discovery-plan1.md))
shipped discovery itself: a fixed ladder of UC datagram sizes
(`RUNGS = [1408, 8832, 8960]`), a per-peer prober on every node, the
leader's commit rule that raises the cluster-wide `datagram_mtu` only once
every member has proven a rung, and do-not-fragment on the replication
socket so an oversize send fails by name instead of fragmenting silently.
Plan 2
([`2026-09-12-uc2-jumbo-frame-discovery-plan2.md`](../superpowers/plans/2026-09-12-uc2-jumbo-frame-discovery-plan2.md))
adds `force_jumbo_frames` (a startup fail-stop gate), the join-time
`path_below_committed_mtu` refusal, the developer notification, the seven
`uc2_*` metric series (eight, after the review added
`uc2_jumbo_gate_pending`), two alerts, a fuzz target, and — this task — the
pre-committed gate itself. Plain-language explainer: [Jumbo frames and
path-MTU discovery,
explained](../notes/uc2-jumbo-frame-discovery-explained.md); the spec and
this section are the normative source for the numbers.

**Two facts plan 1 learned that this gate must respect, carried forward
from the spec's own "Errata (plan 1, as built)" section:**

1. **Errata 1 — a narrow cluster probes forever, and that is not a leak.**
   The stop condition needs BOTH `verified == MTU_BOUND` for a peer AND that
   peer's own advertised minimum caught up to the top rung; on a cluster
   with one permanently narrow path neither ever happens, so every node
   keeps probing every peer at the 30 s cadence forever (2-3 datagrams per
   30 s per peer). **Row b must expect `uc2_probe_sent_total` to keep
   climbing throughout the run — a flat series is the anomaly, not a
   passing one.**
2. **Errata 4 — a solo cluster never raises; discovery needs a second
   member.** `ProbeTable::table_min(&[])` is `None`, not `Some(MTU_BOUND)`:
   an empty peer set is no evidence, not universal evidence, because the
   rung is monotone and a top-rung commit on zero measurements would be
   irreversible. **Row a needs at least 2 nodes before any raise is
   observable at all** — a rep run against a solo node is not a miss, it is
   not a measurement.

**Row d's refusal names are corrected to the as-built strings, not the
spec table's prose.** Spec §10's table (and §6's prose) name the two
`force_jumbo_frames` refusals `JumboPathTooNarrow`/`JumboPeerSilent` —
Rust enum-variant style. What plan 2's Task 2 actually ships, pinned by
`uc_node/tests/jumbo.rs` and the `obs_event!` call sites in `node.rs`, are
the lower `snake_case` reason strings **`jumbo_path_too_narrow`** and
**`jumbo_peer_silent`** — this is the string a fleet run's log/audit trail
actually contains, so row d's bar is stated in that form below. For
completeness (not because row d exercises it): Task 2 also ships a THIRD
refusal, **`path_below_committed_mtu`** (the join-time gate, spec §5.4),
which underwent its own fix rounds after the plan was written
(`.superpowers/sdd/2026-09-12-uc2-jumbo-frame-discovery-plan2/progress.md`,
Task 2 rulings; `task-2-report.md` §"The Important — refuse only a peer past
its fast ladder") — as built, it refuses **only** a peer that **answered**
below the committed rung **and** has spent its fast ladder (~5 attempts);
a silent peer never fail-stops there, it only holds `can_serve` false until a
quorum of voters has proven the rung (`JOIN_CHECK_WINDOW` from the original
plan was deleted; the quorum pass replaced a later unproven pass).
Row d's own two arms are both `force_jumbo_frames` (`Forcing`) arms; neither
exercises `Joining`/`path_below_committed_mtu`, so that gate's as-built
behaviour changes nothing about row d's bar — it is recorded here only so a
reader of this doc does not carry the spec's pre-fix wording into row d by
association.

**Coverage statement.** This gate measures fleet-scale discovery
convergence, the force gate's refusal behaviour, one throughput bar and two
reported (no-bar) hop costs. It is not a substitute for the correctness
tier: probe/ack encode-decode, the Settings v1/v2 decoder, the FSM's
`max()` rule and the leader's commit rule are unit-tested; the three-node
fault-layer test (`uc_net::fault::FaultConfig::max_datagram`) proves
convergence onto a capped rung, that the ceiling never rises while one
member is silent, both `force_jumbo_frames` refusals under a real cap, the
join-time refusal against a committed rung above the cap, and a client
seeing the raised ceiling through cnc after a live raise; `uc_protocol_probe`
covers the fuzz tier. See spec §10 for the full unit/fault-layer/fuzz
breakdown and `docs/VERIFICATION.md` §11 for what none of it covers once
this gate lands.

## The bar

Pre-committed, copied verbatim from spec §10's table with the two errata
folded in as call-outs (not as bar changes) and row d's refusal names
corrected to the as-built strings (see above). Rows a-d are fleet rows; row
c also carries a pre-arm blackhole probe (spec §10's closing paragraph).
Rows e and f are **reported, no bar**.

| row | arm | bar | result |
|---|---|---|---|
| a | AWS, 3 voters, interface MTU 9001 as provisioned | every node reports `uc2_datagram_mtu_bytes = 8960` within 10 s of the last node's start, 3 of 3 reps (errata 4: needs >= 2 nodes before any raise is observable) | **PASS** — 3/3 reps, every node at 8960 by +4.9 s ([results](#row-a)) |
| b | 1500 B path (interface MTU forced to 1500 by ansible on the same fleet) | rung stays 1408 on every node; `uc2_send_emsgsize_total = 0` throughout; `uc2_probe_sent_total` keeps CLIMBING throughout (errata 1 — not a leak); 64 B throughput paired delta against the base tree within −3 %, pair count fixed from the base tree's observed spread per the 2026-08-31 lesson, minimum 5 pairs | functional clauses **HOLD**; throughput clause **INCONCLUSIVE** (driver: FAIL, "only 12 pairs, need ≥ 29"; mean −3.81 %, sem 7.62 pp) — bar unmoved ([results](#row-b)) |
| c | the envelope-map brief's §6 disposition, run as written on the AWS arm | jumbo soak plateau ≥ 15 % over standard **and** the jumbo 64 B rung within −3 % of standard (throughput and p99) — this decides whether the runbook *recommends* jumbo, not whether the feature ships | **NOT RUN** — blackhole probe cleared (all nodes 8960 within 30 s); the soak's instrument does not exist ([results](#row-c)) |
| d | force gate on the 1500 B arm; then one node held down on the 9001 arm | all three nodes refuse **`jumbo_path_too_narrow`** within 30 s naming a peer; the two live nodes refuse **`jumbo_peer_silent`** naming the third | **PASS** — 3/3 and 2/2, every refusal at its node's own 30 s window ([results](#row-d)) |
| e | appender relaxed load | `apply_bench`-style isolated A/B is the wrong harness (the appender is leader ingress); `m5_gate` on the fleet, standard arm, paired against the base tree — reported, **no bar**: rate bars for a one-load change cannot be resolved by this rig (the FSM-identity gate's row a straddled the same bar), and the number is recorded for the next gate to pair against | **REPORTED** — −1.23 % (sem 4.53 pp, 3 pairs) ([results](#row-e)) |
| f | client-hop cost of the per-submit cnc load | `scripts/hop1_ab.sh` with its same-source rebuild control, dev-box **smoke**, reported with the control's resolution, **no bar** | **REPORTED** — −2.29 % against a −0.68 % control, ranges overlap ([results](#row-f)) |

Row c carries a path-MTU blackhole probe before the arm, as the envelope-map
brief requires: with do-not-fragment in place (spec §4.3) that probe is
UC's own discovery ladder — a jumbo arm whose nodes still report
`uc2_datagram_mtu_bytes = 1408` after **30 s** aborts the arm loudly rather
than running the soak against a path that never carried jumbo at all.

### Reading the rules

**Row a** is a pure convergence-and-timing check: every member must reach
the top rung, and it must do so inside the window measured from the LAST
node's start (not the first) — a cluster whose slowest-to-boot member takes
9 s to bind still has only 1 s left on the clock for every pairwise probe
round to land. Fewer than 2 members in a rep is not a smaller version of the
measurement, it is no measurement (errata 4): `ProbeTable::table_min` on an
empty peer set answers "no evidence", not "top rung by default", precisely
so the grow-from-one path (`add-learner`, promote) cannot commit an
irreversible top-rung raise on zero measurements.

**Row b's four clauses are independent and all four must hold.** The rung
must never rise (a 1500 B path cannot carry a jumbo rung, so any rise is a
correctness bug, not a slow convergence); `uc2_send_emsgsize_total` must
stay at zero throughout, because a DATA send refused for size is a path
that degraded BELOW the committed rung — which the rung itself never lowers
(this is also `Uc2PathBelowMtu`'s predicate); `uc2_probe_sent_total` is
EXPECTED to climb without bound for the run's whole duration (errata 1) —
recording that as a leak would be recording the documented cost as a defect;
and the throughput clause is a fixed **−3 %** bar, not a same-source-rebuild
resolution like the time-and-timers/log-clock gates' null bars — it says
"jumbo discovery costs the steady-state path at most 3 % versus not having
it," which needs enough pairs to tell a real 3 % from noise. **The pair
count is not fixed in this document**: it is derived, per the 2026-08-31
core-count-sweep lesson (CLAUDE.md: "fix a spread bar's rep count from
observed arm-to-arm variance … n=4 cannot tell width from tail"), from a
preliminary same-source measurement of the base tree's own spread, the same
arithmetic the time-and-timers gate used to explain why its row a needed
"≈430 reps" to resolve a 1.12 % bar from an observed 13.364 % arm sem:
`required_pairs = ceil(observed_n * (observed_stat_pct / target_stat_pct) ** 2)`,
floored at **5**. `bench-infra/scripts/jumbo_gate.py`'s `required_pairs`
implements exactly this and is pinned by `--selftest`.

**Row c is the envelope-map brief's own disposition, quoted verbatim**
([`docs/superpowers/specs/2026-08-02-uc2-envelope-map-brief.md`](../superpowers/specs/2026-08-02-uc2-envelope-map-brief.md)
§6):

> Jumbo MTU (9001) becomes the recommended fleet configuration in the
> runbook iff both hold: the jumbo arm's soak-sustained bytes-bound plateau
> exceeds the standard arm's by **≥ 15 %**, **and** the jumbo 64 B rung is
> within **−3 %** of the standard 64 B rung (throughput and p99). Otherwise
> jumbo remains a documented knob.
>
> Borderline (10–20 % on the first clause) is resolved only by a re-run or
> treated as not justified — never by local smoke.

Read "within −3 %" per-metric, not as one signed number: throughput must
not be more than 3 % LOWER on jumbo, and p99 latency must not be more than
3 % HIGHER — both are "no worse than 3 %" in that metric's bad direction.
**This row decides a documentation recommendation, not whether the feature
ships** — spec §10 says so explicitly, and it is why row c carries a
verdict but is not gate-fatal the way rows a/b/d are: a miss here means
"the runbook keeps calling jumbo a documented knob," never "the feature is
reverted." `bench-infra/scripts/jumbo_gate.py`'s `verdict_row_c` implements
the two-clause comparison and the 10–20 % borderline call-out; the pre-arm
blackhole-probe abort is `check_blackhole_probe`.

**Row d exercises `force_jumbo_frames`'s `Forcing` gate only, on two
separate arms.** The first arm runs all three nodes with the flag set on the
1500 B path: every node answers every peer, but below the jumbo minimum
(8832), so all three refuse `jumbo_path_too_narrow` naming a peer — which
peer is unconstrained (the offender list is sorted by member id and the
FIRST offender is named, so it is deterministic per run but not
prescribed here). The second arm runs on the 9001 (jumbo-capable) path with
one node never started: the two live nodes never hear from the third at
any rung, so both refuse `jumbo_peer_silent` naming it — and because the
offender-selection is deterministic by member id, both live nodes must name
the SAME third node. Both refusals must land within the **30 s**
`JUMBO_GATE_WINDOW` (spec §6, unconfigurable). `verdict_row_d` checks all of
this, including that the two live nodes agree on the named peer.

**Rows e and f carry no bar and are not adjudicated pass/fail.** Row e
reuses `m5_gate` on the fleet rather than `apply_bench`, because the change
under test (one relaxed load on the leader's append path — the do-not-
fragment socket option plus the per-pass probe/ack bookkeeping) lives on the
appender, which is leader INGRESS, not the FSM apply hop `apply_bench`
isolates; the FSM-identity gate's row a already showed a one-load change's
rate bar sits below this rig's resolvable floor, so the number is recorded
for pairing rather than gated. Row f is `scripts/hop1_ab.sh` dev-box smoke
with its own same-source rebuild control (the M14b lesson: measure the
harness's build-to-build resolution before trusting any delta against it).

## Results

**Run 2026-09-13.** Fleet: 4 × c6id.2xlarge, us-east-1a, on-demand, cluster
placement group, journals on instance-store NVMe (`/opt/bench` ext4), chrony
in `Normal` on every host; interface `ens5` at MTU **9001** as provisioned.
Head tree: `main` at `3d5c585` (jumbo + the quorum join gate + the bounded
crashtest waits, on top of the log clock), rsynced to `/opt/bench/uc`;
`uc2-node` built on each host, sha256 `f275f60a7ab6…` on all four. Base
tree for rows b and e: `f46db43`, the last commit before jumbo, at
`/opt/bench/uc-base`. Full transcript: the controller's
`~/scratch/fleet-2026-09-13/` (`RUN-RECORD.md`, `jumbo-acd.log`,
`jumbo-b.log`, `rowe.log`, `rowf.log`, `hop1-ab.log`).

**Resolution on the day: 0.27 %** — `scripts/hop1_ab.sh` on node0, 6 reps,
two builds of the same source at two different absolute paths (sha256
`e7f1aa4e88df…` vs `065455d293b0…`, distinct — the 2026-09-07 lesson), A mean
2 542 881 vs B mean 2 549 773 resp/s, ranges overlap.

### Row a

**PASS.** Three cold starts from wiped instance dirs; every node reported
`uc2_datagram_mtu_bytes = 8960` within the window. The times below are
**upper bounds set by the harness, not adoption instants**: the driver
scrapes the three hosts sequentially, one ssh round trip (~1.6 s) each, and
every node was already at 8960 on the *first* scrape of the loop — so the
three columns are one, two and three round trips after the last start, which
is also why they repeat across reps. What the run shows is that all three
nodes had adopted the top rung within ~1.7 s of the last node's start; where
inside that window each one adopted, this harness cannot say.

| rep | node0 | node1 | node2 |
|---|---|---|---|
| 1 | +1.7 s | +3.3 s | +4.9 s |
| 2 | +1.7 s | +3.3 s | +4.8 s |
| 3 | +1.7 s | +3.4 s | +4.9 s |

(times from the last node's start, each an upper bound as explained above).

### Row b

Interface `ens5` forced to **1500** on all three voters for the whole row
(driver `force_interface_mtu`, restored to 9001 afterwards and verified).

**Functional clauses — all three HOLD.** Over a 60 s sample on a fresh
real-daemon cluster (33 samples, 0 scrape misses): `uc2_datagram_mtu_bytes`
read **1408** on every sample of every node; `uc2_send_emsgsize_total` read
**0** throughout; `uc2_probe_sent_total` climbed fleet-wide from **53 to
501** and never went flat — errata 1's "a narrow path probes forever" as
predicted, not a leak.

**Throughput clause — INCONCLUSIVE, recorded as the driver's FAIL; the bar
is not moved.** The pre-committed rule fixes the pair count from the base
tree's own spread: four base-only prelim reps read 1 600 937 / 1 417 225 /
1 101 075 / 1 556 142 ops/s (sem/mean **7.96 %**), so `required_pairs`
asked for **29**. **Twelve were run, and that cap is a deviation to own,
not a property of the procedure:** the pre-committed step 2(d) has only a
floor of 5, and the `--pairs-max` default of 12 was added by the driver
commit made for this run (`cf2ab77`), chosen for fleet time (~2 min per
pair) and not raised when the prelim spread turned out to call for 29. The
verdict's "need ≥ 29" is computed from the spread independently of the cap,
so the shortfall is stated truthfully; a re-run with `--pairs-max 29` (~1 h
of fleet time) is what it would take to give this clause a verdict. On the
observed sem, 29 pairs would land near 4.9 pp — likely still inconclusive
against a 3 % bar, but that is a projection, not a measurement. Twelve
interleaved pairs (base first on odd pairs) on `m12_gate` clusters, 64 B,
direct client on the leader host, the driver's standard 2 s warm-up / 8 s
window:

| pair | base | head | delta |
|---|---|---|---|
| 1 | 1 996 504 | 1 006 282 | −49.6 % |
| 2 | 1 238 318 | 1 742 156 | +40.7 % |
| 3 | 1 144 442 | 1 009 673 | −11.8 % |
| 4 | 1 538 236 | 959 321 | −37.6 % |
| 5 | 1 249 496 | 1 489 307 | +19.2 % |
| 6 | 1 656 554 | 2 019 138 | +21.9 % |
| 7 | 1 843 960 | 1 339 429 | −27.4 % |
| 8 | 1 458 862 | 1 301 701 | −10.8 % |
| 9 | 1 225 742 | 1 162 843 | −5.1 % |
| 10 | 1 445 260 | 1 429 036 | −1.1 % |
| 11 | 1 289 862 | 1 233 851 | −4.3 % |
| 12 | 1 148 397 | 1 381 224 | +20.3 % |

Paired mean **−3.81 %**, sem **7.62 pp** (2·sem = 15.2 pp). The mean sits
0.8 pp below the −3 % bar, inside 2·sem by a factor of ~19. The base tree
alone spans 1.10–2.00 M ops/s across its 16 reads on this rig, in the same
family as the 15–43 % arm-to-arm spread the 2.11.0 time-and-timers gate hit.
This is not a pass and not a demonstrated regression: at 12 pairs the rig
did not resolve a −3 % rate bar, and the pre-committed rule says so rather
than letting a noisy mean through. Unlike the log clock's bar (0.27 %,
unreachable at this spread — see that gate doc), a 3 % bar at 29 pairs is a
feasible run that simply was not made. The driver's verdict text is the
honest one: `only 12 pair(s), need >= 29`.

### Row c

**NOT RUN.** The pre-arm blackhole probe ran for real on a fresh cluster: all
three nodes cleared the jumbo minimum inside the 30 s window (every one read
8960). The soak beyond it is the envelope-map brief's own multi-rung
instrument (`uc_node/examples/envelope_map.rs`, brief §3), which was never
built; the maintainer chose on 2026-09-12 to record the row as NOT RUN rather
than build it inside the release, since the row decides only whether the
runbook *recommends* jumbo. Jumbo stays a documented knob.

### Row d

**PASS.** Arm 1 (`force_jumbo_frames = true` on all three, `ens5` at 1500):
n0, n1, n2 fail-stopped `jumbo_path_too_narrow` naming peers 1, 0, 0. Arm 2
(`ens5` back at 9001, two nodes forced, the third never started): n0 and n1
fail-stopped `jumbo_peer_silent`, both naming node 2.

| arm | node | reason | named peer | elapsed |
|---|---|---|---|---|
| 1500 | n0 | `jumbo_path_too_narrow` | 1 | +18.1 s |
| 1500 | n1 | `jumbo_path_too_narrow` | 0 | +23.9 s |
| 1500 | n2 | `jumbo_path_too_narrow` | 0 | +29.9 s |
| 9001 − one | n0 | `jumbo_peer_silent` | 2 | +23.9 s |
| 9001 − one | n1 | `jumbo_peer_silent` | 2 | +29.9 s |

**Reading the elapsed column.** It is measured from the *last* unit's start
to the refusal record's own `ts_ns`. The units are started one ssh round
trip apart (~6 s), so n0's and n1's readings understate their own age by
roughly 12 and 6 s: each node fired at its **own** 30 s window, which is the
product's `JUMBO_GATE_WINDOW`, armed at the first consensus pass. For the
*last*-started node the reference is its own start to within the
`systemd-run` return, so its 29.9 s is a real reading against a bar that is
exactly the product's own window — the clause has no headroom by
construction and is a boundary check the harness resolves to about ±0.1 s.
A 30.1 s reading for n0 or n1 would be the reference artefact; for the last
node it would be a genuine miss. What row d proves, and what its PASS means,
is that both refusals fire **by name, at the window, naming the right
peer** on every node; the "within 30 s" clause is met at the boundary, and a
re-run on a slower control path should read it with that in mind.

### Row e

**Reported, no bar.** `m5_gate` standard arm (admission 64 KiB, inflight
4096, 15 s, 64 B), head vs the pre-jumbo base tree, three interleaved pairs
(head/base/base/head/base/head): head 1 007 403 / 1 008 652 / 973 162, base
1 063 229 / 935 632 / 1 038 034 ops/s; paired delta **−1.23 %**, sem
4.53 pp, against the day's 0.27 % resolution. Recorded for the next gate to
pair against. (The `m5_gate` binary's own `RESULT` line reads FAIL on every
run of *both* trees — that is the 2026-08-15 M5 engine bar it carries, not
this row's question.)

### Row f

**Dev-box smoke, no bar.** `scripts/hop1_ab.sh`, 6 reps, 64 B, sink = A.
Control (A = base `f46db43` vs A2 = the same source built at a different
path): **−0.68 %**, ranges overlap. Row f (A vs B = head): **−2.29 %**,
ranges overlap; A mean 5 703 244, B mean 5 572 458 resp/s, with one of B's
six runs an outlier at 4 870 812 against 5.59–5.76 M on the other five.
About three times the control's resolution, with an outlier: the per-submit
cnc load's cost is not resolvable from this smoke, and per CLAUDE.md's
standing rule it carries no bar.

### What this run changes, and what it does not

- Discovery converges (a), the force gate refuses by name on both arms (d),
  a narrow path pins the rung with no EMSGSIZE and no probe leak (b's
  functional clauses). Those are the feature's correctness claims on a real
  fabric, and they hold.
- Row b's −3 % rate bar was not resolved at the 12 pairs run (29 were
  called for); a 29-pair re-run is feasible and was not made. The log clock's
  0.27 % bar is the one this rig's spread cannot reach at any practical rep
  count — the standing bar question from the 2.11.0 gates, unchanged.
- Whether the runbook should *recommend* jumbo (c) is unanswered; the
  runbook keeps jumbo as a knob.

## When this gate is run

**The driver's exit code carries the worst finding across every requested
arm — read it before reading anything else** (fix round 1, plan 2 Task 5
review): `jumbo_gate.py --fleet --arms ...` exits **0** only when every
requested arm produced a passing `Verdict`; **1 (FAIL)** when any requested
arm's `Verdict` missed its bar (a bar miss, or row c's pre-arm blackhole
probe aborting — which now includes a host whose `/metrics` never answered at
all, not only one stuck at the baseline); **3 (NOT RUN)** when at least one
requested arm produced no `Verdict` at all (a print-only stub, per this
driver's current scope) and none of the others FAILED. NOT RUN is **3, not
2**, because argparse exits **2** on a usage error (a bad `--arms`, an unknown
flag) and a wrapper reading `$?` must not confuse "you typed it wrong" with "an
arm produced no verdict". FAIL always outranks NOT RUN, which always
outranks PASS (`exit_code_for_results`, pinned by `--selftest`), so a CI
step or a human reading only `$?` can never mistake a stub run for a pass —
which is exactly the confusion the fix exists to close. Every invocation
also prints a `JUMBO GATE — SUMMARY` block naming each requested arm's
outcome (`[PASS]`/`[FAIL]`/`[NOT RUN]`) before exiting.

1. **Row a.** Provision 3 voters on the fleet's jumbo-MTU (9001) shape,
   start all three from cold with `force_jumbo_frames = false`, and record
   every node's `uc2_datagram_mtu_bytes` sampled at least once a second from
   the LAST node's start until 10 s have elapsed or every node reads 8960,
   whichever comes first. Repeat 3 times from a clean instance dir each
   time. `python3 bench-infra/scripts/jumbo_gate.py --fleet --arms a
   --hosts <pub/priv,...>` drives this; it prints one `[PASS]`/`[FAIL]`
   line via `verdict_row_a`.
2. **Row b.** On the same fleet shape, force the replication interface's
   MTU down to 1500 with ansible (or `jumbo_gate.py`'s `force_interface_mtu`
   helper, which issues the same `ip link set mtu` change over ssh) on
   every voter, restart the cluster, and: (a) record every node's
   `uc2_datagram_mtu_bytes` for the run's duration (must stay 1408); (b)
   record `uc2_send_emsgsize_total` the same way (must stay 0); (c) record
   `uc2_probe_sent_total` at the start and end of the run (must have risen —
   errata 1); (d) run `scripts/hop1_ab.sh`-style interleaved base/head 64 B
   throughput pairs against the pre-jumbo base tree, first a small
   preliminary run to measure the base tree's own spread, then
   `required_pairs(...)` more pairs (floor 5) to resolve the −3 % bar.
   Feed all four into `verdict_row_b`.
3. **Row c.** `run_arm_c` runs the pre-arm blackhole probe FOR REAL as its
   first step (fix round 1, Important 2 — this is wired, not a stub):
   it samples every node's `uc2_datagram_mtu_bytes` for up to 30 s and calls
   `check_blackhole_probe`; a jumbo arm that never clears 8832 returns a
   FAIL `Verdict` naming the offending host(s) and the arm stops there —
   `jumbo_gate.py --fleet --arms c ...` exits 1, never silently proceeding
   to the soak on a blackholed path. Once the probe clears, run the
   envelope-map brief's own soak procedure (its §3-§5) on the AWS/9001 arm
   and the standard/1408 arm by hand (this part of the arm is still a
   stub — it prints the procedure and returns no `Verdict`, i.e. NOT RUN,
   not a pass) and feed the two arms' plateau and 64 B rung numbers into
   `verdict_row_c`.
4. **Row d.** Arm 1: all three nodes, `force_jumbo_frames = true`, on the
   1500 B path; record each node's fail-stop reason, elapsed time from
   start, and named peer from its `obs_event!` line / exit log. Arm 2: two
   nodes with `force_jumbo_frames = true` on the 9001 path, the third never
   started; record the same for the two live nodes. Feed both into
   `verdict_row_d`.
5. **Row e.** Run `bench-infra/scripts/m5_fleet_gate.py`'s standard arm on
   this tree and on the pre-jumbo base tree, interleaved, and report the
   paired delta and its resolution with `report_row_e` — no pass/fail.
6. **Row f.** `scripts/hop1_ab.sh --sink <bin> --a <base> --b <head> --reps
   6 --secs 6 --root $HOME/scratch/jumbo-hop1-ab` on an idle box (never
   `/tmp` — CLAUDE.md), and report with `report_row_f` — no pass/fail.
7. Fill in the Results section above; do not edit the bar table to match
   whatever the run produced. Rename this file to
   `uc2-jumbo-frame-discovery-gate-<run date>.md` and update every pointer
   to it (this document's own header, `RELEASES.md`/`docs/releases.md` when
   `2.12.0` ships, and any `docs/BACKLOG.md` line that names it).
8. This gate, the fault-layer tests and the fuzz target are jumbo's proof
   surface; only after it runs (or the maintainer explicitly defers rows
   e/f as reported-only, which spec §10 already anticipates) does
   [Cut a release](../how-to/cut-a-release.md) apply for `2.12.0`.

## Related

- [`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md`](../superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md) —
  §10 (this gate's source table and errata), §4.1 (rungs), §5 (discovery),
  §6 (`force_jumbo_frames`).
- [`docs/superpowers/specs/2026-08-02-uc2-envelope-map-brief.md`](../superpowers/specs/2026-08-02-uc2-envelope-map-brief.md) —
  §6, row c's disposition, quoted verbatim above.
- [`docs/superpowers/plans/2026-09-10-uc2-jumbo-frame-discovery-plan1.md`](../superpowers/plans/2026-09-10-uc2-jumbo-frame-discovery-plan1.md),
  [`docs/superpowers/plans/2026-09-12-uc2-jumbo-frame-discovery-plan2.md`](../superpowers/plans/2026-09-12-uc2-jumbo-frame-discovery-plan2.md) —
  what shipped; plan 2 Task 2's fix rounds are the source of row d's
  as-built refusal-naming note (`path_below_committed_mtu`'s own fix is
  recorded in the plan-2 ledger, not in the spec's plan-1 errata section).
- [Time-and-timers gate](uc2-time-and-timers-gate-2026-09-03.md) — the
  paired-delta ruling, the `required_pairs`/"~430 reps" derivation row b
  reuses, and the `apply_bench`-is-the-wrong-harness reasoning row e
  restates for the appender.
- [Log-clock gate skeleton](uc2-log-clock-gate-2026-09-08.md) — the closest
  prior model for a small, mostly-UNRUN gate skeleton with dev-box-legal
  no-bar rows alongside a user-gated fleet row.
- `bench-infra/scripts/m13_hop_bench.py --selftest` — the driver idiom this
  gate's `jumbo_gate.py --selftest` follows (pure row functions, canned
  inputs, no fleet).
- `bench-infra/scripts/tt_fleet_gate.py` — the dependency-free paired-
  statistic module (`parse_prom`/`prom_get`, `ab_stats`/`ab_reading`) row b
  and row e's harness reuse rather than reinvent.
