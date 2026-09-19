# FSM upgrade lifecycle — design

**Status:** draft for maintainer review. **Date:** 2026-09-19.
**Tree:** worktree `fsm-upgrade-lifecycle`, branched from `main` @ `47e74e4` (UC 2.12.0).

Companion documents that already exist and that this spec amends rather than
replaces: `docs/reference/application-sdlc.md` (the written standard, whose §5
is currently a promissory note), `docs/how-to/upgrade-an-application.md` (the
flag-day procedure), and issues [#33], [#36], [#38], [#41], [#42], [#31].

---

## 0. Summary

An application built on UC has no supported way to move its state machine from
version N to version N+1 other than a flag day, and no tooling to tell it
whether that flag day is safe. This spec models the lifecycle, names the
failure modes, and proposes one piece of tooling — a **differential replay
harness** — that addresses most of them at once.

Three findings drive the design:

1. **Divergence between versions is normal and must not be prohibited.** What
   matters is that every instance of the new version starts from the **same
   origin**. (§2.2 — the maintainer's framing, and the correction to an earlier
   draft of this spec that tried to forbid semantic change outright.)
2. **UC's default configuration actively breaks that requirement.** With purge
   disabled — the shipped default — a restarting service replays the journal
   from genesis and ignores the pinned snapshot; a node that joined via a
   snapshot session installs an artifact instead. The two classes of node then
   apply different histories under the same binary. (§2.3, verified in code.)
3. **The comparison surface is not the snapshot image.** Cross-version
   comparison cannot use image bytes, because image format change is the thing
   under test. The version-independent surface is the FSM's own responses and
   queries. (§4.)

---

## 1. Scope

### 1.1 In scope (this spec)

- A stage model for taking an FSM from version N to N+1 (§3).
- The comparison surface that decides whether an upgrade is correct (§4).
- Conventions the platform should mandate and document (§5).
- A differential replay harness: corpus format, three modes, black-box
  execution (§6).
- A skill that drives the harness and interprets its output (§8).

### 1.2 Named second phase (same spec, later work)

- `Shadow<Old, New>` and the learner shadow deployment (§7).

### 1.3 Out of scope (Track 2 — separate spec)

- The committed application level and the live-commit version gate ([#33],
  shaped like [#31]'s wire level). This spec assumes the flag-day posture and
  is designed to be useful under it; §9 states what remains unsolved without
  Track 2.
- Node-level rolling upgrades ([#31] proper).
- Leadership transfer.

---

## 2. The problem

### 2.1 Three compatibility axes

Conflating these is the root of most confusion in this area. They have
different windows and different closure conditions.

| axis | question | window | closed by |
|---|---|---|---|
| **P — peer** | Does my binary interoperate with my sibling's binary, right now? | the upgrade window | finishing the roll |
| **H — history** | Can my binary apply commands written to the log arbitrarily long ago? | **unbounded by default** | purge floor past the last frame of that shape, on every node |
| **F — framework** | Does my FSM version work on UC 2.11 *and* 2.12? | while straddling UC releases | a UC flag day |

Axis-P failures are loud and bounded — you are in the window, watching.
Axis-H failures are quiet and detonate later, on a node reconstructing from a
journal that contains a command shape the current binary no longer handles the
same way. Axis-F is undocumented: an application developer has a 2-D
compatibility matrix (FSM version × UC release) and no document states which
cells are legal.

### 2.2 The common-origin requirement

Two versions of an FSM will produce different internal state from the same
input. That is the *point* of an upgrade, not a defect, and any rule that
forbids it forbids useful work.

The requirement is therefore not equality between versions. It is:

> **Every instance of the new version must begin from the same log position,
> reconstructed from the same artifact.**

If all v_new instances install the same snapshot at position P and tail-replay
P→Q, they diverge from v_old *identically* and remain mutually consistent. If
some v_new instances instead rebuild the prefix below P by replaying it under
new semantics, those instances disagree with their peers — a divergence inside
a **homogeneous, fully-upgraded cluster**, with no mixed-version window
anywhere in the story.

UC already owns the primitive that produces a cluster-wide common origin: the
coordinated snapshot instant (`FRAME_TYPE_SNAPSHOT = 7`, shipped 2.11.0). The
frame's end position P *is* the instant; every declared row and the cluster FSM
freeze at P; a node holds the complete set at P when every row's
`snap-<P>.ultsnap` plus `snapshots/cluster/snap-<P>.ultcluster` exist. That set
is committed by construction. It is exactly the pinned origin an upgrade needs.

What is missing is any mechanism that *makes* a new-version service start from
it.

### 2.3 What UC does today — verified

`uc_service/src/replay.rs:207-228`, the M6 gap guard:

```rust
let mut start_pos = guard.last_applied().unwrap_or(0);
let first = reader.first_meta()?.unwrap_or(0);
if first > start_pos {
    // ... pick the newest covering artifact <= target, install it ...
}
```

A snapshot is installed **only when the journal has been purged above what the
state machine needs**. Consequences, for a service restarting fresh after a
binary swap (`last_applied() == None`, so `start_pos == 0`):

- **Purge disabled** (`PurgePolicy::Disabled` is `#[default]`,
  `uc_node/src/node.rs:171-173`) and a complete journal: `first == 0`, so
  `0 > 0` is false. The service **replays the entire log from genesis under the
  new binary.** The pinned pre-upgrade snapshot is never consulted.
- **A node that joined later via a snapshot session**: `first > 0`, so the
  guard fires and the service **installs the newest covering artifact** — one
  built by whichever version was running when that instant fired — and
  tail-replays from there.

Both behaviours are individually correct. Together, in one cluster, they are
two different reconstruction paths for the same binary. Call node A the one
with a genesis-complete journal and node B the one that joined by snapshot: A
computes `apply_new` over `[0, Q]`; B computes `install(artifact_old@P)` then
`apply_new` over `(P, Q]`. If v_new changed the apply semantics of any command
below P, **A and B hold different state while reporting the same version**.

Heterogeneous journal history across a fleet is the normal condition of any
long-lived cluster, because nodes get replaced.

> **Evidence status.** The code path above is read and quoted. The divergence
> it implies is **not yet experimentally demonstrated** — building that
> demonstration is §6.2's `reconstruction` mode, and it should be the harness's
> first teeth-check.

This also interacts with [#36] (a service restart is silent about how it
reconstructed): today an operator cannot tell which of the two paths a node
took, which is precisely the observation needed to detect this class of
divergence. [#36] is load-bearing for upgrade safety, not cosmetic.

### 2.4 The change taxonomy

The first step of any version bump is classifying the change, because the
classification determines every obligation downstream. This table is missing
from `application-sdlc.md`, which jumps from "write a design note" to
"determinism rules".

| what changed | replicated? | axis-P risk | axis-H risk | downgrade-safe? |
|---|---|---|---|---|
| New command variant | yes | **severe** — [#33]: old replica cannot apply; leader acks anyway | permanent while a log holds one | no |
| Field added to existing command | yes | severe (decode) | permanent | no |
| Changed `apply` semantics of an existing command | yes | **worst** — silent divergence, no decode error to catch it | permanent; also breaks §2.2 | no |
| Response schema | no (published, not replicated) | client-compat only | none | yes |
| Query / QueryResponse | no | client-compat only | none | **yes — the one surface that genuinely rolls today** |
| In-memory state shape | not directly | none | via snapshot image only | depends on image |
| Snapshot image format | payload is the app's | — | new binary must read old image | **no — a downgraded binary fail-stops on a new image** |
| New or repurposed timer id | yes (log-derived pending set) | v_new inherits v_old's in-flight timers | yes | no |
| Changed number of `ids()` calls on an existing path | yes | **silent id-stream divergence** | yes | no |
| Adding `SnapshotStateMachine` to a row that lacked it | capability flag | flips `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`; changes the leader's instant refusal (48) | — | n/a |

Two rows deserve expansion because they are invisible to ordinary testing.

**The `IdGen` trap.** `uc_service/src/ids.rs` computes
`permute(position, (ordinal << 32) | fold32(identity))`. The id depends on the
**ordinal within the apply call**. If v_new mints two ids where v_old minted
one — on a path that is otherwise behaviourally identical — every subsequent id
in that call differs. No single-version test can see this; it is a *diff*
property. The module doc warns about stashing a generator across calls; it does
not warn about the call count changing across versions.

**The decode trap.** The typed tier's blanket impl decodes with
`.expect("corrupt committed frame (fail-stop)")` (`uc_service/src/traits.rs`).
An old binary meeting a command it cannot decode does not refuse gracefully —
it **panics the service**. That is the right posture (fail-stop beats
divergence), but it means the axis-P failure mode is "half the fleet's services
crash", not "half the fleet logs a warning", and the docs should say so.

> **Open, unverified:** bincode's exact behaviour on an unknown enum variant
> index versus a truncated struct — whether it reliably errors or can silently
> misparse into a valid-but-wrong value. The difference matters enormously (the
> second case is divergence, not fail-stop) and should be measured, not
> assumed. Tracked as open question Q1 (§10).

---

## 3. The lifecycle

Nine stages. Each names its artifact and its gate.

### S1 — Classify the change

Against §2.4. Output: the set of obligations this change incurs. This is the
step that should be mechanized first (§8), because everything else keys off it.

### S2 — Declare the version

Bump `const VERSION` via `identity::pack_version(major, minor, patch)`
(`uc_protocol/src/identity.rs:151`). What that buys today, verified:

- snapshot-session refusal on nonzero-vs-nonzero inequality at any row
  (`uc_net/src/receiver.rs:2364-2385`; `0` means "unknown", never a mismatch),
- the `uc2_service_version` metric and the `Uc2ServiceVersionDrift` alert
  (`packaging/prometheus/uc2-alerts.yml:178`),
- a cnc status word.

What it does not buy: any live-commit gate ([#33]).

`NAME` is never bumped. It is the identity hash *and* the `fold32` input to
`IdGen`; changing it is not a version change, it is a different FSM with a
different id stream.

**Gap this spec should close:** there is no stated mapping from the §2.4
classification to *which digit* moves. Proposal: major = any change in the
"severe"/"worst" rows; minor = additive-but-inert; patch = no replicated
behaviour change at all. Aeron's convention (major equality as the kill switch)
is named in [#31] as a reserved validator and is the natural consumer of this
mapping.

### S3 — Write the compatibility shims

Three shims, and they are not interchangeable:

- **Forward decode** (old binary, new command) — *cannot be written
  retroactively*. It has to have been in v_old. This is the single strongest
  argument for §5.1's command version tag being mandatory from day one.
- **Backward apply** (new binary, old command) — permanent, axis-H.
- **Snapshot dual-read** (new binary, old image) — what `examples/kv` hand-rolled
  as `IMAGE_VERSION_V1`/`V2` (`examples/kv/src/lib.rs:66-68`).

### S4 — Pin the origin

**The stage that does not exist today.** Before stopping any service:

1. Command a coordinated snapshot instant (`uc2ctl snapshot`) and confirm the
   complete set at P on every node.
2. Record P as the upgrade origin.
3. Ensure every new-version service reconstructs **from the artifact at P** and
   not by replaying below it.

Step 3 has no mechanism (§2.3). Options, for maintainer decision (Q2, §10):

- **(a) Advance the purge floor to P as part of the upgrade.** Makes the
  artifact the only reachable path, using machinery that exists. Cost: destroys
  the on-disk prefix — which is in tension with [#41] (the upgrade silently
  destroying its rollback artifact), though the off-node backup taken in step 1
  of `upgrade-an-application.md` is the real rollback point and is unaffected.
- **(b) An explicit "start from artifact P" service option.** New surface, but
  no data destroyed and no purge-policy change forced on the operator.
- **(c) Detect and refuse.** The service records the origin it reconstructed
  from; a mismatch against the cluster's recorded upgrade origin is a named
  refusal. Weakest — it detects rather than prevents — but it is the smallest
  change and it subsumes [#36].

My recommendation is **(b) with (c) as the backstop**: (a) couples an upgrade
to a purge-policy change and takes an irreversible action on the operator's
behalf, which is the wrong default for a step whose whole purpose is safety.

### S5 — Verify differentially

Run the harness (§6). Gate: the divergence profile contains no *unexpected*
entries (§4.3).

### S6 — Roll out

The existing flag day (`docs/how-to/upgrade-an-application.md`), with S4
inserted before "stop every service".

### S7 — Confirm

Every row reports the new version; every replica agrees. Note that
cross-replica agreement is an *image-digest* comparison and is valid here
precisely because all replicas are now the same version (§4.1).

### S8 — Decide the point of no return

"Rollback" is largely a fiction in an SMR system: swapping the binary back does
not roll back the log, and once v_new semantics have been applied to committed
frames that state is v_new's. `upgrade-an-application.md` is already honest
about this — the real rollback is restoring the off-node backup on every node,
discarding every write acked since. The SDLC standard's "rollback plan"
(§5) should therefore be renamed and reframed as a **point-of-no-return plan**:
what is the last moment abandonment is possible, and what does it cost?

### S9 — Close axis H

When can the v_old command arm be deleted? Only when no reachable log prefix
contains one — purge floor past the last such frame, on every node. Both
numbers are known to UC and neither is exposed for this purpose. Proposal: a
`uc2ctl` reading that answers "the oldest replayable position, fleet-wide", so
the closure condition is checkable instead of guessed.

---

## 4. The comparison surface

### 4.1 Why image hashes do not work across versions

The obvious answer — hash the snapshot image — is valid only *within* a
version, for cross-replica agreement. Across versions it is meaningless,
because a changed image format is exactly the case under test. Cross-version
comparison needs a version-independent observation surface.

### 4.2 The surfaces

| surface | catches | version-independent? |
|---|---|---|
| **Response bytes, per position** | per-command divergence, localized to a position | yes, if response schema is stable |
| **Probe-query answers** at checkpoints | state divergence not yet visible in responses | **yes** |
| **`svc_sched` records** (schedule/cancel) | timer-behaviour drift | yes |
| **`on_committed` emission sequence** | side-effect drift | yes |
| **`IdGen` ordinal per apply call** | the silent id-stream divergence (§2.4) | yes |
| **Snapshot image bytes** | cross-*replica* agreement only | no |

The design choice that keeps this cheap: **the developer's own queries are the
logical digest.** They already exist, they are already kept stable for clients,
and §2.4 identifies the query path as the one surface that genuinely rolls.
`examples/kv` had to invent a bespoke `digest` query because nothing told it
queries were the right place — so this is a convention to state, not a
mechanism to build. No new trait surface is required.

Limitation to write down: a probe suite can only cover the **intersection** of
the two versions' state. State that exists only in v_new is verified by the
v_new-only command tests, not by the differential.

### 4.3 Divergence profile, not equality

Because §2.2 makes divergence legitimate, the harness must not assert equality.
It emits a **divergence profile**: for each surface, the positions at which the
two versions disagreed and what they produced. The developer records the
expected profile alongside the change (an approval/characterization test). The
gate is:

- an entry in the profile that the developer did not declare → **fail**;
- a declared entry that did not occur → **fail** (the change did not do what it
  claimed);
- declared entries that occurred → pass.

This is strictly more informative than a boolean, and it is the only formulation
compatible with intentional semantic change.

---

## 5. Conventions to mandate

### 5.1 A command version tag

Every command carries a version tag readable **without decoding the rest**, so
an old binary can determine "this is beyond me" and refuse *before* the ack.
`examples/kv` independently converged on this: `FORMAT_VERSION: u8 = 1`
(`examples/kv/src/wire.rs:13`), checked at `wire.rs:312` returning
`BAD_FORMAT_VERSION`. A clean-room builder arriving at it unprompted is decent
evidence it is the right convention.

Open design tension (Q3, §10): should this be a *framework-owned* envelope
(like `ULTSNAP1 ‖ P` is for artifacts) or a documented application convention?
A framework envelope is enforceable but shrinks the payload ceiling further —
the builder report already notes `Sessioned`'s envelope quietly shrinking it.

### 5.2 Tolerant readers are wrong for replication

A tolerant reader (skip unknown fields) is correct for messaging and actively
harmful in an apply loop: it lets an old replica **silently apply a different
command** than the new replica applied, converting a loud failure into a quiet
one. The requirement in an SMR system is the opposite of tolerance — it is
cheap, reliable *recognition of incapacity*.

The nuance worth preserving: tolerant decode **is** correct when the added
field is genuinely inert to the old version (an audit comment that touches no
state). But the platform cannot distinguish that case from the dangerous one —
only the author can. So tolerant decode is an explicit per-change assertion the
developer makes and the harness *checks* (identical state and responses for
that command across versions), never a schema-level default.

### 5.3 Package the FSM as a library crate

Not as a module inside the service binary. This is what lets cargo depend on
two versions under renamed packages, which is the precondition for §6.3's
white-box mode and for §7's `Shadow`. Retrofitting it is painful, so it should
be mandated early even though nothing needs it on day one.

### 5.4 Queries are the probe suite

Per §4.2. Keep a stable, documented set of queries whose answers characterize
state; treat their schema as a compatibility surface.

### 5.5 Expand / migrate / contract — two releases, not one

Adding a command is **two rollouts**: v1.5 understands the new command but never
emits it; deploy v1.5 everywhere; only then flip clients to emit it. This works
today with zero platform change, and it is what the Track 2 committed
application level would later make *enforceable* rather than merely documented.

### 5.6 Timer ids are a permanent namespace

The pending timer set is log-derived and `Timed<S>` makes delivery
exactly-once, so v_new inherits v_old's in-flight instances. The replicated
schedule table keys on `(identity_hash, timer_id)`. Repurposing a timer id
across versions is therefore a replicated-state change, not an implementation
detail.

---

## 6. Tooling: the differential replay harness

One machine, three configurations. Building it as one tool rather than two
avoids two comparison implementations drifting apart.

### 6.1 Corpus

A corpus is **(snapshot artifact at P, journal span P→Q)** — which is what
`uc2ctl backup` already produces. No new capture mechanism is needed; what is
wanted is *trimming*, and [#42] already notes that backup copies the 64 MiB
preallocation file, so the trimming work is independently justified.

### 6.2 Three modes

| mode | setup | catches |
|---|---|---|
| **`determinism`** | one build, **two processes**, same corpus | ambient clock, RNG, `HashMap` iteration order — *for free*, since Rust randomizes `RandomState` per process, so two processes already disagree if the FSM depends on hash order |
| **`upgrade`** | two builds, same corpus | axis-H breakage, semantic drift, id-stream drift |
| **`reconstruction`** | one build, **two start states**: genesis-replay vs. install-artifact-at-P + tail-replay | the §2.3 divergence |

That the determinism check falls out as a degenerate case is the main argument
for this shape. It also makes [#38]'s item 2 (a determinism *lint*) largely
redundant: a runtime differential is strictly stronger, because a lint can
enumerate `SystemTime::now()` but can never catch "this version mints a
different number of ids".

The `reconstruction` mode should be built **first**, and its first job is to
demonstrate the §2.3 divergence — the teeth-check that the harness detects a
real defect rather than passing vacuously.

### 6.3 Black-box before white-box

| | black-box (two binaries, subprocess, 1-node cluster) | white-box (two versions linked in one process) |
|---|---|---|
| Linking constraint | none | FSM must be a library crate dependable at two versions (§5.3) |
| Language | any — the raw tier explicitly invites non-Rust | Rust only |
| Speed | slow (process + cluster per run) | fast enough for per-PR CI |
| Enables `Shadow` | no | yes |

**Black-box first.** It is unconditional, it is what §7 needs anyway, and
`testing/uc_crashtest` already builds this rig (reference bins, node + service
halves over a shared instance dir). White-box is the fast CI path, added once
the comparison surface has settled.

### 6.4 Output

The divergence profile of §4.3, plus, on failure, the first divergent position
and both versions' values on the offending surface.

---

## 7. Phase 2 — `Shadow<Old, New>`

White-box mode deployed live. It lands in the existing wrapper family beside
`Timed`, `Sessioned` and `Tagged` — `uc_service/src/tagged.rs` shows the idiom:
a forwarding newtype that overrides `NAME`/`VERSION` and delegates the rest.

`Shadow<Old, New>`:

- applies every committed frame to **both** inner state machines;
- **publishes Old's response**, so cluster behaviour is bit-identical to not
  shadowing;
- records divergence (position, surface, both values) to metrics and `uc_obs`;
- reports `NAME`, `VERSION` and `last_applied` from Old.

Three implementation constraints:

1. **Two independent `ApplyCtx`es** from the same `(position, identity)`. A
   shared ctx would let New's `ids()` calls advance Old's ordinal, corrupting
   the very thing being measured.
2. **Memory is 2× state.** Acceptable on a dedicated learner; needs stating.
3. **Learner safety has an edge.** `uc_service/src/output.rs:148` gates the
   output agent on the cnc leader flag (`if !is_leader(&st.cnc) { ... idle }`),
   so a learner emits no external side effects — but a learner can be
   *promoted* by reconfiguration, at which point the output agent starts.
   `Shadow` must refuse to attach on a voter and fail-stop on promotion, rather
   than relying on the operator not to promote it.

---

## 8. Skill support

The boundary matters; "add AI" is easy to over-claim.

### 8.1 Where a skill genuinely helps — diff-relative and semantic checks

- **Classify the diff** against §2.4 and emit the obligations it creates.
- **Generate the v_old→v_new command corpus from the schema diff** — a harness
  cannot invent an application's commands; an agent reading both versions'
  command types can.
- **Spot the invisible determinism hazards**: a changed `ids()` call count, a
  newly-introduced `HashMap` iteration, a float op. The first is a *diff*
  property and genuinely outside a lint's reach.
- **Localize a divergence**: given "first disagreement at position P", bisect
  the corpus and read both implementations' arms for that command.

### 8.2 Where it does not

Validating that a snapshot loads, comparing two digests, checking every row
reports the new version. These are assertions; dressing them as agent work
makes them slower and less trustworthy.

**So: the harness is code; the skill decides what to run and explains what
broke.**

---

## 9. What this does not solve

Without Track 2 ([#33]), a mixed-version *live commit* can still acknowledge a
write no quorum can apply. Everything in this spec makes the flag day
verifiable; none of it makes a rolling application upgrade safe. The honest
answer to "how do I upgrade" remains "flag day" until the committed application
level and the live-commit gate exist.

What this spec *does* buy Track 2: [#31]'s own proposal (step 7) requires a
proof surface — a two-binary crashtest under load with a linearizable history —
to ship *with* the rolling-upgrade flag day. §6 is that apparatus. Building it
first means Track 2 arrives with its proof already in the tree.

---

## 10. Open questions for the maintainer

- **Q1.** bincode's behaviour on an unknown enum variant index vs. a truncated
  struct: reliable error, or possible silent misparse? Decides whether §2.4's
  "decode trap" is a fail-stop story or a divergence story. Needs measurement.
- **Q2.** S4 mechanism: (a) purge to P, (b) explicit start-from-artifact
  option, or (c) detect-and-refuse. Spec recommends (b) + (c).
- **Q3.** §5.1: framework-owned command envelope, or documented application
  convention? Trades enforceability against payload ceiling.
- **Q4.** Is per-row version skew across an 8-row multi-FSM deployment
  *supported*, or merely representable? The identity/version arrays are
  positional and per-row with `0` = unknown, so the wire permits it; nothing
  states whether it is allowed.
- **Q5.** Scope confirmation: §§1–6 + §8 as this spec's deliverable, with §7
  named and deferred?

---

## 11. Work breakdown

| # | deliverable | kind | depends on |
|---|---|---|---|
| 1 | §2.1 axes, §2.4 taxonomy, §2.2 common origin, §3 stages → folded into `application-sdlc.md` | docs | — |
| 2 | §5 conventions | docs | 1 |
| 3 | Corpus format + trimmed export (§6.1) | code | — |
| 4 | Harness, black-box, `reconstruction` mode first (§6.2–6.4) | code | 3 |
| 5 | S4 origin-pinning mechanism (Q2) | code | 4 |
| 6 | Skill (§8) | skill | 1, 4 |
| 7 | White-box mode + `Shadow` (§7) | code | 4, §5.3 |
| 8 | Learner shadow deployment | docs + ops | 7 |
| — | *Track 2: committed app level + live-commit gate* | separate spec | — |

---

## 12. Evidence register

Read and quoted in this tree (worktree `fsm-upgrade-lifecycle`, `main` @ `47e74e4`):

- `uc_service/src/replay.rs:207-228` — the gap guard; snapshot installed only when `first > start_pos`.
- `uc_node/src/node.rs:171-173` — `PurgePolicy` with `#[default] Disabled`.
- `uc_net/src/receiver.rs:2364-2385` — `SNAP_BEGIN` version comparison; nonzero-vs-nonzero inequality refuses; `0` is "unknown".
- `uc_service/src/ids.rs` — `IdGen::next` = `permute(position, (ordinal << 32) | fold32)`.
- `uc_service/src/traits.rs` — `NAME`/`VERSION` on both tiers; typed-tier decode `.expect("corrupt committed frame (fail-stop)")`.
- `uc_service/src/tagged.rs` — the forwarding-wrapper idiom.
- `uc_service/src/output.rs:148` — output agent idles on non-leaders.
- `uc_protocol/src/identity.rs:151` — `pack_version`; `hash()` is over the name only.
- `examples/kv/src/wire.rs:13,312` and `examples/kv/src/lib.rs:66-68` — the command version tag and snapshot image dual-read.
- `examples/kv/tests/cluster.rs:541` — `upgrade_v1_to_v2_flag_day`, which skips unless v1 binaries are staged by hand.
- `packaging/prometheus/uc2-alerts.yml:178` — `Uc2ServiceVersionDrift`.
- `docs/reference/application-sdlc.md`, `docs/how-to/upgrade-an-application.md` — read in full.
- Issues [#31], [#33], [#36], [#38], [#41], [#42] — read via the REST API.

**Not verified / not run:** no test, build, or experiment was executed for this
spec. The §2.3 divergence is derived from the quoted code path, not
demonstrated. Q1 is explicitly unmeasured.

[#31]: https://github.com/PeterKnego/ultima_cluster/issues/31
[#33]: https://github.com/PeterKnego/ultima_cluster/issues/33
[#36]: https://github.com/PeterKnego/ultima_cluster/issues/36
[#38]: https://github.com/PeterKnego/ultima_cluster/issues/38
[#41]: https://github.com/PeterKnego/ultima_cluster/issues/41
[#42]: https://github.com/PeterKnego/ultima_cluster/issues/42
