# FSM upgrade lifecycle — design

**Status:** draft for maintainer review, revision 2. **Date:** 2026-09-19.
**Tree:** worktree `fsm-upgrade-lifecycle`, branched from `main` @ `47e74e4` (UC 2.12.0).

Companion documents that already exist and that this spec amends rather than
replaces: `docs/reference/application-sdlc.md` (the written standard, whose §5
is currently a promissory note), `docs/how-to/upgrade-an-application.md` (the
flag-day procedure), `.superpowers/SBE vs serde+bincode 2 — Handover Doc.md`
(the codec comparison this spec's §5.7 responds to), and issues [#33], [#36],
[#38], [#41], [#42], [#31].

---

## 0. Summary

An application built on UC has no supported way to move its state machine from
version N to version N+1 other than a flag day, and no tooling to tell it
whether that flag day is safe. This spec models the lifecycle, names the
failure modes, and proposes one piece of tooling — a **differential replay
harness** — that addresses most of them at once.

Five findings drive the design:

1. **Divergence between versions is normal and must not be prohibited.** What
   matters is that every instance of the new version starts from the **same
   origin** (§2.2 — the maintainer's framing, which replaced the blanket
   prohibition proposed during brainstorming).
2. **The application version is an input to the state transition, exactly like
   a command — and it is not in the log** (§2.5). State at position Q is a
   function of *(the log prefix, the sequence of versions that applied each
   span)*; UC records only the first half. A log that does not describe how its
   own state was produced is an incomplete log.
3. **UC's default reconstruction path computes a counterfactual.** With purge
   disabled — the shipped default — a restarting service replays from genesis
   under the new binary, producing "the state this cluster would have had if it
   had always run v_new". That state never existed (§2.3, verified in code).
4. **The comparison surface is not the snapshot image** (§4). Cross-version
   comparison cannot use image bytes, because image format change is the thing
   under test. The version-independent surface is the FSM's own responses and
   queries.
5. **The default codec silently misparses across versions in 5 of 12 measured
   shapes, and 4 of those 5 are UC's own missing length check** (§2.4,
   Appendix A — measured, not asserted).

---

## 1. Scope

### 1.1 In scope (this spec)

- A stage model for taking an FSM from version N to N+1 (§3).
- The comparison surface that decides whether an upgrade is correct (§4).
- Conventions the platform should mandate and document (§5).
- A differential replay harness: corpus format, three modes, black-box
  execution (§6).
- A skill that drives the harness and interprets its output (§8).
- Two separable platform defects found while writing this (§9.1).

### 1.2 Named second phase (same spec, later work)

- `Shadow<Old, New>` and the learner shadow deployment (§7).

### 1.3 Out of scope (Track 2 — separate spec)

- The committed application level and the live-commit version gate ([#33],
  shaped like [#31]'s wire level). This spec assumes the flag-day posture and
  is designed to be useful under it; §9.2 states what remains unsolved.
- Node-level rolling upgrades ([#31] proper).
- Leadership transfer.

### 1.4 Explicit non-goal: faithful replay from genesis

Replaying the whole log from position 0 is **not** a goal, now or later, for
two independent reasons:

- It is unbounded work that grows for the life of the cluster. Snapshots exist
  precisely so that it never has to happen.
- After any upgrade it is not even *possible* to do faithfully without every
  historical binary **and** the exact log positions at which each version took
  over (§2.5). Reconstructing the true state — including every historical
  snapshot — would require replaying `[0,P₁)` under v₁, `[P₁,P₂)` under v₂, and
  so on.

This non-goal is load-bearing. It is why §6.2's `reconstruction` mode tests
that the system *refuses* the genesis path rather than that the two paths
agree, and why the pinned artifact (§3 S4) is the only correct origin.

---

## 2. The problem

### 2.1 Three compatibility axes

Conflating these is the root of most confusion in this area. They have
different windows and different closure conditions.

| axis | question | window | closed by |
|---|---|---|---|
| **P — peer** | Does my binary interoperate with my sibling's binary, right now? | the upgrade window | finishing the roll |
| **H — history** | Can my binary apply commands written to the log arbitrarily long ago? | **unbounded by default** | a pinned origin above the last occurrence (§3 S9) |
| **F — framework** | Does my FSM version work on UC 2.11 *and* 2.12? | while straddling UC releases | a UC flag day |

Axis-P failures are loud and bounded — you are in the window, watching.
Axis-H failures are quiet and detonate later, on a node reconstructing across a
span whose commands the current binary interprets differently. Appendix A's
case C is a measured instance: a new binary reading an old `Put(11,22)` decodes
it as `Delete(11)`.

Axis-F is undocumented: an application developer has a 2-D compatibility matrix
(FSM version × UC release) and no document states which cells are legal.

### 2.2 The common-origin requirement

Two versions of an FSM will produce different internal state from the same
input. That is the *point* of an upgrade, not a defect, and any rule that
forbids it forbids useful work.

The requirement is therefore not equality between versions. It is:

> **Every instance of the new version must begin from the same log position,
> reconstructed from the same artifact.**

If all v_new instances install the same snapshot at position P and tail-replay
P→Q, they diverge from v_old *identically* and remain mutually consistent.

The justification is stronger than peer agreement. A snapshot built at P by
v_old encodes *the state as v_old actually produced it over `[0,P)`* — and per
§2.5 that state is not recoverable from the log bytes alone once v_old is gone.
**The artifact is the only faithful carrier of the pre-upgrade history.** That
is what makes pinning the origin a correctness step rather than a convenience.

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
state machine needs**. For a service restarting fresh after a binary swap
(`last_applied() == None`, so `start_pos == 0`):

- **Purge disabled** (`PurgePolicy::Disabled` is `#[default]`,
  `uc_node/src/node.rs:171-173`) and a complete journal: `first == 0`, so
  `0 > 0` is false. The service **replays the entire log from genesis under the
  new binary.** The pinned artifact is never consulted.
- **A node that joined later via a snapshot session**: `first > 0`, so the
  guard fires and the service **installs the newest covering artifact** — one
  built by whichever version was running when that instant fired — and
  tail-replays from there.

**These two paths are not symmetric, and an earlier revision of this spec was
wrong to present them as two equally-valid answers that merely disagree.**

Call node A the one with a genesis-complete journal and node B the one that
joined by snapshot:

- **B is correct.** It installs v_old's artifact at P — the faithful record of
  v_old's history — and applies v_new only to `(P, Q]`, which is the span
  v_new actually owns.
- **A is wrong.** It computes `apply_v_new` over `[0, Q]`: the state this
  cluster *would* have had if it had always run v_new. That state never
  existed. A does not merely differ from its peer; it differs from the truth.

So the shipped default makes the **incorrect** path the common one, and does so
silently. Heterogeneous journal history across a fleet is the normal condition
of any long-lived cluster, because nodes get replaced — so both paths are
typically live at once.

Genesis replay is faithful **iff exactly one version has ever applied that
span**. That is true for a never-upgraded cluster and false forever after.
Per §2.5, UC cannot currently tell the two cases apart.

> **Evidence status.** The code path above is read and quoted. The resulting
> divergence is **not yet experimentally demonstrated** — building that
> demonstration is §6.2's `reconstruction` mode, and it is the harness's first
> teeth-check.

This also makes [#36] (a service restart is silent about how it reconstructed)
load-bearing rather than cosmetic: it is precisely the observation needed to
tell path A from path B.

### 2.4 The change taxonomy

The first step of any version bump is classifying the change, because the
classification determines every obligation downstream. This table is missing
from `application-sdlc.md`, which jumps from "write a design note" to
"determinism rules".

| what changed | replicated? | axis-P risk | axis-H risk | downgrade-safe? |
|---|---|---|---|---|
| New command variant (appended) | yes | **severe** — [#33]: old replica cannot apply; leader acks anyway | permanent until S9 closes it | no |
| **Command variant inserted mid-enum** | yes | severe | **worst — measured silent misparse (App. A, case C)** | no |
| Field appended to an existing command | yes | severe; **silently drops the field** (App. A, case A) | permanent | no |
| **Fields reordered** | yes | **silent, and undetectable by any length check** (App. A, case E) | permanent | no |
| Integer field widened (`u32`→`u64`) | yes | **none measured** — varint makes it transparent (App. A, case D) | none | yes |
| Changed `apply` semantics of an existing command | yes | **severe** — silent divergence, no decode error to catch it | permanent; **safe only under S4's pinned origin** | no |
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

**The decode trap — measured, and worse than fail-stop.** An earlier revision
recorded this as a clean fail-stop (`.expect("corrupt committed frame")`) and
called that the right posture. Appendix A refutes that: across 12 probes of
realistic schema changes, **5 decode successfully into a wrong value**.

The amplifier is UC's own call site. All three typed-tier decodes discard
`bytes_read`:

```rust
// uc_service/src/traits.rs:326, 336, 484
let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, standard())
    .expect("corrupt committed frame (fail-stop)");
```

`decode_from_slice` does not require consuming the buffer. **Four of the five
silent misparses leave `bytes_read < cmd.len()`** and would become the intended
fail-stop under a length check. Only case E (field reorder) is byte-identical
and survives it. See §9.1 for the fix.

### 2.5 The version is an input, and it is not in the log

The state at position Q is not a function of the log prefix `[0,Q]`. It is a
function of

> `(the log prefix [0,Q], the sequence of application versions that applied
> each span of it)`

because two versions produce different state from the same commands — which
§2.2 establishes is legitimate and expected. The application version is an
**input to the state transition, exactly like a command.**

In a state-machine-replication system, inputs go in the log. That is the whole
principle: the log is meant to be a complete description of how the state got
here. Today only half of that description is recorded. Where the upgrade points
are, and which version owned which span, lives in no durable place — not in
the log, not in the artifact, not in node state. It exists only in the
operator's memory.

Two concrete consequences, both verified:

- **The artifact is anonymous about its version.**
  `uc_service/src/snapshots.rs:58,62` — the framework envelope is 16 bytes,
  `ULTSNAP1 ‖ P`: magic and position, nothing else. The snapshot *session*
  carries per-row versions (`SnapBeginBody.version[8]`), but a file on disk
  does not. For a system in which version is an input to state, an artifact
  that cannot say which semantics it embodies is missing its provenance.
- **Nothing records whether a span was upgraded across.** So UC cannot
  distinguish "genesis replay is faithful here" from "genesis replay computes a
  counterfactual" (§2.3).

**The proposal.** Record the version change as a committed frame — a
`FRAME_TYPE_CLUSTER` record, the same shape the cluster FSM already uses for
Settings — carrying `(position, row, old_version, new_version)`. Then:

- the upgrade origin P is **committed data**, not an operator note;
- a node can **refuse** to reconstruct across a span whose recorded version is
  not its own, which is what turns §2.3's wrong path from silent into named;
- the artifact's provenance becomes checkable against the log.

**What this deliberately does not buy:** faithful genesis replay. Even with a
complete version history in the log, replaying `[0,Q]` correctly would require
every historical binary. That remains the explicit non-goal of §1.4. The record
exists to make the *wrong* path detectable and refusable, not to make the
impossible path possible.

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

What it does not buy: any live-commit gate ([#33]), and no durable record of
*when* the version changed (§2.5).

`NAME` is never bumped. It is the identity hash *and* the `fold32` input to
`IdGen`; changing it is not a version change, it is a different FSM with a
different id stream.

**Gap this spec should close:** there is no stated mapping from the §2.4
classification to *which digit* moves. Proposal: major = any row whose axis-P
risk is "severe" or "worst"; minor = additive-but-inert (and the harness must
confirm the inertness); patch = no replicated behaviour change at all. Aeron's
convention (major equality as the kill switch) is named in [#31] as a reserved
validator and is the natural consumer of this mapping.

### S3 — Write the compatibility shims

Three shims, and they are not interchangeable:

- **Forward decode** (old binary, new command) — *cannot be written
  retroactively*. It has to have been in v_old. This is the single strongest
  argument for §5.1's command version tag being mandatory from day one.
- **Backward apply** (new binary, old command) — bounded by S9, not permanent.
- **Snapshot dual-read** (new binary, old image) — what `examples/kv` hand-rolled
  as `IMAGE_VERSION_V1`/`V2` (`examples/kv/src/lib.rs:66-68`).

### S4 — Pin the origin

**The stage that does not exist today.** Before stopping any service:

1. Command a coordinated snapshot instant (`uc2ctl snapshot`) and confirm the
   complete set at P on every node.
2. Record P as the upgrade origin — durably, per §2.5.
3. Ensure every new-version service reconstructs **from the artifact at P** and
   not by replaying below it.

Step 3 has no mechanism (§2.3). Options, for maintainer decision (Q2, §10):

- **(a) Advance the purge floor to P as part of the upgrade.** Makes the
  artifact the only reachable path, using machinery that exists. **Under §2.5
  this is the semantically motivated option, not merely the convenient one:**
  below the last upgrade point the log is not faithfully replayable by *any*
  single binary, so leaving it reachable leaves a hazard whose only use is to
  produce a counterfactual state. Cost: it consumes the on-disk prefix, which
  interacts with [#41] — though the off-node backup taken in step 1 of
  `upgrade-an-application.md` is the actual rollback point and is unaffected.
- **(b) An explicit "start from artifact P" service option.** No data
  consumed, no purge-policy change forced on the operator; new surface.
- **(c) Detect and refuse.** The service records the origin it reconstructed
  from and refuses a mismatch against the committed upgrade origin. Smallest
  change, and it subsumes [#36]; detects rather than prevents.

**Revised recommendation: (a) as the default, with (c) as the backstop.** An
earlier revision recommended (b)+(c) on the grounds that (a) "takes an
irreversible action on the operator's behalf". That reasoning was made under
the wrong model — it treated the prefix as valuable data being discarded. Under
§2.5 the prefix below P is not a usable replay source for any binary the
cluster will ever run again; (c) alone would leave it reachable and rely on a
refusal firing. (b) remains attractive where an operator has a reason to retain
the prefix (forensics, a deliberate multi-version replay experiment), so it is
worth having — but as the exception, not the default.

### S5 — Verify differentially

Run the harness (§6). Gate: the divergence profile contains no *unexpected*
entries (§4.3).

### S6 — Roll out

The existing flag day (`docs/how-to/upgrade-an-application.md`), with S4
inserted before "stop every service".

### S7 — Confirm

Every row reports the new version; every replica agrees. Cross-replica
agreement is an *image-digest* comparison and is valid here precisely because
all replicas are now the same version (§4.1).

### S8 — Decide the point of no return

"Rollback" is largely a fiction in an SMR system: swapping the binary back does
not roll back the log, and once v_new semantics have been applied to committed
frames that state is v_new's. `upgrade-an-application.md` is already honest
about this — the real rollback is restoring the off-node backup on every node,
discarding every write acked since. The SDLC standard's "rollback plan" (§5)
should be renamed and reframed as a **point-of-no-return plan**: what is the
last moment abandonment is possible, and what does it cost?

### S9 — Close axis H

When can the v_old command arm be deleted?

Under §2.2 the closure condition is cleaner than a purge-floor rule. Once a
pinned origin P sits **above the last occurrence** of that command shape, and
every node reconstructs from P or later, no binary will ever decode those bytes
again — the artifact at P already carries their effect. The shim can go.

Both quantities are knowable to UC and neither is exposed for this purpose.
Proposal: a `uc2ctl` reading that answers "the oldest position any node could
still reconstruct from, fleet-wide", so the closure condition is checkable
instead of guessed.

---

## 4. The comparison surface

### 4.1 Why image hashes do not work across versions

Hashing the snapshot image is valid only *within* a version, for cross-replica
agreement. Across versions it is meaningless, because a changed image format is
exactly the case under test. Cross-version comparison needs a version-
independent observation surface.

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

This is strictly more informative than a boolean, and it is the only
formulation compatible with intentional semantic change.

---

## 5. Conventions to mandate

### 5.1 A command version tag

Every command carries a version tag readable **without decoding the rest**, so
an old binary can determine "this is beyond me" and refuse *before* the ack.
`examples/kv` independently converged on this: `FORMAT_VERSION: u8 = 1`
(`examples/kv/src/wire.rs:13`), checked at `wire.rs:312` returning
`BAD_FORMAT_VERSION`. A clean-room builder arriving at it unprompted is decent
evidence it is the right convention.

Appendix A supplies the demonstration. Case E — two same-typed fields
reordered — decodes successfully, with the correct byte count, into swapped
values. No codec-level check catches it. **A tag ahead of the payload is the
only thing that can.**

Open design tension (Q3, §10): framework-owned envelope (like `ULTSNAP1 ‖ P`
is for artifacts), or documented application convention? A framework envelope
is enforceable but shrinks the payload ceiling further — the builder report
already notes `Sessioned`'s envelope quietly shrinking it.

### 5.2 Tolerant readers are wrong for replication

A tolerant reader (skip unknown fields) is correct for messaging and actively
harmful in an apply loop: it lets an old replica **silently apply a different
command** than the new replica applied, converting a loud failure into a quiet
one. The requirement in an SMR system is the opposite of tolerance — it is
cheap, reliable *recognition of incapacity*.

Appendix A's case A is this failure in its mildest form: an old decoder reads a
new command, drops the appended field, and applies a command nobody sent. It is
"tolerance" arrived at by accident rather than by design, and it is exactly as
unsound.

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

### 5.7 Codec choice — responding to the SBE handover doc

`.superpowers/SBE vs serde+bincode 2 — Handover Doc.md` (2026-09-19) compares
the two formats for schema evolution across the Ultima suite and concludes:
SBE where nodes on different versions must interoperate, bincode for
same-deploy-unit use. Three responses, in increasing order of consequence.

**(i) Its central factual claim is confirmed.** The doc asserts bincode can
"misalign silently (producing wrong values with no error)". Appendix A measures
exactly that: 5 of 12 probes decode into a wrong value with no error.

**(ii) Its recommendation inverts where it meets an apply loop.** The doc
recommends SBE because forward compatibility is *automatic* — an old decoder
reads up to `blockLength` and skips what it does not recognize. **For an SMR
apply path that automatic skip is a liability, not a feature.** It converts
"old node cannot decode this command" — loud, and safe — into "old node applies
a truncated interpretation of this command" — silent, and unsound. On this one
path, bincode's hard failure is *safer* than SBE's graceful skip. The doc is
not wrong about the mechanism; it is answering "how do I keep a message
decodable across versions", which is the right question for a bus and the wrong
one for a replicated log.

**(iii) The synthesis: take SBE's header, suppress SBE's tolerant read.** SBE
still wins here, for a reason the doc does not give. Its header carries
`version` and `blockLength`, so an old decoder *knows* it is looking at a newer
message before interpreting the body — which is precisely what §5.1 asks for,
generated and enforced rather than hand-rolled. The correct posture is to
decode the version, refuse anything above what this binary supports, and
re-enable the skip only per-change, where the author asserts inertness (§5.2).

**A consequence the doc's own taxonomy implies.** Its split is cross-version →
SBE, same-deploy-unit ("WAL records read only by the process that wrote them")
→ bincode. A replicated command log is **neither**: it is written by one
version and read by every replica *and by every future version*. By the doc's
own criteria, bincode is the wrong choice for UC command payloads — and the
typed tier, the documented easy path, uses exactly that. The raw tier already
permits the alternative (`RawStateMachine`'s rustdoc: "Implement this directly
for SBE / flatbuffers / hand-laid frames"), but only for developers who know to
reach for it.

Whether to change that default steer is a product decision larger than this
spec; it is recorded here as Q6 (§10). *One input worth re-measuring rather
than trusting: a 2026-08-22 codec spike is recorded as finding SBE's cost equal
to the raw tier, which would make this free at the hop. That has not been
re-run and predates several apply-loop changes.*

---

## 6. Tooling: the differential replay harness

One machine, three configurations. Building it as one tool rather than two
avoids two comparison implementations drifting apart.

### 6.1 Corpus

A corpus is **(snapshot artifact at P, journal span P→Q, the version that built
the artifact)** — the first two are what `uc2ctl backup` already produces, and
the third is the provenance §2.5 says is currently missing and must be carried
alongside until the envelope records it.

No new capture mechanism is needed; what is wanted is *trimming*, and [#42]
already notes that backup copies the 64 MiB preallocation file, so the trimming
work is independently justified.

Note that §1.4 bounds the corpus: it always starts from a real artifact, never
from position 0. That makes the harness cheaper as well as more honest.

### 6.2 Three modes

| mode | setup | catches |
|---|---|---|
| **`determinism`** | one build, **two processes**, same corpus | ambient clock, RNG, `HashMap` iteration order — *for free*, since Rust randomizes `RandomState` per process, so two processes already disagree if the FSM depends on hash order |
| **`upgrade`** | two builds, same corpus | axis-H breakage, semantic drift, id-stream drift |
| **`reconstruction`** | one build, **two start states**: genesis-replay vs. install-artifact-at-P + tail-replay | the §2.3 counterfactual |

That the determinism check falls out as a degenerate case is the main argument
for this shape. It also makes [#38]'s item 2 (a determinism *lint*) largely
redundant: a runtime differential is strictly stronger, because a lint can
enumerate `SystemTime::now()` but can never catch "this version mints a
different number of ids".

**The `reconstruction` mode's assertion is not equality.** An earlier revision
had it comparing the two paths and expecting them to agree. Under §1.4 and
§2.3 that is testing for something we do not want: after any semantic change
the two paths *should* differ, because one of them is computing a
counterfactual. The mode's job is therefore to

1. **demonstrate** that the two paths diverge (the teeth-check that the harness
   detects a real defect rather than passing vacuously), and
2. **verify that the system refuses the wrong one** — i.e. that S4's mechanism
   actually prevents a v_new service from reconstructing below the pinned
   origin.

This mode should be built **first**; item 1 is the demonstration §2.3 currently
lacks.

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
- **Spot the Appendix A shapes in a diff**: a variant inserted mid-enum, two
  fields reordered. Both are one-line diffs with catastrophic consequences and
  both are trivially recognizable by reading the diff.
- **Localize a divergence**: given "first disagreement at position P", bisect
  the corpus and read both implementations' arms for that command.

### 8.2 Where it does not

Validating that a snapshot loads, comparing two digests, checking every row
reports the new version. These are assertions; dressing them as agent work
makes them slower and less trustworthy.

**So: the harness is code; the skill decides what to run and explains what
broke.**

---

## 9. Separable findings, and what remains unsolved

### 9.1 Two platform defects found while writing this spec

Both are independent of the lifecycle work and could ship on their own.

**(1) The typed tier discards `bytes_read`.** All three decode sites
(`uc_service/src/traits.rs:326, 336, 484`) destructure as `let (cmd, _) = ...`,
and `decode_from_slice` does not require consuming the buffer. Per Appendix A,
**four of the five measured silent misparses leave `bytes_read < len`** and
would become the intended fail-stop under a length check.

The check is viable: `uc_log/src/reader.rs:150` slices the payload as
`&buf[self.boff + HEADER_LEN..self.boff + length]` — the header's `length`, not
the 32-byte-aligned span — so the payload handed to `apply` is exact and the
assertion has no padding false-positives.

*Verified for the bare frame path only. The `Sessioned` and `Timed` wrappers
derive their inner slice from their own envelopes; whether those are equally
exact has not been traced and must be before the check is added there.*

**(2) The snapshot artifact has no version provenance.** §2.5. The envelope is
`ULTSNAP1 ‖ P` and nothing else (`uc_service/src/snapshots.rs:58,62`). For a
system where version is an input to state, the artifact cannot say which
semantics it embodies.

### 9.2 What this spec does not solve

Without Track 2 ([#33]), a mixed-version *live commit* can still acknowledge a
write no quorum can apply. Everything here makes the flag day verifiable; none
of it makes a rolling application upgrade safe. The honest answer to "how do I
upgrade" remains "flag day" until the committed application level and the
live-commit gate exist.

What this spec *does* buy Track 2: [#31]'s own proposal (step 7) requires a
proof surface — a two-binary crashtest under load with a linearizable history —
to ship *with* the rolling-upgrade flag day. §6 is that apparatus. Building it
first means Track 2 arrives with its proof already in the tree.

---

## 10. Open questions for the maintainer

- ~~**Q1.** bincode's behaviour on a version mismatch.~~ **Answered by
  measurement** — Appendix A. Silent misparse in 5 of 12 probes; 4 of the 5 are
  UC's missing length check (§9.1).
- **Q2.** S4 mechanism: (a) purge to P, (b) explicit start-from-artifact
  option, or (c) detect-and-refuse. Spec now recommends **(a) + (c)**, revised
  from (b) + (c) — see §3 S4 for why the earlier reasoning was wrong.
- **Q3.** §5.1: framework-owned command envelope, or documented application
  convention? Trades enforceability against payload ceiling.
- **Q4.** Is per-row version skew across an 8-row multi-FSM deployment
  *supported*, or merely representable? The identity/version arrays are
  positional and per-row with `0` = unknown, so the wire permits it; nothing
  states whether it is allowed.
- **Q5.** Scope confirmation: §§1–6, §8 and §9.1 as this spec's deliverable,
  with §7 named and deferred?
- **Q6.** §5.7: does the typed tier stay the documented default for commands,
  given that the handover doc's own taxonomy rules bincode out for a replicated
  log? Changing the steer is a product decision beyond this spec.
- **Q7.** §2.5: is the version-change record a new `CLUSTER` kind, or does it
  ride the existing Settings record? The former is cleaner; the latter is
  smaller.

---

## 11. Work breakdown

| # | deliverable | kind | depends on |
|---|---|---|---|
| 0 | `bytes_read` length check at the three decode sites (§9.1) | **code, separable** | — |
| 1 | §2.1 axes, §2.4 taxonomy, §2.2 common origin, §2.5 version-as-input, §3 stages → folded into `application-sdlc.md` | docs | — |
| 2 | §5 conventions, including §5.7's codec guidance | docs | 1 |
| 3 | Corpus format + trimmed export (§6.1) | code | — |
| 4 | Harness, black-box, `reconstruction` mode first (§6.2–6.4) | code | 3 |
| 5 | Version-change record + artifact provenance (§2.5, §9.1) | code | — |
| 6 | S4 origin-pinning mechanism (Q2) | code | 4, 5 |
| 7 | Skill (§8) | skill | 1, 4 |
| 8 | White-box mode + `Shadow` (§7) | code | 4, §5.3 |
| 9 | Learner shadow deployment | docs + ops | 8 |
| — | *Track 2: committed app level + live-commit gate* | separate spec | — |

---

## 12. Evidence register

Read and quoted in this tree (worktree `fsm-upgrade-lifecycle`, `main` @ `47e74e4`):

- `uc_service/src/replay.rs:207-228` — the gap guard; snapshot installed only when `first > start_pos`.
- `uc_node/src/node.rs:171-173` — `PurgePolicy` with `#[default] Disabled`.
- `uc_log/src/reader.rs:150` — `FrameIter::next` slices the payload to the header's `length`, not the aligned span.
- `uc_service/src/traits.rs:326, 336, 484` — three typed-tier decodes, each discarding `bytes_read`.
- `uc_service/src/snapshots.rs:58,62` — the 16-byte `ULTSNAP1 ‖ P` envelope; no version field.
- `uc_net/src/receiver.rs:2364-2385` — `SNAP_BEGIN` version comparison; nonzero-vs-nonzero inequality refuses; `0` is "unknown".
- `uc_service/src/ids.rs` — `IdGen::next` = `permute(position, (ordinal << 32) | fold32)`.
- `uc_service/src/tagged.rs` — the forwarding-wrapper idiom.
- `uc_service/src/output.rs:148` — output agent idles on non-leaders.
- `uc_protocol/src/identity.rs:151` — `pack_version`; `hash()` is over the name only.
- `examples/kv/src/wire.rs:13,312` and `examples/kv/src/lib.rs:66-68` — the command version tag and snapshot image dual-read.
- `examples/kv/tests/cluster.rs:541` — `upgrade_v1_to_v2_flag_day`, which skips unless v1 binaries are staged by hand.
- `packaging/prometheus/uc2-alerts.yml:178` — `Uc2ServiceVersionDrift`.
- `Cargo.toml:38` / `Cargo.lock` — `bincode = "2"`, locked at `2.0.1`.
- `docs/reference/application-sdlc.md`, `docs/how-to/upgrade-an-application.md` — read in full.
- `.superpowers/SBE vs serde+bincode 2 — Handover Doc.md` — read in full; §5.7 responds to it.
- Issues [#31], [#33], [#36], [#38], [#41], [#42] — read via the REST API.

**Run this session:** the Appendix A measurement (`cargo run` against bincode
`=2.0.1`, private `CARGO_TARGET_DIR`), output reproduced verbatim below.

**Not verified / not run:** the §2.3 divergence is derived from the quoted code
path, not demonstrated (§6.2 mode 1 is that demonstration). The `Sessioned` /
`Timed` inner-slice exactness for §9.1 is untraced. The recorded 2026-08-22
finding that SBE costs the same as the raw tier has not been re-run.

---

## Appendix A — the bincode schema-evolution measurement

**Question.** Does `bincode 2` with `config::standard()` fail loudly on a
schema version mismatch, or can it silently misparse into a valid-but-wrong
value?

**Method.** A standalone crate pinned to `bincode = "=2.0.1"` (matching
`Cargo.lock`), reproducing UC's exact call shape —
`decode_from_slice::<T, _>(bytes, standard())` with `bytes_read` discarded, as
at `uc_service/src/traits.rs:326`. Six schema-change shapes, probed in both
directions where both are meaningful. Source:
`/home/claude/scratch/bincode-evo` (outside the repo, throwaway).

**Results — 12 probes.**

| case | direction | outcome |
|---|---|---|
| **A** field appended to a struct | old reads new | **Ok, WRONG** — `ttl` silently dropped; read 2/3 |
| A | new reads old | `Err: UnexpectedEnd { additional: 1 }` |
| **B** enum variant appended at end | old reads new | `Err: invalid value: integer 2, expected variant index 0 <= i < 2` |
| B | new reads old | Ok, correct; read 3/3 |
| **C** enum variant inserted mid-enum | **new reads old** | **Ok, WRONG — `Put(11,22)` decodes as `Delete(11)`**; read 2/3 |
| C | old reads new | `Err: invalid variant index` |
| **D** `u32`→`u64`, value 5 | new reads old | Ok, **correct**; read 2/2 |
| D | `u32`→`u64`, value 4 000 000 000 | Ok, **correct**; read 6/6 |
| **E** two same-typed fields reordered | new reads old | **Ok, WRONG — values swapped**; read 2/2, **no trailing bytes** |
| **F** `Option<T>` appended, `None` | old reads new | **Ok, WRONG** — field dropped; read 1/2 |
| F | appended, `Some(99)` | **Ok, WRONG** — `Some(99)` dropped; read 1/3 |
| F | new reads old | `Err: UnexpectedEnd { additional: 1 }` |

**Findings.**

1. **Silent misparse is real: 5 of 12.** The handover doc's claim is confirmed.
2. **Four of the five leave `bytes_read < len`** and would fail-stop under a
   length check UC does not perform (§9.1).
3. **Case E survives any length check.** Reordering two same-typed fields is
   byte- and length-identical. Only a version tag ahead of the payload catches
   it (§5.1).
4. **Case C is the axis-H demonstration.** A new binary reading an old
   `Put(11,22)` — after someone inserted a variant mid-enum — applies
   `Delete(11)`. A write becomes a deletion, with no error anywhere.
5. **Case D is a genuine positive.** Varint encoding makes `u32`→`u64`
   widening transparent at both small and large values. It is the only
   evolution measured here that is safe in both directions.
6. **Case F refutes a common belief.** Appending `Option<T>` is unsafe in both
   directions: the old decoder silently drops it, the new decoder errors on old
   data.

[#31]: https://github.com/PeterKnego/ultima_cluster/issues/31
[#33]: https://github.com/PeterKnego/ultima_cluster/issues/33
[#36]: https://github.com/PeterKnego/ultima_cluster/issues/36
[#38]: https://github.com/PeterKnego/ultima_cluster/issues/38
[#41]: https://github.com/PeterKnego/ultima_cluster/issues/41
[#42]: https://github.com/PeterKnego/ultima_cluster/issues/42
