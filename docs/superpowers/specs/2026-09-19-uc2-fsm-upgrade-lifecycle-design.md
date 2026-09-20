# FSM upgrade lifecycle — design

**Status:** revision 3 — open questions **decided** 2026-09-20 (§10); ready
for the maintainer's read-through before planning. **Dates:** drafted
2026-09-19, decisions 2026-09-20.
**Tree:** worktree `fsm-upgrade-lifecycle`, branched from `main` @ `47e74e4`
(UC 2.12.0).

Companion documents that already exist and that this spec amends rather than
replaces: `docs/reference/application-sdlc.md` (the written standard, whose §5
is currently a promissory note), `docs/how-to/upgrade-an-application.md` (the
flag-day procedure), `.superpowers/SBE vs serde+bincode 2 — Handover Doc.md`
(the codec comparison this spec's §5.7 responds to), and issues [#33], [#36],
[#38], [#41], [#42], [#31], [#49].

---

## 0. Summary

An application built on UC has no supported way to move its state machine from
version N to version N+1 other than a flag day, and no tooling to tell it
whether that flag day is safe. This spec models the lifecycle, names the
failure modes, and proposes the platform pieces and one piece of tooling — a
**differential replay harness** — that make the flag day correct and
verifiable.

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
   Appendix A — measured, not asserted). The maintainer's decision is to
   **replace the codec** (§10 Q6), which is its own deliverable.

The decisions of 2026-09-20 are collected in §10; the sections below are
written to them.

---

## 1. Scope

The work splits into **three deliverables** (§10 Q5). This spec is the first.

### 1.1 Deliverable 1 — this spec: make the flag day correct and verifiable

- The **row** — one FSM slot — is the unit of every stage (§10 Q4).
- A stage model for taking a row from version N to N+1 (§3).
- The comparison surface that decides whether an upgrade is correct (§4).
- Conventions the platform mandates and documents (§5).
- **Platform pieces**: the `UpgradePin` cluster record and per-row version
  history (§2.5, §10 Q7); origin pinning — `uc2ctl upgrade pin`, the cnc origin
  words, unconditional install at attach, attach refusal (§3 S4, §10 Q2); the
  artifact envelope's version stamp (§9.1).
- **Tooling**: a differential replay harness — corpus format, three modes,
  black-box execution (§6) — and a skill that drives it (§8).

Everything here is **codec-agnostic**: the pin does not read the payload, the
harness compares bytes and query answers, the taxonomy is about shapes of
change. Deliverable 1 can ship with bincode still in place.

**This deliverable is a UC flag day.** A new `CLUSTER` kind is not additive —
[#31] records that an old node refuses an unknown kind with 42 while `applied`
still advances, so a mixed cluster diverges silently. Stop every node, start
every node; a `2.13.0`, shipped the way every feature since 2.11.0 has been.
The new cnc words *are* additive under the "reads 0 as absent" rule.

### 1.2 Named second phase of deliverable 1

- `Shadow<Old, New>` and the learner shadow deployment (§7).

### 1.3 Out of scope for this spec

- **Deliverable 2 — the typed-over-SBE tier** (§10 Q3, Q6). Replacing
  bincode is an SDK redesign with its own concerns: generator maturity,
  re-measuring the hop cost after the 2.11.0 apply-loop changes, migrating
  `examples/counter`, `uc_lincheck::RegisterSm` (and so the lin capstones and
  the crashtest service), and whatever the remote protocol touches. It gets
  its own spec. This spec *assumes* it: §5.1 and §5.7 are written to the SBE
  header and say so.
- **Deliverable 3 — [#49]**, the `bytes_read` length check. Ships alone, now,
  as the interim fix for the tier that exists today; moot once deliverable 2
  lands, which is exactly why it should not wait.
- **Track 2** — the committed application level and the live-commit version
  gate ([#33], shaped like [#31]'s wire level). §9.2 states what remains
  unsolved without it.
- Node-level rolling upgrades ([#31] proper); leadership transfer.

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

What was missing is any mechanism that *makes* a new-version service start from
it. §3 S4 is that mechanism.

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

**There is a third path the gap guard also gets wrong**, and it is the one that
decided §10 Q2. A **durable** state machine — one whose `last_applied()` is
non-`None` on attach, a shape the SDK explicitly supports (`traits.rs`
documents under-reporting as safe and over-reporting as refused at attach,
which only makes sense for a persisted value) — stopped at `X > P` sees
`first > X` as false whether or not the journal is purged, and simply
**continues from X**, carrying v_old's state for `(P, X]` while its fresh
peers compute `(P, X]` under v_new. Each node's X differs, because "stop every
service" stops each wherever it is. Purging to P does nothing for this case.

Genesis replay is faithful **iff exactly one version has ever applied that
span**. That is true for a never-upgraded cluster and false forever after.
Per §2.5, UC cannot currently tell the two cases apart.

> **Evidence status.** The code paths above are read and quoted. The resulting
> divergences are **not yet experimentally demonstrated** — that demonstration
> is §6.2's `reconstruction` mode, part 1, and it is the harness's first
> teeth-check.

This also makes [#36] (a service restart is silent about how it reconstructed)
load-bearing rather than cosmetic: it is precisely the observation needed to
tell the paths apart.

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

The codec rows describe the **bincode** tier as shipped. Under deliverable 2
(typed-over-SBE, §5.7) the first five rows change character: appended fields
and appended variants become *decodable* by a newer FSM under `sinceVersion`
semantics and *refused by name* by an older one; mid-enum insertion and
reordering become schema-tool errors rather than runtime hazards. The table
should be re-derived when that spec lands.

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
and survives it. That check is [#49], deliverable 3 — the interim fix until the
codec is replaced.

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

**Decided (§10 Q7): the `UpgradePin` cluster record.** A new `CLUSTER kind =
4`, carrying `row: u8 ‖ reserved [u8; 3] ‖ from: u32 ‖ to: u32 ‖ origin: u64`
(20 bytes). It is an *event*, not a tunable — "at position `origin`, row `r`
went from `from` to `to`" — which is why it is its own kind and not a Settings
field: Settings holds current values and an apply replaces them, whereas §2.5's
whole point is that the *sequence* matters.

- **Leader-only, single-in-flight** with the other three kinds (already the
  rule). Refused unless the complete set at `origin` exists on the leader.
- **Cluster FSM state**: a small bounded per-row history of
  `(origin, from, to)` — 8 rows × a handful of entries, since retention keeps
  only a couple of sets. It rides the cluster artifact under `service_id =
  255`, so a below-floor joiner holds the pin **before** its service attaches.
  That ordering is automatic and it is the one that matters.
- **Applied at commit** by `uc2-cluster`, which writes the row's
  `upgrade_origin` (u64) and `pinned_version` (u32) words to the row's cnc slot
  line. Additive: 0 = no pin.
- `uc2ctl upgrade pin --row r --to <ver> --origin P`; `uc2ctl upgrade show`;
  audited as `upgrade_pin`; gauges `uc2_upgrade_pin_origin{row}` and
  `uc2_upgrade_pin_version{row}`.
- **Refusals 51–54, by name**: `pin_row_undeclared`, `pin_from_mismatch`
  (`from` ≠ the row's current pinned/attached version), `pin_no_set` (no
  complete set at `origin`), `pin_not_monotone` (`origin` ≤ the row's current
  pin).

With the pin history in the cluster FSM, "which version built `snap-<P>`?" is
answerable for any retained artifact: the pin in effect at P. §9.1 adds the
envelope stamp that lets the artifact answer it for itself.

**What this deliberately does not buy:** faithful genesis replay. Even with a
complete version history in the log, replaying `[0,Q]` correctly would require
every historical binary. That remains the explicit non-goal of §1.4. The record
exists to make the *wrong* path detectable and refusable, not to make the
impossible path possible.

---

## 3. The lifecycle

Nine stages, **each scoped to one row** (§10 Q4). A whole-deployment upgrade is
N per-row upgrades that happen to share an origin; nothing requires them to.
Each stage names its artifact and its gate.

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

What it does not buy: any live-commit gate ([#33]). What it *will* buy after
deliverable 1: a durable record of when the version changed (§2.5), and attach
refusal against the pin (S4).

`NAME` is never bumped. It is the identity hash *and* the `fold32` input to
`IdGen`; changing it is not a version change, it is a different FSM with a
different id stream.

**The digit mapping** (closing a gap the standard leaves open): major = any
§2.4 row whose axis-P risk is "severe" or "worst"; minor = additive-but-inert
(and the harness must confirm the inertness); patch = no replicated behaviour
change at all. Under deliverable 2 this maps onto SBE directly — **UC major ↔
SBE `templateId`/`schemaId`, UC minor ↔ SBE `version`** — and the framework's
header check (§5.1) is written to that mapping. Aeron's convention (major
equality as the kill switch) is named in [#31] as a reserved validator and is
the natural consumer.

### S3 — Write the compatibility shims

Three shims, and they are not interchangeable:

- **Forward decode** (old binary, new command) — *cannot be written
  retroactively*. It has to have been in v_old. This is the single strongest
  argument for the version tag (§5.1) being present from day one — which the
  SBE header makes automatic.
- **Backward apply** (new binary, old command) — bounded by S9, not permanent.
  Under SBE this is `sinceVersion` semantics rather than hand-kept per-version
  decode paths.
- **Snapshot dual-read** (new binary, old image) — what `examples/kv` hand-rolled
  as `IMAGE_VERSION_V1`/`V2` (`examples/kv/src/lib.rs:66-68`).

### S4 — Pin the origin

**The stage that does not exist today; decided (§10 Q2): explicit pin,
unconditional install, attach refusal.** Before stopping any instance of the
row:

1. `uc2ctl snapshot` → the complete set at P on every node (exists).
2. `uc2ctl upgrade pin --row r --to <ver> --origin P` → appends the `UpgradePin`
   record (§2.5). Refused by name if the set at P is incomplete, the row is
   undeclared, `from` does not match, or P is not above the row's current pin.
3. The cluster agent applies it and writes the row's `upgrade_origin` /
   `pinned_version` cnc words.
4. Stop every instance of row r, swap the binary, start. At attach a service
   reads the words: `upgrade_origin > 0` and its `VERSION` equals
   `pinned_version` → **install `snap-<P>` unconditionally**, overriding both
   the gap guard and its own `last_applied()`. A durable SM at `X > P` is
   rewound to P and recomputes `(P, X]` under v_new — the same thing its fresh
   peers do. It already has to support `install_snapshot` to be
   snapshot-capable, and step 1 already requires that capability (refusal 48),
   so this asks nothing new of it.
5. **Refusal backstop**: a service whose `VERSION` ≠ `pinned_version` is
   refused at attach, by name. A stale v_old binary cannot rejoin after the pin
   — a partial [#33] mitigation at the attach boundary, without touching the
   live-commit path.

**Why not purge.** An earlier revision recommended advancing the purge floor
to P as the mechanism. It is insufficient: purge only removes the genesis path
for an SM that starts *empty*. §2.3's third path — a durable SM continuing from
its own `last_applied()` above P — is untouched by purge, and diverges. Purge
to P remains **correct and worthwhile as hygiene** (the prefix below P has no
replay use for any binary the cluster will ever run again, and reclaiming it
also settles [#41]'s framing — the off-node backup is the rollback point, the
on-disk prefix never was), but correctness does not rest on it.

**Upgrade requires snapshot capability.** Step 1 refuses on a row started with
plain `start()`. That is not a new constraint; it is the existing one made
visible.

**A documented limit, not a defect.** Between the pin (step 2) and the stop
(step 4), v_old keeps applying, and its leader's `on_committed` emits external
side effects for `(P, X]`. After the rewind, v_new recomputes `(P, X]` but the
durable, increase-only `output_progress` marker stops it re-emitting. So the
outside world saw v_old's effects for a span whose *state* is now v_new's. It
is bounded and small; stopping promptly after the pin shrinks it. The clean
fix — the pin also halting apply at P — costs a check in the apply hot loop,
which the 2.11.0 regression record argues against paying for a window this
size. Recorded as a limit; revisit if a real deployment finds it matters.

### S5 — Verify differentially

Run the harness (§6). Gate: the divergence profile contains no *unexpected*
entries (§4.3).

### S6 — Roll out

The existing flag day (`docs/how-to/upgrade-an-application.md`) becomes a
**per-row** flag day: "stop every instance of row r", not "stop every
service". S4 is inserted before it.

**Cost to the other rows.** Under bounded or lockstep lag, stopping row r's
service on a quorum of hosts stalls commit until it is back — M14's "one
stalled FSM on a quorum stalls commit by design". A per-row upgrade is not
invisible to its neighbours; it is exactly the same commit pause a
whole-deployment flag day costs, so it is not a reason to prefer one over the
other. Also: the coordinated instant in S4 step 1 freezes *every* row at P,
not just row r — harmless (the others take a snapshot they did not strictly
need) and useful (P is a complete set, well-defined cluster-wide regardless of
which row is moving).

### S7 — Confirm

Every instance of row r reports the new version; every replica agrees.
Cross-replica agreement is an *image-digest* comparison and is valid here
precisely because all replicas are now the same version (§4.1).

### S8 — Decide the point of no return

"Rollback" is largely a fiction in an SMR system: swapping the binary back does
not roll back the log, and once v_new semantics have been applied to committed
frames that state is v_new's. `upgrade-an-application.md` is already honest
about this — the real rollback is restoring the off-node backup on every node,
discarding every write acked since. The SDLC standard's "rollback plan" (§5)
should be renamed and reframed as a **point-of-no-return plan**: what is the
last moment abandonment is possible, and what does it cost? After S4 step 2 the
pin is committed and monotone; that is the moment.

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

### 5.1 The version tag is the SBE header; the framework enforces it

**Decided (§10 Q3).** Every command must carry a version tag readable
**without decoding the rest**, so an old binary can determine "this is beyond
me" and refuse *before* interpreting the body. Appendix A's case E is the
demonstration: two same-typed fields reordered decode successfully, with the
correct byte count, into swapped values. No codec-level check catches it. A tag
ahead of the payload is the only thing that can.

Under deliverable 2 that tag is **SBE's own message header** (`schemaId`,
`templateId`, `version`, `blockLength`). UC adds nothing ahead of it. What UC
*does* own is the check, and it is split the way the codebase already splits:

- **Typed tier — framework-enforced.** The tier owns the codec, so it owns
  the check. The blanket impl reads the header before handing bytes to the
  generated decoder:
  - `templateId`/`schemaId` ≠ the FSM's → **refuse, by name** (a major
    mismatch, either direction).
  - header `version` > the FSM's schema version → **refuse, by name**. This
    suppresses SBE's tolerant skip (§5.7): an old FSM never applies a
    truncated reading of a newer command.
  - header `version` ≤ the FSM's → decode with `sinceVersion` semantics. A 1.4
    FSM genuinely reads a 1.3 command, absent fields at their declared nulls.
    **This is the part bincode could never give.**
- **Raw tier — documented convention.** The app took ownership of its bytes;
  forcing UC's header onto a flatbuffers user is presumptuous, and an SBE user
  already has one. The contract is written down ("your first bytes identify
  the schema version; SBE's header satisfies it"), not enforced.

The rule follows from §2.4 and the S2 digit mapping: a major bump is a breaking
schema change (refuse both directions); a minor is additive, so an FSM knows
every minor *below* its own and cannot decode one above it.

`examples/kv` independently converged on the prefix-byte form of this
(`FORMAT_VERSION: u8 = 1`, `examples/kv/src/wire.rs:13`); it is the hand-rolled
version of what the SBE header provides.

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
that command across versions), never a schema-level default. §5.1's header
check is where the default is suppressed.

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
same-deploy-unit use. Three responses, in increasing order of consequence, and
then the decision.

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
generated and enforced rather than hand-rolled.

**A consequence the doc's own taxonomy implies.** Its split is cross-version →
SBE, same-deploy-unit ("WAL records read only by the process that wrote them")
→ bincode. A replicated command log is **neither**: it is written by one
version and read by every replica *and by every future version*. By the doc's
own criteria, bincode is the wrong choice for UC command payloads — and the
typed tier, the documented easy path, uses exactly that.

**Decided (§10 Q6): bincode goes. The typed tier becomes typed-over-SBE.**
SBE is not a serde backend — it is codegen from an XML schema — so this is a
redesign of the tier's contract (the `serde` bounds on `Command`/`Response`/
`Query`/`QueryResponse` go away; the tier becomes "a `StateMachine` over
SBE-generated message types"), not a backend swap. UC supplies the glue: a
bound on generated codecs, the §5.1 header check, an encode helper for clients.
The tier survives rather than collapsing into raw because that is the only
shape in which the version check cannot be skipped. It is **deliverable 2**,
its own spec (§1.3).

*Two inputs that spec must verify rather than trust: the maturity of the Rust
SBE generator, and the 2026-08-22 codec spike's finding that SBE costs the same
as the raw tier at the hop — recorded, not re-run, and older than the 2.11.0
apply-loop changes.*

---

## 6. Tooling: the differential replay harness

One machine, three configurations. Building it as one tool rather than two
avoids two comparison implementations drifting apart.

### 6.1 Corpus

A corpus is **(snapshot artifact at P, journal span P→Q, the version that built
the artifact)** — the first two are what `uc2ctl backup` already produces, and
the third comes from the pin history (§2.5) or the envelope stamp (§9.1) once
those exist, and is carried alongside by hand until then.

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
| **`reconstruction`** | one build, **two start states**: genesis-replay (or continue-from-X) vs. install-artifact-at-P + tail-replay | the §2.3 counterfactuals |

That the determinism check falls out as a degenerate case is the main argument
for this shape. It also makes [#38]'s item 2 (a determinism *lint*) largely
redundant: a runtime differential is strictly stronger, because a lint can
enumerate `SystemTime::now()` but can never catch "this version mints a
different number of ids".

**The `reconstruction` mode's assertion is not equality.** Under §1.4 and §2.3
the two paths *should* differ after any semantic change, because one of them
is computing a counterfactual. The mode's job is therefore in two parts, with
different dependencies:

1. **Demonstrate** that the paths diverge — the teeth-check that the harness
   detects a real defect rather than passing vacuously, and the demonstration
   §2.3 currently lacks. Depends on nothing; **build this first.**
2. **Verify that the system refuses the wrong one** — that S4's unconditional
   install and attach refusal actually prevent a v_new service from
   reconstructing below the pinned origin, for both the empty and the
   durable-SM shapes. Depends on S4 landing.

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
  command types (or SBE schemas) can.
- **Spot the invisible determinism hazards**: a changed `ids()` call count, a
  newly-introduced `HashMap` iteration, a float op. The first is a *diff*
  property and genuinely outside a lint's reach.
- **Spot the Appendix A shapes in a diff**: a variant inserted mid-enum, two
  fields reordered. Both are one-line diffs with catastrophic consequences and
  both are trivially recognizable by reading the diff. (Under SBE these become
  schema-tool errors; the skill's job then is to read the schema diff instead.)
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

**(1) The typed tier discards `bytes_read`** — filed as [#49], **deliverable
3**. All three decode sites (`uc_service/src/traits.rs:326, 336, 484`)
destructure as `let (cmd, _) = ...`, and `decode_from_slice` does not require
consuming the buffer. Per Appendix A, **four of the five measured silent
misparses leave `bytes_read < len`** and would become the intended fail-stop
under a length check.

The check is viable: `uc_log/src/reader.rs:150` slices the payload as
`&buf[self.boff + HEADER_LEN..self.boff + length]` — the header's `length`, not
the 32-byte-aligned span — so the payload handed to `apply` is exact and the
assertion has no padding false-positives.

*Verified for the bare frame path only. The `Sessioned` and `Timed` wrappers
derive their inner slice from their own envelopes; whether those are equally
exact has not been traced and must be before the check is added there.*

Moot once deliverable 2 replaces the codec; ships now regardless.

**(2) The snapshot artifact has no version provenance** — **decided (§10 Q7
rider): stamp it.** The envelope becomes `ULTSNAP2 ‖ P ‖ version`, with
pre-`ULTSNAP2` artifacts refused by name, exactly as the last envelope bump was
handled ("clear a dev box's `snapshots/` once"). The `UpgradePin` history is
the authority; the stamp lets `install_snapshot` cross-check that the artifact
it is about to install was built by the version the pin says was in effect at
P, and makes an artifact self-describing off-cluster (a backup on a shelf).
Part of deliverable 1.

### 9.2 What this spec does not solve

Without Track 2 ([#33]), a mixed-version *live commit* can still acknowledge a
write no quorum can apply. S4's attach refusal narrows the window — a stale
binary cannot rejoin after the pin — but a v_old service that is *already
attached* when a v_new leader commits a v_new-only command is still the [#33]
hazard. Everything here makes the flag day verifiable; none of it makes a
rolling application upgrade safe. The honest answer to "how do I upgrade"
remains "per-row flag day" until the committed application level and the
live-commit gate exist.

What this spec *does* buy Track 2: [#31]'s own proposal (step 7) requires a
proof surface — a two-binary crashtest under load with a linearizable history —
to ship *with* the rolling-upgrade flag day. §6 is that apparatus. Building it
first means Track 2 arrives with its proof already in the tree.

---

## 10. Decisions (2026-09-20)

Taken with the maintainer one question at a time; the sections above are
written to them.

- **Q1** *(bincode's behaviour on a version mismatch)* — **answered by
  measurement**, Appendix A. Silent misparse in 5 of 12 probes; 4 of the 5 are
  UC's missing length check ([#49]).
- **Q2** *(origin-pinning mechanism)* — **(b) explicit pin with unconditional
  install at attach, plus (c) attach refusal. (a) purge-to-P demoted to
  hygiene.** Deciding argument: purge only removes the genesis path for an SM
  that starts empty; a durable SM continuing from its own `last_applied()`
  above P is untouched by purge and diverges (§2.3, third path). Reverses
  revision 2's recommendation, which was made without considering that shape.
- **Q3** *(framework envelope vs. convention)* — **framework-enforced on the
  typed tier, documented convention on the raw tier**; the tag itself is
  SBE's message header once Q6 lands. Rule: template/schema id must match;
  header version above the FSM's is refused; at or below decodes under
  `sinceVersion`.
- **Q4** *(per-row version skew)* — **supported; the row is the unit of every
  stage.** The `SNAP_BEGIN` check (`receiver.rs:2367`) compares each row
  between nodes, never across rows, and `Uc2ServiceVersionDrift` is
  `count by (row)`; the state model is per-row throughout. Two cross-row
  costs documented in S6.
- **Q5** *(scope)* — **three deliverables**: this spec (docs + pin + record +
  harness + skill, with `Shadow` as phase 2); the typed-over-SBE tier as its
  own spec; [#49] alone, now. Deliverable 1 is a UC flag day (new `CLUSTER`
  kind).
- **Q6** *(does the typed tier stay on bincode?)* — **no. bincode is
  replaced; the typed tier becomes typed-over-SBE.** Own spec.
- **Q7** *(version-change record shape)* — **a new `CLUSTER kind = 4`,
  `UpgradePin`**, 20-byte payload, per-row bounded history in the cluster FSM,
  refusals 51–54, audited `upgrade_pin`; **plus** the `ULTSNAP2` envelope
  stamp. Not a Settings field: it is an event with a position, the *sequence*
  is what matters, and it wants its own refusals.

---

## 11. Work breakdown

### Deliverable 1 — this spec

| # | deliverable | kind | depends on |
|---|---|---|---|
| 1 | §2.1 axes, §2.4 taxonomy, §2.2 common origin, §2.5 version-as-input, §3 per-row stages → folded into `application-sdlc.md`; the how-to becomes per-row | docs | — |
| 2 | §5 conventions, written to the SBE header and pointing at deliverable 2 | docs | 1 |
| 3 | Corpus format + trimmed export (§6.1) | code | — |
| 4 | Harness, black-box; `reconstruction` mode **part 1** first (§6.2) | code | 3 |
| 5 | `UpgradePin` cluster record + per-row history + cnc words + `uc2ctl upgrade pin/show` + refusals 51–54 + audit + gauges (§2.5) | code, **flag day** | — |
| 6 | Unconditional install at attach + attach refusal (§3 S4 steps 4–5) | code | 5 |
| 7 | `ULTSNAP2` envelope stamp + `install_snapshot` cross-check (§9.1) | code | 5 |
| 8 | `reconstruction` mode **part 2** — verify the refusal, empty and durable shapes (§6.2) | code | 4, 6 |
| 9 | Skill (§8) | skill | 1, 4 |
| 10 | White-box mode + `Shadow` (§7) — phase 2 | code | 4, §5.3 |
| 11 | Learner shadow deployment — phase 2 | docs + ops | 10 |

### Deliverable 2 — typed-over-SBE (own spec)

Not broken down here. Inputs it must settle: Rust SBE generator maturity; hop
cost re-measured; migration of `examples/counter`, `uc_lincheck::RegisterSm`,
the crashtest service; whether the remote protocol is touched; the §2.4 table
re-derived for SBE.

### Deliverable 3 — [#49]

Ships alone. Trace `Sessioned`/`Timed` inner-slice exactness first.

### Not scheduled

Track 2 ([#33] / [#31]) — separate spec.

---

## 12. Evidence register

Read and quoted in this tree (worktree `fsm-upgrade-lifecycle`, `main` @ `47e74e4`):

- `uc_service/src/replay.rs:207-228` — the gap guard; snapshot installed only when `first > start_pos`.
- `uc_node/src/node.rs:171-173` — `PurgePolicy` with `#[default] Disabled`.
- `uc_log/src/reader.rs:150` — `FrameIter::next` slices the payload to the header's `length`, not the aligned span.
- `uc_service/src/traits.rs:326, 336, 484` — three typed-tier decodes, each discarding `bytes_read`; `last_applied()` documented as under-report-safe / over-report-refused (the durable-SM shape).
- `uc_service/src/snapshots.rs:58,62` — the 16-byte `ULTSNAP1 ‖ P` envelope; no version field.
- `uc_net/src/receiver.rs:2364-2385` — `SNAP_BEGIN` version comparison; per-row between nodes; nonzero-vs-nonzero inequality refuses; `0` is "unknown".
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
- Issues [#31], [#33], [#36], [#38], [#41], [#42] — read via the REST API; [#49] filed from this spec.

**Run this session:** the Appendix A measurement (`cargo run` against bincode
`=2.0.1`, private `CARGO_TARGET_DIR`), output reproduced verbatim below.

**Not verified / not run:** the §2.3 divergences are derived from the quoted
code paths, not demonstrated (§6.2 part 1 is that demonstration). The
`Sessioned` / `Timed` inner-slice exactness for [#49] is untraced. The recorded
2026-08-22 finding that SBE costs the same as the raw tier has not been re-run.
The Rust SBE generator's maturity has not been assessed.

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
   length check UC does not perform ([#49]).
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
[#49]: https://github.com/PeterKnego/ultima_cluster/issues/49
