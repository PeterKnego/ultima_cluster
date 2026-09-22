# SDLC Standard for Applications Built on UltimaCluster

**Scope:** This document defines the software development lifecycle for any application logic that runs inside the UltimaCluster state machine replication (SMR) layer, or otherwise interacts directly with the replicated log. It is not a general engineering process document — it exists because SMR applications fail in ways ordinary applications don't: silent state divergence, replay corruption, and non-reproducible bugs that only appear after a leader election or restart.

---

## 1. Design Phase

Before any code is written, every new state machine feature or app must produce a short design note covering:

- **Determinism boundary.** Which parts of the feature execute inside the replicated state machine ("hot path") vs. outside it (client-side, read-only services, async side effects)? Draw this boundary explicitly.
- **State shape.** What new fields/collections are added to replicated state? What is their serialization format and version?
- **Failure semantics.** What happens to this feature during leader election, node crash-restart, network partition, or a mixed-version cluster during rolling upgrade?
- **Invariants.** What must always be true about this state (e.g., "position size is monotonically adjusted only via matched trades," "applied index never regresses")? These become property-based test targets later.

Design notes are reviewed by at least one person with cluster-layer experience before implementation starts — this is the cheapest point to catch a determinism violation, long before it becomes a 2am incident.

### The three compatibility axes

Name which axis a change touches before you design it. The three have different windows and different closure conditions, and conflating them is where most upgrade reasoning goes wrong.

| axis | question | window | closed by |
|---|---|---|---|
| **P — peer** | Does my binary interoperate with my sibling's binary, right now? | the upgrade window | finishing the roll |
| **H — history** | Can my binary apply commands written to the log arbitrarily long ago? | **unbounded by default** | a pinned origin above the last occurrence ([S9](#s9-close-axis-h)) |
| **F — framework** | Does my FSM version work on UC 2.12 *and* 2.13? | while straddling UC releases | a UC flag day ([Upgrade a cluster](../how-to/upgrade-a-cluster.md)) |

Close axis P by finishing the roll, and keep the window short. Axis-P failures are loud and bounded — you are in the window, watching — which is why the shipped answer is a flag day per row rather than a rolling swap: UC has no live-commit version gate, so a mixed-version row can commit a command an old replica cannot apply, and a failover to that replica then loses an acknowledged write.

Treat axis H as permanent until you close it deliberately. Axis-H failures are quiet and detonate later, on a node reconstructing across a span whose commands the current binary interprets differently — a new binary reading an old `Put(11,22)` after someone inserted an enum variant mid-enum applies `Delete(11)`, with no error anywhere (measured; the spec's Appendix A, case C). Nothing in the upgrade window sees that, so nothing in the upgrade window closes it: [S9](#s9-close-axis-h) does.

Pin axis F to one UC minor and move it deliberately. UC publishes no per-cell FSM-version × UC-release matrix; it publishes [the semver policy](semver-policy.md) — what is API and what is a flag day — and [Upgrade a cluster](../how-to/upgrade-a-cluster.md). A UC release that moves the wire version or the `cnc.dat` layout is all-nodes-together, so schedule it as its own event and never inside an application upgrade.

### The change taxonomy

Classify the change before you design against it: the classification determines every obligation downstream, which is why it is stage [S1](#s1-classify-the-change) of the lifecycle and the first thing a version bump produces.

| what changed | replicated? | axis-P risk | axis-H risk | downgrade-safe? |
|---|---|---|---|---|
| New command variant (appended) | yes | **severe** — [#33](https://github.com/PeterKnego/ultima_cluster/issues/33): old replica cannot apply; leader acks anyway | permanent until S9 closes it | no |
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

"App. A, case X" is the probe that measured that cell: the spec's Appendix A (`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`), 12 probes of realistic schema changes, of which **5 decode successfully into a wrong value** with no error.

**These rows describe the `bincode` typed tier as shipped.** Under the planned typed-over-SBE tier the first five rows change character — appended fields and appended variants become decodable by a newer FSM and refused *by name* by an older one, and mid-enum insertion and reordering become schema-tool errors rather than runtime hazards. That tier is its own unwritten spec (the design spec's §5.7, deliverable 2); classify against the table above until it lands, and re-derive the table when it does.

Two rows deserve expansion, because ordinary testing cannot see them.

**The `IdGen` trap.** `uc_service/src/ids.rs` derives an id from the frame's position, the FSM's identity, and the **ordinal within the generator that minted it** — and those three are its only inputs. Two different changes follow, and they are not interchangeable. Ask for a *second generator* in the same `apply` (a second `ctx.ids()`) and it starts from ordinal 0 over the same position and identity, so it mints the **identical series** as the first: duplicated ids, not shifted ones. Take one *more id from one generator* (an extra `IdGen::next()`) and every later id in that call shifts, and a replica replaying the span under the new binary gets different ids than the old binary wrote. No single-version test can see either; both are *diff* properties. Diff replay counts each frame's `ctx.ids()` calls (`ApplyCtx::ids_calls`) and reports a change as a surface of its own — so the first is checked directly, and the second, which no counter records, surfaces only through the responses and the state it feeds. Review both: how many generators an arm takes, and how many ids it mints from each.

**The decode trap — worse than a fail-stop.** Of the 5 silent misparses above, **4 leave `bytes_read < cmd.len()`**: a decoder that required the buffer to be consumed would turn them into the intended fail-stop. UC's typed tier discards `bytes_read` at all three decode sites (`uc_service/src/traits.rs:361`), so today it does not; the length check is filed as [#49](https://github.com/PeterKnego/ultima_cluster/issues/49) and is not shipped. The fifth case — two same-typed fields reordered — is byte- and length-identical and survives any length check; only a version tag ahead of the payload catches it ([Schema and protocol conventions](#schema-and-protocol-conventions)).

### The common origin

Two versions of an FSM produce different state from the same input. That is the *point* of an upgrade, not a defect, so the requirement is not equality between versions. It is:

> **Every instance of the new version must begin from the same log position, reconstructed from the same artifact.**

Design to that requirement rather than to peer agreement, because the artifact is load-bearing. A snapshot built at position P by the old version encodes the state *as that version actually produced it* over `[0, P)`, and once the old binary is gone that state is not recoverable from the log bytes alone. **The artifact is the only faithful carrier of pre-upgrade history.** Replaying `[0, Q]` under the new binary instead computes the state the cluster *would* have had if it had always run the new version — a counterfactual that never existed, and one a long-lived fleet reaches silently, because nodes get replaced and their journal histories differ.

UC supplies both halves. The **coordinated snapshot instant** (`FRAME_TYPE_SNAPSHOT = 7`, 2.11.0) is the origin: the frame's end position P *is* the instant, every declared row and the cluster FSM freeze at P, and a node holds the complete set at P when every row's `snap-<P>.ultsnap` and `snapshots/cluster/snap-<P>.ultcluster` exist — committed by construction. The **upgrade pin** (2.13.0) is the mechanism that makes every new-version instance actually start from it, and it is stage [S4](#s4-pin-the-origin).

### The version is an input

The state at position Q is not a function of the log prefix `[0, Q]`. It is a function of that prefix **and** of the sequence of application versions that applied each span of it — because two versions legitimately produce different state from the same commands. The application version is therefore an input to the state transition, exactly like a command, and in an SMR system inputs belong in the log.

Since 2.13.0 this one is recorded. `uc2ctl upgrade pin` appends an `UpgradePin` cluster record (`CLUSTER kind = 4`, 20 bytes: `row ‖ from ‖ to ‖ origin`) — an *event*, not a tunable: "at position `origin`, row `r` went from `from` to `to`". Like every cluster command it is leader-only and single-in-flight; its payload is staged as `<instance_dir>/upgrade.pending` (mode `0600`, fsync, rename) with only a 10-byte digest riding the signed admin line; and it is applied at commit by the `uc2-cluster` agent. What that buys:

- **A durable per-row history**, at most 4 events per row, held in the cluster FSM and carried in the cluster artifact under `service_id = 255` — so a below-floor joiner holds the pin *before* its service attaches. `uc2ctl upgrade show` prints it, and "which version built `snap-<P>`?" is answerable: the pin in effect at P.
- **Four cnc words per row**, on the row's service status line (cnc 3.3): `upgrade_origin` (`+16`, `0` = no pin), `pinned_version` (`+24`), the seqlock commit word `pin_seq` (`+32`), and `pinned_from` (`+40`) — the version that *built* the origin's artifact, which the pinned install cross-checks against. Every reader goes through the seqlock, and a read that never settles is its own outcome at the decision that matters: an attach refuses rather than treating it as "no pin". A *display* is looser — `uc2ctl status` renders an unsettled read as zeros, the same as no pin — so a single zero line is a reading to repeat, not evidence that the row is unpinned.
- **Refusals by name, 52–59**: `pin_row_undeclared` (52), `pin_from_mismatch` (53), `pin_no_set` (54), `pin_not_monotone` (55), `pin_digest` (56), `pin_missing` (57), `pin_decode` (58), and `report_stale` (59) for the companion `SnapshotReport` record. [uc2ctl § Refusal reasons](uc2ctl.md#refusal-reasons) is the table.

What this deliberately does **not** buy is faithful replay from genesis: replaying `[0, Q]` correctly would require every historical binary, which UC does not keep. The record exists to make the wrong path detectable and refusable, not to make the impossible path possible.

---

## 2. Implementation Phase

### Determinism rules (non-negotiable)

Code inside the replicated path must never:
- Read wall-clock time directly (`SystemTime::now()`, `Instant::now()`) — time must come from the replicated log entry (`ctx.time_ns`).
- Generate randomness without a log-supplied seed, or mint ids other than through `ctx.ids()`.
- Rely on iteration order of non-ordered collections (`HashMap`/`HashSet`) — use `BTreeMap`/`BTreeSet` or an explicitly ordered structure.
- Perform I/O, spawn threads, or call external services.
- Use floating-point operations whose results can vary across CPU architectures, unless verified bit-identical across your deployment targets (relevant given cross-arch dev/prod use, e.g. Graviton in production).

**Enforcement:** a custom clippy lint (or at minimum a documented code-review checklist) flags these patterns. Ideally, the "deterministic core" lives in its own crate/module with a restricted dependency graph so non-deterministic crates can't even be imported. A lint is the weaker half: run `uc2-diffreplay determinism` (one build, two processes, one corpus) as the everyday check, because it catches what a lint cannot — Rust randomizes `RandomState` per process, so a state machine that depends on hash order already disagrees with itself across two processes, and a changed `ids()` call count shows up as a diff that no enumeration of banned calls could find.

### Schema and protocol conventions

Nine rules. Each is the rule an application follows today, with what enforces it today.

#### Carry a version tag ahead of the payload

Every command should carry a version tag readable **without decoding the rest**, so an old binary can determine "this is beyond me" and refuse *before* interpreting the body. Nothing else catches a field reorder: two same-typed fields swapped decode successfully, with the correct byte count, into swapped values. **Today** the only tag UC checks is the FSM's `const VERSION` (`identity::pack_version(major, minor, patch)`), and it is checked at two boundaries — the snapshot session, where a nonzero-vs-nonzero inequality is refused per row, and the pinned attach ([S4](#s4-pin-the-origin)) — never per command. So put a version byte in your own command encoding too, and check it in `apply`: `examples/kv` does (`FORMAT_VERSION: u8 = 1`, `examples/kv/src/wire.rs`). The framework-enforced per-command header arrives with the typed-over-SBE tier (the design spec's §5.1 and §5.7, deliverable 2); until then the check is yours.

#### Tolerant readers are wrong for replication

A tolerant reader — one that skips fields it does not recognize — is correct for messaging and actively harmful in an apply loop: it lets an old replica **silently apply a different command** than the new replica applied, converting a loud failure into a quiet one. What an SMR system needs is the opposite of tolerance: cheap, reliable recognition of incapacity. Tolerant decode *is* correct when the added field is genuinely inert to the old version, but the platform cannot tell that case from the dangerous one — only the author can — so it is an explicit per-change assertion you declare and diff replay checks (identical state and responses for that command across versions), never a schema-level default.

#### Package the FSM as a library crate

Not as a module inside the service binary. A library crate is what lets cargo depend on **two versions under renamed packages**, which is the precondition for white-box diff replay and for the future `Shadow<Old, New>` learner. Retrofitting it is painful, so do it on day one even though nothing needs it yet.

#### Queries are the probe suite

Keep a stable, documented set of queries whose answers characterize the state, and treat their schema as a compatibility surface of its own. Queries are the one surface that genuinely rolls (the taxonomy's `Query`/`QueryResponse` row), which is what makes them usable as a probe across a version boundary — and `query` gets no `ApplyCtx`, so it can never perturb what it measures.

#### Expand, migrate, contract — two releases, not one

Adding a command is **two rollouts**: version 1.5 understands the new command but never emits it; deploy 1.5 everywhere; only then flip clients to emit it. This works today with zero platform change, and it is the only way to add a command without an axis-P window in which a leader acks something a replica cannot apply. The same shape covers a field: add it inert, migrate, and remove the old reader in a later version.

#### Timer ids are a permanent namespace

The pending timer set is log-derived and `Timed<S>` makes delivery exactly-once, so the new version **inherits the old version's in-flight timer instances**, and the replicated schedule table keys on `(identity_hash, timer_id)`. Repurposing a timer id across versions is therefore a replicated-state change, not an implementation detail: allocate ids monotonically and retire them rather than reusing them.

#### Choose the codec deliberately

The typed tier is `serde` + `bincode` today, and its evolution properties are measured, not assumed: silent misparse is real (5 of 12 probes), 4 of those 5 would fail-stop under a length check UC does not perform ([#49](https://github.com/PeterKnego/ultima_cluster/issues/49)), and one — a field reorder — survives any length check. Three consequences for your schema: never insert an enum variant mid-enum, never reorder fields, and never append a field (an `Option` included) to a command an older binary will read. Widening an integer is the one measured-safe evolution. The decided direction is to replace the tier's codec with SBE-generated message types — taking SBE's header and *suppressing* its tolerant skip, which is a liability in an apply loop — but that is deliverable 2's own spec (§5.7) and is **not shipped**: 2.13.0 ships the bincode tier, so the constraints above are the ones in force.

#### Provide a state projection

An FSM that can be snapshotted should render its state as **canonical, diffable text** — one record per line, sorted, stable, no timestamps or addresses — so two builds' states can be compared across a version boundary where the image bytes cannot be. **Today** that is the provided `SnapshotStateMachine::project(&self, out)` hook (default: a named refusal, so a bare SM keeps working), plus a `project` subcommand on your service binary for black-box use; `examples/kv` is the worked example. Two SMs with identical logical state must produce byte-identical projections — `uc2-diffreplay determinism` checks exactly that.

#### Keep a regression corpus

A bug that needed a particular input to trigger is reproducible from a corpus, so keep the corpus: `uc2-diffreplay corpus export --around <pos>` captures the smallest one, and it is replayed under every future build. A corpus is the artifact, the span, **and** the declaration ("position X now does Y, nothing else changes") — a corpus without its `intent.toml` is a recording, not a test. `examples/kv/tests/corpora/README.md` is the worked convention; its `regression_corpora.rs` replays every corpus there on each `cargo test`.

### Code review gates

In addition to normal review, cluster-path changes require sign-off confirming:
- No determinism rule violations (the determinism rules above)
- The change is classified against [the taxonomy](#the-change-taxonomy), and the obligations that classification carries are met
- Any new state field has a versioning plan
- Invariants from the design note are covered by tests (Section 3)

---

## 3. Testing Phase

Testing for SMR applications is structured as a pyramid, with the top layers being mandatory CI gates, not optional extras.

| Layer | Purpose | Gate |
|---|---|---|
| Unit tests | Standard logic correctness | Every PR |
| Property-based tests (`proptest`) | Verify invariants from the design note hold under randomized inputs | Every PR touching state machine logic |
| Golden replay tests | Record real log sequences; replay on a fresh instance; assert identical resulting state hash | Every PR touching replicated state |
| Cross-node divergence checks | Periodic state hash comparison across replicas | Staging, continuous |
| Fault injection / chaos tests | Network partition, crash-restart, disk full, clock skew | Required before merging cluster-layer changes |
| Consistency checking (Jepsen/Elle-style) | Catch linearizability violations that unit/integration tests miss | Scheduled runs + before major releases |
| Fuzzing (wire format decode paths) | Malformed/adversarial input handling | Continuous (`cargo-fuzz`), tracked as a backlog burn-down |

Two of those rows have shipped tools. Golden replay is `uc2-diffreplay` over a kept corpus ([Diff replay an FSM change](../how-to/diff-replay.md)); cross-node divergence checking is live and needs no harness of yours — every node hashes each row artifact as its builder streams it, the leader commits one `SnapshotReport` per `(row, P)`, and every replica computes the same verdict from it, which you read as `uc2_snapshot_hash_mismatch` on `/metrics` or with `uc2ctl upgrade show`. UC also ships an Elle-based checker and a linearizability checker for its own core (`docs/VERIFICATION.md`).

---

## 4. Verification Phase (for high-risk core logic)

Not every feature needs formal proof, but the highest-risk components do:
- Log durability and commit semantics
- Snapshot correctness and restore logic
- Any code that determines what gets applied vs. rejected

For these, the standard is: property-based tests as the everyday gate, with formal verification (Lean 4 / Aeneas) reserved for the small, stable core where a bug would cause silent, unrecoverable state corruption rather than a crash.

---

## 5. Rollout Phase

> **Status at 2.13.0.** An application upgrade is a **flag day, per row**: take
> a coordinated instant, pin the row's next version to it, then stop every
> instance of that row, swap the binary, and start. A *rolling* application
> upgrade — two FSM versions applying on the live commit path — is still not
> supported, because a `VERSION` mismatch is not detected at commit
> ([#33](https://github.com/PeterKnego/ultima_cluster/issues/33); the
> log-stamped half of an FSM version is `docs/BACKLOG.md` item 3's "FSM
> version" bullet). What 2.13.0 added is the pinned origin and the
> refusals that make the flag day *correct and verifiable*, not a rolling
> path. The stages are [The upgrade lifecycle, per row](#the-upgrade-lifecycle-per-row)
> below; the operator procedure is
> [Upgrade an application](../how-to/upgrade-an-application.md).

- **Rehearse; do not canary.** Do not run a newer FSM version on a learner ahead of the cluster, and note that nothing stops you: **before** the pin exists, attach reads "no pin" and refuses nothing, so such a learner either fail-stops on a cross-version artifact when reconstruction needs one (the unpinned install requires the artifact's version to equal the running binary's) or, on an unpurged journal, quietly replays the span itself and computes the counterfactual — no error, and a state that never existed. **After** the pin, any binary that is not the pin's `to` is refused at attach by name. So rehearse off the live path instead: `uc2-diffreplay upgrade` over a captured corpus, and `uc2-diffreplay pin-verify` against a throwaway node to prove the pin steers the new binary and turns the old one away.
- **Mixed-version tolerance.** State it explicitly, and for a replicated FSM the answer today is *none*: stop every instance of the row. The exceptions are the non-replicated surfaces — `Query`/`QueryResponse` and response schemas — which roll on their own.
- **Point-of-no-return plan.** Not "redeploy the old binary": that does not roll back the log, and state already computed by the new version is the new version's. Document the last moment abandonment is possible — the pin commits ([S8](#s8-decide-the-point-of-no-return)) — and what it costs after that: restoring the off-node backup on every node, discarding every write acked since.
- **Upgrade playbook** (required for any snapshot/schema change):
  1. The per-row order, and which rows move in which window
  2. Expected behaviour of the cluster during the window (commit pauses while the row's service is down on a quorum)
  3. Verification steps post-upgrade (`uc2ctl upgrade show`'s verdict, `uc2_snapshot_hash_mismatch`, every instance reporting the new version)
  4. Abandonment trigger conditions, and the restore procedure after the point of no return

### The upgrade lifecycle, per row

Nine stages, **each scoped to one row**. A whole-deployment upgrade is N per-row upgrades that happen to share an origin; nothing requires them to. Below is what each stage is for and what refuses you if you skip it; [Upgrade an application](../how-to/upgrade-an-application.md) is the operator's command-by-command procedure.

#### S1: Classify the change

Classify against [the change taxonomy](#the-change-taxonomy). The output is the set of obligations this change incurs — which shims S3 owes, whether the change is downgrade-safe, and which axis stays open afterwards. Everything below keys off it, so do it first and write it down.

#### S2: Declare the version

Bump `const VERSION` through `identity::pack_version(major, minor, patch)`. Major = any taxonomy row whose axis-P risk is severe; minor = additive but inert, and the harness must confirm the inertness; patch = no replicated behaviour change at all. What the declaration buys today: a per-row snapshot-session refusal on nonzero-vs-nonzero inequality, the `uc2_service_version` metric with its `Uc2ServiceVersionDrift` alert, the row's cnc version word, and — since the pin — a durable record of when the version changed plus S4's attach refusal. `0` is the "unversioned" sentinel: it is the trait's default, it never mismatches anything, `uc2ctl status` prints it as `unversioned`, and `uc2ctl upgrade pin` refuses to read a `--from` off it. **`NAME` is never bumped**: it is the identity hash *and* the `fold32` input to `IdGen`, so changing it is not a version change — it is a different FSM with a different id stream.

#### S3: Write the compatibility shims

Three shims, and they are not interchangeable:

- **Forward decode** (old binary, new command) — *cannot be written retroactively*. It has to have been in the old version already, which is why a version tag ahead of the payload has to exist from day one.
- **Backward apply** (new binary, old command) — bounded by [S9](#s9-close-axis-h), not permanent, but until S9 it must be exact: the old command's *old* semantics, not the new ones.
- **Snapshot dual-read** (new binary, old image) — what `examples/kv` hand-rolled as `IMAGE_VERSION_V1`/`V2`. Without it the new binary fail-stops on the very artifact S4 pins it to.

#### S4: Pin the origin

The stage that makes the flag day correct: explicit pin, unconditional install, attach refusal. Before stopping any instance of the row:

1. `uc2ctl snapshot` — one coordinated instant, giving a complete set at P on every node. It refuses (48, `snapshot_unsupported`) a row started with plain `start()`; an upgrade requires snapshot capability, which is the existing constraint made visible.
2. `uc2ctl upgrade pin --row <R> --to <MAJOR.MINOR.PATCH> --origin <P> [--from <MAJOR.MINOR.PATCH>]` — appends the `UpgradePin` record. Refused by name if the row is undeclared (52), `--from` is not the row's current version (53), `--origin` is not this node's newest complete set (54), or the origin is not above the row's current pin (55).
3. The `uc2-cluster` agent applies the record at commit and publishes the row's four pin words under their seqlock.
4. Stop every instance of the row, swap the binary, start. At attach — before it publishes anything to its slot — the service reads the pin, and when the pin's `to` equals its own `VERSION` it **installs `snap-<P>` unconditionally**, overriding both the reconstruction gap guard and its own `last_applied()`. A durable state machine sitting at `X > P` is rewound to P and recomputes `(P, X]` under the new version, which is exactly what its fresh peers do.
5. Four refusals guard that attach, all named: `PinnedVersionMismatch` (a binary that is not the pin's `to`: a stale binary cannot rejoin after the pin), `PinUnreadable` (the pin words did not read consistently through the seqlock — it fails **closed**, because treating an unreadable pin as "no pin" would skip an install the cluster requires), `PinRequiresSnapshots` (a pinned row started with `start()` has no install closure, and replaying the origin's prefix under the new binary is the counterfactual), and `PinnedArtifactMissing` (the origin's artifact is not on this node — take `uc2ctl snapshot fetch`, or re-pin at a retained instant).

Three facts to plan around. **The artifact carries its builder's version**: every artifact begins with a 24-byte `ULTSNAP2 ‖ P ‖ version` envelope, an unpinned install requires that version to equal the running binary's, and a pinned install requires it to equal the pin's `from` — the one sanctioned crossing of a version boundary, and only at the origin: a pinned row's reconstruction prefers the artifact at the **origin** over any newer one the old version left behind while you were pinning, and expects `from` for that one artifact alone; every other artifact is still held to the same-version rule. The pre-2.13.0 16-byte `ULTSNAP1` envelope is refused by name, so moving to 2.13.0 means clearing each row's `snapshots/<row>/` once and letting the next instant rebuild it. **A pinned origin's set is exempt from retention** on every node, from the moment the pin commits until a newer pin for that row supersedes it, so the artifact is still there when the new binary attaches; a node also holds its snapshot/purge floor at the origin until the row is consumed there, which means a pin placed and then abandoned holds the journal at that origin indefinitely — there is no bound or alert on that hold. And **the pin only moves forward**: there is no unpin, and a lower origin is refused (55), which is why [S8](#s8-decide-the-point-of-no-return) is where abandonment stops being free.

One documented limit, not a defect: between the pin and the stop the old version keeps applying, and its leader's `on_committed` emits side effects for `(P, X]`. After the rewind the new version recomputes that span, but the durable, increase-only output progress marker stops it re-emitting — so the outside world saw the old version's effects for a span whose state is now the new version's. Stop promptly after pinning and the window is seconds.

#### S5: Diff replay

Run the change against a corpus before the flag day, as a procedure: **declare** the expected delta per surface (with the change, before the run), **run** `uc2-diffreplay upgrade` with both builds from the artifact at P, **diff** every captured surface, **attribute** each entry to a hunk, and **confirm** against the declaration. The gate is that no entry is unexplained, undeclared, or declared-but-absent. A failure there is a finding, not a verdict on the change: an undeclared diff may be a bug or a forgotten line in the declaration, and telling those apart is the attribution step's job. Then rehearse the pin itself — [Diff replay § Verify the pin live](../how-to/diff-replay.md#5-verify-the-pin-live-reconstruction-mode-part-2) runs `uc2-diffreplay pin-verify`, which places a real pin on a throwaway node and checks that the old binary is turned away by name and the new one installs the origin.

#### S6: Roll out

Stop every instance of row `R`, swap the binary, start — [Upgrade an application](../how-to/upgrade-an-application.md) is the procedure. Budget the cost to the *other* rows: under bounded or lockstep lag, stopping one row's service on a quorum of hosts stalls commit until it is back, so a per-row upgrade is not invisible to its neighbours. The coordinated instant in S4 step 1 also freezes every row, not just `R` — harmless, and useful, because P is then a complete set cluster-wide regardless of which row is moving.

#### S7: Confirm

Every instance of the row reports the new version (`uc2ctl status`, per row: `version=`, `upgrade_origin=`, `pinned=`, `pinned_from=`), and every replica agrees on the state. Cross-replica agreement is an image-digest comparison, and it is valid here precisely because all replicas are now the same version. Read it two ways, and know which is prompt: `uc2_snapshot_hash_mismatch{service,row}` on `/metrics` is recomputed at scrape time from the committed cluster view, so it reflects an instant as soon as its record commits; `uc2ctl upgrade show`'s verdict line reads this node's newest cluster **artifact** and is therefore always at least one instant behind, by construction.

#### S8: Decide the point of no return

Rollback is largely a fiction in an SMR system: swapping the binary back does not roll back the log, and once the new version's semantics have been applied to committed frames, that state is the new version's. So decide in advance the last moment abandonment is possible — **it is the moment the pin commits**. After that the pin is committed and monotone, there is no unpin, the old binary is refused at attach by name, and the only way back is the pre-upgrade **off-node backup** restored on every node, discarding every write acked since. Take that backup before you pin, and keep it off the node: the new version rewrites the row's artifacts in place as it snapshots.

#### S9: Close axis H

The old command arm may be deleted when two things hold: a **pinned origin sits above the last occurrence** of the old command shape in the log, and **the oldest artifact any node could still reconstruct that row from is at or above that origin**. Then no binary will ever decode those bytes again — the artifact at the origin already carries their effect — and the arm can go in the *next* version, not this one. Both quantities are knowable but neither is exposed as a single fleet-wide reading: check each node's `snapshots/<row>/` listing and its purge floor, and remember that a pinned origin's set is retained on purpose. Until you have checked, keep the arm — an axis-H failure is silent, and the node that trips it is one you are not watching.

---

## 6. Observability Requirements

Before a feature is considered production-ready, it must expose:
- Apply latency (per state machine operation)
- Log replication lag
- Snapshot duration and size
- Leader election frequency
- Durability level in effect (quorum vs. per-node fsync)
- Feature-specific invariant violations, if detectable at runtime (defensive assertions that log rather than crash, where safe)

Every app also documents a short runbook: what does this feature do, and what should an operator expect to see, during a leader change or network partition?

---

## Summary checklist (per feature)

- [ ] Design note: determinism boundary, state shape, failure semantics, invariants
- [ ] Determinism rules followed (no time/randomness/hash-order/I-O in hot path)
- [ ] Change classified against the taxonomy, and the axis it opens named
- [ ] Schema/snapshot versioning plan
- [ ] `const VERSION` bumped, with the digit chosen from the classification
- [ ] The three shims written (forward decode, backward apply, snapshot dual-read)
- [ ] Unit + property-based tests for stated invariants
- [ ] Golden replay test added; the corpus kept with its declaration
- [ ] Fault injection test passes
- [ ] Diff replay run and confirmed against the declaration; the pin rehearsed with `pin-verify`
- [ ] Origin pinned (`uc2ctl snapshot` → `uc2ctl upgrade pin`) before any instance of the row is stopped
- [ ] Off-node backup taken before the pin — it is the only way back afterwards
- [ ] Upgrade / point-of-no-return playbook written (if state format changes)
- [ ] Post-upgrade confirmed on `/metrics` (`uc2_snapshot_hash_mismatch`) and `uc2ctl upgrade show`
- [ ] Axis H closed on the next version, once a pinned origin sits above the old shape's last occurrence
- [ ] Observability metrics wired
- [ ] Runbook documented
