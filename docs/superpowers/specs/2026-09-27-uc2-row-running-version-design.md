# UC2 row running version — design (#33)

**Status:** design, approved in conversation 2026-09-27; this document is
the written spec for review. **Revision 2 (2026-09-27):** `adopt` and the
genesis pin were dropped — every version change is a pin (§3.1); snapshots
become mandatory under [#67]. **Issue:** [#33] (P1). **Track:** "Track 2" of
the FSM upgrade lifecycle spec
(`2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §1.3, §9.2) — the safety
half only.

**Explainer:** `docs/notes/uc2-row-running-version-explained.md` carries
this spec's argument in plain language; the operator's rules are
`docs/how-to/upgrade-an-application.md` § The version rules.

#### Errata (as built)

Where the implementation diverged from the body below. Read these first; each
cites the commit that settled it.

- **R5 — the page-1 word at `4056` is `cluster_applied`, not
  `cluster_consumed`** (§5.2). The `uc2-cluster` agent writes it only after a
  batch that applied or installed something (and at boot, next bullet), with
  the frame-END position the FSM has applied `CLUSTER` frames up to — not
  the walk cursor on every pass. That is what §5.2's own false-sharing
  argument asked for, and a service waits on it only at a `CLUSTER` frame,
  which is exactly an applying batch. (`c63c9ef`, `a9af9cb`, `60dc03f`)
- **R6 — recovery publishes `cluster_applied` too.** `ClusterAgent::new`'s
  recovery-time publish also stores `cluster_applied` = the recovered
  `applied`. Without it the word starts at 0 after a node restart, and a
  service waiting at a refused record at or below the recovered position
  would wait for a write that no replay ever makes. (`60dc03f`)
- **R12 — journal replay DOES need the version stop** (§7.2 says "replay
  needs no arm"; that is wrong). `replay_into` runs on every `Overrun`,
  including the attach-time catch-up, and walks past `attach_record_pos`, so
  it stops at a superseding record exactly as the live loop does; a test
  fails without it. (`ccd8008`)
- **R13 — the wait is bounded; a verdict that is not ready yields
  `Pending`.** §7.2 step 2 waits for `cluster_consumed ≥ pos` with no bound.
  As built, the arm spins (1024) then yields (64) and, if the cluster agent
  has still not applied the record (or the view stayed contended), returns
  `Gate::Pending`: the live loop rewinds its cursor to the record's start,
  publishes `applied = pos` and ends the cycle, and the SM lock is not held
  across the wait; the replay path returns and rejoins at the record. So a
  dead cluster agent can never make `Service::stop` hang. (`e9282c2`)
- **R9 — attach takes ONE `row_view()` read** for the pin, the running
  version and `record_pos`, and derives the pin decision from it, rather than
  §7.1's pin read followed by a second row-view read (a TOCTOU: the two could
  come from different publishes). `RowRead::Contended` → the existing
  `ServiceError::PinUnreadable`; the separate `RowViewUnreadable` refusal
  never shipped. (`a8de0e2`)
- **Migration record position** (§5.3). A v1/v2 image gives each pinned row
  `record_pos` = the image's `applied`, which depends on the position the
  image was taken at, so replicas may hold **different** `record_pos` values
  for one row until its next pin. That is harmless — cluster artifacts
  already differ byte-wise across the flag day, and nothing compares
  `record_pos` across nodes or decides on it cluster-wide — and it has one
  visible effect: after a legacy install, a same-line service may take ONE
  spurious stop at an old pin record above its attach position. That is
  safe; the next attach covers the record and there is no stop loop.
  (`4042ef1`)
- **Records inside a span jumped by a mid-life snapshot install are never
  walked** by the version gate. That path relies on the envelope's
  `same_line` check at install instead: an artifact built on another line is
  refused. (`855a0aa`)
- **R8/R10 — test fixtures.** Under D3 raw small `VERSION` integers are all
  one line (`0.0.x`), which silently aliased the `uc_lincheck` register
  fixtures that exist to model different versions; they moved to distinct
  lines (`DoublingRegisterSm` `0.2.0`, `DoublingCasRegisterSm` `0.3.0`).
  Tests that used to observe an unpinned mixed-version stall now observe the
  earlier attach refusal (`VersionMismatch`), the stronger form of the same
  protection. (`a8de0e2`)
- **S4 of the lifecycle spec (the purge-floor hold for pins).** With the old
  pinned row now stopped at the pin record, no newer complete set can form
  before the new build attaches, so `hold_floor_for_pins` has no end-to-end
  scenario left; its three `pinned_attach` tests were reworked to wait for
  the old service's stop (R11). (`54caa65`)
- **R14 — the client gate reaches harnesses.** A harness that declares a row
  and submits with no leader-side service now waits forever at the gate
  (§6.2). The M10 alert-fire harness's real-cluster scenarios had to attach a
  service on every node before driving load. (`e23973e`)
- **R17 — pins name a line, not a build; SNAP_BEGIN compares lines;
  `Uc2RowVersionMismatch` requires a fresh heartbeat.** D3 ("patch is free")
  was false in three places as first built. (1) Pinned attach refused any
  `VERSION != to`; it now refuses `!same_line(to, VERSION)`, so a patch
  build of `to` takes the pinned install. The same rule reached the node's
  `hold_floor_for_pins` (a row attached at a patch of `to` consumes the pin),
  the door's no-running-version half of 53 (`same_line(from, attached)`),
  and `uc2-diffreplay pin-verify`'s two version arms (a same-LINE run is
  refused up front; NEW at a patch of `--to` is at the pinned line).
  `uc2ctl upgrade show`'s `by=pin` inference stays exact: it compares two
  recorded values, not a build. (2) The receiver's `SNAP_BEGIN` per-row
  version check is `!same_line(ours, theirs)`; `0` still means unknown.
  (3) A stopped old service keeps its ATTACHED bit until the new build
  re-attaches, so the alert paged on any swap slower than `for:`; it now
  also requires `uc_service_heartbeat_age_seconds < 10` on the row.
  (`72c80fb`, `732f896`, `02b0b6b`)
- **R18 — a version stop clears ATTACHED.** §7.2's fail-stop left the
  slot's ATTACHED bit set, as a crash does. A version stop is deliberate, so
  `version_gate::stop_at_record` (the one path the live loop and replay
  share) now clears it before the panic, as `Service::stop` does — but only
  that bit: the incarnation and `SNAPSHOT_CAPABLE` stay, so `uc2ctl
  snapshot` is not refused 48 on a row stopped at a pin (two `pinned_attach`
  tests take an instant in exactly that window). The row reads absent until the new build attaches.
  `Uc2ServiceAbsent` (30 s) and `Uc2ServiceWedged` (1 m, which reads the
  stalest declared row's heartbeat whether attached or not) still fire on a
  swap slower than their `for:`; that is an accurate report of a row that
  applies nothing, not a false page. (`967fe04`, `2e55714`)
- **Smaller as-built facts.** Refusal 52's `reason_str` is the bare
  `row_undeclared` (the explanation moved to `docs/reference/uc2ctl.md`), and
  genesis refuses only 60, not 52. §6.1's audit line is op `row_genesis`
  (audit-only op code 100) with `source="genesis"`. `upgrade show`'s running
  line reads `running=<v> set_at=<end> by=pin|genesis`, with `by` inferred
  from whether the newest pin's `to` equals the running version.
  `Uc2ServiceVersionDrift` was also moved to compare lines (major.minor),
  filtering the `0` sentinel before the floor. (`389e40d`, `2d38335`,
  `0af2f16`)

## 1. Problem

A row (one declared FSM, `[services] names`, index 0–7) can today be served
by two different builds of its state machine at once, and nothing notices:

- A row that was never pinned has **no committed version**. Any binary
  attaches (`uc_service/src/attach.rs:247-271` checks only a pin).
- A service that is **already attached** when a pin commits keeps applying
  past it: the apply loop ignores `CLUSTER` frames
  (`uc_service/src/apply.rs:564-647`).

[#33]'s repro: the leader's `kv` is 2.0, the followers' 1.0. A 2.0-only
command is appended, replicated and quorum-durable; the followers store it
but cannot apply it (`BAD_REQUEST unknown op`); the leader's 2.0 service
applies it and the client is acknowledged. After a leader change the write
is gone. The flag-day procedure prevents this only if every operator follows
it; the platform does not enforce it.

## 2. Scope

**In:** a row can never be applied by two versions at once. Every version
change of a row is a pin — an enforced, per-row flag day.

**Out, tracked elsewhere:**

- Rolling **application** upgrades — two versions of one row running on
  purpose, gated by a committed feature level — [#66]. This design is its
  floor: #66 relaxes "must equal" (§4.3) to "at or above the level", and
  nothing here forecloses that.
- Rolling upgrades of **UC itself** (node binary, wire, cnc) — [#31].
- Making snapshot support mandatory for every service — [#67]. This design
  only needs what is already true: a pin requires a snapshot-capable row
  (refusal 48).
- Any change to the consensus kernel, `uc_sim`, or the Lean model: the
  running version is cluster-FSM state applied at commit, like pins.

## 3. Decisions (from the design conversation)

| # | decision | why |
|---|---|---|
| D1 | Scope is the safety floor, not rolling app upgrades | P1 bug; rolling is #66 and builds on this |
| D2 | **Every version change is a pin**; there is no other way to change a row's version | a pin is the only change in which every replica starts the new version from one common state (§3.1) |
| D3 | "Same version" = equal **major.minor**; patch may differ | the lifecycle spec defines patch as "no replicated behaviour"; patch releases can roll node by node |
| D4 | `VERSION = 0` is an ordinary version, equal only to 0 | otherwise an app that never set `VERSION` gets no protection |
| D5 | Per row: each FSM has its own running version | rows are independent FSMs; `audit` need not match `kv` |
| D6 | Snapshots are mandatory ([#67]); no "genesis pin" for rows that cannot snapshot | without snapshots a long-running cluster cannot purge or rebuild an FSM in reasonable time (§3.1) |
| D7 | A service defers to the cluster FSM's verdict on a version record; it never decides from the record bytes | the cluster FSM can refuse a record, and a refused record must change nothing |

### 3.1 Why a pin, and only a pin

A service reconstructs its state at attach by one of three routes: replay
the journal from the start, install its newest local snapshot and replay the
tail, or — a durable state machine — continue from its own `last_applied()`
(`uc_service/src/attach.rs`, `start_pos = last_applied.unwrap_or(0)` when
unpinned). Every route except a pin can hand a new binary **state or
commands produced under the old version**. Whether that is sound depends on
the new version applying old commands exactly as the old one did *and*
reading the old version's saved state — a semantic promise the platform
cannot check.

A pin removes the question: every replica of the row installs **the same
origin artifact** and applies only frames after it under the new version.
The one residual requirement — the new version reads the artifact the old
version wrote (the lifecycle spec's "snapshot dual-read" shim) — is checked
at install by the artifact's envelope (`ULTSNAP2` carries the builder's
`VERSION`), not trusted.

**Rejected alternatives** (recorded so they are not re-proposed):

- **`uc2ctl upgrade adopt`** — commit "the row now runs V" with no origin
  artifact, the operator promising V treats every existing command and the
  old saved state exactly as before. Rejected: the promise is the hardest
  property to get right, unenforceable inside the cluster
  (`uc2-diffreplay upgrade` can only sample it), and a false promise
  re-creates exactly the divergence this spec exists to prevent.
- **A genesis pin** — pin at position 0, every replica rebuilding the row
  from an empty state by replaying the whole log under V; its only use was a
  row that cannot snapshot. Rejected with [#67]: at 10 % of the measured
  single-FSM ingest (1 362 555 commands/s, `uc2-m14-gate-2026-08-29.md:42`,
  ~128 B per command) a cluster writes ~1.5 TB of log per day, and the
  measured restart replay rate (~550 000 commands/s, same doc :415-422)
  needs ~7.5 days per month of history. A row without snapshots is not a
  deployment option, so it does not get an upgrade path of its own.

**Where the origin comes from.** A standby instant (`uc2ctl snapshot
--standby`) freezes only learners, and `uc2ctl snapshot fetch` brings the
set to the node that will accept the pin — FSM determinism is what makes
one learner's artifact valid for every replica. The docs recommend this as
the normal source, so voters never pause for an upgrade.

## 4. Model

### 4.1 The running version

The cluster FSM (`uc_node/src/cluster_fsm.rs`, `ClusterState` at :81-132)
gains `running: [Option<u32>; CNC_MAX_SERVICES]` — per row, the packed
version (`uc_protocol::identity::pack_version`) the row runs, or `None`
before its first record. `None` and `Some(0)` are different: `Some(0)` is an
unversioned FSM that has been recorded (D4).

Two records set it, both applied at commit on every node:

| record | set by | effect | refused (on every node) unless |
|---|---|---|---|
| **genesis** — new kind 6 `RowGenesis` | the leader's node, automatically, the first time a row has no running version (§6.1) | `running[row] = Some(version)` | `running[row]` is `None` → else **60 `version_already_set`** |
| **pin** — kind 4 (existing) | operator, `uc2ctl upgrade pin` | existing pin effects, **plus** `running[row] = Some(to)` | existing pin rules (52–59), **plus**: when `running[row]` is `Some(r)`, `same_line(from, r)` → else **53 `pin_from_mismatch`** (existing code, new clause) |

Genesis is the only record without an operator; it records a fact (the
version the leader already runs) and never changes one. After genesis, only
a pin moves the version — in either direction: a pin to an *older* version
is sound for the same reason as any pin (every replica reinstalls the
origin), so rollback is just another pin.

The state also records, per row, the frame-end position of the last
**accepted** version record (`running_record_pos`), for §5.2 and §7.

Refused records advance `applied` and change nothing, as every `CLUSTER`
kind does today (`cluster_fsm.rs:465-480`).

### 4.2 Version comparison

`fn same_line(a: u32, b: u32) -> bool` in `uc_protocol::identity`: equal
major and minor (`a >> 16 == b >> 16`), patch ignored (D3). `0` is compared
like any other value (D4). One helper, used by attach, the apply-loop arm,
the pin rule above, the pinned install (§7.3), metrics and `uc2ctl`.

D3 makes a promise about patch releases that covers **everything
replicated**, and that includes the snapshot payload format: a patch bump
must write and read artifacts the other patch builds of its line can read.
The docs state this with the version rules.

### 4.3 The guarantee

For every row and every accepted version record R for that row (genesis or
pin, at position `p_R`, setting version `v_R`): **no frame after
`p_R` is applied by a service whose `VERSION` is not `same_line` with
`v_R`** — until the next accepted record for the row. Such a service is
refused at attach (§7.1) or stops at exactly `p_R` (§7.2).

Frames *before* `p_R` are never applied under `v_R` at all when R is a pin:
every `v_R` service starts from the pin's origin artifact. #66 later weakens
the `same_line` predicate above, and nothing else.

## 5. Wire, cnc, image

### 5.1 Wire `0.9.0` → `0.10.0` — flag day

`ClusterKind::RowGenesis = 6` (`uc_protocol/src/v2/frame.rs:72-93`),
payload 8 bytes, `uc_protocol::v2::upgrade`:

```
row u8 @0 ‖ reserved [u8; 3] @1 ‖ version u32 @4
```

- Exact length, reserved bytes zero, `row < CNC_MAX_SERVICES`.
- `CURRENT = 0.10.0` (`uc_protocol/src/version.rs:84`).

A `0.9.0` peer reads kind 6 as undecodable (refused, `applied` advances —
`cluster_fsm.rs:465-473`), so its cluster FSM diverges silently: this is a
flag day, stop every node before starting any node, as `0.9.0` was.

### 5.2 cnc `3.3` → `3.4` — free words only, no layout move

| word | offset | writer | meaning |
|---|---|---|---|
| `running_version` | service slot status line **+48** (today `_pad[0]`, `uc_log/src/cnc.rs:188`) | cluster agent | bit 32 = present; bits 0–31 = packed version. `0` = absent |
| `running_record_pos` | status line **+56** (today `_pad[1]`) | cluster agent | frame-end position of the last accepted version record for this row; `0` = none |
| `cluster_consumed` | page 1 **4056** (free word of the 4032 line) | cluster agent | the cluster agent's walk cursor (`view.consumed`) |

- `running_version` and `running_record_pos` are published under the
  **existing pin seqlock** (`pin_seq`, `uc_log/src/cnc.rs:259-293`):
  `store_pin` becomes `store_row_view(pin, running, record_pos)`, and the
  reader returns all of it from one consistent read (`PinRead` gains the two
  fields). One seqlock, one writer, one read.
- `cluster_consumed` is written after `publish_view`, in the same place
  `note_consumed` runs today (`cluster_agent.rs:551-568`), so a reader that
  sees `cluster_consumed ≥ p` also sees every row word as of `p`. The 4032
  line gains a second live writer (the archive agent writes 4048 every
  frame); the cluster agent writes 4056 only after a `CLUSTER` batch, so the
  added false sharing is rare — noted, not measured to matter.
- Offsets pinned in both `uc_protocol::v2::cnc` and `uc_log::cnc` with
  offset asserts, as always. Every new word reads `0` as absent.

### 5.3 Cluster image v2 → v3

`uc_protocol/src/v2/cluster_image.rs` gains a length-prefixed `running`
blob: `CNC_MAX_SERVICES × (present u8 ‖ reserved [u8; 3] ‖ version u32 ‖
record_pos u64)`. Decode accepts v1, v2 and v3 (the plan-B1 precedent,
`cluster_image.rs:172`):

- **v3:** read as written.
- **v1/v2:** for each row with pin history, `running = Some(last pin's to)`
  and `record_pos` = the image's `applied` (a pin stores its origin, not
  its own record position; `applied` is at or above it, which is all §7.1
  needs — every record at or below it counts as decided at attach); rows without pins → `None`, which the leader then fills with a
  genesis record (§6.1). This is the migration path for an existing cluster
  crossing the flag day.

## 6. Node side

### 6.1 Genesis append

`maybe_append_row_genesis`, `#[inline(never)]`, in the consensus `do_work`
beside `maybe_commit_datagram_mtu` (`uc_node/src/node.rs:4096-4105`), same
gating (`serving && !hold_clients`), same shape (`node.rs:6803-6847`):

1. Return at once unless some declared row has `running == None` (one branch
   on a cached flag in steady state).
2. Single-in-flight: `last_cluster_append > cluster_view.position` → skip.
3. For the lowest such row whose slot on **this** (the leader's) page reads
   ATTACHED with a non-stale heartbeat: append kind 6 `RowGenesis`,
   `version = status.version()`. One row per pass.
4. `obs_event` `row_version_genesis_proposed` and an audit line
   `actor="node"`, `source="genesis"` (the `audit_datagram_mtu` precedent,
   `node.rs:6868-6898`).

Attach today stores ATTACHED **before** the version word
(`uc_service/src/attach.rs:441-445`), so the leader could read a stale
version beside a fresh ATTACHED bit. **Reorder:** store the version first,
then the status word with ATTACHED (Release), and read them in the opposite
order (Acquire).

### 6.2 Client gate

In `drain_ingress_ring` (`node.rs:7810-7825`), beside the two admission
terms: while any declared row has `running == None`, stop draining — the
records stay in the ring (clients see backpressure, not an error). Not
folded into `serving`/`can_serve`: that would also block the genesis append
itself, flip `NODE_FLAG_CAN_SERVE`, and redirect clients with `NOT_LEADER`.
The in-process `drain_ingress` (`node.rs:5926-5949`) gets the same clause.

While the gate holds, `version_gate_waiting` is logged (throttled, like
`note_declared_withheld`, `node.rs:5538-5560`), naming the row and whether
the leader's own service for it is attached. A row whose leader-side service
never attaches keeps the cluster closed to clients — correct, since only the
leader's service acknowledges writes — and the log says which row.

### 6.3 Pin door

`apply_upgrade_pin` (`node.rs:9001-9060`) is unchanged except that
`validate_cluster_command` now includes the new `same_line(from, running)`
clause (§4.1), so a pin whose `from` names a line the row is not running is
refused 53 at the door as well as at apply. The row-undeclared code 52 is
renamed `row_undeclared` in `reason_str` and the docs (number unchanged),
since genesis uses the same check.

## 7. Service side (`uc_service`)

### 7.1 Attach

After the existing pin decision (`attach.rs:247-271`), read the row view
(one seqlock read, §5.2):

- `running` absent → proceed (a genesis record is coming; §7.2 adjudicates
  it).
- `running` present and `!same_line(S::VERSION, running)` → refuse
  **`ServiceError::VersionMismatch { name, row, running, mine }`**:
  "row `kv` runs 2.1.0; this binary is 2.0.3 — install 2.1.x, or move the
  row to this version with `uc2ctl upgrade pin`".
- Remember `attach_record_pos = running_record_pos` in `ApplyState`. Every
  version record at or below it is already decided by this attach.

A pinned attach keeps its existing check (`to == S::VERSION` exactly, since
the pin names one build).

### 7.2 The apply-loop arm

`apply_cycle`'s dispatch (`uc_service/src/apply.rs:564-647`) gains one arm,
the type test plus an out-of-line call (the M14a rule: nothing inline but
the test):

```
else if hdr.frame_type == FRAME_TYPE_CLUSTER { on_cluster_frame(...) }
```

`on_cluster_frame`, `#[inline(never)]`:

1. Peek the kind and the row byte. Not kind 4/6, or not this row, or
   `pos ≤ attach_record_pos` → return.
2. Wait until `cluster_consumed ≥ pos` (the cluster agent applies at commit
   and this frame is already committed, so the wait is short; spin-then-yield,
   never a sleep on a live peer — the M14a rule).
3. Read the row view:
   - `running_record_pos < pos` → the record was **refused** → continue.
   - `running_record_pos == pos` and `same_line(S::VERSION, running)` →
     accepted and still ours → continue.
   - otherwise → **stop at `pos`**: publish `applied = pos` (every frame
     before it is applied, nothing after), log `version_superseded`, and
     fail-stop with the named panic "row `kv` moved to 2.1.0 at position P;
     this binary (2.0.3) stopped there — restart it as 2.1.x" (the existing
     fail-stop idiom, `apply.rs:737-746`).

The stop is safe to restart from: the next attach reads a
`running_record_pos ≥ pos`, so the record is behind it — no stop loop.

`replay.rs` (journal replay at attach) needs **no** arm: every record it
walks is at or below `attach_record_pos`, already decided by the attach.

### 7.3 Pinned install across patch builds

Today the pinned install requires the artifact's envelope version to equal
the pin's `from` **exactly** (`ServiceError::MistaggedSnapshot`,
`uc_service/src/config.rs:92-114`). Under D3 the origin artifacts on
different nodes may have been built by different patch builds of `from`'s
line (1.0.1 on one learner, 1.0.3 on another). The check becomes
`same_line(envelope_version, from)` — consistent with D3 and with §4.2's
statement that patch builds share the artifact format. The unpinned
envelope check (`== S::VERSION`) relaxes the same way.

## 8. Operator surface

- **No new admin op.** `uc2ctl upgrade pin`'s `--from` default becomes the
  row's `running_version` off the local page when present (today it is the
  attached service's version, `uc_ctl/src/upgrade.rs:83-95`).
- **`uc2ctl status`**: each row line gains `running=<ver>|none
  running_pos=<p>`.
- **`uc2ctl upgrade show`**: each row's running version and the record that
  set it (genesis or pin, position), from the committed artifact.
- **`reason_str`** + `docs/reference/uc2ctl.md`: 60; 52 renamed; 53's new
  clause.
- **Metrics**: `uc2_row_running_version{row,service}` (packed, absent → not
  exported); alert `Uc2RowVersionMismatch` — an attached service whose
  version is not `same_line` with its row's running version (it can only
  last until the service stops, so a firing alert means a stuck stop). Plus
  `scripts/m10_alert_fire.sh` `RULE_BUILDERS` coverage for the new rule.
- **Log events**: `row_version_genesis_proposed`, `row_version_recorded`
  (cluster agent, on every accepted genesis or pin), `version_gate_waiting`,
  `version_superseded` (service).

## 9. Docs

- `docs/how-to/upgrade-an-application.md` (the existing per-row upgrade
  how-to): the version rules (major.minor must match, patch free and what
  "patch" must not change, `0` is a version), what a refused or stopped
  service's message means, and the standby instant on a learner as the
  recommended origin.
- References: `uc2ctl.md`, `cnc-page.md` (3.4 words), `wire-protocol.md`
  (kind 6, 0.10.0), `semver-policy.md`.
- `docs/how-to/upgrade-a-cluster.md`: the `0.10.0` / cnc `3.4` flag day.
- Explainer `docs/notes/` entry: why two versions of one FSM can never both
  apply, in plain language.
- Update the lifecycle spec's §9.2 with a pointer here; `RELEASES.md` +
  `docs/releases.md` at the cut.

## 10. Proof

1. **The #33 repro, end to end** (multi-process, `testing/uc_crashtest`
   shape): three nodes, the leader's `kv` service at 2.0, the followers' at
   1.0, a 2.0-only command, then a leader change. **Before the fix it must
   fail** — an acknowledged write is gone — and the failure is recorded.
   After: the 1.0 services are refused by name at attach, and no
   acknowledged write is lost.
2. **Already-attached stop**: 1.0 services applying under load; a pin to
   2.0 commits; each 1.0 service stops with `applied == record position`
   exactly — closing the lifecycle spec's §9.2 window; 2.0 services attach,
   install the origin and the row resumes. Linearizable history across the
   switch (`uc_lincheck`).
3. **Refused record**: a pin whose `from` is not the running line → refused
   (53) at apply, and the attached services keep applying (no stop). The
   test forces the record past the leader's door to exercise the apply-side
   refusal.
4. **Genesis**: a fresh cluster admits no client frame until every declared
   row has a record; mixed bootstrap (leader 2.0, follower 1.0) records 2.0
   and refuses the follower by name.
5. **Unit**: rule 60 and 53's new clause; `same_line` (patch free, 0 exact);
   codec golden bytes for kind 6; image v1/v2 → v3 migration (fixtures); the
   seqlock row view; the attach ordering fix; the relaxed envelope check.
6. **Fuzz**: `uc_node_cluster_artifact` and the cluster-command decode
   targets extended to kind 6 and image v3.
7. **Regression**: workspace tests, `lin_v2`, `lin_partition_v2`, the
   hard-crash suite, `pin_verify`, and the MSRV clippy gate.
8. **Apply hop**: `apply_bench` A/B for the new arm (smoke on a dev box;
   any bar is fleet-only), with the same-source rebuild control.

## 11. Risks and limits

- **Flag day**: wire `0.10.0` + cnc `3.4` + image v3 — stop every node
  before starting any node. #31 is what ends this class.
- **Every upgrade needs snapshots.** A row that cannot snapshot cannot
  change version at all once this lands; [#67] makes that the only kind of
  row there is. Until #67 ships, such a row's docs say so plainly.
- **Patch is trusted (D3).** An app that ships a replicated behaviour change
  — or a snapshot format change — as a patch bump defeats the check.
  `uc2-diffreplay upgrade` is how to catch it before release.
- **Leader-only acks make the gate a hard wait**: a declared row whose
  leader-side service never attaches closes the cluster to clients, named in
  the log (§6.2).
- **Not addressed**: rolling app upgrades (#66), rolling UC upgrades (#31),
  mandatory snapshots (#67).

[#31]: https://github.com/PeterKnego/ultima_cluster/issues/31
[#33]: https://github.com/PeterKnego/ultima_cluster/issues/33
[#66]: https://github.com/PeterKnego/ultima_cluster/issues/66
[#67]: https://github.com/PeterKnego/ultima_cluster/issues/67
