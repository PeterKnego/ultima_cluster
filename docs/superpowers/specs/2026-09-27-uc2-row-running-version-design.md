# UC2 row running version — design (#33)

**Status:** design, approved in conversation 2026-09-27; this document is
the written spec for review. **Issue:** [#33] (P1). **Track:** "Track 2" of
the FSM upgrade lifecycle spec
(`2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §1.3, §9.2) — the safety
half only.

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
change of a row becomes an enforced, per-row flag day.

**Out, tracked elsewhere:**

- Rolling **application** upgrades — two versions of one row running on
  purpose, gated by a committed feature level — [#66]. This design is its
  floor: #66 relaxes "must equal" (§4.3) to "at or above the level", and
  nothing here forecloses that.
- Rolling upgrades of **UC itself** (node binary, wire, cnc) — [#31].
- Any change to the consensus kernel, `uc_sim`, or the Lean model: the
  running version is cluster-FSM state applied at commit, like pins.

## 3. Decisions (from the design conversation)

| # | decision | why |
|---|---|---|
| D1 | Scope is the safety floor, not rolling app upgrades | P1 bug; rolling is #66 and builds on this |
| D2 | Two ways to change a row's version: `uc2ctl upgrade adopt` and the existing pin | pin alone would lock every row without snapshots out of any upgrade |
| D3 | "Same version" = equal **major.minor**; patch may differ | the lifecycle spec defines patch as "no replicated behaviour"; patch releases can roll node by node |
| D4 | `VERSION = 0` is an ordinary version, equal only to 0 | otherwise an app that never set `VERSION` gets no protection |
| D5 | Per row: each FSM has its own running version | rows are independent FSMs; `audit` need not match `kv` |
| D6 | `adopt` is **monotone** (never lowers major.minor) | an older binary cannot honestly apply newer commands; going back is a pin or the off-node backup |
| D7 | A service defers to the cluster FSM's verdict on a version record; it never decides from the record bytes | the cluster FSM can refuse a record, and a refused record must change nothing |

### 3.1 `adopt` vs pin — the contract

- **`adopt` to V** promises: *V applies every command already in the log
  exactly as the previous version did.* It is for additive changes — new
  command variants appended, existing ones untouched (the lifecycle spec
  §2.4 row "New command variant (appended)"; exactly #33's case). A lagging
  or restarted replica may replay pre-adopt frames under V, and the promise
  is what makes that sound. `uc2-diffreplay upgrade` is how the promise is
  checked; the docs say so.
- **A pin** is required whenever V changes what an existing command does.
  Every replica reinstalls the same origin artifact, so no replica applies
  old frames under V. Pin requires snapshot capability (unchanged, refusal
  48).

## 4. Model

### 4.1 The running version

The cluster FSM (`uc_node/src/cluster_fsm.rs`, `ClusterState` at :81-132)
gains `running: [Option<u32>; CNC_MAX_SERVICES]` — per row, the packed
version (`uc_protocol::identity::pack_version`) the row runs, or `None`
before its first record. `None` and `Some(0)` are different: `Some(0)` is an
unversioned FSM that has been recorded (D4).

Three records set it, all applied at commit on every node:

| record | set by | effect | refused (on every node) unless |
|---|---|---|---|
| **genesis** — kind 6, `source = 1` | the leader's node, automatically (§6.1) | `running[row] = Some(to)` | `running[row]` is `None` → else **60 `version_already_set`** |
| **adopt** — kind 6, `source = 2` | operator, `uc2ctl upgrade adopt` (§7.1) | `running[row] = Some(to)` | `running[row]` is `Some` → else **61 `version_unset`**; `from == running[row]` → else **62 `adopt_from_mismatch`**; `major.minor(to) ≥ major.minor(from)` → else **63 `adopt_not_monotone`** |
| **pin** — kind 4 (existing) | operator, `uc2ctl upgrade pin` | existing pin effects, **plus** `running[row] = Some(to)` | existing pin rules (52–59) |

The state also records, per row, the frame-end position of the last
**accepted** version record (`running_record_pos`), for §5.2 and §6.4.

Refused records advance `applied` and change nothing, as every `CLUSTER`
kind does today (`cluster_fsm.rs:465-480`).

### 4.2 Version comparison

`fn same_line(a: u32, b: u32) -> bool` in `uc_protocol::identity`: equal
major and minor (`a >> 16 == b >> 16`), patch ignored (D3). `0` is compared
like any other value (D4). One helper, used by attach, the apply-loop arm,
the leader's door checks, metrics and `uc2ctl`.

### 4.3 The guarantee

For every row and every accepted version record R for that row (genesis,
adopt or pin, at position `p_R`, setting version `v_R`): **no frame after
`p_R` is applied by a service whose `VERSION` is not `same_line` with
`v_R`** — until the next accepted record for the row. Such a service is
refused at attach (§7.1) or stops at exactly `p_R` (§7.2).

Frames *before* `p_R` may be applied by a later version only in the two
sanctioned ways: under an `adopt`, by its promise (§3.1); under a pin, from
the origin artifact every replica reinstalls. #66 later weakens the
`same_line` predicate above, and nothing else.

## 5. Wire, cnc, image

### 5.1 Wire `0.9.0` → `0.10.0` — flag day

`ClusterKind::RowVersion = 6` (`uc_protocol/src/v2/frame.rs:72-93`),
payload 12 bytes, `uc_protocol::v2::upgrade`:

```
row u8 @0 ‖ source u8 @1 ‖ reserved [u8; 2] @2 ‖ from u32 @4 ‖ to u32 @8
```

- `source`: `1 = genesis`, `2 = adopt`; anything else is undecodable.
- Genesis writes `from = 0` and the decoder requires it (reserved-style).
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
   ATTACHED with a non-stale heartbeat: append kind 6 `source = 1`,
   `to = status.version()`. One row per pass.
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

### 6.3 Leader door for `adopt` (admin op 11)

`apply_upgrade_adopt` in `handle_admin`. The payload is 9 bytes (`row`,
`from`, `to`), so it rides in the signed admin line's `id ‖ ip ‖ port`
fields directly — no staged file, unlike pin (§7.1). Order:

1. Not leader → retry (2). Single-in-flight → retry (2).
2. Row not declared → **52** (existing code; its name becomes
   `row_undeclared` in `reason_str` and the docs, number unchanged).
3. `validate_cluster_command` (the FSM rules of §4.1, 60–63) → refuse by code.
4. Append kind 6 `source = 2`; audit `upgrade_adopt`.

## 7. Service side (`uc_service`)

### 7.1 Attach

After the existing pin decision (`attach.rs:247-271`), read the row view
(one seqlock read, §5.2):

- `running` absent → proceed (a genesis record is coming; §7.2 adjudicates
  it).
- `running` present and `!same_line(S::VERSION, running)` → refuse
  **`ServiceError::VersionMismatch { name, row, running, mine }`**:
  "row `kv` runs 2.1.0; this binary is 2.0.3 — install 2.1.x, or move the
  row with `uc2ctl upgrade adopt`/`pin`".
- Remember `attach_record_pos = running_record_pos` in `ApplyState`. Every
  version record at or below it is already decided by this attach.

A pinned attach keeps its stricter existing check (`to == S::VERSION`
exactly, since the artifact install depends on it).

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

## 8. Operator surface

- **`uc2ctl upgrade adopt --row <name|id> --to <x.y.z> [--from <x.y.z>]`** —
  admin op 11 (`ADMIN_OP_UPGRADE_ADOPT`), signed, audited. `--from` defaults
  to the row's `running_version` off the local page; the command refuses
  locally if the row has none yet. Replies as `pin` does (0 accepted with
  the position, 1 refused with the reason, 2 retry).
- **`uc2ctl status`**: each row line gains `running=<ver>|none
  running_pos=<p>`.
- **`uc2ctl upgrade show`**: each row's running version and the record that
  set it (genesis / adopt / pin, position), from the committed artifact.
- **`reason_str`** + `docs/reference/uc2ctl.md`: 60–63, 52 renamed.
- **Metrics**: `uc2_row_running_version{row,service}` (packed, absent → not
  exported); alert `Uc2RowVersionMismatch` — an attached service whose
  version is not `same_line` with its row's running version (it can only
  last until the service stops, so a firing alert means a stuck stop). Plus
  `scripts/m10_alert_fire.sh` `RULE_BUILDERS` coverage for the new rule.
- **Log events**: `row_version_genesis_proposed`, `row_version_recorded`
  (cluster agent, on every accepted kind-6 or pin), `version_gate_waiting`,
  `version_superseded` (service).

## 9. Docs

- `docs/how-to/upgrade-an-application.md` (the existing per-row upgrade
  how-to): a new first section, "adopt or pin?" (§3.1), the `adopt`
  procedure beside the existing pin procedure, and `uc2-diffreplay upgrade`
  as the check for adopt's promise.
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
2. **Already-attached stop**: 1.0 services applying under load; `adopt 2.0`;
   each 1.0 service stops with `applied == record position` exactly; 2.0
   services attach and the row resumes. Linearizable history across the
   switch (`uc_lincheck`).
3. **Refused record**: an `adopt` with a stale `from` → refused (62), and
   the attached services keep applying (no stop).
4. **Genesis**: a fresh cluster admits no client frame until every declared
   row has a record; mixed bootstrap (leader 2.0, follower 1.0) records 2.0
   and refuses the follower by name.
5. **Unit**: FSM rules 60–63; `same_line` (patch free, 0 exact); codec
   golden bytes for kind 6; image v1/v2 → v3 migration (fixtures); the
   seqlock row view; the attach ordering fix.
6. **Fuzz**: `uc_node_cluster_artifact` and the cluster-command decode
   targets extended to kind 6 and image v3.
7. **Regression**: workspace tests, `lin_v2`, `lin_partition_v2`, the
   hard-crash suite, `pin_verify`, and the MSRV clippy gate.
8. **Apply hop**: `apply_bench` A/B for the new arm (smoke on a dev box;
   any bar is fleet-only), with the same-source rebuild control.

## 11. Risks and limits

- **Flag day**: wire `0.10.0` + cnc `3.4` + image v3 — stop every node
  before starting any node. #31 is what ends this class.
- **`adopt`'s promise is the operator's.** A false promise (V changes an
  existing command) diverges replicas that replay old frames under V. The
  platform cannot see semantics; `uc2-diffreplay upgrade` can. Pin is the
  safe path when in doubt.
- **Patch is trusted (D3).** An app that ships a replicated behaviour change
  as a patch bump defeats the check. Same mitigation.
- **Leader-only acks make the gate a hard wait**: a declared row whose
  leader-side service never attaches closes the cluster to clients, named in
  the log (§6.2).
- **Not addressed**: rolling app upgrades (#66), rolling UC upgrades (#31).

[#31]: https://github.com/PeterKnego/ultima_cluster/issues/31
[#33]: https://github.com/PeterKnego/ultima_cluster/issues/33
[#66]: https://github.com/PeterKnego/ultima_cluster/issues/66
