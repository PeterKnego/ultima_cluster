# UC v2 post-M7 follow-up wave — design

Date: 2026-07-14
Base: `main` @ 3d469c4 (v2.1.0, M7 fleet gate passed)
Branch: `uc2/post-m7-followups`
Source list: `.superpowers/sdd/progress.md` — the M7 POST-MERGE FOLLOW-UP LIST plus
the archive-panic open ticket (T10) and the triaged review minors.

## Goal

Burn the M7 known-debt ledger down to zero in one wave: root-cause the one open
correctness question (the archive `Replay::next` panic), land five small behavior
changes, and close the observability/test/doc minors. After this wave the ledger's
follow-up list is empty and the next milestone (Maelstrom/elle, leases, wire
crypto — user's pick) starts from a clean slate.

## Decisions already made (with the user, 2026-07-14)

1. **Scope: everything** — correctness + observability + minors (tiers A+B+C).
2. **MPSC underflow (pre-existing v2.0 bug, fixed on main in 8c1ae01): release
   note only.** No v2.0.1 tag, no backport branch. Nobody is deployed on v2.0.x
   and v2.1.0 is a pure superset.
3. **`DemoteVoter{self}`: refuse it.** Do not extend the step-down machinery.
   Operator recourse for "leader becomes learner": `RemoveNode{self}` (the proven
   `StepDownRemoved` path), then rejoin as a learner with a fresh id.
4. **Recovered self-tombstone at boot: refuse to start.** Construction returns an
   error rather than booting an idle zombie or a boot-halted process.
5. **Structure: one branch, three phases**, subagent-driven with a progress
   ledger (the M7 SDD pattern). One final whole-branch review, then merge.
6. **No fleet run.** Nothing changes protocol shape; the local proof stack gates
   the merge.

## Phase 1 — archive `Replay::next` panic: investigate, then harden

The one item with an unresolved correctness question. Seen exactly once, in the
failover capstone: an out-of-bounds slice in `Replay::next`
(`uc_log/src/archive.rs:511`, `payload_range` past block end), i.e. a recorded
block whose frame header disagreed with the block length. Pre-existing (predates
M7 — that journal had no config frames).

Known facts (verified on main):

- `Replay::next` (`archive.rs:479-514`) walks frames inside a journal block with
  **no** frame-vs-block consistency guard before slicing
  `self.block[payload_range]`. Its sibling header-walk `walk_block_terms` has
  exactly that guard (`archive.rs:281`: reject `length < HEADER_LEN` or
  `off + aligned > block.len()`), with a comment calling the condition
  "unreachable" for archived blocks.
- Upstream, the archiver builds blocks from `recordable_slice`
  (`buffer.rs:191-221`) — the **only** live-buffer reader that does not use the
  post-copy seqlock re-check the validated readers use
  (`read_frame_validated` / `read_run_validated`, `buffer.rs:256-266, 318-322`).
  Its safety argument (bytes in `[durable, append)` are committed and immutable
  until recorded, so plain length reads are safe) is plausible but unproven under
  the failure; the intra-block consistency invariant is a `debug_assert!` only
  (`buffer.rs:212`).

Work, in order:

1. **Repro attempt (bounded).** A targeted stress test around concurrent
   append + archive + deep-NAK replay, biased toward buffer wrap and
   archiver-lag conditions. Budget-bounded; if the race does not reproduce,
   the verdict says so honestly — no manufactured repro.
2. **Root-cause verdict.** Either confirm the `recordable_slice` seam (and fix
   it with the same validated-read discipline as the other readers), or identify
   the real mechanism. Other candidates to check: the journal
   read-before-fsync seam (the `uc_journal` append-readability contract,
   fix 1de711a), block-chaining edges at `meta`/`block_base`, and torn state
   *around* buffer wrap where `durable` semantics could momentarily disagree
   with the frame walk.
3. **Unconditional hardening (lands regardless of verdict).** `Replay::next`
   gets the `walk_block_terms`-style bounds guard and returns a structured,
   diagnosable error (block seq, base position, offset, claimed length) instead
   of an unlabeled OOB panic. Replay of a corrupt block must degrade into an
   error the caller can adjudicate (wipe-and-rejoin is the existing recourse),
   never a process abort without context. Promote the `recordable_slice`
   `debug_assert` to a checked error on the same principle.

Exit criteria: verdict written down (confirmed root cause, or documented
non-repro with the hardening in place), regression/stress test committed, no
panic path left from a malformed block to an unlabeled abort.

## Phase 2 — behavior changes (five items)

### 2.1 Refuse `DemoteVoter{self}` (+ dedicated malformed-op reason code)

Today a leader demoting its own id is a legal, accepted op
(`ClusterConfig::apply`, `uc_consensus/src/config.rs:103-111`) and the leader
then leads-as-learner indefinitely — `StepDownRemoved` only covers
`RemoveNode{self}` (`election.rs:1350-1356`).

- New `ProposeError` variant (e.g. `SelfDemote`), guard in `propose_config`
  (`election.rs:821-839` — the only validation site that knows `self.id`).
  `ClusterConfig::apply` stays pure and id-blind.
- Two new wire reason codes (both currently unused; `uc2ctl` maps >10 to
  "unknown/malformed"): **11 = malformed/unknown wire op** (re-targets the
  fallback that today deliberately reuses 6/NotFound at
  `uc_node/src/node.rs:2148`; ledger minor (m)), **12 = self-demote refused**.
- `uc2ctl` `reason_str` arms (`examples/uc2ctl.rs:139-152`); new rows in the
  refusal-matrix test `every_refusal_surfaces`
  (`uc_node/tests/reconfig.rs:1271`); pure-mapping unit test updated
  (`config.rs:224`).
- Runbook: document the refusal and the recourse (remove self, rejoin as
  learner with a fresh id).

### 2.2 Refuse to start on recovered self-tombstone

Today a node restarting with its own id tombstoned in the recovered config boots
normally and idles as a zombie: `halt_removed` is seeded `false` at construction
(`node.rs:761`), and the runtime latch cannot re-fire because adoption is
version-gated (`election.rs:700-703`) — no higher-version `ConfigObserved`
arrives for an already-adopted removal.

- Construction-time check after boot config recovery resolves
  (`recover_config_record` / `rederive_config`, near the existing self-role
  computation at `node.rs:447-452`): if the operational config tombstones own
  id, construction returns an error naming the recourse — decommission, or wipe
  the instance dir and rejoin with a fresh id (fresh-forever ids: a tombstoned
  id can never rejoin).
- Integration test: remove a node, restart its process on the same instance
  dir, assert the construction error (not a hang, not a zombie).
- Audit existing tests/scenarios that restart a removed node and update their
  expectations.
- Runbook decommission section updated. The T8-documented truncation-revert
  edge (durable-but-uncommitted self-removal later truncated cluster-wide)
  previously recovered via restart; with this change its recourse becomes
  wipe-and-rejoin — document that explicitly.

### 2.3 `ConfigObserved` position≤durable belt

`adopt_config` trusts the event's `position` with only the version gate
(`election.rs:700-705`). On the **follower observation** path, followers adopt
at durable, so `position > durable` is a protocol violation worth a belt. The
leader's adopt-at-append path legitimately runs ahead of durable and is
untouched — the belt lives where follower-side observations are generated/fed,
not inside pure SM code that both roles share.

- Debug-assert + release-mode ignore-with-log (a belt, not new semantics).
- Sim: one scenario or unit test that a violating observation is ignored.

### 2.4 Wipe-fiat equal-version content hardening

The version gate (`election.rs:701`, `config.version <= self.config.version` →
return) silently ignores an equal-version config. A same-version,
different-content config (possible only under the wipe-fiat position-reset
fiat, or a bug elsewhere) is divergence that today goes undetected.

- On equal-version observation, compare content; on mismatch: debug-assert +
  release log/flag (surfaced via the existing violation/warn channel, exact
  mechanism picked in the plan). No behavior change for identical content.

### 2.5 Admin-band single-writer audit

Ledger item: "admin-band single-writer note or seqlock re-check". Audit all cnc
admin-band writers (the kind-16/17 request/reply band, offsets 3456-3648,
accessors in `uc_log/src/cnc.rs`). If the single-writer discipline holds,
document it at the accessor sites (load-bearing comment). If the audit finds a
real second writer, add the seqlock re-check instead. Evidence-first: the audit
decides which lands.

## Phase 3 — observability, minors, docs

### Observability

- **cnc publishes `admission_bytes`.** New field in the cnc reserved band,
  offsets pinned in BOTH `uc_protocol` and `uc_log` with offset-assertion
  tests (standing convention), written once at boot from `cfg.admission_bytes`.
  `uc2ctl status` reads it when nonzero and demotes `--admission-bytes` to an
  override (ledger minor (n)). One wire **minor** version bump for the whole
  wave (additive change).
- **Fiat install clears cnc `config_pending`.** The fiat adopt block in
  `maybe_adopt_incoming_snapshot` (`node.rs:1431-1452`) already writes
  `config_version` and the durable record; add `store_config_pending(false)` —
  a fiat install never has a pending change.

### Ledger minors (test/comment debt)

- (b) `decode_config` exact-`==8`-bytes boundary success test.
- (c) port-width test gains a raw-byte pin (roundtrip is width-blind).
- (f) `ElectionSm::new` sizing not can-vote-aware: footgun comment at the site.
- (g) `world.rs` `pending_violation` parked until next `step_once`: fix or
  document the drop hazard (caller that never steps again loses the violation).
- (k) T5-revert-then-rederive composition test (the pair is untested together).
- (q) Strengthen the committed-monotonicity assertion (assert the new leader's
  commit strictly exceeds the old leader's last, not merely non-regression).
- (r) SM-level pin test discriminating latch-vs-raw `contains()` (today only
  the 40s integration test discriminates).
- (x) `World::run_until` returns a timeout signal instead of silent `Ok(())`
  (`uc_sim/src/world.rs:619-629`; same pattern in `run_steps`). All callers
  updated; scenario tests that relied on silent timeout get explicit intent.
- (y) `config_ops_committed` counts local accepts, not durable commits:
  rename/re-doc to match reality.

### Docs

- Config record (`config.state`) added to the durable-state lists in CLAUDE.md
  and the runbook (the "doc enum nit").
- Crash-window operator UX note (ledger minor (p)): reply is never written if
  the leader crashes between append and reply; timeout + `uc2ctl status` is the
  correct recourse.
- **v2.0.x known-issue release note** for the MPSC free-space underflow
  (pre-existing in v2.0.0, fixed in v2.1.0 by 8c1ae01): blast radius is a
  debug-build panic or spurious backpressure on the clients→node ingress ring
  under producer contention — **not** data corruption; remedy is upgrading to
  v2.1.0. Location: a new `docs/releases.md` (v2.1.0 summary + v2.0.0
  known-issues entry) — the repo has no release-notes file yet; gate docs stay
  gate docs.

## Testing & merge gate

- Every behavior change lands with a discriminating test (red-verified where
  the M7 pattern applies).
- Merge gate: full local proof stack green — `cargo test` (49 suites), the
  sim-heavy fuzz arms, both lincheck capstones + partition capstone, the
  hard-crash tests, `cargo clippy --workspace --all-targets -- -D warnings`.
- No fleet run: no datagram/frame layout changes, no quorum-rule changes. The
  cnc field is reserved-band additive; the reason codes are new values in an
  existing field.

## Out of scope

- Maelstrom/elle, leader leases, wire crypto (next-milestone candidates).
- Any v2.0.1 tag or backport branch.
- Fleet re-run / gate-doc changes beyond the release-note file.
- Extending step-down to cover self-demote (explicitly decided against).
