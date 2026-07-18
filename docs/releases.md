# ultima_cluster releases

## Unreleased (wire protocol 0.3.0)
Post-M7 follow-up hardening (no new externally-visible features). Wire protocol
bumped **0.2.0 → 0.3.0**, additive only:
- cnc-page `admission_bytes` field pinned at offset 3712.
- admin reply reason codes **11** (malformed/unknown op) and **12**
  (self-demote refused).

A 0.3.0 node accepts a 0.2.0 peer (same major, peer minor not newer — see
`cnc::version_compatible`, the live gate; `version::CURRENT`/`MIN_COMPATIBLE`
are documentation-only and enforce nothing).

Safety fixes in this line:
- **Commit advance was not clamped to the current term's NewTerm base — a
  Raft §5.4.2 / Figure-8 acked-write-loss window** (Finding #6b, lean
  leader-completeness effort; affects all prior v2 releases): the leader's
  commit ranking (`rank_leader`) advanced/stored/gossiped off the
  positions-only `CommitTracker` unconditionally — `new_term_pos` (the NewTerm
  no-op frame appended at every election) gated only linearizable reads,
  ingress admission, and M7 proposals (`serving`), never the commit store. At
  any failover inheriting an uncommitted tail, followers reconcile clean and
  their 20 ms AppendPosition floor reports the election base BEFORE the
  NewTerm frame is quorum-durable, so the leader could commit (and ack, apply,
  fire outputs for) an OLD-TERM-ONLY range; a divergent higher-lastTerm rival
  could then still win the next term with a commit-quorum member's grant
  (their data-stamped `last_term` had not yet reached the new term) and
  truncate the committed bytes cluster-wide. The loss continuation needs a
  rival's vote datagrams to beat the in-flight NewTerm byte to a voter — a
  real race under loss/NAK repair — but the unsafe commit itself fires in the
  normal post-reconcile path; never observed outside the directed
  reproductions (no production deployment exists — pre-release fix). Fixed:
  `rank_leader` now advances/stores/gossips ONLY once the ranked position
  covers `new_term_pos` (Raft §5.4.2: never commit a prior-term range by
  counting replicas; cost: commit stalls at most one NewTerm replication round
  per election, which the read path already paid via `serving`). Found by the
  Lean commit-certification model (46-step kernel-checked Figure-8
  countermodel), reproduced RED-first and pinned by the sim
  (`old_term_range_must_not_commit_before_new_term_quorum`, inv2 at the
  violating advance) plus a `uc2_consensus` unit pin
  (`commit_clamped_to_new_term_base_never_certifies_old_term_only_range`).
  Remedy: upgrade; no back-port is planned.
- **Boot-open intake gate could certify a phantom commit** (Finding #5, lean
  leader-completeness effort; affects all prior v2 releases): a voter that
  granted a term-T vote (persisted), held a divergent tail, and crashed before
  reconciling rebooted with the receiver intake gate OPEN — its 20 ms
  AppendPosition floor report (raw divergent durable, stamped term T) could
  reach the T-leader before the 100 ms idle term-map re-ship and be counted
  toward quorum commit over content the reporter does not hold (worst case:
  committed-acked write loss after a leader crash). Requires the 4-way
  conjunction divergent-tail voter + persisted vote above the data-stamped map
  + crash before reconcile + report-beats-gossip; never observed outside the
  directed reproduction. Fixed: the gate (and the reconcile latch) now boots
  CLOSED iff the recovered vote term exceeds the data-stamped term map's last
  term, reopening via the existing reconcile paths (cost: one extra reconcile
  round after such a reboot). Found by the Lean commit-certification model
  (machine-checked countermodel), reproduced and pinned by the sim's inv7
  phantom oracle (`rebooted_unreconciled_voter_must_not_certify_phantom_commit`,
  RED pre-fix → GREEN post-fix). Remedy: upgrade; no back-port is planned.

Loose-end hardening in this line:
- **Leader-as-learner wedge closed** (T1): a leader that adopts its own demote
  from the log now relinquishes leadership to a non-voting learner-follower once
  the demote commits (a commit-triggered step-down mirroring self-removal),
  instead of leading-as-a-learner until an operator intervened. Safety was never
  affected; this removes the silent liveness wedge.
- **Config observations delivered losslessly** (T5): a dropped config-frame
  observation could silently run stale membership until a restart; delivery is
  now lossless.

## v2.1.0 — 2026-07-14
M7 live single-server reconfiguration (promote/demote/add/remove under load,
no restarts, `uc2ctl` admin path, tombstone-based fresh-forever ids, leader
self-removal). 5-host fleet gate passed: worst transition dip 4.7% (<10%),
self-removal gap 3.22 s (<10 s), zero loss/divergence, snapshots+purge paired.
Wire protocol 0.2.0 (FRAME_TYPE_CONFIG=4, admin datagram kinds 16/17).

## v2.0.0 — known issues
- **MPSC ingress ring free-space underflow under producer contention**
  (clients→node ingress only): a stale `claim_pos` snapshot overtaken by the
  consumer could underflow the free-space computation — debug builds panic,
  release builds see spurious backpressure. **Not data corruption** (the CAS
  re-validates before any write). Fixed in v2.1.0 (8c1ae01, regression test
  98900fd). Remedy: upgrade to v2.1.0; no v2.0.1 is planned.
