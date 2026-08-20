# ultima_cluster releases

## v2.4.0 — 2026-08-20 — M10 observable cluster

**A running cluster can now be watched, probed, and alerted on without
touching the source.** Metrics, structured logs, health probes, and shipped
alert rules — the whole layer reads state the hot path already publishes, and
the fleet gate measured its cost at ~1.7% under a 1s all-nodes scrape.

- **An in-daemon observability endpoint** (`[metrics]` config section, off
  when absent): `GET /metrics` (Prometheus text, 62 metric families —
  commit/apply/replication lag, admission saturation, heartbeat ages, per-peer
  lag on the leader, and every repair/drop/crypto counter), `/healthz`
  (liveness: the four agents alive + node heartbeat fresh), `/readyz`
  (role-aware readiness). Hand-rolled over `std::net`; zero new dependencies;
  the exporter reads the same atomics the agents publish — no lock the hot
  path can contend on.
- **Readiness keys on `can_serve`, never the leader flag.** The elected-but-
  not-serving `0x01` window is exactly what a naive `leader == true` probe
  gets wrong; the fleet gate killed leaders three times and never observed a
  ready response from a node in that state.
- **Transition-triggered structured logging** (`[log]` section): one JSON
  line per election, truncation, snapshot install, config adoption, removal,
  NAK storm, seal-failure burst, snapshot publication — never one per
  operation. The daemon now also **fails fast when an agent fail-stops**
  (exit 1 for systemd to restart) instead of lingering as a healthy-looking
  zombie.
- **Shipped ops artifacts**: `packaging/prometheus/uc2-alerts.yml` (13 rules,
  every one proven to fire against a deliberately broken cluster via
  promtool; the per-peer rules are leader-scoped — the peer band is
  leader-authoritative and followers export zeros), a Grafana dashboard, and
  `docs/how-to/monitor-a-cluster.md`.
- **Fleet gate** (`docs/benchmarks/uc2-m10-gate-2026-08-20.md`): a 10-minute
  healthy soak under a real Prometheus fired zero alerts with full series
  coverage from every node; the scrape-perturbation A/B held at median 0.9830
  against a pre-committed >= 0.95 bar; wire-0.5.0 hygiene held
  (`reports_unattested` 0 everywhere). Runs 1-2 were honest failures —
  harness defects, recorded in the gate doc, including one operational
  finding worth knowing: the journal holds an fd per segment, so keep the
  packaged unit's `LimitNOFILE` and enable purge for long-lived clusters.

No wire, cnc-page, or consensus changes. `[log]`/`[metrics]`, reserved in
v2.3.0, now have their schema — unknown keys inside them refuse at boot like
everywhere else.


## v2.3.0 — 2026-08-19 — M9 deployable node

**UC is now deployable by someone who is not the author.** Before this tag the
only way to start a node was an example binary configured in Rust source; the
docs described a daemon the build did not produce. M9 ships it.

- **A real `uc2-node` daemon.** Starts from a TOML config file
  (`packaging/node.example.toml` is the shipped reference;
  `docs/reference/configuration.md` documents every field). The file is a
  one-to-one mirror of `NodeConfig` with `deny_unknown_fields` — a typo is a
  startup refusal naming the key, not a silently-ignored setting. `[log]` and
  `[metrics]` are reserved for M10: parsed, announced as inert on every boot,
  never silently swallowed. `seed` defaults to a distinct per-id derivation so
  operators cannot livelock a cluster through identical election timers.
- **Named startup refusals.** Every rule that used to fail later and look like
  something else now refuses at boot with the offending field named: `bind`
  must equal this node's own members entry (the mismatch that elects a leader
  whose followers never commit); `max_payload` must fit one datagram against
  the MTU (the assert that used to panic inside the sender); `buffer_bytes`
  power-of-two; membership disjointness/uniqueness/8-cap; election window
  ordering; and an instance_dir on a RAM-backed filesystem is refused **by
  name** — every fsync there is a silent no-op. The tmpfs override
  (`allow_volatile_fs` / `UC2_ALLOW_VOLATILE_FS`) is never silent: the node
  warns on every boot it is active.
- **Clean lifecycle.** `SIGTERM` → bounded archive drain → exit 0, so a planned
  restart rejoins from the journal instead of paying reconstruction. Packaged
  systemd units: `TimeoutStopSec=10` (room for the drain),
  `RestartPreventExitStatus=2` (a config refusal is not retried into a restart
  loop), and a `BindsTo=` service unit so the service's lifecycle follows its
  node's.
- **Service-binary template.** `docs/how-to/write-a-service-binary.md` plus the
  `counter` example's SIGTERM handling and `is_alive` supervision — the shape a
  user's crate instantiates. Docs are cut over from example binaries to the
  packaged daemon.
- **Fleet-gated** (`docs/benchmarks/uc2-m9-gate-2026-08-19.md` is the record,
  including run 1's honest FAIL and its diagnosis — the harness's load model,
  not the cluster): leader stop under load **0.042 s, exit 0**; restart rejoins
  with **no snapshot install** (snapshot builds proven at ~25 MB alongside);
  commit rate recovered by **10.5 s observable** against a pre-committed 15 s
  bar (the observable figure is plumbing-dominated — an upper bound). Cluster
  switchover after a leader stop is **≈0.4 s** (derived from the ungated
  8.5 % × 5 s dip window).
- **Deployment model, stated plainly.** `uc2_client` is a same-host SDK: the
  intended shape is one app client per node — the leader's serves requests, a
  follower's answers its callers with a redirect to the leader
  (`NotLeader` carries a leader hint). Place `instance_dir` on a real disk;
  the node now refuses tmpfs by name.

**Rollup.** v2.3.0 is the first tag since v2.1.0 and therefore ships everything
below it: wire protocol **0.3.0** (post-M7 hardening, including the three
consensus safety fixes found by the Lean effort), **0.4.0** (M8 wire crypto —
opt-in and **off by default**; its cross-host fleet A/B remains a separate open
step, which is why no v2.2.0 was cut), and **0.5.0** (content-attested durable
reports, a consensus safety fix; **flag day** — upgrade all nodes together, a
mixed cluster stalls commits rather than committing unsoundly). Also since
v2.1.0: the pipelined client SDK (the public `Engine`/`PipelinedClient` tiers)
and the Rung A linearizable-read batch-probe rounds (~953k lin reads/s @ p50
1.08 ms mixed on the read-profile fleet).

## Shipped in v2.3.0 — wire protocol 0.5.0 — content-attested durable reports

**Consensus safety fix. Flag day for node↔node traffic: run one version.**

A follower's `AppendPosition` report used to carry a POSITION only — "I hold
this many bytes" — with nothing saying WHICH bytes. A leader ranking those
reports was therefore taking a position quorum, not a content quorum, and a
replica holding a deposed leader's copy of the same byte range counted toward
committing the current leader's history. Under rapid leader churn that
certified commits no live quorum backed; a later leader then truncated a
follower BELOW its own commit counter, and the service applied — and could
serve — bytes from a dead timeline.

- **Wire protocol → 0.5.0** (`version::CURRENT`). `DGRAM_KIND_APPEND_POSITION`
  gains an 8-byte body (`AppendPositionBody`) carrying `durable_term`: the term
  the sender attributes to the byte below its reported position. The 16-byte
  header is UNCHANGED, and the `cnc.dat` page is untouched
  (`CNC_V2_VERSION` unmoved), so service/client binaries are unaffected.
- **Leader-side check.** A report whose `durable_term` disagrees with the
  leader's own term map is declined (counted in `reports_unattested`). Equal
  terms at the same position imply identical prefixes (Log Matching), so this
  is the `(index, term)` pair Raft carries — it upgrades the ranking to a
  content quorum.
- **Mixed-version behaviour.** A 0.4.0 peer's header-only report decodes as
  *unattested* and is not counted. A mixed cluster therefore STALLS commits
  rather than making unsound ones — safe, but it means upgrading all nodes.
- Companion fixes in the same arc: the tracker's per-follower slot takes the
  latest report instead of a high-water mark (a follower's durable regresses
  when it truncates); term observations are delivered losslessly; the SM's
  durable is clamped to its term-observation frontier; and the follower's
  commit advance and its reports are both bounded by a validated frontier.

Measured on the directed rig (`uc2_node/tests/stale_read_hunt.rs`, 300 s of
500 ms-cadence leader kills): log rewinds beneath the applied frontier went
from 11 per run to **0**, with zero acked-write loss throughout.

## Shipped in v2.3.0 — wire protocol 0.4.0 — M8 wire crypto

**Opt-in, off by default.** Authenticated + encrypted node↔node UDP transport.
A cluster runs either all-encrypted or all-cleartext — **flag day, no mixed
mode**. Nothing changes for a deployment that does not set `CryptoConfig`.
Design: `docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md`. Gate:
`docs/benchmarks/uc2-m8-gate-2026-07-29.md`. Operator setup: runbook §11.

- **Identity + handshake.** Each node holds an X25519 static keypair; peers are
  authorized by an allowlist (`node id → static public key`, SSH
  `authorized_keys`-style, re-read at runtime so M7 node-adds need no restart).
  Noise `IK` (`Noise_IK_25519_AESGCM_SHA256`, via `snow`) establishes per-peer
  pairwise keys; the allowlist is enforced explicitly on the responder side.
- **Two key scopes, split by datagram kind.** Pairwise keys seal the unicast /
  low-rate kinds; a **cluster group key** seals the byte-identical fan-out
  (`DATA`/`HEARTBEAT`/`COMMIT_POSITION`/`READ_PROBE`) so the leader seals once
  and sends N times. The group key is minted by the leader, delivered per peer
  over the pairwise channel, and **rotates** on becoming leader, on a timer /
  byte budget, and on a committed `Remove*`.
- **Wire envelope.** The 16-byte datagram header stays cleartext and is
  authenticated as AES-256-GCM **associated data** (so `position`/`term`/`kind`/
  `key_epoch` cannot be rewritten undetected); an 8-byte per-sender counter and
  a 16-byte tag follow the payload — **24 bytes overhead**. The nonce is
  `0 ‖ counter` under a key derived **per sender per boot**
  (`HKDF(group_key, sender_id ‖ boot_salt)`), which makes counter reuse after a
  restart impossible by construction. RFC-6479 sliding-window anti-replay per
  `(sender, epoch)`.
- **Wire protocol → 0.4.0** (`version::CURRENT`). The `cnc.dat` page layout and
  its live `CNC_V2_VERSION` compatibility gate are **unchanged** — M8 changes
  the UDP datagram format, not the shmem page, so a 0.4.0 node's service/client
  IPC still accepts the older peers it did before. A new cnc observability
  field (`seal_failures`) is added in the reserved band.
- **Threat model.** A network-path adversary (read / inject / replay / reorder /
  corrupt, no node private key). **Out of model, documented residuals:** a
  compromised host; a malicious cluster member (the group key is symmetric, so
  any holder can forge fan-out traffic as any node); a removed node retains
  decryption of captured traffic until the next rotation; cleartext headers
  leak positions/terms/kinds to a passive observer.
- **Boot refusal.** An `Enabled` node whose key files are missing or unreadable
  refuses to start (it must not silently fall back to cleartext).
- **Correctness.** The full local proof stack and all four capstones
  (`lin_v2`, `lin_partition_v2`, the multi-process SIGKILL crashtest, and the
  elle tier under both models) pass with crypto ON, with the anti-vacuity of
  "crypto was actually on" proven by mutation (T15). Deterministic sim coverage
  of the handshake under loss/partition and key rotation (T13); an adversarial
  tier proving a replayed VOTE is refused, a revoked/impostor peer cannot
  establish, a cleartext downgrade is refused, and a corruption+replay storm
  neither panics nor diverges (T14).
- **Throughput (local same-box A/B, gate doc):** encrypted median **94.1%** of
  the cleartext control — a **5.9% regression, PASS** against the pre-committed
  ≤10% bar — on a deliberately worst-case contention box (3 in-process nodes,
  4 cores). Hardware AES-NI dispatch verified (8.2× vs a forced-software build).
  The definitive absolute number is the cross-host fleet A/B, owner-approved
  separately.
- **Known benign observability wart:** on an encrypted leader, the in-window
  `seal_failures` counter climbs continuously — the receiver reports its
  position to `cfg.leader`, which on the leader is *itself*, and there is no
  self-session, so each self-addressed report fails to seal. Pre-existing v2
  self-send made visible by the counter; harmless (the leader's position
  reaches commit ranking in memory). A follow-up will suppress the
  self-addressed report.
- **Deferred / follow-up:** the lock-free `sealing_epoch` fast path (not needed
  — arm A passed); suppressing the leader self-send; a release-mode OOB-read in
  `uc2_log`'s `read_frame_validated` (`debug_assert!`-only bounds guard,
  pre-existing v2 code from `72f649b`, out of M8 scope, surfaced during T14).

*The 0.3.0 items below shipped in the same tag (v2.3.0); 0.5.0 supersedes the
version number.*

## Shipped in v2.3.0 — wire protocol 0.3.0
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
- **Intake-gate reopen was keyed to `current_term`, not the data-plane term
  handle — a candidate cross-stream accept / acked-write-loss window**
  (Finding #9, lean LC-closure effort; affects all prior v2 releases): the
  receiver filters inbound DATA on the node-level `term_handle`
  (`receiver.rs:635` `dropped_stale_term`), but both intake-gate REOPEN sites
  keyed off `current_term` — the clean-reconcile arm (`node.rs` feed,
  `t >= sm.current_term()`) and the truncation-ack arm (`on_truncated`). A
  CANDIDATE's handle LAGS its `StartElection`-bumped `current_term`
  (`Action::StartElection` stores no handle, `node.rs:2440-2450`), so a
  candidate that adopted term T (handle T, gate closed), campaigned to T+1,
  then cleanly reconciled a term-T+1 leader's map REOPENED intake for its
  stale handle-T stream — and then accepted a term-T `serveTail`/NAK-repair
  byte its own term map never attributed (a cross-stream write), which its
  role-blind AppendPosition report (`receiver.rs:1049-1078`, retargeted to the
  new leader) could then feed into a commit over content that leader does not
  hold (§5.4.2 / Figure-8 acked-write-loss family, same class as #6b).
  Requires a candidate with a lagged handle + a clean higher-term reconcile +
  a co-term leader ranking the report; never observed outside the directed
  reproduction (no production deployment exists — pre-release fix). Fixed:
  BOTH reopen arms now fire only when `current_term == adopted_term` (== the
  `term_handle` the receiver filters at); a candidate's data intake stays
  CLOSED until it resolves (win / step-down / higher-term adoption re-keys the
  handle), costing nothing in steady state (followers always satisfy the
  equality). Found by the Lean LC-closure model (`n=5`, 56-step kernel-checked
  countermodel `finding_candidate_gate_reopen_fca_violation`, later deleted
  with the fix), reproduced RED-first and pinned by the sim
  (`finding9_lagged_handle_candidate_reopen_needs_handle_keyed`: the
  `handle_keyed:false` counterfactual reopens a lagged-handle candidate's gate,
  the shipped `handle_keyed:true` keeps it closed + converges). Remedy:
  upgrade; no back-port is planned.
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
