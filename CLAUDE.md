# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`ultima_cluster` (UC) is an **Aeron-shaped, Rust-native State Machine Replication
application server**. This is **UC v2**: milestones M1–M6 are complete and on
`main` (log+archive, replication, static-leader commit pipeline, elections, the
end-to-end SDK, and snapshots/learners/purge). The M5 throughput gate and M6
fleet gate both passed on real AWS fleets.

**M7 (live single-server reconfiguration)** ships promote/demote/add/remove
membership changes, one at a time, under load, via the `uc2ctl` admin CLI — no
restarts, no joint consensus (branch `uc2/m7-reconfig`; design spec
`docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`). Green on the full
local proof stack; the 5-host fleet run is a separate, user-approved step
(`v2.1.0` tags only once it lands — see
`docs/benchmarks/uc2-m7-gate-2026-07-13.md`). This bumps the wire protocol
version once (`FRAME_TYPE_CONFIG=4`, admin datagram kinds 16/17).

**M8 (wire crypto) ships authenticated, encrypted node↔node UDP — opt-in, off
by default** (branch `uc2/m8-wire-crypto`; design spec
`docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md`; gate doc
`docs/benchmarks/uc2-m8-gate-2026-07-29.md`). This **changes v2.0's stated
security posture**: encryption/auth was an *explicit non-goal* ("trusted
private network, same as stock Aeron"); it is now available and **flag-day
opt-in** (a cluster runs all-encrypted or all-cleartext, no mixed mode). Noise
`IK` handshake over an allowlist of X25519 static keys, AES-256-GCM over the
datagram envelope with the 16-byte header authenticated as AAD, a rotating
cluster group key for the fan-out plane, RFC-6479 anti-replay. Threat model:
a network-path adversary (read/inject/replay/corrupt, no private key); **out
of model**: a compromised host or a malicious cluster member (the group key is
symmetric — any holder can forge fan-out traffic as any node, a documented
residual). Bumped the wire protocol to **0.4.0** (`version::CURRENT`; the
`cnc.dat` page layout and its `CNC_V2_VERSION` gate are unchanged — M8 touches
the UDP datagram format, not the shmem page). The full local proof stack and
all four correctness capstones pass with crypto ON (T15, anti-vacuity proven);
the cross-host fleet A/B is a separate, user-approved step (`v2.2.0` tags only
once it lands). Do not reintroduce `quinn`/QUIC for this — UC seals its own
reliable-UDP transport directly.

**Wire protocol 0.5.0 (content-attested durable reports)** — a consensus
safety fix, not a milestone. `AppendPosition` carries an 8-byte body with the
term the sender attributes to the byte below its reported position; the leader
declines a report that disagrees with its own term map, which turns commit
ranking from a POSITION quorum into a CONTENT quorum (Raft's `(index, term)`
pair, sound by Log Matching). Header and `cnc.dat` layout unchanged; a 0.4.0
peer's header-only report reads as unattested and is not counted, so a mixed
cluster stalls commits rather than making unsound ones — **upgrade all nodes
together**. Found by the 2026-08-16 nightly flake hunt; see
`docs/superpowers/plans/2026-08-16-nightly-flake-hunt-brief.md` and
`docs/notes/uc2-term-map-window-loss-explained.md`.

**M9 (deployable node) is complete and released (`v2.3.0`)**: the real
`uc2-node` daemon (TOML config file, named startup refusals, SIGTERM
drain-and-stop), the `counter-service` binary template, systemd units and
`node.example.toml` under `packaging/`, and the restart-cost fleet gate
(`docs/benchmarks/uc2-m9-gate-2026-08-19.md`). The production-readiness arc
(M9–M12) is specced in
`docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md`.

**M10 (observable cluster) is merged; fleet rows pending (`v2.4.0` tags only
when they pass)**: an in-daemon `/metrics` + `/healthz` + `/readyz` endpoint
over the cnc page (hand-rolled `std::net`, zero new dependencies — enabled by
the `[metrics]` config section, off when absent), transition-triggered
JSON-lines records (`[log]` section sets the level), a fail-fast daemon on
agent death, and shipped alert rules + Grafana dashboard under `packaging/`
with every rule proven to fire (`scripts/m10_alert_fire.sh`, needs
`promtool`). Readiness keys on `can_serve`, never the leader flag — the
elected-but-not-serving `0x01` state is not ready. The peer-slot band is
leader-authoritative (followers export zeros). Gate doc:
`docs/benchmarks/uc2-m10-gate-2026-08-20.md`; operator docs:
`docs/how-to/monitor-a-cluster.md`.

**The v1 stack (an `openraft`-based design) has been retired** and its crates
deleted — v2 owns consensus, elections, and transport directly. Do not
reintroduce `openraft`, `quinn`/QUIC, or the `uc_node`/`uc_service`/`uc_client`
crate names; those are gone.

Canonical documents, in order:

1. `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` — the
   canonical v2 design spec. Read it end-to-end before substantial work.
2. `docs/benchmarks/uc2-m{1..7}-gate-*.md` — the per-milestone gate docs (the
   permanent record for v2; there is no `docs/tasks/` consolidation for v2 — the
   `taskNN` docs under `docs/tasks/` are v1-era history). M7's design spec is
   `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md` (a second,
   later spec than the M1-M6 one above).
3. `docs/ops/uc2-runbook.md` — operational runbook (instance-dir layout, cnc
   decode, purge enablement, live reconfiguration ops).
4. Storage primitives: `../ultima_db/docs/tasks/task26_journal.md`
   (`ultima_journal` log primitives) and `task27_snapshot_stream.md` (the
   `ultima_db` snapshot wire format). The `ultima-db` *code* dependency comes
   from crates.io (the workspace builds standalone); the sibling checkout is
   only needed for its docs or lockstep local development
   (`[patch.crates-io]`).

## Build & Test Commands

```bash
cargo build --workspace                          # build all workspace crates
cargo test                                       # in-process integration + sim tests (default)
cargo test -p uc2_node --test lin_v2             # WGL linearizability capstone (failover + purge/snapshot churn)
cargo test -p uc2_node --test lin_partition_v2   # network-partition / quorum-loss linearizability
cargo test -p uc2-crashtest --features hard-crash-tests   # spawn real node+service procs; SIGKILL mid-load, assert linearizable
cargo test -p uc2_service --features ultima_db   # the (non-default) ultima-db store adapter — the default build never compiles it
cargo clippy --workspace --all-targets -- -D warnings     # lint (must pass with zero warnings)
cargo run -p uc2_node --release --example m5_gate # throughput gate harness (see the gate doc)
cargo run -p uc2_node --release --example m6_gate -- all --secs 6 --cycles 5   # snapshots/learners/purge gate
cargo run -p uc2_node --release --example m7_gate -- all --secs 6             # live reconfig gate (replace/resize/self-removal)
cargo run -p uc2ctl -- status --instance-dir D --app-id A  # M7 admin CLI: add/promote/demote/remove/status
scripts/elle_check.sh                            # elle consistency tier: 5 list-append passes, both models (needs java+jq)
scripts/elle_mutation.sh                         # elle mutation testing: control clean + 3 injected consensus bugs caught
(cd proofs && lake exe cache get && lake build)   # Lean proofs: model + theorems + conform checker (needs elan)
cargo run -p uc2_consensus --release --example conform_gen -- --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 1 && (cd proofs && lake exe conform $HOME/.cache/uc2-conform/vectors.jsonl)  # model<->Rust conformance
```

The elle scripts write histories to `$HOME/.cache/uc2-elle*` (disk) — never
override `ELLE_DIR`/`ELLE_MUT_DIR` to `/tmp` (RAM tmpfs, no swap → OOM; see
"Local box" below). Nightly CI runs the clean tier (`elle` job); the weekly
`elle-weekly.yml` runs the mutation tier.

Cross-host fleet gates run via `bench-infra/` (terraform + ansible provisioning,
`bench-infra/scripts/m6_fleet_gate.py` as the orchestrator — pass `--m7` for
the M7 scenarios, which drive `uc2ctl` for admin ops and run over a 5-host
topology instead of M6's 4).

Workspace crates:

- `uc_protocol` — wire spec; `core`-friendly data types (`version`, `magic`,
  `error_codes`) plus the lock-free ring buffers (`ring`: SPSC/MPSC/Broadcast)
  and the v2 wire spec (`v2`): the `cnc.dat` 4 KiB page layout, the self-locating
  UDP datagram header, and per-message frame layouts. Multi-language gate.
- `uc2_log` — the log buffer + archive. File-backed shared log buffer (readers
  poll positions in place, bounded by the commit counter) and the archive agent
  that records ≤1 MiB blocks into `ultima_journal` (the retransmit + recovery
  store). Owns snapshot builder + below-floor reconstruction primitives.
- `uc2_net` — own reliable-UDP transport (no QUIC): sender/receiver polling
  agents, NAK-based retransmit off the log buffer, quorum-paced flow control,
  snapshot sessions. A seeded fault layer drives the sim.
- `uc2_crypto` — **M8 wire crypto (opt-in, off by default)**: pure-sync,
  socket-free crypto plane for node↔node UDP. Noise `IK` handshake (`snow`,
  X25519), per-peer pairwise keys + a rotating cluster group key, AES-256-GCM
  seal/open over the datagram envelope (16-byte header authenticated as AAD),
  RFC-6479 anti-replay, and the `SharedTransport`/`SendHalf`/`ReceiveHalf`
  split that keeps the per-datagram hot path off a lock. `uc2_net` calls it at
  two seams; `uc2_node` owns config, handshake routing, and key rotation.
- `uc2_consensus` — pure-sync Raft-safety core over **byte positions**:
  `CommitTracker` (quorum-th highest committed position), `ElectionSm`
  (lexicographic `(last_term, last_durable)` vote, data-stamped term map,
  truncation). No async, no I/O — driven deterministically by the sim.
- `uc2_sim` — virtual-time deterministic world + safety invariants + seeded
  fuzz. The gate that proves consensus safety without hardware.
- `uc2_node` — the node binary + library. Wires the **four single-writer polling
  agents** (consensus / sender / receiver / archive), the `cnc.dat` page, the
  ingress ring, and the linearizable-read barrier. Owns elections and truncation.
- `uc2_service` — service-side SDK. User implements `StateMachine` (sync `apply`,
  sync `query`) + optionally `SnapshotStateMachine` (M6 purge) + `OutputHandler`
  (async, leader-only). The apply agent polls committed positions in the log
  buffer; reconstruction replays the journal or installs a snapshot + tail-replays.
- `uc2_client` — sync local-shmem input-client SDK. Small dep set (no transport,
  no consensus); matcher over the broadcast response ring.
- `uc-lincheck` — test/verification library: WGL linearizability `checker`, op
  `history` recorder, `model`, and the in-memory CAS-`register` SM
  (`Cmd`/`CmdResp`/`RegisterSm: uc2_service::StateMachine`). One source of truth
  shared by the in-process lincheck capstone (`uc2_node/tests/lin_v2.rs`) and the
  multi-process hard-crash test.
- `examples/uc2-crashtest` — multi-process test harness: reference bins (node +
  service halves over a shared instance_dir) + the hard-crash tests behind the
  `hard-crash-tests` feature. The real `kill -9` path for reconstruction validation.
- `ultima_journal` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved).

## Local box: do NOT write heavy artifacts to `/tmp`

**`/tmp` on this box is `tmpfs` — RAM-backed — and there is NO swap.** Anything
written under `/tmp` (including the agent scratchpad at `/tmp/claude-*/`) consumes
resident RAM. Large test outputs there (multi-tens-of-thousands-of-event elle
histories, journal segments, load-test dumps) race the busy-spin node clusters
and `cargo` release builds for a ~15 GiB pool, and the kernel then `SIGKILL`s the
biggest process (exit 137/143) — which manifests as tests dying mid-run or the
Claude Code harness itself getting torn down ("previous process exited"). This
recurs; avoid it structurally:

- **Write test/scratch artifacts to real disk** (`/dev/sda1`, mounted at `/`, ~66
  GiB free), NOT `/tmp`. For the elle harness, set `ELLE_DIR` under `/home/claude`
  (e.g. `/home/claude/elle-out`), never the default `/tmp/uc2-elle`.
- Test **instance dirs / journals already go to ext4** via
  `env!("CARGO_TARGET_TMPDIR")` (the `tempdir()` helper in the test suites) — keep
  it that way; do not `tempdir()` under `/tmp`.
- Keep generated histories small (cap op targets), bound `elle-cli`'s JVM heap
  (`-Xmx`), and `rm -rf` scratch between runs to reclaim RAM.

## Architecture overview

UC is a State Machine Replication application server. Three process roles;
same-host inter-process traffic via shared memory, cross-host traffic via UC's
own reliable-UDP transport between nodes:

```
[client process]      ──shmem──▶  [uc2_node]  ◀──reliable-UDP──▶  [uc2_node on peer host]
                                      ▲
                                      │ shmem (file-backed log buffer + cnc page)
                                      ▼
                                 [uc2_service]
```

Each node is **four single-writer polling agents**, counter-coordinated (no
locks on the hot path): **consensus** (commit tracking + elections), **sender**
and **receiver** (reliable-UDP replication + NAK repair), and **archive** (record
the log buffer into `ultima_journal` in ≤1 MiB blocks). All coordination is
through atomic counters in the `cnc.dat` page and monotonic byte **positions**
(the absolute-offset analog of a Raft log index); `apply` is keyed on position.

- `uc2_node` owns consensus, log durability, snapshot transport, leader election.
- `uc2_service` owns the user's deterministic business logic (`apply`, `query`)
  and side-effecting `on_committed` (leader-only, at-least-once).
- Client processes translate external requests into Commands and submit via shmem.

The shmem layer is a fixed-layout `cnc.dat` 4 KiB control page (`uc_protocol::v2::cnc`,
offsets pinned in both `uc_protocol` and `uc2_log` so they never drift) plus the
file-backed log buffer and per-stream ring buffers under an instance directory.
Ring buffers are lock-free; SPSC for service↔node, MPSC for clients→node,
Broadcast for node→clients (position-keyed responses bypass the node via an
egress broadcast).

Storage primitives:
- Log buffer: `uc2_log` file-backed ring; the appender never overwrites bytes not
  yet recorded (one hard overrun gate); all other readers degrade to journal replay.
- Archive / recovery: `ultima_journal::Journal` (segmented append, group commit,
  CRC per block; block seq = block index, meta = base position).
- Durable state: `ultima_journal::StableValue<T>` (rotating two-slot atomic value)
  for vote, term map, snapshot floor, output progress, cluster-config record (config.state).
- App state + snapshots: the user's `StateMachine`; M6 snapshots use the
  `SnapshotStateMachine` capability and (for the default store) `ultima_db`'s
  `snapshot_stream` wire format.

Replication is reliable-UDP: the log buffer doubles as the retransmit buffer, a
receiver that falls behind sends NAKs repaired from the buffer (or, below the
purge floor, upgraded to a snapshot session), and flow control is a quorum
order-statistic over follower durable positions. A follower more than one buffer
behind is served from the journal (deep-NAK replay), never prefilled.

Commit / apply pipeline (steady state):
- Client writes a submit frame into the ingress MPSC ring (admission window at the door).
- The leader appends to the log buffer, replicates via the sender agent, and the
  consensus agent advances the commit counter when a quorum's durable positions cross it.
- The service's apply agent polls `min(commit, durable)` in the log buffer in
  place and calls `state_machine.apply(position, cmd)`, publishing the response
  to the egress broadcast (position-keyed) for the client's matcher.

Snapshots + purge (M6, **OFF by default** — `PurgePolicy::Disabled`): a
service-built snapshot lets a node drop the journal prefix below the snapshot
floor (`PurgePolicy::BelowSnapshot { slack_bytes }`). A node that has fallen
below the floor — a crashed-and-restarted service, a fresh **learner**
(replicated-to, never counted in quorum), a cold-started node — converges by
**installing a snapshot + tail-replaying**, never by reading the purged prefix.
`NoCommonPrefix` = wipe-and-rejoin.

Linearizable reads go through a `READ_PROBE`/`ACK` quorum barrier, wait for the
service to catch up to the read position, and use a follower header-term check +
capture-recheck + a service-epoch backstop (accept the answer only if the service
didn't restart during the query) to close the TOCTOU against a crashing service.

Correctness is proven at three levels: the deterministic sim (`uc2_sim`, safety
invariants + seeded fuzz), the WGL lincheck capstones (`uc2_node/tests/lin_v2.rs`
under failover AND purge/snapshot churn; `lin_partition_v2.rs` under
partition/quorum-loss — all driving the untouched `uc-lincheck` checker), and the
multi-process SIGKILL crashtest (`examples/uc2-crashtest`).

## Code conventions

- **`uc_protocol` core types stay `core`-friendly.** `version`/`magic`/`error_codes`
  import nothing outside `core`; the ring buffers need `std::sync::atomic` + `memmap2`.
  No `tokio` in the protocol layer.
- **Apply is sync, deterministic, no I/O.** The trait signature enforces it:
  `fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response`. No
  `async`, no clock, no randomness. Non-negotiable for SMR correctness. `position`
  (the absolute byte offset) is the idempotency key.
- **Consensus is pure-sync.** `uc2_consensus` (CommitTracker, ElectionSm) has no
  async and no I/O — it is driven by the node's polling agents and the sim. Safety
  logic lands there so the sim can adjudicate it deterministically.
- **`output_handler` is async, leader-only, retryable.** Returns
  `Result<(), OutputError>` where `Retryable` retries while leader and `Permanent`
  advances the durable (increase-only) progress marker anyway.
- **Reads are typed `Query` / `QueryResponse`, not closures.** The IPC boundary
  doesn't carry closures; the framework routes linearizable vs. snapshot reads.
- **`AppCommand = bytes::Bytes` end-to-end.** Refcounted; flows from the log
  buffer through apply without intermediate copies.
- **Per-record framing uses an atomic-after-write length prefix.** Reader sees
  length=0 → record not yet committed → spin/yield. Standard torn-record protection.
- **cnc page offsets are pinned in BOTH `uc_protocol` and `uc2_log`** with
  offset-assertion tests, and must never drift. Add fields in the reserved band.
- **Snapshot `freeze`/`install_snapshot` are keyed on `position`.** `install_snapshot`
  takes the target position and rejects a mis-tagged artifact.
- **One node per instance directory** — an exclusive flock prevents accidental
  coexistence; service and clients take a shared lock as a liveness probe.
- **`app_id` + `instance_id` + `protocol_version` checked at every IPC entry.**
  Wrong `app_id` = wrong cluster; changed `instance_id` = node restart since last
  attach; protocol mismatch = refuse.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature
development is fine — the generated plans/notes under `docs/superpowers/` are
working artifacts. For v2, the per-milestone record is the **gate doc**
(`docs/benchmarks/uc2-mX-gate-*.md`) plus the runbook and the retained superpowers
plan; there is **no `docs/tasks/` consolidation** for v2 (that pattern was v1-era).

**Leave the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`,
`docs/superpowers/specs/*.md`) in place.** Do NOT delete them as part of finishing
a feature — they are retained as historical scaffolding; the maintainer removes
them manually if ever.

## Pointers to dependent crates

- `ultima_journal/` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved). Design notes:
  `../ultima_db/docs/tasks/task26_journal.md`.
- `ultima-db` — MVCC copy-on-write B-tree store with `snapshot_stream` wire
  format (the default app-state store + snapshot format, behind `uc2_service`'s
  non-default `ultima_db` feature). **Dependency comes from crates.io** — no
  sibling checkout required to build. Docs live in the `../ultima_db/` repo
  (`CLAUDE.md`, `docs/tasks/task27_snapshot_stream.md`) when checked out.
