# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`ultima_cluster` (UC) is a **Rust-native State Machine Replication
application server**. This is **UC v2**; the v1 stack (an
`openraft`-based design) is retired and its crates deleted — v2 owns
consensus, elections, and transport directly. Do not reintroduce `openraft`,
`quinn`/QUIC, or the `uc_node`/`uc_service`/`uc_client` crate names.

**Current version: `2.8.0` (M14). Milestones M1–M13 are all complete**, each
closed by a fleet-proven gate doc under `docs/benchmarks/` (bars are
pre-committed before any run; a miss is recorded as FAIL and keeps the bar —
the honest-failure protocol); M14's gate doc has its bars committed and its
fleet run pending (see the gate doc). The per-milestone history that used to
live in this section is in `RELEASES.md` (user-facing), `docs/releases.md` (the
engineering record), and the gate docs; this section keeps only the map and
the standing facts that bind new work.

| milestone | release | what it shipped | gate doc (`docs/benchmarks/`) |
|---|---|---|---|
| M1–M6 | v2.0.0 | the v2 core: log+archive, replication, commit pipeline, elections, end-to-end SDK, snapshots/learners/purge | `uc2-m{4,5,6}-gate-*` |
| M7 | v2.1.0 | live single-server reconfiguration (promote/demote/add/remove via `uc2ctl`, one at a time, under load) | `uc2-m7-gate-2026-07-13` |
| M8 | v2.3.0 rollup | opt-in node↔node wire crypto (Noise IK + AES-256-GCM, flag-day) | `uc2-m8-gate-2026-07-29` |
| wire 0.5.0 | v2.3.0 rollup | content-attested durable reports — a consensus safety fix (commit ranking becomes a CONTENT quorum) | `docs/notes/uc2-term-map-window-loss-explained.md` |
| M9 | v2.3.0 | deployable node: `uc2-node` daemon, TOML config, named startup refusals, systemd | `uc2-m9-gate-2026-08-19` |
| M10 | v2.4.0 | observable cluster: `/metrics` `/healthz` `/readyz`, alert rules, dashboard | `uc2-m10-gate-2026-08-20` |
| M11 | v2.5.0 | survivable cluster: offline backup/verify/restore, quorum-loss recovery, ENOSPC fail-stop | `uc2-m11-gate-2026-08-20` |
| M12a–d | v2.6.0 | adoptable cluster: gateway kit + remote client, admin authn/audit, packaging/publishing, security posture + fuzz tier | `uc2-m12-gate-2026-08-22` |
| M13 | v2.7.0 | remote path at the cluster's speed: per-record MPSC ring (no publish convoy), Engine-shaped remote client, edge grant budget | `uc2-m13-gate-2026-08-24` |
| M14 | v2.8.0 | multi-service: one log → N FSMs (bounded/lockstep lag, per-FSM routing + fan-in, 0.6.0 snapshot stream, per-FSM observability) | `uc2-m14-gate-2026-08-29` |

(Tag state: `v2.2.0` was never tagged — M8 and wire 0.5.0 rolled into
`v2.3.0`; `v2.6.0` shipped as `v2.6.0-rc.1` only and is superseded by
`v2.7.0`, with no final `v2.6.0` tag. The ordered crates.io publish has
never been run; `docs/how-to/cut-a-release.md` §6 is the procedure.)

Next up: **M14c2** — the two-FSM capstones (`lin_v2 two_fsm`,
`lin_partition_v2`, hard-crash, Elle) as a proof-only `2.8.1`; spec §15.1.

### Standing facts that bind new work

- **Wire protocol is 0.6.0** (`uc_protocol::version::CURRENT`) (`0.6.0`
  changed `SNAP_BEGIN` only; a `0.5.0` sender's session is refused by name,
  so a mixed cluster stalls a joiner rather than installing half a set); the
  node↔node wire and the `cnc.dat` page layout are **flag days, never
  mixed-version** — a 0.4.0 peer's durable report reads as unattested and is
  not counted, so a mixed cluster stalls commits rather than making unsound
  ones; upgrade all nodes together. The client↔gateway remote protocol is
  separate and stays v1. What is API vs. what is flag-day:
  `docs/reference/semver-policy.md`.
- **Wire crypto is opt-in and OFF by default**, all-encrypted or
  all-cleartext per cluster (no mixed mode). Threat model: a network-path
  adversary; out of model: a compromised host or a malicious member — the
  fan-out group key is symmetric, so any holder can forge fan-out traffic as
  any node (a documented residual). UC seals its own reliable-UDP transport;
  no QUIC.
- **`[crypto]` and `[admin]` are explicit config choices since 2.6.0** — a
  `node.toml` without both refuses to start by name (a per-host edit, not a
  wire flag day; see `docs/how-to/upgrade-a-cluster.md`). Admin requests are
  HMAC-SHA256-signed, with an append-only, fsync-per-record audit log
  (`<instance_dir>/audit.jsonl`). Residual: a follower forwards an
  authenticated admin request to the leader over the node↔node UDP plane, so
  `[admin] auth = "hmac"` authenticates cluster-wide only when paired with
  `[crypto].enabled = true`.
- **Command payload ceiling: ≤ 1344 B crypto-off / ≤ 1312 B crypto-on** (one
  datagram, `MTU_DEFAULT = 1408`, not configurable — `preflight` refuses
  above it). `bincode` is `NoLimit`; the typed tier's decode is bounded by
  the payload cap and serde's 1 MiB pre-allocation cap, not by the codec.
- **Purge is OFF by default** (`PurgePolicy::Disabled`). The
  `/metrics`/`/healthz`/`/readyz` endpoint exists only when `[metrics]` is
  configured; readiness keys on `can_serve`, never the leader flag; the
  peer-slot metric band is leader-authoritative (followers export zeros).
- **Instance dirs reserve ~78 MiB at boot** (the IPC backing files are
  fallocated, not sparse, so a full disk is a named startup refusal instead
  of a SIGBUS mid-run); a node that cannot reserve it refuses to start.
- **M13 mechanics worth knowing**: the MPSC ingress ring commits per record
  (ring magic `ULTRNG2` — a same-host restart re-initialises the ring; a
  dead producer's hole is skipped and counted, cnc offsets 3968/3976);
  `RemoteClient` is a thin blocking layer over `RemoteEngine`'s send/poll
  halves; the gateway holds a global grant budget (the Engine window less
  1/8 headroom, divided across live connections). The M12 "collapse past the
  admission window" diagnosis was wrong — the cause was a ring publish
  convoy; see `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` before
  trusting any pre-2.7.0 gateway sizing advice.
- **M14 mechanics worth knowing**: ≤ 8 FSMs, id 0 mandatory and
  remote-reachable; lag policy per node must match cluster-wide (checked on
  the snapshot path); one stalled FSM on a quorum of hosts stalls commit by
  design (report ceiling); `service.<id>.lock` per FSM.
- **12 publishable crates, versioned in lockstep** with the tag and the
  image; `uc2_sim`, `uc-lincheck` and the example crates are
  `publish = false`. Publishing is manual and ordered
  (`docs/how-to/cut-a-release.md` §6); `deny.toml` + `cargo-deny` run in CI
  (one documented ignore: RUSTSEC-2025-0141, `bincode` unmaintained, no
  patched version exists). Docker/compose/ghcr/cosign are CI-only; aarch64
  binaries are built but never executed in CI.
- **Security posture**: `docs/security/{threat-model,attack-surface,self-assessment}.md`
  + root `SECURITY.md` (supported = latest minor; GitHub private
  vulnerability reporting). The whole proof surface is mapped in
  `docs/VERIFICATION.md` — sim, lincheck/crashtest capstones, Elle, Lean
  proofs + conformance, loom (log-buffer frame visibility + the MPSC ring's
  per-record commit), 15 fuzz targets, Miri (pure decoders + `uc2_remote`'s
  Vec-backed SPSC internals; the mmap'd IPC rings are out of Miri's reach).
- **`cargo fmt` is DEFERRED** (a one-shot reformat measures ~2 731 hunks)
  until the long-lived worktree (`fix/remaining-flakes`) lands; then run
  `cargo fmt --all` as a single mechanical commit and add `--check` to
  `ci.yml`.

Canonical documents, in order:

1. `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` — the
   canonical v2 design spec; read it end-to-end before substantial work.
   Later milestones have their own specs beside it (M7 reconfig, M8 wire
   crypto, M12 adoptable, M13 remote path — each amended with as-built
   errata where execution diverged from the draft).
2. `docs/benchmarks/uc2-m*-gate-*.md` — the per-milestone gate docs (the
   permanent record for v2; the `taskNN` docs under `docs/tasks/` are v1-era
   history).
3. `docs/ops/uc2-runbook.md` — operational runbook (instance-dir layout, cnc
   decode, purge enablement, live reconfiguration ops).
4. Storage primitives: `../ultima_db/docs/tasks/task26_journal.md`
   (`ultima_journal` log primitives) and `task27_snapshot_stream.md` (the
   `ultima_db` snapshot wire format). The `ultima-db` *code* dependency comes
   from crates.io (the workspace builds standalone); the sibling checkout is
   only needed for its docs or lockstep local development
   (`[patch.crates-io]`).

## Build & Test Commands

MSRV is 1.89 (`rust-version` in the root `Cargo.toml`'s `[workspace.package]`
— see that field's comment for how it was probed; CI's `msrv` job runs
`cargo clippy --workspace --all-targets --locked -- -D warnings` directly
against a 1.89.0 toolchain, not just `check`). Local dev, the rest of CI, and releases
build on the newer stable pinned in `rust-toolchain.toml` (currently 1.96.0;
rustup auto-installs it). To bump the pin: `rustup toolchain install <ver>
--profile minimal --component rustfmt --component clippy`, update `channel`
in `rust-toolchain.toml`, then run the full local proof stack before
committing — the MSRV floor is a separate, deliberate decision, not moved by
this.

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
scripts/fuzz_smoke.sh 60 --min-runs 10000         # fuzz regression gate: every target, 60s each (needs nightly + cargo-fuzz)
(cd fuzz && cargo +nightly fuzz run uc_protocol_datagram -- -max_total_time=600)  # hunt one target
scripts/elle_check.sh                            # elle consistency tier: 5 list-append passes, both models (needs java+jq)
scripts/elle_mutation.sh                         # elle mutation testing: control clean + 3 injected consensus bugs caught
(cd proofs && lake exe cache get && lake build)   # Lean proofs: model + theorems + conform checker (needs elan)
cargo run -p uc2_consensus --release --example conform_gen -- --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 1 && (cd proofs && lake exe conform $HOME/.cache/uc2-conform/vectors.jsonl)  # model<->Rust conformance
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release --test loom_mpsc  # MPSC ring loom model (log buffer: -p uc2_log --test loom_frame)
python3 bench-infra/scripts/m13_hop_bench.py --selftest  # M13 gate row arithmetic, no fleet/ssh
```

`fuzz/` is a `cargo-fuzz` crate **outside the workspace** (the root manifest
excludes it; it has its own `[workspace]` and lockfile), so `cargo
build/test/clippy --workspace` never sees it and it needs the nightly
toolchain plus `cargo install cargo-fuzz`. `scripts/fuzz_smoke.sh [--min-runs
N] [SECS] [TARGET…]` is the regression gate CI runs (`--min-runs 10000`
against 600 s per target); `fuzz/README.md` covers adding a target,
regenerating the corpus, and `tmin`/`cmin`.

The elle scripts write histories to `$HOME/.cache/uc2-elle*` (disk) — never
override `ELLE_DIR`/`ELLE_MUT_DIR` to `/tmp` (often RAM-backed tmpfs → OOM; see
"Local scratch" below). Nightly CI runs the clean tier (`elle` job); the weekly
`elle-weekly.yml` runs the mutation tier.

Cross-host fleet gates run via `bench-infra/` (terraform + ansible
provisioning); each milestone has its own driver under `bench-infra/scripts/`
— `m6_fleet_gate.py` (`--m7` for the M7 scenarios) through
`m9`/`m10`/`m11`/`m12_fleet_gate.py`, and `m13_hop_bench.py`, whose
`--arms gate` adjudicated the M13 bars on the fleet and whose `--selftest`
checks the row arithmetic locally.

Workspace crates:

- `uc_protocol` — wire spec; `core`-friendly data types (`version`, `magic`,
  `error_codes`) plus the lock-free ring buffers (`ring`:
  SPSC/MPSC/Broadcast — the MPSC ring commits per record since M13, so no
  producer ever waits on another)
  and the v2 wire spec (`v2`): the `cnc.dat` 8 KiB (cnc 3.0: page 2 is the
  per-service slot band) page layout, the self-locating
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
- `uc2_service` — service-side SDK. **M12a: two tiers.** `RawStateMachine`
  (bytes-in/bytes-out, the core contract) or the typed `StateMachine` (sync
  `apply`/`query`), which gets `RawStateMachine` for free via a blanket impl —
  a type implements exactly one of the two. Optionally `SnapshotStateMachine`
  (M6 purge) + `RawOutputHandler`/`OutputHandler` (async, leader-only,
  `TypedOutput` adapts the latter onto the former). `uc2_service::session::
  Sessioned<S>` wraps either tier for exactly-once-over-a-remote-hop: a
  16-byte `client_id ++ seq` envelope, a 1-byte FRESH/REPLAYED/EXPIRED tag,
  replicated `SessionConfig` enforced at snapshot install. The apply agent
  polls committed positions in the log buffer; reconstruction replays the
  journal or installs a snapshot + tail-replays.
- `uc2_client` — sync local-shmem input-client SDK. Small dep set (no transport,
  no consensus); matcher over the broadcast response ring.
- `uc2_remote` — **M12a**: the remote wire protocol (protocol v1: framed TCP,
  credit-gated flow control, `REDIRECT`/`LEADER_CHANGED`/`RETRY`) and
  `RemoteClient`, the pipelined, redirect-following, re-sending Rust
  implementation of it — for clients that cannot attach to shmem directly.
  **M13**: rebuilt as the `RemoteEngine` split halves
  (`RemoteSendHalf`/`RemotePollHalf`, lock-free SPSC internals, count-based
  admission); `RemoteClient` remains as a thin blocking layer on top.
- `uc2_gateway` — **M12a**: `Edge`, a per-node TCP front door that terminates
  `uc2_remote` traffic and relays it over the local `uc2_client::Engine`;
  ships as the `uc2-gateway` binary + `gateway.toml` + a systemd unit.
  **M13**: a global outstanding-grant budget — the sum of per-connection
  credits never exceeds the node's admission window.
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

## Local scratch: keep heavy artifacts off `/tmp`

On many Linux dev boxes `/tmp` is `tmpfs` — RAM-backed — and swap may be small
or absent. Anything written there (including the agent scratchpad at
`/tmp/claude-*/`) then consumes resident RAM, and large test outputs
(multi-tens-of-thousands-of-event elle histories, journal segments, load-test
dumps) race the busy-spin node clusters and `cargo` release builds for the
same pool until the kernel `SIGKILL`s the biggest process (exit 137/143) —
which manifests as tests dying mid-run or the Claude Code harness itself
getting torn down ("previous process exited"). This has happened repeatedly
on developer machines; avoid it structurally rather than by assuming the
current machine is big enough (do not encode a particular box's size,
mounts, or free space here — this file is shared):

- **Write test/scratch artifacts to real disk** (a path under `$HOME`), NOT
  `/tmp`. For the elle harness, set `ELLE_DIR` under `$HOME` (e.g.
  `$HOME/elle-out`), never the default `/tmp/uc2-elle`. Check with
  `findmnt /tmp` if unsure what backs it.
- Test **instance dirs / journals already go to the cargo target tree** via
  `env!("CARGO_TARGET_TMPDIR")` (the `tempdir()` helper in the test suites) —
  keep it that way; do not `tempdir()` under `/tmp`.
- Keep generated histories small (cap op targets), bound `elle-cli`'s JVM heap
  (`-Xmx`), and `rm -rf` scratch between runs to reclaim RAM.

## Benchmarking discipline

Perf **rate bars are fleet-only** (`bench-infra/`); a local run is **smoke**,
never a gate — never move a bar because a dev-box run went red. A dev box is
noisy whatever its size (busy-spin agents contend for the scheduler): on one,
the same dip measured 7× spanned 0–18% against a 10% bar.

The cargo target dir (`~/.cache/cargo-target`) is **shared by the main
checkout and every worktree**, so another checkout's build can silently swap
your binaries mid-measurement. For any measurement or proof stack run from a
worktree, set a private `CARGO_TARGET_DIR=<path>` and verify binary
provenance before trusting a number.

## Finding a performance bottleneck

UC's SMR is a chain of hops; throughput is bounded by the slowest. Don't
micro-optimize blindly — **isolate each hop, measure it alone** (realistic
stand-ins at the boundaries: dummy sink, dummy upstream, raw driver), and
compare against the whole-chain number: the hop whose solo throughput ≈ the
whole-chain throughput is the limiter, and optimizing any faster hop measures
null end-to-end. Two refinements from M13:

- A whole-system **collapse** can be an emergent pathology under a stress
  dimension, not any hop's slow steady state — sweep the stress axes
  (concurrency, inflight), and **reproduce the collapse in the smallest
  isolated hop** before believing a causal story.
- **Measurement refutes plausible-but-wrong stories.** M12 blamed the credit
  budget because the collapse "appeared past the admission window"; the
  isolation matrix (window large *and* small, sink with *no* window, load
  inside the envelope) proved it was an ingress-ring publish convoy —
  "consistent with the symptom" is not "the cause."

Two more from M14a's apply-hop isolation (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`):

- **Code in a hot loop's body costs even on paths that never run.** A wait
  ladder added inline to the apply loop's `Wait` arm cost 9 % at N=1 — a path
  N=1 never executes — through codegen alone; out of line it cost 1.5 %.
  A/B the *exact binaries* back to back on an idle box before attributing a
  delta to the change's semantics, and keep the hot body small.
- **A barrier wait must never sleep on a live peer.** One lockstep FSM in a
  50 µs sleep stalls every sibling's next frame, their ladders exhaust, and
  the set cascades into sleeping in lockstep (18 k frames/s); the yield budget
  has to exceed *any* plausible handshake, not the common one, and spinning
  on a slow peer's line only slows that peer (−6 % bounded at N=8).
- **Exact binaries are not enough — rebuild the same source twice first.**
  M14b's client-hop A/B read −4.2 % on one binary pair (17 pairs, no
  overlap); fresh builds of the same two commits read ±0.3 %, and two
  builds of the *same* commit differed by 1 %. Before attributing a delta to
  code, measure the harness's build-to-build resolution with a same-source
  rebuild control, and only trust deltas outside it (`scripts/hop1_ab.sh`,
  `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`).

Harness models: `uc2_gateway/examples/hop_bench` (client/edge/node hops),
`uc2_node/examples/apply_bench` (the FSM hop alone), `scripts/hop1_ab.sh`
(the client hop A/B, with a same-source rebuild control); worked example
`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`; the convoy mechanism
`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`.

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

The shmem layer is a fixed-layout `cnc.dat` 8 KiB (cnc 3.0: page 2 is the
per-service slot band) control page (`uc_protocol::v2::cnc`,
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

## Release documentation (required for every release)

The root **`RELEASES.md`** is the user-facing release document; `docs/releases.md`
is the deep per-release engineering record behind it. **Every new release adds a
new section at the top of `RELEASES.md`** (latest first), structured as:

1. one bullet per **feature**, briefly explained, each linking to a separate
   detailed doc (how-to / reference / `docs/notes/` explainer) — **write the
   detailed doc if it does not exist yet**;
2. one optional bullet for **fixed bugs**, with links to the docs that cover
   them (if they exist);
3. one optional bullet for **performance** results, with links to the gate /
   benchmark docs (if they exist).

Do this — plus the matching `docs/releases.md` entry and a sweep of
QUICKSTART / how-to / reference for statements the release invalidated —
**before tagging**, so the tag contains the writeup. README's "Scope and
limits" section stays a pointer to `RELEASES.md` plus the standing limits, not
a parallel prose copy of the release history.

## Pointers to dependent crates

- `ultima_journal/` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved). Design notes:
  `../ultima_db/docs/tasks/task26_journal.md`.
- `ultima-db` — MVCC copy-on-write B-tree store with `snapshot_stream` wire
  format (the default app-state store + snapshot format, behind `uc2_service`'s
  non-default `ultima_db` feature). **Dependency comes from crates.io** — no
  sibling checkout required to build. Docs live in the `../ultima_db/` repo
  (`CLAUDE.md`, `docs/tasks/task27_snapshot_stream.md`) when checked out.
