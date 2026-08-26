# Releases

What each release introduced, newest first. Every feature links to the doc
that explains it in full. The deep engineering record — complete safety-fix
analyses, wire-version mechanics, upgrade remedies — is
[`docs/releases.md`](docs/releases.md); the per-milestone proof records
(pre-committed bars, fleet runs) are in
[`docs/benchmarks/`](docs/benchmarks).

## v2.7.0 — 2026-08-26 — the remote path at the cluster's speed (M13)

The remote path — `client → TCP → gateway → shared memory → node` — now runs
at the backend's own rate and **degrades instead of collapsing** when a host
has more connections than cores. Three defects, located by a per-hop
isolation bench that measured every hop alone
([the bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)) and fixed
together. Nothing here touches consensus, the node-to-node wire protocol, or
the cnc page; the remote wire protocol stays v1. Proof record, row by row:
[M13 gate](docs/benchmarks/uc2-m13-gate-2026-08-24.md).

- **A rebuilt remote client** (`uc2_remote`): the same blocking
  `RemoteClient::submit` / `Ticket::wait` surface, over an `Engine`-shaped
  split — a submitter that encodes straight into a preallocated outgoing
  ring, a writer thread that coalesces whatever is queued into one `write`,
  a reader that resolves completions without a lock, and a poll half for
  callers that want batches instead of tickets. The old client paid one
  `write` and about seven futex operations **per request**; it capped at
  ~171k responses/s against a sink that answered instantly, while a raw
  client through the same shipped gateway into the same shipped cluster did
  1.14M/s. That gap was the remote path's bottleneck, by 7×. →
  [Remote protocol](docs/reference/remote-protocol.md) ·
  [the hop bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)
- **A shared-memory ingress ring that cannot convoy**
  (`uc_protocol::ring::mpsc`): producers now commit their own record and no
  producer ever waits for another; the single consumer walks records in claim
  order and stops at the first uncommitted one. A producer that is preempted
  mid-record costs one consumer stall, not a pile-up of every other producer
  spinning on it. A producer that *dies* mid-record leaves a hole the
  consumer skips after `hole_timeout`, counted and logged, instead of
  wedging every producer forever. →
  [The MPSC publish convoy, explained](docs/notes/uc2-m13-mpsc-publish-convoy-explained.md)
- **A global credit budget at the gateway**: the edge holds one `Engine`
  window, keeps an eighth back as headroom, and divides the rest equally
  across its live connections instead of promising each one the same
  constant. A shrinking share is pushed as a `STATUS` before the client can
  send into it; a growing one rides the next response. Two new startup
  checks come with it — `per_conn_inflight` above the budget is a named
  refusal, `max_connections` above it a printed warning. The old
  halve-on-backpressure ladder is still there and is now the exception path.
  →
  [The grant budget](docs/reference/gateway-config.md#the-grant-budget-270) ·
  [Run a gateway](docs/how-to/run-a-gateway.md#operating-envelope-270)
- **Fixed:** the `2.6.0` gateway collapse — ~30× throughput loss, second-scale
  p95 and lost responses past eight connections on an eight-core host — is
  gone, and its diagnosis is corrected. It was **not** the missing credit
  budget the `2.6.0` envelope blamed: it reproduced at 2,048 outstanding
  requests, well inside that envelope, against a sink with no admission
  window at all. It was the ingress ring's publish convoy. The `2.6.0`
  operating envelope and the `CPUQuota=` advice that went with it are both
  retired — CPU containment made the convoy *worse*. →
  [the correction](docs/notes/uc2-m12a-edge-flow-control-gap.md) ·
  [M12 gate row 2, closed](docs/benchmarks/uc2-m12-gate-2026-08-22.md)
- **Performance:** measured on a 4× `c6id.2xlarge` fleet, the gate's
  adjudicated rows — one connection through the gateway against the direct
  shared-memory arm on the same cluster generation, the N-connection
  aggregate against the same reference, the 1→16 connection ladder for
  monotonicity, and N shared-memory engines on an oversubscribed host. →
  [M13 gate](docs/benchmarks/uc2-m13-gate-2026-08-24.md) ·
  [per-hop bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)

**Upgrade consequence.** The ingress ring's on-disk header changed, so its
magic is bumped and a stale attach is refused by name. **Restart the node,
the service, the gateway and every local client on a host together** — this
is a same-host restart, not a cluster flag day: nodes on different hosts do
not talk to each other through this ring, and the node-to-node wire protocol
is untouched. A gateway `[limits]` section with `per_conn_inflight` above the
grant budget (`max_inflight` less an eighth) now refuses to start, by name. →
[Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)

## v2.6.0 — adoptable cluster (M12) — *shipped as `v2.6.0-rc.1`; superseded by v2.7.0, no final tag*

**Written before the tag, as every release here is.** The four M12
sub-milestones — M12a gateway kit, M12b admin authentication and audit, M12c
packaging and publishing, M12d security posture — land together as `v2.6.0`.
**The tag itself is a separate maintainer step**, taken after deciding what to
do about gate row 2 — which has now been run on a fleet twice and, in the
process, showed its own bar to be mis-specified (see *Gates* below);
`v2.6.0-rc.1` goes first, because the
release workflow has never been run for real and a release candidate is the
right place to find that out. Running record, row by row:
[M12 gate record](docs/benchmarks/uc2-m12-gate-2026-08-22.md).

**This release is what makes the cluster adoptable by someone who is not
its author**: clients that do not live on a node's host can reach it, admin
operations have a credential and a paper trail, the software installs from a
signed artifact with no toolchain, and what is defended — and what is not — is
written down.

- **A two-tier state-machine contract**: `RawStateMachine` (bytes in, bytes
  out) is now the core trait, and the typed `StateMachine` you already write
  is a blanket adapter on top of it. Existing services change **nothing** —
  the adapter is the same bincode call, so a typed state machine's frames are
  byte-identical to `v2.5.0`'s. What is new is the escape hatch: a service
  with hot or large commands can own its own framing and skip the codec
  entirely. The dev-box spike that motivated this measured the typed tier at
  **75.8 %** of the apply cycle against the raw tier's **5.8 %** at a 509 B
  payload — a share, on a box that is not a bench. The fleet run (gate row 3)
  has since confirmed it on real hardware at the same 509 B payload: typed
  `sm_apply` 1173 ns/frame (**87.7 %** of the apply cycle) against raw's 14 ns
  (**8.0 %**), an ~84× per-frame drop. →
  [State-machine contract](docs/reference/state-machine-contract.md) ·
  [Two tiers, one contract](docs/notes/uc2-two-tier-state-machine-contract.md) ·
  [the codec budget spike](docs/notes/2026-08-22-codec-budget-spike.md)
- **Exactly-once over a remote hop** (`uc2_service::session::Sessioned<S>`):
  wrap either tier and a re-sent request after a failover is classified
  `FRESH` / `REPLAYED` / `EXPIRED` instead of silently applied twice. The
  dedup table is replicated state — it rides snapshots, and
  `install_snapshot` refuses one whose embedded `SessionConfig` disagrees with
  the live node rather than silently retuning it. →
  [State-machine contract](docs/reference/state-machine-contract.md)
- **A remote protocol and client** (`uc2_remote`, protocol v1): framed TCP,
  credit-gated flow control, pipelined submit/query, and a `RemoteClient` that
  follows `REDIRECT`/`LEADER_CHANGED` across an election and re-sends
  unanswered requests in order. Written to be re-implemented in another
  language — the frame layout and every state transition are specified. →
  [Remote protocol](docs/reference/remote-protocol.md)
- **A gateway** (`uc2-gateway` + `gateway.toml`): a per-node TCP front door
  that terminates the remote protocol on a node's host and relays over the
  existing shared-memory `Engine`. Clients no longer have to share a host with
  a node to talk to the cluster. It touches no consensus code, no wire
  protocol between nodes and no cnc field. →
  [Run a gateway](docs/how-to/run-a-gateway.md) ·
  [Gateway configuration](docs/reference/gateway-config.md) ·
  [gateway shapes and flow control](docs/notes/uc2-gateway-shapes-and-flow-control.md)
- **Admin authentication and an audit log**: mutating `uc2ctl` verbs are
  signed with a named HMAC-SHA256 key (`--admin-key`/`--admin-key-name`/
  `--admin-ttl-secs`; `uc2ctl gen-admin-key PATH` writes one), every admin
  request is recorded — accepted *or* refused, with the signing key's name —
  in an append-only, `fsync`-per-record `<instance_dir>/audit.jsonl`, and
  `uc2ctl audit` reads it back offline. A request that cannot be recorded is
  refused rather than answered unrecorded. **Residual, stated wherever it
  matters:** a follower forwards an authenticated request to the leader over
  the node-to-node UDP plane, which is only address-filtered unless wire
  crypto is on — so `[admin] auth = "hmac"` authenticates cluster-wide only
  paired with `[crypto].enabled = true`. →
  [Configuration § admin authentication](docs/reference/configuration.md#admin-authentication) ·
  [Who may change the cluster](docs/notes/uc2-admin-authentication.md) ·
  [Change cluster membership](docs/how-to/change-cluster-membership.md)
- **Two config choices are now explicit, and an old `node.toml` will not
  start without them.** `[crypto]` and `[admin]` are both required sections:
  a config written for `v2.3.0`–`v2.5.0` refuses to start with a named error
  (`CryptoChoiceRequired` / `AdminChoiceRequired`) until each host's file says
  which posture it wants. This is a per-host config edit, not a wire flag day
  — nodes on either side of it interoperate. →
  [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md) ·
  [Configuration](docs/reference/configuration.md)
- **Install without a toolchain**: signed tarballs for x86-64 and aarch64, a
  `SHA256SUMS`, a CycloneDX SBOM and a distroless `ghcr.io/peterknego/uc2`
  image are published per tag, all signed keylessly (cosign, identity-pinned
  verification written out), and a `quickstart-local.sh` inside the tarball
  brings up three nodes, three services and three gateways on one host and
  prints `PASS`. The publish gate is a smoke run in a bare container with no
  Rust installed — nothing is released unless it passes. →
  [QUICKSTART](docs/QUICKSTART.md) ·
  [Cut a release](docs/how-to/cut-a-release.md) ·
  [`packaging/README-release.md`](packaging/README-release.md)
- **A version identity and a compatibility promise**: all 12 publishable
  crates move in lockstep at `2.6.0`, with the metadata crates.io needs, a
  written semver policy that says what is public API and what is not, an MSRV
  floor of **1.89** enforced by a CI job that runs `clippy` on that exact
  toolchain, and supply-chain gates (`cargo-deny` advisories/licenses/bans, on
  both the default and `--all-features` graphs). →
  [Semver policy](docs/reference/semver-policy.md)
- **A security package, and a fuzz tier that found real defects**: a threat
  model, a per-parser attack surface (19 rows), a self-assessment with its
  findings and its *accepted* weaknesses, and a `SECURITY.md` with a reporting
  channel. Alongside it, 14 `cargo-fuzz` targets over every decoder that
  touches untrusted bytes, run nightly at 600 s per target with a minimum-runs
  floor (because a fuzz tier can be green and vacuous — one was, and that is
  written up too), plus Miri over the pure decoders. The README now states the
  posture and the scope limits up front. →
  [`docs/security/`](docs/security) ·
  [Verification § fuzzing](docs/VERIFICATION.md) ·
  [SECURITY.md](SECURITY.md)
- **Fixed bugs** — every one of them found by the sub-milestones' own review
  and fuzz loops rather than by a user. Three are in code that never shipped
  in any tag; the fourth is older, and what was never true of it in a released
  tag is *reachability* — every caller guarded it. Full findings, with
  severity and status:
  [security self-assessment §2](docs/security/self-assessment.md#2-findings).
  - **A captured admin request could be replayed after a restart** by an actor
    with instance-directory write access and no key at all: the HMAC was
    verified against an `instance_id` re-read from the cnc page — a file whose
    header is only magic-checked — so the captured value could simply be
    written back. The tag is now bound to the node's boot-time state, pinned
    by a regression test that performs the forgery (F4, fixed pre-merge,
    `50473d5`). →
    [Who may change the cluster](docs/notes/uc2-admin-authentication.md) ·
    [self-assessment §2](docs/security/self-assessment.md#2-findings)
  - **`Sessioned::apply` violated the buffer contract it was itself a caller
    of** — a contract-abiding inner state machine that cleared `out` truncated
    the session tag away and panicked **on the apply thread**, killing the
    service on its first command. Found by fuzzing (F2, `7c908b1`). →
    [Verification § fuzzing](docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes)
  - **`Sessioned::install_snapshot` pre-allocated up to 1 GiB** from an
    unvalidated 8-byte length before reading a byte of the blob. Bounded;
    20 000 executions went 91.8 s → 0.34 s. Found by fuzzing (F3,
    `7c908b1`). →
    [self-assessment §2](docs/security/self-assessment.md#2-findings)
  - **Five UDP datagram readers could panic on a short slice.** This code
    shipped in every tag through `v2.5.0`, and in none of them was the panic
    reachable — every caller guarded it — but the totality of the first code
    an unauthenticated packet reaches should not rest on five call sites
    remembering. All five now return `Option`, the pre-guards are kept, and
    the hot path is byte-identical (F1, `112b81f`). →
    [Verification § fuzzing](docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes)
  - Also: `uc2_remote`'s `request_timeout` is now enforced *while
    reconnecting* (it could be outlived by a reconnect loop — F5,
    `ae0f245`/`fc27536`/`b4b3b0c`), and the architecture doc's log-buffer
    default is corrected to `buffer_bytes`' real 64 MiB.
- **Performance — a remote-path batching fix, and the network budget
  measured.** The remote path now batches on every hop: a `RemoteClient`
  writes its pending frames in a single `write_all` (flushing when the queue
  drains), the edge driver batches its writes per drain, both sides parse
  multiple frames out of one `recv`, and admission notifications are coalesced
  to one per read batch — with `request_timeout`/deadline semantics and the
  exactly-once and credit-flow-control invariants unchanged (reviewed, not
  assumed). On the fleet the single-connection gateway/direct ratio moved
  **0.072 → 0.098** with the session envelope on and **0.064 → 0.101** with it
  off, ~+40 % throughput; on the dev box — a smoke observation, not a bench —
  p50 fell from ~112 ms to ~10 ms at 4096 inflight. Separately, a
  network-budget characterization settled whether a leader box is near its NIC
  limit at peak: **it is not.** The 1,424,941 resp/s peak drives ~3.21 Gbps
  and ~392k pkt/s — about a quarter of the instance's ~12.5 Gbps ceiling —
  because replication is batched to ~0.28 packets and ~281 bytes per committed
  command, and **p99 < 1 ms holds to 518,287 resp/s** (inflight 256: p50 0.472
  / p90 0.568 / p95 0.611 / p99 0.660 ms, NIC ~1.14 Gbps). There is ample
  headroom for a co-located gateway client; the ~1.4M/s ceiling is software,
  not the network. →
  [M12 gate record § network budget](docs/benchmarks/uc2-m12-gate-2026-08-22.md#network-budget-characterization-2026-08-24-path-1) ·
  [Remote protocol](docs/reference/remote-protocol.md)
- **Known limits — a gateway's flow control is per-connection only, and past
  the node's admission window the edge collapses rather than degrading.**
  Every connection is granted `per_conn_inflight` credits in full at
  `HELLO_OK` and the halve/relax ladder runs per connection; **nothing bounds
  the sum across connections** against the co-located node's ingress admission
  window (`admission_bytes`, default 256 KiB ≈ 4–6k frames). Inside that
  envelope the edge aggregates near-linearly — 451k resp/s across 4
  connections, 0.32× the backend's peak. Outside it, the 2026-08-24 fleet
  ladder measured a ~30× aggregate collapse at 8 connections (p95 4.3 s) and
  9,126 lost responses at 16, with the edge burning ~7 of the host's 8 cores
  and starving the node beside it — reproduced with the edge's protective
  per-connection cap active, so it is a product defect, not a
  misconfiguration. The fix (a global, admission-aware outstanding-grant
  budget at the edge) is planned as the next milestone. **Until then**: keep
  total client inflight across all connections to one edge under the node's
  admission window, and bound a co-located gateway's CPU (`CPUQuota=`, shipped
  commented in the unit file). →
  [Operating envelope](docs/how-to/run-a-gateway.md#operating-envelope-270) ·
  [gate record § the confirmed defect](docs/benchmarks/uc2-m12-gate-2026-08-22.md#clean-discipline-re-run-same-day-the-collapse-is-a-product-defect-not-a-harness-artifact)
- **Gates** — [M12 gate record](docs/benchmarks/uc2-m12-gate-2026-08-22.md),
  reported the way this project reports: what ran, and what did not.
  - **PASS**: admin authentication, audit and refusal behaviour end to end
    (row 4, per-PR CI); crates package and the leaf crates publish (row 7);
    the MSRV floor (row 11); the supply-chain gates (row 12).
  - **Pending its first nightly**: the remote lincheck capstone — three
    gateways in the loop, repeated leader SIGKILLs, zero acked writes lost —
    is green three consecutive local runs and awaits CI adjudication (row 1);
    the fuzz job is built and locally proven across ~118 M executions but has
    never run on a GitHub runner (row 8).
  - **Fleet-run, and reported the way the run came out**: the codec share on
    the apply thread (row 3) **PASSES** as a measurement row — the fleet put
    the typed `CountSm` at `sm_apply` 1173 ns/frame (87.7 % of the apply
    cycle) against the raw `RawCountSm`'s 14 ns (8.0 %), an ~84× per-frame
    drop that confirms on real hardware the spike finding behind the two-tier
    contract. Gateway throughput versus the direct `Engine` (row 2) **fails
    its ≥ 0.8× bar — and the bar is the part that is wrong**: it compares one
    `RemoteClient` on one TCP connection to one shmem client, and no single
    TCP request/response connection matches shared memory at any batching
    level (Little's Law fits both arms with no residual). The honest numbers
    are **~0.1× per connection** (0.098 envelope on / 0.101 off, after the
    batching fix above) and **451k resp/s aggregate across 4 connections —
    0.32× the backend's measured 1.42M/s peak**, the edge scaling
    near-linearly to that point. Re-specifying row 2 as an N-connection
    edge-saturation ratio is recommended and **not yet done**; the
    single-connection number stands recorded meanwhile.
  - **Built, and partly proven**: the artifact quickstart (row 5) has its
    tarball assembly, layout and rendered configs proven locally, but its
    bare-container run, image build and compose stack are CI-only until the
    first `-rc` tag — and `release-smoke` runs the **x86_64** tarball only, so
    the aarch64 binaries are built and packaged but executed nowhere until
    somebody runs them on arm hardware. Signing and verification (row 6) are
    written out and identity-pinned but unproven until that same tag: keyless
    signing needs a GitHub OIDC identity the dev box does not have.
  - **Deferred, on the spec's own condition**: the `cargo fmt --check` gate
    (row 13) — two long-lived worktrees are open and the one-shot reformat
    measures 2 731 hunks, every one a conflict in both; the re-run condition
    is written verbatim in the gate doc. `clippy -D warnings` is enforced on
    both the pinned stable and the MSRV floor regardless.
  - **Pending**: the external security review (row 10), which is
    user-scheduled. Row 9 claims that the security package exists and is
    honest — not that the system is secure.
- **Upgrade notes.**
  - **Edit every host's `node.toml` before starting a `2.6.0` node**: add a
    `[crypto]` section (`enabled = true|false`) and an `[admin]` section
    (`auth = "hmac"|"none"`). Without them the daemon refuses to start, by
    name. **The config edit is per-host, not a wire flag day** — the
    node-to-node protocol is unchanged at **0.5.0**
    (`uc_protocol::version::CURRENT`), and nothing about the cnc page or what
    another node sees moves. (The binary swap itself is still run the way
    every upgrade in this system is run: everyone stopped together, per the
    how-to. Do the config edit in that same window — you are touching every
    `node.toml` anyway.) →
    [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
  - **The ~78 MiB instance-directory reservation from `v2.5.0` is
    unchanged**: a node still reserves `buffer_bytes` plus ~14 MiB of rings at
    startup and refuses to start if it cannot. →
    [Instance directory](docs/reference/instance-directory.md#on-disk-footprint)

## v2.5.0 — 2026-08-21 — survivable cluster (M11)

**A cluster you can back up, restore onto a new host, force out of quorum
loss, and upgrade on a measured schedule — each one proven by a test that
destroys something real, not by a documented procedure.** Every gate row
passes: the fleet flag day measured 14.0 s and 14.7 s of downtime against a
60 s bar, and the full-disk row is confirmed independently by CI's sudo
`survival` job against a real loopback filesystem. Full record, including
the honest FAILs along the way and the two product defects they exposed:
[gate record](docs/benchmarks/uc2-m11-gate-2026-08-20.md). Design deep-dive:
[M11 explained](docs/notes/uc2-m11-survivable-cluster-explained.md).

- **Offline backup, verify, and restore** (`uc2ctl backup / verify-backup /
  restore`): safe against a *running* node's purge and snapshot churn via an
  enforced copy ordering, with `verify` asserting the coverage invariant
  instead of trusting the operator. →
  [Back up a cluster](docs/how-to/back-up-a-cluster.md)
- **Quorum-loss recovery** (`uc2ctl force-single-member`): when a majority of
  the cluster is permanently gone, rebuild from one surviving node — offline,
  provably non-persisting until confirmed, with the data-loss window stated
  before anything is written. →
  [Recover from quorum loss](docs/how-to/recover-from-quorum-loss.md)
- **Full-disk fail-stop, observed end-to-end**: a node that hits the disk wall
  halts loudly instead of acking writes it cannot persist, naming the errno
  (`StorageFull` / `os error 28`) so the operator knows to free space; a new
  `uc2_free_disk_bytes` metric and the `Uc2DiskLow` alert give the early
  signal before the wall. Proving this end-to-end exposed two real defects,
  both fixed here: the node's mmapped IPC files were sparse, so a full disk
  killed whichever process (node, service, or client) next touched an
  unbacked page with `SIGBUS` — bypassing the fail-stop path entirely — and
  the journal's segment preallocator discarded the underlying errno, so even
  a correct fail-stop said only "segment preallocation failed". **Operational
  consequence:** those files now reserve their blocks at startup, so a
  default instance dir needs ~78 MiB free before a node will boot, and a node
  that cannot reserve it refuses to start with a named error. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md) ·
  [gate record, row 3b](docs/benchmarks/uc2-m11-gate-2026-08-20.md)
- **Upgrading to this release needs free disk before the node boots.** The
  memory-mapped files in an instance directory now reserve their blocks at
  startup instead of filling in lazily, so a node needs `buffer_bytes` plus
  ~14 MiB of rings free — about 78 MiB at the defaults — and refuses to start
  with a named error if it is not there. Check free space on every host before
  a rolling restart or a flag day. →
  [Run a cluster](docs/how-to/run-a-cluster.md) ·
  [Instance directory](docs/reference/instance-directory.md#on-disk-footprint)
- **Measured flag-day upgrades** (`scripts/uc2_flag_day.sh`): the
  stop-all/upgrade/start-all procedure as a script with preflight refusals,
  an un-upgrade path, and a printed downtime number. →
  [Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
- **Fixed bugs**: four pre-existing journal-layer defects surfaced by the
  backup work's adversarial testing — a healable crash state that refused
  boot, a heal-residue permanent wedge, a masked acked-durability hole at
  segment rolls, and a latent writer panic. →
  [M11 explained §5](docs/notes/uc2-m11-survivable-cluster-explained.md) ·
  [gate record](docs/benchmarks/uc2-m11-gate-2026-08-20.md)

## v2.4.0 — 2026-08-20 — observable cluster (M10)

A running cluster can now be watched, probed, and alerted on without touching
the source — and it costs the hot path ~1.7%.

- **In-daemon observability endpoint**: `GET /metrics` (Prometheus text, 60+
  metric families), `/healthz` (liveness), `/readyz` (role-aware readiness —
  keyed on `can_serve`, so an elected-but-not-yet-serving leader is correctly
  not ready). Zero new dependencies; enabled by the `[metrics]` config
  section, off when absent. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Transition-triggered structured logging** (`[log]` config section): one
  JSON line per state transition — election, truncation, snapshot install,
  config adoption — never one per operation. →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Shipped alert rules and dashboard**: 13 Prometheus alert rules (every one
  proven to fire against a deliberately broken cluster) and a Grafana
  dashboard, under [`packaging/`](packaging). →
  [Monitor a cluster](docs/how-to/monitor-a-cluster.md)
- **Fail-fast daemon**: an internal agent failure now exits the daemon (for
  systemd to restart) instead of lingering as a healthy-looking zombie. →
  [Run a cluster](docs/how-to/run-a-cluster.md)
- **Performance**: the fleet gate measured scrape cost at median 0.983
  on/off throughput ratio (≈1.7%, bar ≥ 0.95) under a 1 s all-nodes scrape,
  with zero false alerts over a 10-minute healthy soak. →
  [M10 gate record](docs/benchmarks/uc2-m10-gate-2026-08-20.md)

## v2.3.0 — 2026-08-19 — deployable node (M9) + rollup

The first tag since v2.1.0, so it ships everything landed in between: the
deployable daemon, wire crypto, a consensus safety fix, the pipelined client,
and the batched read barrier.

- **A real `uc2-node` daemon**: starts from a TOML config file; every config
  mistake is a *named startup refusal* (a typo names the key, a semantic
  error names the rule) instead of a later failure that looks like something
  else. Clean `SIGTERM` drain-and-stop so planned restarts replay a journal
  tail instead of paying reconstruction; packaged systemd units. →
  [Run a cluster](docs/how-to/run-a-cluster.md) ·
  [Configuration reference](docs/reference/configuration.md)
- **Service-binary template**: the shape a user's crate instantiates —
  SIGTERM handling, supervision, the `counter-service` example. →
  [Write a service binary](docs/how-to/write-a-service-binary.md)
- **Wire crypto (M8) — opt-in, off by default**: authenticated + encrypted
  node↔node UDP (Noise `IK` over an X25519 allowlist, AES-256-GCM, a rotating
  group key for the fan-out, anti-replay). A cluster runs all-encrypted or
  all-cleartext — no mixed mode. →
  [Encrypt node traffic](docs/how-to/encrypt-node-traffic.md)
- **Content-attested durable reports (wire protocol 0.5.0)**: a consensus
  safety fix that upgrades commit ranking from a position quorum to a content
  quorum. **Flag day**: upgrade all nodes together — a mixed cluster stalls
  commits rather than committing unsoundly. →
  [the plain-language explainer](docs/notes/uc2-term-map-window-loss-explained.md) ·
  [wire protocol reference](docs/reference/wire-protocol.md)
- **Pipelined client SDK**: `uc2_client`'s public `Engine` (split send/poll
  halves, exactly-once correlation) and `PipelinedClient` with an
  `await`-able `Ticket` per request. →
  [QUICKSTART — beyond one-shot CLI calls](docs/QUICKSTART.md) ·
  [API docs](https://peterknego.github.io/ultima_cluster/)
- **Batched linearizable read barrier**: linearizable reads ride shared probe
  rounds; the barrier's throughput cost fell from ~58% to ~0% at ~953k
  linearizable reads/s. →
  [Read path reference](docs/reference/read-path.md) ·
  [the read-barrier explainer](docs/notes/uc2-read-barrier-explained.md)
- **Fixed bugs**: three consensus-safety windows found by the Lean proof
  effort before any production deployment existed — a Raft Figure-8
  acked-write-loss window in commit ranking, a candidate intake-gate reopen,
  and a boot-open gate phantom commit (Findings #6b, #9, #5). →
  [detailed record](docs/releases.md) ·
  [verification overview](docs/VERIFICATION.md)
- **Performance**: planned leader restart under load stops in 0.042 s and is
  back at baseline ≤ 10.5 s (M9 gate); end-to-end 1.48 M responses/s @ p99
  0.905 ms through the public pipelined client; UC leads Aeron Cluster
  1.3–1.8× on the matched-durability scorecard. →
  [M9 gate](docs/benchmarks/uc2-m9-gate-2026-08-19.md) ·
  [client re-run + A/B](docs/benchmarks/uc2-m5-engine-gate-2026-08-15.md) ·
  [Aeron scorecard](docs/benchmarks/uc2-aeron-parity-2026-08-15.md)

## v2.1.0 — 2026-07-14 — live reconfiguration (M7)

- **Single-server membership changes, live, under load**: promote / demote /
  add / remove one member at a time via the `uc2ctl` admin CLI — no restarts,
  no joint consensus (adjacent configs differ by one member, so majorities
  always intersect). Removed ids are tombstoned forever; a returning host
  rejoins as a fresh id. →
  [Change cluster membership](docs/how-to/change-cluster-membership.md) ·
  [uc2ctl reference](docs/reference/uc2ctl.md)
- **Fixed bugs**: the v2.0.0 MPSC ingress-ring free-space underflow under
  producer contention (spurious backpressure, not corruption). →
  [detailed record](docs/releases.md)
- **Performance**: the 5-host fleet gate held every membership transition's
  commit-rate dip ≤ 4.7% (bar < 10%) with a 3.22 s leader self-removal
  handoff and zero loss or divergence. →
  [M7 gate record](docs/benchmarks/uc2-m7-gate-2026-07-13.md)

## v2.0.0 — 2026-07-13 — the v2 core (M1–M6)

The Aeron-shaped rewrite: UC owns consensus, elections, and transport
directly (the openraft-based v1 is retired).

- **The SMR core**: four single-writer polling agents per node over a
  shared-memory log buffer and control page; replication is a byte-stream
  fan-out over UC's own reliable UDP; monotonic byte positions instead of log
  indices. →
  [Architecture](docs/ARCHITECTURE.md)
- **The end-to-end SDK**: you write a sync, deterministic `StateMachine` in
  your own process; the service SDK applies committed commands and the client
  SDK submits and queries over shared memory. →
  [QUICKSTART](docs/QUICKSTART.md)
- **Leader elections and failover**: automatic, with zero committed-write
  loss across repeated leader kills. →
  [Architecture](docs/ARCHITECTURE.md) ·
  [M4 gate record](docs/benchmarks/uc2-m4-gate-2026-07-11.md)
- **Snapshots, learners, and journal purge** (purge off by default): a node
  below the purge floor converges by snapshot install + tail replay; learners
  replicate without counting toward quorum. →
  [Bound journal growth](docs/how-to/bound-journal-growth.md)
- **Linearizable reads**: a quorum read barrier plus a service-epoch check
  that closes the TOCTOU against a service crashing mid-query. →
  [Read path reference](docs/reference/read-path.md)
- **Performance**: 1.64 M responses/s @ p50 0.600 ms end-to-end (M5 gate,
  pre-pipelined client); learner join under load dipped commit rate 0.9%
  (M6). →
  [M5 gate record](docs/benchmarks/uc2-m5-gate-2026-07-12.md) ·
  [M6 gate record](docs/benchmarks/uc2-m6-gate-2026-07-12.md)
- **Known issue at release**: the MPSC ingress underflow, fixed in v2.1.0. →
  [detailed record](docs/releases.md)
