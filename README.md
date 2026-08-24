# ultima_cluster

An **Aeron-shaped, Rust-native State Machine Replication (SMR) application
server**: Raft-style consensus safety over a shared-memory log buffer, with the
user's deterministic business logic running in a separate process at
memory-channel speed.

## What it is

Ultima_cluster is a State Machine Replication application server. You write a deterministic state machine; it runs your state machine on every node in a cluster, applying the same commands in the same order, and survives node failure without losing acknowledged writes. Replicated command log is what drives changes in user-supplied state machine. Read more about in in the [ARCHITECTURE](https://github.com/PeterKnego/ultima_cluster/blob/main/docs/ARCHITECTURE.md).

## Why?

SMR is used when you need ultimate performance and correctness at the same time. All state is held an manipulated in memory (only occasional snapshots are persisted) and correctness is guaranteed via guaranteed order of commands across all state machines in the cluster.

### High-perf: 1.48 M responses/s at p99 0.905 ms

End to end through the SDK — client submit, consensus, apply, response — with
**every operation quorum-fsync'd before it is acked** and reads linearizable.
p50 0.653 ms, p90 0.757 ms. Measured on 3 × `c6id.2xlarge`, 64 B payloads,
through the **pipelined client** (`uc2_client`'s public `Engine`) that ships
today.

The earlier 1.64 M / p50 0.600 ms headline came from the pre-pipelined client
on an older fleet. A controlled A/B settled which part of the difference is
real: re-running the OLD client on the NEW fleet also measured ~10 % lower, so
most of it is fleet-to-fleet variance, and the pipelined engine's own cost is
about 4 %. What you get in exchange is exactly-once correlation and an
`await`-able ticket per request.
→ [M5 gate record](/docs/benchmarks/uc2-m5-gate-2026-07-12.md) ·
[pipelined-client re-run + A/B](/docs/benchmarks/uc2-m5-engine-gate-2026-08-15.md)

### Leader failover p50 202 ms, zero committed loss in 10 of 10 kills

p90 279 ms, worst 394 ms. Measured on a **4-vCPU sandbox over loopback, not the
fleet** — failover here is timeout-dominated and real NVMe fsync is faster than
the sandbox's ext4, so this is a conservative upper bound; the fleet
detection-timing confirmation is still outstanding.
→ [M4 gate record](/docs/benchmarks/uc2-m4-gate-2026-07-11.md)

---

**Status: `v2.5.0` released; `v2.6.0` (M12) prepared, tag pending** —
milestones M1–M12: the consensus core and SDK (M1–M6), live reconfiguration
(M7), opt-in wire crypto (M8), the deployable `uc2-node` daemon (M9), the
observability layer (M10), backup/restore, quorum-loss recovery, full-disk
fail-stop and measured flag-day upgrades (M11), and — the gateway kit, admin
authentication and audit, signed release artifacts and the security posture
package (M12), of which M12a–c are on `main` and M12d completes the milestone
with this writeup. What each release introduced, with links to the detailed
docs: **[RELEASES.md](/RELEASES.md)**.

| Gate | Result | Measured on |
|---|---|---|
| End-to-end SDK round trip (M5) | **1.48 M responses/s** @ p50 0.653 / p99 0.905 ms (3.7× the ≥400 k bar), pipelined client | 3-host fleet |
| Commit pipeline ceiling (M3) | 2.88 M commits/s @ p50 0.946 / p99 1.132 ms | 3-host fleet |
| Leader failover (M4) | p50 202 ms, p90 279 ms, 10/10 zero committed loss | 4-vCPU sandbox, loopback |
| Learner join under load (M6) | commit-rate dip **0.9 %** (gate < 10 %) | 4-host fleet |
| Below-floor snapshot reconstruction (M6) | worst **2.80 s** across 5 purge cycles, zero read divergence | 4-host fleet |
| Live single-server reconfiguration (M7) | per-transition dip **0.0–4.7 %**, leader self-removal handoff 3.22 s | 5-host fleet |
| Opt-in wire crypto (M8) | **94.1 %** of cleartext throughput | 4-vCPU dev box; ratio only |
| Planned leader restart (M9) | stop **0.042 s** under load, journal-tail rejoin (no snapshot), back at baseline ≤ 10.5 s | 3-host fleet |
| Metrics scrape cost (M10) | median **0.983** scrape-on/off throughput ratio (≈1.7 % cost, bar ≥ 0.95), 1 s all-nodes scrape | 3-host fleet, interleaved A/B |
| Flag-day upgrade downtime (M11) | **14.0 s / 14.7 s** across two runs (bar ≤ 60 s), equal durable on every stopped node, zero committed loss | 4-host fleet |
| Alerts on a healthy cluster (M10) | **0** firing alerts over a 10-min real-Prometheus soak, 62/62 metric families from every node | 3-host fleet |
| Readiness probe under leader kill (M10) | **0** ready-responses during the elected-not-serving window, 3 kills | 3-host fleet |

Fleet runs are `c6id.2xlarge`, us-east-1, single AZ, cluster placement group,
NVMe journals, fsync on. The two non-fleet rows say so rather than borrowing the
fleet's credibility — M4's fleet confirmation and M8's fleet ratio are both open
work. Full per-milestone records live in
[`docs/benchmarks/`](/docs/benchmarks) (`uc2-m*-gate-*.md`).

**Every gate record commits its pass/fail rule to this repository before the
run.** The decide rule and the result are separate commits, in that order; git
history is the audit trail. Records that failed their bar say so and keep the
bar.

## Try it

No toolchain required — download a signed release tarball and run its
quickstart:

> **Not published yet.** `v2.6.0` is written up but not tagged, so there is
> nothing at the download URL today. Until it lands, build from source and use
> `packaging/quickstart-local.sh --bin-dir target/release`, or take the
> one-process version further down.

```bash
tar xzf uc2-2.6.0-x86_64-unknown-linux-gnu.tar.gz
uc2-2.6.0-x86_64-unknown-linux-gnu/packaging/quickstart-local.sh
```

Three `uc2-node` daemons, three services and three gateways come up on this
host, a real election happens, two writes are committed by a majority and a
linearizable read comes back through a gateway from the outside. It prints
`PASS` and cleans up after itself.

Tarballs (x86-64 and aarch64), a `SHA256SUMS`, a CycloneDX SBOM and
`ghcr.io/peterknego/uc2` are published per tag, all signed keylessly by the
release workflow. **[`docs/QUICKSTART.md`](/docs/QUICKSTART.md)** has the
download-and-`cosign verify-blob` step, the annotated configs, and the path
onto real hosts.

From source, the smallest version of the same thing is one process:

```bash
cargo run -p counter --bin counter-single
```

## Shape

```
[client process]  ──shmem──▶  [uc2_node]  ◀──reliable UDP──▶  [uc2_node on peer host]
                                  ▲
                                  │ shmem (file-backed log buffer + cnc page)
                                  ▼
                             [uc2_service]   ← your StateMachine lives here
```

Each node is **four single-writer polling agents** — consensus, sender,
receiver, archive — coordinated only through atomic counters in a 4 KiB
`cnc.dat` page and monotonic byte *positions* (the absolute-offset analog of a
Raft log index). No locks on the hot path; no async in the consensus core.

- **Replication is a byte-stream fan-out** of the log buffer itself over UC's
  own reliable UDP (NAK repair served from the buffer, quorum-paced flow
  control). Consensus is a control plane: coalesced durable-position gossip.
- **Durability** is the archive agent recording the buffer into
  [`ultima_journal`](/ultima_journal) in ≤1 MiB CRC'd blocks, fsync per block.
- **Your code** implements a sync, deterministic `StateMachine`
  (`apply(position, cmd)`, `query`) — optionally `SnapshotStateMachine`
  (enables journal purge) and an async leader-only `OutputHandler`. The apply
  agent polls committed bytes in place; responses reach clients through a
  position-keyed egress broadcast that bypasses the node.
- **Snapshots make purge safe** (off by default): a node below the purge floor
  — crashed service, fresh learner, cold start — converges by snapshot install
  + tail replay, never by reading a purged prefix.
- **Linearizable reads** run a quorum read-barrier plus a service-epoch check
  that closes the TOCTOU against a service crashing mid-query.

## Correctness story

Machine-checked proofs, checked properties, and bug-hunting — kept clearly
separated, because those words mean very different things.

- **Proved (Lean 4, sorry-free, standard axioms only):** the consensus safety
  kernels; `election_safety` and `log_matching` over an N-node protocol model. A
  conformance rig replays 100,000+ vectors of *real Rust output* through the Lean
  model and diffs it bit for bit. `leader_completeness` is reduced to one named
  obligation and remains **open**.
- **Checked:** nine whole-cluster invariants under seeded fault fuzz (`uc2_sim`);
  WGL linearizability under leader kills, partitions, and purge; Elle
  transactional safety under both the serializable and strict real-time models,
  with a mutation tier proving the harness can actually fail; multi-process
  `SIGKILL` recovery; a `loom` model of frame visibility.
- **Bug-hunted only:** Veil bounded model checking — deliberately excluded from
  the trust story and never the proof of record.

The proof effort has found and fixed **four real, shipped safety bugs** that the
fuzz and crash tiers had missed, two of them acked-write-loss class.

**→ [`docs/VERIFICATION.md`](/docs/VERIFICATION.md)** for the full picture,
including what is *not* verified and how to reproduce every layer.

## Workspace

| Crate | Role |
|---|---|
| `uc_protocol` | Wire spec: cnc page layout, datagram/frame formats, lock-free rings (SPSC/MPSC/broadcast) |
| `uc2_log` | Shared log buffer + archive agent (journal recording, snapshots, purge floor) |
| `uc2_net` | Reliable-UDP sender/receiver agents, NAK repair, flow control, snapshot sessions |
| `uc2_consensus` | Pure-sync safety core: commit tracker, elections, term maps, truncation |
| `uc2_crypto` | Opt-in node-to-node wire crypto (M8, off by default): Noise `IK` handshake, AES-256-GCM over the datagram envelope, rotating group key, anti-replay; plus the M12b admin-request HMAC |
| `uc2_sim` | Deterministic simulation + invariants + fuzz |
| `uc2_node` | The node: agents wired together, IPC surface, read barrier, gate harnesses |
| `uc2_service` | Service SDK: `StateMachine` traits, apply agent, reconstruction; optional [`ultima-db`](https://crates.io/crates/ultima-db) store adapter (feature `ultima_db`) |
| `uc2_client` | Client SDK in three tiers: the pipelined `Engine` (split send/poll halves, exactly-once slot correlation), `PipelinedClient` + `Ticket` (`wait()` or `.await`), and a blocking `Client` shim. Submit, linearizable/snapshot queries |
| `uc2_remote` | The remote wire protocol (framed TCP, credit-gated flow control) and `RemoteClient`, its pipelined redirect-following Rust implementation — for clients that can't attach to shmem directly |
| `uc2_gateway` | The `Edge`: a per-node TCP front door relaying `uc2_remote` traffic over the local `Engine`; ships as the `uc2-gateway` binary + `gateway.toml` |
| `uc-lincheck` | WGL linearizability checker + history recorder + register model |
| `ultima_journal` | Segmented append journal + atomic `StableValue`s |

Builds standalone — the only external storage dep, `ultima-db`, comes from
crates.io.

**To crates.io, first published by the `v2.6.0` tag:** twelve crates,
prepared and gated in CI, published **in lockstep at one version** — which is also the git tag, the tarball name and the image
tag. That is the thirteen crates in the table above, minus `uc2_sim` and
`uc-lincheck`, plus `uc2ctl` (the admin CLI: a binary crate, so it has no row
here). `uc2_sim`, `uc-lincheck` and the two example crates are
`publish = false`: proof and teaching apparatus, not product. What that
version number promises, and what it deliberately does not, is
[the semver policy](/docs/reference/semver-policy.md).

## Build & test

**MSRV is Rust 1.89** (`rust-version` in `[workspace.package]`; the floor is
`std::fs::File::try_lock_exclusive`). `rust-toolchain.toml` pins a newer
stable — currently **1.96.0** — for this repository's own builds and for
releases; rustup installs it on the first `cargo` invocation. CI's `msrv` job
proves the floor separately by running clippy on a real 1.89.0 toolchain.

```bash
cargo build --workspace
cargo test --workspace                                    # includes the lincheck capstones
cargo clippy --workspace --all-targets -- -D warnings     # the lint gate
cargo test -p uc2_sim --features sim-heavy                # 1000-seed fuzz tier
cargo test -p uc2-crashtest --features hard-crash-tests   # multi-process SIGKILL
RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
```

`ci.yml` runs the fast gate on every PR (plus `msrv`, `cargo-deny` and a
`publish-check` that packages all twelve publishable crates); `nightly.yml`
runs the full proof suite (capstones, sim-heavy, loom, crashtest, and the
binary quickstart); `release.yml` builds, smoke-tests and signs the artifacts
on a tag — see [Cut a release](/docs/how-to/cut-a-release.md).

## Documentation

Start here:

- **[RELEASES.md](/RELEASES.md)** — what each release introduced, feature by
  feature, with links to the detailed doc for each.
- **[API documentation](https://peterknego.github.io/ultima_cluster/)** — rustdoc
  for every library crate, rebuilt on each push to `main`.
- **[`docs/QUICKSTART.md`](/docs/QUICKSTART.md)** — zero to a running three-node
  cluster, with the state machine you write shown in full. Every command and
  output is from a real run.
- **[`docs/ARCHITECTURE.md`](/docs/ARCHITECTURE.md)** — how it works: positions
  instead of indices, the four agents, the data and control planes, the apply
  path. Written for someone who knows Raft and has not read the specs.
- **[`docs/VERIFICATION.md`](/docs/VERIFICATION.md)** — what is proved, what is
  checked, what is only bug-hunted, and how to reproduce each.
- **[`docs/BENCHMARKS.md`](/docs/BENCHMARKS.md)** — every measured result, what
  it was measured on, and the read-barrier arc.
- **[`docs/how-to/`](/docs/how-to)** — task guides for running a cluster:
  getting nodes onto real hosts, changing membership live, encrypting node
  traffic, bounding journal growth, diagnosing a node, investigating a red
  correctness run, reproducing a published result.
- **[`docs/reference/`](/docs/reference)** — the surfaces those tasks drive:
  `uc2ctl`, the instance directory, the cnc control page, configuration and
  environment switches, the wire protocol, the linearizable read path. The
  library API is the rustdoc above, not here.
- **[`docs/security/`](/docs/security)** — the review-ready security package:
  the [threat model](/docs/security/threat-model.md) (assets, actors, trust
  boundaries, what is out of model), the
  [attack surface](/docs/security/attack-surface.md) (every parser, its guards,
  its fuzz target, bind-address guidance), and the
  [self-assessment](/docs/security/self-assessment.md) (what was found, what is
  accepted, where an external reviewer should look). Reporting:
  [`SECURITY.md`](/SECURITY.md).
- **[`docs/ops/uc2-runbook.md`](/docs/ops/uc2-runbook.md)** — the operations
  landing page; indexes the two directories above.

Reference:

- **Design specs (canonical):** [`docs/superpowers/specs/`](/docs/superpowers/specs) — [the v2 core design](/docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md) (M1–M6), [reconfiguration](/docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md) (M7), [wire crypto](/docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md) (M8)
- **Raw gate records:** [`docs/benchmarks/`](/docs/benchmarks) — the dated
  originals behind [`BENCHMARKS.md`](/docs/BENCHMARKS.md), including the
  correctness gates (elle, lean, veil)
- **v1 history:** [`docs/tasks/`](/docs/tasks) — the retired openraft-based v1 stack's consolidated record (kept as the negative-results archive that shaped v2)

Bench/fleet tooling lives under [`bench-infra/`](/bench-infra) (terraform +
ansible + the fleet-gate orchestrator; refuses to run journal-bearing gates on
RAM-backed filesystems).

## Security posture

**Wire crypto is opt-in and off by default.** With `[crypto].enabled = false`,
node-to-node UDP is cleartext and its source is unauthenticated: the
`uc_protocol::v2` decoders parse untrusted bytes with nothing in front of them.
That is why they are total on `&[u8]` and fuzzed nightly — fourteen
`cargo-fuzz` targets, 600 s each, with an asserted execution floor
([VERIFICATION §7](/docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes)).

With crypto **on**, the threat model is a network-path adversary: Noise `IK`
over an allowlist of X25519 statics, AES-256-GCM with the 16-byte header
authenticated as AAD, RFC-6479 anti-replay. **Out of model:** a compromised
host, and a malicious cluster member — the group key is symmetric, so any
holder can forge fan-out traffic as any node.

Also true, and stated rather than implied:

- the remote client link is **plain TCP with no client authentication** in this
  release, and `/metrics`, `/healthz`, `/readyz` are unauthenticated — the bind
  address is the control;
- **`app_id` is a wrong-cluster guard, not a credential**;
- admin operations need an HMAC key unless the operator chose
  `[admin] auth = "none"`, and that signature authenticates cluster-wide only
  when paired with `[crypto].enabled = true`.

[Threat model](/docs/security/threat-model.md) ·
[attack surface](/docs/security/attack-surface.md) ·
[self-assessment](/docs/security/self-assessment.md) ·
[reporting a vulnerability](/SECURITY.md).

## Scope and limits

What is shipped, release by release — each feature with a link to its
detailed doc — lives in **[RELEASES.md](/RELEASES.md)**. The standing limits:

- **8 total members**, a hard cap (voters + learners share the eight cnc peer
  slots).
- **One node per instance directory**, enforced by an exclusive flock.
- **One state machine per cluster** — one leader, one apply thread. No
  sharding, no dynamic loading.
- **Command payload ≤ 1344 bytes** (crypto off) or **≤ 1312 bytes** (crypto
  on). A command travels in one datagram; the node does not fragment frames,
  and `MTU_DEFAULT = 1408` is not configurable.[^mtu]
- **Clients attach over shmem on a node host, or over TCP through a
  `uc2-gateway`** on a node host. There is no remote ingress inside the node.
- **A gateway's flow control is per-connection only.** Credits are granted and
  shrunk per connection; nothing bounds the *sum* across connections against
  the node's ingress admission window (`admission_bytes`), and past that sum
  the edge collapses rather than degrading — a confirmed defect, fix planned.
  Until then, keep total client inflight per edge under the admission window
  and bound the co-located gateway's CPU:
  [Operating envelope](/docs/how-to/run-a-gateway.md#operating-envelope-260) ·
  [gate record](/docs/benchmarks/uc2-m12-gate-2026-08-22.md#clean-discipline-re-run-same-day-the-collapse-is-a-product-defect-not-a-harness-artifact).
- **Wire crypto and journal purge are off by default**; `[crypto]` and
  `[admin]` are explicit choices a `node.toml` must make or the node refuses to
  start. Admin operations require a signed HMAC request unless
  `[admin] auth = "none"` — see
  [Configuration: Admin authentication](/docs/reference/configuration.md#admin-authentication).
- **All published fleet measurements are single-AZ.** They are reproducible on
  the hardware each record names, not universal.

[^mtu]: `uc2_node::preflight::check_semantics` computes the ceiling and refuses
    to start with `PayloadExceedsMtu` above it:
    `align32(max_payload + HEADER_LEN) + DATAGRAM_HEADER_LEN + crypto ≤ MTU_DEFAULT`,
    i.e. `align32(p + 32) + 16 [+ 24] ≤ 1408`. The 32-byte frame alignment is
    what makes the answer 1344 rather than 1360, and 1312 rather than 1336.

## License

Apache-2.0
