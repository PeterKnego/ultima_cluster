# Limits

Every hard limit, standing constraint and accepted residual of
`ultima_cluster`, in one place. Each row points at the document that owns
it — that document is the authority; this page is the index. Numbers here
are quoted from those pages and from the constants they name, not
re-measured.

This page describes `main`. Where a limit changed between releases, the row
says so.

## Hard limits

Fixed by the wire, the control page or the instance-directory layout. None of
these is a configuration knob.

| Limit | Value | Where it comes from |
|---|---|---|
| Members per cluster (voters + learners) | **8** | the cnc page's peer-slot band; enforced on the wire too — [Configuration § Cluster limits](configuration.md#cluster-limits), [Change cluster membership](../how-to/change-cluster-membership.md) |
| Membership changes in flight | **1** | single-server change rule — [Configuration § Cluster limits](configuration.md#cluster-limits) |
| State machines (FSMs) per cluster | **8**, declared by name in `[services] names`, row = list index (contiguous from 0) | the cnc page's per-service band (2.8.0) — [Configuration § `[services]`](configuration.md#services) |
| FSM names | `1..=32` bytes of lowercase ASCII letters, digits, `_`, `-`, starting with a letter; no duplicates in one node's list | FSM identity (2.11 pending) — the bound lets the name sit verbatim in one cnc slot line, and the alphabet keeps it a valid metric label value — [Configuration § `[services]`](configuration.md#services), [the FSM identity explainer](../notes/uc2-fsm-identity-and-deterministic-ids-explained.md) |
| One FSM *type* per row | Identity is declared once, on the state-machine type (`const NAME`); a harness that wants the same type at several rows wraps it in `uc_service::Tagged<const ROW: u8, S>` — a production deployment never needs this, since two rows running the same logic on one log compute the same state twice | FSM identity (2.11 pending) §3.4 |
| FSMs the remote path can reach | **row 0 only** | a gateway submits to and queries the default responder (M14); row 0's *name* is now checked cluster-wide on the snapshot path (FSM identity, 2.11 pending) — [Configuration § `[services]`](configuration.md#services) |
| Command payload, wire crypto off | **≤ 1344 B** | `MTU_DEFAULT = 1408` minus the 32 B frame header (32-aligned) and the 16 B datagram header — [Remote protocol § payload](remote-protocol.md), [State-machine contract § Payload ceiling](state-machine-contract.md#payload-ceiling) |
| Command payload, wire crypto on | **≤ 1312 B** | the same budget less `CRYPTO_OVERHEAD` = 24 B (8 B counter + 16 B AES-GCM tag, `uc_protocol::v2::crypto`) |
| Datagram MTU | **1408 B**, not configurable | `uc_protocol::v2::datagram::MTU_DEFAULT` — [Wire protocol](wire-protocol.md) |
| Command chunking | **none** — a command that does not fit one datagram is refused (`PayloadExceedsMtu` at preflight, `PAYLOAD_TOO_LARGE` at the edge), never split | [Remote protocol § payload](remote-protocol.md) |
| Nodes per instance directory | **1** | `instance.lock`, exclusive `flock` — [Instance directory § Limits](instance-directory.md#limits) |
| Admin clients per instance directory | **1** at a time | [Instance directory § Limits](instance-directory.md#limits) |
| Disk reserved at boot | `buffer_bytes` + 15 MiB of rings + 6 MiB × (N − 1) FSMs + 4 KiB: **~79 MiB** at the defaults with one FSM, **~121 MiB** with eight (was 14 MiB + 5 MiB × (N − 1) before `svc_sched.<id>.ring`, 2.11 pending) | `fallocate`d, not sparse, so a full disk is a named startup refusal instead of a `SIGBUS` mid-run — [Instance directory § On-disk footprint](instance-directory.md#on-disk-footprint) |
| Timers fired per leader pass | **64** (`TIMERS_PER_PASS`, a source constant, no knob). At the bound the pass appends **no** client frames at all, so the ordering guarantee holds and clients are backpressured for one pass | log time and timers (2.11 pending) — [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md) |
| Pending timer instances per `(FSM, timer id)` | **1** — scheduling an id that is already pending replaces its deadline; a *scheduled instance* is the triple `(FSM identity, id, deadline)` | log time and timers (2.11 pending), spec §4.2 |
| Timer payload | **none** — a `TIMER` frame carries `(identity hash, id, deadline)` and nothing else; an FSM keeps a timer's context in its own state, keyed by the id | log time and timers (2.11 pending) — [Wire protocol § `TIMER` body](wire-protocol.md#timer-body-wire-070) |
| Entries in the replicated schedule table | **32** (`MAX_SCHEDULE_ENTRIES`, a source constant, no knob), across every FSM. A full table is `8 + 32 × 33` = **1064 B**, which is why it always fits one datagram | the schedule table (2.11 pending) — [Wire protocol § `SCHEDULE_TABLE` body](wire-protocol.md#schedule_table-body-wire-070) |
| Schedule rule kinds | **three**: `every` (a period from an anchor), `at` (daily, **UTC**) and `once` (one fixed deadline, after which the entry **parks** — it stays in the table with no next deadline, so re-applying the same file does not re-fire it). No timezones, no cron syntax; a cron-shaped rule would be a fourth kind byte | the schedule table (2.11 pending) — [Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table) |
| Schedule tables in flight | **1** — the leader answers `retry` (status 2) while the previous table frame is still above commit, which is what makes one level of `ScheduleRecord.prev` sufficient to revert a truncated table | the schedule table (2.11 pending), spec §5 |
| Schedule-table catch-up after downtime | **one tick per entry**, not a backlog. A due entry fires at the *latest* occurrence at or before the log's clock, so a cluster down for an hour with a one-second rule fires one tick on recovery and continues from it | the schedule table (2.11 pending) — [Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table) |
| Linearizable-read rounds in flight | **1** per leader, cluster-wide; per-read deadline **1 s** | [Linearizable read path](read-path.md) |
| Driver threads per gateway edge | **1**, writing every connection's responses in completion order | [Run a gateway § The single-driver head-of-line caveat](../how-to/run-a-gateway.md#the-single-driver-head-of-line-caveat) |
| Session (exactly-once) table | bounded by the replicated `SessionConfig`: `window` responses per client, `max_clients`, `max_bytes` (default **256 MiB**) — beyond them, whole clients are evicted and their retries answer `EXPIRED` | [State-machine contract § `Sessioned`](state-machine-contract.md) |

## Standing constraints

Design decisions that bind an operator. Each is deliberate and is not
expected to change.

| Constraint | What it means for you | Where |
|---|---|---|
| **Linux only**, x86-64 or aarch64 | The IPC layer is file-backed shared memory plus futex wakeups. Release tarballs are built on native runners for both architectures; the release smoke test unpacks only the x86-64 tarball (`.github/workflows/release.yml`). | [Quickstart](../QUICKSTART.md) |
| **The node↔node wire and the cnc page are flag days, never semver** | Every node in a cluster stops and restarts on the new version together; mixed-version operation is not supported. A mixed cluster is designed to *stall* rather than commit unsoundly (a `0.4.0` peer's durable report reads as unattested and is not counted). `2.7.0` shipped wire `0.5.0`; `2.8.0` moves it to `0.6.0` (`SNAP_BEGIN` grew), a whole-cluster restart on the same terms. Wire `0.7.0` and cnc `3.1` are implemented but unreleased (2.11 pending): **one combined flag day** carrying FSM identity's `SNAP_BEGIN` change, the relaid log frame header with its `time_ns` stamp, and the new `TIMER` frame type. | [Versioning and the semver promise § flag-day](semver-policy.md#the-wire-and-the-cnc-page-are-flag-day-not-semver), [Upgrade a cluster](../how-to/upgrade-a-cluster.md) |
| **One membership change at a time** | `add-learner` / `promote` / `demote` / `remove-learner` are serialized; a second proposal while one is in flight is refused by name. A fresh learner catching up by log replay alone can be outrun indefinitely under sustained writes — enable snapshots + purge first. | [Change cluster membership](../how-to/change-cluster-membership.md) |
| **Wire crypto is opt-in, OFF by default, and all-or-nothing per cluster** | No mixed cleartext/encrypted mode. Turning it on or off is itself a flag day. `[crypto]` must be present in `node.toml` either way — an absent section refuses to start rather than silently running cleartext. | [Encrypt node traffic](../how-to/encrypt-node-traffic.md), [Configuration](configuration.md) |
| **`[admin]` must be present**, and `auth = "hmac"` is cluster-wide only with `[crypto].enabled = true` | A follower forwards an authenticated admin request to the leader over the node↔node socket, which the leader cannot re-verify; without wire crypto that hop is unauthenticated. | [Configuration § Admin authentication](configuration.md#admin-authentication), [Threat model § 6](../security/threat-model.md#6-residuals-stated-elsewhere-and-where) |
| **Purge is OFF by default** (`PurgePolicy::Disabled`) | The journal grows without bound until you enable `[purge]` and your state machine implements `SnapshotStateMachine`. | [Bound journal growth](../how-to/bound-journal-growth.md) |
| **A node below the purge floor converges only by snapshot install + tail replay** | A crashed-and-restarted service, a fresh learner or a cold node never reads the purged prefix; `NoCommonPrefix` means wipe-and-rejoin. | [Bound journal growth § below the floor](../how-to/bound-journal-growth.md#what-happens-to-a-node-that-falls-below-the-floor) |
| **`/metrics`, `/healthz`, `/readyz` exist only when `[metrics]` is configured** | Readiness keys on `can_serve`, never on the leader flag; the peer-slot metric band is leader-authoritative (followers export zeros). The endpoint is unauthenticated and read-only. | [Monitor a cluster](../how-to/monitor-a-cluster.md), [Configuration](configuration.md) |
| **Your state machine's determinism is your responsibility** | SMR replicates bytes and guarantees identical order on every replica. A *host* clock, a random source, float rounding, hash-map iteration order or any ambient state inside `apply` produces divergence that no layer here can detect. Since 2.11 (pending) there **is** a deterministic clock: `ctx.time_ns`, the leader's stamp on the frame being applied, identical on every replica. `apply` is sync and does no I/O by construction; `OutputHandler` side effects are leader-only and at-least-once. | [State-machine contract](state-machine-contract.md), [VERIFICATION § 11](../VERIFICATION.md#11-what-is-not-verified) |
| **The remote (client↔gateway) link is plain TCP** | No TLS and no client authentication. Reachability is authorization: keep the port private or front it with a proxy. | [Remote protocol](remote-protocol.md), [Self-assessment § 3](../security/self-assessment.md#3-known-weaknesses-not-fixed) |
| **`SessionConfig` is replicated** | Every replica must run identical `window`/`max_clients`/`max_bytes`; changing them is a coordinated change, not a per-host edit. | [State-machine contract § `Sessioned`](state-machine-contract.md) |

## Accepted residuals and known weaknesses

Each is a decision with a stated reason, documented rather than fixed. The
security ones are owned by the threat model and the self-assessment; the
rest by the page named.

| Residual | Where the reasoning lives |
|---|---|
| Cleartext headers leak metadata (positions, rates, terms, message kinds) to a wire observer even with crypto on | [Threat model § 6](../security/threat-model.md#6-residuals-stated-elsewhere-and-where), residual 1 |
| A removed node keeps decryption ability until the next group-key rotation | [Threat model § 6](../security/threat-model.md#6-residuals-stated-elsewhere-and-where), residual 2 |
| Any group-key holder can forge fan-out traffic as any node (the fan-out key is symmetric) | [Threat model § 6](../security/threat-model.md#6-residuals-stated-elsewhere-and-where), residual 3 |
| No compromised-host story: the identity key file *is* the credential | [Threat model § 6](../security/threat-model.md#6-residuals-stated-elsewhere-and-where), residual 4 |
| A malformed `QUERY` frame from an unauthenticated remote client fail-stops a **typed-tier** service pre-commit; the raw tier is the workaround | [Self-assessment § 3](../security/self-assessment.md#3-known-weaknesses-not-fixed), [Attack surface](../security/attack-surface.md) |
| The typed tier decodes with `bincode` `NoLimit`; the bounds are the payload cap and serde's 1 MiB pre-allocation cap | [Self-assessment § 3](../security/self-assessment.md#3-known-weaknesses-not-fixed) |
| `bincode` is unmaintained (RUSTSEC-2025-0141); replacing it is a wire-format migration | [Self-assessment § 3](../security/self-assessment.md#3-known-weaknesses-not-fixed), `deny.toml` |
| One gateway's single driver thread means a wedged client can stall every other client on that edge for up to the 1 s write timeout; others may then see `UNKNOWN` and must resend (safe with the session envelope on) | [Run a gateway § head-of-line caveat](../how-to/run-a-gateway.md#the-single-driver-head-of-line-caveat) |
| Lockstep multi-FSM mode (`fsm_lag = "lockstep"`, M14) costs an N-way cross-core handshake per frame, and a stalled sibling makes every other FSM burn a core yielding on it | [Configuration § `[services]`](configuration.md#services) |
| Timers are **at-least-once** at the node layer: an in-flight instance is re-armed on any leadership loss, so the next leader may fire it again. `uc_service::Timed<S>` makes delivery exactly-once per instance; a state machine that skips the wrapper accepts duplicates, the same trade as running without `Sessioned` | [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md), spec §4.5–§4.6 |
| No per-timer precision guarantee. The contract is "never early; on time or marked late", and lateness after a leader change is a designed-for case (`ev.late(ctx)` reports it), not a defect. The gate *measures* the distribution rather than promising a number | [Time-and-timers gate](../benchmarks/uc2-time-and-timers-gate-2026-09-03.md), spec §10 |
| Leader clock discipline is the operator's. A backward step is clamped (the log's time freezes until wall time catches up) and alerted (`Uc2LogTimeFrozen`); a forward step is not detectable in-band and fires every timer in between. NTP is the answer, as it is in Aeron | [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md#failure-modes) |
| The schedule table **is** carried on the snapshot session since `2.11.0` (`SNAP_TABLE`, kind 21, after every `SNAP_BEGIN`), so a below-floor joiner installs the leader's table before it can serve or lead. Two narrow windows remain: a node **restarted** but not yet past its first commit advance **under-ships** — its one-level predecessor, or nothing — because the cnc commit counter is not primed at boot, which is the safe direction but can leave a joiner served in that window with an older table, or none until the next `uc2ctl schedule apply`; and a node whose newest shippable record sits at position `0` — a **wiped** node, which keeps its table armed locally by design, or one holding the canonical "no table" record — ships `(0, 0, [])`, so a joiner it serves installs **no table** until the next `uc2ctl schedule apply` or the next table frame. Position `0` means the table is unanchored in the log, so the wipe keep-alive is deliberately local-only and does not propagate by snapshot | [Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#known-limits-of-the-table), [`docs/BACKLOG.md` § 2a](../BACKLOG.md) |
| A node that crashes in the sub-millisecond window between the archive recording a table frame and the consensus agent persisting `state/schedules.state` loses that adoption: there is no journal re-scan for type-6 frames on the recovery path. The remedy is the same one the alert names — re-apply the table | [Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#known-limits-of-the-table) |
| Boot arming has no delivered set until the service announces, so a restarted node may re-append the latest occurrence of every table entry once, a parked `once` included. `uc_service::Timed<S>` drops it; a state machine without the wrapper sees the duplicate, the same at-least-once trade programmatic timers already carry | [Log time and timers, explained § The schedule table](../notes/uc2-log-time-and-timers-explained.md#known-limits-of-the-table) |
| Lockstep needs a free CPU per declared FSM plus the node's own agents: once the runnable set exceeds the available CPUs it collapses by up to **~880×** (624 k → **709** frames/s per FSM at N=2 with 3 busy threads on 1 CPU), while bounded mode on the identical rung is unaffected at 7.4 M frames/s. An envelope fact, not a defect — lengthening the yield ladder ×4/×16, and making it unbounded, both measured 1.00× | [M14c2 lockstep-under-oversubscription record](../benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md) |

## What is not verified

The proof surface has named holes; they are listed in one place and not
repeated here:

- **[VERIFICATION § 11 — What is *not* verified](../VERIFICATION.md#11-what-is-not-verified)**
  — the open `leader_completeness` theorem, the durable dual-reader model
  gap, the IPC rings that Miri cannot reach (only the MPSC ring has a loom
  model), what fuzzing does and does not prove, and that the published
  numbers are fleet measurements on named hardware, not universal.

## Where the rest is

- **Per-release known issues** — each section of [`RELEASES.md`](../../RELEASES.md)
  and [`docs/releases.md`](../releases.md) carries the issues known at that
  release; a "known limits" bullet there is historical once a later release
  fixes it (the `2.6.0` per-connection gateway collapse, fixed in `2.7.0`, is
  the standing example).
- **Per-gate caveats** — every gate doc under [`docs/benchmarks/`](../benchmarks/)
  records its own accepted residuals and standing caveats beside the numbers.
- **Configuration refusals** — the full list of things `uc2-node` refuses at
  startup, by name, is [Configuration § Startup refusals](configuration.md#startup-refusals).
