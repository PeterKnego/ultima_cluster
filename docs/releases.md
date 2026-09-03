# ultima_cluster releases

## Unreleased — FSM identity and log time (next minor, 2.11.0 when cut)

**Implemented on branch `uc2/fsm-identity`; not tagged. Release on hold** —
more changes are planned on this branch first. This entry is a draft written
ahead of the tag, per the standing writeup rule (CLAUDE.md), so the record
is ready when the maintainer green-lights it.

**Two features, one flag day.** Both were implemented before the release was
cut, and both move the wire to `0.7.0` and the cnc page to `3.1`, so they
ship together:

| feature | spec | plan |
|---|---|---|
| FSM identity | `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` | `docs/superpowers/plans/2026-09-02-uc2-fsm-identity.md` (T0–T10, all done) |
| Log time and timers, plan 1 | `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` | `docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan1.md` (T0–T14, all done) |

Plan 2 of the time spec (the replicated schedule table, §5) is **not** in
this release; it is the next item under that feature in `docs/BACKLOG.md`.

### The problem this closes

M14 (`v2.8.0`) identified an FSM by a bare row number: the service declared
it in `ServiceConfig::service_id`, the node declared the set in `[services]
ids`, and nothing anywhere checked what *logic* a number held. Two nodes
agreeing on the set of numbers never agreed on the code behind each one —
same logic at different numbers stalled a joiner with an unexplained
declared-set mismatch, and different logic at the same number diverged
**silently**, with no refusal and no alert.

### What changed

- **Identity moves into code.** `RawStateMachine`/`StateMachine` gain a
  required `const NAME: &'static str` (1..=32 bytes, lowercase ASCII +
  `_`/`-`, starting with a letter — `uc_protocol::identity`, a new
  `core`-only leaf module alongside `version`/`magic`) and an optional
  `const VERSION: u32` (packed semver, `0` = unversioned).
  `uc_service::ServiceConfig` **loses** `.service_id()`; a service attaches
  by scanning the node's declared name lines for its own `NAME`
  (`ServiceError::UnknownFsm { name, declared }` on no match).
- **`[services]` becomes required, and is now `names`, not `ids`.**
  `node.toml` without `[services]` refuses to start
  (`ConfigError::ServicesChoiceRequired`) — the same explicit-choice
  posture `[crypto]`/`[admin]` have had since 2.6.0. The old `ids` key is
  refused by name, pointing at `names`; row = list index, still contiguous
  from 0, still cluster-wide-required to match. `ServicesConfig::from_names`
  carries the refusal set (empty, > 8, invalid name, duplicate);
  `ServicesConfig::single`/`tagged`/`from_cli`/`none_for_tests` are the
  programmatic/harness forms.
- **`ApplyCtx` replaces the bare `position: u64` apply parameter.**
  `apply(&mut self, ctx: &mut ApplyCtx, cmd, out)`; `ApplyCtx` is
  `#[non_exhaustive]` and carries `position` plus the FSM's identity, built
  once per frame by the apply loop, journal replay and snapshot
  tail-replay. `#[non_exhaustive]` is deliberate: the parallel leader-
  timestamps/scheduler design (its own future wire release) adds fields
  here later without a second trait-signature break. `Sessioned<S>`
  forwards `NAME`/`VERSION` and passes `ctx` straight through.
- **`uc_service::ids::IdGen`** — `ctx.ids()` mints deterministic `u128` IDs
  from `position ‖ ordinal:u32 ‖ fold32(identity.hash)` through a frozen
  three-round Feistel permutation (murmur3 `fmix64` round function, golden
  vectors pinned). The ordinal resets to zero every apply call — this, not
  the permutation, is the actual correctness property: a snapshot-installed
  replica and a journal-replayed one mint the identical series for the
  identical future command, with zero state to snapshot. `IdGen` is
  `!Send`, reachable only via `ApplyCtx::ids()`, unreachable from `query` by
  construction (no context there to build one from).
- **cnc 3.1** (`CNC_V2_VERSION = (3 << 24) | (1 << 16)`): the once-reserved
  per-slot line 7 becomes node-written at boot — `name` (`[u8; 32]`,
  NUL-padded, offset `+448`) and `identity_hash` (`u64` FNV-1a 64, offset
  `+480`) — the one line in the service-slot band written by the **node**,
  not the service. The status line's second word (offset `+8`) becomes the
  attached service's packed version, service-written at attach.
- **Wire `0.7.0`**: `SnapBeginBody`'s `services_declared: u64` bitmask is
  replaced by `identity: [u64; 8]` (per-row identity hash, `0` = row
  undeclared — the mask is now derived) and a new `version: [u32; 8]`
  (per-row packed version, `0` = unknown). `SNAP_BEGIN_FIXED_LEN` grows
  34 → 122. The receiver's check is **positional**: `identity[r]` must
  equal the receiver's own row-`r` hash for every `r` — same names in a
  different order now fails too, where the old bitmask compare would have
  silently accepted it. On mismatch, refused **by name** ("row 1:
  ours=orders, theirs=kv" — a hash the receiver recognizes anywhere in its
  own list prints as that name), counted in the existing
  `uc2_snapshot_refused_declared_set_total`. `version` is compared per row
  only when both sides are non-zero; a mismatch refuses by name with both
  versions, counted in a new `uc2_snapshot_refused_version_total`. A 0.6.0
  sender's 34-byte body is shorter than 122, so it is dropped by the same
  length check that has dropped every prior mismatched wire version — the
  standing flag-day rule, unchanged: a mixed cluster stalls a joiner rather
  than installing a wrong or half-checked artifact.
  `Receiver::set_snapshot_intake` grows a fourth parameter — the node's own
  per-row identity array plus a closure the receiver calls for the node's
  own per-row versions (read fresh off the cnc slot band; `uc_net` still has
  no cnc dependency of its own).
- **Observability**: metric labels gain the name (`service="<name>",
  row="<r>"`, alongside the existing `row` grouping key); two new gauges,
  `uc2_service_identity_hash` and `uc2_service_version`, feed two new alert
  rules, `Uc2ServiceIdentityDrift` (critical) and `Uc2ServiceVersionDrift`
  (warning) — a cross-node config or version mismatch now pages in steady
  state, before any snapshot session ever runs, which is the **early**
  guard for the **late** `SNAP_BEGIN` check. `uc2ctl status` prints `row=
  name= version= hash=` per row (`uc_ctl/src/main.rs`).
- **Harnesses and capstones run by name**: `uc_service::Tagged<const ROW:
  u8, S>` (zero-cost, `NAME = "fsm{ROW}"`) is the one-type-at-several-rows
  wrapper for `apply_bench`, the two-FSM lincheck capstones and
  `m12_gate`'s fleet rows; `m14_fleet_gate.py`'s `fsm_name`/`node_args`/
  `service_args` translate `(row, spin)` to names at the CLI-argv boundary;
  `m12_fleet_gate.py`'s node role gained a previously-missing `--services`
  flag (unconditionally required since Task 4, an unrelated pre-existing
  gap this work's scope surfaced and fixed). Two new negative scenarios in
  `uc_node/tests/learner.rs`: same names in the other order (refused by
  name, `RefusalKind::Identity`) and same names with differing attached
  versions (refused with both versions, `RefusalKind::Version`).

### Log time and timers, plan 1

**The problem it closes.** A state machine may not read a clock (the apply
contract: "no clock, no randomness"), and until now it had no substitute. The
log frame header carried no time, `ApplyCtx` carried only the position and the
identity, and nothing in the shipped stack needed one — the session layer's
`EXPIRED` is a sequence window, not a TTL. A TTL, a rate window, an expiry, or
"do X at 14:00" was therefore impossible to express deterministically. This
puts time **on the log**, where every replica reads it identically, and builds
one thing on it: a scheduler whose fired timers are frames on the same log,
placed so the timestamp series stays in order across them. Plain-language
explainer: `docs/notes/uc2-log-time-and-timers-explained.md`.

- **The frame header is relaid, not grown.** Through `0.6.0` the 32-byte
  header carried `session_id: u64` and `correlation_id: u64`, of which the
  client only ever filled 32 bits each (the shmem ingress record packs
  `client_id: u32 ‖ local_seq: u32` into one word and the leader widened
  them). They become `client_id: u32` at offset 12, `seq: u32` at 16, four
  reserved bytes at 20, and **`time_ns: u64` at 24**. `HEADER_LEN`,
  `FRAME_ALIGNMENT`, `MTU_DEFAULT` and the payload ceiling (1344 / 1312) are
  all unchanged. This is the sharper half of the flag day: unlike every prior
  wire bump, an old peer's frames are the same length and *parse*, so a mixed
  cluster misreads rather than stalls. `docs/how-to/upgrade-a-cluster.md` says
  so explicitly.
- **The leader stamps, once per pass, with a monotone clamp.** One wall-clock
  read per consensus pass (not per frame), and `stamp = max(now, last_stamp)`
  applied inside `uc_log::Appender`, the one place every frame type is
  written — so the clamp is a property of the log, not of any caller. Equal
  stamps are allowed and expected: position is the order, time is never a
  tie-breaker. Every frame type is stamped (MESSAGE, NEW_TERM, CONFIG,
  PADDING, TIMER), so the log's time is defined at every position. The archive
  agent carries the highest recorded stamp into the new cnc word
  `log_time_ns`, and a new leader seeds its clamp from it after the
  leader-open collapse; the word is never lowered, which is what makes the
  seed monotone-safe even when the collapse cut a frame above the archived
  base.
- **`ApplyCtx` gains `time_ns` and `term`** as public fields (this is exactly
  what `#[non_exhaustive]` was carried into the identity spec for — no second
  signature change), plus `schedule(id, at_ns)`, `cancel(id)` and `timers()`.
  **`query` receives no time**: a read has no position that means the same
  thing on every replica, and time is no better defined there.
- **`FRAME_TYPE_TIMER = 5`**, a fixed 24-byte body of three LE `u64`s
  (`identity_hash ‖ timer_id ‖ deadline_ns`), 64 B total after alignment.
  `client_id`/`seq` are 0; `FLAG_TIMER_TABLE = 0x01` is reserved for plan 2.
  It names the FSM by **identity hash, not row**, because a log frame outlives
  a row reorder. Id-only, no payload: an FSM keeps a timer's context in its own
  state, keyed by the id. **One pending instance per `(fsm, id)`** —
  re-scheduling replaces the deadline, Aeron's per-correlation-id semantics.
- **This is the first per-FSM frame in a broadcast log**, and the M14 spec now
  carries an as-built erratum saying so. A `TIMER` frame is delivered only to
  the FSM whose hash it names and skipped by every other apply loop, while
  still counting as a yielded frame for bounded-lag and lockstep accounting.
  The same erratum records a second consequence the same-type `Tagged`
  harnesses had masked: **heterogeneous FSMs on one log must share one wire
  command type** (an envelope enum), or each must treat the other's commands
  as a deterministic no-op. `uc_lincheck::timer::MixedCmd` is the worked
  example.
- **The ordering guarantee, and why it holds.** Each leader pass: read the
  clock; fire every due timer (stamped with its **deadline**, clamped),
  bounded by `TIMERS_PER_PASS = 64`; only then append client frames. At the
  bound, or on `WouldOverrun`, the pass appends **no** client frames at all,
  because interleaving one between two due timers would stamp it above a later
  deadline. So: a timer with deadline `D` fired in pass `k` was not due in
  pass `k-1`, whose `now` was therefore below `D`, and pass `k` places it
  before anything stamped `now >= D`. The one case the invariant cannot hold
  is a leader that stamped past `D` and died before firing: the clamp writes
  `last_stamp` in the header and leaves `D` in the body, and `ev.late(ctx)` is
  true. That is an OS timer firing late under load, not a correctness loss.
- **At-least-once at the node, exactly-once at the service.** Each node keeps
  a per-row `pending` map plus a lazy-deletion min-heap, on **every** role;
  only the leader pops by time. On any leader exit (`BecomeFollower` and
  `halt`) every in-flight instance is **re-armed**, so a *missed* fire is
  impossible: the only way an instance leaves every node's heap is a
  `consumed` record from a service that saw it on the log. `uc_service::
  Timed<S>` turns that into exactly-once, deterministically, from log content
  alone: it delivers only when `(id, deadline)` is still in its own
  log-derived pending set. Without the wrapper a state machine gets
  at-least-once timers, documented as the same trade as running without
  `Sessioned`. No node-side persistence: the heap is a cache of what the
  services know, re-announced after attach and after replay.
- **`svc_sched.<row>.ring`** (SPSC, service → node, 1 MiB, `MSG_V2_SCHED = 8`,
  17-byte records `op ‖ timer_id ‖ deadline_ns`) is the first per-row ring the
  **node consumes**; `svc_query`'s consumer half is dropped at creation, so
  the consensus agent's drain is new code beside `drain_query_ring`, not a
  refactor of it. It takes the per-row reservation from 5 MiB to 6 MiB
  (~79 MiB boot reservation with one FSM, ~121 MiB with eight).
- **cnc 3.1 gains two words in place**, both inside the same unreleased page
  version: `log_time_ns` at page 1 offset `4048` (the third word of the
  boot-once `4032` line, whose only live writer is the archive agent) and
  per-row `timers_pending` at slot line 7 `+488` (consensus-agent-written each
  pass). Offsets pinned in both `uc_protocol` and `uc_log`, as always.
- **Observability**: `uc2_timers_{pending,fired_total,late_total,rearmed_total}`
  per row, `uc2_log_time_ns` everywhere, `uc2_log_time_lag_seconds` on the
  leader only, and the `Uc2LogTimeFrozen` rule
  (`uc2_log_time_lag_seconds > 5 and on(instance) uc2_is_leader == 1`, 30 s,
  warning). `uc2ctl status` prints `log_time_ns=` (raw ns — `uc_ctl` has no
  date formatter, and the raw value is what the metric and the cnc word both
  hold) and a per-row `timers_pending=`. **Two** `[log]` records, not three:
  `timer_late` fires only when a fire is late, and `timers_rearmed` on
  leadership loss. There is deliberately **no per-fire record** — that would
  be a `stderr` write per timer on the consensus agent, and `uc_obs` has no
  Debug level to demote it to; `uc2_timers_fired_total` is the on-time signal.
  (Spec §6 erratum.)
- **Proof surface**: `uc_log`'s pass-order property test at the appender;
  `uc_node/tests/timers.rs` (two end-to-end tests, in the CI fast list);
  `uc_sim::timers::PassModel`, a pure model of the leader pass across seeds
  and leader changes, whose `check()` has **five** rules — the spec's four
  plus "lateness must pre-date the pass", added during execution because the
  first four cannot tell a clients-before-timers order swap from legitimate
  lateness once the clamp is applied (spec §8 erratum); two capstones,
  `two_fsm_timer_churn_under_failover` (`lin_v2`) and
  `two_fsm_timer_service_sigkill` (hard-crash), both adjudicated by **one
  shared oracle**, `uc_lincheck::timer::assert_timer_report` (never-early;
  exactly-once as *no duplicate* **and** *no loss*, with a 250 ms completeness
  margin justified by the asynchronous service → node ring hop;
  cancel-honoured; §4.3 order; replication equivalence); and two new fuzz
  targets, `uc_protocol_timer_frame` and `uc_protocol_sched_record`, taking
  the tier from 15 to 17. Lean, conformance vectors and the loom models are
  re-run as regression only: the stamp is data carried by consensus, not a
  consensus decision.
- **One spec deviation worth naming**: §4.8's proposed `trait TimerSource` is
  as built a provided `RawStateMachine::pending_timers()` with an empty
  default, overridden by `Timed`. A separate trait would have added a second
  bound to every generic path the apply loop already threads
  `S: RawStateMachine` through, for a hook whose default is "nothing".

### Breaking, and why it ships as a minor

Under [the semver policy](reference/semver-policy.md) this is a real
breaking change: the trait gains a required const and changes `apply`'s
signature, `ServiceConfig` loses a setter, `[services]` goes from optional
to required with `ids` refused outright, and every `--service-id` CLI flag
is gone. Log time and timers adds nothing further to that list — its whole
`uc_service` surface is additive (`#[non_exhaustive]` fields, two provided
trait methods with defaults, new types, a new wrapper), and `uc_protocol`'s
`FrameHeader` change is on an item the policy does not promise. The maintainer's decision (spec §10, 2026-09-02) ships it as the
**next minor**, `2.11.0`, rather than `3.0.0` — on the project having no
external users yet, not on the "nothing published" fact `2.9.0`'s carve-out
relied on (crates.io publishing started at `2.9.0`). This is one decision
for one release, not a standing exception — see
[the semver policy § FSM identity carve-out](reference/semver-policy.md#fsm-identity-a-breaking-trait-and-config-change-riding-as-a-minor).

### Release evidence

**Nothing below has run yet — every row is `pending`.** No fleet gate, no
tag, no crates.io publish. Filled in when the maintainer green-lights the
release, following the same table shape every prior release entry in this
file uses ("What proves the release").

| what | evidence | result |
|---|---|---|
| `ci.yml` (fmt gate, clippy, workspace tests, MSRV 1.89) | — | pending |
| `docs.yml` (rustdoc, link check) | — | pending |
| `release.yml` (build, SBOM, cosign, image) | — | pending |
| workspace correctness stack (`lin_v2`, `lin_partition_v2`, `learner`, `elle_check.sh`, hard-crash) | dev-box smoke only so far, see the branch's Task 9 report | pending (fleet-equivalent, not yet a gate) |
| `cargo test --workspace --doc` | Task 10, this worktree | see below (run as part of this docs sweep, not a release gate on its own) |
| FSM identity fleet gate (rows a/b/e/j) | `docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md` | pending — bars committed, no run |
| time-and-timers gate (rows a/b/c/d) | `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` | pending — bars committed, no run. Row d is an isolated `apply_bench` A/B under `scripts/hop1_ab.sh`'s same-source rebuild control, added because this work *does* touch two hot loops (M14a's codegen lesson) |
| artifact integrity (`sha256sum -c`) | — | pending |
| artifact provenance (`cosign verify-blob`) | — | pending |
| crates.io | — | pending |

## v2.10.0 — 2026-08-31 — one log stream, config from the environment, and a weak-memory fix

Thirteen crates, still lockstep. No wire change (protocol stays 0.6.0), no cnc
change, no `node.toml` schema change — a `2.9.0` config starts unmodified on
`2.10.0`. Two breaking changes for people *around* the daemons rather than
inside them, and one correctness fix.

### The Broadcast seqlock was unsound on weak memory

The node→client egress ring is the only ring with **no backpressure**: the
single producer may lap a reader mid-copy, so a read's validity rests entirely
on re-reading `publish_position` after the copy. That argument needs

> if lap N+1's bytes are visible, then `publish_position >= N+1` is visible

and the producer's `Release` store does **not** provide it. Release orders
accesses *before* the store; it says nothing about accesses after, so the next
call's `write_record_at` may be observed ahead of it. A consumer could then
copy half of lap N and half of lap N+1 while still reading a stale
`publish_position`, and the re-check would pass a **torn record** into the crc
— which `try_read`'s own comment claims cannot happen.

Fix: one `fence(Release)` at the top of `BroadcastProducer::write`, between the
previous record's publish store and this record's body writes. Cost measured
with `rustc --emit asm` on both targets: **no instruction on x86_64** (the
fence emits only `#MEMBARRIER`; identical machine code) and one `dmb ish` on
aarch64 — paid exactly where the bug is reachable.

**No test could have found this.** x86-TSO forbids the store-store reordering
it needs; aarch64 permits it; CI builds aarch64 binaries and never executes
them. `broadcast::tests::{wrap_no_torn_read, overwrite_during_read_never_tears}`
hammer this exact path and pass. It was found by
`uc_protocol/tests/loom_broadcast.rs`, a new loom model written to close the
gap `docs/VERIFICATION.md` had disclosed ("the broadcast seqlock has never been
model-checked"), which failed on its first run. Two `#[should_panic]` mutations
keep the model honest. Impact where reachable: a torn read surfaces as a
spurious `BadCrc` instead of the defined `Overwritten`, with a ~2^-32 tail
where the crc passes and a client sees a corrupt response. No consensus, log or
journal exposure. Full writeup:
[`docs/notes/uc2-broadcast-seqlock-explained.md`](notes/uc2-broadcast-seqlock-explained.md).

### One output stream per daemon (twelve-factor #11)

Both daemons wrote to stdout *and* stderr, so a consumer merged two streams and
two formats. Now: every record from startup onward is a JSON line on stderr;
**stdout is byte-empty**, enforced by
`the_daemon_writes_nothing_to_stdout_and_everything_to_stderr`, which pipes
both streams over a full start/SIGTERM/exit-0 lifecycle.

Two lines were **deleted rather than converted**, because both duplicated
records the library already emitted:

  * `node {id} is now LEADER/follower (term N)` — `node.rs` emits
    `became_leader`/`became_follower` **on the transition**, while the daemon
    polled `is_leader()` every 100 ms and so could miss a flap shorter than
    its interval. Strictly worse, and redundant.
  * `agent {name} fail-stopped; exiting` — printed beside the
    `agent_failstopped` record on the line above it.

The gateway could not reach `uc_node::obs::log` (and must not depend on
`uc_node` — that would pull consensus, transport and crypto into a front-door
process), so the 361-line log core moved to a new dependency-free crate,
**`uc_obs`**. Named for its purpose rather than `common`/`util` deliberately:
the workspace was searched for what a general utility crate would hold and the
answer was "this and nothing else" (`now_ns` appears at 8 sites but is
`self.base.elapsed()` on 8 different structs; the rest are test fixtures), and
`semver-policy.md` records the 2.9.0 rename as a **spent** carve-out, so the
name is effectively permanent.

Stated exception, documented rather than hidden: the pre-start refusal lines
stay human prose on stderr. They are emitted before `[log] level` has been
read and are addressed to whoever runs `systemctl status`; their
machine-readable half is the exit code.

### Environment overrides (twelve-factor #3)

Eleven `UC2_*` keys, environment winning over file. Three boundaries were
deliberate:

  * **No key material, ever.** `crypto.key_path` and the `[admin]` key paths
    stay file-based — an env var is readable in `/proc/<pid>/environ`, in
    `docker inspect`, and by every child process. `no_env_override_carries_key_material`
    fails if anyone adds one.
  * **No tuning knobs.** `buffer_bytes`, `election_timeout_*` and
    `admission_bytes` are part of the build's behaviour, which is what the
    twelve-factor page itself recommends.
  * **`parse_str` stays a pure function of its text.** The env layer lives in
    a new `parse_str_with_env(text, env)`. Two load-bearing reasons:
    `parse_str` is the `uc_node_toml`/`uc_gateway_toml` fuzz targets' entry
    point and a target depending on ambient state is not reproducible; and
    `std::env::set_var` is `unsafe` in edition 2024, so tests take an explicit
    lookup rather than mutating the process environment under a threaded test
    binary.

Overrides apply **before** validation, so a bad value is refused by the same
path a bad file value is — but the message names the **variable**, because the
file is valid and naming the TOML key would send the operator to edit the
wrong thing. `UC2_MEMBERS` **replaces** the table rather than merging: a
membership list must agree cluster-wide, so a half-overridden one is never what
anyone means.

Payoff: `packaging/compose.yml` rendered six near-identical config files
through a busybox shell; it now renders **two**.

### Release identity (twelve-factor #5)

`config_loaded` {path, sha256} at startup — plain SHA-256 over the file's
bytes, pinned in a test against the published vectors for `"abc"` and `""`
rather than against itself, so a change of algorithm fails loudly instead of
silently renumbering every operator's ledger. It digests the file **as read,
before overrides**: that is the artifact under version control, and hashing a
post-override "effective config" would need a canonical serialisation this
crate does not have. `sha2` was already a workspace dependency, so no crate
enters the lockfile.

### `ultima-db` removed

An optional dep behind `uc_service`'s non-default `ultima_db` feature, carrying
a `StoreStateMachine` adapter that **nothing in the tree used except its own
test** — `StoreStateMachine` appeared in exactly two files, and
`snapshot_stream` nowhere outside the adapter. Dropped three crates from
`Cargo.lock` (`ultima-db`, `dashmap`, `hashbrown`) and four CI/nightly/docs
steps. Not a major version: `semver-policy.md` already excluded it by name
under the non-default-features carve-out. This also corrected a stale claim —
CLAUDE.md had called it "the default app-state store + snapshot format", which
was never true of the shipped code.

### Toolchain and CI

`cargo fmt --all` finally ran (3 393 hunks, 189 files, one mechanical commit)
once the last long-lived worktree landed, and `cargo fmt --all -- --check` is
now the first step of `ci.yml`. History before that commit is unformatted, so
`git blame` across it needs `-w`.

New: `scripts/check_doc_links.py`, run first in `docs.yml`. It checks internal
links against **both** renderers in use — GitHub's slug rules and md-tui's,
which differ in two ways (md-tui drops `_` and collapses repeated `-`). Dead
files, dead GitHub anchors and links nested inside `*...*` emphasis (inert in
md-tui) are errors; md-tui-only divergences are warnings, because making them
fatal would hold 239 em-dashed headings — including gate docs, which are
permanent records — hostage to a third-party TUI. It found one genuine
pre-existing dead link on its first run.

### Fleet work, all methodological

No performance change ships in `2.10.0`; nothing on the commit path was
touched. Three fleet runs were made and all three are recorded:

  * **CPU pinning: NOT adopted.** Pre-committed bar was "adopt iff the pinned
    spread is < 5 %"; measured 14.3 %, plus a −9.4 % throughput cost. The
    `c6id.2xlarge` sibling map's assumption was verified on real hardware for
    the first time (`lscpu -e=CPU,CORE` → `CORE 0..3 0..3`).
  * **Core count: 4 physical cores**, one per polling agent, flat past 5 —
    measured by a pin-width sweep on `c8id.4xlarge`, direct shmem path only.
  * **A "two operating regimes" finding was published and refuted the same
    day.** A 16-arm probe with per-second timelines showed the gap fills in
    with more samples: one broad distribution with a long low tail, no arm
    ever transitioning. Pinning's real value is variance reduction — **31×
    tighter p50 spread**. The superseded claim is marked as such rather than
    deleted.

Two standing lessons came out of that and are recorded in CLAUDE.md: size a
spread bar's rep count from observed arm-to-arm variance (n=4 cannot
distinguish a distribution's width from its tail), and **check whether a driver
passes `m12_gate`'s `--warmup-secs`/`--measure-secs` steady window before
comparing its rates with another's**. `m14_fleet_gate.py` does, so the M14
gate's rows a/b/e are steady-window numbers; the 2026-08-31 sweep and probe
drivers do not, so their rates include a 3–5 % warm-up climb.

### What proves the release

All run ids below are on the tag commit `32024e5` (`v2.10.0`) unless stated
otherwise.

| what | evidence | result |
|---|---|---|
| `ci.yml` (fmt gate, clippy, workspace tests, MSRV 1.89) | run `33435180950` | success |
| `docs.yml` (rustdoc, link check) | run `33435180996` | success |
| `release.yml` (build, SBOM, cosign, image) | run `33435475868`, 7/7 jobs | success |
| artifact integrity | `sha256sum -c` against the published manifest | `OK` |
| artifact provenance | `cosign verify-blob` (keyless) | `Verified OK` |
| the 2.10.0 stdout contract, in the shipped binary | ran `uc2-node` from the release tarball | **0 bytes on stdout**; `config_loaded`, `node_listening`, `became_leader`, `serving_changed` present as JSON-lines records |
| crates.io | all 13 crates published in dependency order, 2026-08-31 | live at `2.10.0`, verified against the sparse index; 59 s, zero retries |

Two things this table deliberately does **not** claim:

- **`nightly.yml` has not run on the tag commit.** The last nightly before the
  tag (`33379077096`, on the docs-only commit `cb3eb9d`) went red in
  `capstones`. It was root-caused post-tag to a **test** defect, not a product
  one: `two_fsms_apply_the_same_log_and_fsm_zero_answers_the_client` asserted
  page 1's `service_applied` equal to slot 0's `applied` outright, but that
  field is the node's once-per-cycle mirror of the `min` over declared slots
  (`Node::publish_service_mins`), so it converges a cycle later and — being a
  min — can still hold the older sample after both slots are level. It failed
  by exactly one 64-byte record (6368 vs 6432). The assertion is now a
  `wait_until`. The race was not reproduced locally (it passes locally either
  way); the diagnosis rests on the CI log and the code path.
- **`uc2-gateway` had no `--version` flag** while `uc2-node` and `uc2ctl` did,
  and it shipped that way in 2.10.0 — found while verifying the tarball. The
  cause was one missing word: clap's derive emits `--version` only when
  `#[command(version)]` is set, and the gateway's attribute did not set it.
  **Fixed after the tag** (on `main`, so it lands in the next release), with a
  unit test that asserts `Args::command().get_version()` is the crate version —
  the assertion fails (`left: None`) with the fix reverted.

The first-ever ordered publish, at `2.9.0`, took about an hour because all
twelve names were new and crates.io rate-limits *new names* far harder than new
versions. `2.10.0` had exactly one new name (`uc_obs`) and took 59 seconds —
which is the measurement behind `docs/how-to/cut-a-release.md` §6's
rate-limit note.

## v2.9.0 — 2026-08-30 — the `uc_*` crate rename

**Mechanical. No behaviour change, no wire or cnc change, no binary rename.**
Every package took a uniform `uc_` prefix, and every crate directory was
renamed with it. The user-facing writeup, with the migration `sed` and the
old→new table, is the `2.9.0` section of `RELEASES.md`; this entry is the
engineering record of how it was done and what proves it.

### What moved, and what deliberately did not

Renamed (package **and** directory): `uc2_log`→`uc_log`, `uc2_net`→`uc_net`,
`uc2_crypto`→`uc_crypto`, `uc2_consensus`→`uc_consensus`, `uc2_node`→`uc_node`,
`uc2_service`→`uc_service`, `uc2_client`→`uc_client`, `uc2_remote`→`uc_remote`,
`uc2_gateway`→`uc_gateway`, `uc2ctl`→`uc_ctl`, `ultima_journal`(package
`ultima-journal`)→`uc_journal`, plus the unpublished `uc2_sim`→`uc_sim`,
`uc-lincheck`→`uc_lincheck`, `uc2-crashtest`→`uc_crashtest` and the
out-of-workspace `uc2-fuzz`→`uc_fuzz`. `uc_protocol` already conformed.

Deliberately **not** renamed, because they are operator-facing contracts rather
than package names:

- the three binaries `uc2-node`, `uc2ctl`, `uc2-gateway` — and therefore the
  systemd units, the `ghcr.io/peterknego/uc2` image, `compose.yml`'s
  healthchecks and every runbook command. `uc_ctl/Cargo.toml` gained an
  explicit `[[bin]] name = "uc2ctl"` block: its binary had been named
  implicitly from the package (`src/main.rs`), so without the pin the package
  rename would have silently renamed the CLI.
- every `/metrics` name (`uc2_is_leader`, `uc2_fsm_lag_bytes`,
  `uc2_free_disk_bytes`, …) — none contains a crate token, so the rename could
  not have reached them; dashboards and `uc2-alerts.yml` are unchanged.
- the agent thread names (`uc2-consensus`, `uc2-apply`, `uc2-sender`,
  `uc2-receiver`, `uc2-archive`), visible in `ps`/`perf` output, and
  `scripts/uc2_flag_day.sh`.
- doc filenames (`docs/ops/uc2-runbook.md`, the `uc2-m*-gate-*.md` records,
  `docs/reference/uc2ctl.md` — which documents the binary).

### How it was executed

1. `git mv` for all 14 directories, so history follows by rename detection.
2. One `sed` pass over the 376 tracked files matching any old crate token,
   applied to the 15 unambiguous identifiers.
3. `uc2ctl` was handled separately, because the string is *both* a package name
   and the binary name: only `-p uc2ctl`, the `uc2ctl/` path prefix, the
   workspace `members` entry, and the package arg of the two `(pkg, bin)`
   helper call sites (`cargo_bin`, `build_bin`) were rewritten.
4. `fuzz/` (outside the workspace, own lockfile) followed: its 15 target names
   contain crate tokens, so `fuzz_targets/*.rs` and the matching
   `fuzz/corpus/<target>/` directories were `git mv`'d to keep the committed
   seed corpora attached to their targets.
5. `CLAUDE.md`'s standing ban on the names `uc_node`/`uc_service`/`uc_client`
   — they were v1's — was rewritten to record that the ban is lifted and that a
   pre-rename commit naming them means the deleted v1 crate.

### Why a minor and not a major

`docs/reference/semver-policy.md` says a promised item changing incompatibly
means `3.0.0`, and every promised path moved
(`uc2_service::traits::RawStateMachine` → `uc_service::traits::…`). The
exception was granted on one fact: the §6 crates.io publish had **never been
run**, so no resolver could hold the old names and no lockfile could break; the
wire, cnc page, on-disk layout and binaries were untouched. That fact expired
the moment `2.9.0` was published, and the policy now says so in a dedicated
section — any later rename of a promised path is a `3.0.0`.

### Verification

A rename either compiles or it does not, so the evidence is the whole local
stack at `2.9.0`, plus the checks that a `sed` can silently break — packaging,
the fuzz corpora, and the docs' links.

- `cargo build --workspace` → exit 0; `cargo clippy --workspace --all-targets
  -- -D warnings` → exit 0.
- `cargo test --workspace` → **exit 0, 1 436 passed / 0 failed across 102 test
  binaries** (`~/uc-rename-verify/test-2.9.0.log`).
- `scripts/check_publish_metadata.sh` → `ok: publish metadata (keywords,
  categories) within crates.io limits`.
- `cargo publish --workspace --dry-run` → exit 0, all 12 crates verified and
  ordered by cargo itself: `uc_consensus, uc_journal, uc_protocol, uc_remote,
  uc_crypto, uc_log, uc_net, uc_client, uc_node, uc_ctl, uc_service,
  uc_gateway`, each at `v2.9.0`. (Run with `--allow-dirty` because the rename
  was still uncommitted; `cargo publish` otherwise refuses a dirty tree.)
- Fuzz: `RUSTFLAGS="--cfg fuzzing" cargo +nightly check --all-targets` in
  `fuzz/` → exit 0 with all 15 targets resolving, and
  `cargo +nightly fuzz run uc_crypto_open -- -max_total_time=10` → exit 0,
  `2 595 640 runs`, `corp: 19/5680b` — the 19 seeds prove the renamed target
  found its moved `fuzz/corpus/uc_crypto_open/`. (A plain `cargo check` in
  `fuzz/` fails on `route_raw`/`ObsSources::for_tests`, which are
  `cfg(any(test, fuzzing))`; that is the wrong command, not a defect.)
- `git grep` for every pre-rename token — `uc2_log`, `uc2_net`, `uc2_crypto`,
  `uc2_consensus`, `uc2_node`, `uc2_service`, `uc2_client`, `uc2_remote`,
  `uc2_gateway`, `uc2_sim`, `ultima_journal`, `ultima-journal`, `uc-lincheck`,
  `uc2-crashtest`, `uc2-fuzz` — returns nothing.
- Links: all 173 tracked `.md` files scanned for repo-relative targets;
  **0 broken links point into a renamed crate directory**. 44 broken links
  exist, all inside `docs/superpowers/plans/`, and all resolve identically
  badly at the `v2.8.1` tag — pre-existing, untouched.

### The release itself

- `ci.yml` run `33337780002` at `214ac2a` — **all four jobs green**
  (`msrv`, `test`, `deny`, `publish-check`); `docs` run `33337780032` green.
- `release.yml` run `33339079316` on the `v2.9.0` tag — **all seven jobs
  green** (`version`, both `build`s, `sbom`, `release-smoke`, `image`,
  `release`).
- §5 verified as a stranger, from a clean directory against the release page:
  `sha256sum -c` OK on all three artifacts; `cosign verify-blob` `Verified OK`
  on all four signed blobs (both tarballs, the SBOM, and `SHA256SUMS` itself);
  `cosign verify ghcr.io/peterknego/uc2:2.9.0` claims validated; the tarball's
  own `packaging/quickstart-local.sh` → **PASS**. The shipped `bin/` holds
  `uc2-node`, `uc2ctl`, `uc2-gateway` — the rename's central promise, checked
  in the artifact a user actually downloads. `v2.9.0` took the **Latest**
  pointer automatically (`v2.8.1` was not a prerelease).
- **crates.io: the first publish this project has ever done.** All 12 crates
  are live at `2.9.0` under their `uc_*` names, confirmed by API and not by
  the publish logs. It took 62 minutes: crates.io rate-limits *new crate
  names* at roughly one per 2.5 minutes after a burst of ~5, so seven of the
  twelve needed 2–5 attempts with a 150 s backoff. Nothing was bumped and
  nothing was skipped; the mechanism and its one-time nature are now written
  into [Cut a release](how-to/cut-a-release.md) §6. **The semver carve-out
  above is spent from this moment** — "nothing had ever been published"
  stopped being true at 23:32 UTC on 2026-08-30.

Not run locally, and left to CI: the MSRV job (1.89 clippy), `cargo-deny`, the
`ultima_db`-feature and `apply-profile` clippy arms, the hard-crash suite, elle
and the Lean tiers. Nothing in this release changes what any of them execute —
only the `-p` names they are invoked with, which `ci.yml` and `release.yml`
carry.

## v2.8.1 — 2026-08-30 — M14c2: the multi-service proof pass

**Proof only. No feature, no configuration change, no wire or cnc change —
`2.8.1` is API-compatible with `2.8.0` by construction** (spec §15.1's own
words). It closes the coverage gap `2.8.0` disclosed: the WGL linearizability,
partition, hard-crash and Elle tiers now each run with **two state machines
attached to every node**, one history per FSM; the M14 gate's row-e lockstep
finding is settled by a pre-registered experiment; the M14c deferrals are
closed. Commits `bb60d20..e5cc299` on `worktree-uc2-m14c2`, base `1d80136`
(= `main` at the `v2.8.0` tag). Plan and the binding spec section:
`docs/superpowers/plans/2026-08-30-uc2-m14c2-proof-pass.md`,
`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` §15.1, §16.
Coverage record: `docs/VERIFICATION.md` §11.

Twelve tasks, in the order they ran: T0 a spec erratum; T1 the shared
`--services`/`--fsm-lag` parser; T2 the harness's second FSM; T3–T5 the WGL
capstones; T6 the two hard-crash scenarios; T7 the Elle tier; T8 the lockstep
experiment; T9 the fleet-rig pinning; T10a/T10b/T11 the deferrals; T12 this
writeup.

### The harness (T1, T2 — `85d4cf1`, `862b9d4`)

- **`ServicesConfig::from_cli(Option<&str>, Option<&str>)`** (`uc_node/src/services.rs`)
  is now the one parser behind `--services` / `--fsm-lag`; `m12_gate` and the
  crashtest node bin both take it, so a test process declares FSMs exactly the
  way `uc2-node` does.
- **`FsmSet::{Single, Two { lag }}`** in `uc_node/tests/lincheck_v2/mod.rs`
  starts a second `uc_service` per node, plus `Slow<SM, MICROS>` (a wrapper
  that sleeps `MICROS` per apply), `Corrupt<SM>` (used only to make an oracle
  fail), `submit_all_cmd` (one submit, both answers, recorded into two
  histories) and `read_leader_on(id, …)`. `ClusterCfg` gained
  `buffer_bytes` (T11) with its previous hardcoded `1 << 22` as the default, so
  no existing capstone's geometry moved. The single-FSM paths — `worker`,
  `spawn_workers`, `submit_cmd`, `read_leader` — are untouched: T3's diff into
  those two files is 308 insertions, **0 deletions**.

### The capstones (T3–T5 — `2ea78d2`, `24bff5b`, `02896a4`, `a71d89e`)

Every two-FSM capstone asserts **per-FSM linearizability** with the untouched
`uc_lincheck` WGL checker *plus* the **replication-equivalence oracle**: each
`submit_all`'s per-FSM answers must be byte-equal, an unequal pair is counted
and recorded as `Indeterminate` in *both* histories rather than resolved from
FSM 0, and `equiv == 0` is asserted before any checker verdict is read.

- `two_fsm_bounded` / `two_fsm_lockstep` (`uc_node/tests/lin_v2.rs`) — the M6
  fault set (leader kills, service crashes, purge/snapshot churn) at
  `FsmLag::Bounded(64 KiB)` and `FsmLag::Lockstep`. Green at two seeds each
  (~7 s per run); the file's whole suite ran 13/13 in 178 s
  (task-3/task-4 reports).
- `two_fsm_oracle_bites` — `#[should_panic(expected = "replication-equivalence
  violated")]`, FSM 1 = `Corrupt<RegisterSm>`. Observed panic:
  `[(0, CasResult(true)), (1, CasResult(false))]`. An oracle nobody has made
  fail is not evidence; this is the demonstration.
- `two_fsm_slow` / `two_fsm_slow_lockstep` — FSM 1 = `Slow<RegisterSm, 200>`.
  Two assertions beyond linearizability: (i) at every 50 ms sample the
  separation is inside the policy (≤ `fsm_lag`; ≤ one 288 B frame in
  lockstep), (ii) over the run's second half the two FSMs' apply rates are
  within 10 %. Measured `ratio=1.000` in all runs (`rate0=19681 rate1=19681`,
  then `rate0=22156 rate1=22156`; task-4 report) — both FSMs progressed at the
  same rate.

  Scoped by measurement (final fix wave, 2026-08-30, dev-box smoke — never a
  gate): the runs also report `max_lag`, and the separation never approached
  the bound. `two_fsm_slow` = `max_lag=192 bound=65536` (~3.7 frames at
  ~52 B/frame, 0.3 % of the bound) at `rate0=rate1=22111`;
  `two_fsm_slow_lockstep` = `max_lag=64 bound=288` (~1.2 frames) at
  `rate0=rate1=22227`. ~22 KB/s at ~52 B/frame is ~425 records/s, well under
  `Slow<RegisterSm, 200>`'s ~5 000 applies/s ceiling — so the **client loop**,
  not FSM 1, is the limiter, and the lag policy never binds. The run does not
  exercise a bound-pinned state; the slow-FSM oracle is evidence of equal
  progress, not of the barrier's behaviour at the bound. Earlier wording here
  ("the fast FSM tracks the slow one … rather than sitting at the bound")
  claimed a pacing mechanism the run had not measured.
- `minority_partition_and_heal_two_fsm` (`uc_node/tests/lin_partition_v2.rs`)
  — `Run` gained `h1`/`equiv` and a `start_cfg_two_fsm`/`finish_two` sibling
  pair; `run_minority` takes a `two_fsm` flag whose `false` arm is the
  pre-existing code moved verbatim into an `else`. Measured
  `ops=804 ok=799` (FSM 0) and `ops=800 ok=800` (FSM 1) at seed 7; suite 8/8 in
  59.9 s.

### Hard crash (T6 — `6e14dcc`)

`two_fsm_service_sigkill` (FSM 1's process killed and respawned mid-load) and
`two_fsm_node_sigkill` (node + both services killed, node respawned, both
services reattached), in `examples/uc_crashtest/tests/hard_crash.rs`, over
`spawn_service_id` / `spawn_node_with_services` in that crate's
`tests/common/mod.rs`. Both FSM histories linearizable and `equiv == 0` across
6 kill cycles; two seeds × two runs each plus the default seed; the whole
`hard_crash` suite 6/6 in 9.6 s (task-6 report).

### Elle (T7 — `11d4ecc`)

`elle_quiet_two_fsm` (`uc_node/tests/elle_v2.rs`) records **one list-append
history per FSM**; `scripts/elle_check.sh` learned the per-FSM shape (its
generation trigger and verdict loop both handle `<pass>/fsm*/history.edn`) and
its default pass list is now **six**. Measured at `ELLE_TARGET_OPS=8000`:
`completed0=8031 ok0=8031 completed1=8030 ok1=8030 equiv=0`, then
`quiet_two_fsm/fsm0` 16 062 events and `fsm1` 16 060 events, **clean under both
`serializable` and `strong-serializable`** (task-7 report). The single-history
path was re-run afterwards to prove it is unchanged.

`equiv=0` here is weaker than it looks and must not be cited as equivalence
evidence: `LaResp` has one variant, so `elle_worker2`'s vector comparison can
only fail on a **malformed** fan-in (a missing id, a non-ascending pair), never
on a genuine state divergence. The general check is kept for the day the
response type grows variants; the equivalence evidence for `2.8.1` is the WGL
capstones' `equiv_failures == 0` (T3–T6), whose `RegisterSm` responses do carry
state.

### The lockstep experiment (T8 — `3c7122d`, `74f8109`)

Record: `docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`.
Dev-box smoke throughout — what it adjudicates is a *ratio* and a *mechanism*,
never a rate bar.

- **Reproduced, harder than the fleet.** Unconstrained lockstep N=2 runs
  624 276 / 623 031 / 629 333 frames/s; at `--cores 0` (3 runnable threads on
  1 CPU) it reads **709 / 709 / 710** — **880× down** — while **bounded** N=2 on
  the identical rung is **7.4 M frames/s** and lockstep N=1 is 338 k. The
  control was measured first: **1.0 % peak-to-peak** unconstrained, and the
  `stress-ng` rungs vary by ~5×, which is why the bisect was read on the
  0.1 %-stable `--cores 0` rung.
- **The pre-registered mechanism is refuted.** `lag_waits = 0` on every
  collapsed run: the yield ladder never exhausts, so `APPLY_IDLE`'s 50 µs sleep
  is never reached and M14a's "one sleeper cascades the set" is *not* what row
  e is. 1/709 = **1.41 ms per frame** — a scheduler quantum, not a handshake.
- **The rule, applied verbatim** (plan Global Constraints, restating spec
  §16.4): (a) ladder ×4 and ×16 → **1.00×**; (b) unbounded yield while the
  sibling is live → **1.00×**; (c) futex wait on the sibling's `applied` word →
  **116×** (82 639 frames/s) but only **13 %** of the unconstrained rate, where
  the bar is 50 % (≥ 312 k). No variant clears it → **operating-envelope fact,
  stated with the number**. Landed as one sentence with the gradient in
  `docs/reference/configuration.md` (`[services]`, `fsm_lag`) and
  `docs/reference/limits.md`. **No behavioural code change**: `apply.rs` gains
  only a "do not retune" comment at `LAG_WAIT_YIELDS` and a paragraph on
  `lockstep_wait`, both pointing at the bench doc.
- Variant (c) was built, measured and **discarded** with its patch shape
  written into the doc, so the next attempt does not re-derive it.

### The pinned fleet rig (T9 — `e4b4fc0`, `ad4658d`) — written, not yet run

`--pin` (default off) in `bench-infra/scripts/m14_fleet_gate.py` and
`m14_ab_27_vs_28.py`, over new plumbing in `m12_fleet_gate.py`
(`unit_start_cmd(..., cpus=)` → `systemd-run -p CPUAffinity=`, and a `taskset`
prefix for the foreground client paths). `PIN_MAP_C6ID_2XL` assigns node
`0,1,4,5`, service0 `2`, service1 `6`, client/edge `3,7`, and
`require_pin_layout` refuses the run unless `lscpu -p=CPU,CORE` on **every host
the run starts units on — the row-f learner included** — reports the assumed
`(i, i+4)` sibling pairs. `--selftest` went 37 → 47 pure checks, all passing;
`cpus=None` leaves every command line byte-identical.
**Step 2, the fleet validation run, has not happened** (user-gated; the fleet
was destroyed). `docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md` is a
stub saying so, and the sibling map is a documented, machine-verified
assumption until it does.

### The M14c deferrals (T10a — `69856d2`, `21ab79a`)

In `uc_net`, all with red-first evidence (the report records a
behavioural-red round in which each fix was re-disabled individually):

- **Sender**: `snap_open_failed` counts the `File::open` TOCTOU refusal that
  previously had no counter (latched log, re-armed when a session opens); three
  refusal-path unit tests, two of them characterisation tests over guards that
  already existed (stated as such); a repair `SNAP_NAK` inside an artifact
  whose `SNAP_BEGIN` has not gone out is **skipped**, not served — reachable
  only for non-head requests, which the report names.
- **Receiver**: `SNAP_INTAKE_TIMEOUT_NS = 60 s` (deliberately 2× the sender's
  30 s session timeout, so the leader gives up first) abandons an intake with
  no chunk, unlinks its unfinished `.part` files and counts
  `snap_intake_abandoned`; an undecodable `SNAP_BEGIN` is counted once per
  session; `snap_chunk` seek/write failures are counted (tested by swapping the
  intake's handle for a read-only `File` on the same `.part`, so a real EBADF
  travels the shipped path); the publish re-drive is paced to one attempt per
  250 ms **on the chunk path as well as the duty cycle** — the fix round's own
  test measured **11 failing renames where the paced behaviour is 1** before
  the cadence moved inside `snap_publish_complete_parts`, and only a *failed*
  attempt arms the interval, so a normal publish is never delayed.
- Three series added to `CONTRACT_SERIES` and exported:
  `uc2_snapshot_open_failed_total`, `uc2_snapshot_intake_abandoned_total`,
  `uc2_snapshot_begin_undecodable_total`;
  `docs/how-to/monitor-a-cluster.md`'s family count 73 → 76.
- **Behaviour change worth naming:** a locally-obstructed install (a directory
  in the way of the final rename) now loses its completed-but-unpublished
  `.part` after 60 s and re-downloads the set, instead of publishing the
  instant the operator clears the obstacle. The trade is deliberate — a
  stranded full-size `.part` is real disk — and the two counters rising
  together is documented in three places as the signature.

### The M14c deferrals (T10b — `14ffb4e`, `3c5f962`)

- **Ruling K, `uc_service_lag_waits_total` undercounted to zero.** `lag::plan`
  returns `Wait` only when the cap is at or below the cursor; a byte bound
  rarely divides the frame stream, so the common pinned state is a cap *inside*
  the next frame — `Apply { target: cap }`, a zero-frame batch, no cursor
  movement and nothing counted. Now an out-of-line `note_lag_wait` fires at the
  "batch moved nothing" break when `target < head` (exactly "the barrier set
  this target"), and an episode ends when the **cursor advances**, not when the
  plan stops saying `Wait`. Red-first: `left: 0 / right: 1`, verbatim the
  reported symptom. The edge is `#[inline(never)]` per M14a's codegen lesson;
  **no A/B was run on `apply_bench`** and the report says so.
- `note_service_transitions` now takes the words `service_mins_and_liveness`
  already loaded (one pass per declared id) instead of re-reading them, so both
  readers adjudicate the same sample; no performance claim is made.
- The learner two-FSM join now pins the **positions** of the artifacts it
  installed (parsed from `snap-<pos>.ultsnap`) against the voter's
  `service_slot(id).snapshot_pos`, not merely their presence.
- `uc2ctl status` prints `fsm_lag=n/a` when a node declares no FSMs, instead of
  a resolved bound it paces nothing against.
- The snapshot decline latch was lifted out of a closure into
  `snapshot_set_for(…)` and is covered by an 11-call transition test.
- **`Uc2ServicePinnedAtLagBound`'s threshold moved** to
  `max(bound − 1408, 0.9 × bound)`, written as two `and`-ed clauses because
  PromQL has no scalar-vector `max` (and `group_left()` needs its empty parens
  or the parser reads the following expression as a label list). **Disclosure:**
  for bounds at or below one MTU (1408 B) the previous expression fired at any
  lag ≥ 1 and the new one only at ≥ 0.9 × bound — a loosening at very small
  bounds; an exact frame is not expressible because `max_payload` is not an
  exported metric. `promtool check rules` → 16 rules;
  `scripts/m10_alert_fire.sh` → **16 PASS / 0 FAIL**.

### The snapshot-restart pin (T11 — `5727a36`, `e5cc299`)

`snapshot_restart_installs_only_with_purge` (`uc_node/tests/lin_v2.rs`) pins
the fact that cost the M14 gate its row-d run 1: a `SnapshotPolicy` shortens a
service restart **only together with purge, and only once the live log buffer
has wrapped past `start_pos`** — below the wrap the fresh service reads the
still-live ring (`replay_into` is reached only via `Batch::Overrun`) and
touches neither the journal nor a snapshot, whatever the purge posture. The
test asserts `INSTALLS == 1` with purge on, `== 0` with purge off, **and** that
the rebuilt SM holds the last write, so a `0` cannot pass by nothing having
happened at all. Flip evidence recorded both ways; ~31 s.

### Deviations found by execution — the reports' own corrections

Each of these was reported rather than papered over, with the measurement that
refutes the brief's premise:

1. **The brief's crashtest warm-up was a real bug** (T6). Every declared FSM
   applies a committed command, so a plain `warmup_write` recorded into FSM 0's
   history only made FSM 1's history unexplainable — a *deterministic*
   violation reproduced with zero SIGKILLs. Fixed by recording the warm-up into
   both histories (`warmup_write2`).
2. **`snap_sessions == 1` is false at cluster scale** (T10b). A fresh learner
   re-NAKs below the floor until its floor adoption sticks, so the leader opens
   **3** sessions, not 1 — measured, stable over 3 runs. The exact claim is
   pinned where it belongs, at the seam:
   `uc_net/tests/snapshot_session.rs::a_two_artifact_stream_lands_in_per_id_dirs_under_chunk_loss`.
   *Newly measured, not previously recorded: three full re-transfers of every
   artifact on every fresh below-floor join. Correctness-neutral, worth a look.*
3. **"Two words `service_mins` just loaded" was one word** (T10b), and it is
   per-id, so the intent (no slot word read twice per duty cycle) was
   implemented instead of the letter.
4. **A harness page does not read `fsm_lag_bytes == 0`** (T10b) — the node
   writes a resolved bound whether or not it declares FSMs.
5. **The receiver has no injectable clock in the integration test binary**
   (T10a), so the timeout and cadence tests live in `receiver.rs`'s unit module
   driving the real `snap_upkeep(now)`; `uc_net/tests/snapshot_session.rs` is
   unchanged.
6. **3000 writes never wrap a 4 MiB ring** (T11), so the brief's literal test
   body could never reach `install_snapshot` in either arm. Fixed by shrinking
   the *ring* (`buffer_bytes: 1 << 16` in that test only), not by inflating the
   write count — a 100 k-write variant ran 10+ minutes and was rejected.
7. **The wrong-id mutation the review proposed does not discriminate** on the
   learner fixture (T10b): both FSMs snapshot at the same position, so
   `shipped + 1` was used instead. Recorded in the test.

### Re-parked (open, named, not done)

- **`remote_lin_envelope_off` can return Inconclusive** — the WGL search's
  5 000 000-visited-state budget exhausts intermittently on the 4-vCPU
  nightly runner (~2 of the last 9 crashtest legs, either crypto posture,
  first seen 2026-08-28 — predates this branch). Inconclusive is not a
  violation and the test rightly refuses to count it as a pass; the fix is
  a larger-budget retry in `assert_linearizable` or a lower op target
  (`THROTTLE`/`LOAD`), per the panic message's own hint.
- **Twelve-factor hygiene, postponed by the maintainer on 2026-08-30**
  (`docs/notes/uc2-twelve-factor-assessment.md`): env-var overrides for
  deploy-varying config keys (factor 3 — new config surface, wants its own
  spec; key material stays file-based) and one log stream (factor 11 —
  the `println!` lifecycle lines and the gateway stats line routed through
  `obs::log::emit` on one descriptor). Both are behaviour changes and
  belong to a feature release, not a proof-only patch. The release-ledger
  suggestion (factor 5) is a `cut-a-release.md` §7 line, not code.
- **No in-process test pins a bound-pinned FSM** — the whole two-FSM WGL
  family runs at ~425 records/s behind a synchronous client loop (the final
  fix wave's `max_lag` measurement: 192 B of a 64 KiB bound, 64 B of a 288 B
  lockstep slack), so the lag policy never binds; the capstones prove
  per-FSM linearizability and replication equivalence, not the barrier's
  behaviour at the bound. That state is exercised only by the M14 fleet
  gate's rows b and e; a paced in-process arm (a `Slow` FSM plus a
  pipelined driver) is the follow-up.
- **Peer-gate session replacement** — `snap_begin` still lets any address that
  passes the term filter replace a live intake from a different peer (T10a,
  pre-existing).
- **`snap_last_done`'s partial `(id, pos)` match** — the DONE latch re-acks on
  a partial match against the session's set (T10a, pre-existing).
- **The alert fixture does not exercise the new tolerance clauses** — the
  `fsm_pinned` scenario's 96 B payload divides its 8 KiB bound, so `16/16` says
  nothing about either clause; the honest close is a second arm at a
  non-dividing payload, and the scenario's own `Disclosure` string now says so.
- **Exporting `max_payload` as a metric** would let the pinned-at-bound rule
  subtract a real frame and retire both the MTU and the `0.9` approximations.
- **The lockstep 50 % bar may be unreachable for any blocking design** — the
  implementer's argument (a lockstep frame at N=2 needs 2–3 mandatory context
  switches on 1–2 CPUs, ~12 µs, against 3.2 µs for 50 % of 624 k), and the
  observation that the rule's regression clause is vacuous against a
  lockstep-gated implementation. **Not acted on: the bar stands as
  pre-committed** (honest-failure protocol); re-specifying it against a
  scheduling-aware ceiling is the maintainer's call, and the argument is in the
  bench doc's §3 either way.

### Verification evidence

Every line below is quoted from the task reports of the commits above; none is
a fleet measurement, and `2.8.1` claims no rate.

- `cargo test -p uc_node --test lin_v2` → `13 passed; 0 failed … 178.46s` at
  the T4 tree (task-4); the 14th test
  (`snapshot_restart_installs_only_with_purge`, T11) landed later and ran
  alone → `1 passed … 31.61s` (task-11). The whole file at the final fix
  wave's HEAD, inside the workspace run: `14 passed; 0 failed … 212.84s`.
  `--test lin_partition_v2` → `8 passed; 0 failed … 59.42s` (task-5).
- `cargo test -p uc_crashtest --features hard-crash-tests --test hard_crash` →
  `6 passed; 0 failed … 9.62s` (task-6).
- Elle: `OK: quiet_two_fsm/fsm0 clean under serializable` /
  `strong-serializable` and the same two lines for `fsm1`;
  `elle consistency check passed (quiet_two_fsm, crypto=0)` (task-7).
- `cargo test --workspace` → **1435 passed, 0 failed**, 2 ignored, across 102
  suites (task-10b, on the final T10b tree); `cargo test -p uc_net -p uc_node`
  → 29 `test result: ok` lines, 0 failed (task-10a).
- `cargo clippy --workspace --all-targets -- -D warnings` → clean (task-10a,
  task-10b, and again at the version bump).
- `promtool check rules packaging/prometheus/uc2-alerts.yml` → `SUCCESS: 16
  rules found`; `scripts/m10_alert_fire.sh` → 16 PASS / 0 FAIL (task-10b).
- `python3 bench-infra/scripts/m14_fleet_gate.py --selftest` → 47 checks,
  `selftest: PASS` (task-9).
- Release-mechanics checks at the bump:
  `cargo metadata … uc_node … .version` → `2.8.1`; `cargo build --workspace`
  → exit 0; `cargo package -p uc_protocol --allow-dirty --no-verify` →
  `Packaged 25 files, 363.6KiB`.
- `cargo fmt` was not run — the project-wide deferral is unchanged.

### CI and nightly

**No workflow matrix change was needed.** `capstones` runs
`cargo test --workspace`, which picks up `lin_v2`'s `two_fsm_*` and
`lin_partition_v2`'s two-FSM scenario; `crashtest` runs
`cargo test -p uc_crashtest --features hard-crash-tests`, which picks up both
new SIGKILL scenarios; `elle` and `elle-crypto` both run
`scripts/elle_check.sh` with no arguments, whose default pass list now includes
`quiet_two_fsm`. Only the stale "5 passes" comments and step names in
`.github/workflows/nightly.yml` (and `scripts/elle_check.sh`'s usage line,
`docs/how-to/investigate-a-failed-run.md` and `docs/VERIFICATION.md` §4)
changed, to six.

**CI + nightly evidence (post-merge, 2026-08-30, `main` = `b2035c7`).**
`ci.yml` run `33328724115` on the merge push: `test` / `deny` /
`publish-check` / `msrv` all success. `nightly.yml` run `33329230537`
(workflow_dispatch at `b2035c7`): every job success — capstones (the whole
workspace suite including the seven two-FSM capstones, on the 4-vCPU
runner), crashtest, crashtest-crypto, survival, sim-heavy, loom, miri,
lean-proofs, elle and elle-crypto (six passes each, `quiet_two_fsm`
included), quickstart, fuzz-groups and the four fuzz legs. One disclosure:
attempt 1's `crashtest-crypto` failed on `remote_lin_envelope_off` —
the WGL checker's visited-state budget (5 000 000) exhausted, verdict
Inconclusive, no violation. That is a pre-existing intermittent (the
2026-08-28 nightly `33184711408` failed identically, crypto **off**, at
`4347bc2`, before this branch existed; the test and the remote path are
untouched here; the two new two-FSM crashtests passed in the same job;
two local runs pass in ~20 s). The leg was re-run and passed; the flake
stays open in the tracked list above (a budget retry or a lower op
target is the fix the test's own panic message names).

## v2.8.0 — 2026-08-30 — M14 multi-service: one log, N state machines

**One replicated log, up to eight state-machine processes per node.** M14a
(`main` 6111257) put the FSMs on the control page and bound them together;
M14b (`4347bc2`) gave the client per-FSM routing and a fan-in; M14c
(`b3f1053`) shipped the snapshot session that carries every FSM's artifact,
the per-FSM metrics, and a client hot-path investigation that refuted its own
premise. Spec:
`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` — §1–§9 the
design, §14 the M14c as-built amendments, §15 the M14d gate and release
scope. Gate: `docs/benchmarks/uc2-m14-gate-2026-08-29.md`. Explainer:
`docs/notes/uc2-m14-multi-service-explained.md`. The per-sub-milestone SDD
ledgers are the execution records at the end of
`docs/superpowers/plans/2026-08-27-uc2-m14a-foundation.md`,
`2026-08-27-uc2-m14b-query-routing-and-fan-in.md` and
`2026-08-28-uc2-m14c-perf-wire-observability.md`.

**What this release touches at the wire and page level — two flag days.**
`uc_protocol::version::CURRENT` moves `0.5.0` → `0.6.0`
(`uc_protocol/src/version.rs:65`) because `SNAP_BEGIN`'s payload grows:
`SNAP_BEGIN_FIXED_LEN` **26 → 34** fixed bytes
(`uc_protocol/src/v2/datagram.rs:170`), reusing the 0.5.0 pad rather than
appending — `[4] layout:u8 = 1 · [5] service_id:u8 · [6..8] zero · [8..16]
snapshot_pos · [16..24] total_len · [24..32] services_declared:u64 · [32..34]
config_len · config [34..]` (spec §14.3). Every other datagram — the 16-byte
header, `DATA`, `NAK`, `AppendPosition`, `TermMap`, the admin datagrams — is
byte-identical to `0.5.0`. The cnc page goes `2.0` → **`3.0`** and
`CNC_PAGE_LEN` 4096 → **8192** (`uc_protocol/src/v2/cnc.rs:51`): page 1 keeps
its byte layout exactly (every existing offset test holds, including M13's
`offsets_do_not_overlap`), page 2 is `ServiceSlot[8]` at
`CNC_OFF_SERVICE_SLOTS = 4096`, stride 512 B, and page 1's **last free line**
takes the 4032 pair — `services_declared: u64` at 4032 and `fsm_lag_bytes:
u64` at 4040, one writer, the pattern M13 set with 3968/3976. **Page 1 is
thereby full**; a further page-1 field grows the file by another 4 KiB behind
the next cnc major (spec §3.2). The client↔gateway remote protocol stays v1
and the log frame header is untouched (spec §6.3).

### M14a — the foundation (`main` 6111257; plan HEAD `bbac7a8`)

- **Page 2, `ServiceSlot[8]`** (spec §3.1). Eight 64-byte lines per slot, one
  writer each: `status` (`service_id | attached | incarnation`), `applied`,
  `epoch`, `output_completed`, `snapshot_pos`, `heartbeat_ns`, `lag_waits`,
  and line 7 reserved. Offsets pinned in **both** `uc_protocol::v2::cnc` and
  `uc_log::cnc` with the const-asserts and tests the `PeerSlots` band has.
- **Page 1's singular fields become node-written aggregates** (spec §3.2):
  `service_applied` (512), `output_completed` (640), `service_heartbeat_ns`
  (960) and `service_snapshot_pos` (1152) are now the **min over declared
  ids**, computed once per consensus poll; `service_epoch` (576) is retired
  and held at 0, readers moving to the slot. `uc2ctl status`, the M10 metric
  families, the dashboard and the purge floor keep reading one number whose
  meaning is now "the slowest FSM", and the one-writer-per-line rule survives
  (the writer changed from the service to the node; it did not become shared).
- **`[services]` in `node.toml`** (spec §3.3): `ids` (absent section ⇒ `[0]`)
  and `fsm_lag` (a byte size, or `"lockstep"`; default `buffer_bytes / 4`),
  with M9-style named startup refusals before any file is created — empty
  `ids`, a duplicate id, an id ≥ 8, an unparsable `fsm_lag`, and `fsm_lag >=
  buffer_bytes / 2` (a bounded policy must provably keep the FSMs on the ring:
  half the ring is the conservative bound that still leaves room for the
  leader's admission window).
- **The lag barrier** (spec §4.2), one step in `apply_cycle` before each
  frame at `[p, p + len)`: `floor = min(slot[i].applied)` over declared ids,
  then `lockstep: wait while floor < p` / `bounded: wait while p + len − floor
  > fsm_lag`. It reads only shared memory, is role-agnostic, keeps the
  heartbeat ticking while waiting, counts `lag_waits` per episode, and **does
  not apply during journal replay** — a replaying FSM is the one holding the
  min down. As-built errata, both in §4.2: (1) the pairwise bound is a target
  cap on *live* apply — on a follower, a sibling that falls off the ring
  rejoins via `replay_into` to the archived frontier, so the excursion is
  possible there and harmless (responses are leader-only), while the leader's
  admission door enforces the bound before a frame is even appended, so the
  leader's own FSMs never see it; (2) a lockstep `Wait` is served out of line
  by `lockstep_wait` — spin, then yield with a heartbeat refresh, and only
  then the agent's 50 µs sleep — because a lockstep FSM that sleeps on a live
  sibling stalls every sibling's next frame and the whole set cascades into
  sleeping in lockstep. Measured on the dev box (smoke, not a bar): **18 k →
  631 k frames/s at N=2** (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`).
  A *bounded* `Wait` still goes straight to the sleep: it is `fsm_lag` bytes
  ahead of the slowest FSM, and spinning on that FSM's `applied` line slows it
  (−6 % at N=8 when tried).
- **Q — the quorum-gated durable report** (spec §5.3). The node's published
  frontier becomes `ceiling = min(sm.validated_up_to(), min_applied +
  fsm_lag_eff)`, attested with `term_map.term_at(ceiling − 1)`, stored term
  first then position, both `Release` — the 0.5.0 content-attestation
  ordering, unchanged, and `ceiling ≤ validated_up_to` always, so a node never
  attests content it has not validated. The receiver in `uc_net` is
  untouched. Consequence: if a **commit quorum's** FSMs fall more than
  `fsm_lag` behind, the leader's `CommitTracker` cannot advance and its
  admission door closes — cluster-wide back-pressure; a lagging **minority**
  does not stall the cluster, it falls to journal replay and rejoins.
- **Per-id everything** (spec §4.4, §5.5, §7.5): `service.<id>.lock` held for
  the service's life (`uc_service/src/attach.rs:95`), `snapshots/<id>/`,
  `state/output_progress.<id>.state`, and, created by the node,
  `svc_query.<id>.ring` + `egress_service.<id>.broadcast` per declared id. The
  legacy singular ring names are **not** created. M11's offline
  backup/verify/restore learned the `snapshots/<id>/` layout in the same
  sub-milestone.
- **The boot reservation formula** (spec §5.5). Each declared id adds a 1 MiB
  `svc_query.<id>.ring` and a 4 MiB `egress_service.<id>.broadcast`, all
  fallocated so ENOSPC stays a named startup refusal, so the reservation is
  **`buffer_bytes` + 14 MiB + 5 MiB × (N − 1)** (+4 KiB for page 2) — about
  113 MiB at N = 8 with the default buffer, against ~78 MiB at N = 1.

### M14b — routing and fan-in (`main` 4347bc2; plan HEAD `efc5339`)

- **The query codec splits** (spec §5.4, §6.3): `query.ring`'s `MSG_V2_QUERY`
  payload becomes `service_id:u8 ++ query bytes`; `drain_query_ring` forwards
  to that id's `svc_query.<id>.ring`. The `svc_query` payload
  (`expected_epoch ++ query`) and the log frame header are unchanged, and the
  M13 MPSC record framing is untouched — the service byte lives inside the
  message payload.
- **`MSG_V2_BAD_SERVICE`** (payload `service_id:u8`, on
  `egress_node.broadcast`): a query naming an undeclared id is **answered**
  instead of parking until the client's deadline.
- **The slot table grows two masks and a flag**: `expected` (the rings a
  request awaits) and `received`, plus `fan_in` as a separate `AtomicBool` —
  execution Ruling A, because deriving fan-in from `expected.count_ones() > 1`
  breaks on a node declaring only FSM 0, where `try_submit_all` has
  `expected = 0b1` and would have completed as a single response. Completion
  is `received == expected` or a ring-less terminal, whichever comes first;
  exactly-once still rests on the single owner CAS, not on `received`
  (`uc_client/src/slots.rs` module invariants 7 and 8).
- **The fan-in buffer is `PollHalf`-owned**, one entry per slot index, and
  resets on a generation's **first** piece rather than on a sequence change —
  execution Ruling E: a partial fan-in abandoned by a ring-less terminal or
  the deadline sweep leaves pieces behind, and the generation 2^32 requests
  later at that index carries the same u32 wire seq, so a seq key cannot tell
  them apart while `first` (decided by the slot table) always can.
- **The API**: `submit_to(id)` / `try_submit_to`, `submit_all` /
  `try_submit_all` (`FanInTicket<R>` → `Vec<(u8, R)>` ascending by id, one
  ticket, one completion), and `query_snapshot_on` / `query_linearizable_on`.
  An undeclared id fails locally with `ClientError::ServiceNotDeclared`
  before touching a ring. `Engine::attach` reads `services_declared` off the
  page and opens one egress consumer per declared id; `poll` round-robins
  them ascending, then the node ring.
- **Sim invariant 10** (M14b plan Task 7): the four inline `Msg::Report` sites
  in `uc_sim`'s `world.rs` are funnelled through `send_report`, which clamps
  to the node's `apply_ceiling` — the sim's model of "the slowest FSM's
  applied position + `fsm_lag`" — and runs inv10, which catches a report above
  its unclamped value, above its ceiling, or decreasing without a reset
  (truncation, restart, role change). Two capped-quorum scenarios came with
  it, and the discrimination check is recorded in the M14b execution record:
  with the ceiling ignored, commit ran to 18 912 against a cap of 288.
- **A ruling worth keeping** (Ruling C): a sibling FSM's answer to a
  single-FSM request is *dropped and counted as a duplicate*, not as
  `wrong_ring` — `poll` drains ring 0 first, FSM 0's answer frees the slot,
  and the late piece loses the owner check before the ring check is reached.

### M14c — the client hop, wire `0.6.0`, observability (`main` b3f1053; plan HEAD `74f16bc`)

- **The A/B that refuted its own premise.** M14c was scoped around M14b's
  measured −4.2 % client-hop cost. Rebuilding the identical two commits and
  A/B-ing the exact binaries back to back (`scripts/hop1_ab.sh`, 6 reps,
  alternated order, fixed sink) read **−0.30 %, +0.31 % and −0.05 %** across
  three configurations, all with overlapping ranges — and a control that A/B'd
  **the same commit built twice** manufactured **+1.02 %**, larger than the
  effect being hunted. The planned three-variant bisection was therefore
  **stopped before it started**: recording three "refuted" variants would have
  been a claim the instrument cannot support. What ships from that workstream
  is Task 1's single-ring fast path — `resolve` skips the `received.fetch_or`
  when `expected == bit`, since a set of one is opened and closed by its only
  piece — kept for its measured **tail** win (p90 3 → 2 µs) at a rate delta
  of −0.05 %, OVERLAP. The standing rule this produced, now CLAUDE.md's third
  benchmarking bullet: **build the same source twice and A/B those binaries
  first**; treat anything smaller than that control's spread as unmeasurable
  on the box. Record:
  `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md` (dev box, smoke).
- **The snapshot session becomes a stream of artifacts** (spec §14.3). One
  `SnapSession`/`SnapIntake` as before, but for every declared id in ascending
  order: one `SNAP_BEGIN` naming the id, that id's newest artifact position
  and length, followed by that artifact's chunks. **Chunk offsets are
  stream-global** — the session is one concatenated byte stream with artifact
  boundaries announced by the BEGINs — so `SNAP_NAK` repair is byte-identical
  to `0.5.0`. The receiver writes each artifact to
  `snapshots/<id>/incoming-<pos>.part`, renames on completion, tracks
  `received` against the BEGIN's `services_declared`, and adopts the floor
  **only when `received == services_declared`**, so no FSM is ever stranded
  below an adopted floor. Two named, counted refusals on the receiving node:
  `layout == 0` → **`peer wire 0.5.0`**, and a mask that is not this node's
  declared set → **`declared-set mismatch`**; both drop the session, the
  follower keeps NAKing, and the operator sees the counter
  (`Node::snapshot_session_refusals`).
- **The flag day, stated honestly** (spec §14.3's "§3.4 correction"). The
  16-byte datagram header carries **no version field**, and
  `uc_protocol::version::CURRENT` has no caller on any receive path — it is
  documentary. So the spec's original "a 0.5.0 peer refuses a 0.6.0 datagram
  at the existing version check" is false. The truth: a mixed cluster
  replicates and elects normally, and only a snapshot session between versions
  goes wrong — detectable only on the **0.6.0** side, via the `layout` byte. A
  0.5.0 receiver of a 0.6.0 BEGIN misreads `config_len`. The flag day is real
  and rests on the standing operational rule (upgrade all nodes together),
  not on a check that does not exist. Adding a real header version field is
  out of scope — the header is full, and it would be its own flag day.
- **Two deliberate strictnesses** (spec §14.3/§14.4 errata; neither is a bug
  to "fix"): the receiver validates `services_declared` on **every**
  `SNAP_BEGIN`, not only the first, because a session whose later BEGIN
  disagrees with its first is exactly the mixed/forged case the refusal
  exists for; and `service_detached` also fires when the slot's ATTACHED bit
  clears, not only when the heartbeat ages past the wedged threshold, so an
  orderly stop is not reported a stale-window late.
- **Per-FSM observability** (spec §14.4). Labelled twins via the existing
  `push_labeled` (`service="<id>"`) — the peer-slot band's mechanism, not a
  new one; the unlabeled aggregates keep their names and now mean "slowest
  FSM". New families: `uc_service_attached`, `uc_service_lag_bytes`,
  `uc_service_lag_waits_total`, `uc_services_declared`, `uc2_fsm_lag_bytes`
  (0 = lockstep) — all in `CONTRACT_SERIES`, so the presence test and the
  `m10_gate` live scrape cover them. Two alert rules with `m10_alerts`
  scenarios that prove them firing: `Uc2ServiceAbsent` and
  `Uc2ServicePinnedAtLagBound` (`scripts/m10_alert_fire.sh` 16/16, both
  `state=real`). `uc2ctl status` prints a per-service table off the page it
  already opens, and `service_attached` / `service_detached` land as
  transition records.
- **A documented limitation, not a fix** (execution Ruling K):
  `uc_service_lag_waits_total` reads **0 while an FSM is parked at the
  bounded barrier** — M14a's known undercount, surfaced now that the counter
  is exported. The writer is in `uc_service`, which M14c does not touch; the
  limitation is written into `monitor-a-cluster.md` and `diagnose-a-node.md`,
  the alert keys on `lag_bytes` instead, and the service-side fix is M14c2.
- **Fixed on the transfer plane**: a `SNAP_BEGIN` resend no longer refreshes
  the session's activity clock (a dead peer would have pinned the session
  slot), and a `SNAP_NAK` that is unservable forever no longer keeps a dead
  session alive (`uc_net/src/sender.rs`'s
  `an_unservable_snap_nak_does_not_keep_a_dead_session_alive`); snapshot
  intake I/O failures are retried and counted
  (`uc2_snapshot_intake_io_failures_total`).
- **Evidence across the M14c finish** (`2ef480d`, then the fix wave
  `74f16bc`): `cargo test --workspace` 1 411 passed / 0 failed (102
  binaries) at `74f16bc` (`2ef480d` read 1 407, before the fix wave);
  `lin_v2` 7/7 and `lin_partition_v2` 7/7 Linearizable;
  `uc_crashtest --features hard-crash-tests` green; sim-heavy 38/38;
  `m10_gate coverage` 72/72; fuzz `uc_protocol_datagram` 51 019 361 runs
  clean. All single-FSM capstones — see *Deferred* for what two FSMs still
  lack.

### The gate

`docs/benchmarks/uc2-m14-gate-2026-08-29.md`, bars pre-committed verbatim
from spec §15.4 before any run, per the honest-failure protocol M7–M13 use.
Topology (spec §15.2): four `c6id.2xlarge` in one placement group,
`us-east-1`, NVMe journals, fsync on — M13's shape, three voters plus a
learner idle until row f. The measuring client is the direct `Engine`, which
is shmem-attached and therefore runs **on the leader host**, exactly as the
M12 and M13 direct arms did; rate is completed ops/s over the middle 8 s of a
12 s arm at `--inflight 4096`, 64-byte payload, session envelope on, fan-in
whenever two FSMs are declared (one completion = every declared FSM
answered). Rows a–f are fleet-only; row g is CI.

| row | what it compares | bar |
|---|---|---|
| a | two bounded `CountSm` FSMs (`n2eq`) vs one (`n1`), same run | ≥ 0.90 |
| b | `CountSm` + `SpinCountSm(K)` (`pair`) vs `SpinCountSm(K)` alone (`slow1`), same K, same run | within [0.90, 1.10] |
| c | after every arm, every FSM on every host answers the same count | any mismatch = FAIL; blocks the release |
| d | SIGKILL FSM 1 on the leader host under fan-in load, restart at once | ≤ 15 s to a confirmed 2 s window ≥ 80 % of baseline, FSM 1 re-attached with lag ≤ bound |
| e | lockstep arms vs their bounded twins | reported, no bar |
| f | a learner declaring `{0,1}` joins a purged two-FSM leader under load | ≤ 60 s, `snapshot_session_refusals() == (0, 0)` on every node, both artifacts on the learner, row c on the learner |
| g | `ci.yml` and the newest `nightly.yml` at or after the gated commit | green; the doc states the M14c2 deferral |

`K` is not fixed in advance: a calibration ladder (`SpinCountSm(K)` alone at
K ∈ {250 … 8000}) runs first and picks the K whose rate is nearest 0.5 × `n1`,
and row b then compares `pair` against `slow1` **at that same K in the same
run**, so the bar does not depend on where the calibration lands. Row d reuses
M9's recovery rule verbatim (`m9_fleet_gate.py:343-379`), read off the
client's own per-second timeline on the host being killed, so there is no
clock skew. Row f runs the voters at `PurgePolicy::BelowSnapshot {
slack_bytes: 0 }` with 16 MiB journal segments and a 32 MiB snapshot interval,
so the joiner is genuinely below the floor and must converge by a snapshot
session. Driver: `bench-infra/scripts/m14_fleet_gate.py` (`--selftest` checks
the verdict arithmetic with no fleet).

**Results: the fleet run happened on 2026-08-29** (4 × `c6id.2xlarge`,
us-east-1a; voters `54.167.140.44` / `54.210.38.235` / `3.80.224.32`, learner
`54.90.243.67`; gated commit `711bf58`; driver log retained at
`~/.cache/uc2-m14-gate-2026-08-29.log`). **The driver's verdict was
`RESULT: FAIL (honest) — 1 of 6 rows missed: ['d …']`.** The bar was not
touched.

| row | result |
|---|---|
| a | **PASS** — 0.961 (`n2eq` 1 309 702 / `n1` 1 362 555 ops/s), bar ≥ 0.90. A second bounded FSM behind the same log costs 3.9 %. |
| b | **PASS** — 1.015 (`pair` 774 043 / `slow1` 762 272 ops/s at K = 500), bar [0.90, 1.10]. The bounded pair converges to the slow FSM's solo rate. |
| c | **PASS** — 57 `check-fsms` invocations, zero mismatches, every arm. Includes the kill arm, where the SIGKILLed-and-rebuilt FSM's count matches the survivor's and both remote hosts' in both read modes. |
| d | **FAIL** — the attach clause was met at **21.6 s** against a ≤ 15 s bar, and the client's rate never recovered inside the arm. Diagnosed; bar kept; re-specified and re-run 2026-08-29 — result: see below |
| e | **reported** (no bar) — lockstep at **0.0166×** its bounded twin (21 707 vs 1 309 702 ops/s), i.e. **60×** slower; `pair-ls` 0.0282× (36×). |
| f | **PASS** — the two-FSM learner joined in **24.12 s** (bar ≤ 60 s), `snapshot_session_refusals() == (0, 0)` on all four nodes, both artifacts (45 121 B each) present under `snapshots/0/` and `snapshots/1/`, one `snapshot_installed`, and the learner passes row c. |
| g | **pending** — the gated commit is on an unpushed branch, so no `ci.yml` or `nightly.yml` run exists at `711bf58`. The newest green nightly on `main` is run `33246873016` on `5242054` (`crashtest`, `survival`, `crashtest-crypto` and every other job green); the 2026-08-28 failure `33184711408` on `4347bc2` was closed by `a4a7a9c`. |

**Row d, in one paragraph.** The driver restarts the killed FSM with **no
snapshot policy** (`service_args(h, 1, K, 0)`), so the fresh in-memory SM
rebuilds by replaying the *whole* journal — ~11.9 M commands, roughly 1.3 GB —
where a service configured with a `SnapshotPolicy` would install an artifact
and tail-replay one interval. Meanwhile `uc_service`'s replay path
deliberately **suppresses leader-publish** (`replay.rs:44-46`: those responses
were already answered by the previous incarnation), so the client's up-to-4 096
in-flight *fan-in* requests — each of which completes only when **every**
declared FSM answers — never received FSM 1's half and could only retire on
their 30 s `request_timeout`. With a count-based `--inflight 4096` window fully
pinned, the client's rate read 0 for the rest of the 45 s arm no matter how
fast FSM 1 recovered, and `lost` came out at exactly 4 096. M9's 15 s bar,
which row d inherited verbatim, was set for a **node** restart whose services
ran with purge and snapshots on — M9's own gate doc says the restart cost it
budgeted for was "a short tail" replay, and M9 read its rate from a *survivor*,
not from a client that could be pinned by the restarted process. So the row as
specified cannot measure what it claims to. No product defect is implicated:
row c is green on that same arm. The gate doc carries the three-layer diagnosis
(harness / client / spec, each labelled FACT or HYPOTHESIS) and the re-specification — purge on and a snapshot policy on the row-d FSMs,
and a measuring client that does not wait on the killed FSM — applied as a
separate pre-committed step and re-run the same day (result below), following
the M12 and M9 precedent of recording the FAIL and keeping the bar.

**Row d was re-specified and re-run on 2026-08-29.** The maintainer adopted
the re-specification: the row-d arm now runs a 32 MiB snapshot policy on both
FSMs **together with purge below the snapshot floor** — a `SnapshotPolicy`
shortens a service restart only with purge, because reconstruction installs the
newest artifact only when the journal no longer covers the start position
(`uc_service/src/replay.rs:73-78`), so with purge off the restart replays the
whole journal however often it snapshots — and the measuring client submits to
**FSM 0 only** rather than fan-in (so the rate clause reads the bounded lag
barrier releasing, not the client's 30 s request timeout). **The ≤ 15 s bar is
unchanged and run 1's FAIL stays in the record**; the re-specified row starts
its own honest-failure clock. →
[M14 gate § row d](benchmarks/uc2-m14-gate-2026-08-29.md#row-d--the-fail-diagnosed).
Result of run 2:

Row d alone was re-run on the re-specified procedure (purge on, 32 MiB
snapshots on both FSMs, FSM-0-only client) on **2026-08-29** at commit
`6228365`: **PASS** — recovered at 5.5 s and attached+caught-up at 7.9 s,
both well inside the unchanged ≤ 15 s bar; run 1's FAIL under the original
procedure stays in the record and is not superseded. →
[M14 gate § Run 2 (re-specified)](benchmarks/uc2-m14-gate-2026-08-29.md#run-2-re-specified).

Dev-box smoke, which sets no bar and does not predict the fleet's shape:
`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md` (the FSM hop alone —
bounded mode free of N, and the 18 k → 631 k lockstep fix, measured with the
FSMs alone on the box) and
`docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md` (the client hop A/B and
its same-source rebuild control).

### Deferred

- **M14c2 — the two-FSM proof tier, a proof-only `2.8.1`** (spec §15.1, a
  2026-08-29 ruling that reversed §14.1's order). `2.8.0` ships multi-service
  with the coverage VERIFICATION §11 states — unit tests, in-process
  integration on one node and a 3-node cluster (`uc_node/tests/services.rs`,
  `learner.rs`'s two-FSM join, `uc_net/tests/snapshot_session.rs`'s
  two-artifact stream), the M14b sim scenario, and the fuzz seeds — and says
  so. What M14c2 adds: `lin_v2 two_fsm` (lockstep, bounded, a slow-FSM
  oracle), `lin_partition_v2` with two FSMs, the two hard-crash scenarios, and
  the Elle clean tier with two FSMs, plus the M14c plan's own "Deferred to
  M14c2" list — the receiver-side intake timeout, the `snap_chunk` write-failure
  counter and the publish re-drive cadence (execution Ruling M), the sender's
  refusal-path unit tests, and the `lag_waits` bounded-mode undercount
  (Ruling K). **This is a disclosed gap, not a claim.**
- **The M14a and M14b deferred minors** are listed, each with what it costs if
  wrong, in the "Deferred to M14b/M14c" and "Deferred to M14c" sections of the
  two plans' execution records
  (`docs/superpowers/plans/2026-08-27-uc2-m14a-foundation.md`,
  `2026-08-27-uc2-m14b-query-routing-and-fan-in.md`). None is on a correctness
  path.
- **Out of scope, unchanged since §14.5**: a datagram header version field, a
  remote-protocol service selector (a protocol-v2 item — in stage 1 a remote
  client always gets FSM 0's answer, spec §6.4).

### Three behaviours that live only in rustdoc

The M14b execution record asked for these to be carried into the release
writeup, because they are real, user-visible, and documented nowhere but the
API docs.

1. **A query at exactly the payload cap is refused.** The cap describes the
   **wire** payload, and a query now carries its one-byte service id, so a
   query body of exactly `max_payload` bytes fails with
   `PayloadTooLarge { len: n + 1, max: n }` — deviation 6, documented at
   `uc_client/src/engine.rs:429-436` (and the inherited-cap rationale at
   `engine.rs:74-84`).
2. **A parked driver is woken only by FSM 0's ring.** `PollHalf::wait_handle`
   returns ONE futex — the lowest declared id's egress broadcast — so a
   completion that lands solely on another FSM's ring resolves at the park
   timeout (≤ 1 ms in the pipelined driver) rather than at the publish;
   deviation 3, documented at `uc_client/src/engine.rs:680-690`. The gateway
   only ever issues FSM-0 requests, so its driver is unaffected.
3. **`submit_all` against a declared-but-unattached FSM times out every
   fan-in.** `try_submit_all` sets `expected` to the page's *declared* set
   (`uc_client/src/engine.rs:506-509`) and completion is `received ==
   expected` (`uc_client/src/slots.rs`, module invariant 7), so an id that is
   declared but has no process attached never contributes its piece and every
   fan-in resolves via the engine's deadline sweep as `Outcome::TimedOut` —
   the only way a `Timeout` can reach a ticket at all
   (`uc_client/src/ticket.rs:179-184`). Wait for every declared FSM to attach
   before fanning in.

## v2.7.0 — 2026-08-26 — M13 remote path: performance and flow control

**Three defects, one milestone.** Located by
`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`, a per-hop isolation bench
that put a dummy sink behind each hop and a minimal driver in front of it, so
the bottleneck was found by subtraction rather than inferred from an
end-to-end number. Spec:
`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`. Gate:
`docs/benchmarks/uc2-m13-gate-2026-08-24.md`.

**Nothing in this release touches consensus, the node-to-node wire protocol,
or the cnc page layout.** `uc_protocol::version::CURRENT` stays `0.5.0`. The
remote wire protocol stays v1, with two clarifications to its reference (a
`credits` value MAY decrease and is honoured immediately for new seqs;
`STATUS` MAY be sent at any time) that describe behaviour the client already
had.

### The client (`uc_remote`)

`RemoteEngine::connect` returns `(RemoteSendHalf, RemotePollHalf)`; the old
single-lock `RemoteClient` is now a thin blocking layer over them. Each
connection is three lock-free SPSC structures — an outgoing byte ring (the
writer drains it in batches, one `write_all` per drain), a bounded completion
queue with a body arena (a drop-guard publishes so a panicking drain callback
never re-delivers), and a generation-tagged slot table (owner-CAS gives
exactly-once). The admission window is **count-based**
(`inflight < credits && inflight < max_inflight`, credits bounding unanswered
requests of *both* kinds), not the seq-based rule the old client used — which
starved after `credits` queries because a gateway only advances `acked_seq`
on SUBMITs. A connection *generation* (an `epoch` plus a `FlowWord` packing
generation and credits in one word) gates redial and credit updates, so a
stale redial cannot tear down a fresh connection and a stale grant cannot
overwrite a fresh one. Failover — `REDIRECT` / `LEADER_CHANGED` / `RETRY`, a
not-serving latch, probe-before-flush, resend every live slot on redial — is
generation-gated throughout. `query` now takes a `Consistency`. What moved
from the old client unchanged: the frame codec and the session envelope. The
`engine_fake_edge` / `client_fake_edge` scenarios were ported onto the halves.

### The ring (`uc_protocol::ring::mpsc`)

The MPSC ingress ring moves from publish-in-claim-order to **per-record
commit**. A producer CAS-claims a slot, stamps a LAP-tagged commit word with
the length written *last*, copies the body, then Release-commits; the consumer
reads the commit word and stops at the first record not yet committed. No
producer ever spins on its predecessor, which is what removes the convoy the
`2.6.0` collapse was made of. A producer that dies mid-record leaves a hole
the consumer CAS-marks (so `RingError::Skipped` is exact, never raised for a
delivered record); a torn or impossible word fail-stops the node as
`IngressRingWedged` / `IngressRingCorrupt`, with per-ring `holes_skipped`
counters on the cnc page. A loom model checks four properties (every committed
record delivered exactly once in claim order; a stalled producer never blocks
another's commit; a skip-and-commit race has exactly one winner; a later
claimant overwrites the marker and is still delivered), and a preemption test
reproduces the old convoy's trigger. The ring magic is bumped (`ULTRNG2`), so
a same-host restart re-initialises the ring rather than reading an old-format
buffer.

### The edge budget (`uc_gateway`)

`Shared` carries `budget = max_inflight − max_inflight / 8` and a `live`
count of handshaken connections; a connection's grant is
`clamp(budget / live, 1, per_conn_inflight)`, exported as
`uc_gateway::budget_for` / `uc_gateway::grant_for`. `Conn` gains a dynamic
`ceiling` that `relax` climbs towards, so a connection that relaxes after a
backpressure episode cannot climb past the share its neighbours leave it.

Two ordering decisions carry the invariant "the sum of grants never exceeds
the budget", and they are the whole of the design:

- **On connect**, a handshake takes a `grant_lock` and, in one critical
  section, counts itself into `live`, recomputes every already-attached
  connection's ceiling to the new smaller share, sets its own ceiling, and
  marks itself ready — *then* releases the lock and writes its grant into
  `HELLO_OK`. Doing the whole recompute-and-admit under one lock is what makes
  the invariant hold at *every instant*: an earlier design let the newcomer
  wait for a separate driver pass and publish before that pass had counted it,
  so two concurrent connects could each admit against a stale, smaller `live`
  and over-promise the window (found in review, replaced by the lock — see the
  spec's §5 as-built erratum). The handshake also holds the connection's
  writer across the lock and the `HELLO_OK` write, so a driver `STATUS` can
  never reach the socket before `HELLO_OK`.
- **On disconnect**, the connection leaves the table *before* it leaves the
  budget. The reverse order would let the survivors grow into a share the
  departing connection still nominally held.

The reduction itself is written by the **driver** thread, never by a
handshaking reader: the driver is the only thread allowed to write on a
connection other than its own, and a reader that took another connection's
writer lock could stall for the socket write timeout. A reduction is also
pushed from the reader on its *own* connection the moment `Conn::squeeze`
fires — the call site M12's §4.2 asked for and `edge.rs` never had.

`EdgeStats` gains `grant_changes`. `EdgeConfig::validate` gains
`PerConnExceedsBudget` (a named refusal: one connection must be grantable in
full) and `EdgeConfig::warnings()` gains a `max_connections > budget`
advisory, printed once by the daemon — past that point every grant floors at
1 and the sum stops fitting the window, which is legal and miserable.

### What the 2.6.0 collapse actually was

`docs/notes/uc2-m12a-edge-flow-control-gap.md` blamed the missing budget. The
bench refuted it: the collapse reproduced with the edge's window at 65536 and
at 4096, with a raw client and with `RemoteClient`, against a sink with no
admission window, and at 2,048 total outstanding — inside the envelope that
note prescribed. The cause was the ingress ring's publish convoy. That note
now carries a correction paragraph; `run-a-gateway.md`'s operating envelope
and the `CPUQuota=` advice in the systemd unit are retired, the latter
because CPU containment starved the preempted producer and made the convoy
worse.

### M12 gate row 2

Closed by reference to M13 gate row b. Row 2 compared one TCP client to one
shared-memory client at equal inflight, which the bench showed to be the
wrong comparison; row b is the re-specification the bench recommended.

## v2.6.0 — M12 adoptable cluster — *shipped as `v2.6.0-rc.1`; superseded by v2.7.0, no final tag*

**Four sub-milestones, one tag.** M12a (gateway kit) merged at `185783e`,
M12b (admin authn + audit) at `9897219`, M12c (packaging + publishing) at
`e571c27`, M12d (security posture) on `uc2/m12d-security-posture`. They tag
together as `v2.6.0`; the tag, the `v2.6.0-rc.1` rehearsal that exercises the
release workflow for the first time, and the external review are all separate,
user-owned steps. The two fleet rows have since been run — see *The 2026-08-24
fleet trips* below; row 3 passes, row 2 fails a bar the run showed to be
mis-specified. Gate record:
`docs/benchmarks/uc2-m12-gate-2026-08-22.md`. Spec:
`docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md`, whose four
"As built" amendments are the authoritative correction of the sketch against
what shipped.

**Nothing in this release touches consensus, the node-to-node wire protocol,
or the cnc page layout.** `uc_protocol::version::CURRENT` stays `0.5.0`. The
one cnc change is M12b's 64-byte admin-auth line at `CNC_OFF_ADMIN_AUTH =
3904`, inside the existing reserved band — no version bump, no flag day.

### M12a — the gateway kit (`185783e`)

- **The two-tier state-machine contract.** `RawStateMachine` (`apply(&mut
  self, position, cmd: &[u8], out: &mut Vec<u8>)`) is the core trait;
  `ServiceBuilder` takes `S: RawStateMachine`; the typed `StateMachine` is a
  blanket impl onto it. The decision record is
  `docs/notes/2026-08-22-codec-budget-spike.md`: an isolated codec ladder
  measured serde+bincode's `Vec<u8>` handling at 25–42× a hand-laid frame on
  encode and up to 21× on decode — *not* the format's fault, but serde typing
  a byte vector as a sequence of `u8` and walking it element-wise — and a
  **dev-box** `m5_gate` `apply-profile` run put the typed `CountSm` at
  `sm_apply = 731 ns` per frame (75.8 % of the apply cycle) against the raw
  `RawCountSm`'s 12 ns (5.8 %). Those are shares from a box that is not a
  bench; the fleet run (row 3, 2026-08-24) reproduced the effect on real
  hardware at a 509 B payload — typed `sm_apply` 1173 ns/frame (87.7 % of the
  apply cycle) vs raw 14 ns (8.0 %), ~84×.
  Byte-identity with `v2.5.0` is asserted, not assumed
  (`uc_service/tests/raw_contract.rs`). A cheaper intermediate exists and is
  documented: typing a blob field as `bytes::Bytes`/`serde_bytes` gives the
  identical wire at 1.2–1.9× raw.
- **`Sessioned<S>`** — a 16-byte `client_id ++ seq` envelope, a one-byte
  FRESH/REPLAYED/EXPIRED tag, an LRU dedup table that rides snapshots, and a
  replicated `SessionConfig` that `install_snapshot` refuses to silently
  retune.
- **`uc_remote` protocol v1 and `RemoteClient`** — framed TCP, per-connection
  credits, `REDIRECT`/`LEADER_CHANGED`/`RETRY`/`UNKNOWN`, pipelined submit and
  query, ordered re-send after failover.
- **`uc_gateway::Edge` + the `uc2-gateway` binary + `gateway.toml`** — a
  per-node TCP front door over `uc_client::Engine`, static `node_id → address`
  member map (Aeron's `ingressEndpoints` shape), leader watch off the cnc page.

**Rulings and mechanisms worth remembering.**

- **Redirect, not forward.** A `SUBMIT` that arrives at an edge whose node is
  not serving gets a `REDIRECT` to the leader's gateway address — the edge
  never relays it onward. Queries are answered locally regardless of role.
  This keeps the edge stateless and keeps the leader from having to trust
  another node's framing.
- **The per-connection not-serving latch.** A connection told once that this
  node cannot take writes is told the same thing for every later `SUBMIT` on
  that connection, even if the node starts serving immediately after. The
  invariant it buys: *the set of `SUBMIT`s a connection gets accepted is
  always a prefix of what it sent*. Without it, `Sessioned`'s
  FRESH/REPLAYED/EXPIRED classification breaks on a gap the dedup table cannot
  classify.
- **Probe-before-flush.** A freshly (re)connected client writes exactly one
  request and waits for proof the far end will serve it before releasing the
  rest of its window — and acts on `HELLO_OK`'s named leader first, so a
  pipelined window is flushed *at* the leader instead of being redirected
  frame by frame. `docs/notes/uc2-gateway-shapes-and-flow-control.md` is the
  writeup.
- **The faulted-exit contract.** `InstanceRestart` latches the edge `faulted`
  permanently; the daemon polls `is_faulted` and exits 1, so `Restart=on-failure`
  brings up a fresh edge against the new node instance.
- **Head-of-line blocking is documented, not tuned away.** One driver thread
  per edge serializes outbound writes, so a stalled client's write (up to the
  1 s `WRITE_TIMEOUT`) can delay other clients on that edge. Any fleet cost
  number for gate row 2 includes it.
- **What the capstone does and does not cover.** The remote
  lincheck capstone is `submit` → `wait` at concurrency 4 (an op must be a
  single interval to be a linearizability history entry at all). Pipelining is
  exercised by `failover.rs` and the `m12_gate` harness, not by the capstone.

### M12b — admin authentication and audit (`9897219`)

- **Signed admin requests.** `HMAC-SHA256` over
  `len(app_id) ‖ app_id ‖ instance_id ‖ seq ‖ nonce ‖ op ‖ id ‖ ip ‖ port ‖
  expiry_ns`, every integer little-endian, under a named 32-byte key
  (`uc_crypto::admin`). New reason codes 20 `auth_missing`, 21 `auth_bad_tag`,
  22 `auth_expired`, 23 `auth_unknown_key`, 24 `audit_failed`. `uc2ctl` gains
  `--admin-key`/`--admin-key-name`/`--admin-ttl-secs` on every mutating verb,
  plus `gen-admin-key` and offline `audit`.
- **`audit.jsonl`** — append-only, one `write_all` + `sync_data` per record,
  written *before* the answer is published at every answer site. A failed
  record refuses the request (24) rather than answering it unrecorded.
- **Explicit-choice config.** `[crypto]` and `[admin]` are required sections;
  absence is a named startup refusal (`ConfigError::CryptoChoiceRequired` /
  `AdminChoiceRequired`).

**Rulings.**

- **No `(seq, nonce)` replay ring** — a deliberate deviation from spec §5.2's
  sketch. The tag covers `seq`; the consensus agent only acts on
  `seq > last_admin_seq`, so a capture cannot be re-presented at its original
  `seq` and re-presenting it higher invalidates the tag. A restart resets
  `last_admin_seq` but re-randomizes `instance_id`, which the tag also covers.
  `expiry_ns` bounds the one remaining case (a live, correctly-sequenced
  request delayed in flight). A ring would refuse nothing these two checks do
  not already refuse.
- **`AdminPolicy` is not a `NodeConfig` field.** It lives on `StartOpts`
  beside the optional pre-bound socket — both live process resources, not
  values a `Clone`-able TOML mirror should carry. Library callers get
  `AdminPolicy::Filesystem`, the pre-M12b posture byte-for-byte; only the
  `uc2-node` daemon builds a policy from `[admin]`.
- **The dedup-re-send carve-out.** A byte-identical re-send of an
  already-answered proposal is served from the leader's cache and *counted*
  (`config_proposal_dedup_resend`), not re-recorded — it repeats an answer the
  file already holds. Without this, one captured kind-16 datagram re-sent in a
  loop drove one `fsync` per packet on the consensus thread.

**The review finding that mattered (C1, fixed pre-merge in `50473d5`).**
`verify_admin` originally read `instance_id`/`app_id` from `self.cnc.meta()`
*per request*. The cnc page is a file in the instance directory whose header
is only magic-checked, so an actor with directory write access and **no admin
key** could capture a signed `(auth, req)` pair, await or induce a restart
(resetting `last_admin_seq` to 0), `pwrite` the captured `instance_id` back
into `CNC_OFF_INSTANCE_LO/HI`, re-present the captured lines, and have the
change applied a second time — which also falsified the restart half of the
no-replay-ring argument above. The binding now comes from
`Consensus::admin_instance_id`/`admin_app_id`, set once in `Node::start_with`.
Pinned by `uc_node/tests/admin_auth.rs::a_capture_replayed_after_a_restart_is_refused`,
with anti-vacuity confirmed: reverting the binding makes that replay verify and
reach `propose_config`.

**The residual, stated in four places rather than one.** A follower forwards an
authenticated request to the leader as a `ConfigProposal` (wire kind 16) over
the node-to-node UDP socket. The leader cannot re-verify the operator's HMAC
there — the canonical message is bound to the *requesting* node's identity — so
it records which peer vouched (`peer:<id>`). `on_config_proposal`'s membership
guard drops a datagram whose source address resolves to no current member, but
an address filter is not authentication: with `[crypto].enabled = false`, a
spoofing network-path adversary can inject a proposal. **`[admin] auth = "hmac"`
authenticates cluster-wide only paired with `[crypto].enabled = true`**, and a
flood of *fresh* nonces from a member still costs one `fsync` each.

### M12c — packaging and publishing (`e571c27`)

- **Version identity.** Lockstep `2.6.0` across the workspace, publish metadata
  and path-dep version pins for all 12 publishable crates, `rust-version =
  "1.89"` (probed to `File::try_lock_exclusive`, not guessed) with the pinned
  stable at 1.96.0, and `docs/reference/semver-policy.md`.
- **Supply chain.** `deny.toml` plus two `cargo-deny` passes (default graph and
  `--all-features`, so `uc_service`'s non-default `ultima_db` adapter is
  actually resolved), a CycloneDX SBOM, and CI `deny` / `publish-check` / `msrv`
  jobs. Dropping `snow`'s `std` feature removed `ring` and made the spec's
  "exactly one AES-GCM implementation in the graph" rule true. Dead workspace
  deps (`quinn`, `rustls`, `tokio`, `futures`) removed.
- **`release.yml`** — native x86_64 and aarch64 builds, tarballs +
  `SHA256SUMS` + SBOM, keyless cosign signatures (`--recursive` on the image,
  so a client pulling by platform digest still finds one), a distroless ghcr
  image, and a `release-smoke` publish gate that unpacks the tarball in a bare
  `ubuntu:24.04` with no toolchain and runs `packaging/quickstart-local.sh`
  out of it, then brings up `packaging/compose.yml` as three nodes + three
  services + three gateways and drives `counter-remote` to `value=10`.
- **Docs.** Artifacts-first `docs/QUICKSTART.md`, `docs/how-to/cut-a-release.md`,
  `packaging/README-release.md`.

**Rulings and honest caveats.**

- **Leaves-only `cargo publish --dry-run`.** `publish-check` runs
  `cargo package --no-verify` over **all 12** crates in one invocation (which
  is what forces every path dep to carry a resolvable `version =`), but the
  per-crate `--dry-run` covers only the **4 dependency-free leaves**
  (`uc_journal`, `uc_protocol`, `uc_remote`, `uc_consensus`). A non-leaf
  crate's dry run cannot pass before the first publish — its path deps must
  resolve against the real registry — and this is not only a bootstrap gap:
  `uc_node`'s dev-dependency on `uc_service` is a genuine dev-only cycle no
  publish order avoids. Row 7 therefore claims *packaging* for 12 and
  *publishing* for 4; the full sequence is first exercised by the manual
  ordered publish in `cut-a-release.md` §6.
- **`cargo fmt --check` deferred, per the spec's own condition.** Spec §1 made
  the one-shot reformat conditional on no long-lived branch being open. Two
  worktrees are open (`fix/remaining-flakes`, `worktree-uc2-multi-service`) and
  the reformat measures **2 731 hunks**, every one of which would become a
  conflict in both. The re-run condition is written verbatim in gate row 13.
  `clippy -D warnings` — the gate that catches defects rather than whitespace —
  is enforced on both the pinned stable and the MSRV floor.
- **What CI cannot prove locally, said as such.** Docker, buildx and ghcr do
  not exist on the dev box, so the bare-container run, the image build and the
  compose stack are CI-only; keyless signing needs a GitHub OIDC identity the
  box does not have (a local `cosign sign-blob` would either fail or, worse,
  sign under some *other* identity). Both are first exercised by the
  `v2.6.0-rc.1` tag. And one gap CI does not close at all: `release-smoke`
  runs the **x86_64** tarball only — the aarch64 binaries are built and
  packaged but never executed anywhere, so that half is unclaimed until
  somebody runs the arm tarball on arm hardware.
- **One accepted advisory, written into `deny.toml` with its reasoning:**
  RUSTSEC-2025-0141, `bincode 2.0.1` *unmaintained* — a maintenance-status
  advisory with no patched version to move to. bincode is the wire codec for
  the cnc page, log records and the remote protocol, and the typed tier's
  byte-identity promise is defined against it, so replacing it is a wire-format
  migration, not a hygiene fix.

**Fixed on the way:** `uc_remote`'s `request_timeout` was not enforced while
reconnecting — the sweep now runs between every dial attempt, the per-attempt
connect-shortening (which pinned the dial budget under load) is gone, and the
`HELLO` read is capped at the attempt deadline so the documented
`2 × connect_timeout` bound is literal (`ae0f245`, `fc27536`, `b4b3b0c`). The
architecture doc's log-buffer default was also corrected from a stale
"~512 MiB" to `buffer_bytes`' real 64 MiB.

### M12d — security posture (this branch)

- **A `cargo-fuzz` crate outside the workspace** (`exclude = ["fuzz"]` plus its
  own empty `[workspace]`, because `libfuzzer-sys` needs nightly and the
  workspace pins stable at an MSRV floor), **14 targets** across the datagram,
  log-frame, cnc, remote-frame, crypto (open/handshake/group-key/admin),
  journal (record/stable-value), session-envelope, node/gateway TOML and
  observability-HTTP decoders, a committed seed corpus, `scripts/fuzz_smoke.sh`,
  and two nightly jobs — `fuzz-groups` (asserts the four matrix legs' union is
  *exactly* the manifest's target set, and emits the matrix, so the checked
  list and the matrix are one object) and `fuzz` (600 s per target,
  `--min-runs 10000`, crash artifacts uploaded).
- **Five `uc_protocol` datagram readers made total** — they return `Option`
  instead of relying on caller guards.
- **The security package**: `docs/security/threat-model.md`,
  `attack-surface.md` (19 parser rows), `self-assessment.md`, plus
  `SECURITY.md` and a README **Security posture** / **Scope and limits**
  section. `docs/VERIFICATION.md` gains §7 Fuzzing.

**What the fuzzing found** (numbering matches the self-assessment):

1. **F1** — five caller-guarded readers panicked on short slices. Never
   reachable through the receiver, but the totality of the first code an
   unauthenticated UDP packet reaches held only by the discipline of five call
   sites. Pre-guards kept, hot path byte-for-byte unchanged (`112b81f`).
2. **F2** — `Sessioned::apply` violated the `out`-is-cleared contract it was
   itself a caller of: a contract-abiding inner state machine starting with
   `out.clear()` truncated the session tag away and the slice panicked **on the
   apply thread**, killing the service on its first command. User-reachable
   (`7c908b1`).
3. **F3** — `Sessioned::install_snapshot` pre-allocated up to 1 GiB from an
   unvalidated 8-byte length, using a sanity bound as an instruction. Bounded
   with `take()`; 20 000 executions went 91.8 s → 0.34 s (`7c908b1`).
4. **F6 (the harness finding).** Four of fourteen targets were executing ~16 inputs
   per 60 s run while printing a clean line — `llvm-symbolizer` needed ~90 s to
   index a 27 MB sanitizer binary for one address. `-print_funcs=0` fixed it
   (400 runs: 90 180 ms → 57 ms). **A fuzz tier can be green and vacuous**,
   which is why `--min-runs` exists and why CI asserts it (`736c1f3`).

**Rulings.**

- **Corpus is deterministic seeds only.** Every seed is generated by the real
  encoders in `fuzz/src/seeds.rs` — no captured traffic, no accumulated
  coverage corpus in the tree — so the corpus is reproducible from source and
  reviewable as code.
- **Miri is blocked on the rings, and each blocker was reproduced, not
  assumed.** Miri runs the *pure* decoders (`uc_protocol`'s `v2::` wire/cnc/ipc
  layer and `version` packing, 43 tests; `uc_journal`'s segment and
  `stable_value` decoders, 19 tests) — 62 tests, all passing **with isolation
  left on**. The IPC rings cannot be checked: isolation on gives
  ``unsupported operation: `open` not available``; isolation off gives
  ``unsupported flags for `fallocate` … 16`` (`FALLOC_FL_ZERO_RANGE`, the M11
  block-reservation fix); past both, ``Miri does not support file-backed memory
  mappings``. The spec's fallback — a `Vec`-backed ring variant — was
  **deliberately not built**: it would check a different object than the one
  that ships. The gap is restated in `docs/VERIFICATION.md` §11.
- **Two seams exposed for fuzzing, with their posture stated.**
  `uc_node::config_file::parse_str` and `uc_gateway::config_file::parse_str`
  are ordinary public API (the loaders' pure inner half).
  `uc_node::obs::http::route_raw` and `ObsSources::for_tests` are
  `#[cfg(any(test, fuzzing))]` and absent from a shipped build, with
  `check-cfg = ['cfg(fuzzing)']` declared so `clippy -D warnings` stays clean
  without promoting the seam to a Cargo feature (which would have made it API).
  `uc_journal::fuzz_seams` is `pub` (a separate compilation unit cannot see
  `pub(crate)`) but `#[doc(hidden)]`.
- **`--min-runs 10000` is a stall floor, not a coverage bar.** It catches a
  symbolizer-class stall; it does not catch a target that has merely become
  100× slower. A tighter per-target bar needs per-target numbers from a runner,
  which do not exist yet.

**Two things documented rather than fixed.** (i) A malformed **query** frame
fail-stops a *typed* state machine pre-commit, from an unauthenticated client:
the blanket `RawStateMachine` impl decodes with `.expect("corrupt query frame
(fail-stop)")` and `apply.rs`'s query branch calls it while holding the SM
mutex, so one bad `QUERY` body panics the apply thread and poisons the lock —
no quorum, no leadership, no commit involved. The same `.expect` guards the
post-commit apply path, where fail-stop *is* right, so changing its error
semantics is a design decision; parked as a follow-up, with the raw tier as
the workaround. (ii) The `uc_protocol::ring` buffers have **no interleaving or
UB coverage at all** — an earlier draft's claim that loom covered them was
wrong and was corrected everywhere; the tree's one loom model
(`uc_log/tests/loom_frame.rs`) checks the *log buffer's* frame-visibility
protocol, and nothing checks the MPSC claim-then-commit sequence or the
broadcast seqlock.

### The 2026-08-24 fleet trips: the batching fix, the network budget, and a
confirmed gateway defect

Rows 2 and 3 were run on a fleet (4 × `c6id.2xlarge`, us-east-1 cluster
placement group; driver `bench-infra/scripts/m12_fleet_gate.py`) after the
sub-milestones merged. Two trips, plus two follow-up investigations, on branch
`uc2/m12-remote-batching`. All numbers below are copied from
`docs/benchmarks/uc2-m12-gate-2026-08-22.md`, which stays the authoritative
record.

**The remote-path batching fix** (`74cf53b`, `10132b3`, `f8b4540`, `59db7da`).
The first fleet trip put the single-connection gateway/direct ratio at
**0.072** (session envelope on) / **0.064** (off) — enough of an outlier
against the local smoke picture to look at the write path rather than accept
the number. Four changes, all on the remote hop and none in consensus:

1. `RemoteClient` frame writes are batched into one `write_all` per flush,
   with the flush triggered on the queue emptying rather than per frame
   (`74cf53b`).
2. The edge driver batches the frames it writes per drain of the completion
   ring instead of writing them one at a time (`10132b3`).
3. Both sides do coalesced buffered reads — one `recv` can yield several
   frames, parsed without re-entering the syscall — with `request_timeout`
   and the reader's deadline semantics preserved exactly (`f8b4540`).
4. Admission notifications are coalesced to one per read batch instead of one
   per frame (`59db7da`).

The second trip measured **0.098** (on) / **0.101** (off) — +36 % / +58 %,
~+40 % throughput, 0 lost in both arms of both runs; on the dev box p50 at
4096 inflight fell from ~112 ms to ~10 ms. **The exactly-once and
credit-flow-control invariants are preserved by construction and were
reviewed for it**: batching changes when bytes leave, never which frames or in
what order, and the credit accounting is untouched.

**What the row-2 bar turned out to be.** The residual ~0.1× is architectural,
not a defect and not removable by more batching: it is ONE pipelined
`RemoteClient` over ONE TCP connection (a kernel crossing per operation, and
an edge that relays a connection single-threaded) measured against ONE shmem
`Engine` client at ~1.5M/s with no syscall at all. Little's Law fits both arms
with no residual. The ≥ 0.8× bar therefore compares the wrong two things; the
gate doc records the number honestly and **recommends re-specifying row 2** as
an N-connection edge-saturation ratio (or a per-connection cost plus a
max-connections-per-edge figure). That re-spec has not been done.

**Path 1 — the network budget** (`ba6cad5`, `6fde3d3`, `d6b7750`). One
hypothesis for the ceiling was that the box was already near its NIC limit at
~1.5M/s, which would have made a co-located TCP client a bad idea on its face.
The measurement refutes it: at peak (inflight 4096) 1,424,941 resp/s drives
401.0 MB/s (**3.21 Gbps**) and 392,556 pkt/s — roughly a quarter of the
instance's ~12.5 Gbps burst ceiling — because replication is batched to
~0.28 packets / ~281 bytes per committed command rather than a datagram per
command per follower. **p99 < 1 ms holds to 518,287 resp/s** (inflight 256:
p50 0.472 / p90 0.568 / p95 0.611 / p99 0.660 ms) at ~1.14 Gbps and 157,421
pkt/s, under 10 % of the bandwidth ceiling. A co-located gateway client at
~140k/s adds ~0.3 Gbps and ~40k pps: ample headroom. The ~1.4M/s ceiling is
software (the single apply thread / consensus), not the network — consistent
with every other measurement in this milestone.

**Edge saturation, and the falsification chain that ended in a product
defect** (`1cc0162`, `a7137f5`, `ef6c48c`). Because the single-connection
ratio is a per-connection fact, the interesting question is the edge's
aggregate. A new `m12_fleet_gate.py --row edgesat` ladder scales concurrent
`RemoteClient` connections against one leader edge.

- **First ladder**, run with the edge's own inflight cap deliberately lifted
  to 65536 so the ladder would measure the edge's service rate: near-linear to
  N = 4 (145,600 → 225,166 → **407,722** resp/s aggregate, ~102k/s per
  client, 0 lost), then **collapse** — N = 8 fell 30× to 12,775 resp/s with
  p95 5.8 s, and N = 16 lost 7,960 responses, while the edge process burned
  6.7–7.9 of the host's 8 cores and the client host sat idle at 3 %. The
  harness's automatic "knee" and "saturation ratio 0.009" are artifacts of the
  collapse rung and are disclaimed in the gate doc as such.
- **The obvious hypothesis was that lifting the cap disabled the
  protection**, i.e. a harness artifact. The **clean-discipline re-run** —
  edge inflight back at its row-2 value of 4096, credit ladder active —
  reproduced the collapse almost identically: N = 1/2/4 = 141k / 217k /
  **451k** aggregate (per client ~108–113k, clean), then N = 8 = **10,774**
  (p95 4.3 s) and N = 16 = **3,840** with **9,126 lost**. **The hypothesis is
  falsified.**
- **Confirmed defect** (`uc_gateway/src/edge.rs:753,848,1328`): credits are
  **per-connection**. Every connection is granted `per_conn_inflight` in full
  at `HELLO_OK`, halved reactively on Engine `Backpressure` and relaxed back
  toward the cap when clear. **There is no global budget across connections**
  tied to the node's admission capacity, so 8 clients each asking for 1024 —
  below either cap tested — total 8192 grants against a ~4–6k-frame admission
  window, and the reactive halve/relax ladder oscillates instead of converging
  to a bounded queue. The churn burns most of the shared host's cores, which
  starves the co-located node and service and amplifies the collapse.
- **Fix direction, planned as the next milestone**: a global outstanding-grant
  budget at the edge, sized from (or adaptively probed against) the node's
  admission window and distributed across connections, plus CPU containment
  guidance. **Until then the documented operating envelope is**: total client
  inflight across all connections to one edge stays under the node's
  admission window (`admission_bytes`), and a co-located gateway gets a
  `CPUQuota=`. Within that envelope the edge aggregates cleanly — 451k resp/s
  at N = 4, 0.32× the backend peak. Written up for operators in
  `docs/how-to/run-a-gateway.md` (*Operating envelope (2.6.0)*),
  `docs/reference/gateway-config.md`, `docs/reference/remote-protocol.md`,
  README *Scope and limits*, and a commented `CPUQuota=` line in
  `packaging/systemd/uc2-gateway.service`.

### Gate status at writeup time

| Row | Status |
|---|---|
| 1 remote lincheck capstone | green 3× locally under `hard-crash-tests`; **CI adjudication pending the next nightly** |
| 2 gateway throughput vs direct `Engine` (bar ≥ 0.8×) | **FAIL vs the bar, bar mis-specified** — fleet-run twice; single-connection 0.098 (envelope on) / 0.101 (off) after the batching fix, 4-connection aggregate 451k resp/s = 0.32× the backend peak; re-spec recommended, not done |
| 3 codec share on the apply thread | **PASS** (measurement row, no bar) — fleet 2026-08-24: typed 1173 ns/frame (87.7 %) vs raw 14 ns (8.0 %), ~84× |
| 4 admin authn + audit + replay | **PASS**, per-PR CI |
| 5 quickstart from artifacts, no toolchain | **BUILT, partially proven** — container half is CI-only until the first `-rc` tag; aarch64 unclaimed |
| 6 artifacts and image verifiable | **BUILT, unproven** until the first `-rc` tag |
| 7 crates publishable | **PASS** for packaging (12) and publishing (4 leaves), with the stated dry-run caveat |
| 8 decoder fuzz job green | **BUILT, first nightly run pending** |
| 9 security package present | **PASS** — it claims the package exists and is honest, not that the system is secure |
| 10 external review | **pending**, user-scheduled |
| 11 MSRV floor real and enforced | **PASS** |
| 12 supply chain (advisories/licenses/bans) | **PASS**, one documented ignore |
| 13 `cargo fmt --check` gate | **DEFERRED** — the spec's own condition is not met (2 731 hunks, two open worktrees) |

### Upgrade

- **Per-host config edit, before the binary swap:** add `[crypto]` (with
  `enabled`) and `[admin]` (with `auth`) to every `node.toml`. Absence is a
  named startup refusal. `packaging/node.example.toml` ships both sections
  uncommented and annotated. Full remedy, including the paste that keeps
  today's posture unchanged: `docs/how-to/upgrade-a-cluster.md`.
- **No wire flag day.** `uc_protocol::version::CURRENT` is unchanged at
  `0.5.0`; the cnc page layout is unchanged (M12b's admin line sits in the
  existing reserved band at 3904). The binary swap is still run the way every
  upgrade in this system is run — everyone stopped together, per the how-to —
  but nothing in `2.6.0` *adds* a wire reason for it.
- **The `v2.5.0` instance-directory reservation is unchanged**: ~78 MiB at the
  defaults (`buffer_bytes` + ~14 MiB of rings), reserved at startup, refused
  loudly if unavailable.

## v2.5.0 — 2026-08-21 — M11 survivable cluster

**A cluster survives losing a host, losing quorum, filling its disk, and
being upgraded — and each of those is asserted by a test that actually
destroys something, not described in a runbook.** The milestone's own review
loop and its final gate row turned up four pre-existing journal-layer defects
and two IPC-layer ones; all six are fixed here.

- **Offline `uc2ctl backup` / `verify-backup` / `restore`.** The artifact is
  an ordered copy: state before journal before the log buffer, so a backup
  taken while the node is running under load and racing its own purge can
  still be proven complete. `verify-backup` asserts the purge-straddle
  coverage invariant rather than trusting the copy — a deliberately
  wrong-ordered artifact is reported as a `Hole`, which the gate's
  anti-vacuity test pins. All three verbs refuse a live instance directory.
  The acceptance case is a CI crashtest, not a procedure: a follower is
  backed up under load, its host is destroyed (`rm -rf`), a new host is
  restored from the artifact alone, and it rejoins and converges.
- **`uc2ctl force-single-member` for quorum loss.** An offline, explicitly
  non-persisting recovery wrapper: it states the data-loss window before
  writing anything, and refuses the doubly-ahead crash window outright.
  Dropped peers rejoin as fresh ids with fresh instance directories — the
  runbook's fresh-id rule, enforced rather than documented.
- **Full-disk fail-stop, asserted end to end**, plus a `free_disk_bytes` cnc
  field (reserved band 3840) and the `Uc2DiskLow` alert for the warning
  before the wall. This row is where the milestone earned its keep — see
  "What the ENOSPC row found" below.
- **`scripts/uc2_flag_day.sh`**: stop-all → verify every stopped node agrees
  on `durable` → run the operator's upgrade hook on every host → start-all →
  wait for one serving leader, with a measured downtime number and a
  load-bearing abort path (any failure on the way back up restarts every node
  on whatever binary is in place, so the cluster is never left down). Exit
  codes 0/1/3.

### What the ENOSPC row found

The gate's true-`ENOSPC` row (3b) could not run on the dev box for lack of
passwordless sudo and was carried as `SKIPPED-PENDING`. Its first real CI run
failed — and the pending status turned out to have been concealing a test
that could never have passed, followed by two genuine product defects:

1. **The test could not induce the fault.** Its load is a single serial CAS
   writer, measured at ~15.2 KB/s of instance-dir growth; the fixture left
   8 MiB of headroom, which needs ~550 s to exhaust against a 60 s bound. The
   test now squeezes the remaining space itself after warm-up
   (`squeeze_free_space`, leaving 256 KiB — ~17 s at the measured rate), with
   a 1 GiB interlock so an operator-supplied `UC2_ENOSPC_DIR` pointing at a
   real volume aborts instead of filling it.
2. **A full disk killed processes with `SIGBUS` instead of fail-stopping.**
   `uc_protocol::ring::create_shared_backing_file` zeroed via
   `FALLOC_FL_PUNCH_HOLE`, which keeps the mapped files sparse by design
   (measured: `log.buf` 1 MiB apparent / 80 KiB allocated). A sparse mapping
   has pages with no block behind them, so the first write to such a page
   allocates at **page-fault time**; on a full filesystem that fails, and the
   kernel raises `SIGBUS` — not an `io::Error`, so it cannot be returned,
   matched, or handled. It kills whichever process touched the page — node,
   service, *or* client, since all three map these files — and the documented
   fail-stop chain (journal halt → `ArchiveError` → `agent_failstopped` →
   exit 1) never runs. Observed directly: `code=None signal=Some(7)
   core=true` with no `agent_failstopped` in stderr, and separately the test's
   own client process taking the `SIGBUS` instead.
   **Fixed** with `fallocate(FALLOC_FL_ZERO_RANGE)`, which zeroes *and*
   reserves the blocks as unwritten extents — no zeroes are written, so
   startup stays fast — moving the failure to `fallocate`'s return value,
   where the daemon already refuses to start with a named error. Aeron
   reaches the same answer from the same constraint: sparseness is a knob
   there (`aeron.term.buffer.sparse.file`, "save space at the expense of
   latency") and storage checks are on by default
   (`FileStoreLogFactory.checkStorage`, *"insufficient usable storage for new
   log of length="*). The `fallocate` form is stronger — Aeron's
   `getUsableSpace()` check is look-then-leap and races; a reservation either
   succeeds or reports `ENOSPC` atomically.
   **Upgrade note:** these files are no longer sparse. A default instance
   directory reserves ~78 MiB at startup (64 MiB log buffer + ~14 MiB rings),
   and a node that cannot reserve it refuses to start.
3. **Even a correct fail-stop did not say why.** `uc_journal`'s segment
   preallocator replaced the underlying `io::Error` with
   `Error::other("segment preallocation failed")`, so a full disk halted the
   node without ever naming `ENOSPC`. The failing error's kind and errno are
   now captured and rebuilt for each waiter. This was latent for *every*
   preallocation errno, not just this one.

With all three fixed, row 3b passes as written — named `StorageFull` /
`os error 28`, daemon exit 1, survivors committing throughout, and node 0
rejoining and converging once space is returned — locally and in CI's
`survival` job.

### Journal-layer fixes from the review loop

- **A crash-torn tail refused boot.** `Journal::open` now heals a torn tail on
  the active segment instead of refusing, and zeros the healed span through
  physical EOF so the residue cannot wedge the next truncate.
- **A masked acked-durability hole at segment rolls**: a rolled-off segment is
  now fsynced before its successor exists, making the acked-durability
  guarantee real at the boundary.
- **A latent writer panic**: the dirty flag survives truncation, so an emptied
  segment list no longer panics.

### Gate

`docs/benchmarks/uc2-m11-gate-2026-08-20.md`. Six rows, bar pre-committed at
plan commit `7ff6b4b` before implementation and never edited. Rows 1, 2, 3a,
3b, 4 local/CI; row 5 fleet-only, measured at **14.007 s and 14.709 s**
against a 60 s bar on a 4-host `c6id.xlarge` fleet in us-east-1, with equal
durable positions across every stopped node, no committed-high-water loss,
and 314 KB/s of new writes committed after the upgrade. Driver:
`bench-infra/scripts/m11_fleet_gate.py` — a new one, because every earlier
fleet gate launches nodes as transient `systemd-run` units, which cannot
serve `uc2_flag_day.sh`'s `systemctl start` after its `systemctl stop`; the
M11 fleet installs the shipped `packaging/systemd` unit instead. Three rows
were recorded FAIL on the way and diagnosed before re-running, including one
worth remembering: GNU `install` truncates its destination in place rather
than unlinking it, so an inode-equality witness reports "never replaced" for
a successful install.

Two limits of the fleet row, stated rather than implied: it ran on 4 hosts,
not 5 (the account's 32-vCPU cap, plus three instances that booted with no
networking), and the upgrade installed a byte-identical binary, since there
is one tree — so it measures downtime, not cross-version interoperation.

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
- **Deployment model, stated plainly.** `uc_client` is a same-host SDK: the
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

Measured on the directed rig (`uc_node/tests/stale_read_hunt.rs`, 300 s of
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
  `uc_log`'s `read_frame_validated` (`debug_assert!`-only bounds guard,
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
  violating advance) plus a `uc_consensus` unit pin
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
