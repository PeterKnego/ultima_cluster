# UC v2 — Read-your-writes reads (position-token reads on any node)

**Date:** 2026-10-08
**Status:** design, approved section by section in a brainstorming session;
amended 2026-10-09 with the `durable` bound and the parked-read structure
(§5.1, §6.4), after review raised the denial-of-service question. Awaiting
review of this written form. Implemented on branch `design/session-reads`,
unreleased (see the As built block).
**Base:** `origin/main` @ `b7ebcc5`. Every file/line cited below was checked
against that commit.
**Motivation:** `docs/notes/smr-read-options-compared.md` (branch
`bench/read-spread`) and `docs/benchmarks/uc2-read-spread-2026-10-07.md`.
UC offers linearizable reads (leader-only ReadIndex) and snapshot reads
(local, any node). The read-spread run showed snapshot read capacity scales
with the number of nodes answering (spread/leader-only = 2.74 read-only, 3.65
under 20k writes/s, against a pre-committed 2.4 bar). But a snapshot read on a
follower can miss the caller's own just-acknowledged write. This feature closes
that gap: **read-your-writes and monotonic reads on any node**, at near
snapshot-read cost, with no node↔node protocol change.

---

#### Errata (planning, 2026-10-09) — read these before the body

Found while writing the implementation plan
(`docs/superpowers/plans/2026-10-09-uc2-read-your-writes.md`), each checked
against `b7ebcc5`:

1. **§4.5 is half wrong: the cnc bump refuses only one direction.**
   `version_compatible(local, peer)` accepts `peer_minor <= local_minor`
   (`uc_protocol/src/v2/cnc.rs:593-599`). A 3.4 client on a 3.5 page is
   refused, but a **3.5 client on a 3.4 page attaches**, and would send a
   prefixed record that an old node misreads as query bytes. So the client
   gates the flag itself: it reads the page's header version at attach
   (new `CncPage::header_version`), and on a page older than 3.5 a
   `ReadYourWrites` read with a non-zero token is refused at the door with
   `SubmitError::ReadYourWritesUnsupported` (→
   `ClientError::ReadYourWritesUnsupported`). A token of 0 still goes as a
   plain snapshot read, which every page understands. The jumbo bump (3.2)
   handled the same direction by degrading on a missing word; here degrading
   silently would weaken the guarantee, so it refuses by name.
2. **The client's serving gate blocks every query on a follower, not only
   writes.** `SendHalf::send` refuses with `NotServing` whenever
   `serving_gate` is on and the node is not a serving leader
   (`uc_client/src/engine.rs`), and `PipelinedConfig::default()` and
   `Client::connect` turn it on. So today a default `Client` on a follower
   cannot send even a snapshot read. The gate now applies to **writes and
   linearizable reads only**; `Snapshot` and `ReadYourWrites` reads pass it.
   This also lets snapshot reads through a default `Client` on a follower,
   which is the behaviour the snapshot mode always described.
3. **§7's capstone teeth are restated.** Tooth (b), "the guard disabled while
   `applied` is rewound", cannot be induced on demand in an in-process
   cluster. And with the client guard on, tooth (a) (the node forwards
   without waiting) is *masked*: the guard turns the stale answer into Retry,
   so the checker sees nothing. The teeth become: **T1** node skips the wait
   **and** the client guard is off → the checker must report a violation;
   **T2** node skips the wait with the guard on → the checker must pass
   **and** the engine's stale-answer counter must be non-zero (the guard
   caught what the node let through). The rewind case is covered by a
   synthetic unit test that feeds the engine an answer below the token sent.
   The client's guard switch lives behind `uc_client`'s new
   `mutation-testing` feature, which `uc_node`'s `mutation-testing` feature
   turns on (`uc_client` is a normal dependency of `uc_node`).
4. **The gateway relays parked reads with no extra work.** An engine-side
   Retry for a read-your-writes query already becomes `RETRY` with
   `RETRY_SERVICE_UNAVAILABLE` on the remote wire
   (`uc_gateway/src/edge.rs`, the `Outcome::Retry` arm), and a remote client
   re-sends in place after the backoff. The remote client's own guard (§5.5)
   uses that same in-place re-send instead of resolving the request.

#### As built (2026-10-09) — deviations found while executing

- **cnc version folded into 3.4 (2026-10-10, after merging `main`).** The body
  and Errata 1 describe a bump to cnc 3.5. `main`'s snapshot-lifecycle work
  had meanwhile folded its own cnc additions into the unreleased 3.4
  (released `2.13.0` shipped 3.3) and pinned that in a test, so this feature
  follows the same practice: `CNC_V2_VERSION` stays 3.4 and
  `CNC_MIN_POSITION_MINOR` is 4. The client's own gate (Errata 1) now refuses
  read-your-writes with a non-zero token on a released 3.3 page. Residual:
  an unreleased dev build of 3.4 from before this feature would accept the
  flag and misread it; none was released. Wherever this spec says 3.5, read
  3.4.

Plan execution (tasks 1 to 12, branch `design/session-reads`) followed the
design with these recorded rulings and gaps:

- **R1.** The client's version gate is written
  `((page_version >> 16) & 0xFF) >= CNC_MIN_POSITION_MINOR`; the plan's
  unparenthesised form is a type error under Rust precedence.
- **R2.** `Client::connect` must work on a follower for snapshot and
  read-your-writes reads (Errata 2). Task 7 asserts it, and also exercises
  `Client::query_at_least_on` (explicit token) in the follower test.
- **R3.** Task 7 adds that `query_at_least_on` assertion because Task 6's API
  had only a compile-only doc-test.
- **R4.** `uc_remote` has its **own `ReadToken`** newtype (same `u64`
  representation, same 16-hex-digit `Display`/`FromStr`, `NONE`/`from_u64`/
  `as_u64`), pinned equal in text to `uc_protocol`'s by a dev-dependency
  test. `uc_protocol` stays a **dev-only** dependency of `uc_remote`, the
  crate third parties copy; the plan's `pub use uc_protocol::v2::ipc::
  ReadToken` was a plan defect. A caller bridging `uc_client` and `uc_remote`
  converts through `as_u64`/`from_u64`.
- **R5.** The gateway answers `SubmitError::ReadYourWritesUnsupported` (a 3.5
  gateway beside a pre-3.5 node) with the **transient**
  `RETRY_SERVICE_UNAVAILABLE`, not the permanent `RETRY_PAYLOAD_TOO_LARGE` it
  shares with `ServiceNotDeclared`: it is an upgrade-ordering condition that
  the node's upgrade clears, so the remote client keeps retrying within its
  budget.
- **R6.** The flood smoke (§6.4, §7) as the plan wrote it was vacuous: with
  the follower's service running, at-durable tokens take the fast path and
  never park (peak 0), so the cap was never exercised. The smoke **stops the
  follower's service** for the flood window so `applied` freezes while
  `durable` climbs. Measured: parked peak 4096 (= the cap) reached,
  `refused_cap` about 1.87 M, commit progress under flood 602 to 609 commits,
  forged tokens refused ahead. The test is `#[ignore]`d.
- **Errata 2's premise corrected.** "`Client::connect` on a follower fails the
  serving gate" holds for `PipelinedConfig`/`EngineConfig` defaults
  (`serving_gate: true`), not for `Client::connect`, which has always used
  `serving_gate: false` (`uc_client/src/client.rs:71`). The local SDK also
  does not re-ask on a stale answer: it surfaces `ClientError::Retry`.
- **Not done, recorded.** (a) The perf smoke that the fast path is about equal
  to a snapshot read was not run. (b) The Elle session pass was not attempted.
  (c) There is no unit test that a lost leadership never RETRYs a min-position
  read; it is structurally true (admission reads no leadership state).
  (d) **R7:** the capstone has no snapshot/purge churn arm (gap; the
  install-path jump of `applied` is covered by design and by the client
  guard). (e) **R8:** a remote QUERY carrying both `FLAG_MIN_POSITION` and
  `FLAG_LINEARIZABLE` is a protocol violation; the gateway closes the
  connection, matching §4.1's drop of that combination on shmem.
- **Cross-row integration test not written.** §7 planned a test that a write
  to one row gives read-your-writes on another. With single-node rows the
  other row is trivially caught up, so such a test could not fail; the
  property rests on the shared-cursor argument in §3.3 and on the parked
  heaps being per row.
- **Capstone results** (`uc_node/tests/read_your_writes_capstone.rs`, Errata
  3's teeth): clean run 585 reads, 0 violations; **T1** (node skips the wait,
  client guard off) 34 violations, caught; **T2** (node skips the wait,
  guard on) 0 violations with 112 stale answers caught by the client guard.
  The final-review fix wave closed the churn gap (it now asserts at least one
  isolation and one leader change) and added a read-only token-rotation phase
  that isolates monotonic reads across nodes.
- **Documentation owed (§8) done** except the `smr-read-options-compared.md`
  update, which lives on `bench/read-spread` and is not on this branch.
  `docs/reference/remote-protocol.md` was also moved to v2.

## 1. Goals and non-goals

**Goals**

- A third read mode, `ReadYourWrites`, served by **any** node (leader,
  follower, learner). It guarantees, per client or per carried token:
  - **read-your-writes**: a read reflects every write this client has had
    acknowledged;
  - **monotonic reads**: a read never reflects an older state than a previous
    read this client saw;
  - **writes-follow-reads**: implied, because every write goes through the
    leader in log order.
- **Automatic tokens** inside one client process (`Engine`, `RemoteEngine` /
  `RemoteClient`), plus **explicit tokens** an application can carry across
  processes (in a cookie or header, say).
- Both client paths: local shared memory (`uc_client`) and remote
  (`uc_remote` through `uc_gateway`).
- No new cost for existing reads: a snapshot read and a linearizable read are
  byte-identical on the wire to today and take today's code path.

**Non-goals**

- Linearizability. A `ReadYourWrites` read may miss *another* client's
  recent write. That is the documented contrast with `Linearizable`.
- Follower ReadIndex (linearizable reads on followers). This design builds the
  "hold a read until `applied ≥ P`" primitive it would reuse, but does not
  build the leader round.
- A cluster identity inside the token (see §6.4).
- Per-state-machine tokens (one token covers every row; see §3.3).
- Fixing the read-only idle-ladder throughput cap recorded in the read-spread
  doc §5.4. It is unrelated to correctness here (a waiting read is waiting on
  writes, which keep the apply thread awake), and the maintainer declined that
  fix for now.

## 2. Naming

"Session" is taken: `uc_service::session::Sessioned<S>` is exactly-once
delivery over a remote hop (a `client_id ++ seq` envelope). This feature uses:

- `Consistency::ReadYourWrites`: the client-facing mode, beside
  `Consistency::Linearizable` and `Consistency::Snapshot`
  (`uc_client/src/engine.rs:139`).
- `ReadToken`: the opaque client-side token type.
- "min-position read" / `MinPosition`: the internal name for a query record
  that carries a token.

## 3. The token

### 3.1 What a token means

A token `T` means: **answer only from state whose applied frontier is ≥ `T`**.
The frontier is the per-row word the service already publishes,
`service_slot(row).applied`, which holds the service's cursor: the
**exclusive end** of the last applied frame (`uc_service/src/apply.rs`, the
`applied.store_release(st.follower.cursor)` after each batch).

Every token, whatever produced it, is tested the same way: `applied ≥ T`.

### 3.2 Where tokens come from

Two sources, normalized to the one test:

- **A write's acknowledgement.** The response carries the frame's **start**
  position `p`: the apply loop publishes `pos` (`uc_service/src/apply.rs`,
  `st.egress.publish(hdr.client_id, hdr.seq, pos, …)`), which
  `FrameIter::next` yields as the frame start (`uc_log/src/reader.rs:149-153`).
  The client records **`p + 1`**. `applied` only ever lands on frame ends, so
  `applied ≥ p + 1` ⇔ `applied > p` ⇔ the frame at `p` has been applied. The
  client never needs a frame length. A fan-in write (`try_submit_all`) is the
  same: every piece carries the one commit position.
- **A query's answer.** After this change, an answer carries the service's
  applied frontier at answer time (§4.3). That is already exclusive, so the
  client records it **as is**.

The client's token is the **maximum** of everything it has seen. Write tokens
give read-your-writes; read tokens give monotonic reads.

**Token 0 (or no token) is a snapshot read**: every node satisfies
`applied ≥ 0`. A new client that has seen nothing starts there. That is
correct, since it has nothing of its own to read back.

### 3.3 Scope

One token per `Engine` / `RemoteEngine` (shared by its send and poll halves),
held in an `AtomicU64` raised with `fetch_max`. It is **not** per row: every
row's cursor walks the same log, so a token from a write to row Y is a valid
lower bound for a read of row X. X answers once it has passed that position.
This gives read-your-writes across state machines for free.

### 3.4 Why every token is a committed position

A write is acknowledged only after the leader's service applied it, and apply
is gated on `min(commit, durable)`. A query is answered from the same applied
state. So every token names a committed position. Committed bytes are never
truncated, and every node eventually applies them. This is what makes tokens
valid across leader changes and truncations (§6.1).

## 4. Formats

### 4.1 Client → node query record (`query.ring`)

- New header flag **`FLAG_V2_MIN_POSITION: u16 = 2`** in
  `uc_protocol::v2::ipc`, the next free bit after `FLAG_V2_LINEARIZABLE = 1`
  (`uc_protocol/src/v2/ipc.rs:68`).
- With the flag set, the payload is
  **`service_id: u8 ‖ min_position: u64 LE ‖ query bytes`**.
- Without it, the payload is unchanged (`service_id ‖ query`), so snapshot
  and linearizable records are byte-identical to today.
- **Malformed and dropped** (exactly as a record without a service id is
  today): `MIN_POSITION` together with `LINEARIZABLE`, or a `MIN_POSITION`
  payload shorter than 9 bytes. The SDK never builds either.

### 4.2 Node → service record (`svc_query`): unchanged

Still `expected_epoch: u64 LE ‖ query bytes` (`forward_svc_query`,
`uc_node/src/node.rs`). The node strips the token before forwarding.

### 4.3 Query answers (egress broadcast): fill the position slot

`Egress::publish_query_answer` writes `0u64` as the answer's position today
(`uc_service/src/egress.rs:72`). It will write the service's **applied
frontier at answer time** (the apply thread's `st.follower.cursor` when
`drain_queries` runs). That is the state the query actually read: queries run
on the apply thread, after the cycle's applies (`drain_queries(st)` at the end
of `apply_cycle`).

This applies to **every** query answer, snapshot and linearizable included,
so a linearizable answer also raises the client's token. That is correct and
costs nothing. The client distinguishes an answer's position (exclusive, taken
as is) from a write response's (a frame start, taken `+1`) by
`FLAG_V2_IS_QUERY`, already on the record (`uc_protocol/src/v2/ipc.rs:71`).

### 4.4 Remote protocol (`uc_remote`, client ↔ gateway)

- New query flag **`FLAG_MIN_POSITION: u8 = 0x20`**, the next free bit after
  `FLAG_ENVELOPED = 0x10` (`uc_remote/src/frame.rs:38`), with the same
  `min_position: u64 LE` prefix on the query payload.
- Responses are unchanged: `ResponseMeta.position` already exists
  (`uc_remote/src/frame.rs:294`), and the gateway passes the answer's position
  through it.
- **`PROTOCOL_VERSION` 1 → 2** (`uc_remote/src/frame.rs:19`). An old gateway
  would read the prefix as query bytes, so the new client must be refused by
  name. `HELLO_REFUSED_VERSION` already does that. This retires the standing
  statement that the remote protocol "stays v1".

### 4.5 Versioning

- **cnc 3.4 → 3.5** (`CNC_V2_VERSION`, `uc_protocol/src/v2/cnc.rs:72`), so a
  client or service and a node from different sides of the change refuse to
  attach instead of misreading the query prefix.
- **The node↔node wire does not change.** No cluster-wide flag day for nodes.
  As with any cnc bump, a host's node, services and clients upgrade together.
- `docs/reference/semver-policy.md` gets a line for the new flag, the cnc
  minor bump and the remote protocol version.

## 5. Behaviour

### 5.1 Node (consensus agent)

Both read steps already run on every consensus pass regardless of role
(`uc_node/src/node.rs:4419-4424`: `drain_query_ring()` then
`advance_pending_reads()`). Min-position reads get their own parked-read
structure and their own advance step beside them; linearizable reads and
`pending_reads` are untouched.

**The three positions this section uses** (all per node, all byte offsets in
the one log):

- `durable`: how much of the log is on this node's disk.
  `LogCounters.durable` (`uc_log/src/counters.rs:127`), published by the
  archive agent after the journal sync (`uc_log/src/archive.rs:405-426`).
  Local, and lowered only when the archive truncates an uncommitted tail
  (`archive.rs` truncate paths).
- this node's **view of commit**: `LogCounters.commit`. Monotone, but a
  follower learns it from the leader after the bytes arrive, so it trails.
- `applied`: the row's exclusive apply frontier,
  `applied ≤ min(commit, durable)` (`uc_service/src/apply.rs`, the `head`
  computed at the top of `apply_cycle`).

Every legitimate token is a committed position (§3.4), so it is at or below
the cluster's real commit. It can still be above a lagging node's `durable`,
commit view and `applied`.

**Admission (`drain_query_ring`, `node.rs:9332`)**, for a record carrying
`FLAG_V2_MIN_POSITION`:

1. Parse and strip the token. An unknown row gets `BAD_SERVICE`, as today.
2. **No leadership gate.** Like a snapshot read, it is served on any role.
3. **The durable bound:** if `token > durable`, answer `RETRY` at once. This
   node does not hold the bytes the token names: either it is lagging (the
   write committed on a quorum without it), so a retry or another node is the
   right answer, or the token is forged or from another cluster and nothing
   would ever satisfy it. Either way, nothing is parked. This is the primary
   defence against unsatisfiable tokens (§6.4).
4. **Fast path:** the same capture-recheck B uses. Capture the slot's epoch
   `e`, require `e ≥ 1 ∧ applied ≥ token`, then require the epoch is still `e`.
   If so, forward at once with **`expected_epoch = e`**. This deliberately
   differs from a snapshot read, which forwards `0` ("skip the check"). With the
   real epoch, a service restart between the check and the answer makes the old
   incarnation refuse with RETRY (its existing stale-epoch refusal in
   `drain_queries`), instead of answering from a rebuilt state that may be
   behind the token.
5. **Cap:** if the node already holds `MAX_PARKED_MIN_POSITION_READS` parked
   min-position reads, answer `RETRY` at once. The constant is fixed in the
   plan (order of thousands).
6. **Park:** otherwise the bytes are on this node, and it is waiting only to
   learn they are committed and to apply them. Park the read (below) with
   `deadline = now + READ_BARRIER_TIMEOUT_NS` (1 s, `node.rs:376`).

Both refusals (3 and 5) are counted on a metric labelled by reason (`ahead` /
`cap`), so a lagging node and an attack are visible.

**Parked structure: per-row, cost driven by what is released.** A plain list
scanned every pass would make the consensus agent's per-pass cost grow with
the number of parked reads, which a client controls. Instead:

- a slab of parked reads (`client_id`, `local_seq`, row, query bytes, token,
  deadline), indexed by a small id;
- per row, a **min-heap of `(token, id)`**: the lowest token surfaces first, so
  a high token never holds back a lower one;
- one **FIFO of `(deadline, id)`**: every parked read gets the same timeout, so
  admission order is deadline order and expiry pops from the front.

**Advancing (`advance_min_position_reads`, a new step after
`advance_pending_reads`):**

- **Release:** for each row, while the heap's top has `token ≤ applied`, pop
  it and run the same epoch capture / `applied` check / epoch recheck as the
  fast path, then forward with the real epoch. If `svc_query` is momentarily
  full, leave it at the top and stop for this row until the next pass.
- **Expire:** while the FIFO's front has passed its deadline, answer `RETRY`
  and drop it (entries already released are skipped as tombstones).
- **No leadership dependence.** A min-position read is never RETRY'd for lost
  or changed leadership.

A pass that releases and expires nothing costs one heap peek per row with
parked reads, plus one FIFO peek, whatever the number parked. Released and
expired reads cost `O(log n)` each.

**Why a read still needs a deadline after the durable bound.** A legitimate
token at or below `durable` is committed and always resolves. A forged token
can also be at or below `durable` if it names an uncommitted tail this node
holds. If that tail is later truncated (`durable` drops), `applied` never
reaches the token. The deadline turns that into RETRY.

**Cost on the common path.** On a caught-up node every min-position read is
admitted by one comparison against `durable` and the fast path: three atomic
loads plus the epoch recheck, then the same forward a snapshot read does.
Parking only ever holds reads for bytes the node already has, so parked reads
live about as long as the node's commit-learning and apply lag.

### 5.2 Service

One change: `publish_query_answer` takes the applied frontier and writes it in
place of `0` (§4.3). `drain_queries` passes `st.follower.cursor`. Nothing else
changes. The epoch refusal, the 64-per-cycle drain bound and the record format
all stay as they are.

### 5.3 `uc_client` (local shared memory)

- `Consistency::ReadYourWrites`.
- `Engine` shared state gains `token: AtomicU64`. On each polled completion:
  a write response raises it to `position + 1`; a query answer (any mode,
  `FLAG_V2_IS_QUERY`) raises it to `position`. Both use `fetch_max`.
- Sending a `ReadYourWrites` query: load the token. If it is 0, send a plain
  snapshot record (byte-identical to today). Otherwise set
  `FLAG_V2_MIN_POSITION` with the prefix, and record the **token sent** in the
  request's slot.
- **Client guard:** when an answer to a min-position query has
  `position < token_sent`, the engine reports `Outcome::Retry` instead of the
  answer. That is side-effect-free, so callers' existing retry handling covers
  it, and stale data never reaches the caller.
- Explicit API:
  - `ReadToken`: `Copy + Ord`, `Display` / `FromStr` (hex), `from_u64` /
    `as_u64`.
  - `Engine::read_token() -> ReadToken`: snapshot of the automatic token.
  - `Engine::observe(ReadToken)`: merge a carried token (`fetch_max`).
  - `Engine::try_query_at_least(user_data, id, bytes, ReadToken)`: a one-off
    read with an explicit token, **independent of the automatic token**.
- Blocking `Client` (`uc_client/src/client.rs:106-180`):
  `query_read_your_writes` / `query_read_your_writes_on`, mirroring
  `query_snapshot` / `query_linearizable`, plus `read_token` and `observe`.

### 5.4 `uc_gateway` (Edge)

The gateway **must not** use its Engine's automatic token. One Engine relays
for many remote clients, so automatic tracking would merge their positions and
make every remote client wait on everyone's writes. Instead, a remote query
with `FLAG_MIN_POSITION` is relayed through `try_query_at_least` with that
query's own token. The answer's position is returned in
`ResponseMeta.position`.

### 5.5 `uc_remote` (`RemoteEngine` / `RemoteClient`)

The same mode, the same automatic token (fed from `ResponseMeta.position`,
with the same `+1` rule for write responses), the same explicit API and the
same client guard. No new redirect logic: a RETRY (the node stayed behind past
its deadline) follows `RemoteClient`'s existing RETRY handling.

## 6. Failure cases and edges

### 6.1 Events that tokens survive

- **Leader change.** Tokens are committed positions (§3.4), so a new leader
  leaves them meaningful. A node that already holds the bytes waits until it
  applies them; one that does not yet hold them answers RETRY at once (§5.1).
- **Truncation** removes only uncommitted bytes, which no token can name.
- **Snapshot install** (below-floor joiner, restarted service): `applied`
  jumps forward, and the reads it unblocks are correct.
- **Learners** serve min-position reads like any node. Lag shows up as an
  immediate RETRY while the learner lacks the bytes, and as a short wait once
  it holds them.

### 6.2 When the state behind the check moves

- **Service restart between check and answer.** The forward carries the real
  epoch, so the old incarnation refuses with RETRY.
- **`applied` moving backwards within one incarnation.** The version gate
  rewinds the cursor to a pinned record (`crate::version_gate::rewind_to_record`
  in `apply_cycle`), and a replay path has been reported to rewind `applied`
  briefly. The epoch check cannot see either: same incarnation. **The client
  guard (§5.3) covers it:** the answer carries the cursor at answer time, and
  an answer below the token sent becomes Retry. This case is why the guard
  exists.

### 6.3 When a node cannot catch up

- **Multi-FSM lag barrier.** A row held behind a slower sibling may not reach
  the token before the deadline, so the read gets RETRY. That is the barrier
  working as designed, and it is documented.
- **Halted or removed node.** `durable` and `applied` stop. Tokens above
  `durable` get RETRY at once; tokens between `applied` and `durable` get RETRY
  at the deadline. Snapshot reads keep answering stale data (as today).
- **Booting node.** The existing `NodeBooting` attach refusal applies unchanged.

### 6.4 Bad, forged and adversarial tokens

- **Tokens ahead of this node (`token > durable`)**: `RETRY` at once (§5.1
  step 3). This covers a lagging node, a token from a cluster rebuilt from
  genesis (positions restarted at 0, so the old token is ahead of everything),
  and any forged token such as `u64::MAX`. Each costs one comparison and is
  never parked. A restore from backup keeps positions and is unaffected.
  Embedding a cluster identity in the token was considered and left out
  (YAGNI); `ReadToken` is opaque, so it can be added later without changing
  the API shape.
- **Flooding with tokens just at `durable`**: such reads resolve as soon as
  the node learns commit and applies, so they cannot be made to wait. Under
  load, though, `applied` can trail `durable` by milliseconds, and at 64
  admissions per pass that can still be many parked reads. The per-row heaps
  keep the per-pass cost independent of how many are parked, and
  `MAX_PARKED_MIN_POSITION_READS` bounds memory (§5.1 steps 5 and 6).
- **Forged tokens inside an uncommitted tail** that is later truncated: never
  satisfied, so `RETRY` at the deadline (§5.1).
- **Who can attack.** Remote clients through `uc2-gateway` are the realistic
  source: that path is reachable from the network. The gateway's global grant
  budget already bounds what one gateway keeps in flight. Local shared-memory
  clients are same-host processes that can write raw records past any SDK
  limit; the bound, heaps and cap protect against them too, as defence in
  depth.
- **Malformed records** (§4.1) are dropped.

### 6.5 Back-pressure

A parked read holds its client request slot until answered or RETRY, under the
existing admission window, and the node holds at most
`MAX_PARKED_MIN_POSITION_READS` of them. Beyond the cap, reads get `RETRY`
immediately.

## 7. Testing and proof

**Unit**

- `uc_protocol`: query-record round trip with and without the flag; rejects
  `MIN_POSITION ∧ LINEARIZABLE` and short payloads; pins the flag value and
  the cnc 3.5 constant.
- `uc_client` / `uc_remote`: the `+1` rule for write positions, answer
  positions taken as is; `fetch_max` never lowers; `observe` merges;
  `ReadToken` `Display`/`FromStr` round trip; token 0 sends a byte-identical
  snapshot record; the guard turns `position < token_sent` into `Retry`.
- `uc_node`: `token > durable` → immediate RETRY, never parked (including
  `u64::MAX`); fast path forwards immediately with the real epoch; slow path
  parks then forwards when `applied` reaches the token; a lower token parked
  after a higher one is released first; the cap → immediate RETRY; deadline →
  RETRY (including a parked token whose tail is truncated); lost leadership
  never RETRYs a min-position read; linearizable reads and Rung A behave
  exactly as before (their tests unchanged and green); both refusal reasons
  are counted.
- `uc_service`: a query answer carries the cursor, not 0.

**Fuzz:** extend the query-record decode coverage to the new flag and prefix
(an existing target if one decodes query records, else a new one).

**Integration (`uc_node/tests`)**

1. Read-your-writes on a follower: write through the leader, then immediately
   `ReadYourWrites`-read a follower's instance dir; the write is visible.
2. The wait is real: stop a follower's service; its read RETRYs at the deadline
   rather than answering stale; restart it and the same token is answered.
3. A carried token crosses processes: Engine A writes and exports
   `read_token()`; Engine B on another node `observe`s it and reads the write.
4. Across rows: write row 1, read row 0 with that token.

**Consistency capstone**

- A session-guarantees checker in `uc_lincheck` (linearizability is the wrong
  test for a weaker guarantee): per client, every read reflects at least that
  client's last acknowledged write, and no read reflects an older state than a
  previous read.
- `ryw_v2`, shaped like `lin_v2`: clients read from random nodes under failover
  and purge/snapshot churn; the checker judges the history.
- **Mutation teeth** behind the existing `mutation-testing` feature: (a) the
  node forwards a min-position read without waiting; (b) the client guard is
  disabled while the service's `applied` is rewound. The capstone must fail on
  both.
- Elle: one session-guarantee pass **if** the list-append models in
  `scripts/elle_check.sh` support one (not checked at design time).

**Adversarial:** under steady write load, a client floods a node with
(a) `u64::MAX` tokens and (b) tokens exactly at `durable`, at the cap. Assert
that commit keeps advancing at the unloaded rate within smoke tolerance, that
no read is parked for (a), and that parked count never exceeds the cap for (b).

**Remote:** a gateway end-to-end test (write through the leader's gateway,
`ReadYourWrites`-read through a follower's gateway, see the write), and an
old-protocol client refused with `HELLO_REFUSED_VERSION`.

**Performance:** no fleet gate. This is not a throughput feature, and the
read-spread run already bounds its capacity. One local smoke run checks that
the fast path costs about the same as a snapshot read; like any dev-box number
it is smoke, not a gate.

**Proof stack before a PR:** workspace tests, `lin_v2`, `lin_partition_v2`,
the crashtest suite, `ryw_v2`; clippy on the pinned toolchain and the 1.89
MSRV gate; clippy with `--features hard-crash-tests` (exhaustive `Outcome` /
`ClientError` matches live behind that feature).

## 8. Documentation owed with the feature

- A `docs/notes/` explainer (what the three read modes guarantee, when to use
  which, how to carry a token across processes), linked from the release
  bullet.
- `docs/reference/semver-policy.md` (§4.5).
- `CLAUDE.md`'s standing facts: cnc 3.5, remote protocol v2.
- `docs/notes/smr-read-options-compared.md`: move I from "not implemented" to
  implemented.
- The release entries in `RELEASES.md` and `docs/releases.md` at the next cut.
