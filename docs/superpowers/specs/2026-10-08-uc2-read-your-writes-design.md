# UC v2 — Read-your-writes reads (position-token reads on any node)

**Date:** 2026-10-08
**Status:** design, approved section by section in a brainstorming session;
awaiting review of this written form. No code yet.
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

Both steps already run on every consensus pass regardless of role
(`uc_node/src/node.rs:4419-4424`: `drain_query_ring()` then
`advance_pending_reads()`).

**Admission (`drain_query_ring`, `node.rs:9332`)**, for a record carrying
`FLAG_V2_MIN_POSITION`:

1. Parse and strip the token. An unknown row gets `BAD_SERVICE`, as today.
2. **No leadership gate.** Like a snapshot read, it is served on any role.
3. **Fast path:** the same capture-recheck B uses. Capture the slot's epoch
   `e`, require `e ≥ 1 ∧ applied ≥ token`, then require the epoch is still `e`.
   If so, forward at once with **`expected_epoch = e`**. This deliberately
   differs from a snapshot read, which forwards `0` ("skip the check"). With the
   real epoch, a service restart between the check and the answer makes the old
   incarnation refuse with RETRY (its existing stale-epoch refusal in
   `drain_queries`), instead of answering from a rebuilt state that may be
   behind the token.
4. **Slow path:** park a `PendingRead` with `phase = AwaitApplied`,
   `commit_at = token`, `deadline_ns = now + READ_BARRIER_TIMEOUT_NS` (1 s,
   `node.rs:376`). It never enters `AwaitQuorum`.

**`PendingRead` gains `kind: ReadKind { Linearizable, MinPosition }`**
(`node.rs:925`).

**Advancing (`advance_pending_reads`, `node.rs:9445`):**

- RETRY on deadline: both kinds.
- RETRY on lost leadership (`!can_serve`): **`Linearizable` only.** A
  `MinPosition` read does not depend on who leads.
- Otherwise the existing `AwaitApplied` branch, unchanged: epoch capture,
  `applied ≥ commit_at`, epoch recheck, forward with the real epoch, and restore
  the read if `svc_query` is momentarily full.

**Rung A probe-round reset, tightened.** Today a round is dropped when
`pending_reads` is empty (the head of `advance_pending_reads`). With
`MinPosition` reads parked, the list may never be empty, so a stale round could
linger. That is harmless to safety (the round-order gate still stops it
certifying later reads), but it delays the next linearizable read behind a
round that certifies nobody. The condition becomes "no read is in
`AwaitQuorum`", the same test `maybe_issue_round` uses (`node.rs:9218`).
Parked `MinPosition` reads never cause a probe round: `maybe_issue_round` only
counts `AwaitQuorum` reads.

**Cost.** On a caught-up node every `MinPosition` read takes the fast path:
two atomic loads plus the epoch recheck, then the same forward a snapshot read
does. The pending list holds only lagging reads, bounded by the client
admission window, as linearizable reads are.

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
  leaves them meaningful. A lagging node waits until it applies them.
- **Truncation** removes only uncommitted bytes, which no token can name.
- **Snapshot install** (below-floor joiner, restarted service): `applied`
  jumps forward, and the reads it unblocks are correct.
- **Learners** serve min-position reads like any node; lag shows up as
  waiting, then RETRY.

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
- **Halted or removed node.** `applied` stops, so min-position reads get RETRY
  at the deadline, while snapshot reads keep answering stale data (as today).
- **Booting node.** The existing `NodeBooting` attach refusal applies unchanged.

### 6.4 Bad or unusual tokens

- **Token from a cluster rebuilt from genesis.** Positions restart at 0, so the
  old token points ahead of everything, and every read with it gets RETRY at
  the deadline. Documented remedy: drop the token. A restore from backup keeps
  positions and is unaffected. Embedding a cluster identity in the token was
  considered and left out (YAGNI); `ReadToken` is opaque, so it can be added
  later without changing the API shape.
- **Malformed records** (§4.1) are dropped.

### 6.5 Back-pressure

A parked read holds its client request slot until answered or RETRY, under the
existing admission window. No new limit.

## 7. Testing and proof

**Unit**

- `uc_protocol`: query-record round trip with and without the flag; rejects
  `MIN_POSITION ∧ LINEARIZABLE` and short payloads; pins the flag value and
  the cnc 3.5 constant.
- `uc_client` / `uc_remote`: the `+1` rule for write positions, answer
  positions taken as is; `fetch_max` never lowers; `observe` merges;
  `ReadToken` `Display`/`FromStr` round trip; token 0 sends a byte-identical
  snapshot record; the guard turns `position < token_sent` into `Retry`.
- `uc_node`: fast path forwards immediately with the real epoch; slow path
  parks then forwards when `applied` reaches the token; deadline → RETRY; lost
  leadership does not RETRY a `MinPosition` read but still RETRYs a
  `Linearizable` one; the tightened round reset drops a stale round when only
  `MinPosition` reads are pending.
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
