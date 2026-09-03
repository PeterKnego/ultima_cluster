# The state-machine contract

The two tiers a service can implement, their exact signatures, and the
discipline each one imposes.

For why there are two tiers (not one), see
[the codec budget spike](../notes/2026-08-22-codec-budget-spike.md) — it
measured serde+bincode typing at 56–85% of the apply thread's cycles at
256 B–1 KiB payloads and is the decision record this page implements.

## The two tiers

| | `RawStateMachine` | `StateMachine` |
|---|---|---|
| Sees | the committed frame's raw bytes | your typed `Command`/`Query` |
| Decode/encode | none — you own the wire format, or there is none | bincode-standard, done by a blanket adapter |
| Pick it for | SBE / flatbuffers / hand-laid frames, a polyglot gateway payload, large or hot commands where codec share matters | everything else — the common case, and what every SM used through v2.5.0 |

**A type implements exactly one of the two.** `RawStateMachine` is the core
contract; `StateMachine` is typed convenience with a blanket impl onto it
(below). You never implement both for the same type — implementing
`StateMachine` already gives you `RawStateMachine` for free.

`ServiceBuilder::new(cfg, sm)` accepts either tier: `S: RawStateMachine`. A
typed `sm: impl StateMachine` passes the same call unchanged, because the
blanket impl makes it also `RawStateMachine`.

## Trait signatures

```rust
/// The core state-machine contract: bytes in, bytes out. The framework hands
/// `apply` the committed frame payload exactly as it sits in the log buffer
/// and reuses `out` across calls — no decode, no allocation in steady state.
pub trait RawStateMachine: Send + 'static {
    /// The FSM's identity, declared in code (FSM identity, 2.11 pending).
    const NAME: &'static str;
    /// Packed semantic version of this FSM's logic; `0` = unversioned.
    const VERSION: u32 = 0;
    /// Provided, derived from the two above.
    const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME, Self::VERSION);

    /// Apply the committed command at `ctx.position` (the absolute log byte
    /// offset, the idempotency key). Write the response bytes into `out`
    /// (cleared by the caller). Deterministic, sync, no I/O.
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>);
    /// Answer a read. `out` is cleared by the caller.
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    /// Highest position applied so far (`None` before the first).
    fn last_applied(&self) -> Option<u64>;

    /// A timer this FSM scheduled has reached its position on the log.
    /// PROVIDED, default no-op (log time and timers, 2.11 pending).
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {}
    /// Framework hook: the pending instances a wrapper holds, re-announced
    /// to the node after attach and after replay. PROVIDED, default empty.
    fn pending_timers(&self) -> Vec<(u64, u64)> { Vec::new() }
}
```

```rust
/// The user's deterministic business logic, typed.
pub trait StateMachine: Send + 'static {
    const NAME: &'static str;
    const VERSION: u32 = 0;

    type Command: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Query: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type QueryResponse: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Self::Command) -> Self::Response;
    fn query(&self, q: Self::Query) -> Self::QueryResponse;
    fn last_applied(&self) -> Option<u64>;

    /// PROVIDED, default no-op.
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {}
}
```

Both are sync, deterministic, no I/O, **no host clock**, no randomness —
non-negotiable for state-machine-replication correctness, and the reason
neither signature takes `async`.

## `ApplyCtx`: position, time, term, identity

`apply` receives `&mut ApplyCtx` rather than a bare `position: u64` (FSM
identity, 2.11 pending). It is `#[non_exhaustive]`, built once per frame by
the apply loop, by journal replay and by snapshot tail-replay, and it carries:

| item | what it is |
|---|---|
| `ctx.position: u64` | the frame's absolute byte position — the idempotency key, exactly what `position` used to be |
| `ctx.time_ns: u64` | **the leader's stamp on this frame**: ns since the Unix epoch, non-decreasing along the log, identical on every replica. This is your `now()` (log time and timers, 2.11 pending) |
| `ctx.term: u32` | the frame's `leadership_term_id` |
| `ctx.ids()` | an `IdGen` for this apply call: deterministic IDs from `(position, identity, an ordinal that resets every call)` |
| `ctx.schedule(id, at_ns)` / `ctx.cancel(id)` | ask for / withdraw a timer (log time and timers) |

`ApplyCtx::for_sm::<MySm>(position)` builds one in your own unit tests, with
`.with_time(..)` and `.with_term(..)` as builders.

**`query` gets no context**, so it has neither time nor `IdGen`: a read has no
position that means the same thing on every replica, and time is no better
defined there than position is.

## Timers: `on_timer` and `Timed<S>`

`on_timer` is a **provided** method on both tiers with a no-op default, so an
existing state machine compiles and behaves exactly as before. Implement it
to receive the timers your `apply` scheduled:

```rust
fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
    // ev.id is the id you passed to ctx.schedule; ev.deadline_ns is what you asked for.
    // ctx.time_ns is the frame's stamp: equal to the deadline unless ev.late(ctx).
    if ev.late(ctx) { /* fired after a leader change, or scheduled in the past */ }
    self.last_applied = Some(ctx.position);   // advance it exactly as in `apply`
}
```

The node fires timers **at least once**: an instance in flight when a leader
loses leadership is re-armed and may fire again. `uc_service::Timed<S>` wraps
either tier and makes delivery **exactly once** per scheduled instance, by
keeping the pending set that your own `schedule`/`cancel` calls implied and
dropping any frame that is no longer pending. It composes with `Sessioned`
(`Timed<Sessioned<S>>`), forwards `NAME`/`VERSION`, and carries its maps in
the snapshot artifact ahead of the inner state machine's. Running without it
is the same trade as running without `Sessioned`: correct under a contract you
have to honour, weaker.

Full semantics, the ordering guarantee, and the failure-mode table:
[Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md).

## The blanket adapter and the byte-identity promise

```rust
impl<S: StateMachine> RawStateMachine for S {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        let resp = StateMachine::apply(self, ctx, cmd);
        bincode::serde::encode_into_std_write(&resp, out, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
    }
    // query, last_applied, on_timer: same shape (on_timer forwards straight through)
}
```

This is **exactly the codec the framework used through v2.5.0** — the same
`bincode::serde` call with the same `bincode::config::standard()`. A typed
state machine's wire format under M12a is byte-identical to what it produced
before the raw tier existed: nothing about upgrading to this version changes
what a `StateMachine` implementor's frames look like on disk or on the wire.
A corrupt committed frame is unrecoverable corruption and fail-stops rather
than propagating a decode error — committed bytes are trusted.

## `bytes::Bytes` / `serde_bytes` for blobs

If your typed `Command` or `Response` carries a blob field, type it as
`Vec<u8>` and bincode decodes it **element-wise** — the codec ladder in the
spike note measured this at 25–42× a hand-laid frame's encode cost and up to
21× its decode cost, dominated entirely by that element-wise walk, not by the
bincode format. Typing the same field as `bytes::Bytes` (or `Vec<u8>` tagged
`#[serde(with = "serde_bytes")]`) gives the **identical wire bytes** — this
was asserted, not assumed — at 1.2–1.9× a hand-laid frame's cost: an order of
magnitude cheaper, for a one-type change, with no wire-compatibility
consequence for existing deployments (old and new bytes decode to the same
value either way; only the field's Rust type changes).

If you don't want any of this cost — not even a cheap one — implement
`RawStateMachine` directly and skip typing the blob at all.

## The `out` buffer discipline

`apply`'s and `query`'s `out: &mut Vec<u8>` is **cleared by the caller**
before each call and reused across calls — this is the allocation the raw
tier saves relative to a typed response's `encode_into_std_write` into a
fresh scratch buffer path from earlier versions. Your implementation:

- writes only its own response into `out` — never assumes it starts non-empty
  beyond what the caller guarantees (empty), never leaves stale bytes behind
  for the framework to publish by accident;
- never reads `out`'s prior contents;
- may reserve capacity into it, but does not need to — the framework owns its
  lifetime and reuse across the apply loop.

## Every replica pays the typed tier's cost, not just the leader

The framework **publishes** a response only on the leader (`is_leader` gates
the egress-ring write). It does **not** gate `apply` itself: every replica —
every follower doing steady-state apply, and the replay path a service uses
to reconstruct state after a restart or below-floor catch-up — calls
`sm.apply(pos, payload, &mut out)` on every committed frame, whether or not
that replica ever publishes the result. For a typed `StateMachine`, the
blanket adapter's `apply` both decodes the command **and** encodes the
response inside that one call — so the response encode a typed tier pays is
not a leader-only cost that a follower gets to skip. It runs everywhere the
frame is applied, publish or no publish. This is one of the reasons to go raw
for large or hot commands: the codec cost is paid by every member of the
cluster, on every commit, not amortized down to just the node that happens to
be leader at the time.

## `RawOutputHandler` vs `OutputHandler`

The same two-tier split exists for the leader-only, at-least-once
`on_committed` side effect:

```rust
pub trait RawOutputHandler<S: RawStateMachine>: Send + 'static {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError>;
}

pub trait OutputHandler<S: StateMachine>: Send + 'static {
    async fn on_committed(&self, position: u64, cmd: &S::Command, state: &S) -> Result<(), OutputError>;
}
```

`TypedOutput<O>` adapts a typed `OutputHandler` onto `RawOutputHandler` (one
bincode decode per committed command, on the output thread — the same shape
as pre-M12a). `ServiceBuilder::output_handler(typed)` installs a typed
handler through that adapter; `ServiceBuilder::raw_output_handler(raw)`
installs a raw one directly. Unlike `apply`, `on_committed` genuinely is
leader-only — the output agent only runs on the leader — so this decode does
not repeat cluster-wide the way `apply`'s does.

## `Sessioned<S>` wraps either tier

`uc_service::session` (M12a spec §4.4) ships `Sessioned<S>`, the exactly-once
wrapper the gateway's session envelope needs. It implements `RawStateMachine`
for any `S: RawStateMachine`, and `SnapshotStateMachine` for any
`S: SnapshotStateMachine` — because `StateMachine` blankets onto
`RawStateMachine`, `Sessioned` wraps a typed state machine exactly as it
wraps a raw one; no separate typed `Sessioned` exists.

**Envelope.** Every command is expected to carry a fixed
`SESSION_HEADER_LEN = 16`-byte header — `client_id: u64` LE, then `seq: u64`
LE — ahead of the inner command bytes. A command shorter than 16 bytes is
malformed and is treated as unanswerable (`TAG_EXPIRED`, no inner bytes),
never a panic on the apply thread.

**Response tag.** The response is `tag: u8` followed by the inner SM's
response bytes (`TAG_EXPIRED` carries none):

- `TAG_FRESH = 0` — `seq` is new for this client (including a gap: only a
  `seq` at or below the client's highest seen is ever rejected as stale); the
  inner SM ran and its response was cached.
- `TAG_REPLAYED = 1` — a retry of a `seq` still inside this client's window;
  the cached response is returned and the inner SM does **not** run again
  (this is what makes a non-idempotent inner op, e.g. a CAS, safe to retry).
- `TAG_EXPIRED = 2` — `seq` is at or below the client's highest seen but has
  already fallen out of the window, so no cached response exists; the
  framework and the inner SM never see this frame's effect.

**`SessionConfig` is part of the replicated contract.** `window`,
`max_clients`, and `max_bytes` (below) all feed directly into the
FRESH/REPLAYED/EXPIRED classification and into which clients get evicted, so
**every replica must run identical `SessionConfig` values; changing it is a
flag day**, the same as changing `apply` itself — never a rolling per-node
tweak. `install_snapshot` enforces this at load time (see "Snapshot
composition" below): a mismatched config is refused outright rather than
silently retuning, or silently diverging, a running node.

**Window and eviction — deterministic by construction**, because `apply` runs
on every replica and must reach the same table everywhere: a `BTreeMap<u64,
ClientState>` (never a `HashMap`, whose iteration order is not a replay
guarantee), each client remembering up to `SessionConfig::window` responses
(default 4096) as an oldest-first FIFO keyed by `seq`. Two independent
budgets can trigger eviction, both by the same deterministic victim order —
smallest `(last_seen_pos, client_id)`: smallest log **position**, never
wall-clock time, with the client id itself as the tiebreak:

- **Client count** — when tracked clients exceed `SessionConfig::max_clients`
  (default 65536), the oldest client is dropped.
- **Cached-response bytes** — when the sum of all clients' cached response
  bytes exceeds `SessionConfig::max_bytes` (default 256 MiB), whole clients
  are dropped oldest-first, same order, **except** the client whose frame
  just pushed the total over budget — evicting it would erase the very
  response the caller is waiting on. If that client ends up the only one
  left and the budget is still exceeded, its own window is trimmed from the
  front (oldest response first) until back under budget or empty. Since
  `max_clients * window * avg_response_size` alone can reach multi-GB at the
  defaults, the real memory ceiling in practice is
  `min(max_clients * window * avg_response_size, max_bytes)`.

`last_seen_pos` advances on *every* frame for a client, including `REPLAYED`
and `EXPIRED` ones, so eviction ranks strictly by log position. An evicted
client's next frame is simply a fresh client starting over (`TAG_FRESH`), not
an error — as is any client whose `seq` legitimately starts at `0`: a
client's very first frame is always fresh regardless of which `seq` value it
carries (`ClientState` tracks "never seen" as `Option::None`, not a `0`
sentinel, precisely so a genuine first `seq` of `0` cannot collide with it).

**`last_applied()` is deliberately not a bare passthrough to the inner SM.**
The inner SM only advances on `TAG_FRESH` frames; a `REPLAYED`/`EXPIRED`
frame only touches the dedup table. `Sessioned` tracks its own
`max_pos_seen` — the position of the last `apply`/`install_snapshot` call
regardless of tag — and `last_applied()` returns
`max_pos_seen.max(inner.last_applied())`. Reporting the inner's value alone
would still be *safe* (the framework's contract only requires
under-reporting, never over-reporting, and replaying a dedup-only frame
through `Sessioned::apply` again is idempotent — it lands on the identical
branch and produces the identical tagged output) but would make every
restart redundantly re-run every skipped/replayed frame; `max_pos_seen` is
the exact resume frontier instead. The dedup table itself is always a pure
function of the applied prefix, so it is correct however far back a replay
starts.

**Snapshot composition.** `Sessioned<S>: SnapshotStateMachine` when
`S: SnapshotStateMachine`. `freeze` pins the inner snapshot handle plus a
bincode-encoded `TableImage { window, max_clients, max_bytes, clients }` of
the current dedup table (built from the same `BTreeMap`, so encoding is
deterministic); `stream_snapshot` writes a `u64` LE length prefix (capped at
a 1 GiB sanity bound on the read side — `install_snapshot` refuses a larger
declared length before ever allocating a buffer for it), the table blob,
then the inner SM's own stream. `install_snapshot` reverses that and, before
touching the inner SM at all, **refuses the install** with
`SnapshotError::Codec` if the decoded `window`/`max_clients`/`max_bytes`
don't match the live node's `SessionConfig` exactly — the replicated-contract
invariant above, enforced rather than merely documented. Only once the
config matches does it install the inner snapshot and restore `self.clients`
from the decoded table. This is what lets a retry land correctly even
immediately after a below-floor node installs a snapshot instead of
replaying the journal.

**`freeze()`'s returned position can trail `Sessioned::last_applied()`.**
`freeze` reports the *inner* SM's frozen position (`self.inner.freeze()`'s
own `pos`), which only advances on `TAG_FRESH` frames. If the most recent
frames before a freeze were `REPLAYED`/`EXPIRED`, that position sits below
`Sessioned::last_applied()`. That is safe to round-trip: those trailing
frames only ever bump a client's `last_seen_pos` and never evict or change
window contents, so on restart the framework re-feeds the same trailing
frames through `Sessioned::apply` again, reaching the identical branches and
reproducing the exact pre-freeze table.

## Payload ceiling

A command's serialized bytes must fit in one datagram: the wire budget is
`MTU_DEFAULT = 1408` bytes (`uc_protocol::v2::datagram::MTU_DEFAULT`, not
operator-configurable) minus the datagram and frame headers, which leaves
roughly **1.3 KB** for the frame payload. This applies identically to both
tiers — raw bytes or a typed command's encoded form — and is enforced before
the frame ever reaches the ring (`max_payload`, checked at `try_submit`).
There is no chunking; a command that does not fit in one datagram is refused,
not split.
