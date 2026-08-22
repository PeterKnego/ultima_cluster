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
    /// Apply the committed command at `position` (the absolute log byte
    /// offset, the idempotency key). Write the response bytes into `out`
    /// (cleared by the caller). Deterministic, sync, no I/O.
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
    /// Answer a read. `out` is cleared by the caller.
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    /// Highest position applied so far (`None` before the first).
    fn last_applied(&self) -> Option<u64>;
}
```

```rust
/// The user's deterministic business logic, typed.
pub trait StateMachine: Send + 'static {
    type Command: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Query: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type QueryResponse: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;

    fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response;
    fn query(&self, q: Self::Query) -> Self::QueryResponse;
    fn last_applied(&self) -> Option<u64>;
}
```

Both are sync, deterministic, no I/O, no clock, no randomness — non-negotiable
for state-machine-replication correctness, and the reason neither signature
takes `async` or a context handle.

## The blanket adapter and the byte-identity promise

```rust
impl<S: StateMachine> RawStateMachine for S {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        let resp = StateMachine::apply(self, position, cmd);
        bincode::serde::encode_into_std_write(&resp, out, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
    }
    // query, last_applied: same shape
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
spike note measured this at 24–40× a hand-laid frame's encode cost and up to
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

*Forward pointer — not yet on this branch; lands with the gateway kit's
`uc2_service::session` module (M12a spec §4.4).* `Sessioned<S>` is the
exactly-once wrapper the gateway's session envelope needs, and it is
specified to implement `RawStateMachine` for any `S: RawStateMachine` —
peeling a fixed 16-byte `(client_id, seq)` header off the command bytes, then
delegating. Because `StateMachine` blankets onto `RawStateMachine`, this
wraps a typed state machine exactly as it wraps a raw one: no separate typed
`Sessioned` is needed. Full session-envelope and dedup-window details belong
in that module's own docs once it lands, not duplicated here.

## Payload ceiling

A command's serialized bytes must fit in one datagram: the wire budget is
`MTU_DEFAULT = 1408` bytes (`uc_protocol::v2::datagram::MTU_DEFAULT`, not
operator-configurable) minus the datagram and frame headers, which leaves
roughly **1.3 KB** for the frame payload. This applies identically to both
tiers — raw bytes or a typed command's encoded form — and is enforced before
the frame ever reaches the ring (`max_payload`, checked at `try_submit`).
There is no chunking; a command that does not fit in one datagram is refused,
not split.
