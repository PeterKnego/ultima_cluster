# Two tiers, one contract

*Why `RawStateMachine` (bytes in, bytes out) is the core state-machine contract
and the typed `StateMachine` is an adapter on top of it — and why that was a
one-way door worth measuring before walking through.*

Companion to [the codec budget spike](2026-08-22-codec-budget-spike.md), which
is the measurement, and
[the state-machine contract reference](../reference/state-machine-contract.md),
which is the exact signatures.

## The shape of the problem

A command crosses the apply boundary as bytes. It arrives in the log buffer as
bytes, it is replicated as bytes, it is archived as bytes. The only place it
was ever *not* bytes was inside `apply` — because the trait was typed, so the
framework bincode-decoded `S::Command` on the way in and bincode-encoded
`S::Response` on the way out.

That is a pleasant trait to implement. The question nobody had asked was what
it costs on the one thread that must never be slow.

## What the measurement said

Two independent measurements, both in the spike note — **both on the dev box,
which is not a bench**, so read the shares and ignore the absolutes; the fleet
number is gate row 3 and has not been run:

| | typed `Vec<u8>` command | raw bytes |
|---|---|---|
| isolated codec ladder, 1 KiB frame | 653 ns encode / 691 ns decode | 18 ns / 44 ns |
| apply thread in `m5_gate`, 509 B payload | `sm_apply` 731 ns/frame — **75.8 %** of the apply cycle | 12 ns/frame — **5.8 %** |

So at realistic payloads the apply thread was spending most of its budget
decoding and re-encoding, not applying. And the cause was not bincode: serde
types a `Vec<u8>` field as a *sequence of u8*, so the codec walks the payload
one byte at a time. The same field typed as `bytes::Bytes` produces the
identical wire bytes for a fraction of the cost — which is itself a finding an
existing user can act on without changing tiers at all.

**Every replica pays this, not just the leader.** Followers apply too, and so
does the replay path after a restart. Only *publishing* the response is
leader-gated.

## Why a second tier rather than a faster first one

There is no version of the typed trait that is free. The framework must hand
`apply` a `Self::Command`, so something must produce one, so something must
walk the bytes. The only way to not pay it is to not do it — which means a
contract whose `apply` takes `&[u8]` and writes into a caller-owned `Vec<u8>`.

That is `RawStateMachine`, and it is the **core** trait: `ServiceBuilder` takes
`S: RawStateMachine`, and the framework's internals speak only that. The typed
`StateMachine` survives as a blanket adapter — implement it and you get
`RawStateMachine` for free, via exactly the bincode call the framework made
before.

**A type implements one or the other, never both.** The blanket impl already
supplies the other one.

## The one-way door, and why it did not slam

Reversing the tiers later would have been a breaking change to every service
in existence. It did not have to be, because the adapter is byte-identical:
same `bincode::serde` call, same `bincode::config::standard()`, same 8-byte
position prefix on the response. A typed state machine's frames on disk and on
the wire under `v2.6.0` are the same bytes it produced under `v2.5.0` — which
is asserted by a test (`uc_service/tests/raw_contract.rs`), not assumed.

So the door opened without moving anybody through it. Existing services keep
their typed trait and change nothing. A service with a hot or large command
type moves to the raw tier and owns its own framing — SBE, flatbuffers, a
hand-laid frame, or a gateway payload it never interprets at all.

## What the raw tier asks of you in return

Three things, all of them consequences of "no framework decode":

- **`out` is cleared by the caller and reused.** Write your response and
  nothing else. Do not clear it yourself — the framework may already have put
  something there that belongs to it (`Sessioned` puts its tag there), and
  truncating that away is a panic on the apply thread. That is not
  hypothetical: it is defect F2, found by fuzzing, fixed in `7c908b1`.
- **You own the wire format**, including its compatibility story. Nothing
  validates your bytes for you.
- **`apply` is still sync, deterministic, no I/O, no clock, no randomness.**
  The tier changed; the SMR contract did not.

## The residual worth knowing

The typed tier's decode is a `.expect(…)` — a deliberate fail-stop, because a
corrupt *committed* frame is unrecoverable corruption and continuing past it
would replicate the corruption. That is right for `apply`. It is inherited by
`query`, where it is not obviously right: a query is pre-commit and can come
from an unauthenticated client through a gateway, so one malformed `QUERY`
body panics the apply thread and poisons the state-machine mutex. Changing
that decode's error semantics is a design decision rather than a bug fix, so
it is parked as a follow-up and written down in
[the attack surface](../security/attack-surface.md) and
[the self-assessment](../security/self-assessment.md). The raw tier is the
workaround today: it never decodes anything on your behalf, so there is
nothing there to fail-stop on.
