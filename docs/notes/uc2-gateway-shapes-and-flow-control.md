# The gateway's shape, and why it flow-controls the way it does

*Design note, M12a. Records the shape comparison behind `uc_gateway::Edge`
and `uc_remote`'s protocol, so the reasoning survives the design
conversation that produced it.*

## Four shapes for "a client that can't attach to shmem"

Before M12a, reaching a UC cluster meant running in the same process (or at
least the same host) as a node, attached to its shared memory directly
(`uc_client::Engine`) — fast, but it rules out a client on a different
host, in a different language, or behind a network boundary shmem can't
cross. UC's rule that clients are co-located exists only because shmem is
the node's *only* ingress today — not because remote ingress is impossible
in general (see D). Four shapes were on the table for closing the gap.

**A — the user wraps `uc_client` themselves.** Status quo: nothing ships,
and a user who needs a remote-reachable front door writes their own
co-located edge process around `Engine`. Rejected as the *whole* answer:
every single user ends up re-deriving the same hard parts from scratch —
leader discovery (`Engine`'s `NotLeader{hint}` carries a node id, not an
address, so even knowing *who* the leader is doesn't say how to reach it),
redirect vs. forward, reconciling in-flight requests across a failover, and
an exactly-once story for a remote hop. A weak turnkey story for anyone who
isn't willing to build all of that first.

**B — a fixed `uc2-gateway` daemon speaking a UC-owned remote protocol,
mandatory.** Ship one blessed edge binary and a wire protocol every remote
client must speak. This forces one of two shapes, both bad: either UC gets
into the client-library business — a protocol with N language bindings to
build and maintain — or a user who wants their *own* wire format (say,
FIX, or an existing internal RPC) wraps UC's mandatory protocol with their
own edge anyway, giving a two-hop topology (their edge → `uc2-gateway` →
node) for every request. Rejected on top of that because a fixed,
mandatory ingress host contradicts M9's own "template, not host" decision:
the service binary is a template the user instantiates and owns, and a
fixed daemon standing in front of it on the ingress side breaks that same
philosophy applied to the front door.

**C — chosen. A gateway *kit*: the reusable hard part as a library, a thin
reference binary on top.** `uc_gateway::Edge` packages exactly the pieces
every remote-ingress user would otherwise re-derive under A — leader
discovery, redirect (not forward), receiver-driven credits, and the
exactly-once envelope (`Sessioned<S>`) — as a library written over the
existing local `Engine`. `src/bin/uc2-gateway.rs` is a *thin* reference
binary on top, driven by `gateway.toml`, meant for the quickstart and the
gate — not the only legitimate deployment. A user who wants their own wire
format still embeds `Edge` inside their own process, the same way A
imagined building it, but without re-deriving any of the hard parts; a user
who just wants a working front door runs the reference binary as-is. This
keeps faith with M9's template philosophy (the *reusable logic* ships, not
a mandatory host) while still giving B's turnkey story to anyone who wants
it. It is also written transport-agnostic on purpose: nothing in `Edge`
assumes TCP specifically, which is what lets it become D's client SDK later
without a rewrite.

**D — Aeron's own shape, the end-state this does not yet reach.** What
Aeron Cluster actually does: ingress is a channel URI like any other Aeron
channel (`aeron:udp` per member for a genuinely remote client, `aeron:ipc`
when co-located — the same transport abstraction, IPC vs. UDP is just
configuration). A connect to a non-leader member is answered with a
redirect; a `NewLeaderEvent` on an established session makes the client SDK
re-route; followers never forward on the client's behalf. There is no
separate edge tier at all — "the gateway" is just the user's own code
wrapping `AeronCluster` directly (Artio/FIX-over-Aeron-Cluster is the
canonical example of exactly this). D removes the tier C still has, but it
is consensus-agent work, not gateway-kit work: a second ingress path *into*
`uc_node` itself, a client-session/admission/auth story at that layer, and
per-language client libraries if it's going to be genuinely polyglot — each
its own later spec. C is what gets a working, template-shaped front door
in front of the existing shmem `Engine` without first doing all of that.

## Why redirect, not forward

A follower's edge could, in principle, forward a misdirected `SUBMIT` to the
leader's edge itself and relay the answer back — the client would never see
a `REDIRECT` at all. C rejects this:

- **It doubles the hops on every misdirected write**, and does so silently —
  the client has no way to know its "fast" connection to a nearby follower is
  actually two network hops away from where the write lands. A `REDIRECT`
  costs one round trip *once*, after which the client talks to the real
  leader directly.
- **It requires a second, edge-to-edge transport** carrying client-shaped
  payloads with the same credit and session semantics — either reusing the
  node-to-node reliable-UDP plane for something it wasn't designed to carry,
  or building a whole second connection type between edges. Either way it is
  a second failure mode: an edge that could serve its own clients just fine
  now also depends on reaching a *specific other edge's* TCP listener.
- **It hides topology the client benefits from knowing.** Redirect teaches
  the client where the leader actually is, so its *next* write goes there
  directly; forwarding would keep routing every future write through
  whichever edge happened to answer first, forever.
- **It matches how Aeron Cluster itself behaves** (below) — ingress
  redirect, not inter-node ingress proxying.

## Why credits, not TCP alone

TCP's own flow control (the receive window) is real and still load-bearing —
it's the backstop that stops a reader from having to buffer an unbounded
amount from a client that ignores every signal the protocol sends. But it is
not enough on its own, for three reasons:

- **TCP's zero-window is implicit, coarse, and late.** The kernel's receive
  buffers absorb hundreds of KB before the window even starts closing, so by
  the time a client feels back-pressure the edge has already been behind for
  a while — the opposite of Aeron's own Status Messages, which are explicit,
  receiver-driven credit grants sent *before* the receiver is actually full.
  The gateway's `credits`/`STATUS` scheme is that same explicit-and-early
  shape, not TCP's implicit-and-late one.
- **It's per-socket, and the resource it should be protecting is shared.**
  One edge's local `Engine` has one inflight window shared across every
  connection on that edge. A connection whose own TCP window happens to be
  wide open can write far ahead of what the shared window can actually admit,
  starving other connections of slots they'd otherwise get — TCP has no way
  to express "the thing behind me, not this socket, is the scarce resource."
- **It carries no reason.** A closing TCP window says "slow down"; it can't
  distinguish "the network is momentarily congested" from "the shared
  `Engine` is backpressuring every connection" from "you personally have hit
  your fair share." The `credits`/`acked_seq` pair in every `RESPONSE` and
  `STATUS` frame says exactly which is true.
- **The adjustment itself lives entirely on the edge, not the client.**
  `Conn::squeeze`/`Conn::relax` (`uc_gateway/src/conn.rs`) halve a
  connection's credits (floor `1`) the first time a request hits
  `SubmitError::Backpressure` and double them back (capped at
  `per_conn_inflight`) on every completion while squeezed — multiplicative
  both ways, not AIMD. `RemoteClient` does none of this arithmetic itself; it
  only ever obeys whatever `credits` value the edge most recently sent.
- **It still needs the backstop.** A client that ignores its credits and
  keeps writing anyway is stopped anyway: the edge simply ceases reading
  that socket, and TCP's window closes under it. Credits are the signal;
  TCP is still the enforcement of last resort — the two are not competing
  designs, they're layered.
- **`RETRY` is a state signal, never a load signal.** It answers "the world
  changed — reconnect, wait `retry_after_us`, or give up," never "you're
  sending too much." Load is what credits are for; `RETRY` exists
  independently of how many credits a connection currently holds (see
  [the protocol reference](../reference/remote-protocol.md) for the
  reason-by-reason breakdown).

## Aeron parallels

None of this is invented from nothing — it mirrors three pieces of Aeron's
own design, moved up one layer to the client-facing edge:

- **`REDIRECT`** parallels Aeron Cluster's ingress redirect: a client that
  submits to a non-leader member is told where the leader is and reconnects
  there, rather than the cluster silently routing around its own topology.
- **`LEADER_CHANGED`** parallels Aeron Cluster's `NewLeaderEvent`: a
  proactive push to already-connected sessions when leadership moves, so an
  idle client learns the cluster changed without having to try a write first
  and fail.
- **Credits/`STATUS`** parallel Aeron's own **Status Messages** — the
  receiver-driven flow-control primitive `uc_net` already uses between
  nodes (a receiver periodically states how much more a sender may send).
  The gateway's credit scheme is the identical pattern at the client-facing
  edge instead of the node-to-node plane: the edge states how many more
  `SUBMIT`/`QUERY` slots a connection may use, the same shape as a Status
  Message stating a receive window.

## Two lessons from the failover work

Two things were not obvious going in, and both came directly out of getting
`RemoteClient`'s failover behaviour actually correct rather than merely
plausible.

**Lesson 1 — a fresh connection cannot be trusted with the whole pipelined
window.** The natural first design reconnects, re-`HELLO`s, and immediately
flushes every unanswered request. That is wrong: a connection that turns out
to be talking to a stale leader, or one that's about to answer `REDIRECT` to
everything, amplifies the mistake by however deep the pipeline is — the
client discovers the problem request-by-request instead of once.
`RemoteClient` fixes this with **probe-before-flush**: after any (re)connect
it writes exactly one request and waits for proof the far end is actually
willing to serve (a real `RESPONSE`, or a `STATUS` whose `acked_seq` covers
that request) before releasing the rest of the window. The cost is one round
trip; the benefit is that a bad reconnect target is discovered once, not once
per pipelined request.

**Lesson 2 — a connection's answer to "can you take a write" must stay
consistent for its own lifetime.** The first cut let a connection's
not-serving status flip with the node's actual role: if the node started
serving again a microsecond after refusing a write, the *same* connection
would happily accept the next one. That breaks the session model's
classification. `Sessioned<S>`'s FRESH/REPLAYED/EXPIRED answer for a retried
`seq` assumes that once a connection has told a client "not serving," every
later write on that connection gets the same answer — otherwise a client
that got `REDIRECT`ed on `seq` 5, tried `seq` 6 on the same connection a
moment later, and had it *accepted* on the node's newly-won leadership would
leave a gap in what the dedup table can classify. `Conn::latch_not_serving`
fixes this by making the refusal sticky and per-connection: **the set of
`SUBMIT`s any one connection gets accepted is always a prefix of what it
sent.** Once latched, that connection answers every later `SUBMIT` the same
way for the rest of its life, and the client moves on rather than hoping the
same socket recovers.

## Related

- [The remote protocol](../reference/remote-protocol.md) — the frame layout
  these mechanisms ride on.
- [How to run a gateway](../how-to/run-a-gateway.md) — the operational view
  of failover and the not-serving latch.
- [The state-machine contract](../reference/state-machine-contract.md) —
  `Sessioned<S>`'s full FRESH/REPLAYED/EXPIRED semantics.
