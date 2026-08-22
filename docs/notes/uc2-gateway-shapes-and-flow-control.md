# The gateway's shape, and why it flow-controls the way it does

*Design note, M12a. Records the shape comparison behind `uc2_gateway::Edge`
and `uc2_remote`'s protocol, so the reasoning survives the design
conversation that produced it.*

## Four shapes for "a client that can't attach to shmem"

Before M12a, reaching a UC cluster meant running in the same process (or at
least the same host) as a node, attached to its shared memory directly
(`uc2_client::Engine`) — fast, but it rules out a client on a different
host, in a different language, or behind a network boundary shmem can't
cross. Four shapes were on the table for closing that gap.

**A — status quo, no gateway.** Keep shmem attach as the only client path.
Rejected as the *whole* answer (that's why M12a exists), but it stays: a
gateway is an addition, not a replacement — a colocated client still attaches
directly and pays none of the TCP/credit overhead.

**B — a dumb TCP-to-shmem proxy.** One process per node relays bytes between
a TCP socket and the local `Engine`, with no leader-awareness, no session
state, and no flow control beyond whatever TCP itself provides. Simplest to
build. Rejected: a client wired to a follower's proxy gets nothing useful (no
way to learn where the leader is except out-of-band polling), and every retry
after any hiccup — a dropped connection, a timeout — has no exactly-once
story. That pushes both problems onto every client implementation, forever,
which is precisely the cost a shared protocol exists to avoid paying once.

**C — chosen. A purpose-built framed protocol, terminated by a per-node edge
that redirects rather than forwards, with application-level credits and an
opt-in exactly-once envelope.** This is `uc2_gateway::Edge` and
`uc2_remote`: one edge co-located with each node, one framed TCP protocol
([reference](../reference/remote-protocol.md)), a static node-id→address map
for `REDIRECT`/`LEADER_CHANGED`, a credit-based flow-control frame layer
independent of TCP's own windowing, and `Sessioned<S>` for services that want
retries to be provably safe. It answers B's two problems directly: the
protocol itself carries leader location and flow-control state, and the
session envelope makes a re-send a well-defined outcome instead of a maybe.

**D — Aeron's shape, as the end-state this does not yet reach.** Aeron
Cluster's own ingress is richer than C in ways worth naming as the direction,
not the destination: ingress can fan out over multiple concurrent
publications (MDC) rather than one TCP connection per client, egress runs on
its own channel independent of the ingress socket (so a slow ingress write
never contends with delivering a response), and session establishment is a
protocol step in its own right rather than folded into the first frame. None
of that is built here — `uc2_gateway` is deliberately the smaller, one-TCP-
connection-per-client shape that gets a working front door in front of the
existing shmem `Engine` without inventing a second transport. D is what a
future gateway generation reaches for if the one-TCP-connection shape turns
out to be the bottleneck; nothing in C forecloses it.

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
  `STATUS` frame says exactly which is true, so a client's backoff decision
  (and `RemoteClient`'s AIMD-style halve-on-backpressure, climb-back-on-
  completion behaviour) is driven by the real signal, not an inference from
  socket state.
- **It still needs the backstop.** A client that ignores its credits and
  keeps writing anyway is stopped the same way B would have stopped it: the
  edge simply ceases reading that socket, and TCP's window closes under it.
  Credits are the signal; TCP is still the enforcement of last resort — the
  two are not competing designs, they're layered.

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
  receiver-driven flow-control primitive `uc2_net` already uses between
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
