# Linearizable read path

How a linearizable read is certified, and the constants that govern it.

Two read modes exist. A snapshot read is answered from the service's state
without a quorum barrier. A linearizable read is certified by a `READ_PROBE`
round, described here.

For why the barrier exists, see
[The read barrier explained](../notes/uc2-read-barrier-explained.md). The
normative specification is
[the Rung A batch-probe design](../superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md).

## Certification

At most one `READ_PROBE` round is in flight at a time. A round issued while
reads are waiting certifies exactly the reads that were already parked when it
went out. This is an ordering rule; it is not a position comparison. The next
round begins when the previous one completes, so the cadence is self-clocking
at roughly one round per RTT. There is no tuning knob.

One probe round certifies every parked read regardless of which FSM it names
— the round certifies a commit position, which is service-agnostic.

A read is additionally gated on **the named FSM's** service catching up to
the read position (the query's `service_id` selects the slot; M14), on a
follower header-term check, on a capture-recheck, and on a service-epoch
backstop that rejects an answer if the service restarted during the query.

## Constants

| Constant | Value | Applies to |
|---|---|---|
| Round retransmit interval | 2 ms | a round whose datagrams are lost; the nonce is unchanged across retransmits |
| Per-read deadline | 1 s | after which the read resolves `MSG_V2_RETRY` |
| Rounds in flight | 1 | cluster-wide, per leader |

## Wire compatibility

`READ_PROBE` and `READ_PROBE_ACK` datagrams are byte-identical to their
pre-batching form. A follower cannot distinguish a shared round from a
per-read probe. The read path is therefore safe across a version-mixed rolling
upgrade.

Configuration changes are governed separately: complete a rolling upgrade
before reconfiguring.

## Interaction with reconfiguration

Any voter-set change voids the in-flight round.

Parked reads are not dropped. They wait for the next round, issued under the
new configuration, at a worst-case cost of one extra round trip and no
client-visible error.

During leader self-removal the leader continues serving reads until its removal
commits; each is certified by an ack set that intersects every possible old- and
new-configuration election quorum. At the commit-time halt, anything still in
flight is answered `MSG_V2_RETRY`.

## Observed envelope

Measured on a 3-host `c6id.2xlarge` fleet;
[full record](../benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md).

| Measure | Value |
|---|---|
| Linearizable reads | ~953,000/s at p50 1.08 ms, under 20,000 writes/s |
| Relative to barrier-less throughput | within ~3% |
| A single read with no concurrent reads | one probe RTT, ~0.16 ms p50 on LAN |

Batching gains appear only under concurrent reads.

## Diagnostic signatures

| Observation | Meaning |
|---|---|
| A burst of reads all resolving `RETRY` after ~1 s | the round cannot reach quorum — partition or deposition |
| Sub-second read stalls under packet loss | not expected; the 2 ms retransmit covers loss. Suspect a cause other than loss. |
| `MSG_V2_BAD_SERVICE` answered on `egress_node` | the client named an FSM id this node has no ring for (undeclared, ≥ 8, or a non-zero id on a harness node); the SDK refuses such ids locally, so this means a raw ring writer or a client attached to a differently-declared node |
