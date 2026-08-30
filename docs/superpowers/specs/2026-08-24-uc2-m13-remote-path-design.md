# UC v2 M13 — remote-path performance & flow control: design

**Status:** approved approaches (A/A/A) 2026-08-24; spec for user review.
**Baseline & root causes:** `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`
(the per-hop isolation bench) and
`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`.
**Renumbering:** multi-service (spec `2026-08-21-uc2-multi-service-design.md`)
is now **M14**.

## 1. Goal

Make the remote path — `client → TCP → Edge → shmem Engine → node` — run at
the cluster's speed and degrade, never collapse, under more connections
than the host has cores. Three defects found by the hop bench, three fixes:

| # | defect (measured) | fix |
|---|---|---|
| 1 | `uc_remote::RemoteClient` caps at ~170 k resp/s per connection against a sink that answers instantly (1 `write` per `submit`, ~7 futex per request: one state lock across submit/write/every received frame, a channel per ticket). A raw client through the *real* edge and cluster does 1.14 M/s on one connection. | **Engine-shaped split client** (§3) |
| 2 | `uc_protocol::ring::mpsc` producers publish in claim order and spin on their predecessor; once producer threads outnumber free cores a preempted producer convoys all of them — 1.9 M/s → ~5 k/s at 8 gateway connections on 8 vCPU, every core busy. The M12 "collapse". | **Per-record commit, no cross-producer wait** (§4) |
| 3 | The edge grants each connection a flat `per_conn_inflight`; no budget across connections (spec M12 §4.2 promised one). Not the collapse cause, but it is why overload past the `Engine` window is the reactive halve/relax ladder instead of bounded queueing. | **Global outstanding-grant budget at the edge** (§5) |

Out of scope, stated: N drivers over N `Engine`s in the edge (its single
driver's knee ≈ 1.9 M/s equals the cluster's ceiling today — §8 follow-on);
any change to the node↔node wire protocol, the cnc page, `Sessioned`, or the
remote wire protocol v1 (its raw floor is 7.4 M/s per connection — §6).

**No compatibility constraint** (user decision 2026-08-24: no external
users). The client API is replaced; the MPSC ring's header semantics change
(same-host restart of every process attached to an instance dir — not a
wire flag day).

## 2. Pre-committed bars (fleet, 4× `c6id.2xlarge`, `hop_bench` + `m13_hop_bench.py`)

Adjudicated the way every prior milestone was — honestly, in the gate doc
`docs/benchmarks/uc2-m13-gate-<date>.md`, row by row:

| row | measure | bar |
|---|---|---|
| a | ONE new client connection through the real edge into the real 3-node cluster (`hop_bench remote-load` rebuilt on the new client, conns=1, inflight 1024) vs the direct `Engine` arm on the same generation | **≥ 0.5× direct** resp/s |
| b | N-connection aggregate through the edge (new client), best rung ≤ 16 | **≥ 0.75× direct** |
| c | Ladder N = 1,2,4,8,16 at 1024 inflight on the co-located host, with the ring fix + edge budget | **monotone, no collapse**: 0 lost, p99 bounded (< 1 s at every rung), no rung > 20% below the previous rung |
| d | N local `Engine`s into one node on an oversubscribed host (`engine-load --engines 1,2,4,8` on the 8-vCPU server host, and locally on 4 vCPU) | **≤ linear degradation**: resp/s at N engines ≥ (cores / busy threads) × single-engine resp/s × 0.5, never below 10% of single-engine |
| e | Ring correctness: existing 73 `uc_protocol` tests + new preemption test + loom model | green |
| f | Correctness capstones on the new client: `remote_lin` (envelope on/off), `uc_gateway` tests, `client_fake_edge` suite ported | green |

Reference numbers from the bench: direct arm 1.9–2.6 M/s; raw client
through edge+cluster 1.14 M/s (0.6×) at N=1 and 1.43 M/s (0.75×) at N=2.

## 3. The client: `uc_remote` split halves

### 3.1 Shape

Mirror `uc_client::Engine` exactly — the shmem client that moves 2.8 M/s —
over a TCP connection:

```rust
pub struct RemoteEngine;                     // constructor namespace, like Engine
impl RemoteEngine {
    pub fn connect(cfg: RemoteConfig) -> Result<(RemoteSendHalf, RemotePollHalf), RemoteError>;
}
impl RemoteSendHalf {                        // Send, !Sync — one submitter thread
    pub fn try_submit(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError>;
    pub fn try_query(&self, user_data: u64, consistency: Consistency, q: &[u8]) -> Result<(), SubmitError>;
    pub fn credits(&self) -> u32;            // last grant seen
    pub fn inflight(&self) -> u64;
    pub fn stats(&self) -> RemoteStats;
    pub fn leader(&self) -> Option<(u32, String)>;
}
impl RemotePollHalf {                        // one poller thread
    pub fn poll(&mut self, cb: impl FnMut(RemoteCompletion<'_>)) -> usize;
    pub fn wait_handle(&self) -> RemoteWaitHandle;   // park until something completes
}
pub struct RemoteCompletion<'a> { pub user_data: u64, pub position: Option<u64>, pub outcome: RemoteOutcome<'a> }
pub enum RemoteOutcome<'a> { Response { body: &'a [u8], replayed: bool, expired: bool }, Unknown, PayloadTooLarge, TimedOut, Closed }
```

`SubmitError::Backpressure` means "no credit / local window full — try
again"; the submitter decides whether to spin, yield, or park on the wait
handle (exactly `Engine`'s contract). A blocking convenience,
`RemoteClient` (rebuilt: `submit(&[u8]) -> Ticket`, `Ticket::wait`), is
layered on top for the crashtest, `counter-remote` and small callers, the
way `uc_client::Client` sits on `Engine`; its cost is its own and it is not
the path the bars measure.

### 3.2 Hot path (per connection: two threads, no lock, no per-request allocation)

- **Submitter (caller's thread, `try_submit`)** — checks the window from
  two atomics (`acked_seq + credits` from the edge, local `max_inflight`),
  assigns `seq`, encodes the frame **directly into a preallocated outgoing
  byte ring** (in-process SPSC, capacity ≥ `max_inflight × (HEADER_LEN +
  max_payload)`), records `(seq → user_data, kind, sent_ns)` in a slot table
  indexed by `seq & mask` (the `Engine` `SlotTable` design, reused), and
  returns. No syscall.
- **Writer thread** — drains the outgoing ring: whatever is there goes out
  in **one `write_all`** (flush-on-empty, no timer; idle → park on the
  ring's wait word). This is where batching happens *regardless of whether
  the credit window is open*, which is the property the current client
  lacks. It also owns the socket for dial/redial (§3.3).
- **Reader thread** — `read_frame_buffered` + `next_buffered` (64 KiB
  reads, as today), and for each frame: updates `credits`/`acked_seq`
  (atomics), resolves the slot, and pushes `(user_data, position, outcome,
  body)` into a **bounded SPSC completion queue** (capacity `max_inflight`;
  body bytes copied once into a per-queue arena). One wake of the poller
  per read batch, not per frame.
- **Poller (`poll`)** — drains the completion queue and invokes the
  callback; returns the count. A `wait_handle()` lets the poller park
  between batches (futex on the queue's wake word; same shape as
  `PollHalf::wait_handle`).

Syscall budget per request at steady state: `< 1/64 write + < 1/64 read`
per request (batch of 64), 0 futex on the hot path when both sides are
busy; one futex wake per *batch* when the poller/writer is parked.

### 3.3 What stays from today's client (moved, not dropped)

Redirect/`LEADER_CHANGED` following with the static member map; HELLO
handshake and app-id check; `RETRY{not_serving | service_unavailable |
instance_restart}` with `retry_after`; the **not-serving latch**
(a connection refused a write is abandoned, never retried on the same
socket); **probe-before-flush** on an unproven connection (one frame, then
the window); `PING`/`PONG` liveness and `dead_after`; the mid-frame stall
deadline; `request_timeout` as an end-to-end budget swept by the reader;
`FLAG_REPLAYED`/`FLAG_EXPIRED`/`FLAG_ENVELOPED` mapping; `max_credits_seen`
and the other `RemoteStats`. Reconnect + resend: the slot table *is* the
unacked window — on redial the writer re-encodes every slot with
`seq > acked_seq` in order from the slot table (frames are kept in the
outgoing ring until acked, so no re-encoding is needed while the ring has
them). All of this runs on the writer/reader threads; the only lock in the
design is a **reconnect mutex** taken on socket error and on dial — never
per frame.

Ordering/semantic invariants carried over verbatim: seqs are per client,
strictly increasing, start at 1; `acked_seq` is monotone; a `credits` value
in RESPONSE/STATUS is an absolute grant honoured immediately for new seqs;
`Sessioned` exactly-once is unchanged (same `client_id ++ seq` envelope).

### 3.4 Harness, tests

`hop_bench remote-load` and `m12_gate client-remote` move to the halves
(poll-thread + submitter, the `engine-load` shape). `client_fake_edge.rs`'s
28 scenarios are ported to the halves (the scripted fake edge stays); the
`remote_lin` capstone runs on the blocking convenience (unchanged
semantics). New unit tests: window arithmetic (credits vs local cap), the
outgoing ring under wrap, completion-queue backpressure (poller slower than
the reader → reader parks, never drops), resend-after-redial reproduces the
exact frames.

## 4. The MPSC ring: per-record commit

### 4.1 Protocol

The design is the one `../ultima_rings` ships as its `mpsc` (per-slot
**round stamp**, consumer stops at a hole; loom + Miri verified there,
`docs/design.md` §MPSC), carried over to UC's variable-length byte records:
the "round" is the ring **lap** the byte position belongs to.

The record's first word (`length: u32`, today "0 = uncommitted") becomes the
**commit word**:

```
bit 31      CLAIMED   (set between claim and commit)
bits 18–30  LAP       = (record_start_pos / capacity) & 0x1FFF   (13 bits)
bits 0–17   LENGTH    total record bytes (max_msg_size ≤ 64 KiB fits in 18 bits)
```

Producer (`try_write`):
1. CAS-claim `[pos, pos + advance)` on `claim_position` — unchanged, and
   still **bounded** by the Acquire-loaded `consumer_position` (so a claim
   succeeds only after the consumer has consumed the slot's previous
   occupant: the same "index pair proves the slot is safe" argument the
   ultima_rings design makes).
2. Store the claim word `CLAIMED | LAP | advance` (Relaxed) — this is what
   lets a dead producer's hole be sized (§4.2).
3. Write `msg_type`, `flags`, `header_extra`, payload, crc32 (unchanged).
4. **Commit**: store `LAP | total` (CLAIMED clear) with **Release**.
5. `commit_count.fetch_add(1, Release)` — the futex wake word
   (`publish_position`'s slot, renamed; no longer a byte position) — then
   `signal()` (unchanged).

No producer waits for any other producer at any step. On `Full` the
producer returns `RingError::Full` exactly as today — it never spins — and
every UC caller already yields or parks on that (`Engine` maps it to
`Backpressure`; the edge's dispatch ladder yields then parks). That matters:
ultima_rings measured the same per-record-commit ring at 8× oversubscription
at **36 Melem/s with yielding producers vs 4.8 with spinning ones**
(`bench-results/2026-08-12-cpu-cost-and-heap-payload.md`, Part 2) — the
protocol removes the convoy, the yield keeps it removed.

Consumer (`try_read`, single, **read-only on the ring** — it never writes a
slot):
1. Acquire-load the commit word at `consumer_position`; compute
   `expected_lap` from the position.
   - `LAP != expected_lap` → this slot still holds a previous lap's record
     (nothing claimed here yet) → `Ok(None)`.
   - `CLAIMED` set (and lap matches) → claimed, not committed → `Ok(None)`:
     head-of-line on exactly that producer, **no spin, no burn**; start /
     continue the hole timer (§4.2).
   - committed, lap matches → read the record (crc check as today; a crc
     failure is still a fail-stop `Corrupt`), then
     `consumer_position.store(pos + LENGTH, Release)`. Padding markers are
     consumed the same way.

Why the lap and not zeroing: the bounded claim guarantees a producer only
ever overwrites a slot the consumer has consumed, so the only stale value
the consumer can meet is an **older** lap's committed word, which fails the
lap equality; 13 bits is unambiguous because the consumer can never be
8,192 laps behind a claim (the bound is one lap). The consumer therefore
makes no writes into the ring at all — one fewer cache-line transfer per
record than the zeroing variant, and the single-writer-per-line discipline
UC's shmem layer already keeps.

Memory ordering: the producer's free-space check Acquire-loads
`consumer_position`, which synchronizes-with the consumer's Release store
after its read, so the producer's stores never race a read of the previous
occupant. The consumer's Acquire load of a committed word synchronizes-with
the producer's Release commit store made after the whole record, so the
record bytes are visible — this replaces the comment in `write_record_at`
that relied on `publish_position`, and `try_read_record_at` gets an explicit
atomic Acquire load of the commit word on the MPSC path.

### 4.2 The dead-producer hole

A producer that dies (SIGKILL) between step 1 and step 4 leaves a
permanent hole. Today the same death wedges **every** producer forever
(they spin on `publish_position`) and the consumer with them; this design
makes it detectable and bounded:
- CLAIMED hole older than `hole_timeout` (config, default 1 s; the slowest
  legitimate claim→commit is microseconds): the consumer **skips** it
  (advances by the claimed LENGTH), increments a `holes_skipped` counter
  exported on the cnc page reserved band and `/metrics`, and logs once per
  hole. The client that died never gets an answer — correct, it is dead.
- A stale-lap word while `claim_position > consumer_position` for longer
  than `hole_timeout` (the producer died between the CAS and the claim
  word — a window of nanoseconds): the length is unknowable; the node
  fail-stops with a named error (`IngressRingWedged`) rather than guessing.
  Documented as the residual; it is strictly better than today's silent
  wedge.

### 4.3 Scope and tests

MPSC only. SPSC and Broadcast keep their single-producer `publish_position`
protocol (no convoy is possible with one producer). Ring file format: the
header's `publish_position` slot becomes `commit_count`; the magic string
is bumped (`UC2RING2` or similar) so a stale attach is refused, and
`RING_HEADER_LEN` is unchanged. Tests:
- existing 73;
- **preemption test**: producer A claims and *stops* (a barrier), producers
  B..H commit behind it, the consumer returns `None` until A commits, then
  reads A..H in claim order — nobody spun (assert B..H returned from
  `try_write` while A was stopped);
- hole-skip test and the zero-hole fail-stop;
- **loom model** of claim/commit/consume with 2–3 producers over a
  `Vec`-backed header (this is the loom-on-rings item from the M12d
  security package; the mmap'd rings stay outside loom/Miri — the model
  covers the protocol, not the mapping). `../ultima_rings/src/mpsc.rs`'s
  loom harness is the template — same protocol, fixed-size slots there vs
  byte records here;
- the fuzz target `uc_protocol_cnc` is untouched; a new `ring_mpsc_record`
  target fuzzes the reader against arbitrary length words (claimed bit,
  over-length, zero).

Gate row d is the performance proof; the dev-box convoy reproduction
(`engine-load --engines 4` on 4 vCPU) is the regression smoke.

### 4.4 Why not the sharded ingress now

`../ultima_rings` also ships `sharded` — one SPSC ring per producer, the
consumer sweeping them — and it is the stronger answer to oversubscription:
no shared claim, no cross-producer hole at all, **118 Melem/s at 16
producers on 16 cores where the shared-claim ring reads 2.4**, 91 at 64
producers 4× oversubscribed (`bench-results/2026-08-16-sharded-ladder-skew.md`).
Its contract is what UC's ingress does not have today: a **fixed producer
set** (`Sender` is not `Clone`) and per-producer FIFO. UC's ingress
producers are dynamic client processes and edge reader threads; a sharded
ingress means per-client ring files under the instance dir, a shard
registry in the cnc reserved band (owner `client_id`, liveness), reaping of
dead shards, and the node's ingress drain becoming an N-shard sweep. Order
is not the obstacle — UC only needs per-client FIFO (`Sessioned` is keyed
`client_id ++ seq`; cross-client order is arbitrary by design).

Not in M13 because the ring's throughput is not the problem: one `Engine`
already moves 2.8 M records/s through the shared ring against a 1.9 M/s
cluster. What M13 needs is the convoy gone, and per-record commit with
yielding producers does that (§4.1, ultima_rings' own oversubscription
ladder). Sharded per-client ingress is the follow-on (§8) with a named
trigger: row d failing, or the node's ceiling rising past the ring's.

### As built (Task 4/5 review)

Two details of the implemented protocol read differently from §4.1's and
§4.2's prose above. First, "commit" (step 4 of §4.1) is not an
unconditional store: it is a `compare_exchange` expecting the producer's
own claim word, so a producer whose hole was skipped meanwhile learns that
immediately as `RingError::Skipped { position }` rather than silently
overwriting a slot it no longer owns — and the tail-straddle padding
marker publishes through the same CAS, not a bare store. Second, §4.1's
"read-only on the ring — it never writes a slot" holds everywhere except
the skip path itself: marking a timed-out hole (§4.2) is one
`compare_exchange` from the exact claim word the consumer observed to the
skip marker `CLAIMED | LAP | 0`, so a producer that only stalled (not
died) and commits in the same window loses that race harmlessly and is
delivered normally, uncounted. See `uc_protocol/src/ring/mpsc.rs`'s module
doc for the full as-built description.

Third — corrected by the final review, because the earlier wording here
and in four other places was **false**: the crc32 does NOT catch every
resurrection a full lap late. It catches a *partial* stomp. A *complete
same-length* stomp is a self-consistent record (the resurrected
producer's own header_extra, payload and crc), so it is delivered as that
producer's record at the later claimant's position: the later claimant's
submit is silently lost and retried by its client's timeout, and the
resurrected record can be delivered twice — exactly-once across that is
`Sessioned`'s job, not the ring's. A *padding* stomp is not crc-covered
at all, because `decode_record_slice` short-circuits on
`msg_type == PADDING_MSG_TYPE` before hashing anything; the consumer
therefore accepts a padding marker only when its committed length is
exactly `bytes_to_tail` (the only length real padding can have) and sends
everything else down the crc'd record path, with the stated residual that
a record ending flush with the tail is indistinguishable from padding.
`claim` refuses `PADDING_MSG_TYPE` from callers
(`RingError::ReservedMsgType`) so the ambiguity cannot be manufactured
from above. Canonical statement: `RingError::Skipped`'s doc.

## 5. The edge: global outstanding-grant budget

### 5.1 Rule

`Shared` gains `budget = max_inflight − headroom` (headroom default 1/8 of
the window, absorbing frames already on the wire when a grant shrinks) and
`live: AtomicU32` (ready connections). Each connection's grant is

```
grant = clamp(budget / live, 1, per_conn_inflight)
```

recomputed on every connect/disconnect; a reduction is pushed immediately
to every affected connection as `STATUS{acked_seq, credits}` (the client
honours a lower absolute grant for new seqs at once — no protocol change,
the frame and the client path exist), an increase rides the next RESPONSE
or the STATUS timer. `HELLO_OK` carries the current grant, not the config
constant.

### 5.2 What the reactive ladder becomes

`Backpressure` from the `Engine` still halves that connection's *effective*
credits and relaxes back (`Conn::squeeze`/`relax` unchanged) — but with
grants summing to ≤ budget it fires only on the headroom races, so the
dispatch ladder (64 yields → parks) is the exception path. `STATUS` is also
sent on `squeeze` (today a reduction reaches the client only on the next
RESPONSE — `edge.rs` has no call site; §4.2 of the M12 spec asked for
"before frames leave the client").

Config validation adds `per_conn_inflight × 1 ≤ budget` (a single
connection must be grantable in full) and warns when
`max_connections × 1 > budget` (some connections would sit at grant 1).

### 5.3 Fairness, follow-on

Equal shares are the M13 rule; demand-weighted shares (EWMA of each
connection's outstanding) are listed as a follow-on with the N-driver work
(§8) — not built until a measurement asks for it.

### 5.4 Tests

`credits.rs` gains: N connections' grants sum to ≤ budget at every moment
(assert via a test hook on `Shared`), a connect shrinks everyone's grant
and a STATUS carrying it arrives before the next RESPONSE, a disconnect
grows it; the two-client backpressure test keeps passing with
`backpressure_events` now 0 at equal inflight.

## 6. Wire protocol v1 — unchanged, with two clarifications in the reference

- `credits` in RESPONSE/STATUS is an absolute grant and MAY decrease; a
  client MUST NOT send `seq > acked_seq + credits` after seeing it (already
  the client's behaviour; the reference says so explicitly now).
- `STATUS` MAY be sent at any time, including immediately after a grant
  reduction (today's reference describes it as idle-timer + reopen only).

## 7. Deliverables and docs

- Code: `uc_remote` (halves + convenience client; old `RemoteClient`
  removed), `uc_protocol::ring::mpsc` (+ loom model under `cfg(loom)`,
  fuzz target), `uc_gateway::edge` (budget), `hop_bench remote-load` +
  `m12_gate client-remote` on the halves, `counter-remote` example on the
  convenience client.
- Gate: `docs/benchmarks/uc2-m13-gate-<date>.md` rows a–f above, run with
  `m13_hop_bench.py`; M12 gate row 2 closed by reference to row b.
- Docs at release time (not before): `docs/how-to/run-a-gateway.md`
  operating envelope rewritten (the collapse is gone; the remaining rule is
  connections vs cores and the budget), `packaging/systemd/uc2-gateway.service`
  `CPUQuota=` comment corrected, `docs/reference/remote-protocol.md` §6
  clarifications, `docs/notes/uc2-m12a-edge-flow-control-gap.md` gets its
  correction, `docs/reference/state-machine-contract.md` untouched.
  `RELEASES.md` + `docs/releases.md` entry per the release rule.
- Version: workspace `2.7.0`; ring magic bump documented in
  `docs/how-to/upgrade-a-cluster.md` as "restart node, service, gateway
  and clients on a host together".

## 8. Follow-ons (not M13)

- N drivers over N `Engine`s in the edge (raise the edge knee past the
  cluster's ceiling).
- **Sharded per-client ingress** (`ultima_rings::sharded` shape, §4.4) —
  trigger: row d fails, or the cluster's ceiling rises past the shared
  ring's ~2.8 M/s.
- Demand-weighted grant sharing.
- Node-ingress remote transport (M12 design shape D) — the halves are its
  client SDK.
- Multi-service = **M14**.

## 9. Implementation plans

Three plans, one per independent track, executed in this order (A unblocks
row d and retires the yield mitigation; B is the largest; C's gate rows
a–c need B's client and row d needs A):

1. `docs/superpowers/plans/2026-08-24-uc2-m13a-mpsc-ring.md` — §4 (+ the
   cnc field and `/metrics` line of §4.2), 9 tasks.
2. `docs/superpowers/plans/2026-08-24-uc2-m13b-remote-client.md` — §3, §6
   and every caller migration, 16 tasks.
3. `docs/superpowers/plans/2026-08-24-uc2-m13c-edge-budget-and-gate.md` —
   §5, the §2 gate artefacts (`--arms gate`, the gate doc skeleton) and
   the §7 release checklist, 9 tasks.

Cross-track contracts pinned in all three: the `Engine` API is unchanged
(A); `RemoteClient::{connect, submit, query(&[u8], Consistency), stats,
shutdown}` + `Ticket::{wait, wait_timeout}` + `RemoteStats.max_credits_seen`
stay (B, used by C's tests); `hop_bench remote-load` keeps `--gateways
--secs --payload --inflight --conns` and its `RESULT {"arm":"remote",…}`
line (B, consumed by C's gate arm); `hop_bench edge` defaults
`per_conn_inflight` to 1024 so the new budget refusal does not reject the
harness (C).

## 10. Risks

- The ring change touches the one component every process shares; the
  loom model and the preemption test are the mitigation, and the dev-box
  convoy smoke plus row d are the proof. Rollback is the yield mitigation
  (`d8a168d`), which stays in the tree until the new ring lands.
- The client rewrite must reproduce 28 scripted failover behaviours; port
  the fake-edge suite *first* (TDD) so the halves are built against it.
- Bar a assumes the edge's driver can feed one connection at ≥ 0.95 M/s;
  the raw client already showed 1.14 M/s, so the risk is in the client, not
  the edge.


> **As-built erratum (2026-08-25, Task 2 review):** the §5 grant-settle mechanism
> as drafted — a handshaking reader sampling `grant_gen` and waiting via
> `await_settled` for it to advance — does **not** hold the instant-invariant
> under concurrent connects: `grant_gen` is coalesced and cannot prove that the
> push which advanced it observed this connection's `live++`, so a newcomer can go
> ready against a stale `live` and the sum of granted ceilings can transiently
> exceed `budget`. The invariant (sum ≤ budget at every instant) is unchanged and
> still the goal; the mechanism is replaced by **serializing `{join, recompute all
> ready ceilings, set own ceiling, set_ready}` under a single `Shared::grant_lock`**
> (the driver's recompute and the leave path take the same lock). See the M13 gate
> doc and `.superpowers/sdd/.../task-2-fix-ruling.md`.
