# How to diagnose a node that is not serving

Work out what a live node believes about itself and the cluster, when clients
are failing, replication has stalled, or nobody appears to be leading.

These interpretations ship as alert rules — see
[Monitor a cluster](monitor-a-cluster.md#install-the-alert-rules) for the
Prometheus rule that encodes each threshold below, so you do not have to
watch for these by hand.

Everything below is readable from the node's cnc page while it runs. Start with
`uc2ctl status`; drop to raw offsets when you need a field it does not print,
or when you cannot run a binary on the host.

```bash
uc2ctl status --instance-dir D --app-id A
```

Attach read-only with `CncPage`, or read the fixed offsets with `xxd` — the
layout is pinned and does not drift. Field-by-field detail is in
[The cnc control page](../reference/cnc-page.md).

## Is anyone leading?

Read `flags` at offset 768.

| Value | Meaning |
|---|---|
| `0x03` | this node is the serving leader |
| `0x01` | elected, but not yet serving — its NewTerm frame is not yet quorum-committed |
| `0x00` | follower or learner |

A cluster where every node reads `0x00` has no leader. A node stuck at `0x01`
has won an election but cannot get its first frame committed, which usually
means it cannot reach a quorum.

`leader_hint` at 832 gives the last known leader id; `u64::MAX` means unknown.

This same table now also drives `/readyz`: a node reading `0x01` answers the
probe `503` with `"NewTerm"` in the body, so a load balancer routes around it
without needing to decode the cnc page itself. See
[Monitor a cluster](monitor-a-cluster.md#the-probe-endpoints).

## Is the node alive, and is its service alive?

Compare `node_heartbeat_ns` (896) and `service_heartbeat_ns` (960) against your
own clock. They are separate processes and fail separately: a frozen service
heartbeat with a live node heartbeat means the apply loop is wedged, not the
cluster.

## Which FSM is holding the cluster up?

Since M14 a node runs one FSM per declared id, and the slowest one paces
everything: page 1's service band is the `min` over declared ids, the
admission door is `append − min(applied) ≤ fsm_lag`, and this node's durable
report is capped at `min(applied) + fsm_lag`. So a single sick FSM stalls
commits cluster-wide — by design, and visibly.

Reading the raw page (cnc 3.1, FSM identity): within a service slot
(`CNC_OFF_SERVICE_SLOTS + row * CNC_SERVICE_SLOT_STRIDE`, stride 512 B), the
status line's second word (offset `+8`, line 0 word 1) is the attached
service's packed version, and line 7 (offset `+448..+512`) is the row's name
— `[u8; 32]` NUL-padded at `+448`, its FNV-1a 64 hash as a `u64` at `+480` —
**written once by the node itself at boot**, not by the service (every other
line in the slot is written by the service). Field-by-field detail:
[The cnc control page § Service slots](../reference/cnc-page.md#service-slots).

Start with `uc2ctl status`, which prints the whole band without a scrape:

```text
services: declared=[0, 1] fsm_lag=8192 bytes
  row=0 name=kv version=1.2.0 hash=0x9a1c4e2f7b0d3a11 attached=true epoch=3 incarnation=3 applied=1048576 lag=0 snapshot_pos=1040384 heartbeat_age=0.004s
  row=1 name=orders version=0.0.0 hash=0x3f0e7c9a2b8d1f45 attached=false epoch=0 incarnation=0 applied=0 lag=1048576 snapshot_pos=0 heartbeat_age=never
```

(`name=`/`version=`/`hash=` are new since FSM identity, 2.11 pending; earlier
releases printed `id=` where `row=` now is, and no `name=`/`version=`/
`hash=` fields.)

Read it in this order:

1. **`attached=false`** on a declared row (`Uc2ServiceAbsent`) — that FSM's
   process is not running, or it refused to attach. Check the service's own
   logs for `UnknownFsm` (its `S::NAME` — not `name=` above, which is the
   *node's* declared name for that row — is not in this node's `[services]
   names`, or matches no row at all) or the `service.<row>.lock` refusal (two
   processes, one row). `heartbeat_age=never` distinguishes "never started
   since this node booted" from "was running, stopped".
2. **`attached=true` with a stale `heartbeat_age`** (`Uc2ServiceWedged`) —
   the apply loop is wedged inside `apply()`, not the cluster. The
   `[log]` records say which: `service_attached` then no `service_detached`
   means it is still holding its slot.
3. **`lag` pinned at `fsm_lag`** (`Uc2ServicePinnedAtLagBound`) — that FSM is
   running, just slower than the log. (The rule is gated on
   `uc_service_attached == 1`, so a *detached* FSM whose lag has drifted to
   the bound pages as `Uc2ServiceAbsent` instead — case 1, not this one.)
   Nothing is broken; the cluster is being paced to it, which is what a bound
   buys you. Either make that FSM faster or accept the rate. Raising
   `fsm_lag` buys latency headroom, not throughput, and it is refused above
   `buffer_bytes / 2`.

`uc_service_lag_waits_total{service}` tells you the converse: an FSM whose
wait counter climbs is the one *being* paced, i.e. a victim, not the cause.
The cause is the id with the largest `uc_service_lag_bytes`. Since **2.8.1**
the counter is reliable in both modes: a bounded FSM parked at a cap that
sits mid-frame counts its wait episode too (before that it read 0 on exactly
the FSM an operator was looking at). It counts EPISODES, not cycles or
frames — one increment per park, however long the park lasts — so read its
RATE as "how often this FSM is being held", not as a duration.
`uc_service_lag_bytes{service}` (what `Uc2ServicePinnedAtLagBound` keys on)
is the pinned-at-bound signal.

The transition records name arrivals and departures explicitly:
`{"event":"service_attached","node":0,"service":1,"epoch":4}` and
`{"event":"service_detached","node":0,"service":1,"epoch":4}`. Departure is
edge-triggered on either the slot's ATTACHED bit clearing (an orderly stop,
reported within a duty cycle) or the heartbeat ageing past 3 s (a killed
process — nothing clears the bit for it).

## Is replication moving?

Read the counters at 256, 320, 384, 448, 512. On a healthy leader:

```
append ≥ durable ≥ commit,  service_applied trailing commit by the apply lag
```

If `commit` is not advancing while `append` climbs, the leader is not getting
acknowledgements from a quorum. Look at the per-peer band next.

## Which peer is behind?

Only the leader publishes the per-peer band at offset 1408; on a follower the
whole band reads zero. Slots are voting followers first, then learners.

For each slot, per-peer replication lag is `commit − reported_durable`.

A peer whose `reported_durable` is pinned at 0 has never been heard from at
all. The usual cause is an address mismatch rather than a network fault — see
the bind check in [Run a cluster](run-a-cluster.md#bind-the-exact-address-you-advertise).

## Is purging keeping up?

Compare `archive_first_base` (1344) with `node_snapshot_floor` (1216).

| Observation | Meaning |
|---|---|
| `archive_first_base` climbing toward `node_snapshot_floor` | purge is working |
| `archive_first_base` pinned at 0 while the floor advances | purge is off, or not running |
| `archive_first_base` lagging indefinitely with purge enabled | the archive purge is failing — check node logs; errors are logged and retried, never fatal |

If purging is meant to be on, see
[Keep the journal from growing without bound](bound-journal-growth.md).

## Is the disk about to fill?

Read `free_disk_bytes` at offset 3840, or scrape `uc2_free_disk_bytes` if
`[metrics]` is on — see
[Monitor a cluster](monitor-a-cluster.md#watching-the-disk-before-enospc-hits-it)
for the full picture, including the `Uc2DiskLow` alert. It is written by the
`uc2-node` daemon only, on its ~1s outer-loop cadence; `0` (and, on the
metrics scrape, its outright absence) means no daemon is publishing it here,
not "the disk is full."

Free space under about four journal segments' worth is worth acting on before
it reaches zero. The journal writer **fail-stops** on any write or fsync `io`
error — `ENOSPC` included — which halts the archive agent, logs
`agent_failstopped`, and exits the daemon with code 1 for systemd to restart.
This is loud and asserted by design, not a silent degradation: see
`examples/uc_crashtest/tests/enospc.rs`. Recovery is exactly "free the space,
then let systemd restart it" — no special procedure; the node rejoins by
replaying its journal, the same as any other clean restart.

Purging is the durable fix, not a one-time cleanup: see
[Keep the journal from growing without bound](bound-journal-growth.md).

## My node just fail-stopped with `IngressRingWedged`

Since 2.7.0, the two client-facing MPSC rings (`ingress.ring`,
`query.ring`) commit per record. A client claims a slot, stamps a claim
word, writes its body, then commits — and if it dies in the nanosecond
window between the CAS that claims the slot and the store of that claim
word, the consumer can never learn how long the hole is. Rather than
guess, the consensus agent panics:

```
consensus fatal (fail-stop): IngressRingWedged ring=<ingress|query> position=<n>
— a producer died between its claim and its claim word; the hole's length
is unknowable. Restart the node; every attached client must reattach.
```

"A client died" is not the whole story, and worth knowing before you go
looking for a crashed process: **a client that only stalled** — `SIGSTOP`,
a debugger breakpoint, heavy hypervisor CPU steal — in that same
claim-to-stamp window is indistinguishable from a dead one, and produces
the identical fail-stop even though the client is about to resume. This
is the one place in the ring protocol where a merely slow client can take
the node down, possibly the leader, and it is why the window is a few
atomic instructions wide rather than a network round trip.

Contrast the much larger, and safe, window: a client stalled *after*
stamping its claim word (the ordinary case — writing a large record body,
or paused anywhere from there to its commit) is handled by the hole timer
instead. The consumer waits `hole_timeout` (default 1 s), skips the
claimed range, and counts it — on `uc2_ingress_holes_skipped_total` or
`uc2_query_holes_skipped_total`, one counter per ring, so you can see
which client path is affected. If that client then resumes and tries to
commit, its commit CAS fails against the skip marker and it gets back
`Skipped`, not silence.

The residual case is a client so stalled it resumes and writes its body a
full ring lap later, into a slot some later claimant now owns. This is
**not fully caught by the CRC**, and an earlier version of this page said
it was. Three shapes, and only the first is caught:

- **A partial stomp** disagrees with the victim's own trailer, so the
  record fails its CRC. On an MPSC ring that is not a recoverable read
  error — the slot is immutable until the consumer passes it — so it
  surfaces as the `IngressRingCorrupt` fail-stop below, not a retry.
- **A complete same-length stomp** writes a fully self-consistent record
  — the resurrected client's own payload and its own CRC — over the later
  claimant's. The CRC *matches*, and the node delivers the resurrected
  client's record at the later claimant's position. The later claimant's
  submit is silently lost; its client sees no response and retries on
  timeout, and the resurrected record may be applied twice.
  **Exactly-once across that survives only if your service wraps its
  state machine in `Sessioned`** — the `(client_id, seq)` envelope is
  what turns the duplicate into a `REPLAYED` answer.
- **A padding stomp** — a client resurrecting inside the tail-padding
  path — is not CRC-covered at all, because the padding marker is
  recognised before any CRC is computed. The node closes this by
  accepting a padding marker only when its length is exactly the tail
  remnant, which is the only length real padding can ever have; anything
  else goes down the ordinary record path and meets the CRC. The
  remaining residual: a real record that ends flush with the ring tail is
  indistinguishable from padding by that test.

All three need a client stalled for longer than `hole_timeout` *and* a
full ring lap in the meantime. If you are seeing them, the thing to fix
is the stalling client (or `hole_timeout`), not the ring.

Recovery from `IngressRingWedged` is the same as any other fail-stop:
`agent_failstopped` is logged, the daemon exits 1, and systemd restarts
it. The rings are volatile, so the restarted node starts clean — but
because the ring file format changed in 2.7.0 too, every process
attached to this host's instance directory (service, gateway, any shmem
client) needs the node to be back up before it can reattach; see
[the same-host restart rule](upgrade-a-cluster.md#ring-format-change-in-270-restart-a-hosts-processes-together).

## My node just fail-stopped with `IngressRingCorrupt`

```
consensus fatal (fail-stop): IngressRingCorrupt ring=<ingress|query> (<detail>)
— the record at the consumer position does not decode, and an MPSC slot is
immutable until the consumer passes it, so this cannot be retried. Restart
the node; every attached client must reattach.
```

A record at the ring's consumer position failed to decode — a bad CRC, or
a length outside the ring's own bounds. On these rings that is *not* a
transient read error: no producer may reclaim a slot the consumer has not
released, so the next drain cycle would read the identical bytes, forever.
Before 2.7.0's final review the node treated it as transient, which meant
a permanent silent stall of every client on that ring while the node
still reported itself able to serve. It now fail-stops.

The realistic causes are the stomp shapes described above (a client
stalled past `hole_timeout` that resurrected a lap later) and memory or
storage corruption of the mapped ring file. Recovery is identical to
`IngressRingWedged`: restart the node, and every attached process
reattaches.

## Is a joiner recovering by snapshot?

Watch `incoming_snapshot_pos` (1280) on the joining node. It advances when a
below-floor member installs a snapshot before tail-replaying.

## A node that truncated to zero and rejoined

If a rejoining node's log has no common prefix with the leader — because the
leader purged past the point where they diverged — the node truncates to 0 and
rejoins from the snapshot floor. Its `wipes()` counter increments.

This is automatic and safe. It is not a fault to investigate, though a node
doing it repeatedly is worth understanding.

## If crypto is enabled

Check the drop counters, and judge by the followers rather than the leader —
the leader's `seal_failures` climbs benignly. The table is in
[Encrypt traffic between nodes](encrypt-node-traffic.md#confirm-it-is-healthy).

A non-zero `cleartext_peer` means some node in the cluster is still running
cleartext.

## If reads are failing but writes are not

A burst of reads resolving `RETRY` after about a second means the read barrier
cannot reach quorum — a partition, or the leader has been deposed. Sub-second
read stalls under packet loss should not happen; the barrier retransmits every
2 ms. See [Linearizable read path](../reference/read-path.md#diagnostic-signatures).
