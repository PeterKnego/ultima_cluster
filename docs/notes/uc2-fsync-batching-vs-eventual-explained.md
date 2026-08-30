# Batched fsync is not eventual durability, explained

**Audience:** developers reasoning about UC's durability posture, or anyone
tempted by "we already batch fsync, so isn't it eventual anyway?" — and the
mirror temptation, "eventual must be faster, fsync is expensive."
**Status:** explanatory. The normative descriptions live in the code
(`uc_log/src/archive.rs`, `uc_journal/src/journal/writer.rs`,
`ArchiveConfig::durability`) and the benchmark record
(`docs/benchmarks/uc2-aeron-parity-2026-08-15.md`, eventual-durability arm);
if this note and those disagree, they win and this note is stale.

---

## The one-sentence version

Durability is a property of the **acknowledgement**, not of the bytes:
"consistent" means the ack waits for the fsync that covers it, "eventual"
means it doesn't — and **batching is orthogonal**, because a batch just
decides how many entries share one fsync, not which side of it the ack
sits on.

## The invariant, precisely

Under UC's default (`Durability::Consistent`), a byte position may be
*reported durable* — and therefore count toward quorum commit, and
therefore ever produce a client response — only after the fdatasync
covering its bytes has returned. Concretely, one archive duty cycle
(`archive.rs::do_work`):

1. drain a recordable slice (≤1 MiB) from the log buffer — the poll
   batching IS the group commit;
2. one journal append for the whole block;
3. `notifier.wait()` — blocks until the block's fdatasync completes;
4. **only then** advance the durable counter that consensus reads.

Thousands of entries share step 3's single fdatasync; every one of them
waits for it. The batch amortizes the *throughput* cost of the guarantee
without weakening the guarantee. Aeron Cluster at
`aeron.archive.file.sync.level=1` has the same shape: the recorder syncs
before the recorded position advances, and commit follows recorded
positions. Both systems batch; both are honestly durable.

This is the canonical group-commit design. Postgres with
`synchronous_commit=on` batches WAL fsyncs across concurrent transactions
and is uncontroversially durable; etcd's batched WAL sync, Kafka
`acks=all`-with-flush — same family.

## How the batch sizes itself (it is NOT fixed)

Only the CAP is fixed. Each archive duty cycle takes everything appended
since the durable frontier (`buffer.rs::recordable_slice`: append counter
minus durable position), trimmed to whole frames — never splitting one —
and capped at `max_block_bytes` (1 MiB default; the node clamps it to
`min(1 MiB, journal_segment_bytes/2)`, floor 4 KiB; a single frame larger
than the cap records alone as one block).

There is no timer, no minimum, no Nagle-style wait. The batching window is
the duration of the PREVIOUS block's write + fdatasync: whatever arrives
while block N syncs becomes block N+1 — self-clocking group commit, the
Postgres/etcd shape. Consequences:

- Low rate: a lone frame records on the next duty cycle, microseconds
  after landing — no added latency waiting for company; per-frame syncs
  are fine when syncs are rare.
- High rate: batches grow with load automatically (arrival rate × sync
  duration) up to the cap; at the cap, fsync frequency = throughput ÷
  1 MiB (~134 syncs/s at ~1.4 M × 96 B), which is why the per-op fsync
  share is noise.
- Worst-case added latency is structurally bounded: a frame waits at most
  one in-flight block's sync plus its own block's, and blocks cannot
  exceed the cap.

Hence `archive.rs`'s header line: "the poll batching IS the group commit:
fsync frequency scales with block rate, not message rate."

## "But there's still a window where bytes are only in page cache!"

Yes — in every mode, always. The question is what may *escape* during
that window:

- **Consistent:** between `write()` and fdatasync, those positions are not
  durable, not committed, and no client holds an answer. Power loss there
  loses only UNACKNOWLEDGED requests — the client never got a response,
  times out, retries. That is not a durability violation; un-acked
  in-flight data is losable in every system ever built.
- **Eventual** (`UC2_JOURNAL_DURABILITY=eventual`, opt-in): the durable
  counter advances on the buffered write; the fsync happens later (ALSO
  batched — on a 50 ms timer, `writer.rs::eventual_interval`). During
  that window the positions are committable and a client may already hold
  a response. Power loss there loses ACKNOWLEDGED writes; a power-lost
  node can restart with a shorter log than it reported durable.

So "eventual" is not "more batching." It is moving the ack to the wrong
side of the fsync. The contract "no acknowledged write is lost on power
failure" is exactly what changes, and nothing else.

(With quorum replication on top, eventual mode's loss model is
"replication durability": a single node's power loss is covered by the
quorum; a simultaneous quorum power loss can lose acked writes. That is
the standard tradeoff Aeron `sync.level=0` and Kafka acks-without-flush
make — legitimate to CHOOSE, but a different contract, which is why UC's
knob is opt-in, fail-closed on typos, and loud when active.)

## What the fleet measurements showed (2026-08-15/16, both systems, same hardware)

- **Throughput: fsync is not the bottleneck in either system.** UC's
  eventual arm changed throughput by nothing outside fleet noise — at
  ~1.4 M ops/s, one fdatasync per ≤1 MiB block is thousands of entries per
  sync; the per-op share is noise, and the archive pipeline overlaps the
  wait with everything else. Aeron at level 0 didn't demonstrably move its
  knee either (its ceiling is consensus/pipeline-bound too).
- **Latency: depends where the sync sits.** Aeron's recording sync is
  close to its per-op path — level 0 cut its sustained-rate p50 by 3-4×
  (265-354 µs → 84-120 µs). UC's group-commit sync was never a visible
  per-op latency term at the measured operating points — eventual changed
  UC's p50 not at all.
- **The p99 irony: batched-and-WAITING beat batched-and-DEFERRED.** UC's
  eventual mode consistently regressed p99 from ~1 ms to ~4.7 ms. The
  deferred batches are BIGGER (50 ms of accumulation, ~6-7 MB at these
  rates, vs arrival-paced ≤1 MiB blocks), and each lump occupies the
  single journal writer thread for several ms; buffered writes — which
  are what resolve the archive's notifier under Eventual — queue behind
  it, so the commit frontier stalls in periodic bites. Small paced
  flushes smooth I/O; deferral concentrates it (the Postgres
  checkpoint-smoothing lesson). Removing fsync from the ack path does not
  remove it from the machine.

**Standing recommendation** (from the benchmark record): keep
`Durability::Consistent`. The eventual knob has no performance case on
this hardware class — it buys UC no throughput, no median latency, and a
real tail regression, while weakening the contract.

## Rules of thumb this note wants to leave behind

1. Ask "does the ack wait for the fsync?", never "how often does it
   fsync?".
2. Batching amortizes the cost of a guarantee; it does not change the
   guarantee.
3. If someone proposes dropping fsync "for speed", ask which term of the
   measured latency/throughput budget fsync actually occupies — here it
   was ~none of either.
4. Deferring work is not free: deferred batches are bigger batches, and
   bigger batches have worse tails than paced ones.
