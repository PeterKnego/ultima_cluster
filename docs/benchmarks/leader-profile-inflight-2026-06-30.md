# Leader profile + inflight sweep — the 10k "ceiling" was concurrency-starvation — 2026-06-30

A **correction** to the throughput-attribution conclusions. Profiling the leader at the knee
and then sweeping client `inflight` shows the ~10k throughput ceiling was **not a saturated
resource and not structural** — it was the load generator's fixed `inflight=128` being too low
to fill an idle server across a ms-latency commit path.

## 1. Leader profile at the knee — the box is ~90% idle

Profiled the leader `uc-node-launch` process (node0, the bootstrap leader) on a 2xlarge while
driving a sustained rung **above** the knee (12k offered → server pinned at its ~9.8k ceiling),
linger=2, 3-node consistent. (Method: a `profile` hook added to the bench `run` role — `perf
record -g` + `top -bH` + `ps -T` over the load window.)

- **`%Cpu(s) ~93–96% idle`** box-wide while sustaining 9,842 commits/s.
- The process has 7–8 threads; the busiest (the main/Engine thread) averages **~30% of one
  core**, brief spikes to ~40–70%; everything else (service, ring-bridge, tokio worker) ~0–10%.
- perf on-CPU (the small busy slice): `crc32fast` **17%** (journal/record CRC), anon
  page-faults/alloc **6%** — relevant only *if* it were CPU-bound, which it is not.

**⇒ the leader is not CPU-bound, not thread-bound.** Reaching ~10k with the box idle means it
is **blocked off-CPU** on the serial round-trips (replication ack + fsync + IPC), and nothing
is driving enough concurrent work to fill the idle cores. This killed both earlier hypotheses
(single-threaded Engine CPU-bound; a pegged thread).

## 2. Inflight sweep — the knee rises with concurrency

`throughput = inflight ÷ latency` (Little's law). With `inflight=128` and a ~13 ms commit
latency, `128/0.013 ≈ 10k` — exactly the ceiling we kept hitting. We had held `inflight=128`
(the bench default) through **every** throughput measurement this session. Sweeping it
(linger=2, 3-node consistent):

| offered | inflight=128 (achieved, p50/p99 ms) | inflight=256 (achieved, p50/p99 ms) |
|---|---|---|
| 10,000 | 9,996 — **26.5** / 188 | 9,806 — **5.8** / 216 |
| 15,000 | 12,928 — 921 / 1703 (collapse) | 14,632 — **187** / 510 ✓ |
| 20,000 | collapse | 13,134 — 2506 / 5293 (collapse) |

- **Smoking gun:** at the *same* 10k load, raising inflight 128→256 **lowered p50 from 26.5 ms
  to 5.8 ms.** Lower latency at equal throughput only happens if 128 was the binding limit, not
  the server. At 128, 10k already queues; at 256, the server loafs at 10k.
- **The knee moves:** inflight=128 collapses past ~10k; **inflight=256 sustains ~14.6k** at
  p50 187 ms. Doubling inflight bought ~+46% sustainable throughput.

**⇒ concurrency-starvation, not a structural wall.** The ~10k figure was the `inflight=128`
operating point.

## 3. Limits of this run (honest)

- **inflight=256 is rep-noisy** at the boundary (one rep's SLA-knee scored 10k, the other 15k);
  the *curve* above (rep1) is unambiguous, but the single "knee" number is coarse near the edge.
- **inflight 512 and 1024 all failed** (4/4 reps timed out) — they consistently destabilized
  the run on the [..30000] ladder (overload collapse: the 30k rung at high inflight overwhelms
  the client/cluster). So the **true ceiling above ~15k is unmeasured**, and high inflight has
  its own instability. (The hang-hardened driver kept each failure bounded — no leak.)

## 4. What this corrects

Two earlier conclusions were partly artifacts of the fixed low inflight:
- **"Throughput target met at ~10k"** — the system sustains ≥15k with more concurrency; 10k was
  under-driven.
- **"The replication 2× (10k→20k) is structural"** — substantially conflated with
  concurrency-starvation. The 1-node=20k vs 3-node=10k gap shrinks once the 3-node cluster is
  driven with enough inflight.

The honest standing statement: **there is no saturated-resource bottleneck at 10k.** Throughput
is concurrency-starved against a ms-latency commit path; the lever is client concurrency
(`inflight`), traded against tail latency. The real ceiling is ≥15k and unknown; the open
questions are (a) where the knee plateaus and what saturates there (is the box still idle at
15k?), and (b) why inflight≥512 destabilizes. Both are being chased
(`chase_run` → a follow-up addendum).

## 5. Addendum (chase) — the real ceiling IS a named hot path: leader log-read + CRC

Re-profiled the leader at **inflight=256, sustained ~14k** (the higher knee), and mapped the
knee past 256.

- **The busiest thread climbed to 86%** (from 72% at the lower load) while the box stayed
  ~85–95% idle and all other threads ~0–4%. So as throughput rises, **one thread approaches
  saturation** — the ceiling is a single-thread limit, only *unmasked* once inflight is high
  enough (at inflight=128 concurrency-starvation capped throughput below this thread's limit).
- **The knee plateaus at ~15k**: inflight 128→10k, 256→~15k, 384→~15k (rep-noisy 10/15 at the
  SLA boundary). More concurrency past 256 buys nothing → ~15k is the hard ceiling.
- **`inflight ≥ 512` destabilizes** the run regardless of rate ladder (a separate
  high-concurrency instability — unmeasured, worth a look).

**What that thread burns CPU on (perf call-graph at the ceiling):**
```
34.6% syscall → 21.6% __x64_sys_read → ext4_file_read_iter → filemap_read
                                       → copy_page_to_iter (21.4%, page copy) + ~15% page-faults
21.8% crc32fast::update_fast_16   (record CRC, growing with load: 17%→22%)
 5.9% anon page allocation
```

**⇒ The real throughput ceiling (~15k) is the leader's single-threaded log-read path for
replication/apply.** It **re-reads just-appended log entries back from the journal's on-disk
ext4 segment files** (read + page-copy + faults ≈ 27%) and **recomputes CRC32** (≈ 22%) to
build AppendEntries (and to apply) — saturating one core while 7 sit idle. It is reading from
disk what it just wrote to memory.

This **supersedes the "structural replication wall"** framing and reconciles every prior null:
SyncCore null (the cost is read+copy+CRC *compute*, not scheduling); fsync null (the bottleneck
is the *read* path); pipeline-depth null/hurt (more in-flight = more of these reads); the
replication 2× (1-node has no followers → no replication reads → ~2× throughput); and the
repo's open `future-writerstate-lock-contention` note (replication reads).

**Concrete, UC-side fixes (not structural, not openraft-core):**
1. **In-memory cache of recent log entries** — serve replication/apply reads of just-written
   entries from RAM, not `read()` off ext4. Removes the ~27% read+copy+fault. *Biggest lever.*
2. **Skip CRC re-validation on the leader's own freshly-written entries** — removes much of the
   ~22%.
3. **Zero-copy the entry bytes** into the AppendEntries frame (avoid `copy_page_to_iter`).

Eliminating ~40–50% of that thread's CPU should push the ceiling well past 15k — a real,
attackable throughput win, in UC code.

## Method note

The `profile` hook is now in `bench-infra/ansible/roles/run/tasks/main.yml` (gated
`-e profile=true`, default off): installs perf best-effort, backgrounds `perf record` + `top
-bH` + `ps -T` against the leader during the sweep, stages a symbol report + per-thread peak
CPU. Reusable for any future leader profiling.
