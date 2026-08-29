# uc2 M14a — the FSM-side hop in isolation (`apply_bench`, dev-box smoke)

**Date:** 2026-08-27. **Tree:** `main` 6111257 (M14a) vs `main` 4fcad3c (pre-M14a) —
harness `uc2_node/examples/apply_bench.rs` built on each. **Box:** the 32-core
dev box; a private `CARGO_TARGET_DIR` per tree.

> **Smoke, not a gate.** Dev-box numbers are never compared to a bar
> (CLAUDE.md "Benchmarking discipline"). What this document records is
> *ratios and ladders* from one hop measured alone — the divide-and-conquer
> method of CLAUDE.md "Finding a performance bottleneck" — because M14a added
> work to this hop (the lag barrier in `apply_cycle`) and nothing before this
> run had measured it.

## What the harness isolates

The service's apply loop, with everything M14a put in it: the per-iteration
`floor()` over the declared slots, `plan()`, the slot writes, the egress
publish. Around it, stand-ins:

- **upstream:** a driver thread appending 64 B frames through
  `uc2_log::Appender` and playing archive + consensus (`durable = commit =
  append`, published every 64 frames), paced so `append − min(applied) ≤
  buffer/2` — the driver can never be the limiter (it spins ≥ 300 M times per
  run waiting on the FSMs);
- **the fake node:** the cnc page (declared set, lag policy, leader flags),
  `log.buf` (64 MiB), the per-id rings; no node process;
- **downstream:** none — the egress broadcast never blocks its producer;
- **the state machine:** raw tier, a counter (no decode, no allocation), so the
  hop's own cost is what remains.

Each FSM's rate is `Δ slot.applied / frame length`; the SM's own frame count
is printed alongside and agrees to within padding-frame rounding (one per
ring wrap: 104 in 110 M), i.e. the frames were applied, not skipped.

## Results

Payload 64 B, frame 96 B, 6 s after a 1 s warm-up, one run each unless noted.

| tree | FSMs | mode | applied frames/s (min over FSMs) | `lag_waits` (per FSM, 6 s) |
|---|---|---|---|---|
| main 4fcad3c | 1 | (single-service tree) | **22 555 581**, 22 737 507 (two runs) | — |
| M14a 6111257 | 1 | bounded | **21 551 241**, 21 500 268 (two runs, alternated with the above) | 0 |
| M14a | 2 | bounded | 21 985 563 | 28 / 0 |
| M14a | 4 | bounded | 21 856 936 | 563–628 |
| M14a | 8 | bounded | 21 724 544 | 330–611 |
| M14a | 2 | bounded, payload 256 B (frame 288 B) | 17 436 149 (= 5.0 GB/s) | — |
| M14a | 2 | **lockstep** | **18 360** | 55 196 / 55 257 |
| M14a | 4 | **lockstep** | **14 246** | 57 155–64 217 |
| M14a + `APPLY_IDLE = Yield` (experiment, reverted) | 2 | lockstep | **616 680** | 1 574 528 / 1 430 205 |
| M14a + `APPLY_IDLE = Yield` (experiment, reverted) | 4 | lockstep | 578 459 | 1.6–1.9 M |
| M14a + `APPLY_IDLE = Yield` (experiment, reverted) | 2 | bounded (control) | 21 530 549 | — |

## Findings

1. **Bounded mode is free of N.** 21.99 M → 21.72 M frames/s from N=1 to
   N=8 (−1.2 %): the per-iteration floor loads over up to eight sibling slots
   and the occasional `Wait` (a few hundred per 6 s at N ≥ 4, from equal-speed
   FSMs drifting to the 16 MiB bound) do not register at this scale on 32
   cores. At 21.9 M frames/s the whole hop costs ~45 ns per frame including
   the egress publish — far above anything the chain delivers end to end
   (the M13 fleet number was 1.1 M resp/s through client, node and edge), so
   the apply hop with a raw SM is **not the limiter** and M14a did not make it
   one.
2. **M14a costs ~5 % at N=1 on this hop** (22.6 M → 21.5 M, both pairs in the
   same direction, run-to-run spread ~1 %). Candidate: the per-batch
   `floor()`/`plan()`/`lag_waiting` work plus the slot-line stores, on batches
   that are only 64 frames long here. Small, but real and now recorded; not
   chased further because (1) says the hop is not the limiter.
3. **Lockstep is throttled by the apply agent's idle strategy, not by the
   barrier.** Predicted from the code before the run: after applying its one
   frame an FSM re-plans, finds a sibling not yet at the same frame, takes
   `Wait`, `break`s, and the agent idles for `APPLY_IDLE = Sleep(50 µs)` —
   so lockstep is bounded near one frame per ~50 µs. Measured: 18 360
   frames/s ≈ 54 µs/frame, and `lag_waits` ≈ one per applied frame. The
   discriminating experiment: with `IdleStrategy::Yield` in place of the
   sleep (a one-line temporary patch, reverted), lockstep rises **33×** to
   617 k/s while bounded is unchanged — the sleep is the cause. The ~1.6 µs
   per frame that remains under `Yield` is lockstep's inherent cross-core
   handshake (each frame needs every FSM to observe every other's `applied`
   store) plus `sched_yield`; a spin-before-yield ladder or a futex on the
   sibling's slot would recover more, a busy-spin most of it.

## The fix, measured (same day)

Three shapes of a wait ladder were tried in `apply_cycle`'s `Wait` arm, each
re-measured with this harness (5–6 s runs; bounded N=1 unpatched control:
21.85 M / 21.85 M):

| shape | lockstep N=2 / 4 / 8 | bounded N=1 | bounded N=8 | verdict |
|---|---|---|---|---|
| v1: 128 spins + 8 yields, inline in the loop, every mode | 569 k / 235 k / 31.6 k (23–55 k sleeps per FSM at N ≥ 4) | 21.5 M | **20.4 M (−6 %)**, 12× the waits | lockstep N ≥ 4 cascades into sleeps; spinning on the slowest FSM's `applied` line in bounded mode slows it |
| v2: lockstep-only, 256 spins + 32 yields, inline | 552 k / 324 k / 374 k | **19.8 M / 19.7 M (−9 %)** | 18.4 M | bounded never executes the arm — the loss is **codegen of the hot loop body** (A/B'd against the unpatched binary back to back, box idle) |
| **v3 (shipped): lockstep-only, out of line (`#[inline(never)] lockstep_wait`), 256 spins + 2 048 yields with a heartbeat refresh every 256, bounded untouched** | **631 k / 583 k / 458 k, 0–1 sleeps** | 21.36 M / 21.59 M (−1.5 %) | 21.16 M (= unpatched 21.14 M) | the lockstep set never sleeps on a live sibling; the hot body is the original plus one call in a cold arm |

Two lessons the ladder taught on the way: (a) a lockstep FSM must **never
sleep on a live sibling** — one 50 µs sleep stalls every other FSM's next
frame, their ladders exhaust, and the set falls into sleeping in lockstep, so
the yield budget must exceed any plausible handshake, not merely the common
one; (b) code in a hot loop's body costs even on paths that never run — the
9 % at N=1 was a layout effect, found only because the control was re-run on
the exact binary. Cost accepted for a *dead* sibling under lockstep: each
survivor yields for ~0.5–2 ms per 50 µs sleep (≈ a core each) while the
cluster is stalled by contract and the alert fires.

## What this changes

- **Lockstep's idle behaviour was a defect, fixed the same day** (`v3` above,
  `uc2_service/src/apply.rs::lockstep_wait`): 18 k → 631 k frames/s at N=2.
  Spec §13 planned to "measure lockstep on the fleet"; the fleet would have
  measured the 50 µs sleep. The remaining ~1.6 µs/frame is the N-way
  cross-core handshake lockstep inherently costs — the fleet row measures
  that, now.
- **The spec's §12 fleet row measures a *slow* FSM's convergence** (bounded
  mode pacing the cluster to the slow FSM's rate); this document shows the
  equal-speed overhead is ~zero at N ≤ 8. A fleet row for equal-speed N=2
  bounded is cheap and turns finding 1 into a gated number rather than a
  smoke one.
- Bounded mode is the default and needs no change.

## Reproduce

```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a          # private, never the shared dir
cargo build -p uc2_node --release --example apply_bench
B=$CARGO_TARGET_DIR/release/examples/apply_bench
for spec in "1 bounded" "2 bounded" "4 bounded" "8 bounded" "2 lockstep" "4 lockstep"; do set -- $spec
  $B --root /home/claude/apply-bench --fsms $1 --mode $2 --secs 6; done
```

The pre-M14a control was the same file with six mechanical edits (singular
ring names, page-1 `service_applied`, no `service_id`) built in a temporary
worktree at 4fcad3c; the `Yield` experiment changed only
`uc2_service/src/lib.rs`'s `APPLY_IDLE` and was reverted before commit.
