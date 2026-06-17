# Design — pipelined apply across the node↔service shmem boundary

**Date:** 2026-06-17
**Status:** Design approved; pending spec review → implementation plan.
**Topic:** Remove the serial per-entry apply round-trip that caps UC 3-node
throughput at ~5790/s, by pipelining the apply across the node↔service shmem
boundary (publish-all-then-await-all), with no wire-protocol change.

## 1. Motivation

After the consensus-side wins (concurrent `client_write` dispatch + `api_batch_linger`,
task13 §13: ~28× to ~5790/s), the next ceiling is the **apply pipeline**
(task13 profile / §6 territory). `uc_node`'s `RaftStateMachine::apply` drains the
committed batch openraft hands it **one entry at a time**: for each entry it
`publish_apply()`s a single `ApplyFrame` to `service/apply.ring` and **awaits** the
matching `ApplyRespFrame` before advancing. Each entry therefore pays a full
cross-process wakeup round-trip (node → futex-wake service apply thread → `sm.apply`
→ resp → futex-wake node), ≈173 µs/entry → ≈5790/s. `sm.apply` (sub-µs) and the
shmem ring transport (~15 ns) are not the cost — the **per-entry wakeup round-trip**
is.

## 2. Goals / Non-goals

**Goals**
- Lift the apply ceiling by amortizing the cross-process wakeup round-trip from
  per-entry (N) to per-run (~1), while preserving every apply-path invariant.
- **No wire-protocol change** (no `uc_protocol` frame-layout / version bump).
- Keep the catch-up / reconcile / epoch / gap logic byte-for-byte as today.
- Stay linearizable under node-kill + service-crash + churn (lincheck + hard-crash).

**Non-goals**
- Not a batched multi-entry frame (rejected: marginal gain over pipelining for a
  protocol change; ring ops are already ~15 ns).
- Not changing `sm.apply` (still sync, deterministic, per-entry).
- Not touching the consensus/submit path (already optimized, task13 §13).
- Not changing durability or read-gate semantics.

## 3. Architecture — pipelined fast path, per-entry slow path

Restructure the loop in `uc_node/src/raft/state_machine_shmem.rs::apply()`. Today it
is `while entry { lock; publish_one; await_one; bookkeeping }`. New shape:

**Fast path** — a maximal run of contiguous `Normal` entries with **no epoch change
and no prefix gap** (the steady-state common case):
1. Accumulate the run (collect `(entry, responder, cmd_bytes, log_id)` up to the
   in-flight bound `C`).
2. **Publish** all their `ApplyFrame`s to `apply.ring`.
3. **Await** their `ApplyRespFrame`s in log order (SPSC is FIFO → responses arrive in
   publish order; assert each resp's `log_index` matches the expected entry).
4. The service `apply_loop` is **unchanged** — it already drains+applies+responds per
   frame, so it wakes ~once per run, applies the run, responds the run.

**Bounded in-flight (`C`)** — chunk the run so at most `C` frames are outstanding,
where `C ≤ frame-capacity of both apply.ring and apply_resp.ring` (publish C → await
C → repeat). Default `C` = ring frame capacity; optional `apply_pipeline_depth` knob.
This prevents ring overflow / deadlock and still amortizes the wakeup over `C` entries.

**Slow path (rare, unchanged semantics)** — when the next entry needs catch-up
(service epoch changed = reattach, or `service_caught_up_to + 1 < log_index` = gap),
or is `Blank` / `Membership` (no `sm.apply`): **flush the current in-flight run**
(await its outstanding resps), then handle that entry exactly as today
(`drive_catchup` / snapshot install / membership store / blank), then resume batching.

**Per-entry bookkeeping preserved** — for each entry, after its confirmed resp:
fulfill openraft's per-entry `responder`, `output_chan.try_send((log_index, cmd))`,
advance `service_caught_up_to` to `log_index`, set `last_applied`. Only the transport
batches; semantics stay per-entry and in-order.

## 4. Correctness invariants

- **Order / determinism:** apply in strict log order via SPSC FIFO; resps consumed in
  the same order; per-resp `log_index` debug-asserted against the expected entry.
- **Per-entry frontier (the linchpin):** `service_caught_up_to` advances only after
  *that entry's* confirmed resp — never optimistically for the whole chunk.
- **Idempotent mid-chunk crash recovery:** if the service dies after N frames are
  published but before all resps return, `await_apply_resp` detects it (epoch / seqlock
  change → `ApplyOutcome::Reattach`); the run is flushed and `drive_catchup` replays
  `(service_caught_up_to, log_id]` on the fresh service — re-applying exactly the
  unconfirmed tail (deterministic SMR replay, idempotent). Because the frontier only
  advanced on confirmed resps, nothing is lost or double-applied.
- **Shutdown:** the await loop keeps polling the shared `shutdown` flag (as today) so a
  service crash can't wedge `node.shutdown()` mid-chunk.
- **Lock:** `inner` is held across a chunk's publish+await (vs per-entry today). Safe —
  apply is the sole writer, the service is a separate process; a linearizable read
  waits at most one chunk longer (reads already gate on apply).

## 5. Testing (gates, strongest first)

1. **lincheck capstone** (`uc_node/tests/lin_register.rs::linearizable_under_failover`)
   — node+service under node-kill + service-crash + churn; directly exercises
   pipelined apply and mid-chunk service-crash → reattach. Primary gate; green across
   seeds.
2. **Hard-crash multi-process** (`examples/uc-crashtest`, `kill -9` service mid-apply)
   — mid-pipeline crash recovery end-to-end; stays linearizable.
3. **Existing m1/m2/m3 + partition capstones** — pass unchanged.
4. **New focused test** — drive `apply()` with a batch of N `Normal` entries
   (in-process shmem fixture), assert all N applied in log order with correct
   responses and `service_caught_up_to`/`last_applied` advanced; include one
   interleaved `Blank`/`Membership` to verify the flush-and-resume boundary.
5. **`ring_torture`** — confirm the chunk bound `C` never exceeds apply/apply_resp ring
   frame capacity (no overflow).
6. **Throughput validation** — the `uc-throughput` cloud loop (apply path is already in
   its mutable paths), lincheck-gated; confirm the ceiling lifts beyond ~5790/s.

## 6. Files touched

- `uc_node/src/raft/state_machine_shmem.rs` — restructure `apply()` into
  fast-path-pipeline + slow-path-flush; the `publish_apply` / `await_apply_resp`
  helpers stay single-entry (called in publish-phase / await-phase loops). Optional
  `apply_pipeline_depth` plumbed from config (default = ring frame capacity).
- `uc_node/tests/` — the new focused batched-apply test.
- No `uc_protocol`, no `uc_service/apply_loop.rs` changes (the service loop already
  pipelines by draining per frame).

## 7. Out of scope / future

- Batched multi-entry frame + single-lock-acquisition apply (only if pipelining proves
  insufficient).
- Apply-path changes to `sm.apply` signature (stays per-entry sync).
- The next ceiling after this (likely the single RaftCore loop or QUIC replication) —
  separate investigation.
