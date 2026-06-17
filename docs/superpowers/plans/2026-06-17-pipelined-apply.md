# Pipelined Apply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift UC's ~5790/s 3-node throughput ceiling by pipelining apply across the node↔service shmem boundary — publish a run of committed entries, then await their responses — instead of one serial round-trip per entry. No wire-protocol change.

**Architecture:** Restructure `ShmemAdaptedStateMachine::apply()` (`uc_node/src/raft/state_machine_shmem.rs`) to collect the committed batch, then process it as fast-path runs (contiguous `Normal` entries with no epoch-change/gap: publish all → await all in FIFO order → per-entry bookkeeping) bounded by an in-flight cap `C`, falling back to the existing per-entry path for `Blank`/`Membership`/catch-up entries (which flush the in-flight run first). The single-entry `publish_apply`/`await_apply_resp` helpers and the service `apply_loop` are reused unchanged.

**Tech Stack:** Rust, openraft `RaftStateMachine` trait, tokio, parking_lot, the shmem SPSC apply/apply_resp rings. Verified by the lincheck capstone + hard-crash multi-process test (the apply path's existing correctness gates).

**Spec:** `docs/superpowers/specs/2026-06-17-pipelined-apply-design.md`

**Verification regime (read first):** `apply()` is the SMR core; its correctness is proven by the **lincheck capstone** (`uc_node/tests/lin_register.rs::linearizable_under_failover`, node-kill + service-crash + churn) and the **hard-crash** multi-process test. Those are the real tests at each task — TDD here is capstone-driven. Build + `clippy` gate every task; the capstones gate the behavioral tasks. The implementer reconciles borrow-checking against the compiler; the *structure and invariants* below are the contract.

**Key facts (confirmed from source):**
- `publish_apply(&PlMutex<SpscProducer>, log_index, cmd_bytes, log_id, &AtomicBool) -> io::Result<()>` — publishes one `ApplyFrame`, retries on `Full`, locks the producer internally (not the `inner` lock).
- `await_apply_resp(&PlMutex<SpscConsumer>, expected_log_index, log_id, &AtomicBool, &NotifyBridge, Option<ServiceStatusPtr>, expected_epoch) -> io::Result<ApplyOutcome>` — awaits ONE resp, **asserts FIFO log_index order**, returns `ApplyOutcome::Resp(Bytes)` or `ApplyOutcome::Reattach` (service epoch changed = crash/reattach).
- Per-entry bookkeeping after a resp (today, `state_machine_shmem.rs` ~lines 588–607): `output_chan_tx.try_send((log_index, cmd_bytes.clone().into()))`; if `filled_to_here || prev_caught + 1 == log_index` then `service_caught_up_to.store(log_index, Release)` + `reconcile_done.notify_waiters()`; `g.last_applied = Some(log_id)`; after dropping `g`, `responder.send(resp_bytes)`.
- `inner` lock (`g`) guards `apply_producer`/`apply_resp_consumer`/`apply_resp_bridge`/`last_applied`/`last_seen_epoch`/`service_status_ptr`/`output_chan_tx`; `service_caught_up_to` is a separate `Arc<AtomicU64>`.

---

## Task 1: Add the `apply_pipeline_depth` config knob (bounds in-flight `C`)

**Files:**
- Modify: `uc_node/src/config.rs` (the `ServiceRingConfig` struct + its `Default`)
- Modify: `uc_node/src/raft/state_machine_shmem.rs` (thread the value into `ShmemInner` or read it where `apply()` runs)

- [ ] **Step 1: Read the current `ServiceRingConfig`**

Run: `grep -nE "struct ServiceRingConfig|apply_cap|output_cap|impl Default for ServiceRingConfig|max_msg" uc_node/src/config.rs`
Expected: shows the apply-ring capacity fields. Note the apply ring's frame capacity (slot count) — `C` must not exceed it.

- [ ] **Step 2: Add the knob with a safe default**

In `uc_node/src/config.rs`, add to `ServiceRingConfig`:
```rust
    /// Max apply entries published before awaiting responses (pipeline depth).
    /// Bounds in-flight apply frames so the apply/apply_resp rings never overflow.
    /// Must be <= the apply ring's frame capacity. Default 256.
    pub apply_pipeline_depth: usize,
```
and in its `Default` impl add:
```rust
            apply_pipeline_depth: 256,
```

- [ ] **Step 3: Verify it builds**

Run: `cargo build -p uc_node 2>&1 | tail -5`
Expected: compiles (any other `ServiceRingConfig { .. }` literals without `..Default::default()` will error — fix them to include the field or spread the default; check `grep -rn "ServiceRingConfig {" uc_node uc_autobench examples`).

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/config.rs
git commit -m "feat(uc_node): apply_pipeline_depth config knob (bounds apply pipeline in-flight; default 256)"
```

---

## Task 2: Restructure `apply()` into pipelined fast-path + per-entry slow-path

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs` (the `apply()` method, ~lines 501–610)

This is the core, correctness-critical change. The new control flow:

```text
collect the committed batch into items: Vec<(Entry, Option<Responder>)>   // stream yields the batch then ends
i = 0
while i < items.len():
    lock g
    epoch = g.last_seen_epoch; ss_ptr = g.service_status_ptr
    # gather a fast-path run: consecutive Normal entries, no epoch change, no gap, len <= apply_pipeline_depth
    run = []
    while i+len(run) < items.len()
          and items[i+len(run)] is Normal
          and epoch_of(ss_ptr) == epoch
          and service_caught_up_to.load()+1 >= items[i+len(run)].log_index   # contiguous
          and len(run) < apply_pipeline_depth:
        run.push(items[i+len(run)])
    if run not empty:
        # PUBLISH PHASE
        for e in run: publish_apply(&g.apply_producer, e.log_index, e.cmd_bytes, e.log_id, &shutdown).await?
        # AWAIT PHASE (FIFO order)
        reattached_at = None
        for (k, e) in run.enumerate():
            match await_apply_resp(&g.apply_resp_consumer, e.log_index, e.log_id, &shutdown,
                                   &g.apply_resp_bridge, ss_ptr, epoch).await?:
                Resp(b) =>
                    # per-entry bookkeeping (preserve today's block):
                    g.output_chan_tx.try_send((e.log_index, e.cmd_bytes.clone().into()))   # warn on full
                    service_caught_up_to.store(e.log_index, Release); reconcile_done.notify_waiters()
                    g.last_applied = Some(e.log_id)
                    stash (e.responder, b) to send after dropping g
                Reattach => { reattached_at = Some(k); break }   # service crashed mid-run
        i += (reattached_at or run.len())   # advance past confirmed entries only
        drop g
        send all stashed responders
        continue   # if reattached_at is Some, the next loop iteration re-processes from the reattach entry → slow path
    else:
        # SLOW PATH: items[i] is Blank / Membership / needs-catch-up. Handle EXACTLY as today
        # (drive_catchup / membership store / blank), per-entry, then i += 1.
        ... existing per-entry body for items[i], dropping g and sending its responder ...
        i += 1
```

- [ ] **Step 1: Run the lincheck capstone on the CURRENT code to confirm green baseline**

Run: `cargo test -p uc_node --test lin_register linearizable_under_failover --release -- --test-threads=1 2>&1 | tail -5`
Expected: `test result: ok. 1 passed`. (Baseline — the refactor must keep this green.)

- [ ] **Step 2: Implement the restructured `apply()`**

Rewrite the `apply()` body to the flow above. Concrete requirements (the contract — keep these exact):
- **Collect first:** `let mut items = Vec::new(); while let Some(item) = entries.next().await { items.push(item?); }` then process `items` by index. (openraft yields the committed batch then ends the stream.)
- **Fast-path run** = maximal slice of consecutive `EntryPayload::Normal` entries where, re-checked per candidate: `epoch_of(ss_ptr) == g.last_seen_epoch` AND `service_caught_up_to.load(Acquire) + 1 >= log_index` (contiguous — true within a committed batch) AND run length `< apply_pipeline_depth`.
- **Publish phase then await phase**, reusing `publish_apply` / `await_apply_resp` unchanged (single-entry, called in loops).
- **`Reattach` mid-run:** stop the await loop at index `k`; advance `i` only past the `k` confirmed entries; the reattach entry (and the rest) are re-processed on the next outer iteration, where the gap/epoch check routes them to the **slow path** (`drive_catchup`), exactly as today. Do **not** await resps for entries after the reattach point (the crashed service will never answer).
- **Per-entry frontier:** `service_caught_up_to` advances only after each entry's confirmed `Resp` — never for the whole run up front. `g.last_applied` set per confirmed entry.
- **Responders:** fulfilled exactly once per entry, after `g` is dropped (collect `(responder, resp_bytes)` during the await phase, send after `drop(g)`).
- **Slow path** (`Blank`, `Membership`, or epoch-change/gap): byte-for-byte the existing per-entry logic (including `drive_catchup`, membership store, the `filled_to_here`/`prev_caught` frontier rule, `output_chan` handoff). Flushing happens naturally because the fast-path run ends before a non-fast entry.
- **Shutdown:** unchanged — `publish_apply`/`await_apply_resp` already poll `shutdown` and abort.
- `Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>` is `Unpin + Send`; `cmd_bytes` (a `Bytes`) is cheap to clone for the `output_chan` handoff and to hold across the await phase.

- [ ] **Step 3: Build + clippy**

Run: `cargo build -p uc_node 2>&1 | tail -5 && cargo clippy -p uc_node -- -D warnings 2>&1 | tail -5`
Expected: compiles, zero clippy warnings.

- [ ] **Step 4: Lincheck capstone across 3 seeds (the correctness gate)**

Run:
```bash
for s in 1 4359 99999; do echo "seed=$s"; LIN_SEED=$s cargo test -p uc_node --test lin_register linearizable_under_failover --release -- --test-threads=1 2>&1 | grep -E "test result|seed=|FAILED|panicked"; done
```
Expected: `test result: ok. 1 passed` for every seed. Any failure = the refactor broke linearizability or crash-recovery → fix before proceeding (do NOT continue with a red capstone).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "perf(uc_node): pipeline apply (publish run, await run) — amortize per-entry shmem round-trip; per-entry frontier + Reattach flush preserved; lincheck green"
```

---

## Task 3: Hard-crash multi-process test (mid-pipeline service kill)

**Files:**
- Test (existing): `examples/uc-crashtest/tests/hard_crash.rs`

- [ ] **Step 1: Run the hard-crash test (SIGKILL service mid-load)**

Run: `cargo test -p uc-crashtest --features hard-crash-tests hard_crash 2>&1 | tail -15`
Expected: passes — `kill -9` of the service mid-apply, then reconstruct, stays **Linearizable** across its seeds. This exercises mid-pipeline crash recovery (published-but-unconfirmed entries re-applied via catch-up). If it fails, the `Reattach`-mid-run handling or per-entry frontier advance is wrong — fix in `apply()` (Task 2), re-run Task 2 Step 4 + this.

- [ ] **Step 2: Commit (no code change expected; this is a gate)**

If the test required a fix, it lands in `state_machine_shmem.rs` under Task 2's commit scope. If green as-is, no commit needed — record the result in the PR/notes.

---

## Task 4: Focused batched-apply integration test

**Files:**
- Modify: `uc_node/tests/m3_three_node_shmem.rs` (add a test) — or create `uc_node/tests/apply_pipeline.rs` if a standalone in-process shmem fixture is simpler. Read `m3_three_node_shmem.rs` first to reuse its node+service fixture.

- [ ] **Step 1: Read the existing shmem test fixture**

Run: `sed -n '1,60p' uc_node/tests/m3_three_node_shmem.rs; grep -nE "fn |submit|client_write|read|assert" uc_node/tests/m3_three_node_shmem.rs | head -30`
Expected: shows how a node+service is stood up and how submits/reads are issued. Reuse that harness.

- [ ] **Step 2: Add a batched-apply throughput-shape test**

Add a test that, on a single shmem node+service, submits a **burst of N=500 writes concurrently** (so openraft commits them in batches and `apply()` pipelines them), then reads back and asserts: all N applied (final state reflects all writes), responses correct, and `last_applied` advanced to the last index. Include at least one membership change interleaved (e.g., via the existing membership helper) to exercise the fast-path→slow-path→fast-path boundary. Model the submit/read calls on the existing test's helpers. Assert linearizable outcome via the existing `uc-lincheck` helpers if the fixture already uses them, else assert final-state correctness directly.

- [ ] **Step 3: Run the new test**

Run: `cargo test -p uc_node --test <file> <test_name> --release -- --nocapture 2>&1 | tail -10`
Expected: PASS — N writes applied in order, final state correct, membership boundary handled.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/<file>
git commit -m "test(uc_node): batched-apply burst test (N concurrent writes + interleaved membership) exercises the apply pipeline + flush boundary"
```

---

## Task 5: Full gate sweep + clippy

**Files:** none (verification only)

- [ ] **Step 1: Run the workspace-relevant suites**

Run:
```bash
cargo clippy --workspace -- -D warnings 2>&1 | tail -5
cargo test -p uc_node --test m1_single_node --release 2>&1 | grep "test result"
cargo test -p uc_node --test m2_multi_node --release -- --test-threads=1 2>&1 | grep "test result"
cargo test -p uc_node --test m3_three_node_shmem --release -- --test-threads=1 2>&1 | grep "test result"
cargo test -p uc_node --features fault-injection --test lin_partition --release -- --test-threads=1 2>&1 | grep "test result"
cargo test -p uc_autobench --test ring_torture 2>&1 | grep "test result"
```
Expected: every suite `ok`, zero clippy warnings. (These confirm the refactor didn't regress single/multi-node, partition, or ring semantics.)

- [ ] **Step 2: Commit any fixes**

If a suite needed a fix, commit it with a message naming the suite. If all green, nothing to commit — proceed.

---

## Task 6: Throughput validation on a cloud fleet (operator — needs fleet + spend)

**This needs a UC-only Hetzner fleet (`make up-uc`) + `HCLOUD_TOKEN` + spend. The `uc-throughput` loop's mutable paths already include `uc_node/src/raft/`.**

- [ ] **Step 1: Bring up the fleet + baseline**

Run: `cd bench-infra && make up-uc && make iterate`
Expected: a fitness JSON. With this change merged, baseline should already reflect the pipelined apply.

- [ ] **Step 2: A/B the pipeline depth (optional tuning)**

Edit `apply_pipeline_depth` (e.g. 64 / 256 / 1024 — must stay ≤ apply ring frame capacity) and run `bash uc_autobench/scripts/uc-throughput-iter.sh` per value; keep the best by `uc_throughput_msgs`, lincheck-gated. Confirm the ceiling lifts beyond ~5790/s.

- [ ] **Step 3: Record + destroy**

Append the winning row to `uc_autobench/tasks/uc-throughput/results.tsv`, document the new ceiling in `docs/tasks/task13_aeron_vs_uc_commit_path.md` (a §14 follow-up), then `cd bench-infra && make destroy`.

---

## Self-review notes (for the executor)

- **Task 2 is the high-risk core.** Never proceed past it with a red lincheck capstone — that gate is the proof the SMR semantics survived. Run it across multiple seeds (Step 4).
- The single-entry `publish_apply`/`await_apply_resp` helpers are **reused unchanged** — only `apply()`'s control flow changes. Don't modify the helpers or the service `apply_loop`.
- `Reattach` mid-run is the subtle case: advance `i` only past *confirmed* entries, never await post-reattach resps, and let the next outer iteration route the unconfirmed tail to `drive_catchup` (slow path). The hard-crash test (Task 3) is the gate for this.
- `apply_pipeline_depth` must stay ≤ the apply ring's frame capacity (Task 1 Step 1) so the publish phase can't overflow the ring; `publish_apply` retries on `Full` defensively but the bound should prevent it.
- No `uc_protocol` change (no frame-layout / version bump) and no `uc_service/apply_loop.rs` change — the service already pipelines by draining per frame.
