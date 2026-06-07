# Service-State Reconstruction — Phase 2b (async snapshot build) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A snapshot no longer stalls applies. Replace the blocking `build_snapshot(&self, dst)` with a `freeze`/`stream_snapshot` split so the service streams off-lock, AND restructure the node so `build_snapshot` doesn't hold the `inner` lock across the BUILD round-trip.

**Architecture:** Trait gains `type SnapshotHandle: Send` + `freeze(&self) -> Result<(SnapshotHandle, u64)>` (cheap, under a brief lock) + `stream_snapshot(handle, dst)` (consumes, off-lock). `StoreStateMachine`'s handle is `ultima_db::SnapshotReader` (O(1) MVCC pin); ~31 trivial SMs use `Vec<u8>`. The service `snapshot_loop` freezes under a brief read lock then streams via `spawn_blocking`. The node moves the snapshot channel out of `inner` into `Arc<tokio::Mutex<SnapshotChannel>>`; `build_snapshot` does the round-trip under `snapshot_op` (not `inner`), taking `inner` only briefly for the frontier/epoch + persist, with an **epoch-based** race guard (replacing 2a's frontier-based one, since applies now advance the frontier during a build). See spec §6, §6a.

**Tech Stack:** Rust, openraft 0.10, `ultima_db::snapshot_stream`, tokio.

**Spec:** `docs/superpowers/specs/2026-06-06-uc-service-state-reconstruction-design.md` §6, §6a.

**Out of scope:** Phase 3 (RegisterSm revert, node cross-check, lincheck capstone).

**Critical constraint (verified in 2a):** returning `Err` from openraft's `RaftSnapshotBuilder::build_snapshot` is FATAL (→ `Fatal::StorageError` → node shutdown). The race-guard path must therefore return a non-advancing `Ok(Snapshot)` (last-good or empty), never `Err`. Keep this.

---

## File structure

- **Modify** `uc_service/src/state_machine.rs` — the trait (replace `build_snapshot` with `SnapshotHandle`/`freeze`/`stream_snapshot`).
- **Modify** every `impl StateMachine for` (1 real + ~31 trivial — see Task 1 list).
- **Modify** `uc_service/src/runtime/snapshot_loop.rs` — BUILD handler: freeze + `spawn_blocking` stream.
- **Modify** `uc_node/src/raft/state_machine_shmem.rs` — move snapshot channel out of `ShmemInner` into `Arc<tokio::Mutex<SnapshotChannel>>`; restructure `build_snapshot` (inner-brief / snapshot_op / inner-brief, epoch guard); `drive_catchup` INSTALL uses `snapshot_op`; the snapshot helpers take `&mut` halves.
- **Modify** `uc_node/src/runtime/builder.rs` — construct the `SnapshotChannel` + pass to the adapter.
- **Add/Modify** tests — concurrent-build-with-applies; existing snapshot/reattach/lincheck stay green.

---

## Task 1: The trait change + ALL impls (atomic — workspace won't compile until done)

A breaking trait change: the whole workspace fails to build until every `impl StateMachine` is updated. Do it as one unit; the build/test gate is at the end.

**Files:** `uc_service/src/state_machine.rs` + every impl site + the one caller (`snapshot_loop.rs`, updated minimally to compile — full async is Task 2).

- [ ] **Step 1: Change the trait** (`uc_service/src/state_machine.rs`). Replace the `build_snapshot` method:
```rust
    /// A consistent, cheap point-in-time capture of the SM at its current applied
    /// frontier. Returns (handle, snapshot_index). MUST be cheap (O(1) version pin
    /// for an MVCC store; a clone/serialize for trivial SMs) — it is called under a
    /// brief lock. `Send` so the handle can be streamed on a background thread.
    type SnapshotHandle: Send + 'static;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError>;

    /// Stream a frozen handle to `dst`. Consumes the handle; holds NO SM lock.
    fn stream_snapshot(handle: Self::SnapshotHandle, dst: &mut dyn Write)
        -> Result<(), SnapshotError>;
```
Add `type SnapshotHandle` to the associated-types list. Remove `fn build_snapshot(&self, dst)`. Keep `install_snapshot`, `apply`, `query`, `last_applied`. Update the trait doc invariants (build → freeze/stream; freeze returns the index).

- [ ] **Step 2: `StoreStateMachine`** (`uc_service/src/ultima_db/store_state_machine.rs`). Replace its `build_snapshot`:
```rust
    type SnapshotHandle = ultima_db::snapshot_stream::SnapshotReader;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        // Under the caller's brief read lock: pin the current version (O(1) MVCC)
        // and capture its index. snapshot_stream(Some(v)) pins exactly v; latest
        // version is stable here because apply (write lock) can't run concurrently.
        let v = self.store.latest_version();
        let reader = self
            .store
            .snapshot_stream(Some(v))
            .map_err(|e| SnapshotError::Codec(format!("snapshot_stream: {e}")))?;
        Ok((reader, v))
    }

    fn stream_snapshot(mut handle: Self::SnapshotHandle, dst: &mut dyn Write)
        -> Result<(), SnapshotError> {
        std::io::copy(&mut handle, dst)?;
        Ok(())
    }
```
Confirm `ultima_db::snapshot_stream::SnapshotReader` is the public path (the Explore found it; adjust the `use`/path if needed). `SnapshotReader: Read + Send`.

- [ ] **Step 3: The ~31 trivial impls — apply this MECHANICAL RECIPE to each.**
For an SM whose old `build_snapshot(&self, dst)` body serialized state to `dst` and returned an index `i`:
```rust
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let mut buf = Vec::new();
        /* <the OLD build_snapshot body, writing to `&mut buf` instead of `dst`> */
        Ok((buf, /* the index the old body returned, e.g. self.last_applied.unwrap_or(0) */))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
```
For a trivial `Ok(0)` / `Ok(self.last_applied.unwrap_or(0))` body (no bytes written): `freeze` returns `(Vec::new(), <that index>)`; `stream_snapshot` writes the (empty) Vec. Examples:
- `Ok(0)` SMs (`NoopSm`, `StubSm`, `ControllableSM`, `Echo`): `freeze` → `Ok((Vec::new(), 0))`.
- `Ok(self.last_applied.unwrap_or(0))` SMs (`CounterSm` in service.rs, `KvSm`): `freeze` → `Ok((Vec::new(), self.last_applied.unwrap_or(0)))`.
- bincode-serialize `Counter`/`RegisterSm` (m*_*.rs, lincheck): move the `bincode::encode_to_vec(...).map_err(Codec)?` into `freeze`, write into `buf`, return `(buf, self.last_applied.unwrap_or(0))`.
- `reconstruct_snapshot.rs`/`reconstruct_reattach.rs` `CounterSm`: `freeze` writes `sum.to_le_bytes()`+`li.to_le_bytes()` into `buf`, returns `(buf, li)`.

**The full site list (from investigation — update EVERY one):**
`uc_service/src/runtime/service.rs` (NoopSm, CounterSm); `uc_service/src/ultima_db/store_state_machine.rs` (done in Step 2); `uc_node/tests/`: m1_single_node, m2_multi_node, m3_service_crash, m3_three_node_shmem, m3_shutdown_dead_service, m3_ultima_db_adapter (uses StoreStateMachine — no change), m4_client_single_node, m4_client_leader_failover, m4_client_three_node, m4_client_concurrent, m4_client_wrap, m4_client_session_gc, m5_output_smoke, m5_output_idempotent_replay, m5_output_retryable_backoff, m5_output_permanent_advances_marker, m5_output_ring_backpressure_skip, m5_output_leader_transition_replay, m5_output_apply_does_not_stall, drift_detection (ControllableSM), shmem_state_machine (StubSm), reconstruct_reattach (CounterSm), reconstruct_snapshot (CounterSm), lincheck/register_sm.rs (RegisterSm); `uc_autobench/src/bin/`: attribution-bench (Echo), shmem-e2e (Echo), commit-path-load (KvSm), uc-node-launch (KvSm); `uc_autobench/tests/attribution_probes.rs` (Echo); `examples/counter_loop` (uses StoreStateMachine — no change). Use `rg "impl StateMachine for"` + `rg "fn build_snapshot"` to find every one; the build errors will enumerate any missed.

- [ ] **Step 4: Make the one real caller compile** (`uc_service/src/runtime/snapshot_loop.rs` BUILD arm). Minimal change to compile (full async is Task 2) — still under the read lock for now:
```rust
                let (built_index, bytes) = {
                    let guard = sm.read().await;
                    match guard.freeze() {
                        Ok((handle, idx)) => {
                            let mut buf = Vec::new();
                            match S::stream_snapshot(handle, &mut buf) {
                                Ok(()) => (idx, buf),
                                Err(e) => { tracing::error!(error = %e, "snapshot stream failed"); continue; }
                            }
                        }
                        Err(e) => { tracing::error!(error = %e, "snapshot freeze failed"); continue; }
                    }
                };
```
(`S::stream_snapshot` — `S: StateMachine` is the loop's type param.) The node side (`ShmemSnapshotBuilder::build_snapshot`) does NOT call the SM's freeze/stream (it drives the service), and the degenerate node-side `sm: S` only needs the trait to compile — no node-side call-site change in Task 1.

- [ ] **Step 5: Build + full test sweep.** `cargo build --workspace`; fix every impl the compiler flags. Then:
```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p uc_service
cargo test -p uc_node --test reconstruct_snapshot --test reconstruct_reattach -- --test-threads=1
cargo test -p uc_node --test m3_shmem_single_node
```
All green (behavior unchanged — still blocking; the async win comes in Tasks 2-3). Note: `m3_service_crash` is a known flake.

- [ ] **Step 6: Commit**
```bash
git add -A
git commit -m "feat!(uc_service): StateMachine freeze/stream_snapshot trait (replaces build_snapshot)"
```

---

## Task 2: Service `snapshot_loop` — async stream via `spawn_blocking`

**Files:** `uc_service/src/runtime/snapshot_loop.rs`.

Freeze under a brief read lock; release; stream off the runtime worker so applies (separate std::thread, write lock) run during the stream against the frozen MVCC view.

- [ ] **Step 1: Rewrite the BUILD arm**
```rust
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_BUILD_SNAPSHOT => {
                // Freeze under a BRIEF read lock (O(1) for ultima_db), then release
                // before streaming so apply() (write lock) proceeds concurrently.
                let frozen = {
                    let guard = sm.read().await;
                    guard.freeze()
                };
                let (handle, built_index) = match frozen {
                    Ok(f) => f,
                    Err(e) => { tracing::error!(error = %e, "snapshot freeze failed"); continue; }
                };
                // Stream off the tokio worker. handle: Send. Writes the region.
                let region_path2 = region_path.clone();
                let stream_res = tokio::task::spawn_blocking(move || -> Result<(), String> {
                    let mut buf = Vec::new();
                    S::stream_snapshot(handle, &mut buf).map_err(|e| e.to_string())?;
                    uc_protocol::snapshot_region::write(&region_path2, built_index, &buf)
                        .map_err(|e| e.to_string())
                })
                .await;
                match stream_res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => { tracing::error!(error = %e, "snapshot stream/region failed"); continue; }
                    Err(e) => { tracing::error!(error = %e, "snapshot stream task panicked"); continue; }
                }
                publish_resp(&mut resp_producer, MSG_TYPE_SNAPSHOT_BUILT,
                    encode_extra_snapshot_built(built_index), &stop).await;
            }
```
Notes: `spawn_blocking` requires `S::SnapshotHandle: Send + 'static` (trait bound — present) and `S: StateMachine` so `S::stream_snapshot` resolves. The closure captures `handle` (moved) + `region_path2` + `built_index`. The doc-comment about "blocking build under read lock" (the 2a limitation note) should be updated to reflect the new async stream.

- [ ] **Step 2: Build + test.** `cargo build -p uc_service`; `cargo clippy -p uc_service -- -D warnings`; `cargo test -p uc_service`. `cargo test -p uc_node --test reconstruct_snapshot -- --test-threads=1` (the BUILD still produces a valid snapshot; the test settles, so unaffected). Update the module-doc 2a-limitation note (build no longer blocks the service apply loop; the node-side stall is removed in Task 3).

- [ ] **Step 3: Commit**
```bash
git add uc_service/src/runtime/snapshot_loop.rs
git commit -m "feat(uc_service): async snapshot build — freeze + spawn_blocking stream"
```

---

## Task 3: Node — move snapshot channel out of `inner`; non-blocking `build_snapshot` + epoch race guard

**Files:** `uc_node/src/raft/state_machine_shmem.rs`, `uc_node/src/runtime/builder.rs`. **This is the crux — build incrementally.**

- [ ] **Step 1: Define `SnapshotChannel` + move the fields off `inner`**
Add near the adapter:
```rust
/// Snapshot control channel — lives OUTSIDE `inner` so `build_snapshot` can do the
/// BUILD round-trip without holding the apply lock. A dedicated `tokio::Mutex`
/// serializes the only two users (build_snapshot, drive_catchup INSTALL) for
/// SPSC single-producer safety.
pub(crate) struct SnapshotChannel {
    pub(crate) producer: SpscProducer,
    pub(crate) resp_consumer: SpscConsumer,
    pub(crate) resp_bridge: NotifyBridge,
    pub(crate) region_path: std::path::PathBuf,
}
```
Add to `ShmemAdaptedStateMachine`: `pub(crate) snapshot_op: Arc<TokioMutex<SnapshotChannel>>,`. REMOVE `snapshot_producer`, `snapshot_resp_consumer`, `snapshot_resp_bridge`, `snapshot_region_path` from `ShmemInner`. Update `new()` to build the `SnapshotChannel` from the (now `SnapshotChannel`-typed) constructor params and store it as `Arc::new(TokioMutex::new(SnapshotChannel { .. }))`. `ShmemAdaptedStateMachine` derives `Clone` (openraft clones the SM) — confirm `snapshot_op: Arc<..>` clones fine (it does). The `get_snapshot_builder` already clones `self.inner`/`self.shutdown`; also clone `self.snapshot_op` into `ShmemSnapshotBuilder` (add a field).

- [ ] **Step 2: Snapshot helpers take `&mut` halves** (no more `&PlMutex`)
Change `publish_snapshot_cmd(producer: &mut SpscProducer, ...)` and `await_snapshot_resp(consumer: &mut SpscConsumer, ..., bridge: &NotifyBridge, ...)` to take `&mut` (since they're now reached through the `TokioMutex<SnapshotChannel>` guard, not a `PlMutex`). Bodies: drop the inner `producer.lock()` / `consumer.lock()` — call `producer.try_write(...)` / `consumer.try_read(...)` directly on the `&mut`.

- [ ] **Step 3: Restructure `ShmemSnapshotBuilder::build_snapshot`** (the core)
```rust
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        // (1) Brief inner lock: capture the frontier + membership + epoch_before.
        let (frontier, last_membership, epoch_before, ss_ptr) = {
            let g = self.inner.lock().await;
            (g.last_applied, g.last_membership.clone(), epoch_of(g.service_status_ptr), g.service_status_ptr)
        };

        // (2) snapshot_op round-trip — NO inner lock held → apply() runs concurrently.
        let (built_index, bytes) = {
            let mut ch = self.snapshot_op.lock().await;
            ch.resp_consumer.discard_backlog();
            publish_snapshot_cmd(&mut ch.producer, MSG_TYPE_BUILD_SNAPSHOT, [0u8; 8], &self.shutdown).await?;
            let bi = await_snapshot_resp(&mut ch.resp_consumer, MSG_TYPE_SNAPSHOT_BUILT, &self.shutdown, &ch.resp_bridge).await?;
            let (_i, b) = snapshot_region::read(&ch.region_path).map_err(|e| io::Error::other(e.to_string()))?;
            (bi, b)
        };

        // (3) Brief inner lock: EPOCH race guard + persist.
        let mut g = self.inner.lock().await;
        let epoch_after = epoch_of(ss_ptr);
        if epoch_after != epoch_before {
            // A service reattach raced this build — the bytes may be from a fresh
            // (un-reconstructed) incarnation. Refuse: return last-good NON-advancing
            // snapshot (Err is fatal in openraft 0.10), so no purge; openraft retries.
            tracing::warn!(epoch_before, epoch_after, "snapshot build raced a reattach; returning last-good");
            if let Some(s) = &g.current_snapshot {
                return Ok(Snapshot { meta: s.meta.clone(), snapshot: Cursor::new(s.data.clone()) });
            }
            return Ok(Snapshot {
                meta: RaftSnapshotMeta { last_log_id: None, last_membership: Default::default(), snapshot_id: "snap-0".to_string() },
                snapshot: Cursor::new(Vec::new()),
            });
        }
        // Build at the captured frontier (the snapshot represents that applied index;
        // applies that advanced the frontier during the round-trip are covered by the
        // NEXT snapshot — openraft tolerates a snapshot at <= applied).
        let last_log_id = frontier;
        let meta = RaftSnapshotMeta {
            last_log_id,
            last_membership: last_membership.clone(),
            snapshot_id: format!("snap-{}", last_log_id.map(|l| l.index).unwrap_or(0)),
        };
        // Persist to disk (purge-safety) — same block as 2a.
        let bytes_filename = format!("snapshot_{}.bin", last_log_id.map(|l| l.index).unwrap_or(0));
        let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
        std::fs::write(&bytes_path, &bytes).map_err(io::Error::other)?;
        let f = std::fs::File::open(&bytes_path).map_err(io::Error::other)?;
        f.sync_all().map_err(io::Error::other)?;
        drop(f);
        let stored_meta = StoredSnapshotMeta { last_log_id, last_membership, bytes_filename };
        g.snapshot_meta_sv.store(&stored_meta).map_err(io::Error::other)?.wait().map_err(io::Error::other)?;
        g.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: bytes.clone() });
        Ok(Snapshot { meta, snapshot: Cursor::new(bytes) })
    }
```
> **IMPLEMENTER NOTE (lock ordering — get this right):** build holds `inner` and `snapshot_op` in SEPARATE scopes — never nested. Step (1) takes+drops inner; step (2) takes+drops snapshot_op; step (3) takes inner. So build never holds snapshot_op while waiting for inner. `drive_catchup` (Task 4) holds inner and nests snapshot_op under it. Because build releases snapshot_op before re-taking inner, there is no inner↔snapshot_op cycle → no deadlock. Do NOT collapse the scopes. Also: `built_index` is now only informational (frontier may differ legitimately); use `frontier` for the meta, not `built_index`.

- [ ] **Step 4: builder.rs** — construct the `SnapshotChannel` and pass it (instead of the three loose fields). Update the `ShmemAdaptedStateMachine::new(...)` call to pass the channel pieces / a constructed `SnapshotChannel`. Mirror how the loose fields were passed before; the test caller(s) in `shmem_state_machine.rs` update too.

- [ ] **Step 5: Build + clippy + regression**
`cargo build -p uc_node`; `cargo clippy -p uc_node --all-targets -- -D warnings`; `cargo test -p uc_node --test m3_shmem_single_node --test reconstruct_reattach -- --test-threads=1`. (`reconstruct_snapshot` exercised after Task 4 too, but run it here as well.)

- [ ] **Step 6: Commit**
```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs uc_node/tests/shmem_state_machine.rs
git commit -m "feat(uc_node): build_snapshot off the inner lock (snapshot_op channel) + epoch race guard"
```

---

## Task 4: `drive_catchup` INSTALL uses `snapshot_op`

**Files:** `uc_node/src/raft/state_machine_shmem.rs`.

`drive_catchup` previously reached the snapshot ring via `g` (inner). Now the channel is in `snapshot_op`. `drive_catchup` runs inside `apply()` holding `inner` (as `g: &ShmemInner`); it must acquire `snapshot_op` for the INSTALL round-trip (nested under inner — the allowed order).

- [ ] **Step 1: Pass `snapshot_op` into `drive_catchup`**
`drive_catchup` is a free fn `(g: &ShmemInner<S>, shutdown, up_to_log_id)`. Add a `snapshot_op: &TokioMutex<SnapshotChannel>` param. The `apply()` call site has `&self` (the adapter) — pass `&self.snapshot_op`. (Confirm `apply()` can reach `self.snapshot_op` at the `drive_catchup` call — it's an adapter field; `apply` is `&mut self`.)

- [ ] **Step 2: Rewrite the INSTALL round-trip in the NeedsSnapshot arm** to use `snapshot_op` instead of `g.snapshot_*`:
```rust
                // snapshot bytes/index resolved from g.current_snapshot / disk (unchanged)
                ...
                {
                    let mut ch = snapshot_op.lock().await;
                    snapshot_region::write(&ch.region_path, snap_index, &snap_bytes)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    ch.resp_consumer.discard_backlog();
                    publish_snapshot_cmd(&mut ch.producer,
                        uc_protocol::frames::snapshot::MSG_TYPE_INSTALL_SNAPSHOT,
                        uc_protocol::frames::snapshot::encode_extra_install_snapshot(snap_index),
                        shutdown).await?;
                    let installed = await_snapshot_resp(&mut ch.resp_consumer,
                        uc_protocol::frames::snapshot::MSG_TYPE_SNAPSHOT_INSTALLED, shutdown, &ch.resp_bridge).await?;
                    if installed != snap_index {
                        tracing::warn!(snap_index, reported = installed, "install index mismatch");
                    }
                }
                (snap_index, up_to)
```
The region_path now comes from `ch.region_path` (was `g.snapshot_region_path`). The snapshot-source resolution (g.current_snapshot / snapshot_meta_sv / snapshot_bytes_dir) stays on `g` (still in inner). `g` (shared `&ShmemInner`) and `snapshot_op` (separate mutex) are distinct borrows — fine.
> **IMPLEMENTER NOTE:** `drive_catchup` holds `inner` (via `g`) and now nests `snapshot_op` — the allowed lock order (build never holds snapshot_op while awaiting inner; see Task 3 note). Confirm no other code path takes them in the opposite order.

- [ ] **Step 3: Build + clippy + the snapshot reconstruction test**
`cargo build -p uc_node`; `cargo clippy -p uc_node --all-targets -- -D warnings`; `cargo test -p uc_node --test reconstruct_snapshot --test reconstruct_reattach -- --test-threads=1` (both pass — INSTALL still reconstructs to 31; reattach unaffected).

- [ ] **Step 4: Commit**
```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(uc_node): drive_catchup INSTALL via snapshot_op channel (nested under inner)"
```

---

## Task 5: Concurrent-build test + full verification

**Files:** `uc_node/tests/reconstruct_snapshot.rs` (extend) or a new test.

The 2b correctness change worth a dedicated test: a snapshot that fires **while applies continue** (frontier advances during the build, epoch unchanged) must produce a VALID snapshot and NOT be rejected by the epoch guard — i.e. the epoch guard only rejects genuine reattaches, not benign concurrent applies.

- [ ] **Step 1: Add a concurrent-build test**
Mirror `reconstruct_snapshot.rs` setup (small snapshot policy, `max_in_snapshot_log_to_keep = 1`, `CounterSm`). Submit a stream of commands continuously (e.g. spawn a task submitting 1..N) so a snapshot fires mid-stream; DO NOT crash the service (no reattach → epoch stable). After the stream, assert: (a) a snapshot was taken (`_test_last_purged() >= 1`), (b) the final counter == sum(1..N) (snapshot + subsequent applies all intact — the concurrent build neither lost nor double-counted), and (c) NO "raced a reattach" warning was the cause of a missing snapshot. If asserting (c) directly is hard, the (a)+(b) combination (purge happened AND state is exactly correct) demonstrates the concurrent build produced a valid, advancing snapshot.
> If making a snapshot reliably fire *during* in-flight applies is timing-flaky, prefer a slightly larger command stream + small policy; if still not deterministic, document why and rely on (a)+(b) over the whole run (which still proves no corruption from concurrent build). Do NOT ship a flaky test.

- [ ] **Step 2: Full verification**
```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p uc_protocol
cargo test -p uc_service
cargo test -p uc_node --lib runtime::reconstruct
cargo test -p uc_node --test reconstruct_snapshot --test reconstruct_reattach -- --test-threads=1
cargo test -p uc_node --test m3_shmem_single_node
cargo test -p uc_node --test lin_register -- --test-threads=1   # lincheck still green (uses RegisterSm freeze/stream now)
```
All green (`m3_service_crash` known flake — retry).

- [ ] **Step 3: fmt (scoped) + commit**
`cargo fmt`; restore any incidentally-formatted files NOT touched by Phase 2b (mirror Phase 1/2a hygiene); commit:
```bash
git add -A && git commit -m "test(uc_node): concurrent-build snapshot doesn't corrupt; phase 2b fmt" || true
```

---

## Self-review notes (against spec §6/§6a)

- **Trait change** → Task 1 (trait + StoreStateMachine MVCC freeze + ~31 trivial Vec handles + caller).
- **Service async stream** → Task 2 (freeze brief + spawn_blocking).
- **Node off-inner + epoch guard** → Task 3 (SnapshotChannel out of inner, inner-brief/snapshot_op/inner-brief, epoch_before vs epoch_after).
- **INSTALL via snapshot_op** → Task 4.
- **Concurrent-build correctness** → Task 5.
- **Risk centers (flagged):** Task 3 — the lock ordering (build: separate inner/snapshot_op scopes; drive_catchup: inner→snapshot_op nested) — MUST avoid an inner↔snapshot_op cycle; the epoch guard replacing the frontier guard (built_index now informational). Task 1 — the breaking change touches ~31 sites; the build errors enumerate misses (no silent gaps).
- **Constraint preserved:** race-guard returns non-advancing `Ok(Snapshot)`, never `Err` (Err is fatal in openraft 0.10 — verified in 2a).
- **Out of scope:** Phase 3 (RegisterSm revert, cross-check, lincheck capstone) — though lincheck's `RegisterSm` IS updated mechanically for the trait here (it must compile), the self-persistence revert + capstone is Phase 3.
