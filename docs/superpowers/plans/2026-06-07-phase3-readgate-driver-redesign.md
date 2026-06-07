# Phase 3 read-gate — driver redesign (avoids the extra-adapter-clone freeze)

**Why:** The first read-gate (query_link holding an adapter clone, driving `drive_catchup` from the query path) reproducibly froze the 3-node cluster. Isolated cause: a **3rd live adapter clone** (2 = fine, 3 = freeze; proven with `mem::forget(adapter.clone())`). `drive_catchup`'s `await_apply_resp` for the final replayed entry never wakes. Mechanism not fully root-caused; user chose to redesign around it.

**Key enabling fact:** today there are exactly 2 live adapter clones: openraft's (active) + `NodeHandle.sm = SmAdapter::Shmem(adapter)` which in shmem mode is used ONLY for `signal_shutdown()` (just sets a shared `Arc<AtomicBool>`). So we can hold *only the flag* in NodeHandle and repurpose the 2nd clone for a reconstruction driver → **net stays 2 clones**.

## Design (net 2 adapter clones; gate holds none)

- **Reconcile driver task** (owns clone #2): loops `select! { request.notified(), sleep(50ms backstop) }`; on wake calls `adapter.ensure_reconciled()` (rebuilds the service to the node frontier via `drive_catchup` if the service-epoch changed), which updates `reconciled_epoch` and fires `reconcile_done`. Stop flag + JoinHandle, joined in shutdown.
- **Read gate** (`ReconcileGate`, holds only `reconciled_epoch: Arc<AtomicU64>`, `done: Arc<Notify>`, `request: Arc<Notify>`, `service_status_ptr` — NO adapter clone): `wait_until_reconciled()` loops: register `done.notified()` BEFORE the check (no lost wakeup); if `epoch_of(ptr) == reconciled_epoch` return; else `request.notify_one()` (poke driver) and `select!{ done_fut, sleep(25ms backstop) }`. Called at the top of `ShmemQueryLink::submit` (covers BOTH node.submit_query Snapshot reads and the client dispatcher Linearizable reads — single chokepoint).
- **apply() lazy path** also updates `reconciled_epoch` + fires `reconcile_done` after its reattach `drive_catchup` (keeps both drivers consistent; reads waiting are woken whether a write or the driver reconciled).
- Driver + apply() both call `ensure_reconciled`/`drive_catchup`, **serialized by `inner`**; `ensure_reconciled` double-checks `reconciled_epoch` under the lock so no double catch-up. The Task-1 `service_not_ahead` cross-check stays as-is (never fires wrongly: apply() only calls drive_catchup when epoch≠last_seen, at which point service_last≤up_to).

## Edits

1. `state_machine_shmem.rs`
   - imports: `AtomicU64`; `tokio::sync::Notify`.
   - adapter fields: `reconciled_epoch: Arc<AtomicU64>`, `reconcile_done: Arc<Notify>`, `service_status_ptr: Option<ServiceStatusPtr>`. Clone + new() init (`reconciled_epoch = last_seen_epoch`, `reconcile_done = Notify::new()`).
   - `apply()` both reattach branches: after `g.last_seen_epoch = epoch;` add `self.reconciled_epoch.store(epoch, Release); self.reconcile_done.notify_waiters();`.
   - `ensure_reconciled(&self) -> io::Result<()>`: fast path (epoch==reconciled); slow path: lock inner, double-check, `drive_catchup(&g, &self.snapshot_op, &self.shutdown, frontier)` (or set reconciled=cur if `g.last_applied` is None), set `g.last_seen_epoch`, `reconciled_epoch`, `reconcile_done.notify_waiters()`.
   - `pub(crate) struct ReconcileGate { reconciled_epoch, done, request, service_status_ptr }` + `Clone` + `async fn wait_until_reconciled(&self)` (the loop above) + `GATE_BACKSTOP=25ms`.
   - `pub(crate) fn reconcile_gate(&self, request: Arc<Notify>) -> ReconcileGate`.
   - `pub(crate) fn spawn_reconcile_driver<S>(adapter, request: Arc<Notify>, stop: Arc<AtomicBool>) -> JoinHandle<()>` (or a handle struct) — the driver loop; `BACKSTOP=50ms`.
2. `query_link.rs`: `gate: Option<ReconcileGate>` field; `with_gate(producer, consumer, gate)`; `new` delegates with None; at top of `submit`: `if let Some(g)=&self.gate { g.wait_until_reconciled().await; }`.
3. `node.rs`: `SmAdapter::Shmem(Arc<AtomicBool>)` (the shutdown flag) instead of the adapter; line ~367 `if let SmAdapter::Shmem(flag) = &sm { flag.store(true, Release); }`. Add `NodeHandle.reconcile_driver: Option<ReconcileDriverHandle>`; join in shutdown (after signalling shutdown flag, before dropping cnc tasks). `submit_query` Shmem arm unchanged (uses query_link, gate inside).
4. `builder.rs` Shmem arm: `let driver_adapter = adapter.clone();` (#2) BEFORE finish; `let request = Arc::new(Notify::new());` `let gate = adapter.reconcile_gate(request.clone());` `let shutdown_flag = adapter.shutdown.clone();` `query_link = with_gate(.., Some(gate))`; `handle_sm = SmAdapter::Shmem(shutdown_flag)`; after finish, spawn driver with `driver_adapter, request, <a stop flag>`; store handle. Join in NodeHandle.shutdown.

## Tests
- `reconstruct_reattach` (single-node, incl. the new read-after-restart-without-write test) — pass.
- `reconstruct_snapshot` — pass.
- `lin_register fault_roundtrip_keeps_serving` (3-node) — must PASS (was the freeze repro).
- capstone `linearizable_under_failover` across seeds (4359,1,88888,7,42) — Linearizable.
- drift_detection, m3 suites — green.
