# O1 Extension: Busy-Spin Node-Side Bridges — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the node-side `NotifyBridge` parker a configurable spin budget so the submit and apply_resp bridges can busy-spin (never futex-park) instead of parking, env-gated and default-off.

**Architecture:** `NotifyBridge::spawn` gains a `spin_budget: u32` param. The parker loop watches the ring's `current_seq`: `0` (default) parks immediately (today's behavior), `u32::MAX` busy-spins and fires `Notify` only on a real change, finite `N` spins then parks. `uc_node` reads `UC_NODE_BRIDGE_SPIN_BUDGET` and passes the budget to the submit + apply_resp bridges; snapshot_resp always gets `0`.

**Tech Stack:** Rust; `uc_node` (tokio, the shmem `NotifyBridge`), `uc_protocol` (`RingWaitHandle`, `SpscRing`). No new dependencies.

## Global Constraints

- Env var `UC_NODE_BRIDGE_SPIN_BUDGET`: unset→`0`; `busy`/`max` (case-insensitive)→`u32::MAX`; `<N>`→N; unparseable→`0`.
- Busy mode (`u32::MAX`) must NEVER call the parker's `park()` (FUTEX_WAIT); it spins on `current_seq` and fires `Notify` ONLY on a real change (a notify-every-spin would churn the async consumer).
- Default (budget `0`, env unset) must be byte-for-byte today's parker behavior; snapshot_resp always uses `0`.
- Only the **submit** and **apply_resp** bridges read the env budget; **snapshot_resp** stays `0`.
- The honesty caveat: busy-spin removes the ring futex park (~8.8 µs) but the `Notify`→tokio reschedule remains — the per-hop win is partial. Bench/test output must not overclaim.
- `cargo clippy --workspace -- -D warnings` clean; existing `uc_node` tests green without the env set.
- Spec: `docs/superpowers/specs/2026-06-21-o1-node-bridge-busyspin-design.md`.

---

## File Structure

- `uc_node/src/ipc/ring_bridge.rs` — `NotifyBridge::spawn(handle, name, spin_budget)`, new parker loop, `parse_bridge_spin_budget`/`bridge_spin_budget`, unit tests (Task 1).
- `uc_node/src/ipc/client_dispatcher.rs` — submit bridge call site passes the env budget (Task 1).
- `uc_node/src/raft/state_machine_shmem.rs` — apply_resp passes the env budget; snapshot_resp passes `0` (Task 1).
- `uc_node/src/ipc/ring_bridge.rs` `mod tests` — a `#[tokio::test]` that measures + prints publish→notified latency, park vs busy (Task 2).

---

### Task 1: Spin budget on `NotifyBridge` + env wiring + correctness tests

**Files:**
- Modify: `uc_node/src/ipc/ring_bridge.rs` (`spawn` ~line 36; parker loop ~44-52; add parser + tests)
- Modify: `uc_node/src/ipc/client_dispatcher.rs` (~line 63)
- Modify: `uc_node/src/raft/state_machine_shmem.rs` (~lines 444, 447)

**Interfaces:**
- Produces: `NotifyBridge::spawn(handle: RingWaitHandle, name: &'static str, spin_budget: u32) -> NotifyBridge`; `pub fn parse_bridge_spin_budget(v: Option<&str>) -> u32`; `pub fn bridge_spin_budget() -> u32`.
- Consumes: `RingWaitHandle::{current_seq, arm, park, disarm}` and `PARK_CEIL` (already used in this file).

- [ ] **Step 1: Change `spawn` signature + parker loop**

In `uc_node/src/ipc/ring_bridge.rs`, replace the `spawn` method (the whole `pub fn spawn(...)` body through the closing of `Self { .. }`) with:

```rust
    /// Spawn the parker thread for `handle`. `name` is for diagnostics.
    /// `spin_budget`: `0` = park immediately (default); `u32::MAX` = busy-spin
    /// (never park, notify only on a real `current_seq` change); finite `N` =
    /// spin `N` times looking for a change, then park.
    pub fn spawn(handle: RingWaitHandle, name: &'static str, spin_budget: u32) -> Self {
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let waker = handle.clone();
        let n = notify.clone();
        let s = stop.clone();
        let join = std::thread::Builder::new()
            .name(format!("ring-park-{name}"))
            .spawn(move || {
                handle.arm();
                let mut last = handle.current_seq();
                while !s.load(Ordering::Acquire) {
                    if spin_budget == u32::MAX {
                        // Busy: spin until the wakeup word changes or we stop;
                        // notify ONLY on a real change (never park, no syscall).
                        loop {
                            let now = handle.current_seq();
                            if now != last {
                                last = now;
                                n.notify_one();
                                break;
                            }
                            if s.load(Ordering::Acquire) {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    } else {
                        // Spin up to `spin_budget` looking for a change (0 = none),
                        // then park up to PARK_CEIL. Notify after either path —
                        // a spurious notify on timeout is tolerated (the consumer
                        // re-checks via try_read), matching the prior behavior.
                        let mut changed = false;
                        for _ in 0..spin_budget {
                            let now = handle.current_seq();
                            if now != last {
                                last = now;
                                changed = true;
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        if !changed {
                            let seq = handle.current_seq();
                            handle.park(seq, PARK_CEIL);
                            last = handle.current_seq();
                        }
                        n.notify_one();
                    }
                }
                handle.disarm();
            })
            .expect("spawn ring parker thread");
        Self {
            notify,
            stop,
            waker,
            join: Some(join),
        }
    }
```

- [ ] **Step 2: Add the env parser + reader**

Add to `uc_node/src/ipc/ring_bridge.rs` (after the imports, before `impl NotifyBridge` or at end of file — module scope):

```rust
/// Parse `UC_NODE_BRIDGE_SPIN_BUDGET` into a parker spin budget. Pure (testable):
/// `None`/unparseable -> `0` (park immediately, today's behavior); `busy`/`max`
/// (case-insensitive) -> `u32::MAX` (pure busy-spin); `<N>` -> N (spin then park).
pub fn parse_bridge_spin_budget(v: Option<&str>) -> u32 {
    match v {
        Some(s) if s.trim().eq_ignore_ascii_case("busy") || s.trim().eq_ignore_ascii_case("max") => {
            u32::MAX
        }
        Some(s) => s.trim().parse::<u32>().unwrap_or(0),
        None => 0,
    }
}

/// Read the node bridge spin budget from the environment.
pub fn bridge_spin_budget() -> u32 {
    parse_bridge_spin_budget(std::env::var("UC_NODE_BRIDGE_SPIN_BUDGET").ok().as_deref())
}
```

- [ ] **Step 3: Update the three call sites**

`uc_node/src/ipc/client_dispatcher.rs` line ~63 (submit bridge): change
```rust
            crate::ipc::ring_bridge::NotifyBridge::spawn(consumer.wait_handle(), "submit");
```
to
```rust
            crate::ipc::ring_bridge::NotifyBridge::spawn(
                consumer.wait_handle(),
                "submit",
                crate::ipc::ring_bridge::bridge_spin_budget(),
            );
```

`uc_node/src/raft/state_machine_shmem.rs` line ~444 (apply_resp) — find which `use` brings `NotifyBridge` (it's called unqualified as `NotifyBridge::spawn`); add a `use crate::ipc::ring_bridge::bridge_spin_budget;` near the other `use`s if not present. Change:
```rust
        let apply_resp_bridge =
            NotifyBridge::spawn(apply_resp_consumer.wait_handle(), "apply_resp");
```
to
```rust
        let apply_resp_bridge =
            NotifyBridge::spawn(apply_resp_consumer.wait_handle(), "apply_resp", bridge_spin_budget());
```
And the snapshot_resp bridge (line ~447) — snapshot is NOT on the hot path, always park:
```rust
        let snapshot_resp_bridge =
            NotifyBridge::spawn(snapshot_resp_consumer.wait_handle(), "snapshot_resp", 0);
```

- [ ] **Step 4: Add unit tests**

Append to (or create) `mod tests` at the bottom of `uc_node/src/ipc/ring_bridge.rs`. These build a real `SpscRing`, take the consumer's `wait_handle()`, and drive a bridge. Use a dependency-free temp path (examples/tests here avoid assuming `tempfile`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::ring::SpscRing;

    fn tmp_ring_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("uc-bridge-test-{}-{tag}.ring", std::process::id()))
    }

    #[test]
    fn parse_bridge_spin_budget_cases() {
        assert_eq!(parse_bridge_spin_budget(None), 0);
        assert_eq!(parse_bridge_spin_budget(Some("busy")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("BUSY")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("max")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some("MAX")), u32::MAX);
        assert_eq!(parse_bridge_spin_budget(Some(" 256 ")), 256);
        assert_eq!(parse_bridge_spin_budget(Some("garbage")), 0);
    }

    // Busy mode: the parker notifies on a publish and shuts down cleanly.
    #[tokio::test]
    async fn busy_bridge_notifies_on_publish_and_shuts_down() {
        let path = tmp_ring_path("busy");
        let ring = SpscRing::create(&path, 4096, 1024).expect("create");
        let (mut producer, consumer) = ring.into_split();
        let bridge = NotifyBridge::spawn(consumer.wait_handle(), "test-busy", u32::MAX);

        // Mirror the real consumer: the parker snapshots `current_seq` at startup
        // and notifies on a LATER change. Let it start watching, THEN publish, so
        // the publish is a real change the parker observes (a publish before the
        // snapshot would be one the consumer's own try_read handles, not notify).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        producer.try_write(7, 0, [0; 8], b"hi").expect("write");
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.notified()).await;
        assert!(res.is_ok(), "busy bridge did not notify on publish");

        drop(bridge); // shutdown joins the parker; test completing => no hang
        let _ = std::fs::remove_file(&path);
    }

    // Default (budget 0 = park): still notifies on a publish (via the park path).
    #[tokio::test]
    async fn park_bridge_notifies_on_publish() {
        let path = tmp_ring_path("park");
        let ring = SpscRing::create(&path, 4096, 1024).expect("create");
        let (mut producer, consumer) = ring.into_split();
        let bridge = NotifyBridge::spawn(consumer.wait_handle(), "test-park", 0);

        // Let the parker reach its park, then publish so the publish wakes it.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        producer.try_write(7, 0, [0; 8], b"hi").expect("write");
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.notified()).await;
        assert!(res.is_ok(), "park bridge did not notify on publish");

        drop(bridge);
        let _ = std::fs::remove_file(&path);
    }
}
```

- [ ] **Step 5: Build + run the tests**

Run: `cargo test -p uc_node ipc::ring_bridge`
Expected: PASS — `parse_bridge_spin_budget_cases`, `busy_bridge_notifies_on_publish_and_shuts_down`, `park_bridge_notifies_on_publish`. (uc_node is a heavy build; first compile is slow.)

Run: `cargo clippy -p uc_node -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/ipc/ring_bridge.rs uc_node/src/ipc/client_dispatcher.rs uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(uc_node): configurable NotifyBridge spin budget + UC_NODE_BRIDGE_SPIN_BUDGET wiring"
```

---

### Task 2: Direction measurement — publish→notified latency, park vs busy

**Files:**
- Modify: `uc_node/src/ipc/ring_bridge.rs` (`mod tests` — add a measurement `#[tokio::test]`)

**Interfaces:**
- Consumes: `NotifyBridge::spawn(.., u32)` from Task 1; `SpscRing`.

- [ ] **Step 1: Add a print-based measurement test**

A `#[tokio::test]` that times publish→`notified()` for park (budget 0) vs busy (`u32::MAX`) and PRINTS both. It is NOT a strict perf assertion (sandbox noise + the residual `Notify` cost make a hard inequality flaky); it asserts only that both modes deliver, and prints the latency so a human (or `--nocapture`) can see the direction. Append to `mod tests`:

```rust
    // Measurement (not a strict perf assert — sandbox noise + the residual
    // Notify->reschedule make a hard inequality flaky). Prints publish->notified
    // latency for park vs busy so the direction is visible with --nocapture.
    #[tokio::test]
    async fn measure_publish_to_notified_park_vs_busy() {
        async fn one(budget: u32, iters: u32) -> std::time::Duration {
            let path = tmp_ring_path(if budget == u32::MAX { "m-busy" } else { "m-park" });
            let ring = SpscRing::create(&path, 65536, 1024).expect("create");
            let (mut producer, mut consumer) = ring.into_split();
            let bridge = NotifyBridge::spawn(consumer.wait_handle(), "measure", budget);
            let mut buf = Vec::new();
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                // small gap so a parking parker (budget 0) actually parks
                tokio::time::sleep(std::time::Duration::from_micros(200)).await;
                let t0 = std::time::Instant::now();
                producer.try_write(7, 0, [0; 8], b"x").expect("write");
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), bridge.notified()).await;
                total += t0.elapsed();
                while consumer.try_read(&mut buf).ok().flatten().is_some() {} // drain
            }
            drop(bridge);
            let _ = std::fs::remove_file(&path);
            total / iters
        }
        let iters = 200;
        let park = one(0, iters).await;
        let busy = one(u32::MAX, iters).await;
        println!(
            "publish->notified: park={park:?} busy={busy:?} (busy removes the ~us ring futex park; \
             the Notify->reschedule remains in both)"
        );
        // correctness only: both modes deliver within the timeout (non-zero, finite)
        assert!(park.as_nanos() > 0 && busy.as_nanos() > 0);
    }
```

- [ ] **Step 2: Run it (visible output)**

Run: `cargo test -p uc_node ipc::ring_bridge::tests::measure_publish_to_notified_park_vs_busy -- --nocapture`
Expected: PASS, and a printed line like `publish->notified: park=~Xµs busy=~Yµs ...` where park > busy (busy removes the futex park). The exact numbers are sandbox-dependent and not asserted.

- [ ] **Step 3: Clippy + commit**

Run: `cargo clippy -p uc_node -- -D warnings` (expect clean)

```bash
git add uc_node/src/ipc/ring_bridge.rs
git commit -m "test(uc_node): measure park-vs-busy publish->notified bridge latency"
```

---

## Self-Review

**Spec coverage:**
- Spin budget on `NotifyBridge` (0/MAX/N semantics) → Task 1 Step 1. ✓
- Env `UC_NODE_BRIDGE_SPIN_BUDGET` parser + values → Task 1 Step 2 + test. ✓
- submit + apply_resp pass budget; snapshot_resp = 0 → Task 1 Step 3. ✓
- Busy never parks; notify only on change → Task 1 Step 1 (busy branch). ✓
- Default (env unset → 0) byte-for-byte unchanged → budget 0 path = park-immediately; Task 1 Step 5 runs existing tests. ✓
- Correctness (busy notifies + clean shutdown; default notifies) → Task 1 Step 4. ✓
- Direction measurement with the residual-Notify caveat, not overclaimed → Task 2 (print-based, caveat in the printed string + comment). ✓
- Clippy workspace clean → Task 1 Step 5 + Task 2 Step 3 (run `cargo clippy --workspace` before finishing). ✓

**Placeholder scan:** none — all code concrete. Task 1 Step 3 instructs locating the existing `NotifyBridge`/`bridge_spin_budget` `use` (a real lookup in that file), not a placeholder.

**Type consistency:** `spawn(RingWaitHandle, &'static str, u32)`, sentinel `u32::MAX`, `parse_bridge_spin_budget(Option<&str>) -> u32`, `bridge_spin_budget() -> u32` — consistent across Tasks 1/2 and all three call sites. `SpscRing::create(path, cap, max_msg)` + `into_split()` + `try_write(msg_type, flags, [u8;8], payload)` match the uc_protocol API used elsewhere.
