# O1 Prototype: Busy-Spin Apply Consumer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the service apply consumer's spin-before-park window configurable, with a sentinel for pure busy-spin, env-gated and default-off — so a commit's apply hop can skip the ~8.8 µs futex park.

**Architecture:** `uc_protocol`'s `SpscConsumer` gains a `spin_budget` field (default `SPIN_TRIES`); `read_or_park` uses it, and on the `u32::MAX` sentinel polls without ever parking. `uc_service`'s apply loop reads `UC_APPLY_SPIN_BUDGET` and sets the budget once before consuming. The wire layer stays env-free; the default path is byte-for-byte unchanged.

**Tech Stack:** Rust; `uc_protocol` (SPSC ring, `libc` futex), `uc_service` (sync apply loop). No new dependencies.

## Global Constraints

- `uc_protocol` is `no_std`-friendly and **env-free** — it exposes the knob; `uc_service` reads the env var.
- Default behavior (env unset) is **byte-for-byte the current `SPIN_TRIES = 64` spin-then-park**. All existing tests must pass without setting the env.
- Env var name: `UC_APPLY_SPIN_BUDGET` (matches the `UC_JOURNAL_PREALLOC` convention). Values: unset → default; `busy`/`max` (case-insensitive) → pure busy-spin; `<N>` → N spins then park; unparseable → default.
- Busy-spin sentinel = `u32::MAX`. Busy mode never calls `FUTEX_WAIT`; it returns `Ok(None)` after a bounded chunk so the caller re-checks its stop flag.
- `cargo clippy --workspace -- -D warnings` must be clean.
- Spec: `docs/superpowers/specs/2026-06-21-o1-busyspin-apply-consumer-design.md`.

---

## File Structure

- `uc_protocol/src/ring/common.rs` — add `BUSY_SPIN_CHUNK` const (Task 1).
- `uc_protocol/src/ring/spsc.rs` — `SpscConsumer.spin_budget` field + `set_spin_budget` + `read_or_park` busy mode + unit tests (Task 1).
- `uc_service/src/runtime/apply_loop.rs` — `parse_spin_budget` + env wiring + log + parser unit tests (Task 2).
- `uc_protocol/examples/apply_spin_consume_bench.rs` — park-vs-busy consume-latency microbench (Task 3).

---

### Task 1: `uc_protocol` — configurable spin budget + busy mode

**Files:**
- Modify: `uc_protocol/src/ring/common.rs` (add `BUSY_SPIN_CHUNK` after `SPIN_TRIES`, ~line 67)
- Modify: `uc_protocol/src/ring/spsc.rs` (struct ~line 100, `into_split` ~line 361, `read_or_park` ~line 213, tests ~line 369)

**Interfaces:**
- Produces: `SpscConsumer::set_spin_budget(&mut self, budget: u32)`; `pub const BUSY_SPIN_CHUNK: u32`. Sentinel `u32::MAX` on `spin_budget` selects busy mode. Default field value = `SPIN_TRIES`.

- [ ] **Step 1: Add the busy-chunk constant**

In `uc_protocol/src/ring/common.rs`, immediately after the `SPIN_TRIES` const (~line 67):

```rust
/// Busy-spin chunk: in busy mode (`spin_budget == u32::MAX`) the consumer polls
/// this many times per `read_or_park` call before returning `Ok(None)`, bounding
/// stop-flag/shutdown latency without ever parking.
pub const BUSY_SPIN_CHUNK: u32 = 256;
```

- [ ] **Step 2: Add `spin_budget` to the struct**

In `uc_protocol/src/ring/spsc.rs`, the `SpscConsumer` struct (~line 100) becomes:

```rust
pub struct SpscConsumer {
    inner: Arc<SpscInner>,
    /// Wakeup mechanism; must match the producer's `mode` (see `SpscProducer::mode`).
    pub mode: ParkMode,
    /// Spin tries before parking on an empty ring. `SPIN_TRIES` by default;
    /// `u32::MAX` = busy-spin sentinel (never park). Set via `set_spin_budget`.
    spin_budget: u32,
}
```

- [ ] **Step 3: Initialize `spin_budget` in `into_split`**

In `into_split` (~line 361), the `SpscConsumer { .. }` literal becomes:

```rust
            SpscConsumer {
                inner: self.inner,
                mode: ParkMode::default(),
                spin_budget: SPIN_TRIES,
            },
```

- [ ] **Step 4: Add the setter and busy-mode `read_or_park`**

Ensure `BUSY_SPIN_CHUNK` is imported: add it to the existing `use` of `SPIN_TRIES`/`PARK_CEIL` from `common` at the top of `spsc.rs` (they are already imported unqualified). Add the setter inside `impl SpscConsumer` (next to `wait_handle`), and replace `read_or_park`:

```rust
    /// Override the spin-before-park budget. `u32::MAX` selects pure busy-spin:
    /// the consumer never parks; it polls in `BUSY_SPIN_CHUNK`-sized bursts and
    /// returns `Ok(None)` between them so the caller can re-check its own stop
    /// condition. Default is `SPIN_TRIES`.
    pub fn set_spin_budget(&mut self, budget: u32) {
        self.spin_budget = budget;
    }

    /// Blocking read: returns a record, or `Ok(None)` after exhausting the spin
    /// budget (then parking up to `PARK_CEIL`, unless in busy mode). Arm-then-
    /// recheck closes the lost-wakeup race on the parking path. SYNC consumers only.
    pub fn read_or_park(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        // Busy mode (sentinel): poll in a bounded chunk, never park. Returning
        // Ok(None) between chunks lets the caller re-check its stop flag, so
        // shutdown stays prompt without ever entering FUTEX_WAIT.
        if self.spin_budget == u32::MAX {
            for _ in 0..BUSY_SPIN_CHUNK {
                if let Some(rec) = self.try_read(payload_buf)? {
                    return Ok(Some(rec));
                }
                std::hint::spin_loop();
            }
            return Ok(None);
        }
        // Spin-then-park: catch an in-flight publish without a syscall first.
        for _ in 0..self.spin_budget {
            if let Some(rec) = self.try_read(payload_buf)? {
                return Ok(Some(rec));
            }
            std::hint::spin_loop();
        }
        let seq = self.inner.header().current_seq();
        self.inner.header().arm();
        let recheck = self.try_read(payload_buf);
        if !matches!(recheck, Ok(None)) {
            self.inner.header().disarm();
            return recheck;
        }
        self.inner.header().park(self.mode, seq, PARK_CEIL);
        self.inner.header().disarm();
        self.try_read(payload_buf)
    }
```

- [ ] **Step 5: Add unit tests**

Append to `uc_protocol/src/ring/spsc.rs`'s `mod tests`:

```rust
    #[test]
    fn busy_mode_empty_returns_without_parking() {
        use std::time::Instant;
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (_producer, mut consumer) = ring.into_split();
        consumer.set_spin_budget(u32::MAX);
        let mut buf = Vec::new();
        let start = Instant::now();
        let rec = consumer.read_or_park(&mut buf).expect("read");
        let elapsed = start.elapsed();
        assert!(rec.is_none());
        // Busy mode must NOT have parked up to PARK_CEIL (2ms); a chunk is sub-ms.
        assert!(elapsed < PARK_CEIL, "busy mode appears to have parked: {elapsed:?}");
    }

    #[test]
    fn busy_mode_reads_published_record() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (mut producer, mut consumer) = ring.into_split();
        consumer.set_spin_budget(u32::MAX);
        producer.try_write(7, 0, [0; 8], b"hi").expect("write");
        let mut buf = Vec::new();
        let rec = consumer.read_or_park(&mut buf).expect("read").expect("some");
        assert_eq!(rec.msg_type, 7);
        assert_eq!(&buf[..], b"hi");
    }

    #[test]
    fn finite_budget_reads_and_returns_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (mut producer, mut consumer) = ring.into_split();
        consumer.set_spin_budget(4); // small finite window, then park
        let mut buf = Vec::new();
        // empty -> parks up to PARK_CEIL, returns None
        assert!(consumer.read_or_park(&mut buf).expect("read").is_none());
        // published -> read returns it
        producer.try_write(9, 0, [0; 8], b"x").expect("write");
        let rec = consumer.read_or_park(&mut buf).expect("read").expect("some");
        assert_eq!(rec.msg_type, 9);
    }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p uc_protocol ring::spsc`
Expected: PASS, including the 3 new tests and all pre-existing spsc tests.

- [ ] **Step 7: Commit**

```bash
git add uc_protocol/src/ring/common.rs uc_protocol/src/ring/spsc.rs
git commit -m "feat(uc_protocol): configurable SpscConsumer spin budget + busy-spin sentinel"
```

---

### Task 2: `uc_service` — `UC_APPLY_SPIN_BUDGET` wiring

**Files:**
- Modify: `uc_service/src/runtime/apply_loop.rs` (imports; `spawn_apply_loop` ~line 39; new `parse_spin_budget`/`apply_spin_budget`; tests)

**Interfaces:**
- Consumes: `SpscConsumer::set_spin_budget` (Task 1); `uc_protocol::ring::SPIN_TRIES`.
- Produces: `fn parse_spin_budget(v: Option<&str>) -> u32` (pure, testable).

- [ ] **Step 1: Add the pure parser + env reader**

In `uc_service/src/runtime/apply_loop.rs`, add near the top (after imports) — and add `use uc_protocol::ring::SPIN_TRIES;` to the imports:

```rust
/// Parse the `UC_APPLY_SPIN_BUDGET` value into a spin budget for the apply
/// consumer. Pure (testable): `None`/unparseable -> default `SPIN_TRIES`;
/// `busy`/`max` (case-insensitive) -> `u32::MAX` (pure busy-spin); `<N>` -> N.
fn parse_spin_budget(v: Option<&str>) -> u32 {
    match v {
        Some(s) if s.trim().eq_ignore_ascii_case("busy") || s.trim().eq_ignore_ascii_case("max") => {
            u32::MAX
        }
        Some(s) => s.trim().parse::<u32>().unwrap_or(SPIN_TRIES),
        None => SPIN_TRIES,
    }
}

fn apply_spin_budget() -> u32 {
    parse_spin_budget(std::env::var("UC_APPLY_SPIN_BUDGET").ok().as_deref())
}
```

- [ ] **Step 2: Configure the consumer in `spawn_apply_loop`**

In `spawn_apply_loop` (~line 39), after `let stop = ...;` and before the `std::thread::Builder` spawn, set the budget on the (already `mut`) `consumer`:

```rust
    let budget = apply_spin_budget();
    consumer.set_spin_budget(budget);
    if budget == u32::MAX {
        tracing::info!("apply consumer: busy-spin mode (UC_APPLY_SPIN_BUDGET=busy)");
    } else if budget != SPIN_TRIES {
        tracing::info!(spin_budget = budget, "apply consumer: custom spin budget");
    }
```

- [ ] **Step 3: Add parser unit tests**

Add (or extend) a `#[cfg(test)] mod tests` at the bottom of `apply_loop.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spin_budget_parsing() {
        assert_eq!(parse_spin_budget(None), SPIN_TRIES);
        assert_eq!(parse_spin_budget(Some("busy")), u32::MAX);
        assert_eq!(parse_spin_budget(Some("BUSY")), u32::MAX);
        assert_eq!(parse_spin_budget(Some("max")), u32::MAX);
        assert_eq!(parse_spin_budget(Some(" 128 ")), 128);
        assert_eq!(parse_spin_budget(Some("garbage")), SPIN_TRIES);
        assert_eq!(parse_spin_budget(Some("0")), 0);
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_service apply_loop`
Expected: PASS (the new `spin_budget_parsing` test; existing apply-loop tests unaffected).

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/runtime/apply_loop.rs
git commit -m "feat(uc_service): UC_APPLY_SPIN_BUDGET wiring for the apply consumer"
```

---

### Task 3: Direction microbench (park vs busy) over a real SPSC ring

**Files:**
- Create: `uc_protocol/examples/apply_spin_consume_bench.rs`

**Interfaces:**
- Consumes: `SpscRing`, `SpscConsumer::set_spin_budget` (Task 1). No dev-deps (examples can't see `tempfile`).

- [ ] **Step 1: Write the microbench**

```rust
//! Direction microbench for O1 (`docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`):
//! consume-wakeup latency of the SPSC apply consumer in **park** mode (default
//! SPIN_TRIES then FUTEX_WAIT) vs **busy** mode (`set_spin_budget(u32::MAX)`),
//! over a *real* `SpscRing` (Futex ParkMode on Linux — the actual code path).
//!
//! Ping-pong: main writes a token on `fwd`, a responder thread consumes it via
//! `read_or_park` (the mode under test) and writes an ack on `back`; main
//! busy-reads the ack (so only the responder's consume mechanism varies). One
//! round trip ~= one fwd publish->consume wakeup + one ack.
//!
//! Run: `cargo run -p uc_protocol --release --example apply_spin_consume_bench`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uc_protocol::ring::SpscRing;

const ROUND_TRIPS: u32 = 50_000;

fn tmp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("uc-spin-bench-{}-{tag}.ring", std::process::id()))
}

fn run(busy: bool) -> Duration {
    let fwd_path = tmp_path(if busy { "busy-fwd" } else { "park-fwd" });
    let back_path = tmp_path(if busy { "busy-back" } else { "park-back" });
    let fwd = SpscRing::create(&fwd_path, 4096, 1024).expect("fwd");
    let back = SpscRing::create(&back_path, 4096, 1024).expect("back");
    let (mut fwd_tx, mut fwd_rx) = fwd.into_split();
    let (mut back_tx, mut back_rx) = back.into_split();
    if busy {
        fwd_rx.set_spin_budget(u32::MAX);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let s2 = stop.clone();

    let responder = thread::Builder::new()
        .name("responder".into())
        .spawn(move || {
            let mut buf = Vec::new();
            while !s2.load(Ordering::Relaxed) {
                if let Ok(Some(_)) = fwd_rx.read_or_park(&mut buf) {
                    while back_tx.try_write(1, 0, [0; 8], b"a").is_err() {
                        std::hint::spin_loop();
                    }
                }
            }
        })
        .unwrap();

    let mut buf = Vec::new();
    // warm up
    for _ in 0..1000 {
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() { std::hint::spin_loop(); }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) { break; }
            std::hint::spin_loop();
        }
    }

    let start = Instant::now();
    for _ in 0..ROUND_TRIPS {
        while fwd_tx.try_write(1, 0, [0; 8], b"a").is_err() { std::hint::spin_loop(); }
        loop {
            if let Ok(Some(_)) = back_rx.try_read(&mut buf) { break; }
            std::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    // nudge the responder out of a park so it observes stop
    let _ = fwd_tx.try_write(1, 0, [0; 8], b"a");
    responder.join().unwrap();
    let _ = std::fs::remove_file(&fwd_path);
    let _ = std::fs::remove_file(&back_path);
    elapsed
}

fn report(name: &str, d: Duration) {
    let rt = d.as_nanos() as f64 / ROUND_TRIPS as f64;
    println!("{name:<22} {rt:>9.0} ns/round-trip   ({ROUND_TRIPS} round-trips)");
}

fn main() {
    println!("== apply-consumer consume latency: park vs busy (release) ==");
    let park = run(false);
    let busy = run(true);
    report("park (SPIN_TRIES)", park);
    report("busy (u32::MAX)", busy);
    let d = (park.as_nanos() as f64 - busy.as_nanos() as f64) / ROUND_TRIPS as f64;
    println!("\nbusy-mode saves ~{d:.0} ns/round-trip on the consume wakeup");
}
```

- [ ] **Step 2: Build and run**

Run: `cargo build -p uc_protocol --release --example apply_spin_consume_bench`
Expected: compiles clean.

Then run (binaries land under `$CARGO_TARGET_DIR` — here `/home/claude/.cache/cargo-target/release/examples/`):

Run: `cargo run -p uc_protocol --release --example apply_spin_consume_bench`
Expected: two lines; `busy` round-trip materially below `park` (park pays a futex wakeup ~µs; busy is spin ~ns).

- [ ] **Step 3: Clippy + commit**

Run: `cargo clippy -p uc_protocol --release --examples 2>&1 | grep -E 'warning|error'` (expect none)

```bash
git add uc_protocol/examples/apply_spin_consume_bench.rs
git commit -m "bench(uc_protocol): park-vs-busy apply-consumer consume-latency microbench"
```

---

## Self-Review

**Spec coverage:**
- Configurable spin budget + sentinel busy mode → Task 1. ✓
- `uc_protocol` env-free; `uc_service` reads env → Task 1 (knob) + Task 2 (env). ✓
- `UC_APPLY_SPIN_BUDGET` naming + values → Task 2 `parse_spin_budget` + tests. ✓
- Default byte-for-byte unchanged → field defaults to `SPIN_TRIES`, env unset → `SPIN_TRIES`; Task 1 Step 6 / Task 2 Step 4 run existing tests. ✓
- Correctness tests (finite reads+parks; busy reads + no hang) → Task 1 Step 5. ✓
- Direction microbench on the real ring → Task 3. ✓
- Fleet-only throughput claim NOT made locally → microbench reports consume latency only (Task 3 doc comment). ✓
- Clippy clean → Task 3 Step 3 (+ run `cargo clippy --workspace` before finishing). ✓

**Placeholder scan:** none — all code is concrete. Task 3 Step 2 flags one possible closure-binding tweak with the exact fallback (inline the closure), which is a real compile contingency, not a placeholder. ✓

**Type consistency:** `set_spin_budget(&mut self, u32)`, `spin_budget: u32`, sentinel `u32::MAX`, `BUSY_SPIN_CHUNK: u32`, `SPIN_TRIES` (imported) — consistent across Tasks 1/2/3. `parse_spin_budget(Option<&str>) -> u32` used by `apply_spin_budget`. ✓
