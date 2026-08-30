# M13b — Engine-shaped remote client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `uc_remote`'s single-lock `RemoteClient` with an `Engine`-shaped split client (`RemoteEngine::connect -> (RemoteSendHalf, RemotePollHalf)`) whose per-connection hot path is two threads, a handful of atomics, batched writes and buffered reads — lifting one connection from ~50 k resp/s (dev box, `dummy-edge`) towards the blaster's proven ~3.6 M/s — while preserving every failover behaviour the current client owns.

**Architecture:** Per connection: the caller's **submitter** thread encodes a frame straight into a preallocated SPSC **outgoing byte ring** and records `(seq -> user_data, kind, deadline, ring extent)` in a slot table (no syscall, no lock); a **writer** thread drains the ring with one `write_all_bytes` per drain (flush-on-empty, no timer) and owns the socket for dial/redial/resend; a **reader** thread does `read_frame_buffered`/`next_buffered`, updates `credits`/`acked_seq` atomics, resolves slots and pushes completions into a bounded SPSC **completion queue** with a byte arena, waking the poller **once per read batch**. `poll` drains that queue. The only lock on any per-request path is none; a reconnect mutex is taken on socket error and on dial, and the rare RETRY/PONG paths take small cold mutexes.

**Tech Stack:** Rust 2024 (workspace edition), `std` only (`TcpStream`, `Mutex`/`Condvar` for cold paths, `UnsafeCell` + atomics for the two SPSC rings), plus the crate's existing `bytes` and `thiserror`. No `tokio`, no `uc_client` dependency, no new crates.

**Spec:** docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md (this plan implements §3, §6, and the caller migrations; §4 ring and §5 edge budget are other tracks)

## Global Constraints

- MSRV is **1.89** (`rust-version` in the root `Cargo.toml`); local dev and CI build on the pin in `rust-toolchain.toml` (**1.96.0**). Nothing here may need a newer stable.
- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean at every commit.
- **No `tokio`, no async.** `uc_remote` stays dependency-light: `bytes` + `thiserror` only, `serde`/`bincode` in dev-dependencies. It **must not** depend on `uc_client` or `uc_protocol` — the `SlotTable` and the wait cell are *copied and adapted*, with attribution in the module doc, exactly the way `uc_client/src/wait.rs` was ported from `ultima_rings`.
- **No scratch under `/tmp`** — it is RAM-backed tmpfs with no swap on this box. Test artifacts go to `CARGO_TARGET_TMPDIR` or under `/home/claude`.
- **Remote wire protocol v1 is unchanged** (`frame::PROTOCOL_VERSION == 1`): no new frame types, no changed layouts, no changed flag bits. `uc_remote::frame`'s public decoders keep their signatures — `fuzz/fuzz_targets/uc_remote_frame.rs` and `fuzz/src/seeds.rs` import `uc_remote::frame::*` and must keep compiling untouched.
- **Commit after every task**, with the exact `git add`/`git commit` given in the task's last step. Work on branch `uc2/m13b-remote-client` off `main`.
- Every new module starts with the repo's two-line header:
  `// SPDX-License-Identifier: Apache-2.0` then `// Copyright 2026 Peter Knego`.

## Execution errata (2026-08-25, found during subagent-driven execution)

- **Tasks 2, 3 (and 4 if it shares a buffer between threads): never form a
  reference over a whole shared buffer.** The code as written in Task 2
  Step 3 and Task 3 (`UnsafeCell<Box<[u8]>>` / `UnsafeCell<Box<[Record]>>`
  with `let buf = unsafe { &mut *self.x.get() }; buf[idx..]`) goes through
  `Box`'s `DerefMut`, which is a Unique retag over the **entire** allocation
  and invalidates any slice the other thread is holding — even for disjoint
  byte ranges. Confirmed under Miri (Stacked Borrows) during the Task 2
  review. Implement instead with a raw base pointer (`Box<[UnsafeCell<u8>]>`
  or a cached `*mut u8`), `ptr::copy_nonoverlapping` for writes and
  `slice::from_raw_parts` over exactly the accessed range for reads; state
  in each SAFETY comment that no whole-buffer reference is ever formed. Add
  a two-thread test per structure and run it under
  `cargo +nightly miri test -p uc_remote --lib <module>::`.
- Cursor invariants that SAFETY comments rely on (`ack <= send <= write`
  and the like) get `debug_assert!`s at every mutation site; a safe fn whose
  correctness depends on a caller precondition asserts it.
- The branch is `uc2/m13-remote-path` (one branch for tracks A/B/C), not
  `uc2/m13b-remote-client`.
- Task 10 also migrates the `query(&[u8], bool)` call sites so the workspace
  compiles at its commit; Task 5 keeps the old client compiling by pointing
  it at the moved config/stats types.

---

## Provenance: what is being replaced, and every behaviour that must survive

`uc_remote/src/client.rs` (1573 lines) is deleted at Task 11. Before that, each of its behaviours is re-implemented on the halves. This is the checklist; each row names the current code and the task that re-homes it.

| # | Behaviour | Current location (`uc_remote/src/client.rs`) | Re-homed in |
|---|---|---|---|
| 1 | Config validation by name (`app_id`, `members`, `max_inflight`, `dead_after > ping_interval`) | `RemoteConfig::validate`, **204–229** | Task 5 |
| 2 | Dial scan: preferred addr, then round-robin from `member_idx + 1` | `dial`, **1355–1478** | Task 5 |
| 3 | `HELLO`/`HELLO_OK` handshake, `app_id` check, per-attempt `connect_timeout` for connect **and** for the reply | `dial_one`, **1490–1553** | Task 5 |
| 4 | `HELLO_OK` naming another leader is hopped to **before** the connection is adopted; `fallback` connection kept | `dial`, **1428–1444**, `1387` | Task 5 |
| 5 | `REDIRECT` at the handshake, bounded by `MAX_REDIRECT_HOPS = 8` | `dial`, **1446–1452** | Task 5 |
| 6 | `HELLO_REFUSED{FAULTED,BUSY}` costs one member (`refused_members`); `{APP_ID,VERSION}` is terminal | `dial`, **1459–1469**; `on_frame`, **988–1013** | Task 5 / Task 9 |
| 7 | Credit rule `seq <= acked_seq + credits`, `credits` absolute, `acked_seq` monotone (`max`) | `enqueue`, **572–586**; `credit_update`, **810–820** | Task 7 |
| 8 | Local `max_inflight` cap on unanswered requests | `enqueue`, **576** | Task 6 |
| 9 | Batched write: many frames, one `write_all_bytes`, flush-on-empty, no timer | `pump`, **689–756** | Task 2 / Task 6 |
| 10 | **Probe-before-flush** on an unproven connection: exactly one frame, then wait for a `RESPONSE` or a `STATUS` whose `acked_seq` covers the probe | `pump`, **663–685, 729–736**; `on_frame`, **869–884** | Task 8 |
| 11 | `RETRY{SERVICE_UNAVAILABLE,INSTANCE_RESTART}`: honour `retry_after_us` as a `not_before` deadline, jittered, `[MIN_RETRY_SLEEP, MAX_RETRY_SLEEP]`, re-sent **in place** | `on_frame`, **888–926**; `jittered`, **1180–1184** | Task 8 |
| 12 | **Not-serving latch**: `RETRY{NOT_SERVING}` never re-sends on the same socket — reconnect, preferring the known leader | `on_frame`, **901–918** | Task 8 |
| 13 | `RETRY{PAYLOAD_TOO_LARGE}` is terminal, never re-sent | `on_frame`, **890–896** | Task 8 |
| 14 | `UNKNOWN`: re-send when `resend_on_unknown`, else surface `Unknown` | `on_frame`, **927–938** | Task 9 |
| 15 | `REDIRECT`: empty addr = reconnect anywhere; self-addr = reconnect to the same addr after `SELF_REDIRECT_BACKOFF` (10 ms), never re-send in place; else reconnect to the target | `on_frame`, **939–971** | Task 8 |
| 16 | `LEADER_CHANGED`: update `leader`; empty addr = reconnect; same addr = keep the connection | `on_frame`, **972–987** | Task 8 |
| 17 | Reconnect + **ordered resend of the unanswered window**, `proven`/`probe_seq` reset, `credits` from `HELLO_OK`, `acked_seq` carried across | `reconnect`, **1076–1175** | Task 8 |
| 18 | Reconnect backoff 5 ms → 500 ms, slept in `SWEEP_INTERVAL` slices with a sweep between them | `sleep_sweeping`, **793–806**; `reconnect`, **1165–1172** | Task 9 |
| 19 | `request_timeout` sweep: every tick, **and between every dial attempt and every redirect hop** | `sweep`, **765–783**; `dial`, **1396–1404**; `tick`, **1226–1255** | Task 9 |
| 20 | `PING` when nothing written for `ping_interval`; `PONG` answered on receipt | `tick`, **1236–1253**; `on_frame`, **1014–1031** | Task 9 |
| 21 | `dead_after`: nothing **received** for that long = fail over; doubles as the mid-frame stall bound passed to `read_frame_buffered` | `reader_loop`, **1269, 1324–1334** | Task 9 |
| 22 | One admission wake per **read batch**, never per frame | `reader_loop`, **1282–1298** | Task 6 |
| 23 | `FLAG_REPLAYED` / `FLAG_EXPIRED` mapping; `ResponseMeta.position` | `on_frame`, **851–874** | Task 6 / Task 7 |
| 24 | `RemoteStats` counters incl. `max_credits_seen` and `refused_members` | `Stats`, **340–351**; `stats`, **519–533** | Tasks 5–9 |
| 25 | `shutdown` fails every outstanding request with `Closed`; `Drop` does the same | `close`, **540–560**; `Drop`, **623–627** | Task 9 / Task 10 |
| 26 | Every accepted request ends in **exactly one** outcome | module doc, **4–47** | Tasks 6–10 |

**Test-suite note (honest discrepancy):** the spec says "28 scripted scenarios"; `uc_remote/tests/client_fake_edge.rs` actually contains **27** `#[test]` functions. The plan ports all 27 and says so; nothing is missing, the spec's count is off by one.

---

## File Structure

**Created in `uc_remote/src/`:**

| File | Responsibility |
|---|---|
| `park.rs` | `WaitCell`: a seq-stamped park/wake pair (`Mutex<()>` + `Condvar` + `AtomicU64` seq + `AtomicU32` waiters). Used by the writer (outgoing ring non-empty), the poller (completions available) and the reader (completion queue drained). Never on a per-request path when both sides are busy. |
| `outgoing.rs` | `OutRing`: the SPSC byte ring the submitter encodes into and the writer drains. Owns `write`/`send`/`ack` cursors, `peek_upto`/`consume`/`copy_range`, and the writer's wake cell. |
| `completion.rs` | `CompletionQueue`: bounded SPSC queue of completion records plus a byte arena for response bodies. Reader is the producer, `poll` the consumer. Never drops; a full queue parks the reader. |
| `slots.rs` | `SlotTable`: generation-tagged slots indexed `seq & mask`, adapted from `uc_client/src/slots.rs` (copied, not depended on). Adds the per-slot ring extent (`off`,`len`), `sent` flag and `not_before_ns`. |
| `link.rs` | One connection's shared state and its two threads: dial/HELLO/redirect scan, writer loop (drain, probe limit, retransmits, PING, redial + ordered resend), reader loop (frame dispatch, credits, resolve, sweep, liveness). |
| `engine.rs` | The public halves: `RemoteEngine`, `RemoteSendHalf`, `RemotePollHalf`, `RemoteWaitHandle`, `RemoteCompletion`, `RemoteOutcome`, `SubmitError`, `Consistency`, `RemoteConfig`, `RemoteStats`, `RemoteResponse`. |
| `client.rs` | **Rewritten**: the blocking convenience `RemoteClient` + `Ticket` over the halves (its own poller thread + an `Arc<TicketCore>` per request). Not the measured path. |

**Deviation from the spec's suggested file list, stated:** `park.rs` is an extra file — the wait cell is shared by three call sites and does not belong to any one of them. Everything else matches §3's suggested structure.

**Modified:**

| File | Change |
|---|---|
| `uc_remote/src/frame.rs:120-133` | add `encode_header_into(h, payload_len) -> [u8; HEADER_LEN]` beside `encode_frame`; `encode_frame` reuses it. |
| `uc_remote/src/lib.rs:14-21` | new modules + re-exports. |
| `uc_remote/tests/client_fake_edge.rs` | trimmed to the convenience-layer scenarios (Task 10). |
| `uc_remote/tests/engine_fake_edge.rs` | **created**: all 27 scenarios ported to the halves. |
| `uc_gateway/examples/hop_bench/remote_load.rs` | rebuilt on the halves in the `engine_load` shape; `--senders` dropped. |
| `uc_gateway/examples/m12_gate.rs:881-1101, 1384-1415` | `run_remote_measurement`/`print_remote_stats` on the halves. |
| `examples/uc_crashtest/tests/remote_lin.rs`, `examples/counter/src/bin/counter-remote.rs`, `uc_gateway/tests/{credits,credits_wire,failover,roundtrip}.rs` | convenience client; only `query`'s second argument changes. |
| `docs/reference/remote-protocol.md` | §6 clarifications + a "client structure" note. |
| `README.md:179`, `docs/QUICKSTART.md:454`, `docs/how-to/run-a-gateway.md:119` | one-line API-name touch-ups. |

**Unaffected, verified:** `uc_remote/src/conn.rs` and `frame.rs`'s wire types (the halves use `FramedConn::{read_frame, read_frame_buffered, next_buffered, write_all_bytes, write_frame, try_clone, shutdown, set_read_timeout, set_write_timeout}` unchanged); `fuzz/fuzz_targets/uc_remote_frame.rs`, `fuzz/src/seeds.rs`, `fuzz/src/bin/seed_corpus.rs` (they import only `uc_remote::frame::*`); `uc_gateway/src/{edge,conn}.rs` (they import only `frame` + `conn`); `uc_gateway/examples/hop_bench/{blaster,dummy_edge,stats,engine_load}.rs`; `uc_gateway/tests/{bin_smoke,config_file}.rs` (zero `uc_remote` references).

---

## The concurrency contract (read before Task 2)

Five cursors and who may write each. **Every atomic in this design has exactly one writer thread.**

| Atomic | Writer | Readers | Rule |
|---|---|---|---|
| `OutRing.write` | submitter | writer thread | monotone; producer publishes `Release` after the bytes are copied |
| `OutRing.send` | writer thread | submitter (tests), reader (never) | monotone forward except a redial's `set_send_pos`, which is also forward-only |
| `OutRing.ack` | submitter | writer thread (redial) | monotone; the submitter's reclaim frontier, clamped to `send` |
| `Link.credits`, `Link.acked_seq` | reader thread | submitter, writer | `acked_seq` is `fetch_max`; `credits` is an absolute store |
| `SlotTable.next_seq` | submitter | writer thread | monotone; published `Release` after the slot is published |

**The ring safety invariant:** `ack <= send <= write`, and the submitter only ever writes bytes into `[write, ack + capacity)`. Therefore the byte range `[ack, write)` — everything the writer might still send or re-send — is never touched by the submitter. `release_to` enforces `ack <= send` by clamping to `send`; a redial's rewind enforces `send >= ack` by taking a `max`. This is why no lock is needed between the two threads.

**Reclaim is keyed on slot completion, not on `acked_seq`.** The edge advances `acked_seq` with `fetch_max(seq)` on **SUBMIT only** (`uc_gateway/src/conn.rs:309`) — a QUERY never advances it, so `acked_seq` is not a contiguous prefix and cannot drive reclaim. The submitter instead walks `reclaim_seq` forward while the slot at that seq is FREE (completed, timed out or aborted), releasing `off + len` as it goes.

**Seqs are never burned.** `try_submit` checks ring space *before* it assigns a seq (it is the only producer, so check-then-push is race-free) and the slot claim cannot fail once the inflight check has passed, so the on-wire seq sequence is gap-free and strictly increasing from 1.

---

### Task 1: Foundations — `WaitCell` and `encode_header_into`

**Files:**
- Create: `uc_remote/src/park.rs`
- Modify: `uc_remote/src/frame.rs:120-133` (add `encode_header_into`, make `encode_frame` use it)
- Modify: `uc_remote/src/lib.rs:14-18` (declare `pub(crate) mod park;`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub(crate) struct WaitCell` with `fn new() -> WaitCell`, `fn seq(&self) -> u64`, `fn signal(&self)`, `fn park(&self, observed: u64, timeout: Duration)`.
  - `pub fn encode_header_into(h: Header, payload_len: usize) -> [u8; HEADER_LEN]`.

- [ ] **Step 1: Write the failing tests**

Create `uc_remote/src/park.rs` with the implementation stubbed out but the tests written:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `WaitCell` — a seq-stamped park/wake pair.
//!
//! Ported in shape (not as a dependency) from `uc_client`'s
//! `RingWaitHandle` usage: this crate's small dependency set is an advertised
//! property, so the ~40 lines are copied rather than pulled in.
//!
//! The contract is the one every "check, then park" loop needs: a waiter
//! reads [`WaitCell::seq`] BEFORE it re-checks its condition, and passes that
//! value to [`WaitCell::park`]. A [`WaitCell::signal`] that lands between the
//! check and the park bumps the seq, so the park returns immediately instead
//! of sleeping through the wake it was told about.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

pub(crate) struct WaitCell {
    seq: AtomicU64,
    waiters: AtomicU32,
    lock: Mutex<()>,
    cv: Condvar,
}

impl WaitCell {
    pub(crate) fn new() -> WaitCell {
        WaitCell {
            seq: AtomicU64::new(0),
            waiters: AtomicU32::new(0),
            lock: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    pub(crate) fn signal(&self) {
        unimplemented!()
    }

    pub(crate) fn park(&self, _observed: u64, _timeout: Duration) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn a_signal_between_the_check_and_the_park_is_not_missed() {
        let c = WaitCell::new();
        let observed = c.seq();
        c.signal(); // the wake the waiter must not sleep through
        let t = Instant::now();
        c.park(observed, Duration::from_secs(5));
        assert!(t.elapsed() < Duration::from_secs(1), "park slept through a signal");
    }

    #[test]
    fn a_park_without_a_signal_returns_at_its_timeout() {
        let c = WaitCell::new();
        let observed = c.seq();
        let t = Instant::now();
        c.park(observed, Duration::from_millis(50));
        assert!(t.elapsed() >= Duration::from_millis(40), "park returned far too early");
    }

    #[test]
    fn a_signal_from_another_thread_wakes_a_parked_waiter() {
        let c = Arc::new(WaitCell::new());
        let observed = c.seq();
        let c2 = Arc::clone(&c);
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            c2.signal();
        });
        let t = Instant::now();
        c.park(observed, Duration::from_secs(10));
        assert!(t.elapsed() < Duration::from_secs(5), "waiter was not woken by the signal");
        h.join().unwrap();
    }
}
```

Add to `uc_remote/src/lib.rs`, after `pub mod frame;` (line 17):

```rust
pub(crate) mod park;
```

Add to `uc_remote/src/frame.rs`, in the `#[cfg(test)] mod tests` block at the end of the file (create the block if the file has none):

```rust
#[cfg(test)]
mod header_tests {
    use super::*;

    #[test]
    fn encode_header_into_matches_encode_frame() {
        let h = Header {
            ty: FrameType::Submit,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: 0x0102_0304_0506_0708,
            seq: 42,
        };
        let payload = b"hello";
        let mut whole = Vec::new();
        encode_frame(&mut whole, h, payload);
        let hdr = encode_header_into(h, payload.len());
        assert_eq!(&whole[..HEADER_LEN], &hdr[..], "the two encoders must agree byte for byte");
        assert_eq!(&whole[HEADER_LEN..], payload);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib`
Expected: FAIL — `cannot find function 'encode_header_into' in this scope`, and the three `park` tests panic with `not implemented`.

- [ ] **Step 3: Write the implementation**

In `uc_remote/src/park.rs`, replace the two `unimplemented!()` bodies:

```rust
    /// Publish a wake. `SeqCst` on both the bump and the `waiters` load is
    /// load-bearing: it is what makes "signaller saw no waiters" imply
    /// "waiter will see the new seq" (store-buffer / Dekker ordering).
    pub(crate) fn signal(&self) {
        self.seq.fetch_add(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) != 0 {
            let _g = self.lock.lock().unwrap();
            self.cv.notify_all();
        }
    }

    /// Park until the seq moves past `observed`, or `timeout` elapses.
    pub(crate) fn park(&self, observed: u64, timeout: Duration) {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let g = self.lock.lock().unwrap();
        if self.seq.load(Ordering::SeqCst) == observed {
            let _ = self.cv.wait_timeout(g, timeout).unwrap();
        } else {
            drop(g);
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
```

In `uc_remote/src/frame.rs`, replace the body of `encode_frame` (lines 120–133) and add the new function above it:

```rust
/// The 24 header bytes for a frame of `payload_len` bytes, as a stack array —
/// so a caller can copy the header and the payload into a ring (or an
/// `iovec`) without an intermediate `Vec`. `encode_frame` is this plus the
/// payload.
pub fn encode_header_into(h: Header, payload_len: usize) -> [u8; HEADER_LEN] {
    let len = (HEADER_LEN + payload_len) as u32;
    let mut out = [0u8; HEADER_LEN];
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out[4] = h.ty as u8;
    out[5] = h.flags;
    out[6..8].copy_from_slice(&h.version.to_le_bytes());
    out[8..16].copy_from_slice(&h.client_id.to_le_bytes());
    out[16..24].copy_from_slice(&h.seq.to_le_bytes());
    out
}

/// Append one encoded frame (header + payload) to `out`.
///
/// Callers must reject an oversized payload (`HEADER_LEN + payload.len() >
/// MAX_FRAME_LEN`) before calling this — the edge answers oversized SUBMITs
/// with `RETRY_PAYLOAD_TOO_LARGE`, it does not truncate or split them here.
pub fn encode_frame(out: &mut Vec<u8>, h: Header, payload: &[u8]) {
    let len = HEADER_LEN + payload.len();
    debug_assert!(
        len <= MAX_FRAME_LEN as usize,
        "encode_frame: frame of {len} bytes exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN}); caller must reject oversized payloads before calling encode_frame"
    );
    out.reserve(len);
    out.extend_from_slice(&encode_header_into(h, payload.len()));
    out.extend_from_slice(payload);
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote --lib && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 4 tests pass (`park` x3, `header_tests` x1), clippy silent.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git checkout -b uc2/m13b-remote-client
git add uc_remote/src/park.rs uc_remote/src/frame.rs uc_remote/src/lib.rs
git commit -m "feat(remote): WaitCell park/wake primitive + encode_header_into

Foundations for the M13b split client: a seq-stamped park cell (no missed
wakes across a check-then-park) and a stack-array header encoder so a frame
can be written straight into a ring with no intermediate Vec."
```

---

### Task 2: The outgoing byte ring (`outgoing.rs`)

**Files:**
- Create: `uc_remote/src/outgoing.rs`
- Modify: `uc_remote/src/lib.rs` (add `pub(crate) mod outgoing;`)

**Interfaces:**
- Consumes: `crate::park::WaitCell` (Task 1); `crate::frame::{encode_header_into, Header, HEADER_LEN}` (Task 1).
- Produces:
  ```rust
  pub(crate) struct OutRing;
  impl OutRing {
      pub(crate) fn new(capacity: usize) -> OutRing;          // rounded up to a power of two
      pub(crate) fn capacity(&self) -> usize;
      pub(crate) fn free(&self) -> usize;                     // submitter only
      pub(crate) fn push_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)>; // submitter only -> (offset, len)
      pub(crate) fn release_to(&self, pos: u64);              // submitter only
      pub(crate) fn write_pos(&self) -> u64;
      pub(crate) fn send_pos(&self) -> u64;
      pub(crate) fn ack_pos(&self) -> u64;
      pub(crate) fn peek_upto(&self, limit: u64) -> &[u8];    // writer only
      pub(crate) fn consume(&self, n: usize);                 // writer only
      pub(crate) fn set_send_pos(&self, pos: u64);            // writer only, forward-only
      pub(crate) fn copy_range(&self, off: u64, len: u32, out: &mut Vec<u8>); // writer only
      pub(crate) fn wake(&self) -> &WaitCell;
  }
  ```

- [ ] **Step 1: Write the failing tests**

Create `uc_remote/src/outgoing.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `OutRing` — the single-producer / single-consumer byte ring the submitter
//! encodes frames into and the writer thread drains.
//!
//! # Why a byte ring and not a queue of `Vec`s
//!
//! The whole point of M13b is that a submit costs no syscall and no
//! allocation: `try_submit` encodes the frame straight into preallocated
//! bytes, and the writer hands **whatever is there** to one `write_all_bytes`
//! (flush-on-empty, no timer). A queue of buffers would reintroduce an
//! allocation per request and a gather per drain.
//!
//! # The safety invariant
//!
//! `ack <= send <= write`, and the producer only ever writes into
//! `[write, ack + capacity)`. So `[ack, write)` — every byte the writer may
//! still send or re-send — is untouched by the producer, which is why the two
//! threads need no lock. [`OutRing::release_to`] clamps to `send`;
//! [`OutRing::set_send_pos`] only ever moves forward. A frame MAY straddle the
//! wrap: [`OutRing::peek_upto`] then returns the contiguous head and the
//! writer comes back for the tail (two `write_all_bytes` once per lap, not per
//! frame).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frame::{encode_header_into, Header, HEADER_LEN};
use crate::park::WaitCell;

pub(crate) struct OutRing {
    buf: UnsafeCell<Box<[u8]>>,
    mask: usize,
    /// Producer-owned: the end of the encoded bytes.
    write: AtomicU64,
    /// Writer-owned: everything below this has been handed to the socket.
    send: AtomicU64,
    /// Producer-owned: reclaim frontier — bytes below it may be overwritten.
    ack: AtomicU64,
    wake: WaitCell,
}

// SAFETY: every byte of `buf` is written by exactly one thread at a time —
// the producer only touches `[write, ack + capacity)` and the consumer only
// reads `[send, write)`, and the invariant `ack <= send <= write` keeps those
// ranges disjoint. The cursors themselves are atomics with one writer each.
unsafe impl Send for OutRing {}
unsafe impl Sync for OutRing {}

impl OutRing {
    pub(crate) fn new(capacity: usize) -> OutRing {
        let cap = capacity.max(4096).next_power_of_two();
        OutRing {
            buf: UnsafeCell::new(vec![0u8; cap].into_boxed_slice()),
            mask: cap - 1,
            write: AtomicU64::new(0),
            send: AtomicU64::new(0),
            ack: AtomicU64::new(0),
            wake: WaitCell::new(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    pub(crate) fn write_pos(&self) -> u64 {
        self.write.load(Ordering::Acquire)
    }

    pub(crate) fn send_pos(&self) -> u64 {
        self.send.load(Ordering::Acquire)
    }

    pub(crate) fn ack_pos(&self) -> u64 {
        self.ack.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) -> &WaitCell {
        &self.wake
    }

    pub(crate) fn free(&self) -> usize {
        unimplemented!()
    }

    pub(crate) fn push_frame(&self, _h: Header, _payload: &[u8]) -> Option<(u64, u32)> {
        unimplemented!()
    }

    pub(crate) fn release_to(&self, _pos: u64) {
        unimplemented!()
    }

    pub(crate) fn peek_upto(&self, _limit: u64) -> &[u8] {
        unimplemented!()
    }

    pub(crate) fn consume(&self, _n: usize) {
        unimplemented!()
    }

    pub(crate) fn set_send_pos(&self, _pos: u64) {
        unimplemented!()
    }

    pub(crate) fn copy_range(&self, _off: u64, _len: u32, _out: &mut Vec<u8>) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{decode_header, FrameType, PROTOCOL_VERSION};

    fn hdr(seq: u64) -> Header {
        Header {
            ty: FrameType::Submit,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: 7,
            seq,
        }
    }

    #[test]
    fn a_pushed_frame_comes_back_out_byte_for_byte() {
        let r = OutRing::new(4096);
        let (off, len) = r.push_frame(hdr(1), b"abcd").expect("room");
        assert_eq!(off, 0);
        assert_eq!(len as usize, HEADER_LEN + 4);
        let chunk = r.peek_upto(r.write_pos());
        assert_eq!(chunk.len(), HEADER_LEN + 4);
        let (h, plen) = decode_header(chunk).expect("header");
        assert_eq!(h.seq, 1);
        assert_eq!(plen, 4);
        assert_eq!(&chunk[HEADER_LEN..], b"abcd");
    }

    #[test]
    fn peek_stops_at_the_wrap_and_the_tail_comes_next() {
        let r = OutRing::new(4096);
        let cap = r.capacity();
        // Fill to 32 bytes short of the wrap, draining as we go so `ack` keeps up.
        let payload = vec![0u8; 100 - HEADER_LEN];
        while r.write_pos() < (cap - 32) as u64 {
            r.push_frame(hdr(1), &payload).expect("room");
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
            r.release_to(r.write_pos());
        }
        let before = r.write_pos();
        let (off, len) = r.push_frame(hdr(2), &payload).expect("room");
        assert_eq!(off, before);
        // The frame straddles the wrap, so the first peek is the head only.
        let head = r.peek_upto(r.write_pos());
        assert!(head.len() < len as usize, "the peek must stop at the wrap: {}", head.len());
        assert_eq!(head.len(), cap - (off as usize & (cap - 1)));
        let n = head.len();
        r.consume(n);
        let tail = r.peek_upto(r.write_pos());
        assert_eq!(n + tail.len(), len as usize, "head + tail is the whole frame");
    }

    #[test]
    fn a_full_ring_refuses_a_push_and_recovers_after_a_release() {
        let r = OutRing::new(4096);
        let payload = vec![0xAAu8; 1000 - HEADER_LEN];
        let mut pushed = 0;
        while r.push_frame(hdr(1), &payload).is_some() {
            pushed += 1;
            assert!(pushed < 100, "the ring never filled");
        }
        assert!(pushed >= 4, "a 4 KiB ring must hold at least four 1 KiB frames");
        // Nothing sent yet: releasing is clamped to `send`, so it buys nothing.
        r.release_to(r.write_pos());
        assert!(r.push_frame(hdr(1), &payload).is_none(), "release must clamp to send_pos");
        // Send the whole ring, then release: room again.
        while r.send_pos() < r.write_pos() {
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
        }
        r.release_to(r.write_pos());
        assert!(r.push_frame(hdr(1), &payload).is_some(), "a released ring takes new frames");
    }

    #[test]
    fn copy_range_reassembles_a_wrapped_frame() {
        let r = OutRing::new(4096);
        let cap = r.capacity();
        let payload = vec![0xCDu8; 100 - HEADER_LEN];
        while r.write_pos() < (cap - 32) as u64 {
            r.push_frame(hdr(1), &payload).expect("room");
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
            r.release_to(r.write_pos());
        }
        let (off, len) = r.push_frame(hdr(9), &payload).expect("room");
        let mut out = Vec::new();
        r.copy_range(off, len, &mut out);
        assert_eq!(out.len(), len as usize);
        let (h, plen) = decode_header(&out).expect("header");
        assert_eq!(h.seq, 9);
        assert_eq!(plen, payload.len());
        assert_eq!(&out[HEADER_LEN..], &payload[..]);
    }

    #[test]
    fn a_frame_larger_than_the_ring_is_refused_rather_than_wedging() {
        let r = OutRing::new(4096);
        let payload = vec![0u8; 8192];
        assert!(r.push_frame(hdr(1), &payload).is_none());
    }

    #[test]
    fn set_send_pos_never_moves_backwards() {
        let r = OutRing::new(4096);
        r.push_frame(hdr(1), b"xy").expect("room");
        let n = r.peek_upto(r.write_pos()).len();
        r.consume(n);
        let sent = r.send_pos();
        r.set_send_pos(0);
        assert_eq!(r.send_pos(), sent, "set_send_pos is forward-only");
    }
}
```

Add to `uc_remote/src/lib.rs`:

```rust
pub(crate) mod outgoing;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib outgoing`
Expected: FAIL — all six tests panic with `not implemented`.

- [ ] **Step 3: Write the implementation**

Replace the six `unimplemented!()` bodies in `uc_remote/src/outgoing.rs`:

```rust
    /// Bytes the producer may still write. Producer-only: `ack` has a single
    /// writer (this thread), so a `Relaxed` load of it is correct.
    pub(crate) fn free(&self) -> usize {
        let w = self.write.load(Ordering::Relaxed);
        let a = self.ack.load(Ordering::Relaxed);
        self.capacity() - (w - a) as usize
    }

    /// Encode one frame at the write cursor. `None` = no room (the caller
    /// reports `Backpressure`) or a frame larger than the whole ring (the
    /// caller reports `PayloadTooLarge`; the two are told apart by comparing
    /// the need against [`OutRing::capacity`]).
    ///
    /// PRODUCER ONLY.
    pub(crate) fn push_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)> {
        let need = HEADER_LEN + payload.len();
        if need > self.capacity() || need > self.free() {
            return None;
        }
        let start = self.write.load(Ordering::Relaxed);
        let hdr = encode_header_into(h, payload.len());
        // SAFETY: the producer owns `[write, ack + capacity)` (invariant at the
        // top of this module) and `need <= free()`, so these writes cannot
        // touch a byte the consumer may read.
        let buf = unsafe { &mut *self.buf.get() };
        let mut pos = start;
        for src in [&hdr[..], payload] {
            let mut done = 0usize;
            while done < src.len() {
                let idx = (pos as usize) & self.mask;
                let n = src.len().min(self.capacity() - idx).min(src.len() - done);
                buf[idx..idx + n].copy_from_slice(&src[done..done + n]);
                done += n;
                pos += n as u64;
            }
        }
        self.write.store(start + need as u64, Ordering::Release);
        self.wake.signal();
        Some((start, need as u32))
    }

    /// Move the reclaim frontier up to `pos`, clamped to `send` (bytes that
    /// have not been written to the socket must never be overwritten) and
    /// never backwards. PRODUCER ONLY.
    pub(crate) fn release_to(&self, pos: u64) {
        let target = pos.min(self.send.load(Ordering::Acquire));
        let cur = self.ack.load(Ordering::Relaxed);
        if target > cur {
            self.ack.store(target, Ordering::Release);
        }
    }

    /// The contiguous readable run starting at `send`, stopping at `limit`
    /// (the writer's flush limit) and at the ring's wrap. CONSUMER ONLY.
    pub(crate) fn peek_upto(&self, limit: u64) -> &[u8] {
        let s = self.send.load(Ordering::Relaxed);
        let end = limit.min(self.write.load(Ordering::Acquire));
        if end <= s {
            return &[];
        }
        let idx = (s as usize) & self.mask;
        let n = ((end - s) as usize).min(self.capacity() - idx);
        // SAFETY: `[send, write)` is written only by the producer, which
        // published it with a `Release` store to `write` that this
        // `Acquire` load synchronizes with; the producer will not touch it
        // again until it is both sent and released.
        let buf = unsafe { &*self.buf.get() };
        &buf[idx..idx + n]
    }

    /// CONSUMER ONLY: `n` bytes reached the socket.
    pub(crate) fn consume(&self, n: usize) {
        let s = self.send.load(Ordering::Relaxed);
        self.send.store(s + n as u64, Ordering::Release);
    }

    /// CONSUMER ONLY: used by the redial path, which re-sends the live window
    /// by hand and then jumps the cursor to the snapshot it worked against.
    /// Forward-only, so it can never expose bytes the producer has reclaimed.
    pub(crate) fn set_send_pos(&self, pos: u64) {
        let s = self.send.load(Ordering::Relaxed);
        if pos > s {
            self.send.store(pos, Ordering::Release);
        }
    }

    /// CONSUMER ONLY: copy one frame's bytes out (the RETRY / redial paths,
    /// which re-send a frame that is behind `send`). `out` is cleared first.
    pub(crate) fn copy_range(&self, off: u64, len: u32, out: &mut Vec<u8>) {
        out.clear();
        out.reserve(len as usize);
        // SAFETY: the caller re-sends only frames whose slot is still live,
        // and a live slot's bytes are at or above `ack` — never reclaimed,
        // therefore never rewritten by the producer.
        let buf = unsafe { &*self.buf.get() };
        let mut done = 0usize;
        while done < len as usize {
            let idx = ((off + done as u64) as usize) & self.mask;
            let n = (len as usize - done).min(self.capacity() - idx);
            out.extend_from_slice(&buf[idx..idx + n]);
            done += n;
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote --lib outgoing && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 6 tests, clippy silent.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/outgoing.rs uc_remote/src/lib.rs
git commit -m "feat(remote): SPSC outgoing byte ring

The submitter encodes frames straight into preallocated bytes and the writer
thread drains whatever is there in one write_all_bytes. Invariant
ack <= send <= write keeps the producer's and consumer's byte ranges disjoint,
so the two threads share no lock. Tests cover wrap, full-refusal, wrapped
copy_range and the forward-only send cursor."
```

---

### Task 3: The completion queue and its arena (`completion.rs`)

**Files:**
- Create: `uc_remote/src/completion.rs`
- Modify: `uc_remote/src/lib.rs` (add `pub(crate) mod completion;`)

**Interfaces:**
- Consumes: `crate::park::WaitCell` (Task 1).
- Produces:
  ```rust
  #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub(crate) enum OutcomeTag { Response, Unknown, PayloadTooLarge, TimedOut, Closed }
  #[derive(Clone, Copy)]
  pub(crate) struct Record { pub user_data: u64, pub position: u64, pub has_position: bool,
                             pub tag: OutcomeTag, pub replayed: bool, pub expired: bool,
                             pub body_off: u64, pub body_len: u32 }
  pub(crate) struct CompletionQueue;
  impl CompletionQueue {
      pub(crate) fn new(entries: usize, arena_bytes: usize) -> CompletionQueue;
      pub(crate) fn push(&self, r: Record, body: &[u8]) -> bool;   // producer (reader thread); false = full
      pub(crate) fn publish(&self);                                 // producer: one wake per read batch
      pub(crate) fn drain(&self, max: usize, cb: impl FnMut(Record, &[u8])) -> usize; // consumer (poll)
      pub(crate) fn is_empty(&self) -> bool;
      pub(crate) fn ready(&self) -> &WaitCell;    // consumer parks here
      pub(crate) fn drained(&self) -> &WaitCell;  // producer parks here when full
  }
  ```

- [ ] **Step 1: Write the failing tests**

Create `uc_remote/src/completion.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `CompletionQueue` — the bounded SPSC hand-off from the reader thread to
//! `RemotePollHalf::poll`.
//!
//! The reader is the producer; `poll` is the consumer. Response bodies are
//! copied **once**, into this queue's arena, so a completion callback can
//! borrow them without a per-request allocation and without pinning the
//! socket read buffer.
//!
//! **It never drops.** A full queue makes [`CompletionQueue::push`] return
//! `false`; the reader then publishes what it has and parks on
//! [`CompletionQueue::drained`] until `poll` frees space. Dropping a
//! completion would break the crate's central promise (every accepted request
//! ends in exactly one outcome), so backpressure is the only option.
//!
//! Sizing: the arena is at least `MAX_FRAME_LEN`, so any single body that
//! could arrive on this wire fits in an empty arena — which is what makes
//! "park until there is room" terminate rather than deadlock.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::park::WaitCell;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum OutcomeTag {
    Response,
    Unknown,
    PayloadTooLarge,
    TimedOut,
    Closed,
}

#[derive(Clone, Copy)]
pub(crate) struct Record {
    pub user_data: u64,
    pub position: u64,
    pub has_position: bool,
    pub tag: OutcomeTag,
    pub replayed: bool,
    pub expired: bool,
    pub body_off: u64,
    pub body_len: u32,
}

impl Record {
    pub(crate) fn simple(user_data: u64, tag: OutcomeTag) -> Record {
        Record {
            user_data,
            position: 0,
            has_position: false,
            tag,
            replayed: false,
            expired: false,
            body_off: 0,
            body_len: 0,
        }
    }
}

pub(crate) struct CompletionQueue {
    slots: UnsafeCell<Box<[Record]>>,
    slot_mask: usize,
    arena: UnsafeCell<Box<[u8]>>,
    arena_mask: usize,
    /// Producer-owned.
    head: AtomicU64,
    /// Consumer-owned.
    tail: AtomicU64,
    /// Producer-owned: arena bytes written.
    arena_head: AtomicU64,
    /// Consumer-owned: arena bytes released.
    arena_tail: AtomicU64,
    ready: WaitCell,
    drained: WaitCell,
}

// SAFETY: single producer (the reader thread) and single consumer (`poll`),
// each owning its own cursors; `[tail, head)` is read-only for the producer
// and `[head, tail + capacity)` is untouched by the consumer.
unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

impl CompletionQueue {
    pub(crate) fn new(_entries: usize, _arena_bytes: usize) -> CompletionQueue {
        unimplemented!()
    }

    pub(crate) fn push(&self, _r: Record, _body: &[u8]) -> bool {
        unimplemented!()
    }

    pub(crate) fn publish(&self) {
        unimplemented!()
    }

    pub(crate) fn drain(&self, _max: usize, _cb: impl FnMut(Record, &[u8])) -> usize {
        unimplemented!()
    }

    pub(crate) fn is_empty(&self) -> bool {
        unimplemented!()
    }

    pub(crate) fn ready(&self) -> &WaitCell {
        &self.ready
    }

    pub(crate) fn drained(&self) -> &WaitCell {
        &self.drained
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pushed_record_and_its_body_come_back_out() {
        let q = CompletionQueue::new(8, 64 * 1024);
        let mut r = Record::simple(0xDEAD, OutcomeTag::Response);
        r.position = 640;
        r.has_position = true;
        r.replayed = true;
        assert!(q.push(r, b"cba"));
        q.publish();
        let mut seen = Vec::new();
        let n = q.drain(16, |rec, body| seen.push((rec.user_data, rec.position, rec.replayed, body.to_vec())));
        assert_eq!(n, 1);
        assert_eq!(seen, vec![(0xDEAD, 640, true, b"cba".to_vec())]);
        assert!(q.is_empty());
    }

    #[test]
    fn a_full_queue_refuses_rather_than_dropping_and_recovers_after_a_drain() {
        let q = CompletionQueue::new(4, 64 * 1024);
        let mut pushed = 0u64;
        while q.push(Record::simple(pushed, OutcomeTag::Response), b"x") {
            pushed += 1;
            assert!(pushed < 64, "the queue never filled");
        }
        assert_eq!(pushed, 4, "capacity is the entry count, exactly");
        let drained = q.drain(2, |_, _| {});
        assert_eq!(drained, 2);
        assert!(q.push(Record::simple(99, OutcomeTag::Response), b"x"), "a drained queue takes more");
    }

    #[test]
    fn a_full_arena_refuses_even_when_slots_are_free() {
        // 4 KiB arena (rounded), 1024 entries: the arena is the binding limit.
        let q = CompletionQueue::new(1024, 4096);
        let body = vec![0u8; 1000];
        let mut pushed = 0;
        while q.push(Record::simple(pushed, OutcomeTag::Response), &body) {
            pushed += 1;
            assert!(pushed < 64, "the arena never filled");
        }
        assert!(pushed <= 4, "a 4 KiB arena cannot hold five 1000-byte bodies");
        let n = q.drain(1024, |_, b| assert_eq!(b.len(), 1000));
        assert_eq!(n as u64, pushed);
        assert!(q.push(Record::simple(1, OutcomeTag::Response), &body), "a drained arena takes more");
    }

    #[test]
    fn a_body_that_wraps_the_arena_is_returned_contiguous() {
        let q = CompletionQueue::new(64, 4096);
        let body: Vec<u8> = (0..600u32).map(|i| i as u8).collect();
        // Six 600-byte bodies push the seventh over the 4 KiB wrap.
        for round in 0..8 {
            assert!(q.push(Record::simple(round, OutcomeTag::Response), &body), "round {round}");
            let n = q.drain(1, |rec, b| {
                assert_eq!(rec.user_data, round);
                assert_eq!(b, &body[..], "round {round}: a wrapped body must read back whole");
            });
            assert_eq!(n, 1);
        }
    }

    #[test]
    fn drain_is_bounded_by_max() {
        let q = CompletionQueue::new(64, 64 * 1024);
        for i in 0..10 {
            assert!(q.push(Record::simple(i, OutcomeTag::TimedOut), b""));
        }
        assert_eq!(q.drain(3, |_, _| {}), 3);
        assert_eq!(q.drain(100, |_, _| {}), 7);
    }

    #[test]
    fn publish_bumps_the_ready_cell_so_a_parked_poller_wakes() {
        let q = CompletionQueue::new(8, 4096);
        let before = q.ready().seq();
        q.publish();
        assert_ne!(q.ready().seq(), before);
    }
}
```

Add to `uc_remote/src/lib.rs`:

```rust
pub(crate) mod completion;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib completion`
Expected: FAIL — six tests panic with `not implemented`.

- [ ] **Step 3: Write the implementation**

Replace the five `unimplemented!()` bodies in `uc_remote/src/completion.rs`:

```rust
    pub(crate) fn new(entries: usize, arena_bytes: usize) -> CompletionQueue {
        let n = entries.max(16).next_power_of_two();
        // At least MAX_FRAME_LEN so any single body that can arrive fits in an
        // empty arena — the property that makes "park until there is room"
        // terminate.
        let a = arena_bytes.max(crate::frame::MAX_FRAME_LEN as usize).next_power_of_two();
        let blank = Record::simple(0, OutcomeTag::Closed);
        CompletionQueue {
            slots: UnsafeCell::new(vec![blank; n].into_boxed_slice()),
            slot_mask: n - 1,
            arena: UnsafeCell::new(vec![0u8; a].into_boxed_slice()),
            arena_mask: a - 1,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            arena_head: AtomicU64::new(0),
            arena_tail: AtomicU64::new(0),
            ready: WaitCell::new(),
            drained: WaitCell::new(),
        }
    }

    /// PRODUCER ONLY. `false` = no room; the caller must retry the same
    /// record after `poll` has drained (it must never drop it).
    pub(crate) fn push(&self, mut r: Record, body: &[u8]) -> bool {
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Acquire);
        if (h - t) as usize > self.slot_mask {
            return false;
        }
        let ah = self.arena_head.load(Ordering::Relaxed);
        let at = self.arena_tail.load(Ordering::Acquire);
        let arena_cap = self.arena_mask + 1;
        if body.len() > arena_cap - (ah - at) as usize {
            return false;
        }
        // SAFETY: `[arena_head, arena_tail + capacity)` is producer-owned.
        let arena = unsafe { &mut *self.arena.get() };
        let mut done = 0usize;
        while done < body.len() {
            let idx = ((ah + done as u64) as usize) & self.arena_mask;
            let n = (body.len() - done).min(arena_cap - idx);
            arena[idx..idx + n].copy_from_slice(&body[done..done + n]);
            done += n;
        }
        r.body_off = ah;
        r.body_len = body.len() as u32;
        // SAFETY: slot `h & mask` is producer-owned until `head` is published.
        let slots = unsafe { &mut *self.slots.get() };
        slots[(h as usize) & self.slot_mask] = r;
        self.arena_head.store(ah + body.len() as u64, Ordering::Release);
        self.head.store(h + 1, Ordering::Release);
        true
    }

    /// PRODUCER ONLY: one wake per read batch, not per frame.
    pub(crate) fn publish(&self) {
        self.ready.signal();
    }

    /// CONSUMER ONLY: hand at most `max` completions to `cb`, then release
    /// their arena bytes. A body that wrapped is copied into `scratch` so the
    /// callback always sees one contiguous slice.
    pub(crate) fn drain(&self, max: usize, mut cb: impl FnMut(Record, &[u8])) -> usize {
        let arena_cap = self.arena_mask + 1;
        let mut t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Acquire);
        let mut n = 0usize;
        let mut scratch: Vec<u8> = Vec::new();
        let mut arena_to = self.arena_tail.load(Ordering::Relaxed);
        while n < max && t < h {
            // SAFETY: `[tail, head)` is consumer-owned; the producer published
            // both the slot and its arena bytes with `Release` stores that the
            // `Acquire` load of `head` above synchronizes with.
            let rec = unsafe { (*self.slots.get())[(t as usize) & self.slot_mask] };
            let arena = unsafe { &*self.arena.get() };
            let idx = (rec.body_off as usize) & self.arena_mask;
            let len = rec.body_len as usize;
            if idx + len <= arena_cap {
                cb(rec, &arena[idx..idx + len]);
            } else {
                let head_n = arena_cap - idx;
                scratch.clear();
                scratch.extend_from_slice(&arena[idx..arena_cap]);
                scratch.extend_from_slice(&arena[..len - head_n]);
                cb(rec, &scratch);
            }
            arena_to = rec.body_off + rec.body_len as u64;
            t += 1;
            n += 1;
        }
        if n > 0 {
            self.arena_tail.store(arena_to, Ordering::Release);
            self.tail.store(t, Ordering::Release);
            self.drained.signal();
        }
        n
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote --lib completion && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 6 tests, clippy silent.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/completion.rs uc_remote/src/lib.rs
git commit -m "feat(remote): bounded SPSC completion queue with a body arena

Reader-to-poller hand-off: bodies copied once into an arena, one wake per read
batch, and a full queue refuses (the reader parks) rather than dropping — the
crate's every-request-ends-once promise does not survive a dropped completion."
```

---

### Task 4: The slot table (`slots.rs`)

**Files:**
- Create: `uc_remote/src/slots.rs`
- Modify: `uc_remote/src/lib.rs` (add `pub(crate) mod slots;`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  #[derive(Clone, Copy, PartialEq, Eq, Debug)] pub(crate) enum ReqKind { Submit = 0, Query = 1 }
  #[derive(Debug, PartialEq, Eq)] pub(crate) enum Resolve { Won { user_data: u64 }, Miss }
  pub(crate) struct SlotTable;
  impl SlotTable {
      pub(crate) fn new(max_inflight: u32) -> SlotTable;
      pub(crate) fn claim(&self, seq: u64, user_data: u64, kind: ReqKind, deadline_ns: u64, off: u64, len: u32) -> bool;
      pub(crate) fn resolve(&self, seq: u64) -> Resolve;
      pub(crate) fn abort(&self, seq: u64) -> Resolve;
      pub(crate) fn sweep(&self, now_ns: u64, cb: impl FnMut(u64)) -> usize;
      pub(crate) fn drain_abort(&self, cb: impl FnMut(u64)) -> usize;
      pub(crate) fn is_live(&self, seq: u64) -> bool;
      pub(crate) fn extent(&self, seq: u64) -> (u64, u32);
      pub(crate) fn kind(&self, seq: u64) -> ReqKind;
      pub(crate) fn mark_sent(&self, seq: u64, sent: bool);
      pub(crate) fn is_sent(&self, seq: u64) -> bool;
      pub(crate) fn set_not_before(&self, seq: u64, ns: u64);
      pub(crate) fn not_before(&self, seq: u64) -> u64;
      pub(crate) fn bump_attempts(&self, seq: u64) -> u32;
      pub(crate) fn inflight(&self) -> u64;
      pub(crate) fn next_seq(&self) -> u64;
      pub(crate) fn publish_next_seq(&self, seq: u64);
  }
  ```

**Design note (why this is a copy, not a dependency):** `uc_client/src/slots.rs` owns the generation-tagged, single-CAS completion protocol this needs; `uc_remote` must not depend on `uc_client` (dependency-light is an advertised property), so the file is copied and adapted. Three adaptations: (1) `seq` is a full `u64` on this wire, so `resolve` takes `u64` and the u32-truncation argument of the shmem version is unnecessary; (2) the submitter assigns the seq (gap-free, from 1) and passes it in, so `claim` takes a seq instead of allocating one — a "slot busy" claim is impossible because the inflight cap plus the 2x table headroom keep a live occupant out of the way, and `claim` returns `false` for it so the caller can report backpressure without burning a seq; (3) each slot carries the request's **ring extent** `(off, len)`, its `sent` flag and its `not_before_ns`, which the writer thread needs for probe limits, retransmits and the ordered resend.

- [ ] **Step 1: Write the failing tests**

Create `uc_remote/src/slots.rs` with the module doc, the types, the `unimplemented!()` bodies and this test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> SlotTable {
        SlotTable::new(8)
    }

    #[test]
    fn a_claimed_slot_resolves_exactly_once() {
        let t = table();
        assert!(t.claim(1, 0xAA, ReqKind::Submit, 1_000, 0, 88));
        assert_eq!(t.inflight(), 1);
        assert_eq!(t.resolve(1), Resolve::Won { user_data: 0xAA });
        assert_eq!(t.resolve(1), Resolve::Miss, "a second resolve must lose");
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn a_stale_generation_never_resolves_a_live_slot() {
        let t = table();
        let n = 1 + (t.slot_count() as u64); // same index, older generation
        assert!(t.claim(n, 0xBB, ReqKind::Submit, 1_000, 0, 88));
        assert_eq!(t.resolve(1), Resolve::Miss, "seq 1 is a stale generation of that slot");
        assert_eq!(t.resolve(n), Resolve::Won { user_data: 0xBB });
    }

    #[test]
    fn the_window_is_capped_at_max_inflight() {
        let t = table();
        for seq in 1..=8u64 {
            assert!(t.claim(seq, seq, ReqKind::Submit, 1_000, 0, 88), "seq {seq}");
        }
        assert!(!t.claim(9, 9, ReqKind::Submit, 1_000, 0, 88), "the 9th must be refused");
        assert_eq!(t.inflight(), 8);
        assert_eq!(t.resolve(1), Resolve::Won { user_data: 1 });
        assert!(t.claim(9, 9, ReqKind::Submit, 1_000, 0, 88), "a freed slot admits the next");
    }

    #[test]
    fn sweep_fails_everything_past_its_deadline_and_nothing_before_it() {
        let t = table();
        assert!(t.claim(1, 0xA1, ReqKind::Submit, 100, 0, 88));
        assert!(t.claim(2, 0xA2, ReqKind::Submit, 900, 88, 88));
        let mut fired = Vec::new();
        assert_eq!(t.sweep(500, |ud| fired.push(ud)), 1);
        assert_eq!(fired, vec![0xA1]);
        assert_eq!(t.resolve(1), Resolve::Miss, "a swept slot is gone");
        assert_eq!(t.resolve(2), Resolve::Won { user_data: 0xA2 });
    }

    #[test]
    fn drain_abort_takes_every_live_slot_once() {
        let t = table();
        for seq in 1..=4u64 {
            assert!(t.claim(seq, seq, ReqKind::Submit, u64::MAX, 0, 88));
        }
        let mut fired = Vec::new();
        assert_eq!(t.drain_abort(|ud| fired.push(ud)), 4);
        fired.sort_unstable();
        assert_eq!(fired, vec![1, 2, 3, 4]);
        assert_eq!(t.drain_abort(|_| {}), 0);
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn the_ring_extent_and_the_sent_flag_round_trip() {
        let t = table();
        assert!(t.claim(1, 0xAA, ReqKind::Query, 1_000, 4096, 120));
        assert_eq!(t.extent(1), (4096, 120));
        assert_eq!(t.kind(1), ReqKind::Query);
        assert!(!t.is_sent(1), "a fresh slot has not been written yet");
        t.mark_sent(1, true);
        assert!(t.is_sent(1));
        t.mark_sent(1, false);
        assert!(!t.is_sent(1), "a RETRY marks a slot unsent again");
        t.set_not_before(1, 12_345);
        assert_eq!(t.not_before(1), 12_345);
        assert_eq!(t.bump_attempts(1), 1);
        assert_eq!(t.bump_attempts(1), 2);
    }

    #[test]
    fn is_live_tracks_the_slot_and_next_seq_is_published() {
        let t = table();
        assert!(!t.is_live(1));
        assert!(t.claim(1, 1, ReqKind::Submit, 1_000, 0, 88));
        assert!(t.is_live(1));
        t.publish_next_seq(2);
        assert_eq!(t.next_seq(), 2);
        assert_eq!(t.abort(1), Resolve::Won { user_data: 1 });
        assert!(!t.is_live(1));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib slots`
Expected: FAIL — seven tests panic with `not implemented`.

- [ ] **Step 3: Write the implementation**

Write `uc_remote/src/slots.rs` in full:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Correlation slot table: generation-tagged, exactly-once completions.
//!
//! ADAPTED COPY of `uc_client/src/slots.rs` (same invariants, same
//! single-CAS completion protocol) rather than a dependency: `uc_remote`'s
//! tiny dependency set is an advertised property of the crate.
//!
//! # The invariants this file owns
//!
//! 1. A slot's `owner` word is `0` = FREE, `u64::MAX` = RESERVED (mid-claim,
//!    metadata not yet valid), else `seq + 1` — the generation tag.
//! 2. Claim is three-phase: CAS `FREE -> RESERVED`, write metadata, publish
//!    `owner = seq + 1` with `Release`.
//! 3. Exactly-once resolution: whoever CASes `owner: seq+1 -> FREE` (AcqRel)
//!    owns the completion. `resolve`, `abort`, `sweep` and `drain_abort` all
//!    race through that one CAS.
//! 4. The seq is assigned by the submitter (gap-free, from 1) and is a full
//!    `u64` on this wire, so a stale generation is caught by the exact
//!    `owner == seq + 1` test — no truncation argument needed.
//! 5. `extent`, `kind`, `sent`, `not_before_ns` and `attempts` are written by
//!    the submitter before publish and thereafter by the writer/reader
//!    threads; they are advisory (they steer the writer), never the
//!    completion protocol, so `Relaxed` is correct for them.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

const FREE: u64 = 0;
const RESERVED: u64 = u64::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ReqKind {
    Submit = 0,
    Query = 1,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Resolve {
    Won { user_data: u64 },
    Miss,
}

struct Slot {
    owner: AtomicU64,
    user_data: AtomicU64,
    deadline_ns: AtomicU64,
    not_before_ns: AtomicU64,
    off: AtomicU64,
    len: AtomicU32,
    attempts: AtomicU32,
    kind: AtomicU8,
    sent: AtomicU8,
}

pub(crate) struct SlotTable {
    slots: Box<[Slot]>,
    mask: usize,
    inflight: AtomicU64,
    max_inflight: u64,
    next_seq: AtomicU64,
}

impl SlotTable {
    pub(crate) fn new(max_inflight: u32) -> SlotTable {
        assert!(max_inflight >= 1);
        // 2x headroom over the window, 64 floor — same sizing rule as
        // `uc_client`: it keeps a stuck (deadline-pending) occupant off the
        // index a fresh seq lands on.
        let n = (max_inflight.next_power_of_two() as usize * 2).max(64);
        let slots = (0..n)
            .map(|_| Slot {
                owner: AtomicU64::new(FREE),
                user_data: AtomicU64::new(0),
                deadline_ns: AtomicU64::new(0),
                not_before_ns: AtomicU64::new(0),
                off: AtomicU64::new(0),
                len: AtomicU32::new(0),
                attempts: AtomicU32::new(0),
                kind: AtomicU8::new(0),
                sent: AtomicU8::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        SlotTable {
            slots,
            mask: n - 1,
            inflight: AtomicU64::new(0),
            max_inflight: max_inflight as u64,
            next_seq: AtomicU64::new(1),
        }
    }

    fn slot(&self, seq: u64) -> &Slot {
        &self.slots[(seq as usize) & self.mask]
    }

    /// SUBMITTER ONLY. `false` = the window is full or the slot's previous
    /// occupant is still live; either way the caller reports backpressure and
    /// does NOT consume the seq.
    pub(crate) fn claim(
        &self,
        seq: u64,
        user_data: u64,
        kind: ReqKind,
        deadline_ns: u64,
        off: u64,
        len: u32,
    ) -> bool {
        if self.inflight.fetch_add(1, Ordering::AcqRel) >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        let s = self.slot(seq);
        if s.owner
            .compare_exchange(FREE, RESERVED, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        s.user_data.store(user_data, Ordering::Relaxed);
        s.deadline_ns.store(deadline_ns, Ordering::Relaxed);
        s.not_before_ns.store(0, Ordering::Relaxed);
        s.off.store(off, Ordering::Relaxed);
        s.len.store(len, Ordering::Relaxed);
        s.attempts.store(0, Ordering::Relaxed);
        s.kind.store(kind as u8, Ordering::Relaxed);
        s.sent.store(0, Ordering::Relaxed);
        s.owner.store(seq + 1, Ordering::Release);
        true
    }

    fn take(&self, seq: u64) -> Resolve {
        let s = self.slot(seq);
        let owner = s.owner.load(Ordering::Acquire);
        if owner != seq + 1 {
            return Resolve::Miss;
        }
        let user_data = s.user_data.load(Ordering::Relaxed);
        if s.owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss;
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data }
    }

    pub(crate) fn resolve(&self, seq: u64) -> Resolve {
        self.take(seq)
    }

    pub(crate) fn abort(&self, seq: u64) -> Resolve {
        self.take(seq)
    }

    pub(crate) fn sweep(&self, now_ns: u64, mut cb: impl FnMut(u64)) -> usize {
        let mut n = 0;
        for s in self.slots.iter() {
            let owner = s.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            if s.deadline_ns.load(Ordering::Relaxed) > now_ns {
                continue;
            }
            let user_data = s.user_data.load(Ordering::Relaxed);
            if s.owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
                n += 1;
            }
        }
        n
    }

    pub(crate) fn drain_abort(&self, mut cb: impl FnMut(u64)) -> usize {
        let mut n = 0;
        for s in self.slots.iter() {
            let owner = s.owner.load(Ordering::Acquire);
            if owner == FREE || owner == RESERVED {
                continue;
            }
            let user_data = s.user_data.load(Ordering::Relaxed);
            if s.owner
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inflight.fetch_sub(1, Ordering::AcqRel);
                cb(user_data);
                n += 1;
            }
        }
        n
    }

    pub(crate) fn is_live(&self, seq: u64) -> bool {
        self.slot(seq).owner.load(Ordering::Acquire) == seq + 1
    }

    pub(crate) fn extent(&self, seq: u64) -> (u64, u32) {
        let s = self.slot(seq);
        (s.off.load(Ordering::Relaxed), s.len.load(Ordering::Relaxed))
    }

    pub(crate) fn kind(&self, seq: u64) -> ReqKind {
        if self.slot(seq).kind.load(Ordering::Relaxed) == ReqKind::Query as u8 {
            ReqKind::Query
        } else {
            ReqKind::Submit
        }
    }

    pub(crate) fn mark_sent(&self, seq: u64, sent: bool) {
        self.slot(seq).sent.store(u8::from(sent), Ordering::Relaxed);
    }

    pub(crate) fn is_sent(&self, seq: u64) -> bool {
        self.slot(seq).sent.load(Ordering::Relaxed) != 0
    }

    pub(crate) fn set_not_before(&self, seq: u64, ns: u64) {
        self.slot(seq).not_before_ns.store(ns, Ordering::Relaxed);
    }

    pub(crate) fn not_before(&self, seq: u64) -> u64 {
        self.slot(seq).not_before_ns.load(Ordering::Relaxed)
    }

    pub(crate) fn bump_attempts(&self, seq: u64) -> u32 {
        self.slot(seq).attempts.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Acquire)
    }

    /// The lowest seq never yet issued. SUBMITTER publishes, writer reads.
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

    pub(crate) fn publish_next_seq(&self, seq: u64) {
        self.next_seq.store(seq, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote --lib slots && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 7 tests, clippy silent.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/slots.rs uc_remote/src/lib.rs
git commit -m "feat(remote): generation-tagged slot table

Adapted copy of uc_client's SlotTable (no dependency: uc_remote stays
dependency-light) with a full-u64 seq assigned by the submitter, plus the ring
extent, sent flag and not_before deadline the writer thread steers on."
```

---

### Task 5: `Link` — config, dial/HELLO, the two threads (no redirect yet)

**Files:**
- Create: `uc_remote/src/engine.rs` (config, stats, the halves' skeleton)
- Create: `uc_remote/src/link.rs` (shared state, dial scan, writer + reader threads)
- Create: `uc_remote/tests/engine_fake_edge.rs` (the halves' scenario suite; grows in Tasks 6–9)
- Modify: `uc_remote/src/lib.rs` (modules + re-exports)
- Modify: `uc_remote/src/client.rs:97-333` — **delete** the `RemoteConfig`, `RemoteStats` and `RemoteResponse` definitions (they move to `engine.rs` verbatim plus new fields) and `use crate::engine::{RemoteConfig, RemoteResponse, RemoteStats};` instead, so the old client keeps compiling until Task 11.

**Interfaces:**
- Consumes: `OutRing` (Task 2), `CompletionQueue`/`Record`/`OutcomeTag` (Task 3), `SlotTable`/`ReqKind`/`Resolve` (Task 4), `WaitCell` (Task 1), `FramedConn` + `frame::*` (existing).
- Produces:
  ```rust
  // engine.rs
  #[derive(Clone, Debug)] pub struct RemoteConfig { /* today's 9 fields + out_ring_bytes, completion_arena_bytes */ }
  impl RemoteConfig { pub fn validate(&self) -> Result<(), RemoteError>;
                      pub(crate) fn out_ring_bytes_resolved(&self) -> usize;
                      pub(crate) fn arena_bytes_resolved(&self) -> usize; }
  #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub struct RemoteStats { /* today's 9 + socket_writes, frames_written */ }
  #[derive(Clone, Debug, PartialEq, Eq)] pub struct RemoteResponse { pub position: u64, pub bytes: Bytes, pub replayed: bool }
  #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum Consistency { Linearizable, Snapshot }
  #[derive(Debug, thiserror::Error)] pub enum SubmitError { Backpressure, Closed, PayloadTooLarge }
  pub struct RemoteEngine; impl RemoteEngine { pub fn connect(cfg: RemoteConfig) -> Result<(RemoteSendHalf, RemotePollHalf), RemoteError>; }
  pub struct RemoteSendHalf { /* Send, !Sync */ }
  impl RemoteSendHalf { pub fn credits(&self) -> u32; pub fn inflight(&self) -> u64; pub fn stats(&self) -> RemoteStats;
                        pub fn leader(&self) -> Option<(u32, String)>; pub fn client_id(&self) -> u64;
                        pub fn is_connected(&self) -> bool; pub fn connected_addr(&self) -> Option<String>;
                        pub fn shutdown(&self); }
  pub struct RemotePollHalf;
  // link.rs
  pub(crate) struct Link;  // fields listed in the implementation step
  impl Link { pub(crate) fn start(cfg: RemoteConfig) -> Result<Arc<Link>, RemoteError>;
              pub(crate) fn now_ns(&self) -> u64;
              pub(crate) fn complete(&self, r: Record, body: &[u8]);
              pub(crate) fn sweep_deadlines(&self) -> usize;
              pub(crate) fn request_redial(&self, preferred: Option<String>);
              pub(crate) fn close(&self); }
  ```

- [ ] **Step 1: Write the failing test**

Create `uc_remote/tests/engine_fake_edge.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The split client (`RemoteEngine` halves) against the scripted fake edge.
//!
//! This is the port of `client_fake_edge.rs`'s scenario suite onto the halves:
//! the behaviours the old `RemoteClient` owned now live on the writer/reader
//! threads, so they are pinned here. The scripted edge itself is unchanged.

mod common;

use std::time::{Duration, Instant};

use common::fake_edge::{Behaviour, FakeEdge};
use uc_remote::{RemoteConfig, RemoteEngine};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig { app_id: APP.into(), members, ..Default::default() }
}

/// Poll until `pred` holds or `WAIT` elapses, so a test never hangs.
fn until(mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + WAIT;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    pred()
}

#[test]
fn connect_completes_the_handshake_and_adopts_the_granted_credits() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    assert_eq!(send.credits(), 4, "HELLO_OK's grant is the initial window");
    assert_eq!(send.stats().max_credits_seen, 4);
    assert_eq!(send.leader().map(|(id, _)| id), Some(1), "HELLO_OK names the leader");
    assert!(send.is_connected());
    assert_eq!(send.connected_addr(), Some(edge.addr.clone()));
    assert_eq!(edge.observed.hellos.load(std::sync::atomic::Ordering::SeqCst), 1);
    send.shutdown();
}

#[test]
fn an_idle_status_updates_the_window_without_any_traffic_from_us() {
    // The fake edge answers PING with PONG; a STATUS carrying a new grant is
    // what this asserts the reader thread applies.
    let edge = FakeEdge::spawn(Behaviour { credits: 3, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(400),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(send.is_connected(), "PING/PONG must keep an idle connection alive");
    assert_eq!(send.stats().reconnects, 0);
    send.shutdown();
}

#[test]
fn a_dropped_connection_is_re_established_by_the_writer_thread() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        drop_after_first_request: true,
        ..Default::default()
    });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(30),
        dead_after: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    // No request is sent here (that is Task 6); the liveness clock alone must
    // notice the silent edge and re-dial.
    let ok = until(|| send.stats().reconnects >= 1);
    assert!(ok, "dead_after must force a redial: {:?}", send.stats());
    assert!(until(|| send.is_connected()), "the writer thread must re-establish the link");
    send.shutdown();
}

#[test]
fn a_config_that_cannot_work_is_refused_by_name() {
    let bad = |c: RemoteConfig, needle: &str| match RemoteEngine::connect(c) {
        Err(uc_remote::RemoteError::Config(m)) => {
            assert!(m.contains(needle), "message {m:?} must name {needle:?}")
        }
        other => panic!("expected a Config refusal naming {needle:?}, got {other:?}"),
    };
    bad(RemoteConfig { app_id: String::new(), ..cfg(vec!["127.0.0.1:1".into()]) }, "app_id");
    bad(cfg(vec![]), "members");
    bad(RemoteConfig { max_inflight: 0, ..cfg(vec!["127.0.0.1:1".into()]) }, "max_inflight");
    bad(
        RemoteConfig {
            ping_interval: Duration::from_secs(2),
            dead_after: Duration::from_secs(1),
            ..cfg(vec!["127.0.0.1:1".into()])
        },
        "dead_after",
    );
}

#[test]
fn no_reachable_member_is_reported() {
    let e = RemoteEngine::connect(cfg(vec!["127.0.0.1:1".into()])).unwrap_err();
    assert!(matches!(e, uc_remote::RemoteError::NoMembersReachable), "got {e:?}");
}

#[test]
fn hello_refused_is_reported_by_connect() {
    let edge = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_APP_ID),
        ..Default::default()
    });
    let e = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap_err();
    match e {
        uc_remote::RemoteError::HelloRefused { reason, .. } => {
            assert_eq!(reason, uc_remote::frame::HELLO_REFUSED_APP_ID)
        }
        other => panic!("expected HelloRefused, got {other:?}"),
    }
}

#[test]
fn a_faulted_member_is_skipped_and_the_next_one_serves() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let bad = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_FAULTED),
        ..Default::default()
    });
    let (send, _poll) =
        RemoteEngine::connect(cfg(vec![bad.addr.clone(), good.addr.clone()])).unwrap();
    assert_eq!(send.connected_addr(), Some(good.addr.clone()));
    assert!(send.stats().refused_members >= 1);
    send.shutdown();
}

#[test]
fn a_cluster_of_faulted_edges_is_unreachable() {
    let a = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_FAULTED),
        ..Default::default()
    });
    let b = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_BUSY),
        ..Default::default()
    });
    let e = RemoteEngine::connect(cfg(vec![a.addr.clone(), b.addr.clone()])).unwrap_err();
    assert!(matches!(e, uc_remote::RemoteError::NoMembersReachable), "got {e:?}");
}

#[test]
fn halves_have_the_documented_thread_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send::<uc_remote::RemoteSendHalf>();
    assert_send::<uc_remote::RemotePollHalf>();
    assert_send_sync::<uc_remote::RemoteWaitHandle>();
    assert_send_sync::<RemoteConfig>();
    // `RemoteSendHalf` is deliberately NOT `Sync`: one submitter thread owns
    // it. That is enforced structurally by its `PhantomData<Cell<()>>` field,
    // not by an assertion here — a negative trait bound is not expressible in
    // stable Rust, and a test that "checks" it would only ever be a comment.
    // The compile-time proof is that `assert_send_sync::<RemoteSendHalf>()`
    // does not compile; adding it here is how you verify that by hand.
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p uc_remote --test engine_fake_edge`
Expected: FAIL to compile — `unresolved import uc_remote::RemoteEngine`.

- [ ] **Step 3: Write the implementation — `engine.rs`**

Create `uc_remote/src/engine.rs`. Move `RemoteConfig` (with its full doc comments) from `client.rs:97-229`, `RemoteStats` from `client.rs:232-260` and `RemoteResponse` from `client.rs:262-273` verbatim, then add:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The split client: `RemoteEngine::connect` returns a [`RemoteSendHalf`]
//! (one submitter thread, nonblocking) and a [`RemotePollHalf`] (one poller
//! thread), mirroring `uc_client`'s `Engine` over a TCP connection.
//!
//! # The contract
//!
//! An `Ok(())` from [`RemoteSendHalf::try_submit`] obligates the engine to
//! deliver **exactly one** [`RemoteCompletion`] for that `user_data` through
//! [`RemotePollHalf::poll`]. `SubmitError::Backpressure` means the request
//! was never accepted — retry it. Redirects, leader changes, retries and
//! connection loss are absorbed by the link's own threads and are never
//! completions.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::completion::OutcomeTag;
use crate::error::RemoteError;
use crate::link::Link;

// ... RemoteConfig (moved) with these TWO NEW FIELDS appended before `}`:

    /// Bytes reserved for the outgoing frame ring. `None` derives it:
    /// `max_inflight x (HEADER_LEN + 1344)`, floored at `MAX_FRAME_LEN` and
    /// rounded up to a power of two — big enough for a full window of
    /// max-payload commands (the node's 1344-byte ceiling, see
    /// `docs/reference/remote-protocol.md`) and for any single frame this wire
    /// admits. A `try_submit` whose frame does not fit the free space is
    /// `Backpressure`; one that could never fit the whole ring is
    /// `PayloadTooLarge`.
    pub out_ring_bytes: Option<usize>,
    /// Bytes reserved for the completion queue's body arena. `None` derives
    /// it: `max_inflight x 256`, floored at `MAX_FRAME_LEN`, rounded up to a
    /// power of two. The floor is what guarantees any single response body can
    /// be delivered, so a slow poller only ever delays the reader.
    pub completion_arena_bytes: Option<usize>,
```

with `Default` extended by `out_ring_bytes: None, completion_arena_bytes: None,` and these helpers:

```rust
impl RemoteConfig {
    pub(crate) fn out_ring_bytes_resolved(&self) -> usize {
        self.out_ring_bytes.unwrap_or_else(|| {
            let per = crate::frame::HEADER_LEN + 1344;
            (self.max_inflight as usize)
                .saturating_mul(per)
                .max(crate::frame::MAX_FRAME_LEN as usize)
        })
    }

    pub(crate) fn arena_bytes_resolved(&self) -> usize {
        self.completion_arena_bytes.unwrap_or_else(|| {
            (self.max_inflight as usize)
                .saturating_mul(256)
                .max(crate::frame::MAX_FRAME_LEN as usize)
        })
    }
}
```

`RemoteStats` gains two fields (documented as "how many `write_all_bytes` calls the writer made" / "how many frames those calls carried" — their ratio is the batching factor the M13 bench is about):

```rust
    /// `write_all_bytes` calls the writer thread made. `frames_written /
    /// socket_writes` is the batching factor — 1.0 is the old client's
    /// one-write-per-submit behaviour, which is what M13b exists to fix.
    pub socket_writes: u64,
    /// Frames those writes carried, re-sends included.
    pub frames_written: u64,
```

Then the new items:

```rust
/// Read consistency for [`RemoteSendHalf::try_query`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Routed through the node's quorum read-index barrier.
    Linearizable,
    /// Answered from the local replica without a barrier round-trip.
    Snapshot,
}

/// Why a `try_submit`/`try_query` was refused at the door. A refusal means the
/// request was never accepted: no seq was consumed, no completion will come.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubmitError {
    #[error("backpressure: the credit window, the inflight cap or the outgoing ring is full")]
    Backpressure,
    #[error("client is closed")]
    Closed,
    #[error("payload too large for one frame")]
    PayloadTooLarge,
}

/// One resolved request, handed to the callback passed to [`RemotePollHalf::poll`].
pub struct RemoteCompletion<'a> {
    pub user_data: u64,
    /// The log position a command was applied at; `None` for anything but a
    /// `RESPONSE` (and `Some(0)` for a query, which the edge answers with
    /// position 0).
    pub position: Option<u64>,
    pub outcome: RemoteOutcome<'a>,
}

/// What became of a request. Exactly one per accepted `try_submit`/`try_query`.
#[derive(Debug)]
pub enum RemoteOutcome<'a> {
    /// The state machine's answer, borrowed from the completion queue's arena.
    /// `replayed` is the edge's `FLAG_REPLAYED` (the session cache answered);
    /// `expired` is `FLAG_EXPIRED` (the dedup window had moved past this seq,
    /// so the outcome of a write is unknowable) and then `body` is empty.
    Response { body: &'a [u8], replayed: bool, expired: bool },
    /// The edge timed the slot out and `resend_on_unknown` is false.
    Unknown,
    /// The node refused the payload. Never re-sent.
    PayloadTooLarge,
    /// The `request_timeout` budget ran out.
    TimedOut,
    /// The client was shut down with this request outstanding.
    Closed,
}

/// Constructor namespace, like `uc_client::Engine`.
pub struct RemoteEngine;

impl RemoteEngine {
    /// Validate, dial the first reachable member (following a `REDIRECT` at
    /// the handshake and hopping to a leader a `HELLO_OK` names), start the
    /// writer and reader threads, and hand back the two halves.
    ///
    /// The error contract is exactly [`crate::RemoteClient::connect`]'s:
    /// [`RemoteError::Config`] before any socket is opened,
    /// [`RemoteError::HelloRefused`] for a refusal no other member would
    /// answer differently, [`RemoteError::NoMembersReachable`] only after a
    /// full pass.
    pub fn connect(cfg: RemoteConfig) -> Result<(RemoteSendHalf, RemotePollHalf), RemoteError> {
        let link = Link::start(cfg)?;
        Ok((
            RemoteSendHalf {
                link: Arc::clone(&link),
                next_seq: Cell::new(1),
                reclaim_seq: Cell::new(1),
                reclaim_pos: Cell::new(0),
                _not_sync: PhantomData,
            },
            RemotePollHalf { link },
        ))
    }
}

/// The submit side: `&self`, nonblocking, never sleeps, never syscalls.
/// `Send` but **not** `Sync` — one submitter thread owns it (it carries the
/// submitter-local seq and reclaim cursors).
pub struct RemoteSendHalf {
    pub(crate) link: Arc<Link>,
    pub(crate) next_seq: Cell<u64>,
    pub(crate) reclaim_seq: Cell<u64>,
    pub(crate) reclaim_pos: Cell<u64>,
    pub(crate) _not_sync: PhantomData<Cell<()>>,
}

/// The completion side: single owner, `Send`.
pub struct RemotePollHalf {
    pub(crate) link: Arc<Link>,
}

impl RemoteSendHalf {
    /// The last absolute grant the edge advertised.
    pub fn credits(&self) -> u32 {
        self.link.credits.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Requests accepted but not yet completed.
    pub fn inflight(&self) -> u64 {
        self.link.slots.inflight()
    }

    pub fn stats(&self) -> RemoteStats {
        self.link.stats.snapshot()
    }

    /// The leader the current edge last named, if any.
    pub fn leader(&self) -> Option<(u32, String)> {
        self.link.leader()
    }

    /// The identity every frame asserts — the key the edge's session dedup is
    /// per.
    pub fn client_id(&self) -> u64 {
        self.link.client_id
    }

    /// Whether a connection is currently established. `false` is not a
    /// failure: the writer thread is re-dialling and the window will be
    /// re-sent.
    pub fn is_connected(&self) -> bool {
        self.link.is_connected()
    }

    /// The address currently connected to (may be a redirect target that is
    /// not in `members`).
    pub fn connected_addr(&self) -> Option<String> {
        self.link.connected_addr()
    }

    /// Close the link and complete every outstanding request with
    /// [`RemoteOutcome::Closed`]. Idempotent; dropping both halves does the
    /// same.
    pub fn shutdown(&self) {
        self.link.close();
    }
}

impl RemotePollHalf {
    /// Drain up to `POLL_BATCH` completions, invoking `cb` for each; returns
    /// the count. Nonblocking — see [`RemotePollHalf::wait_handle`] to park
    /// between batches.
    pub fn poll(&mut self, cb: impl FnMut(RemoteCompletion<'_>)) -> usize {
        crate::link::drain_completions(&self.link, cb)
    }

    /// A handle a poller thread can park on until something completes.
    pub fn wait_handle(&self) -> RemoteWaitHandle {
        RemoteWaitHandle { link: Arc::clone(&self.link) }
    }

    pub fn stats(&self) -> RemoteStats {
        self.link.stats.snapshot()
    }
}

/// Park until a completion is available. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct RemoteWaitHandle {
    pub(crate) link: Arc<Link>,
}

impl RemoteWaitHandle {
    /// Park for at most `timeout`. Returns immediately if a completion is
    /// already queued or if one is published between the check and the park.
    pub fn park(&self, timeout: Duration) {
        let observed = self.link.completions.ready().seq();
        if !self.link.completions.is_empty() || self.link.closed() {
            return;
        }
        self.link.completions.ready().park(observed, timeout);
    }

    /// Wake every parked poller (used by a caller's own shutdown path).
    pub fn wake(&self) {
        self.link.completions.publish();
    }
}

impl Drop for RemotePollHalf {
    fn drop(&mut self) {
        self.link.close();
    }
}

impl Drop for RemoteSendHalf {
    fn drop(&mut self) {
        self.link.close();
    }
}

/// Map a queue record's tag back to the public outcome.
pub(crate) fn outcome_of<'a>(tag: OutcomeTag, body: &'a [u8], replayed: bool, expired: bool) -> RemoteOutcome<'a> {
    match tag {
        OutcomeTag::Response => RemoteOutcome::Response { body, replayed, expired },
        OutcomeTag::Unknown => RemoteOutcome::Unknown,
        OutcomeTag::PayloadTooLarge => RemoteOutcome::PayloadTooLarge,
        OutcomeTag::TimedOut => RemoteOutcome::TimedOut,
        OutcomeTag::Closed => RemoteOutcome::Closed,
    }
}
```

Note `RemoteResponse` stays exactly as it is today (`position`, `bytes: Bytes`, `replayed`) — the convenience client (Task 10) builds it.

- [ ] **Step 4: Write the implementation — `link.rs`**

Create `uc_remote/src/link.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! One connection's shared state and its two threads.
//!
//! # Shape (design spec 2026-08-24 §3.2)
//!
//! - **Submitter** (the caller's thread, in `engine.rs`): checks the window
//!   from atomics, encodes into [`OutRing`], records the slot. No syscall, no
//!   lock.
//! - **Writer thread**: drains the ring with ONE `write_all_bytes` per drain
//!   (flush-on-empty, no timer), owns the socket for dial/redial, re-sends the
//!   live window after a redial, sends `PING`, and drains the tiny control
//!   buffer (a `PONG` the reader queued).
//! - **Reader thread**: `read_frame_buffered` + `next_buffered`, updates
//!   `credits`/`acked_seq`, resolves slots, pushes completions, and wakes the
//!   poller ONCE per read batch.
//!
//! The only lock either thread takes per frame is none. `reconnect` (the
//! redial request + read-half handoff), `control` (PONG bytes) and
//! `retransmit` (seqs a RETRY asked for again) are cold-path mutexes.

use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::completion::{CompletionQueue, OutcomeTag, Record};
use crate::conn::FramedConn;
use crate::engine::{outcome_of, RemoteCompletion, RemoteConfig, RemoteStats};
use crate::error::RemoteError;
use crate::frame::{
    encode_frame, FrameType, Header, Hello, HelloOk, HelloRefused, Leader, PROTOCOL_VERSION,
    HELLO_REFUSED_BUSY, HELLO_REFUSED_FAULTED,
};
use crate::outgoing::OutRing;
use crate::slots::SlotTable;

/// The reader's tick: how often it sweeps `request_timeout`, notices
/// `shutdown` and re-checks liveness. Also the socket read timeout.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_millis(25);
/// Socket write timeout — the writer thread owns the socket alone, so this
/// only bounds a wedged peer, it never freezes a submitter.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// The writer's park bound when the ring is empty: short enough that a
/// `not_before` backoff and the `PING` clock stay accurate.
const WRITER_PARK: Duration = Duration::from_millis(5);
/// A `RETRY` hint is honoured, but never for longer than this.
const MAX_RETRY_SLEEP: Duration = Duration::from_secs(1);
/// A `RETRY{retry_after_us: 0}` still backs off this much.
const MIN_RETRY_SLEEP: Duration = Duration::from_micros(100);
/// Backoff for a request an edge redirected to itself.
const SELF_REDIRECT_BACKOFF: Duration = Duration::from_millis(10);
const RECONNECT_BACKOFF_START: Duration = Duration::from_millis(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// Hops followed during one connect scan.
const MAX_REDIRECT_HOPS: usize = 8;
/// Completions handed out per `poll` call — a bounded duty cycle.
const POLL_BATCH: usize = 256;

#[derive(Default)]
pub(crate) struct StatCells {
    pub(crate) redirects: AtomicU64,
    pub(crate) leader_changes: AtomicU64,
    pub(crate) reconnects: AtomicU64,
    pub(crate) resends: AtomicU64,
    pub(crate) retries: AtomicU64,
    pub(crate) unknown: AtomicU64,
    pub(crate) expired: AtomicU64,
    pub(crate) max_credits_seen: AtomicU32,
    pub(crate) refused_members: AtomicU64,
    pub(crate) socket_writes: AtomicU64,
    pub(crate) frames_written: AtomicU64,
}

impl StatCells {
    pub(crate) fn snapshot(&self) -> RemoteStats {
        RemoteStats {
            redirects: self.redirects.load(Ordering::Relaxed),
            leader_changes: self.leader_changes.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            resends: self.resends.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unknown: self.unknown.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            max_credits_seen: self.max_credits_seen.load(Ordering::Relaxed),
            refused_members: self.refused_members.load(Ordering::Relaxed),
            socket_writes: self.socket_writes.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
        }
    }
}

/// The redial request + read-half handoff. The READER asks; the WRITER dials.
struct Reconnect {
    needed: bool,
    preferred: Option<String>,
    /// The read half of the connection the writer just dialled, waiting to be
    /// picked up by the reader.
    read_half: Option<FramedConn>,
    /// Bumped on every successful dial, so the reader can tell a fresh half
    /// from the one it already took.
    epoch: u64,
}

pub(crate) struct Link {
    pub(crate) cfg: RemoteConfig,
    pub(crate) client_id: u64,
    pub(crate) slots: SlotTable,
    pub(crate) out: OutRing,
    pub(crate) completions: CompletionQueue,
    pub(crate) credits: AtomicU32,
    pub(crate) acked_seq: AtomicU64,
    /// The current connection has answered something only a serving edge can
    /// answer. Until then the writer sends ONE frame (probe-before-flush).
    pub(crate) proven: AtomicBool,
    /// The single seq written while unproven; `0` = none.
    pub(crate) probe_seq: AtomicU64,
    pub(crate) stats: StatCells,
    pub(crate) t0: Instant,
    closed: AtomicBool,
    connected: AtomicBool,
    leader: Mutex<Option<(u32, String)>>,
    addr: Mutex<String>,
    member_idx: AtomicUsize,
    reconnect: Mutex<Reconnect>,
    reconnect_cv: Condvar,
    /// Frames the READER needs written (only `PONG`). Cold path.
    control: Mutex<Vec<u8>>,
    /// Seqs a `RETRY`/`UNKNOWN` asked to be written again. Cold path.
    retransmit: Mutex<Vec<u64>>,
    /// A handle on the live socket kept purely so `close` can shut it down and
    /// wake both threads out of their blocking calls.
    sock: Mutex<Option<FramedConn>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    rng: AtomicU64,
}

impl Link {
    pub(crate) fn start(cfg: RemoteConfig) -> Result<Arc<Link>, RemoteError> {
        cfg.validate()?;
        let client_id = cfg.client_id.unwrap_or_else(random_u64);
        let stats = StatCells::default();
        let (conn, info, idx, addr) = dial(&cfg, client_id, None, 0, &stats, None)?;
        let read_half = conn.try_clone()?;
        read_half.set_read_timeout(Some(SWEEP_INTERVAL))?;
        conn.set_read_timeout(Some(SWEEP_INTERVAL))?;
        conn.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let watch = conn.try_clone()?;
        stats.max_credits_seen.store(info.credits, Ordering::Relaxed);

        let link = Arc::new(Link {
            slots: SlotTable::new(cfg.max_inflight),
            out: OutRing::new(cfg.out_ring_bytes_resolved()),
            completions: CompletionQueue::new(
                cfg.max_inflight as usize,
                cfg.arena_bytes_resolved(),
            ),
            credits: AtomicU32::new(info.credits),
            acked_seq: AtomicU64::new(0),
            proven: AtomicBool::new(false),
            probe_seq: AtomicU64::new(0),
            stats,
            t0: Instant::now(),
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(true),
            leader: Mutex::new(info.leader),
            addr: Mutex::new(addr),
            member_idx: AtomicUsize::new(idx),
            reconnect: Mutex::new(Reconnect {
                needed: false,
                preferred: None,
                read_half: None,
                epoch: 0,
            }),
            reconnect_cv: Condvar::new(),
            control: Mutex::new(Vec::new()),
            retransmit: Mutex::new(Vec::new()),
            sock: Mutex::new(Some(watch)),
            threads: Mutex::new(Vec::new()),
            rng: AtomicU64::new(client_id | 1),
            client_id,
            cfg,
        });

        let w = Arc::clone(&link);
        let writer = std::thread::Builder::new()
            .name("uc2-remote-tx".into())
            .spawn(move || writer_loop(w, conn))?;
        let r = Arc::clone(&link);
        let reader = std::thread::Builder::new()
            .name("uc2-remote-rx".into())
            .spawn(move || reader_loop(r, read_half))?;
        link.threads.lock().unwrap().extend([writer, reader]);
        Ok(link)
    }

    pub(crate) fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }

    pub(crate) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire) && !self.closed()
    }

    pub(crate) fn leader(&self) -> Option<(u32, String)> {
        self.leader.lock().unwrap().clone()
    }

    pub(crate) fn connected_addr(&self) -> Option<String> {
        if self.is_connected() { Some(self.addr.lock().unwrap().clone()) } else { None }
    }

    /// Push one completion, parking (never dropping) while the poller is
    /// behind. Called by the reader thread and by the sweep.
    pub(crate) fn complete(&self, r: Record, body: &[u8]) {
        loop {
            if self.completions.push(r, body) {
                return;
            }
            // The poller is behind. Publish what is queued so it has something
            // to do, then park on its drain signal. Dropping is not an option:
            // every accepted request owes exactly one completion.
            let observed = self.completions.drained().seq();
            self.completions.publish();
            if self.completions.push(r, body) {
                return;
            }
            self.completions.drained().park(observed, Duration::from_millis(1));
        }
    }

    /// Fail every request past its deadline. Runs on the reader's tick AND
    /// between every dial attempt and redirect hop, which is what keeps
    /// `request_timeout` honest while disconnected.
    pub(crate) fn sweep_deadlines(&self) -> usize {
        let now = self.now_ns();
        let mut fired = Vec::new();
        let n = self.slots.sweep(now, |ud| fired.push(ud));
        for ud in fired {
            self.complete(Record::simple(ud, OutcomeTag::TimedOut), &[]);
        }
        if n > 0 {
            self.completions.publish();
        }
        n
    }

    /// Ask the writer thread for a fresh connection. Idempotent.
    pub(crate) fn request_redial(&self, preferred: Option<String>) {
        let mut g = self.reconnect.lock().unwrap();
        g.needed = true;
        if preferred.is_some() {
            g.preferred = preferred;
        }
        self.connected.store(false, Ordering::Release);
        drop(g);
        self.reconnect_cv.notify_all();
        self.out.wake().signal();
        // Wake a reader parked in a blocking read on the doomed socket.
        if let Some(c) = self.sock.lock().unwrap().as_ref() {
            c.shutdown();
        }
    }

    fn redial_needed(&self) -> bool {
        self.reconnect.lock().unwrap().needed
    }

    /// Queue a frame the reader needs written (a `PONG`).
    pub(crate) fn queue_control(&self, h: Header, payload: &[u8]) {
        let mut g = self.control.lock().unwrap();
        encode_frame(&mut g, h, payload);
        drop(g);
        self.out.wake().signal();
    }

    /// Ask the writer to write `seq`'s frame again, not before `delay`.
    pub(crate) fn queue_retransmit(&self, seq: u64, delay: Duration) {
        self.slots.mark_sent(seq, false);
        self.slots.set_not_before(seq, self.now_ns() + delay.as_nanos() as u64);
        let mut g = self.retransmit.lock().unwrap();
        if !g.contains(&seq) {
            g.push(seq);
        }
        drop(g);
        self.out.wake().signal();
    }

    /// A backoff of `base` plus up to 25% jitter, floored and capped.
    pub(crate) fn jittered(&self, base: Duration) -> Duration {
        let base = base.clamp(MIN_RETRY_SLEEP, MAX_RETRY_SLEEP);
        let span = (base.as_micros() as u64 / 4).max(1);
        base + Duration::from_micros(self.next_rand() % span)
    }

    fn next_rand(&self) -> u64 {
        let mut x = self.rng.load(Ordering::Relaxed);
        if x == 0 {
            x = 0x9E37_79B9_7F4A_7C15;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.store(x, Ordering::Relaxed);
        x
    }

    /// Close the link, complete every outstanding request with `Closed`, and
    /// join both threads. Idempotent, and safe from either half's `Drop`.
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.connected.store(false, Ordering::Release);
        if let Some(c) = self.sock.lock().unwrap().take() {
            c.shutdown();
        }
        self.out.wake().signal();
        self.completions.drained().signal();
        self.reconnect_cv.notify_all();
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.threads.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
        let mut aborted = Vec::new();
        self.slots.drain_abort(|ud| aborted.push(ud));
        for ud in aborted {
            self.complete(Record::simple(ud, OutcomeTag::Closed), &[]);
        }
        self.completions.publish();
    }
}

/// `poll`'s body, here rather than in `engine.rs` so the queue's record shape
/// stays private to the link layer.
pub(crate) fn drain_completions(
    link: &Arc<Link>,
    mut cb: impl FnMut(RemoteCompletion<'_>),
) -> usize {
    link.completions.drain(POLL_BATCH, |rec, body| {
        cb(RemoteCompletion {
            user_data: rec.user_data,
            position: if rec.has_position { Some(rec.position) } else { None },
            outcome: outcome_of(rec.tag, body, rec.replayed, rec.expired),
        })
    })
}
```

Then the two thread bodies and the dial scan, in the same file:

```rust
// ------------------------------------------------------------------ writer

fn writer_loop(link: Arc<Link>, mut conn: FramedConn) {
    let mut scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut last_write = Instant::now();
    loop {
        if link.closed() {
            return;
        }
        if link.redial_needed() {
            match redial(&link, &mut conn) {
                true => {
                    last_write = Instant::now();
                    continue;
                }
                false => return,
            }
        }
        let mut did_work = false;
        // 1) control frames (a PONG the reader queued).
        {
            let mut g = link.control.lock().unwrap();
            if !g.is_empty() {
                scratch.clear();
                std::mem::swap(&mut scratch, &mut g);
                drop(g);
                if conn.write_all_bytes(&scratch).is_err() {
                    link.request_redial(None);
                    continue;
                }
                link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
                did_work = true;
                last_write = Instant::now();
            }
        }
        // 2) the ring drain: everything admissible in ONE write per contiguous
        //    run (two only when a frame straddles the wrap).
        if drain_ring(&link, &mut conn) {
            did_work = true;
            last_write = Instant::now();
        }
        if did_work {
            continue;
        }
        // 3) PING when nothing has been written for `ping_interval`.
        if last_write.elapsed() >= link.cfg.ping_interval {
            let ping = Header {
                ty: FrameType::Ping,
                flags: 0,
                version: PROTOCOL_VERSION,
                client_id: link.client_id,
                seq: 0,
            };
            if conn.write_frame(ping, &[]).is_err() {
                link.request_redial(None);
                continue;
            }
            last_write = Instant::now();
            continue;
        }
        // 4) nothing to do: park on the ring's wake word.
        let observed = link.out.wake().seq();
        if link.out.write_pos() > link.out.send_pos() || link.redial_needed() || link.closed() {
            continue;
        }
        link.out.wake().park(observed, WRITER_PARK);
    }
}

/// Write whatever the ring holds. Returns whether anything went out.
/// TASK 8 extends this with the probe-before-flush limit and the retransmit
/// queue; at this task it drains unconditionally.
fn drain_ring(link: &Arc<Link>, conn: &mut FramedConn) -> bool {
    let limit = link.out.write_pos();
    let mut wrote = false;
    while link.out.send_pos() < limit {
        let chunk = link.out.peek_upto(limit);
        if chunk.is_empty() {
            break;
        }
        let n = chunk.len();
        if conn.write_all_bytes(chunk).is_err() {
            link.request_redial(None);
            return wrote;
        }
        link.out.consume(n);
        link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
        wrote = true;
    }
    wrote
}

/// Dial a fresh connection, publish its read half to the reader, and reset the
/// per-connection flow-control state. `false` = the link is closed, stop.
fn redial(link: &Arc<Link>, conn: &mut FramedConn) -> bool {
    link.stats.reconnects.fetch_add(1, Ordering::Relaxed);
    let mut backoff = RECONNECT_BACKOFF_START;
    loop {
        link.sweep_deadlines();
        if link.closed() {
            return false;
        }
        let preferred = {
            let mut g = link.reconnect.lock().unwrap();
            g.needed = false;
            g.preferred.take()
        };
        let start = link.member_idx.load(Ordering::Relaxed) + 1;
        match dial(&link.cfg, link.client_id, preferred.as_deref(), start, &link.stats, Some(link))
        {
            Ok((fresh, info, idx, addr)) => {
                let Ok(read_half) = fresh.try_clone() else {
                    continue;
                };
                if read_half.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                    || fresh.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                    || fresh.set_write_timeout(Some(WRITE_TIMEOUT)).is_err()
                {
                    continue;
                }
                let Ok(watch) = fresh.try_clone() else { continue };
                if link.closed() {
                    fresh.shutdown();
                    return false;
                }
                link.member_idx.store(idx, Ordering::Relaxed);
                *link.addr.lock().unwrap() = addr;
                if info.leader.is_some() {
                    *link.leader.lock().unwrap() = info.leader;
                }
                link.stats.max_credits_seen.fetch_max(info.credits, Ordering::Relaxed);
                // `credits` resets from HELLO_OK; `acked_seq` is carried
                // across — it only ever moves forward and every live seq is
                // strictly greater than it.
                link.credits.store(info.credits, Ordering::Release);
                link.proven.store(false, Ordering::Release);
                link.probe_seq.store(0, Ordering::Release);
                *link.sock.lock().unwrap() = Some(watch);
                *conn = fresh;
                // TASK 8 inserts the ordered resend of the live window here.
                let mut g = link.reconnect.lock().unwrap();
                g.read_half = Some(read_half);
                g.epoch += 1;
                drop(g);
                link.connected.store(true, Ordering::Release);
                link.reconnect_cv.notify_all();
                return true;
            }
            Err(RemoteError::Closed) => return false,
            Err(RemoteError::HelloRefused { .. }) => {
                // No member would answer differently: fail everything.
                link.close_from_thread();
                return false;
            }
            Err(_) => {
                sleep_sweeping(link, backoff);
                if link.closed() {
                    return false;
                }
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

/// Sleep `total` in `SWEEP_INTERVAL` slices, sweeping between them: a 500 ms
/// backoff must not let a 200 ms request overshoot its budget.
fn sleep_sweeping(link: &Arc<Link>, total: Duration) {
    let end = Instant::now() + total;
    loop {
        link.sweep_deadlines();
        if link.closed() {
            return;
        }
        let now = Instant::now();
        if now >= end {
            return;
        }
        std::thread::sleep((end - now).min(SWEEP_INTERVAL));
    }
}

// ------------------------------------------------------------------ reader

fn reader_loop(link: Arc<Link>, mut rd: FramedConn) {
    let mut last_recv = Instant::now();
    let mut last_sweep = last_recv;
    loop {
        if link.closed() {
            return;
        }
        // `dead_after` is the mid-frame bound as well as the silence bound.
        match rd.read_frame_buffered(link.cfg.dead_after) {
            Ok(Some((h, payload))) => {
                last_recv = Instant::now();
                let mut act = on_frame(&link, h, payload);
                while matches!(act, Act::Continue) {
                    match rd.next_buffered() {
                        Ok(Some((h2, p2))) => act = on_frame(&link, h2, p2),
                        Ok(None) => break,
                        Err(_) => {
                            act = Act::Reconnect(None);
                            break;
                        }
                    }
                }
                // ONE wake for the whole read batch.
                link.completions.publish();
                match act {
                    Act::Continue => {}
                    Act::Stop => return,
                    Act::Reconnect(preferred) => {
                        link.request_redial(preferred);
                        if !await_read_half(&link, &mut rd) {
                            return;
                        }
                        last_recv = Instant::now();
                        last_sweep = last_recv;
                        continue;
                    }
                }
            }
            Ok(None) => {}
            Err(_) => {
                link.request_redial(None);
                if !await_read_half(&link, &mut rd) {
                    return;
                }
                last_recv = Instant::now();
                last_sweep = last_recv;
                continue;
            }
        }
        let now = Instant::now();
        if now.duration_since(last_sweep) >= SWEEP_INTERVAL {
            last_sweep = now;
            link.sweep_deadlines();
        }
        if now.duration_since(last_recv) >= link.cfg.dead_after {
            link.request_redial(None);
            if !await_read_half(&link, &mut rd) {
                return;
            }
            last_recv = Instant::now();
            last_sweep = last_recv;
        }
    }
}

/// Block until the writer thread publishes the read half of a fresh
/// connection. `false` = the link closed while waiting.
fn await_read_half(link: &Arc<Link>, rd: &mut FramedConn) -> bool {
    let mut g = link.reconnect.lock().unwrap();
    loop {
        if link.closed() {
            return false;
        }
        if let Some(fresh) = g.read_half.take() {
            *rd = fresh;
            return true;
        }
        let (guard, _) = link.reconnect_cv.wait_timeout(g, SWEEP_INTERVAL).unwrap();
        g = guard;
        // The sweep has to keep running while we wait, or a disconnected
        // client stops enforcing `request_timeout`.
        drop(g);
        link.sweep_deadlines();
        g = link.reconnect.lock().unwrap();
    }
}

/// What the reader should do after a frame.
pub(crate) enum Act {
    Continue,
    Reconnect(Option<String>),
    Stop,
}

/// TASK 6 adds RESPONSE, TASK 7 the credit plumbing, TASK 8 REDIRECT /
/// LEADER_CHANGED / RETRY, TASK 9 UNKNOWN and HELLO_REFUSED. At this task the
/// reader understands liveness and STATUS only.
fn on_frame(link: &Arc<Link>, h: Header, payload: bytes::Bytes) -> Act {
    match h.ty {
        FrameType::Status => {
            if let Ok(s) = crate::frame::Status::decode(&payload) {
                credit_update(link, s.credits, s.acked_seq);
            }
            Act::Continue
        }
        FrameType::Ping => {
            link.queue_control(
                Header {
                    ty: FrameType::Pong,
                    flags: 0,
                    version: PROTOCOL_VERSION,
                    client_id: link.client_id,
                    seq: h.seq,
                },
                &[],
            );
            Act::Continue
        }
        // A PONG carries no state: having arrived is the point.
        FrameType::Pong => Act::Continue,
        _ => Act::Continue,
    }
}

/// Apply an absolute grant. `credits` MAY decrease; `acked_seq` is monotone.
pub(crate) fn credit_update(link: &Arc<Link>, credits: u32, acked_seq: u64) {
    link.stats.max_credits_seen.fetch_max(credits, Ordering::Relaxed);
    link.credits.store(credits, Ordering::Release);
    link.acked_seq.fetch_max(acked_seq, Ordering::AcqRel);
    // A wider window may have unblocked the writer.
    link.out.wake().signal();
}
```

Finally, port `dial`, `dial_one`, `HelloInfo`, `Dialed`, `addr_or` and `random_u64` from `client.rs:1340-1573` **verbatim**, with three mechanical changes: the `between: Option<&Inner>` parameter becomes `between: Option<&Arc<Link>>`, its body calls `inner.sweep_deadlines()` / `inner.closed()`, and `stats: &Stats` becomes `stats: &StatCells`. Add `Link::close_from_thread` (what `fail_all_and_close` was), which is `close()` minus the self-join:

```rust
impl Link {
    /// `close`, called FROM one of the link's own threads — it must not join
    /// itself, so it only flags, wakes and drains.
    pub(crate) fn close_from_thread(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.connected.store(false, Ordering::Release);
        if let Some(c) = self.sock.lock().unwrap().take() {
            c.shutdown();
        }
        self.out.wake().signal();
        self.reconnect_cv.notify_all();
        let mut aborted = Vec::new();
        self.slots.drain_abort(|ud| aborted.push(ud));
        for ud in aborted {
            self.complete(Record::simple(ud, OutcomeTag::Closed), &[]);
        }
        self.completions.publish();
    }
}
```

and make `Link::close` tolerate being called after `close_from_thread` (the `swap` already does: it then only joins the threads, which is why `close` re-reads `threads` under its own lock).

Update `uc_remote/src/lib.rs`:

```rust
pub mod client;
pub mod completion;   // pub(crate) in effect: `pub(crate) mod completion;`
pub mod conn;
pub mod engine;
pub mod error;
pub mod frame;
pub(crate) mod link;
pub(crate) mod outgoing;
pub(crate) mod park;
pub(crate) mod slots;

pub use client::{RemoteClient, Ticket};
pub use conn::FramedConn;
pub use engine::{
    Consistency, RemoteCompletion, RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf,
    RemoteResponse, RemoteSendHalf, RemoteStats, RemoteWaitHandle, SubmitError,
};
pub use error::{FrameError, RemoteError};
```

(`completion`, `link`, `outgoing`, `park`, `slots` are all `pub(crate)`.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — the 9 new `engine_fake_edge` tests plus the existing 27 `client_fake_edge` tests (the old client still works, it only imports its config types from `engine` now) and the unit tests from Tasks 1–4.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/engine.rs uc_remote/src/link.rs uc_remote/src/lib.rs uc_remote/src/client.rs uc_remote/tests/engine_fake_edge.rs
git commit -m "feat(remote): Link — dial/HELLO, writer and reader threads, halves skeleton

RemoteEngine::connect dials (redirect-at-handshake and leader-hop ported
verbatim), starts a writer thread that owns the socket and a reader thread that
owns frame dispatch, and returns the two halves. Config/stats/response types
move to engine.rs so the old client keeps compiling until it is deleted."
```

---

### Task 6: `try_submit` / `try_query` / `poll` end to end

**Files:**
- Modify: `uc_remote/src/outgoing.rs` — split `push_frame` into `stage_frame` + `commit` (see Step 1's rationale), keep `push_frame` as their composition.
- Modify: `uc_remote/src/engine.rs` — `RemoteSendHalf::{try_submit, try_query, send, reclaim}`.
- Modify: `uc_remote/src/link.rs` — `on_frame`'s `FrameType::Response` arm; `mark_sent` accounting in `drain_ring`.
- Modify: `uc_remote/tests/engine_fake_edge.rs` — port scenario #1 and #7.

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces:
  ```rust
  impl RemoteSendHalf {
      pub fn try_submit(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError>;
      pub fn try_query(&self, user_data: u64, consistency: Consistency, q: &[u8]) -> Result<(), SubmitError>;
  }
  impl OutRing {
      pub(crate) fn stage_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)>; // copies, does NOT publish
      pub(crate) fn commit(&self, len: u32);                                            // publishes + wakes
  }
  ```

- [ ] **Step 1: Write the failing tests**

Add to `uc_remote/src/outgoing.rs`'s test module:

```rust
    #[test]
    fn staged_bytes_are_invisible_until_commit() {
        let r = OutRing::new(4096);
        let (off, len) = r.stage_frame(hdr(1), b"abcd").expect("room");
        assert_eq!(off, 0);
        assert_eq!(r.write_pos(), 0, "a staged frame must not be visible to the writer");
        assert!(r.peek_upto(u64::MAX).is_empty());
        r.commit(len);
        assert_eq!(r.write_pos(), len as u64);
        assert_eq!(r.peek_upto(u64::MAX).len(), len as usize);
    }
```

Add to `uc_remote/tests/engine_fake_edge.rs` (ports of `client_fake_edge.rs`'s `submit_pipelined_under_credits`, **23–50**, and `query_round_trips_and_carries_the_linearizable_flag`, **150–160**):

```rust
use std::sync::atomic::Ordering as AtomicOrdering;
use uc_remote::{Consistency, RemoteOutcome, SubmitError};

/// Drive `n` requests through the halves, returning `(user_data, position,
/// body, replayed)` in completion order. Panics on any non-`Response`
/// outcome, so a test that expects responses says so once, here.
fn run_submits(
    send: &uc_remote::RemoteSendHalf,
    poll: &mut uc_remote::RemotePollHalf,
    n: u64,
    payload: impl Fn(u64) -> Vec<u8>,
) -> Vec<(u64, u64, Vec<u8>, bool)> {
    let mut got = Vec::new();
    let mut issued = 0u64;
    let deadline = Instant::now() + WAIT;
    while (got.len() as u64) < n && Instant::now() < deadline {
        if issued < n {
            match send.try_submit(issued, &payload(issued)) {
                Ok(()) => issued += 1,
                Err(SubmitError::Backpressure) => std::thread::yield_now(),
                Err(e) => panic!("try_submit({issued}): {e}"),
            }
        }
        poll.poll(|c| match c.outcome {
            RemoteOutcome::Response { body, replayed, expired } => {
                assert!(!expired, "unexpected EXPIRED for {}", c.user_data);
                got.push((c.user_data, c.position.unwrap_or(0), body.to_vec(), replayed));
            }
            other => panic!("unexpected outcome for {}: {other:?}", c.user_data),
        });
    }
    got
}

#[test]
fn try_submit_pipelines_under_credits() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 6, |i| vec![i as u8]);
    assert_eq!(got.len(), 6, "every accepted request must complete exactly once");
    for (i, (ud, pos, body, replayed)) in got.iter().enumerate() {
        let i = i as u64;
        assert_eq!(*ud, i, "completions arrive in issue order under one connection");
        assert_eq!(*pos, (i + 1) * 64, "the edge's position rides the completion");
        assert_eq!(body.as_slice(), &[i as u8], "the fake edge reverses the payload");
        assert!(!replayed, "a first-time seq is FRESH");
    }
    let peak = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert!((1..=2).contains(&peak), "the credit window must pace the pipeline: peak {peak}");
    assert_eq!(edge.observed.seq_order(), vec![1, 2, 3, 4, 5, 6], "seqs start at 1, gap-free");
    assert_eq!(send.inflight(), 0, "the window is empty once everything completed");
    // The whole point of M13b: many frames per socket write once the window is
    // wide enough to hold more than one.
    assert!(send.stats().frames_written >= 6);
    send.shutdown();
}

#[test]
fn try_query_round_trips_both_consistencies() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    for (i, c) in [Consistency::Linearizable, Consistency::Snapshot].into_iter().enumerate() {
        send.try_query(i as u64, c, b"abc").unwrap();
        let mut body = None;
        let deadline = Instant::now() + WAIT;
        while body.is_none() && Instant::now() < deadline {
            poll.poll(|comp| {
                if let RemoteOutcome::Response { body: b, .. } = comp.outcome {
                    assert_eq!(comp.user_data, i as u64);
                    body = Some(b.to_vec());
                }
            });
        }
        assert_eq!(body.expect("query answered").as_slice(), b"cba", "{c:?}");
    }
    send.shutdown();
}

#[test]
fn a_payload_larger_than_the_ring_is_refused_at_the_door() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        out_ring_bytes: Some(8192),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let too_big = vec![0u8; 16 * 1024];
    assert_eq!(send.try_submit(1, &too_big), Err(SubmitError::PayloadTooLarge));
    // ... and a merely large one still goes on the wire: the node, not the
    // client, is the authority on `max_payload` (see roundtrip.rs).
    assert!(send.try_submit(2, &vec![0u8; 4096]).is_ok());
    send.shutdown();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib outgoing && cargo test -p uc_remote --test engine_fake_edge`
Expected: FAIL to compile — `no method named 'stage_frame'`, `no method named 'try_submit'`.

- [ ] **Step 3: Split the ring's push in two**

In `uc_remote/src/outgoing.rs`, replace `push_frame`'s body and add the pair. The reason the copy and the publish must be separable: the writer thread may send a frame the instant `write` moves, so the **slot must be published before the bytes are visible** — otherwise a response can arrive for a seq whose slot does not exist yet and the completion is lost.

```rust
    /// Copy a frame in at the write cursor **without publishing it**. The
    /// caller publishes with [`OutRing::commit`] once the slot exists — a
    /// response for an unpublished slot would be a lost completion.
    /// PRODUCER ONLY.
    pub(crate) fn stage_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)> {
        let need = HEADER_LEN + payload.len();
        if need > self.capacity() || need > self.free() {
            return None;
        }
        let start = self.write.load(Ordering::Relaxed);
        let hdr = encode_header_into(h, payload.len());
        // SAFETY: as `push_frame` — the producer owns `[write, ack + capacity)`.
        let buf = unsafe { &mut *self.buf.get() };
        let mut pos = start;
        for src in [&hdr[..], payload] {
            let mut done = 0usize;
            while done < src.len() {
                let idx = (pos as usize) & self.mask;
                let n = (src.len() - done).min(self.capacity() - idx);
                buf[idx..idx + n].copy_from_slice(&src[done..done + n]);
                done += n;
                pos += n as u64;
            }
        }
        Some((start, need as u32))
    }

    /// Publish `len` staged bytes and wake the writer. PRODUCER ONLY.
    pub(crate) fn commit(&self, len: u32) {
        let w = self.write.load(Ordering::Relaxed);
        self.write.store(w + len as u64, Ordering::Release);
        self.wake.signal();
    }

    /// Stage + commit in one call — used by the tests and by any caller that
    /// has no slot to publish first.
    pub(crate) fn push_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)> {
        let (off, len) = self.stage_frame(h, payload)?;
        self.commit(len);
        Some((off, len))
    }
```

- [ ] **Step 4: Implement the submit path**

Add to `uc_remote/src/engine.rs`:

```rust
impl RemoteSendHalf {
    /// Reclaim the ring bytes of every completed request at the head of the
    /// window. Keyed on slot completion, NOT on `acked_seq`: the edge advances
    /// `acked_seq` on SUBMIT only (`uc_gateway/src/conn.rs:309`), so it is not
    /// a contiguous prefix and cannot drive reclaim.
    fn reclaim(&self) {
        let link = &self.link;
        let next = self.next_seq.get();
        let mut seq = self.reclaim_seq.get();
        let mut pos = self.reclaim_pos.get();
        while seq < next && !link.slots.is_live(seq) {
            let (off, len) = link.slots.extent(seq);
            pos = pos.max(off + len as u64);
            seq += 1;
        }
        self.reclaim_seq.set(seq);
        self.reclaim_pos.set(pos);
        link.out.release_to(pos);
    }

    fn send(
        &self,
        ty: crate::frame::FrameType,
        flags: u8,
        kind: crate::slots::ReqKind,
        user_data: u64,
        bytes: &[u8],
    ) -> Result<(), SubmitError> {
        use crate::frame::{Header, HEADER_LEN, MAX_FRAME_LEN, PROTOCOL_VERSION};
        use std::sync::atomic::Ordering;

        let link = &self.link;
        if link.closed() {
            return Err(SubmitError::Closed);
        }
        self.reclaim();
        let need = HEADER_LEN + bytes.len();
        if need > MAX_FRAME_LEN as usize || need > link.out.capacity() {
            return Err(SubmitError::PayloadTooLarge);
        }
        let seq = self.next_seq.get();
        // The credit rule, checked before the seq is consumed: the next seq
        // may go only while `seq <= acked_seq + credits`.
        let window = link
            .acked_seq
            .load(Ordering::Acquire)
            .saturating_add(link.credits.load(Ordering::Acquire) as u64);
        if seq > window {
            return Err(SubmitError::Backpressure);
        }
        // Local cap, and the slot this seq will land in must be free. Both are
        // checked BEFORE the ring is touched, so a refusal never consumes a
        // seq and never leaves orphan bytes: the submitter is the only
        // producer, so nothing can invalidate either check under it.
        if link.slots.inflight() >= link.cfg.max_inflight as u64 || !link.slots.is_free(seq) {
            return Err(SubmitError::Backpressure);
        }
        let h = Header { ty, flags, version: PROTOCOL_VERSION, client_id: link.client_id, seq };
        let Some((off, len)) = link.out.stage_frame(h, bytes) else {
            return Err(SubmitError::Backpressure);
        };
        let deadline_ns = link.now_ns() + link.cfg.request_timeout.as_nanos() as u64;
        // Publish the slot BEFORE the bytes: the writer may send the instant
        // `commit` lands, and a response for a slot that does not exist yet
        // would be a lost completion.
        let claimed = link.slots.claim(seq, user_data, kind, deadline_ns, off, len);
        debug_assert!(claimed, "the slot was checked free above and only this thread claims");
        self.next_seq.set(seq + 1);
        link.slots.publish_next_seq(seq + 1);
        link.out.commit(len);
        Ok(())
    }

    /// Submit a command; nonblocking, no syscall, no allocation.
    /// `Ok(())` obligates exactly one completion for `user_data`.
    pub fn try_submit(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError> {
        self.send(
            crate::frame::FrameType::Submit,
            0,
            crate::slots::ReqKind::Submit,
            user_data,
            cmd,
        )
    }

    /// Issue a read; nonblocking. `Consistency::Linearizable` goes through the
    /// node's read barrier, `Snapshot` is answered by the replica the edge
    /// sits on.
    pub fn try_query(
        &self,
        user_data: u64,
        consistency: Consistency,
        q: &[u8],
    ) -> Result<(), SubmitError> {
        let flags = match consistency {
            Consistency::Linearizable => crate::frame::FLAG_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(
            crate::frame::FrameType::Query,
            flags,
            crate::slots::ReqKind::Query,
            user_data,
            q,
        )
    }
}
```

Add `SlotTable::is_free` to `uc_remote/src/slots.rs`:

```rust
    /// SUBMITTER ONLY: is the index this seq lands on unoccupied? Checked
    /// before the ring is touched so a refusal never consumes a seq.
    pub(crate) fn is_free(&self, seq: u64) -> bool {
        self.slot(seq).owner.load(Ordering::Acquire) == FREE
    }
```

In `uc_remote/src/link.rs`, add the `Response` arm to `on_frame` (before the `_ =>` arm):

```rust
        FrameType::Response => {
            let Ok(meta) = crate::frame::ResponseMeta::decode(&payload) else {
                return Act::Continue;
            };
            let body = payload.slice(crate::frame::ResponseMeta::LEN..);
            // Anything answered with a RESPONSE proves this edge is serving
            // us: the window may flush now (probe-before-flush, Task 8).
            link.proven.store(true, Ordering::Release);
            let expired = h.flags & crate::frame::FLAG_EXPIRED != 0;
            if expired {
                link.stats.expired.fetch_add(1, Ordering::Relaxed);
            }
            if let crate::slots::Resolve::Won { user_data } = link.slots.resolve(h.seq) {
                let rec = Record {
                    user_data,
                    position: meta.position,
                    has_position: true,
                    tag: OutcomeTag::Response,
                    replayed: h.flags & crate::frame::FLAG_REPLAYED != 0,
                    expired,
                    body_off: 0,
                    body_len: 0,
                };
                link.complete(rec, &body);
            }
            credit_update(link, meta.credits, meta.acked_seq);
            Act::Continue
        }
```

and make `drain_ring` account for the frames it wrote, by advancing a per-connection frame cursor. Replace `drain_ring`'s body with:

```rust
/// Write whatever the ring holds, one `write_all_bytes` per contiguous run.
/// `cursor` is the seq of the first frame at `send_pos` — advanced here, which
/// is also what marks a slot `sent`.
fn drain_ring(link: &Arc<Link>, conn: &mut FramedConn, cursor: &mut u64) -> bool {
    let limit = flush_limit(link, *cursor);
    let mut wrote = false;
    while link.out.send_pos() < limit {
        let chunk = link.out.peek_upto(limit);
        if chunk.is_empty() {
            break;
        }
        let n = chunk.len();
        if conn.write_all_bytes(chunk).is_err() {
            link.request_redial(None);
            return wrote;
        }
        link.out.consume(n);
        link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
        wrote = true;
        advance_cursor(link, cursor);
    }
    wrote
}

/// Advance the writer's frame cursor to match `send_pos`, marking each frame
/// it passes as sent and counting it.
fn advance_cursor(link: &Arc<Link>, cursor: &mut u64) {
    let sent_to = link.out.send_pos();
    while *cursor < link.slots.next_seq() {
        let (off, len) = link.slots.extent(*cursor);
        if off + len as u64 > sent_to {
            break;
        }
        if link.slots.is_live(*cursor) {
            link.slots.mark_sent(*cursor, true);
            if link.slots.bump_attempts(*cursor) > 1 {
                link.stats.resends.fetch_add(1, Ordering::Relaxed);
            }
        }
        link.stats.frames_written.fetch_add(1, Ordering::Relaxed);
        *cursor += 1;
    }
}

/// How far the writer may flush. TASK 8 replaces this with the
/// probe-before-flush rule; here it is the whole ring.
fn flush_limit(link: &Arc<Link>, _cursor: u64) -> u64 {
    link.out.write_pos()
}
```

and thread `cursor` through `writer_loop` (`let mut cursor = 1u64;` before the loop, `drain_ring(&link, &mut conn, &mut cursor)`); on a successful `redial`, recompute it with `cursor = link.out_cursor_after_redial()` — for now, in Task 5's redial there is no resend, so set `cursor` to the first seq whose extent ends above `send_pos`:

```rust
/// The seq of the frame that starts at `send_pos` — recomputed after a redial.
fn cursor_at_send_pos(link: &Arc<Link>) -> u64 {
    let sent_to = link.out.send_pos();
    let mut seq = 1u64;
    while seq < link.slots.next_seq() {
        let (off, len) = link.slots.extent(seq);
        if off + len as u64 > sent_to {
            break;
        }
        seq += 1;
    }
    seq
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — the four new tests plus everything from Tasks 1–5.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/outgoing.rs uc_remote/src/engine.rs uc_remote/src/slots.rs uc_remote/src/link.rs uc_remote/tests/engine_fake_edge.rs
git commit -m "feat(remote): try_submit/try_query/poll end to end

The submitter stages a frame into the ring, publishes the slot, then commits
the bytes (slot before bytes, or a fast response is a lost completion); the
reader resolves the slot and queues a completion; poll drains it. Ports
client_fake_edge's submit_pipelined_under_credits and the query round trip."
```

---

### Task 7: Credits, `STATUS`, `acked_seq` — the window

**Files:**
- Modify: `uc_remote/tests/common/fake_edge.rs` — one additive `Behaviour` field (`shrink_credits_to`) and its `Action::Status`.
- Modify: `uc_remote/tests/engine_fake_edge.rs` — window scenarios.
- Modify: `uc_remote/src/engine.rs` — extract the window test into `pub(crate) fn admissible` so it can be unit-tested exhaustively.

**Interfaces:**
- Consumes: Task 6's `send`.
- Produces: `pub(crate) fn admissible(seq: u64, acked_seq: u64, credits: u32, inflight: u64, max_inflight: u32) -> bool` in `engine.rs`.

- [ ] **Step 1: Write the failing tests**

Add to `uc_remote/src/engine.rs` a unit-test module:

```rust
#[cfg(test)]
mod window_tests {
    use super::admissible;

    #[test]
    fn the_credit_rule_is_seq_le_acked_plus_credits() {
        // Nothing acked, a grant of 2: seqs 1 and 2 only.
        assert!(admissible(1, 0, 2, 0, 1024));
        assert!(admissible(2, 0, 2, 1, 1024));
        assert!(!admissible(3, 0, 2, 2, 1024));
        // The edge acks 2: the window slides.
        assert!(admissible(3, 2, 2, 0, 1024));
        assert!(admissible(4, 2, 2, 1, 1024));
        assert!(!admissible(5, 2, 2, 2, 1024));
    }

    #[test]
    fn a_reduced_grant_closes_the_window_for_new_seqs_at_once() {
        // 4 were admitted under a grant of 4; the grant drops to 1.
        assert!(!admissible(5, 0, 1, 4, 1024), "an absolute grant MAY decrease");
        // Once the edge acks 4, seq 5 fits the reduced grant again.
        assert!(admissible(5, 4, 1, 0, 1024));
    }

    #[test]
    fn a_zero_grant_admits_nothing() {
        assert!(!admissible(1, 0, 0, 0, 1024));
    }

    #[test]
    fn the_local_cap_binds_even_when_the_edge_is_generous() {
        assert!(admissible(9, 0, 1000, 7, 8));
        assert!(!admissible(9, 0, 1000, 8, 8), "max_inflight is a local cap on top of credits");
    }
}
```

Add to `uc_remote/tests/common/fake_edge.rs`:

```rust
// in `pub struct Behaviour`, after `delay`:
    /// After the FIRST request is answered, also send a standalone `STATUS`
    /// carrying this (lower) absolute grant. This is the wire's §6
    /// clarification — `credits` MAY decrease and a `STATUS` MAY be sent at
    /// any time, not only on the idle timer.
    pub shrink_credits_to: Option<u32>,

// in `impl Default for Behaviour`:
            shrink_credits_to: None,

// in `enum Action`:
    Status { acked_seq: u64, credits: u32 },

// in `serve`'s request loop, beside `let mut used_once = false;`:
    let mut shrunk = false;

// in `serve`, immediately after `q.0.lock().unwrap().push_back(action);`:
                    if let Some(c) = b.shrink_credits_to
                        && !shrunk
                    {
                        shrunk = true;
                        q.0.lock()
                            .unwrap()
                            .push_back(Action::Status { acked_seq: h.seq, credits: c });
                    }

// in `respond`'s `match action`, beside the other arms:
            Action::Status { acked_seq, credits } => {
                Status { acked_seq, credits }.encode(&mut out);
                wr.write_frame(hdr(FrameType::Status, 0, client_id, 0), &out)
            }
```

(add `Status` to the file's `uc_remote::frame::{..}` import list if it is not already there).

Add to `uc_remote/tests/engine_fake_edge.rs`:

```rust
#[test]
fn a_status_carrying_a_lower_grant_is_honoured_for_new_seqs() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 8,
        shrink_credits_to: Some(1),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 12, |i| vec![i as u8]);
    assert_eq!(got.len(), 12);
    // The edge's own high-water mark is the assertion: after the reduction it
    // must never see more than one unanswered request at a time again.
    let peak = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert!(peak <= 8, "the client must never exceed the grant it was given: {peak}");
    assert_eq!(send.credits(), 1, "the last absolute grant seen is the window");
    assert_eq!(send.stats().max_credits_seen, 8, "max_credits_seen is a high-water mark");
    send.shutdown();
}

#[test]
fn a_window_of_one_serializes_the_pipeline() {
    let edge = FakeEdge::spawn(Behaviour { credits: 1, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 5, |i| vec![i as u8]);
    assert_eq!(got.len(), 5);
    assert_eq!(
        edge.observed.max_unanswered.load(AtomicOrdering::SeqCst),
        1,
        "credits: 1 means exactly one unanswered request"
    );
    send.shutdown();
}

#[test]
fn the_local_inflight_cap_binds_below_the_edges_grant() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 64,
        delay: Duration::from_millis(50),
        ..Default::default()
    });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        max_inflight: 3,
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    for i in 0..3u64 {
        assert!(send.try_submit(i, b"x").is_ok(), "request {i} fits the local cap");
    }
    assert_eq!(send.try_submit(3, b"x"), Err(SubmitError::Backpressure));
    assert_eq!(send.inflight(), 3);
    send.shutdown();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --lib window_tests && cargo test -p uc_remote --test engine_fake_edge`
Expected: FAIL — `cannot find function 'admissible'`; `no field 'shrink_credits_to'`.

- [ ] **Step 3: Write the implementation**

In `uc_remote/src/engine.rs`, extract the rule and call it from `send`:

```rust
/// The whole admission rule, in one place so it can be tested exhaustively:
/// the edge's absolute grant (`seq <= acked_seq + credits`, where `credits`
/// MAY decrease and `acked_seq` only moves forward) AND the caller's local
/// `max_inflight` cap.
pub(crate) fn admissible(
    seq: u64,
    acked_seq: u64,
    credits: u32,
    inflight: u64,
    max_inflight: u32,
) -> bool {
    seq <= acked_seq.saturating_add(credits as u64) && inflight < max_inflight as u64
}
```

and in `send`, replace the two separate checks with:

```rust
        if !admissible(
            seq,
            link.acked_seq.load(Ordering::Acquire),
            link.credits.load(Ordering::Acquire),
            link.slots.inflight(),
            link.cfg.max_inflight,
        ) || !link.slots.is_free(seq)
        {
            return Err(SubmitError::Backpressure);
        }
```

`credit_update` in `link.rs` already stores the absolute grant and `fetch_max`es `acked_seq` (Task 5) — no change is needed there; this task proves it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 4 unit tests + 3 integration tests added.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/engine.rs uc_remote/tests/engine_fake_edge.rs uc_remote/tests/common/fake_edge.rs
git commit -m "feat(remote): the admission window, tested exhaustively

One `admissible` function for the edge's absolute grant and the local inflight
cap, unit-tested across the interesting corners, plus fake-edge scenarios for a
grant that shrinks mid-stream (the protocol's §6 clarification) and a window of
one."
```

---

### Task 8: Failover — REDIRECT, LEADER_CHANGED, RETRY, the not-serving latch, probe-before-flush, resend on redial

**Files:**
- Modify: `uc_remote/src/link.rs` — `on_frame`'s failover arms; `flush_limit`/`drain_ring` probe rule; the writer's `pending` resend list; `redial`'s resend scan.
- Modify: `uc_remote/src/engine.rs` — publish `oldest_unreclaimed` from `reclaim`.
- Modify: `uc_remote/tests/engine_fake_edge.rs` — ports of scenarios #2, #3, #4, #5, #11, #12, #13, #15, #23.

**Interfaces:**
- Consumes: Tasks 5–7.
- Produces: `Link::oldest_unreclaimed: AtomicU64`; `link::flush_limit(link, cursor) -> u64`; `link::take_due_resends(link, now_ns) -> Vec<u64>`.

**Ported scenarios (old name in `client_fake_edge.rs` -> new name in `engine_fake_edge.rs`):**

| old (line range) | new |
|---|---|
| `redirect_is_followed_and_pending_resent_in_order` (52–75) | `redirect_is_followed_and_the_window_is_resent_in_order` |
| `retry_is_honoured_with_hint` (79–93) | `retry_is_honoured_in_place_after_its_hint` |
| `retry_not_serving_moves_the_client_rather_than_re_sending_in_place` (101–117) | `retry_not_serving_moves_the_link_rather_than_re_sending_in_place` |
| `connection_loss_resends_unanswered` (119–137) | `connection_loss_resends_the_unanswered_window` |
| `a_fresh_connection_sends_one_probe_before_flushing_its_window` (216–240) | *(same name)* |
| `a_hello_ok_naming_another_leader_is_followed_before_anything_is_sent` (248–270) | *(same name)* |
| `edges_that_name_each_other_as_leader_do_not_ping_pong` (273–294) | *(same name)* |
| `payload_too_large_is_terminal_and_never_resent` (183–196) | *(same name)* |
| `an_edge_that_redirects_to_itself_does_not_wedge_or_spin` (491–523) | *(same name)* |

- [ ] **Step 1: Write the failing tests**

Add to `uc_remote/tests/engine_fake_edge.rs`:

```rust
#[test]
fn redirect_is_followed_and_the_window_is_resent_in_order() {
    let b = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let a = FakeEdge::spawn(Behaviour {
        credits: 4,
        redirect_all_to: Some(b.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![a.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 3, |i| vec![i as u8]);
    assert_eq!(got.len(), 3);
    for (i, (ud, _, body, _)) in got.iter().enumerate() {
        assert_eq!(*ud, i as u64);
        assert_eq!(body.as_slice(), &[i as u8]);
    }
    assert_eq!(b.observed.seq_order(), vec![1, 2, 3], "re-sent in seq order at the new edge");
    let s = send.stats();
    assert!(s.redirects >= 1, "redirects: {}", s.redirects);
    assert!(s.reconnects >= 1, "reconnects: {}", s.reconnects);
    assert!(s.resends >= 1, "resends: {}", s.resends);
    assert_eq!(send.leader().map(|(id, _)| id), Some(1), "leader from the new edge's HELLO_OK");
    send.shutdown();
}

#[test]
fn retry_is_honoured_in_place_after_its_hint() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, retry_once: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].2.as_slice(), b"cba");
    let s = send.stats();
    assert_eq!(s.retries, 1, "one RETRY honoured");
    assert_eq!(s.reconnects, 0, "a transient RETRY is re-sent in place, not failed over");
    assert_eq!(edge.observed.conns.load(AtomicOrdering::SeqCst), 1, "same connection");
    send.shutdown();
}

#[test]
fn retry_not_serving_moves_the_link_rather_than_re_sending_in_place() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let bad =
        FakeEdge::spawn(Behaviour { credits: 2, not_serving_once: true, ..Default::default() });
    let (send, mut poll) =
        RemoteEngine::connect(cfg(vec![bad.addr.clone(), good.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    let s = send.stats();
    assert_eq!(s.retries, 1);
    assert!(s.reconnects >= 1, "NOT_SERVING is a role statement: go somewhere else");
    send.shutdown();
}

#[test]
fn connection_loss_resends_the_unanswered_window() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        drop_after_first_request: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1, "the request survives the connection that dropped it");
    assert_eq!(got[0].2.as_slice(), b"cba");
    assert_eq!(edge.observed.conns.load(AtomicOrdering::SeqCst), 2, "exactly one reconnect");
    assert_eq!(edge.observed.seq_order(), vec![1], "one logical request, re-sent");
    let s = send.stats();
    assert!(s.reconnects >= 1 && s.resends >= 1, "{s:?}");
    send.shutdown();
}

#[test]
fn a_fresh_connection_sends_one_probe_before_flushing_its_window() {
    let leader = FakeEdge::spawn(Behaviour { credits: 64, ..Default::default() });
    let wrong = FakeEdge::spawn(Behaviour {
        credits: 64,
        redirect_all_to: Some(leader.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        max_inflight: 64,
        ..cfg(vec![wrong.addr.clone()])
    })
    .unwrap();
    let got = run_submits(&send, &mut poll, 50, |i| vec![i as u8]);
    assert_eq!(got.len(), 50);
    assert_eq!(
        wrong.observed.seq_count(),
        1,
        "an edge that cannot serve costs ONE frame, not the whole window"
    );
    assert_eq!(
        leader.observed.seq_order(),
        (1..=50).collect::<Vec<u64>>(),
        "the window lands at the leader, in order"
    );
    send.shutdown();
}

#[test]
fn a_hello_ok_naming_another_leader_is_followed_before_anything_is_sent() {
    let leader = FakeEdge::spawn(Behaviour { credits: 8, ..Default::default() });
    let follower = FakeEdge::spawn(Behaviour {
        credits: 8,
        hello_ok_leader_addr: Some(leader.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![follower.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 3, |i| vec![i as u8]);
    assert_eq!(got.len(), 3);
    assert_eq!(follower.observed.hellos.load(AtomicOrdering::SeqCst), 1, "dialled once");
    assert_eq!(follower.observed.seq_count(), 0, "and never sent a request");
    assert_eq!(leader.observed.seq_order(), vec![1, 2, 3]);
    let s = send.stats();
    assert_eq!(s.redirects, 0, "the hop happens at the handshake, not by REDIRECT");
    assert_eq!(s.resends, 0, "nothing was ever sent to the wrong edge");
    send.shutdown();
}

#[test]
fn edges_that_name_each_other_as_leader_do_not_ping_pong() {
    let a = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let b = FakeEdge::spawn(Behaviour {
        credits: 4,
        hello_ok_leader_addr: Some(a.addr.clone()),
        ..Default::default()
    });
    // `a` names `b`, `b` names `a`: the hop budget must settle it.
    let t = Instant::now();
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![b.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    assert!(t.elapsed() < Duration::from_secs(5), "the handshake hop must be bounded");
    send.shutdown();
}

#[test]
fn payload_too_large_is_terminal_and_never_resent() {
    let edge =
        FakeEdge::spawn(Behaviour { credits: 2, payload_too_large_once: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(7, b"abc").unwrap();
    let mut outcome = None;
    let deadline = Instant::now() + WAIT;
    while outcome.is_none() && Instant::now() < deadline {
        poll.poll(|c| {
            assert_eq!(c.user_data, 7);
            outcome = Some(matches!(c.outcome, RemoteOutcome::PayloadTooLarge));
        });
    }
    assert_eq!(outcome, Some(true), "RETRY{{PAYLOAD_TOO_LARGE}} is a terminal outcome");
    assert_eq!(edge.observed.seq_count(), 1, "seen exactly once on the wire");
    assert_eq!(send.stats().resends, 0);
    send.shutdown();
}

#[test]
fn an_edge_that_redirects_to_itself_does_not_wedge_or_spin() {
    let edge =
        FakeEdge::spawn(Behaviour { credits: 4, redirect_to_self: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(500),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    let mut timed_out = false;
    while !timed_out && t.elapsed() < Duration::from_secs(3) {
        poll.poll(|c| timed_out = matches!(c.outcome, RemoteOutcome::TimedOut));
    }
    assert!(timed_out, "an elected-but-not-serving self-redirect must still time out");
    let frames = edge.observed.seq_count();
    assert!((1..200).contains(&frames), "backed off, not spun: {frames} frames");
    let conns = edge.observed.conns.load(AtomicOrdering::SeqCst);
    assert!(conns < 200, "backed off, not spun: {conns} connections");
    send.shutdown();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --test engine_fake_edge`
Expected: FAIL — the redirect/retry tests hang until `WAIT` and then assert (`got.len() == 0`), because `on_frame` still ignores `REDIRECT`/`RETRY`; the probe test sees all 50 frames at the wrong edge.

- [ ] **Step 3: Publish the reclaim cursor**

In `uc_remote/src/link.rs` add the field to `Link` (`pub(crate) oldest_unreclaimed: AtomicU64`, initialised to `1`), and in `engine.rs`'s `reclaim`, after `self.reclaim_seq.set(seq);` add:

```rust
        // The writer thread's resend scan starts here: the oldest seq whose
        // frame bytes are still in the ring.
        link.oldest_unreclaimed.store(seq, Ordering::Release);
```

- [ ] **Step 4: Implement the failover arms**

In `uc_remote/src/link.rs`, replace `on_frame`'s tail (everything after the `Response` arm added in Task 6) with the full dispatch, ported from `client.rs:875-1038`:

```rust
        FrameType::Status => {
            let Ok(s) = crate::frame::Status::decode(&payload) else { return Act::Continue };
            // A STATUS that acknowledges the probe says what a RESPONSE would:
            // this edge took our write. A bare idle STATUS proves only that
            // the edge is alive, which is not the question.
            let p = link.probe_seq.load(Ordering::Acquire);
            if p != 0 && s.acked_seq >= p {
                link.proven.store(true, Ordering::Release);
            }
            credit_update(link, s.credits, s.acked_seq);
            Act::Continue
        }
        FrameType::Retry => {
            let Ok(r) = crate::frame::Retry::decode(&payload) else { return Act::Continue };
            if r.reason == crate::frame::RETRY_PAYLOAD_TOO_LARGE {
                // Terminal: the payload will not get smaller by being sent again.
                if let crate::slots::Resolve::Won { user_data } = link.slots.resolve(h.seq) {
                    link.complete(Record::simple(user_data, OutcomeTag::PayloadTooLarge), &[]);
                }
                return Act::Continue;
            }
            link.stats.retries.fetch_add(1, Ordering::Relaxed);
            let delay = link.jittered(Duration::from_micros(r.retry_after_us as u64));
            let live = link.slots.is_live(h.seq);
            if live {
                link.queue_retransmit(h.seq, delay);
            }
            if r.reason == crate::frame::RETRY_NOT_SERVING {
                // A statement about the edge's ROLE, not a transient shortage,
                // and one that does not expire on this connection: the edge
                // LATCHES a connection it has refused a write on
                // (`uc_gateway`'s `Conn::latch_not_serving`), so re-sending
                // here would be refused for as long as this socket lived. Go
                // somewhere else; the backoff above still paces it.
                let preferred = link
                    .leader()
                    .map(|(_, a)| a)
                    .filter(|a| Some(a.as_str()) != link.connected_addr().as_deref());
                return Act::Reconnect(preferred);
            }
            Act::Continue
        }
        FrameType::Redirect => {
            link.stats.redirects.fetch_add(1, Ordering::Relaxed);
            let Ok(l) = Leader::decode(&payload) else { return Act::Continue };
            if !l.addr.is_empty() {
                *link.leader.lock().unwrap() = Some((l.node_id, l.addr.to_string()));
            }
            if l.addr.is_empty() {
                // Refused, not answered: it must go out again.
                link.queue_retransmit(h.seq, Duration::ZERO);
                return Act::Reconnect(None);
            }
            if Some(l.addr) == link.connected_addr().as_deref() {
                // "Elected but not serving": the edge redirects us to the
                // address we are already on. Re-sending in place cannot work
                // (the not-serving latch), and a FRESH connection to the same
                // address is what changes the answer. The backoff is what
                // stops that becoming a spin.
                link.queue_retransmit(h.seq, SELF_REDIRECT_BACKOFF);
                return Act::Reconnect(Some(l.addr.to_string()));
            }
            link.queue_retransmit(h.seq, Duration::ZERO);
            Act::Reconnect(Some(l.addr.to_string()))
        }
        FrameType::LeaderChanged => {
            link.stats.leader_changes.fetch_add(1, Ordering::Relaxed);
            let Ok(l) = Leader::decode(&payload) else { return Act::Continue };
            if l.addr.is_empty() {
                *link.leader.lock().unwrap() = None;
                return Act::Reconnect(None);
            }
            *link.leader.lock().unwrap() = Some((l.node_id, l.addr.to_string()));
            if Some(l.addr) == link.connected_addr().as_deref() {
                // Already on the new leader's edge: reconnecting would only
                // churn the in-flight window.
                return Act::Continue;
            }
            Act::Reconnect(Some(l.addr.to_string()))
        }
```

(`Link.leader` needs to be `pub(crate)` for the two direct assignments, or add `Link::set_leader(&self, l: Option<(u32, String)>)` — prefer the setter.)

- [ ] **Step 5: Implement probe-before-flush and the resend list**

In `uc_remote/src/link.rs`, replace `flush_limit` and add the resend machinery:

```rust
/// May the writer send MORE than the single probe frame?
///
/// On a connection that has answered nothing (`!proven`: fresh, reconnected or
/// hopped) the writer sends exactly ONE request and waits. The reason is cost,
/// not politeness: an edge that cannot serve answers EVERY submit with a
/// REDIRECT, so flushing a window of N at the wrong member costs N redirect
/// frames the client then discards — thousands per election, measured. A probe
/// costs one frame and one round trip on a connection that is about to be
/// replaced anyway.
fn probe_gate_open(link: &Arc<Link>) -> bool {
    if link.proven.load(Ordering::Acquire) {
        return true;
    }
    let p = link.probe_seq.load(Ordering::Acquire);
    // A probe counts as outstanding only while it is ON THE WIRE: a
    // RETRY/UNKNOWN answer marks it unsent again, and that re-send is the
    // next probe.
    p == 0 || !(link.slots.is_live(p) && link.slots.is_sent(p))
}

/// How far the ring may be flushed this iteration.
fn flush_limit(link: &Arc<Link>, cursor: u64) -> u64 {
    let write_pos = link.out.write_pos();
    if link.proven.load(Ordering::Acquire) {
        return write_pos;
    }
    if !probe_gate_open(link) {
        return link.out.send_pos();
    }
    // Exactly one frame — the one starting at `send_pos`.
    if cursor >= link.slots.next_seq() {
        return write_pos;
    }
    let (off, len) = link.slots.extent(cursor);
    if link.slots.is_live(cursor) {
        link.probe_seq.store(cursor, Ordering::Release);
    }
    (off + len as u64).min(write_pos)
}

/// Seqs due for a re-write, in seq order. Entries whose backoff has not
/// expired stay queued (order is part of the contract, so a later seq must not
/// overtake an earlier one — the writer stops at the first one not yet due).
fn take_due_resends(link: &Arc<Link>, now_ns: u64) -> Vec<u64> {
    let mut g = link.retransmit.lock().unwrap();
    if g.is_empty() {
        return Vec::new();
    }
    g.sort_unstable();
    let mut due = Vec::new();
    while let Some(&seq) = g.first() {
        if !link.slots.is_live(seq) {
            g.remove(0); // resolved or swept out from under us
            continue;
        }
        if link.slots.not_before(seq) > now_ns {
            break;
        }
        due.push(seq);
        g.remove(0);
    }
    due
}

/// Write the queued re-sends. Returns whether anything went out.
fn write_resends(link: &Arc<Link>, conn: &mut FramedConn, scratch: &mut Vec<u8>) -> bool {
    let due = take_due_resends(link, link.now_ns());
    if due.is_empty() {
        return false;
    }
    let mut one = Vec::new();
    let mut batch: Vec<u8> = Vec::new();
    let mut written = false;
    for seq in due {
        if !probe_gate_open(link) {
            // Put it back: the probe has not been answered yet.
            link.retransmit.lock().unwrap().push(seq);
            break;
        }
        let (off, len) = link.slots.extent(seq);
        link.out.copy_range(off, len, &mut one);
        batch.extend_from_slice(&one);
        link.slots.mark_sent(seq, true);
        if link.slots.bump_attempts(seq) > 1 {
            link.stats.resends.fetch_add(1, Ordering::Relaxed);
        }
        link.stats.frames_written.fetch_add(1, Ordering::Relaxed);
        if !link.proven.load(Ordering::Acquire) {
            link.probe_seq.store(seq, Ordering::Release);
            break; // one frame while unproven
        }
        if batch.len() >= 64 * 1024 {
            break;
        }
    }
    if !batch.is_empty() {
        scratch.clear();
        scratch.extend_from_slice(&batch);
        if conn.write_all_bytes(scratch).is_err() {
            link.request_redial(None);
            return false;
        }
        link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
        written = true;
    }
    written
}
```

Call `write_resends` from `writer_loop` between the control drain and `drain_ring`, and set `did_work` from it.

- [ ] **Step 6: Implement the ordered resend on redial**

In `redial`, at the `// TASK 8 inserts the ordered resend of the live window here.` marker, replace it with:

```rust
                // The slot table IS the unacked window: every live seq whose
                // bytes are still in the ring goes out again, in seq order,
                // through the same probe-gated resend queue as a RETRY — so a
                // reconnect flushes ONE frame first, exactly as `pump` did.
                // Frames past the snapshot are left to the normal drain.
                let snapshot = link.out.write_pos();
                let last = link.slots.next_seq();
                let mut requeue = Vec::new();
                for seq in link.oldest_unreclaimed.load(Ordering::Acquire).max(1)..last {
                    if !link.slots.is_live(seq) {
                        continue;
                    }
                    let (off, len) = link.slots.extent(seq);
                    if off + len as u64 > snapshot {
                        break;
                    }
                    link.slots.mark_sent(seq, false);
                    requeue.push(seq);
                }
                {
                    let mut g = link.retransmit.lock().unwrap();
                    g.clear();
                    g.extend(requeue);
                }
                // Everything up to the snapshot is now the resend queue's job;
                // the byte drain resumes above it.
                link.out.set_send_pos(snapshot);
```

and after it set the writer's frame cursor: `*cursor = cursor_at_send_pos(link);` (pass `cursor` into `redial`).

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 9 new scenarios plus everything before them.

- [ ] **Step 8: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/link.rs uc_remote/src/engine.rs uc_remote/tests/engine_fake_edge.rs
git commit -m "feat(remote): failover on the halves — redirect, retry, probe, resend

REDIRECT / LEADER_CHANGED / RETRY (incl. the not-serving latch and the
terminal PAYLOAD_TOO_LARGE), probe-before-flush on an unproven connection, and
the ordered resend of the live window after a redial — all on the writer and
reader threads, with nine of client_fake_edge's scenarios ported."
```

---

### Task 9: Liveness and budgets — `UNKNOWN`, `EXPIRED`, `PING`/`dead_after`, the mid-frame stall, `request_timeout`, `Closed`

**Files:**
- Modify: `uc_remote/src/link.rs` — `on_frame`'s `Unknown` and `HelloRefused` arms.
- Modify: `uc_remote/tests/engine_fake_edge.rs` — ports of scenarios #6, #8, #18, #19, #20, #22, #24, #25, #26, #27 (incl. the `ParkingMember` helper).

**Interfaces:**
- Consumes: Tasks 5–8.
- Produces: no new signatures; completes `RemoteOutcome`'s five variants.

**Ported scenarios:**

| old (line range) | new |
|---|---|
| `expired_surfaces_as_error` (139–148) | `expired_surfaces_as_a_response_flagged_expired` |
| `unknown_is_resolved_by_a_resend_or_surfaces_when_told_not_to` (162–181) | `unknown_is_resolved_by_a_resend_or_surfaces_as_an_outcome` |
| `an_edge_that_stalls_mid_frame_is_declared_dead_and_the_request_fails_over` (391–420) | *(same name)* |
| `shutdown_fails_outstanding_tickets_with_closed` (422–435) | `shutdown_completes_outstanding_requests_with_closed` |
| `a_request_that_is_never_answered_times_out` (437–452) | `a_request_that_is_never_answered_completes_timed_out` |
| `a_silent_edge_is_declared_dead_and_the_request_fails_over` (465–489) | *(same name)* |
| `ping_pong_keeps_an_idle_connection_alive` (525–547) | *(same name)* |
| `request_timeout_is_enforced_through_an_endless_redirect_churn` (566–590) | *(same name)* |
| `request_timeout_and_shutdown_are_prompt_while_every_member_is_unreachable` (597–629) | *(same name)* |
| `a_dial_pass_over_unanswering_members_is_swept_and_interruptible` (688–726) | *(same name)* |

- [ ] **Step 1: Write the failing tests**

Add to `uc_remote/tests/engine_fake_edge.rs`:

```rust
/// Drive one request and return its outcome, discriminated as a small enum so
/// assertions read like the old `Ticket::wait` ones.
#[derive(Debug, PartialEq, Eq)]
enum Got {
    Response { body: Vec<u8>, replayed: bool, expired: bool },
    Unknown,
    PayloadTooLarge,
    TimedOut,
    Closed,
    Nothing,
}

/// `_send` is taken only so a call site reads like the old `Ticket::wait`
/// pairs; the poll half is what actually resolves.
fn one(
    _send: &uc_remote::RemoteSendHalf,
    poll: &mut uc_remote::RemotePollHalf,
    user_data: u64,
    budget: Duration,
) -> Got {
    let deadline = Instant::now() + budget;
    let mut got = Got::Nothing;
    while matches!(got, Got::Nothing) && Instant::now() < deadline {
        poll.poll(|c| {
            if c.user_data != user_data {
                return;
            }
            got = match c.outcome {
                RemoteOutcome::Response { body, replayed, expired } => {
                    Got::Response { body: body.to_vec(), replayed, expired }
                }
                RemoteOutcome::Unknown => Got::Unknown,
                RemoteOutcome::PayloadTooLarge => Got::PayloadTooLarge,
                RemoteOutcome::TimedOut => Got::TimedOut,
                RemoteOutcome::Closed => Got::Closed,
            };
        });
        std::thread::yield_now();
    }
    got
}

#[test]
fn expired_surfaces_as_a_response_flagged_expired() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, expired: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(1, b"abc").unwrap();
    match one(&send, &mut poll, 1, WAIT) {
        Got::Response { expired, .. } => assert!(expired, "FLAG_EXPIRED must reach the caller"),
        other => panic!("expected an EXPIRED response, got {other:?}"),
    }
    assert_eq!(send.stats().expired, 1);
    send.shutdown();
}

#[test]
fn unknown_is_resolved_by_a_resend_or_surfaces_as_an_outcome() {
    // Default: resend_on_unknown = true, so it resolves itself.
    let edge = FakeEdge::spawn(Behaviour { credits: 2, unknown_once: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(1, b"abc").unwrap();
    assert_eq!(
        one(&send, &mut poll, 1, WAIT),
        Got::Response { body: b"cba".to_vec(), replayed: false, expired: false }
    );
    assert_eq!(send.stats().unknown, 1);
    send.shutdown();

    // Told not to resend, it surfaces.
    let edge2 = FakeEdge::spawn(Behaviour { credits: 2, unknown_once: true, ..Default::default() });
    let (send2, mut poll2) = RemoteEngine::connect(RemoteConfig {
        resend_on_unknown: false,
        ..cfg(vec![edge2.addr.clone()])
    })
    .unwrap();
    send2.try_submit(1, b"abc").unwrap();
    assert_eq!(one(&send2, &mut poll2, 1, WAIT), Got::Unknown);
    send2.shutdown();
}

#[test]
fn an_edge_that_stalls_mid_frame_is_declared_dead_and_the_request_fails_over() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let stalling = FakeEdge::spawn(Behaviour {
        credits: 2,
        partial_frame_then_hang: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(300),
        ..cfg(vec![stalling.addr.clone(), good.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    assert_eq!(
        one(&send, &mut poll, 1, Duration::from_secs(5)),
        Got::Response { body: b"cba".to_vec(), replayed: false, expired: false },
        "a peer that vanishes mid-frame must reach the same verdict as one silent between frames"
    );
    assert!(t.elapsed() < Duration::from_secs(5));
    assert!(send.stats().reconnects >= 1);
    send.shutdown();
}

#[test]
fn a_silent_edge_is_declared_dead_and_the_request_fails_over() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let silent = FakeEdge::spawn(Behaviour { credits: 2, hang: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(300),
        ..cfg(vec![silent.addr.clone(), good.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    assert_eq!(
        one(&send, &mut poll, 1, Duration::from_secs(5)),
        Got::Response { body: b"cba".to_vec(), replayed: false, expired: false }
    );
    let s = send.stats();
    assert!(s.reconnects >= 1 && s.resends >= 1, "{s:?}");
    send.shutdown();
}

#[test]
fn ping_pong_keeps_an_idle_connection_alive() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(send.stats().reconnects, 0, "PING/PONG must hold an idle connection open");
    assert_eq!(edge.observed.conns.load(AtomicOrdering::SeqCst), 1);
    assert!(send.is_connected());
    send.try_submit(1, b"abc").unwrap();
    assert_eq!(
        one(&send, &mut poll, 1, WAIT),
        Got::Response { body: b"cba".to_vec(), replayed: false, expired: false }
    );
    send.shutdown();
}

#[test]
fn a_request_that_is_never_answered_completes_timed_out() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, hang: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(200),
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_secs(30),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    assert_eq!(one(&send, &mut poll, 1, Duration::from_secs(3)), Got::TimedOut);
    send.shutdown();
}

#[test]
fn shutdown_completes_outstanding_requests_with_closed() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        delay: Duration::from_secs(30),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(1, b"abc").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    send.shutdown();
    assert_eq!(one(&send, &mut poll, 1, Duration::from_secs(2)), Got::Closed);
    assert_eq!(send.try_submit(2, b"abc"), Err(SubmitError::Closed));
}

#[test]
fn request_timeout_is_enforced_through_an_endless_redirect_churn() {
    // Every dial SUCCEEDS and every request is redirected to an address that
    // is down: the reader never returns to an idle tick, so the sweep has to
    // run inside the dial scan. (M12c Task 3b regression.)
    let dead = "127.0.0.1:1".to_string();
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        redirect_all_to: Some(dead),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    assert_eq!(one(&send, &mut poll, 1, Duration::from_secs(4)), Got::TimedOut);
    assert!(t.elapsed() < Duration::from_secs(2), "budget overshot: {:?}", t.elapsed());
    assert!(send.stats().reconnects >= 1);
    send.shutdown();
}

#[test]
fn request_timeout_and_shutdown_are_prompt_while_every_member_is_unreachable() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let addr = edge.addr.clone();
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(200),
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(200),
        ..cfg(vec![addr])
    })
    .unwrap();
    drop(edge); // the cluster goes away underneath a connected client
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    assert_eq!(one(&send, &mut poll, 1, Duration::from_secs(4)), Got::TimedOut);
    assert!(t.elapsed() < Duration::from_secs(2), "budget overshot: {:?}", t.elapsed());
    send.try_submit(2, b"abc").ok();
    let t2 = Instant::now();
    send.shutdown();
    assert!(t2.elapsed() < Duration::from_secs(2), "shutdown mid-dial must be prompt");
    assert!(matches!(
        one(&send, &mut poll, 2, Duration::from_secs(2)),
        Got::Closed | Got::TimedOut | Got::Nothing
    ));
}

/// A member that accepts a connection and then parks it, so a dial costs a
/// full `connect_timeout` in the HELLO read — deterministic and portable
/// where a SYN blackhole is not.
struct ParkingMember {
    addr: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    acceptor: Option<std::thread::JoinHandle<()>>,
}

impl ParkingMember {
    fn spawn() -> ParkingMember {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        l.set_nonblocking(true).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = std::sync::Arc::clone(&stop);
        let acceptor = std::thread::spawn(move || {
            let mut held = Vec::new();
            while !s.load(AtomicOrdering::SeqCst) {
                if let Ok((sock, _)) = l.accept() {
                    held.push(sock);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        ParkingMember { addr, stop, acceptor: Some(acceptor) }
    }
}

impl Drop for ParkingMember {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::SeqCst);
        if let Some(h) = self.acceptor.take() {
            let _ = h.join();
        }
    }
}

#[test]
fn a_dial_pass_over_unanswering_members_is_swept_and_interruptible() {
    // Four members that accept and stall: ONE pass is 4 x connect_timeout, so
    // sweeping only "between passes" would blow the 500 ms budget.
    let parked: Vec<ParkingMember> = (0..4).map(|_| ParkingMember::spawn()).collect();
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let mut members: Vec<String> = parked.iter().map(|p| p.addr.clone()).collect();
    members.insert(0, edge.addr.clone());
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(500),
        connect_timeout: Duration::from_millis(500),
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(200),
        ..cfg(members)
    })
    .unwrap();
    drop(edge);
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    assert_eq!(one(&send, &mut poll, 1, Duration::from_secs(5)), Got::TimedOut);
    assert!(t.elapsed() < Duration::from_secs(3), "the sweep must run INSIDE a pass: {:?}", t.elapsed());
    let t2 = Instant::now();
    send.shutdown();
    assert!(t2.elapsed() < Duration::from_secs(2), "shutdown mid-pass must be prompt");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --test engine_fake_edge`
Expected: FAIL — `unknown_is_resolved_...` gets `Got::Nothing` (the `Unknown` arm is missing); the rest depend on it and on the `HelloRefused` mid-life arm.

- [ ] **Step 3: Write the implementation**

Add the last two arms to `on_frame` in `uc_remote/src/link.rs`:

```rust
        FrameType::Unknown => {
            link.stats.unknown.fetch_add(1, Ordering::Relaxed);
            if link.cfg.resend_on_unknown {
                if link.slots.is_live(h.seq) {
                    link.queue_retransmit(h.seq, Duration::ZERO);
                }
            } else if let crate::slots::Resolve::Won { user_data } = link.slots.resolve(h.seq) {
                link.complete(Record::simple(user_data, OutcomeTag::Unknown), &[]);
            }
            Act::Continue
        }
        FrameType::HelloRefused => {
            let (reason, _detail) = match HelloRefused::decode(&payload) {
                Ok(r) => (r.reason, r.detail.to_string()),
                Err(_) => (0, String::new()),
            };
            // Same split as the dial path: what the refusal is ABOUT decides
            // who it is terminal for. FAULTED/BUSY are statements about THIS
            // EDGE, so they cost one member; APP_ID/VERSION are about US and no
            // member would answer differently.
            if reason == HELLO_REFUSED_FAULTED || reason == HELLO_REFUSED_BUSY {
                link.stats.refused_members.fetch_add(1, Ordering::Relaxed);
                return Act::Reconnect(None);
            }
            link.close_from_thread();
            Act::Stop
        }
```

Everything else this task tests is already in place (Task 5's sweep-inside-dial, `sleep_sweeping`, `dead_after`, the writer's `PING`, `close`'s `Closed` drain) — these scenarios are what prove it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 10 new scenarios; total `engine_fake_edge` = 27 ports + 3 extra structural tests.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/link.rs uc_remote/tests/engine_fake_edge.rs
git commit -m "feat(remote): UNKNOWN, EXPIRED, liveness and the request_timeout budget

The last two frame arms plus the ten remaining client_fake_edge scenarios:
mid-frame stall, silent edge, PING/PONG, shutdown-to-Closed, the redirect churn
and unreachable-member regressions (M12c Task 3b), and the swept, interruptible
dial pass."
```

---

### Task 10: The blocking convenience client — `RemoteClient` / `Ticket` on the halves (deletes the old client)

**Note on task boundaries:** the spec's sequence lists "the convenience client" and "delete the old client" as two steps, but both are the same file (`client.rs`) and the same set of names (`RemoteClient`, `Ticket`), so they cannot be split without an intermediate commit that does not compile. They are one task.

**Files:**
- Modify (rewrite): `uc_remote/src/client.rs` — the old 1573-line `Inner { state: Mutex<State>, cv, .. }` client is **deleted**; what remains is ~260 lines of convenience over the halves.
- Modify: `uc_remote/src/lib.rs` — final export list.
- Modify (trim): `uc_remote/tests/client_fake_edge.rs` — down to the convenience-layer scenarios; everything else now lives in `engine_fake_edge.rs`.

**Interfaces:**
- Consumes: `RemoteEngine`, `RemoteSendHalf`, `RemotePollHalf`, `RemoteWaitHandle`, `RemoteCompletion`, `RemoteOutcome`, `SubmitError`, `Consistency` (Tasks 5–9).
- Produces:
  ```rust
  pub struct RemoteClient;  // Send + Sync
  impl RemoteClient {
      pub fn connect(cfg: RemoteConfig) -> Result<Self, RemoteError>;
      pub fn submit(&self, cmd: &[u8]) -> Result<Ticket, RemoteError>;
      pub fn query(&self, q: &[u8], consistency: Consistency) -> Result<Ticket, RemoteError>;
      pub fn stats(&self) -> RemoteStats;
      pub fn leader(&self) -> Option<(u32, String)>;
      pub fn is_connected(&self) -> bool;
      pub fn connected_addr(&self) -> Option<String>;
      pub fn client_id(&self) -> u64;
      pub fn shutdown(&self);
  }
  pub struct Ticket;  // Send
  impl Ticket { pub fn wait(self) -> Result<RemoteResponse, RemoteError>;
                pub fn wait_timeout(self, d: Duration) -> Result<RemoteResponse, RemoteError>; }
  ```
  **The one signature change callers see:** `query`'s second argument is `Consistency` instead of `bool` (`true` -> `Consistency::Linearizable`, `false` -> `Consistency::Snapshot`). `shutdown` takes `&self` instead of `self`, which is strictly more permissive.

- [ ] **Step 1: Write the failing tests**

Replace `uc_remote/tests/client_fake_edge.rs` entirely with the convenience-layer suite (the other 19 scenarios now live in `engine_fake_edge.rs`; the header comment says so):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The blocking convenience client against the scripted fake edge.
//!
//! The failover behaviours themselves are pinned in `engine_fake_edge.rs`,
//! where they now live (on the link's writer/reader threads). What is left
//! here is what the CONVENIENCE layer owns: blocking admission, tickets that
//! outlive the credit window, the outcome-to-`RemoteError` mapping, and
//! shutdown.

mod common;

use std::time::{Duration, Instant};

use common::fake_edge::{Behaviour, FakeEdge};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig { app_id: APP.into(), members, ..Default::default() }
}

#[test]
fn submit_and_wait_round_trips() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let r = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"cba");
    assert_eq!(r.position, 64);
    assert!(!r.replayed);
    assert_eq!(client.leader().map(|(id, _)| id), Some(1));
    assert!(client.is_connected());
    assert_eq!(client.connected_addr(), Some(edge.addr.clone()));
    client.shutdown();
}

#[test]
fn tickets_may_outnumber_the_credit_window() {
    // The shape `uc_gateway/tests/credits.rs` and `failover.rs` rely on:
    // issue first, wait second, deeper than the grant. `submit` BLOCKS while
    // the window is closed — that block is the pacing.
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let tickets: Vec<_> = (0..20u8).map(|i| client.submit(&[i]).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let r = t.wait_timeout(WAIT).unwrap();
        assert_eq!(&r.bytes[..], &[i as u8]);
    }
    assert!(edge.observed.max_unanswered.load(std::sync::atomic::Ordering::SeqCst) <= 2);
    client.shutdown();
}

#[test]
fn query_round_trips_with_both_consistencies() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    for c in [Consistency::Linearizable, Consistency::Snapshot] {
        let r = client.query(b"abc", c).unwrap().wait_timeout(WAIT).unwrap();
        assert_eq!(&r.bytes[..], b"cba", "{c:?}");
    }
    client.shutdown();
}

#[test]
fn expired_surfaces_as_error() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, expired: true, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let err = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::Expired), "got {err:?}");
    assert_eq!(client.stats().expired, 1);
    client.shutdown();
}

#[test]
fn unknown_surfaces_when_told_not_to_resend() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, unknown_once: true, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        resend_on_unknown: false,
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let err = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::Unknown), "got {err:?}");
    client.shutdown();
}

#[test]
fn payload_too_large_is_terminal() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        payload_too_large_once: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let err = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::PayloadTooLarge), "got {err:?}");
    assert_eq!(client.stats().resends, 0);
    client.shutdown();
}

#[test]
fn shutdown_fails_outstanding_tickets_with_closed() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        delay: Duration::from_secs(30),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let t = client.submit(b"abc").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.shutdown();
    let err = t.wait_timeout(Duration::from_secs(2)).unwrap_err();
    assert!(matches!(err, RemoteError::Closed), "got {err:?}");
}

#[test]
fn a_request_that_is_never_answered_times_out() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, hang: true, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        request_timeout: Duration::from_millis(200),
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_secs(30),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let t = Instant::now();
    let err = client.submit(b"abc").unwrap().wait_timeout(Duration::from_secs(3)).unwrap_err();
    assert!(matches!(err, RemoteError::TimedOut), "got {err:?}");
    assert!(t.elapsed() < Duration::from_secs(2));
    client.shutdown();
}

#[test]
fn hello_refused_is_reported_and_does_not_connect() {
    let edge = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_APP_ID),
        ..Default::default()
    });
    let err = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap_err();
    match err {
        RemoteError::HelloRefused { reason, .. } => {
            assert_eq!(reason, uc_remote::frame::HELLO_REFUSED_APP_ID)
        }
        other => panic!("expected HelloRefused, got {other:?}"),
    }
}

#[test]
fn client_handle_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<RemoteClient>();
    assert_send_sync::<RemoteConfig>();
    assert_send::<uc_remote::Ticket>();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_remote --test client_fake_edge`
Expected: FAIL to compile — `query` takes a `bool`, `shutdown` takes `self`, `client_fake_edge`'s old helpers are gone.

- [ ] **Step 3: Rewrite `client.rs`**

Replace `uc_remote/src/client.rs` in full:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`RemoteClient`] — the blocking convenience client, layered on the
//! [`crate::RemoteEngine`] halves the way `uc_client::Client` sits on its
//! `Engine`.
//!
//! # What this is for, and what it is not
//!
//! One request, one [`Ticket`], one blocking `wait`: the shape a CLI
//! (`counter-remote`), a crash test worker, or any caller with a handful of
//! outstanding requests wants. It costs an `Arc` allocation and a condvar
//! wake per request, and a mutex across `try_submit` so the handle can be
//! `Sync`. **It is not the path M13's throughput bars measure** — that is
//! [`crate::RemoteSendHalf::try_submit`] plus [`crate::RemotePollHalf::poll`]
//! on the caller's own threads.
//!
//! # The promise (unchanged)
//!
//! Every `submit`/`query` ends in **exactly one** resolution: `Ok(
//! RemoteResponse)`, or `Err` of [`RemoteError::Expired`] /
//! [`RemoteError::Unknown`] / [`RemoteError::PayloadTooLarge`] /
//! [`RemoteError::TimedOut`] / [`RemoteError::Closed`]. `REDIRECT`,
//! `LEADER_CHANGED`, `RETRY` and connection loss are absorbed by the link.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::engine::{
    Consistency, RemoteCompletion, RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf,
    RemoteResponse, RemoteSendHalf, RemoteStats, RemoteWaitHandle, SubmitError,
};
use crate::error::RemoteError;

/// One outstanding request's resolution cell.
struct TicketCore {
    done: Mutex<Option<Result<RemoteResponse, RemoteError>>>,
    cv: Condvar,
}

impl TicketCore {
    fn new() -> TicketCore {
        TicketCore { done: Mutex::new(None), cv: Condvar::new() }
    }

    fn set(&self, r: Result<RemoteResponse, RemoteError>) {
        let mut g = self.done.lock().unwrap();
        if g.is_none() {
            *g = Some(r);
        }
        drop(g);
        self.cv.notify_all();
    }
}

/// A handle on one outstanding request.
pub struct Ticket {
    core: Arc<TicketCore>,
}

impl Ticket {
    /// Block until the request resolves. Only the client's own
    /// `request_timeout` or `shutdown` can end the wait without an answer.
    pub fn wait(self) -> Result<RemoteResponse, RemoteError> {
        let mut g = self.core.done.lock().unwrap();
        loop {
            if let Some(r) = g.take() {
                return r;
            }
            g = self.core.cv.wait(g).unwrap();
        }
    }

    /// Like [`Ticket::wait`] with a caller-side bound. Giving up here abandons
    /// the request: the link still resolves it, the answer is just dropped.
    pub fn wait_timeout(self, d: Duration) -> Result<RemoteResponse, RemoteError> {
        let deadline = Instant::now() + d;
        let mut g = self.core.done.lock().unwrap();
        loop {
            if let Some(r) = g.take() {
                return r;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RemoteError::TimedOut);
            }
            let (guard, _) = self.core.cv.wait_timeout(g, deadline - now).unwrap();
            g = guard;
        }
    }
}

/// A connected remote client. `Send + Sync`; share it behind an `Arc` or a
/// reference — every method takes `&self`.
pub struct RemoteClient {
    send: Mutex<RemoteSendHalf>,
    wait: RemoteWaitHandle,
    stop: Arc<AtomicBool>,
    poller: Mutex<Option<JoinHandle<()>>>,
    request_timeout: Duration,
}

impl RemoteClient {
    /// Connect (see [`RemoteEngine::connect`] for the error contract) and
    /// start this client's own poller thread.
    pub fn connect(cfg: RemoteConfig) -> Result<Self, RemoteError> {
        let request_timeout = cfg.request_timeout;
        let (send, poll) = RemoteEngine::connect(cfg)?;
        let wait = poll.wait_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let poller = {
            let stop = Arc::clone(&stop);
            let wait = wait.clone();
            std::thread::Builder::new()
                .name("uc2-remote-poll".into())
                .spawn(move || poller_loop(poll, stop, wait))?
        };
        Ok(RemoteClient {
            send: Mutex::new(send),
            wait,
            stop,
            poller: Mutex::new(Some(poller)),
            request_timeout,
        })
    }

    /// Submit a command. Blocks while the edge's credits (or `max_inflight`)
    /// are exhausted, and gives up with [`RemoteError::TimedOut`] if the
    /// window never reopens within `request_timeout`.
    ///
    /// Note that the credit wait is a **separate** `request_timeout` budget
    /// from the one the returned [`Ticket`] then spends, so a caller that
    /// blocks the full wait here and then waits out the request can spend
    /// ~2 x `request_timeout` in total.
    pub fn submit(&self, cmd: &[u8]) -> Result<Ticket, RemoteError> {
        self.enqueue(None, cmd)
    }

    /// Ask a question. Same admission accounting as [`RemoteClient::submit`].
    pub fn query(&self, q: &[u8], consistency: Consistency) -> Result<Ticket, RemoteError> {
        self.enqueue(Some(consistency), q)
    }

    fn enqueue(&self, q: Option<Consistency>, bytes: &[u8]) -> Result<Ticket, RemoteError> {
        let core = Arc::new(TicketCore::new());
        // The engine's `user_data` is an owned reference to the ticket; the
        // completion path (or a refusal below) turns it back with exactly one
        // `Arc::from_raw`.
        let user_data = Arc::into_raw(Arc::clone(&core)) as u64;
        let deadline = Instant::now() + self.request_timeout;
        loop {
            let r = {
                let s = self.send.lock().unwrap();
                match q {
                    None => s.try_submit(user_data, bytes),
                    Some(c) => s.try_query(user_data, c, bytes),
                }
            };
            match r {
                Ok(()) => return Ok(Ticket { core }),
                Err(SubmitError::Backpressure) => {
                    if Instant::now() >= deadline {
                        reclaim(user_data);
                        return Err(RemoteError::TimedOut);
                    }
                    // Park on the completion signal: a completion is exactly
                    // what reopens the window.
                    self.wait.park(Duration::from_micros(200));
                }
                Err(SubmitError::Closed) => {
                    reclaim(user_data);
                    return Err(RemoteError::Closed);
                }
                Err(SubmitError::PayloadTooLarge) => {
                    reclaim(user_data);
                    return Err(RemoteError::PayloadTooLarge);
                }
            }
        }
    }

    pub fn stats(&self) -> RemoteStats {
        self.send.lock().unwrap().stats()
    }

    pub fn leader(&self) -> Option<(u32, String)> {
        self.send.lock().unwrap().leader()
    }

    pub fn is_connected(&self) -> bool {
        self.send.lock().unwrap().is_connected()
    }

    pub fn connected_addr(&self) -> Option<String> {
        self.send.lock().unwrap().connected_addr()
    }

    pub fn client_id(&self) -> u64 {
        self.send.lock().unwrap().client_id()
    }

    /// Close the connection and fail every outstanding request with
    /// [`RemoteError::Closed`]. Idempotent; dropping the client does the same.
    pub fn shutdown(&self) {
        self.send.lock().unwrap().shutdown();
        self.stop.store(true, Ordering::Release);
        self.wait.wake();
        let handle = self.poller.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for RemoteClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.send.lock().unwrap();
        f.debug_struct("RemoteClient")
            .field("client_id", &s.client_id())
            .field("addr", &s.connected_addr())
            .field("credits", &s.credits())
            .field("inflight", &s.inflight())
            .finish()
    }
}

/// Give the ticket reference back without resolving it — the request was
/// refused, so no completion will ever carry this `user_data`.
fn reclaim(user_data: u64) {
    // SAFETY: `user_data` came from `Arc::into_raw::<TicketCore>` in
    // `enqueue`, and this is the ONLY path that reclaims a REFUSED request
    // (an accepted one is reclaimed by `resolve`, exactly once).
    drop(unsafe { Arc::from_raw(user_data as *const TicketCore) });
}

fn resolve(c: RemoteCompletion<'_>) {
    // SAFETY: as `reclaim` — one completion per accepted request, so this
    // runs exactly once for this pointer.
    let core = unsafe { Arc::from_raw(c.user_data as *const TicketCore) };
    let r = match c.outcome {
        RemoteOutcome::Response { body, replayed, expired } => {
            if expired {
                Err(RemoteError::Expired)
            } else {
                Ok(RemoteResponse {
                    position: c.position.unwrap_or(0),
                    bytes: Bytes::copy_from_slice(body),
                    replayed,
                })
            }
        }
        RemoteOutcome::Unknown => Err(RemoteError::Unknown),
        RemoteOutcome::PayloadTooLarge => Err(RemoteError::PayloadTooLarge),
        RemoteOutcome::TimedOut => Err(RemoteError::TimedOut),
        RemoteOutcome::Closed => Err(RemoteError::Closed),
    };
    core.set(r);
}

fn poller_loop(mut poll: RemotePollHalf, stop: Arc<AtomicBool>, wait: RemoteWaitHandle) {
    while !stop.load(Ordering::Acquire) {
        if poll.poll(resolve) == 0 {
            wait.park(Duration::from_millis(1));
        }
    }
    // Final drain: `shutdown` completes every outstanding request with
    // `Closed`, and those completions are queued before the threads stop.
    while poll.poll(resolve) > 0 {}
}
```

Set the final `uc_remote/src/lib.rs`:

```rust
pub mod client;
pub mod conn;
pub mod engine;
pub mod error;
pub mod frame;

pub(crate) mod completion;
pub(crate) mod link;
pub(crate) mod outgoing;
pub(crate) mod park;
pub(crate) mod slots;

pub use client::{RemoteClient, Ticket};
pub use conn::FramedConn;
pub use engine::{
    Consistency, RemoteCompletion, RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf,
    RemoteResponse, RemoteSendHalf, RemoteStats, RemoteWaitHandle, SubmitError,
};
pub use error::{FrameError, RemoteError};
```

and update the crate doc's semver paragraph (`lib.rs:9-12`) to name both surfaces:

```rust
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: the
//! **wire format** — remote protocol v1 ([`frame::PROTOCOL_VERSION`]) — plus
//! [`RemoteEngine`]'s halves and the [`RemoteClient`] convenience built on
//! them. The Rust items that encode the wire are not themselves promised; a
//! port re-implements the format, not this API.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_remote && cargo clippy -p uc_remote --all-targets -- -D warnings`
Expected: PASS — 10 convenience scenarios + 30 halves scenarios + the unit tests. `wc -l uc_remote/src/client.rs` is now under 300.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_remote/src/client.rs uc_remote/src/lib.rs uc_remote/tests/client_fake_edge.rs
git commit -m "feat(remote)!: RemoteClient is now a thin blocking layer over the halves

The single-Mutex<State> client (one lock across submit, write and every
received frame; a channel per ticket; ~7 futex per request) is deleted. What
remains is a poller thread and an Arc<TicketCore> per request over
RemoteEngine's halves. BREAKING: query() takes Consistency, not bool;
shutdown() takes &self."
```

---

### Task 11: Caller migration A — `hop_bench remote-load` on the halves

**Files:**
- Modify: `uc_gateway/examples/hop_bench/remote_load.rs` — rebuilt in `engine_load.rs`'s measurement shape (lines 21–23 imports, 35–60 args, 62–164 `run`, 166–250 `measure`).
- Modify: `uc_gateway/examples/hop_bench/main.rs:60-61` — doc string of the `RemoteLoad` arm.
- Verify: `uc_gateway/examples/hop_bench/local.rs:157-190` never passes `--senders` (it does not — it passes `--gateways/--secs/--payload/--inflight/--conns` only), so dropping the flag needs no change there. `bench-infra/scripts/m13_hop_bench.py` likewise does not use `--senders` — confirm with `grep -n senders bench-infra/scripts/m13_hop_bench.py uc_gateway/examples/hop_bench/local.rs` (expect: no output).

**Interfaces:**
- Consumes: `RemoteEngine`, `RemoteSendHalf`, `RemotePollHalf`, `RemoteOutcome`, `SubmitError` (Task 10); `crate::stats::{SendClock, StreamStats, DRAIN_GRACE, report}` (unchanged).
- Produces: nothing other tasks consume.

**Why `--senders` goes:** it existed to prove the old client could not be driven out of its bottleneck (`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`, "Variant: can the existing client be driven harder?" — +12% and flat). With the halves, one submitter thread is the supported shape and `SendHalf` is `!Sync`, so N callers on one connection is no longer expressible. The measurement is per connection; `--conns` remains the knob.

- [ ] **Step 1: Rewrite the driver**

Replace `uc_gateway/examples/hop_bench/remote_load.rs` in full:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-3 driver: N real remote clients, on the `RemoteEngine` halves.
//!
//! Same measurement shape as `engine_load.rs` (the shmem arm), so the two are
//! comparable line for line: one submitter loop calling `try_submit` with the
//! request's index as `user_data`, one poll thread owning the histogram, and
//! latency correlated through `SendClock` — no `Ticket`, no waiter pool, no
//! channel per request.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use uc_remote::{RemoteConfig, RemoteEngine, RemoteOutcome, SubmitError};

use crate::stats::{self, StreamStats};

/// End-to-end budget per request; generous, because a bar run must never
/// report a timeout it caused itself.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(clap::Args)]
pub struct Args {
    /// Comma-separated gateway addresses; the first is dialled first.
    #[arg(long)]
    pub gateways: String,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    /// SUBMIT payload bytes.
    #[arg(long, default_value_t = 64)]
    pub payload: usize,
    /// `RemoteConfig::max_inflight` — the local cap on unanswered requests,
    /// applied on top of the edge's credits.
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    #[arg(long, default_value_t = 1)]
    pub conns: usize,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    if a.conns == 0 {
        anyhow::bail!("remote-load: --conns must be at least 1");
    }
    let members: Vec<String> = a.gateways.split(',').map(|s| s.trim().to_string()).collect();
    let payload = vec![0xABu8; a.payload];
    let t0 = Instant::now();
    let deadline = t0 + Duration::from_secs(a.secs);

    let mut handles = Vec::with_capacity(a.conns);
    for i in 0..a.conns {
        let cfg = RemoteConfig {
            app_id: a.app_id.clone(),
            members: members.clone(),
            client_id: None,
            max_inflight: a.inflight as u32,
            request_timeout: REQUEST_TIMEOUT,
            ..RemoteConfig::default()
        };
        let payload = payload.clone();
        handles.push(
            thread::Builder::new().name(format!("hb-remote-{i}")).spawn(
                move || -> anyhow::Result<StreamStats> { drive_one(i, cfg, payload, t0, deadline) },
            )?,
        );
    }

    let mut merged = StreamStats::new();
    for (i, h) in handles.into_iter().enumerate() {
        let s = h.join().map_err(|_| anyhow::anyhow!("remote conn {i} panicked"))??;
        println!(
            "   conn {i}: sends={} responses={} lost={} responses/s={:.1}",
            s.sends,
            s.responses,
            s.lost,
            s.responses_per_sec()
        );
        merged.merge(&s);
    }
    stats::report(
        "remote",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[("conns", a.conns.to_string())],
    );
    Ok(())
}

fn drive_one(
    idx: usize,
    cfg: RemoteConfig,
    payload: Vec<u8>,
    t0: Instant,
    deadline: Instant,
) -> anyhow::Result<StreamStats> {
    let (send, mut poll) =
        RemoteEngine::connect(cfg).map_err(|e| anyhow::anyhow!("conn {idx}: connect: {e}"))?;

    let clock = Arc::new(stats::SendClock::new(t0));
    let stop = Arc::new(AtomicBool::new(false));
    let resolved = Arc::new(AtomicU64::new(0));

    // Taken BEFORE `poll` moves into the thread, so the submitter can wake a
    // parked poller at the end of the run.
    let wake = poll.wait_handle();
    let poller = thread::Builder::new()
        .name(format!("hb-remote-poll-{idx}"))
        .spawn({
            let clock = Arc::clone(&clock);
            let stop = Arc::clone(&stop);
            let resolved = Arc::clone(&resolved);
            let wait = wake.clone();
            move || {
                let mut s = StreamStats::new();
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            RemoteOutcome::Response { expired, .. } => {
                                if expired {
                                    s.lost += 1;
                                } else {
                                    let now = clock.now_ns();
                                    let _ = s.hist.record(clock.latency_ns(c.user_data, now));
                                    s.responses += 1;
                                    s.last_response_ns = s.last_response_ns.max(now);
                                }
                            }
                            RemoteOutcome::Unknown
                            | RemoteOutcome::PayloadTooLarge
                            | RemoteOutcome::TimedOut
                            | RemoteOutcome::Closed => s.lost += 1,
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    if n == 0 {
                        wait.park(Duration::from_micros(200));
                    }
                }
                s
            }
        })?;

    let mut sent = 0u64;
    while Instant::now() < deadline {
        clock.stamp(sent);
        match send.try_submit(sent, &payload) {
            Ok(()) => sent += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(),
            Err(e) => anyhow::bail!("conn {idx}: try_submit: {e}"),
        }
    }
    let send_window_end_ns = clock.now_ns();

    let drain_deadline = Instant::now() + stats::DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    wake.wake();
    let mut s = poller.join().map_err(|_| anyhow::anyhow!("conn {idx}: poll thread panicked"))?;

    let st = send.stats();
    println!(
        "   conn {idx}: retries={} redirects={} leader_changes={} reconnects={} resends={} \
         unknown={} expired={} refused_members={} max_credits_seen={} \
         socket_writes={} frames_written={} frames_per_write={:.1}",
        st.retries,
        st.redirects,
        st.leader_changes,
        st.reconnects,
        st.resends,
        st.unknown,
        st.expired,
        st.refused_members,
        st.max_credits_seen,
        st.socket_writes,
        st.frames_written,
        st.frames_written as f64 / st.socket_writes.max(1) as f64
    );
    send.shutdown();

    s.sends = sent;
    s.send_window_end_ns = send_window_end_ns;
    s.lost += sent.saturating_sub(resolved.load(Ordering::Relaxed));
    Ok(s)
}
```

- [ ] **Step 2: Build it**

Run: `cargo build -p uc_gateway --examples --release`
Expected: PASS.

- [ ] **Step 3: Smoke it on the dev box (relative numbers only)**

Run, from the repo root:

```bash
cargo build -p uc_gateway --example hop_bench --release
target/release/examples/hop_bench dummy-edge --listen 127.0.0.1:19301 --credits 1024 &
sleep 1
target/release/examples/hop_bench remote-load --gateways 127.0.0.1:19301 --secs 3 --payload 64 --inflight 1024 --conns 1
kill %1
```

Expected: the `RESULT {...}` line reports **at least 1,000,000 resp/s** where the old client measured 50,480 on this box (`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`, "Dev-box smoke", row C). This is a **relative** check on a 4-vCPU box — never a bar (`docs/notes/dev-box-not-a-bench.md`; bars are fleet-only). `frames_per_write` in the per-conn line must be **> 1.0**, which is the batching the old client never got.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_gateway/examples/hop_bench/remote_load.rs uc_gateway/examples/hop_bench/main.rs
git commit -m "perf(hop_bench): remote-load on the RemoteEngine halves

Same shape as the engine-load arm — one submitter loop, one poll thread,
SendClock correlation by user_data — so hop 3 is comparable with hop 1 line for
line. --senders is dropped: it existed to show the old client could not be
driven out of its bottleneck, and SendHalf is !Sync by design."
```

---

### Task 12: Caller migration B — `m12_gate` client-remote on the halves

**Files:**
- Modify: `uc_gateway/examples/m12_gate.rs:72` (import), `:881-909` (in-process gateway arm), `:911-1080` (`run_remote_measurement`), `:1082-1101` (`print_remote_stats`), `:1384-1415` (`run_client_remote_role`).

**Interfaces:**
- Consumes: Task 10's halves.
- Produces: `fn run_remote_measurement(send: &RemoteSendHalf, poll: &mut RemotePollHalf, secs: u64, payload_len: usize) -> ClientStats` and `fn print_remote_stats(send: &RemoteSendHalf)` — same `ClientStats` mapping as today.

- [ ] **Step 1: Change the import (line 72)**

```rust
use uc_remote::{RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf, RemoteSendHalf, SubmitError};
```

- [ ] **Step 2: Rewrite `run_remote_measurement` (lines 911–1080)**

```rust
/// `RemoteEngine`-side measurement core: ONE submitter loop calling
/// `try_submit` under the halves' own credit/inflight gating, and ONE poll
/// thread owning the histogram — the same shape as
/// [`run_client_measurement`]'s `Engine` arm, correlating latency through the
/// `user_data` the completion carries. (The old shape — a `Ticket` per request
/// and a pool of waiter threads — was the client's structure, not the
/// cluster's, and is what M13b removed.)
fn run_remote_measurement(
    send: &RemoteSendHalf,
    poll: &mut RemotePollHalf,
    secs: u64,
    payload_len: usize,
) -> ClientStats {
    let cmd_bytes = bincode::serde::encode_to_vec(
        &vec![0xABu8; payload_len],
        bincode::config::standard(),
    )
    .expect("encode payload");

    let t0 = Instant::now();
    let send_ns: Arc<Box<[AtomicU64]>> =
        Arc::new((0..SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice());
    let responses = Arc::new(AtomicU64::new(0));
    let resolved = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));

    let mut hist = new_hist();
    let deadline = t0 + Duration::from_secs(secs);
    let mut sends = 0u64;
    // The poll loop runs on THIS thread between submits: `m12_gate`'s remote
    // arm is single-connection and the submitter is the only caller, so a
    // second thread would only add a hand-off. `poll` is nonblocking.
    while Instant::now() < deadline {
        send_ns[(sends as usize) & SLOT_MASK]
            .store(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        match send.try_submit(sends, &cmd_bytes) {
            Ok(()) => sends += 1,
            Err(SubmitError::Backpressure) => {}
            Err(e) => panic!("try_submit: {e}"),
        }
        drain_remote(
            poll,
            &send_ns,
            &mut hist,
            t0,
            &responses,
            &resolved,
            &lost,
            &last_response_ns,
        );
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while resolved.load(Ordering::Relaxed) < sends && Instant::now() < drain_deadline {
        drain_remote(
            poll,
            &send_ns,
            &mut hist,
            t0,
            &responses,
            &resolved,
            &lost,
            &last_response_ns,
        );
    }

    let rs = send.stats();
    ClientStats {
        sends,
        responses: responses.load(Ordering::Relaxed),
        lost: lost.load(Ordering::Relaxed) + sends.saturating_sub(resolved.load(Ordering::Relaxed)),
        not_leader: rs.redirects,
        retried: rs.retries,
        duplicates: rs.resends,
        send_window_end_ns,
        last_response_ns: last_response_ns.load(Ordering::Relaxed),
        hist,
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_remote(
    poll: &mut RemotePollHalf,
    send_ns: &[AtomicU64],
    hist: &mut hdrhistogram::Histogram<u64>,
    t0: Instant,
    responses: &AtomicU64,
    resolved: &AtomicU64,
    lost: &AtomicU64,
    last_response_ns: &AtomicU64,
) {
    poll.poll(|c| {
        resolved.fetch_add(1, Ordering::Relaxed);
        match c.outcome {
            RemoteOutcome::Response { expired: false, .. } => {
                let now = t0.elapsed().as_nanos() as u64;
                let sent = send_ns[(c.user_data as usize) & SLOT_MASK].load(Ordering::Relaxed);
                let _ = hist.record(now.saturating_sub(sent).max(1));
                responses.fetch_add(1, Ordering::Relaxed);
                last_response_ns.fetch_max(now, Ordering::Relaxed);
            }
            _ => {
                lost.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
}
```

Delete the `N_WAITERS`/`TICKET_WAIT` constants (918–922) and the waiter pool (945–984) with them, and drop the now-unused `mpsc`/`AtomicBool` imports if nothing else in the file uses them. Keep `ClientStats`, `SLOTS`, `SLOT_MASK`, `new_hist` and the pass/fail computation exactly as they are; only the fields' sources change. This arm has **no second thread**: the submitter drains `poll` inline between submits, which is legal because `poll` is nonblocking and this arm drives one connection.

- [ ] **Step 3: Rewrite `print_remote_stats` (lines 1082–1101)**

```rust
fn print_remote_stats(send: &RemoteSendHalf) {
    let s = send.stats();
    println!("---------------------------- gateway/remote plane -------------------------");
    println!(
        "redirects {} | leader_changes {} | reconnects {} | resends {} | retries {} | \
         unknown {} | expired {} | refused_members {} | max_credits_seen {} | \
         socket_writes {} | frames_written {}",
        s.redirects,
        s.leader_changes,
        s.reconnects,
        s.resends,
        s.retries,
        s.unknown,
        s.expired,
        s.refused_members,
        s.max_credits_seen,
        s.socket_writes,
        s.frames_written
    );
    println!("============================================================================");
}
```

- [ ] **Step 4: Update the two call sites**

`uc_gateway/examples/m12_gate.rs:885-898` becomes:

```rust
    let leader_addr = edges[leader].local_addr();
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        app_id: APP_ID.into(),
        members: vec![leader_addr.to_string()],
        client_id: None,
        max_inflight: inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .unwrap_or_else(|e| panic!("remote connect {leader_addr}: {e}"));

    let stats = run_remote_measurement(&send, &mut poll, secs, payload);
    print_remote_stats(&send);

    send.shutdown();
```

and `uc_gateway/examples/m12_gate.rs:1399-1411` becomes:

```rust
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        app_id: a.app_id,
        members: gateways,
        client_id: None,
        max_inflight: a.inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .map_err(|e| anyhow::anyhow!("remote connect: {e}"))?;

    let stats = run_remote_measurement(&send, &mut poll, a.secs, a.payload);
    print_remote_stats(&send);
    send.shutdown();
```

- [ ] **Step 5: Build and smoke**

Run: `cargo build -p uc_gateway --examples --release && cargo clippy -p uc_gateway --all-targets -- -D warnings`
Expected: PASS.

Run: `cargo run -p uc_gateway --release --example m12_gate -- --help`
Expected: the `client-remote` role is still listed with the same flags.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_gateway/examples/m12_gate.rs
git commit -m "perf(m12_gate): client-remote arm on the RemoteEngine halves

One submitter loop and an inline poll drain replace the Ticket-per-request
waiter pool; ClientStats is filled from the same counters as before."
```

---

### Task 13: Caller migration C — the crashtest capstone and `counter-remote`

Both stay on the **convenience client**; the only incompatible change is `query`'s second argument.

**Files:**
- Modify: `examples/uc_crashtest/tests/remote_lin.rs:109` (import), `:629`, `:1000`.
- Modify: `examples/counter/src/bin/counter-remote.rs:51` (import), `:182-189` (`query`).

**Interfaces:**
- Consumes: Task 10's `RemoteClient`, `Ticket`, `Consistency`.
- Produces: nothing.

- [ ] **Step 1: `remote_lin.rs` — the exact diff**

Line 109:

```rust
// before
use uc_remote::{RemoteClient, RemoteConfig, RemoteError, RemoteResponse, RemoteStats};
// after
use uc_remote::{
    Consistency, RemoteClient, RemoteConfig, RemoteError, RemoteResponse, RemoteStats,
};
```

Line 629:

```rust
// before
                let r = client.query(&read_query(), true).and_then(|t| t.wait());
// after
                let r =
                    client.query(&read_query(), Consistency::Linearizable).and_then(|t| t.wait());
```

Line 1000:

```rust
// before
            match c.query(&read_query(), true).and_then(|t| t.wait()) {
// after
            match c.query(&read_query(), Consistency::Linearizable).and_then(|t| t.wait()) {
```

Nothing else changes: `connect_remote` (523–549), the `submit → wait` worker (592–716), the `stats().resends` before/after delta (604, 695) and the `RemoteStats` fold (1016–1027) all still compile and still mean the same thing — the worker keeps exactly one request outstanding, so the per-op `resends` delta and `MAX_PHANTOM_DUPLICATES` keep their meaning. The fold uses `RemoteStats::default()` plus field mutation, so the two new counters need no edit.

- [ ] **Step 2: `counter-remote.rs` — the exact diff**

Line 51:

```rust
// before
use uc_remote::{RemoteClient, RemoteConfig, RemoteError};
// after
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};
```

Lines 182–189:

```rust
// before
fn query(client: &RemoteClient, linearizable: bool, deadline: Instant) -> Result<(), Fail> {
    let ticket =
        client.query(&enc(&Query::Value), linearizable).map_err(|e| Fail::Run(e.to_string()))?;
// after
fn query(client: &RemoteClient, linearizable: bool, deadline: Instant) -> Result<(), Fail> {
    let consistency =
        if linearizable { Consistency::Linearizable } else { Consistency::Snapshot };
    let ticket =
        client.query(&enc(&Query::Value), consistency).map_err(|e| Fail::Run(e.to_string()))?;
```

`connect` (114–141) still clones its `RemoteConfig` — `RemoteConfig` keeps `#[derive(Clone, Debug)]`. `client.shutdown()` at line 170 still compiles (it now takes `&self`).

- [ ] **Step 3: Build and run**

Run: `cargo build -p counter -p uc_crashtest --all-targets && cargo clippy -p counter -p uc_crashtest --all-targets -- -D warnings`
Expected: PASS.

Run: `cargo test -p uc_crashtest --test remote_lin -- --nocapture`
Expected: PASS — both envelope-on and envelope-off cases, `Linearizable`.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add examples/uc_crashtest/tests/remote_lin.rs examples/counter/src/bin/counter-remote.rs
git commit -m "refactor(examples): query takes Consistency, not a bool

The crashtest capstone and the counter-remote reference client stay on the
blocking convenience client; only the query signature moved."
```

---

### Task 14: Caller migration D — the `uc_gateway` test suite

**Files:**
- Modify: `uc_gateway/tests/roundtrip.rs:20` (import), `:85`, `:91`, `:133`.
- Modify: `uc_gateway/tests/failover.rs:52` (import), `:261`.
- Verify unchanged: `uc_gateway/tests/credits.rs` (only `submit`/`wait`/`stats`/`shutdown`), `uc_gateway/tests/credits_wire.rs` (`submit`/`wait`/`stats`/`is_connected`/`shutdown` + raw-socket work), `uc_gateway/tests/bin_smoke.rs` and `config_file.rs` (no `uc_remote` at all).

**Interfaces:**
- Consumes: Task 10.
- Produces: nothing.

- [ ] **Step 1: `roundtrip.rs` — the exact diff**

Line 20:

```rust
// before
use uc_remote::{RemoteClient, RemoteConfig};
// after
use uc_remote::{Consistency, RemoteClient, RemoteConfig};
```

Lines 85, 91, 133:

```rust
// before (85)
    let r = client.query(&q, true).unwrap().wait().unwrap();
// after (85)
    let r = client.query(&q, Consistency::Linearizable).unwrap().wait().unwrap();

// before (91)
    let r = client.query(&q, false).unwrap().wait().unwrap();
// after (91)
    let r = client.query(&q, Consistency::Snapshot).unwrap().wait().unwrap();

// before (133)
    let r = client.query(&read_query(), true).unwrap().wait().unwrap();
// after (133)
    let r = client.query(&read_query(), Consistency::Linearizable).unwrap().wait().unwrap();
```

Line 170's oversized-payload test is **unchanged and load-bearing**: `client.submit(&vec![0u8; 4096]).unwrap()` must still reach the edge (the test asserts `edge.stats().retries == 1`), which is why the client refuses locally only above `MAX_FRAME_LEN`/the ring capacity, never at the node's configured `max_payload`.

- [ ] **Step 2: `failover.rs` — the exact diff**

Line 52:

```rust
// before
use uc_remote::{RemoteClient, RemoteConfig, RemoteError};
// after
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};
```

Line 261:

```rust
// before
    let r = client.query(&q, true).unwrap().wait().expect("linearizable read");
// after
    let r = client
        .query(&q, Consistency::Linearizable)
        .unwrap()
        .wait()
        .expect("linearizable read");
```

Lines 134–146 (`connected_addr`, `stats().leader_changes`), 173–180 (`leader()`), 200–226 (200 `Ticket`s across a SIGKILL, `Err(RemoteError::Expired)` collected) and 276–324 (`stats()` + `shutdown()`) are unchanged — every one of those methods survives with the same signature.

- [ ] **Step 3: Run the suite**

Run: `cargo test -p uc_gateway && cargo clippy -p uc_gateway --all-targets -- -D warnings`
Expected: PASS — `credits`, `credits_wire`, `failover`, `roundtrip`, `bin_smoke`, `config_file`.

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add uc_gateway/tests/roundtrip.rs uc_gateway/tests/failover.rs
git commit -m "test(gateway): query takes Consistency, not a bool"
```

---

### Task 15: Documentation

**Files:**
- Modify: `docs/reference/remote-protocol.md:213-244` (the credit section) and `:285-363` (the failover promises) — the two §6 clarifications plus a short "client structure" note.
- Modify: `README.md:179` — the `uc_remote` row.
- Modify: `docs/QUICKSTART.md:454` — the sentence naming `RemoteClient`.
- Modify: `docs/how-to/run-a-gateway.md:119` — the "conforming client" sentence.

**Interfaces:** none (prose only).

**Scope note:** the release-time documentation sweep (`RELEASES.md`, `docs/releases.md`, the `run-a-gateway.md` operating-envelope rewrite, the `uc2-m12a-edge-flow-control-gap.md` correction) belongs to **M13 as a whole** (spec §7: "Docs at release time, not before"), not to this track. This task does only what track B's own change invalidates.

- [ ] **Step 1: Add the two §6 clarifications**

In `docs/reference/remote-protocol.md`, in the **Flow control — credits** section, immediately after the "Credit rule" paragraph (line 219), insert:

```markdown
**`credits` is an absolute grant, and it MAY decrease.** Every `HELLO_OK`,
`RESPONSE` and `STATUS` carries the grant the edge is willing to honour *from
now on* — it is not a delta and not a ceiling that only rises. A client that
sees a lower value MUST NOT send `seq > acked_seq + credits` afterwards; the
requests already on the wire under the older, wider grant are not recalled
(that is what the edge's headroom is for). `uc_remote`'s client has always
behaved this way — it stores whatever the last frame said and gates the next
`seq` on it — and this paragraph makes the requirement explicit for a port.

**`STATUS` MAY be sent at any time.** The reference edge sends one on its idle
timer and when a `relax` widens the window, and it MAY send one the moment it
*reduces* a grant, so a client learns about a narrower window before its next
`RESPONSE` rather than after it. A conforming client therefore treats `STATUS`
as "apply this `(acked_seq, credits)` pair now", never as an idle-only
keepalive.
```

- [ ] **Step 2: Add the client-structure note**

In `docs/reference/remote-protocol.md`, at the end of the **Failover promises** section (after line 363), add:

```markdown
### How the reference client is built (informative)

`uc_remote` implements the promises above with **two threads per connection
and no lock on the request path**, which a port may but need not copy:

- the **submitter** (the caller's own thread) checks the window from two
  atomics (`acked_seq`, `credits`), assigns the next `seq`, encodes the frame
  into a preallocated ring and records the request in a slot table — no
  syscall, no allocation;
- the **writer** thread drains that ring into ONE `write` per drain
  (flush-on-empty, no batch timer), owns the socket for dial/redial, and
  re-sends the live window in `seq` order after a reconnect;
- the **reader** thread reads 64 KiB at a time, applies `(acked_seq,
  credits)`, resolves slots, and hands completions to the caller's poller in
  one batch per read.

The blocking `RemoteClient`/`Ticket` API is a thin convenience over that pair
of halves ([`RemoteEngine::connect`]), not a separate implementation. What the
wire requires of any client is only what the sections above say: the credit
rule, the ordered re-send, the probe before flush, and the liveness clocks.
```

- [ ] **Step 3: The three one-line API mentions**

`README.md:179`:

```markdown
| `uc_remote` | The remote wire protocol (framed TCP, credit-gated flow control) and its Rust client: `RemoteEngine`'s split `SendHalf`/`PollHalf` (two threads per connection, batched writes, no lock on the request path) plus the blocking `RemoteClient` convenience built on them — for clients that can't attach to shmem directly |
```

`docs/QUICKSTART.md:454` — replace the clause naming `RemoteClient` so it reads:

```markdown
  protocol a remote client speaks, and `RemoteEngine`/`RemoteClient`, the Rust
```

`docs/how-to/run-a-gateway.md:119`:

```markdown
A conforming client (`uc_remote`'s `RemoteEngine` halves or the `RemoteClient`
convenience over them, or a port that implements
```

- [ ] **Step 4: Check the docs still read straight**

Run: `grep -rn "RemoteClient" docs/ README.md | grep -v superpowers | grep -v benchmarks`
Expected: every remaining mention is either the convenience client (correct) or in `docs/reference/semver-policy.md` / `docs/releases.md` / `RELEASES.md` (historical or release-time, out of this task's scope). Read each hit and confirm it is not claiming the deleted single-lock design.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/ultima_cluster
git add docs/reference/remote-protocol.md README.md docs/QUICKSTART.md docs/how-to/run-a-gateway.md
git commit -m "docs(remote-protocol): credits MAY decrease, STATUS any time, client structure

The two v1 clarifications M13 §6 asks for (no wire change), plus an informative
note on how the reference client is now built and three API-name touch-ups."
```

---

### Task 16: Local proof

**Files:** none modified (this task only runs things and records what they said).

**Interfaces:** none.

**Reminder before running anything:** this box is a **4 vCPU dev box, not a bench** (`docs/notes/dev-box-not-a-bench.md`). Every number below is a **relative** smoke check against a number measured on the same box on 2026-08-24; none of them is a bar, and none may be quoted as capacity. The M13 bars are fleet-only (spec §2, rows a–f) and are adjudicated in `docs/benchmarks/uc2-m13-gate-<date>.md`. `/tmp` is tmpfs with no swap — do not redirect any output there.

- [ ] **Step 1: The crate's own suite**

Run: `cargo test -p uc_remote`
Expected: PASS — the unit tests from Tasks 1–4 and 7, `codec.rs`, the 10 `client_fake_edge` convenience scenarios and the 30 `engine_fake_edge` scenarios (27 ports + `connect_completes_the_handshake…`, `an_idle_status_updates…`, `a_dropped_connection_is_re_established…`).

- [ ] **Step 2: The gateway suite (the wire's other side)**

Run: `cargo test -p uc_gateway`
Expected: PASS — `credits`, `credits_wire`, `failover`, `roundtrip`, `bin_smoke`, `config_file`.

- [ ] **Step 3: The correctness capstone**

Run: `cargo test -p uc_crashtest --test remote_lin`
Expected: PASS — linearizable with the session envelope on and off.

- [ ] **Step 4: Workspace build + lint**

Run: `cargo build --workspace --all-targets && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS with zero warnings.

- [ ] **Step 5: The rest of the default suite**

Run: `cargo test`
Expected: PASS (no regression outside the crates touched).

- [ ] **Step 6: Confirm the fuzz tier is untouched**

Run: `grep -rn "uc_remote::" fuzz/fuzz_targets/uc_remote_frame.rs fuzz/src/seeds.rs fuzz/src/bin/seed_corpus.rs`
Expected: only `uc_remote::frame::*` — no client, config or ticket import, so the 14 targets need no change. If a nightly toolchain and `cargo-fuzz` are installed, also run `(cd fuzz && cargo +nightly check)` and expect PASS; if they are not, say so rather than claiming it passed.

- [ ] **Step 7: The dev-box throughput smoke — the point of the whole track**

```bash
cd /home/claude/ultima/ultima_cluster
cargo build -p uc_gateway --example hop_bench --release
target/release/examples/hop_bench dummy-edge --listen 127.0.0.1:19301 --credits 1024 &
sleep 1
target/release/examples/hop_bench remote-load --gateways 127.0.0.1:19301 --secs 3 --payload 64 --inflight 1024 --conns 1
kill %1
```

Expected: **>= 1,000,000 resp/s** on the `RESULT {...}` line, against the 50,480 resp/s the old client measured on this same box and sink (bench doc, "Dev-box smoke", row C), and `frames_per_write > 1.0` on the per-conn line. Record the actual number in the commit message. A result below ~500 k/s means the writer is not batching (check `frames_per_write`) or the poller is parking too eagerly — do NOT relax the expectation, debug it.

- [ ] **Step 8: The full local ladder**

```bash
cd /home/claude/ultima/ultima_cluster
target/release/examples/hop_bench local --secs 3 --conns 1,4
```

Expected: every arm completes with `lost = 0`; arms C and E (`remote-load`) are now within the same order of magnitude as B and D (`blaster`) instead of ~50x below them. Again: ordering only, not capacity.

- [ ] **Step 9: Commit the record**

```bash
cd /home/claude/ultima/ultima_cluster
git commit --allow-empty -m "chore(m13b): local proof stack green

cargo test -p uc_remote / -p uc_gateway / -p uc_crashtest --test remote_lin,
cargo test, clippy --workspace -D warnings: all green.
Dev-box smoke (RELATIVE, 4 vCPU, not a bar): remote-load -> dummy-edge, 1 conn,
inflight 1024 = <FILL IN measured resp/s> vs 50,480 for the old client on the
same box and sink; frames_per_write = <FILL IN>.
Fleet bars a-f are a separate, user-approved step (spec section 2)."
```

(The two `<FILL IN>`s are the only values an executor supplies from a run; everything else in this plan is fixed.)

---

## Self-review

Performed against the spec (`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`) after writing the plan.

**1. Spec coverage.**

| Spec item | Covered by |
|---|---|
| §3.1 the halves' exact signatures (`RemoteEngine`, `RemoteSendHalf`, `RemotePollHalf`, `RemoteCompletion`, `RemoteOutcome`) | Task 5 (types) + Task 6 (`try_submit`/`try_query`/`poll`) — matches the pinned interface verbatim, plus `client_id`/`is_connected`/`connected_addr` which existing callers need |
| §3.1 `SubmitError::Backpressure` semantics | Task 6 `send`, Task 7 `admissible` |
| §3.1 blocking `RemoteClient` convenience layered on top | Task 10 |
| §3.2 submitter: window from atomics, seq assignment, encode into a preallocated ring, slot table | Tasks 2, 4, 6 |
| §3.2 writer: one `write_all` per drain, flush-on-empty, no timer, park on the ring's wait word, owns dial/redial | Tasks 5, 8 |
| §3.2 reader: `read_frame_buffered` + `next_buffered`, atomics, bounded SPSC completion queue with a body arena, ONE wake per read batch | Tasks 3, 5, 6 |
| §3.2 poller: drain + `wait_handle` | Tasks 3, 5 |
| §3.3 all sixteen preserved behaviours | the provenance table at the top; Tasks 5, 8, 9 |
| §3.3 "the only lock is a reconnect mutex" | Task 5 (`Reconnect` + `control` + `retransmit`, all cold; the concurrency-contract section states which atomic has which single writer) |
| §3.3 seqs per client, strictly increasing, from 1; `acked_seq` monotone; absolute grants honoured immediately | Task 6 (gap-free seqs), Task 7 (`admissible`, `credit_update`) |
| §3.4 `hop_bench remote-load` and `m12_gate client-remote` move to the halves in the `engine-load` shape | Tasks 11, 12 |
| §3.4 `client_fake_edge`'s scenarios ported, scripted edge stays | Tasks 6–10 (all 27 ported, mapping tables per task; the edge gains one additive `Behaviour` field and is otherwise untouched) |
| §3.4 `remote_lin` on the blocking convenience | Task 13 |
| §3.4 new unit tests: window arithmetic, ring under wrap, completion-queue backpressure, resend-after-redial | Task 7, Task 2, Task 3, Task 8 |
| §6 both wire clarifications | Task 15 |
| §7 `counter-remote` on the convenience client | Task 13 |
| Not in this track (stated): §4 MPSC ring, §5 edge budget, §2 fleet bars, §7 release-time docs and the `2.7.0` version bump | scope notes in Task 15 and Task 16 |

No spec item in §3 or §6 is without a task.

**2. Placeholder scan.** Searched for `TBD`, `TODO`, "similar to Task", "add error handling", "etc.", and steps without code. Five findings, all fixed: the `m12_gate` rewrite had a stray `std::thread::scope` marker line (removed, with an explicit "this arm has no second thread" note); a fake `assert_not_sync` helper that asserted nothing (replaced by the structural `PhantomData<Cell<()>>` argument and a note that the real proof is that `assert_send_sync::<RemoteSendHalf>()` does not compile); a `poll_wake` function in `remote-load` that was a no-op (replaced by a real `RemoteWaitHandle::wake` taken before the poll half moves into its thread); `redial`'s "TASK 8 inserts the resend here" marker is a **deliberate, named forward reference with the replacement code given verbatim in Task 8**, not a placeholder; `drain_ring`/`flush_limit` are introduced in Task 6 in their simple form and *replaced* in Task 8 with the full body, which is written out in full there rather than described. The only values an executor supplies are the two measured numbers in Task 16's final commit message, marked `<FILL IN>`.

**3. Type consistency.** Checked every name used across task boundaries: `OutRing::{new, capacity, free, stage_frame, commit, push_frame, release_to, write_pos, send_pos, ack_pos, peek_upto, consume, set_send_pos, copy_range, wake}`; `CompletionQueue::{new, push, publish, drain, is_empty, ready, drained}` with `Record`/`OutcomeTag`; `SlotTable::{new, claim, is_free, resolve, abort, sweep, drain_abort, is_live, extent, kind, mark_sent, is_sent, set_not_before, not_before, bump_attempts, inflight, next_seq, publish_next_seq}`; `WaitCell::{new, seq, signal, park}`; `Link::{start, now_ns, closed, is_connected, leader, connected_addr, complete, sweep_deadlines, request_redial, queue_control, queue_retransmit, jittered, close, close_from_thread}`; `engine::{admissible, outcome_of, drain_completions}`. Four fixes made while checking: `Link::leader` is reached through a setter rather than a public field (Task 8 says so); `SlotTable::is_free` is introduced in Task 6 where it is first needed; `flush_limit` takes the writer's `cursor`, so `writer_loop`/`drain_ring`/`redial` all thread it (stated in Tasks 6 and 8); `RemoteWaitHandle::wake` is used by the convenience client's `shutdown` and is declared in Task 5.

**Two honest notes carried in the plan rather than smoothed over:** the spec says 28 fake-edge scenarios and the file has 27; and `park.rs` is a seventh file beyond the spec's suggested six, because the wait cell has three unrelated users.
