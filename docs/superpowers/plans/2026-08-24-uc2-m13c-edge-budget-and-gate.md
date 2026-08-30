# M13c — Edge credit budget + M13 gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build spec §5 — a **global outstanding-grant budget** in `uc_gateway::Edge`, so the sum of credits the edge has promised across all its connections never exceeds the `Engine` window it can actually honour, and a shrinking share reaches a client *before* it sends into it. Then build the M13 gate artefacts (spec §2): the gate doc with its pre-committed bars a–f, and `m13_hop_bench.py --arms gate` that adjudicates rows a–d and prints e/f's commands. Finally, a docs-only release checklist (spec §7) that retires the `v2.6.0` operating envelope the fix removes.

**Architecture:** `Shared` gains a `budget` (the `Engine` window less a 1/8 headroom) and a `live` count of handshaken connections; each connection's grant is `clamp(budget / live, 1, per_conn_inflight)`, recomputed on every connect and disconnect. A reduction is published by the **single driver thread** (the only thread allowed to touch other connections' sockets) as `STATUS{acked_seq, credits}`; a handshaking reader waits for that publication before it grants its own connection anything, which is what makes "the sum never exceeds the budget" true at every instant rather than eventually. An increase rides the next `RESPONSE` or the idle `STATUS` timer, because a wider window costs a client nothing to learn late.

**Tech Stack:** Rust 2024 (`uc_gateway`, `uc_protocol` unchanged, `uc_remote` unchanged), Python 3 for `bench-infra/scripts/m13_hop_bench.py`, Markdown for the gate doc and the release sweep.

**Spec:** docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md

---

## Global Constraints

- MSRV is **1.89**; local builds use the pinned **1.96.0** in `rust-toolchain.toml`. Nothing here may need a newer floor.
- `cargo clippy --workspace --all-targets -- -D warnings` must pass with **zero** warnings after every task.
- **Never write scratch, journals, histories or instance dirs to `/tmp`** — it is RAM-backed with no swap on this box (CLAUDE.md "Local box"). Tests use `CARGO_TARGET_TMPDIR` via `common::tempdir()`.
- **Remote wire protocol v1 is unchanged.** `HELLO_OK{credits}`, `RESPONSE{credits}` and `STATUS{acked_seq, credits}` already carry everything; no new frame type, no version bump, no cnc field.
- **Dev-box numbers are smoke only.** Every rate bar in the gate doc is fleet-only; never move a bar because a local run went red (memory: `dev-box-not-a-bench`).
- **Fleet runs are a user-approved step.** This plan produces the driver and the empty gate doc; the last task prints the exact command and stops.
- **Commit after every task**, with the exact message given in that task's final step.
- **Track boundaries:** track B owns `uc_remote` (`RemoteClient` keeps `connect`/`submit`/`Ticket::wait`/`stats`/`shutdown` and `RemoteStats.max_credits_seen`) and `docs/reference/remote-protocol.md` §6; track A owns `uc_protocol::ring::mpsc` and the ring-format restart note. **Do not edit those files here** — reference them.

---

## Execution errata

- **Task 2, `handshake`:** the drafted `let gen = shared.grant_gen.load(...)` does
  not compile — `gen` is a **reserved keyword under Rust edition 2024** (this
  workspace's edition). Use a different local name (implemented as `start_gen`);
  the `Shared::grant_gen` field name is fine (not a keyword). Applies to any later
  code that copies the handshake settle-wait.

## File Structure

```
uc_gateway/
  src/
    config.rs          MODIFY  ConfigError::PerConnExceedsBudget, validate, defaults doc
    conn.rs            MODIFY  Conn.ceiling, set_ceiling -> CeilingChange, relax(), counted flag
    edge.rs            MODIFY  budget_for/grant_for, Shared{budget,live,regrant,grant_gen},
                               handshake grant, driver regrant_tick, STATUS on squeeze,
                               EdgeStats.grant_changes, Edge::grants_for_tests
    lib.rs             MODIFY  pub use edge::{budget_for, grant_for}
    bin/uc2-gateway.rs MODIFY  stats line gains grant_changes=
  tests/
    common/mod.rs      MODIFY  raw framed-client helpers (moved in from credits_wire.rs)
    credits.rs         MODIFY  tests (i) (ii) (iii) (iv)
    credits_wire.rs    MODIFY  use the moved helpers (no behaviour change)
  examples/hop_bench/
    main.rs            MODIFY  EdgeArgs::per_conn_inflight default 4096 -> 1024

packaging/
  gateway.example.toml MODIFY  budget note under [limits]
  systemd/uc2-gateway.service  MODIFY  CPUQuota comment (release task)

bench-infra/scripts/
  m13_hop_bench.py     MODIFY  --arms gate, --selftest, row verdicts, exit code

docs/
  benchmarks/uc2-m13-gate-2026-08-24.md   CREATE  bars a-f, empty results
  benchmarks/uc2-m12-gate-2026-08-22.md   MODIFY  row 2 closed by reference (release task)
  reference/gateway-config.md             MODIFY  budget semantics
  how-to/run-a-gateway.md                 MODIFY  operating-envelope rewrite (release task)
  notes/uc2-m12a-edge-flow-control-gap.md MODIFY  correction paragraph (release task)
  releases.md / ../RELEASES.md            MODIFY  v2.7.0 entry (release task)
```

---

### Task 1: `budget_for` / `grant_for` — the arithmetic, alone

**Files:**
- Modify `uc_gateway/src/edge.rs` — new constant + two pure fns near the constants block (after `DRIVER_PERIODIC_EVERY`, ~line 138); new tests in the existing `mod tests` (~line 1408).
- Modify `uc_gateway/src/lib.rs` (~line 51) — re-export.

**Interfaces:**
- Produces: `pub fn budget_for(max_inflight: u32) -> u32`, `pub fn grant_for(live: u32, budget: u32, per_conn: u32) -> u32`, `pub const BUDGET_HEADROOM_DIV: u32 = 8`.
- Consumes: nothing.

**Steps:**

- [ ] Write the failing tests first. Append to `uc_gateway/src/edge.rs`'s `mod tests`:

```rust
    #[test]
    fn the_budget_holds_back_an_eighth_of_the_engine_window() {
        assert_eq!(budget_for(4096), 3584, "4096 - 4096/8");
        assert_eq!(budget_for(8), 7);
        // Below the divisor the headroom rounds to nothing; the budget is then
        // the whole window, which is still a bound, just a tight one.
        assert_eq!(budget_for(4), 4);
        assert_eq!(budget_for(1), 1);
        assert_eq!(budget_for(0), 1, "a zero budget would wedge every connection");
    }

    #[test]
    fn a_grant_is_an_equal_share_capped_by_the_config_and_floored_at_one() {
        // One connection takes the whole budget, but never more than the
        // operator allowed it.
        assert_eq!(grant_for(1, 3584, 256), 256);
        assert_eq!(grant_for(1, 200, 256), 200, "the budget binds below the cap");
        // Equal shares.
        assert_eq!(grant_for(2, 56, 32), 28);
        assert_eq!(grant_for(4, 56, 32), 14);
        // The floor: past `live > budget` a share would round to zero, which
        // would wedge a connection forever. It is also the point past which
        // the sum can exceed the budget — `validate` warns about it.
        assert_eq!(grant_for(100, 56, 32), 1);
        assert_eq!(grant_for(0, 56, 32), 32, "no live connections reads as one");
    }

    #[test]
    fn grants_sum_within_the_budget_while_live_is_within_it() {
        for budget in [7u32, 56, 3584, 57344] {
            for live in 1..=budget.min(64) {
                let g = grant_for(live, budget, u32::MAX);
                assert!(
                    g * live <= budget,
                    "live={live} budget={budget} grant={g}: the sum over-promises"
                );
            }
        }
    }
```

- [ ] Run: `cargo test -p uc_gateway --lib` → expect failure: `cannot find function 'budget_for' in this scope` (and `grant_for`).

- [ ] Implement. Insert after `const DRIVER_PERIODIC_EVERY: u64 = 64;` in `uc_gateway/src/edge.rs`:

```rust
/// The fraction of the `Engine` window the edge keeps out of the grant
/// budget: `1/8`.
///
/// Deliberately a constant and **not** a config key. It is not a tuning
/// dial — it is the slack that makes a *shrinking* grant safe: a client that
/// is told a smaller absolute number honours it for new seqs, but the frames
/// it already put on the wire are still owed `Engine` slots, and the
/// handshake's grant-settle wait ([`Shared::await_settled`]) falls back to
/// granting a share when the driver has not published in time. Both cases
/// land in this headroom. An operator who wants a smaller sum lowers
/// `per_conn_inflight`; one who wants a bigger one raises `max_inflight`.
pub const BUDGET_HEADROOM_DIV: u32 = 8;

/// The edge's **total** outstanding-grant budget: the `Engine` window less
/// the headroom above. The sum of what every connection has been granted
/// stays at or under this (see [`grant_for`] for the one documented
/// exception).
pub fn budget_for(max_inflight: u32) -> u32 {
    max_inflight.saturating_sub(max_inflight / BUDGET_HEADROOM_DIV).max(1)
}

/// One connection's share of the budget: an equal split, capped by the
/// operator's `per_conn_inflight` and floored at 1.
///
/// The floor is the documented exception to "the sum is at most the budget":
/// once `live > budget` every connection would be entitled to zero, and a
/// zero grant wedges a connection forever (the same reason
/// [`crate::conn::Conn::squeeze`] floors at 1). `EdgeConfig::validate` warns
/// when `max_connections > budget_for(max_inflight)`, which is exactly the
/// configuration in which that can happen.
pub fn grant_for(live: u32, budget: u32, per_conn: u32) -> u32 {
    (budget / live.max(1)).clamp(1, per_conn.max(1))
}
```

- [ ] Re-export in `uc_gateway/src/lib.rs`, replacing the `pub use edge::{Edge, EdgeError, EdgeStats};` line:

```rust
pub use edge::{BUDGET_HEADROOM_DIV, Edge, EdgeError, EdgeStats, budget_for, grant_for};
```

- [ ] Run: `cargo test -p uc_gateway --lib` → expect `test result: ok`, the three new tests passing.
- [ ] Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings` → expect no warnings.
- [ ] Commit:

```sh
git add uc_gateway/src/edge.rs uc_gateway/src/lib.rs
git commit -m "feat(gateway): budget_for/grant_for — the edge's global grant arithmetic

Pure functions, no wiring yet: budget = Engine window - 1/8 headroom,
grant = clamp(budget / live, 1, per_conn_inflight). The floor-at-1
exception (live > budget) is documented and tested; spec §5.1."
```

---

### Task 2: `Shared` budget + `Conn` ceiling, recomputed on connect and disconnect

**Files:**
- Modify `uc_gateway/src/conn.rs` — `Conn` fields (~51-95), `Conn::new` (~98-116), `squeeze`/`relax` (~324-360), new `set_ceiling`/`ceiling`/`mark_counted`/`clear_counted`, `CeilingChange` enum; unit tests (~540-560).
- Modify `uc_gateway/src/edge.rs` — `Shared` fields (~299-328), `Edge::start`'s `Shared` literal (~555-566), `handshake` (~843-855), `reader`'s tail (~778), `complete`'s `relax` call (~1328), `Edge::grants_for_tests`.
- Modify `uc_gateway/tests/credits.rs` — test (i).

**Interfaces:**
- Produces: `pub(crate) enum CeilingChange { Same, Raised, Lowered }`; `Conn::set_ceiling(&self, ceiling: u32) -> CeilingChange`; `Conn::ceiling(&self) -> u32`; `Conn::relax(&self) -> bool` (no argument any more); `Shared::join(&self, conn: &Conn)`, `Shared::leave(&self, conn: &Conn)`, `Shared::current_grant(&self) -> u32`; `#[cfg(feature = "test-util")] Edge::grants_for_tests(&self) -> Vec<(u32, u32)>`.
- Consumes: `grant_for`, `budget_for` from Task 1.

**Steps:**

- [ ] Failing test first — append to `uc_gateway/tests/credits.rs`:

```rust
/// (spec §5.4, i) Whatever the edge has promised across every connection, the
/// SUM of it stays inside the budget — at every observed moment, not merely
/// once the dust settles. Sampled by a watchdog thread while connections
/// arrive, so a transient over-promise between "this connection is ready" and
/// "everyone else has been told their share shrank" is caught.
#[test]
fn the_sum_of_grants_never_exceeds_the_edges_budget() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc_service::ServiceBuilder::new(
        uc_service::ServiceConfig::new(&dir, common::APP),
        uc_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    // budget = 64 - 64/8 = 56; per-connection cap 32. Six connections is
    // well inside `live <= budget`, so the floor-at-1 exception cannot apply.
    let edge = std::sync::Arc::new(Edge::start(edge_config(&dir, 64, 32)).unwrap());
    let budget = uc_gateway::budget_for(64);
    assert_eq!(budget, 56);

    let worst = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (e2, w2, s2) =
        (std::sync::Arc::clone(&edge), std::sync::Arc::clone(&worst), std::sync::Arc::clone(&stop));
    let sampler = std::thread::spawn(move || {
        while !s2.load(std::sync::atomic::Ordering::Relaxed) {
            let sum: u32 = e2.grants_for_tests().iter().map(|(_, g)| *g).sum();
            w2.fetch_max(sum, std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(Duration::from_micros(200));
        }
    });

    let mut conns = Vec::new();
    for i in 0..6u64 {
        let mut c = common::dial_raw(edge.local_addr());
        common::send_hello(&mut c, 0x100 + i, common::APP);
        let ok = common::read_until(&mut c, uc_remote::frame::FrameType::HelloOk,
                                    Duration::from_secs(5));
        assert!(ok.is_some(), "connection {i} never got HELLO_OK");
        let live = (i + 1) as u32;
        let want = uc_gateway::grant_for(live, budget, 32);
        // Settled state: everyone holds the same share.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let g = edge.grants_for_tests();
            if g.len() == live as usize && g.iter().all(|(_, x)| *x == want) {
                break;
            }
            assert!(std::time::Instant::now() < deadline,
                    "grants never settled to {want} at live={live}: {g:?}");
            std::thread::sleep(Duration::from_millis(2));
        }
        conns.push(c);
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().unwrap();
    let worst = worst.load(std::sync::atomic::Ordering::Relaxed);
    assert!(worst <= budget,
            "the edge promised {worst} credits at once against a budget of {budget}");
    assert!(worst > 0, "the sampler never saw a grant at all — vacuous");

    drop(conns);
    std::sync::Arc::try_unwrap(edge).ok().unwrap().stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
```

- [ ] Add the raw-client helpers the test needs to `uc_gateway/tests/common/mod.rs` (moved from `credits_wire.rs`, which keeps working through them). Append:

```rust
// ------------------------------------------------------ raw framed client
//
// A `FramedConn` driven by hand, for the properties a `RemoteClient` cannot
// report: what is on the wire, in what order. Shared by `credits_wire.rs`
// (frame ordering) and `credits.rs` (the grant budget).

use uc_remote::conn::FramedConn;
use uc_remote::frame::{FrameType, Header, Hello, PROTOCOL_VERSION};

/// Mid-frame stall budget for the raw reads below. Nothing in these tests
/// writes a partial frame, so it only bounds a wedged test.
pub const READ_STALL: Duration = Duration::from_secs(10);

/// Open a raw framed connection, with a short read timeout so a silent edge
/// shows up as `Ok(None)` rather than a hang.
pub fn dial_raw(addr: std::net::SocketAddr) -> FramedConn {
    let s = std::net::TcpStream::connect(addr).expect("connect");
    let c = FramedConn::new(s).unwrap();
    c.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    c
}

pub fn send_hello(c: &mut FramedConn, client_id: u64, app_id: &str) {
    let mut out = Vec::new();
    Hello { app_id }.encode(&mut out);
    let h = Header { ty: FrameType::Hello, flags: 0, version: PROTOCOL_VERSION, client_id, seq: 0 };
    c.write_frame(h, &out).expect("write HELLO");
}

/// Read frames until `want` arrives or `budget` runs out, returning its
/// header and payload.
pub fn read_until_frame(
    c: &mut FramedConn,
    want: FrameType,
    budget: Duration,
) -> Option<(Header, Vec<u8>)> {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        match c.read_frame(READ_STALL) {
            Ok(Some((h, p))) if h.ty == want => return Some((h, p.to_vec())),
            Ok(Some(_)) | Ok(None) => {}
            Err(_) => return None,
        }
    }
    None
}

/// [`read_until_frame`] when only the header matters.
pub fn read_until(c: &mut FramedConn, want: FrameType, budget: Duration) -> Option<Header> {
    read_until_frame(c, want, budget).map(|(h, _)| h)
}
```

  Then delete the now-duplicated `dial_raw`, `send_hello`, `read_until` and `READ_STALL` from `uc_gateway/tests/credits_wire.rs` (lines ~47-98) and change its call sites from `dial_raw(&edge)` to `common::dial_raw(edge.local_addr())`, `read_until(...)` to `common::read_until(...)`, `READ_STALL` to `common::READ_STALL`.

- [ ] Run: `cargo test -p uc_gateway --test credits` → expect failure: `no method named 'grants_for_tests' found for struct 'Edge'`.

- [ ] Implement the `Conn` half. In `uc_gateway/src/conn.rs`, add to the `Conn` struct after the `credits` field:

```rust
    /// The ceiling `credits` may climb back to — the connection's current
    /// share of the edge's global budget, **not** the config constant.
    /// Rewritten by the driver on every connect and disconnect
    /// ([`Conn::set_ceiling`]); `relax` aims at whatever it says now.
    ceiling: AtomicU32,
    /// This connection is counted in `Shared::live` — set once, when its
    /// handshake joins it to the budget, cleared once, when it leaves. The
    /// flag is what makes `join`/`leave` idempotent: a connection can be
    /// dropped by its own reader, by a failed unsolicited push, by
    /// `on_instance_restart` and by `stop`, and only the first of those may
    /// move the counter.
    counted: AtomicBool,
```

  and to `Conn::new`, beside `credits: AtomicU32::new(credits)`:

```rust
            ceiling: AtomicU32::new(credits),
            counted: AtomicBool::new(false),
```

- [ ] Add the accessors and `set_ceiling` after `Conn::relax` in `conn.rs`:

```rust
/// What [`Conn::set_ceiling`] did — the driver needs to know whether a client
/// is owed a frame about it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CeilingChange {
    /// The share is unchanged; nothing to say.
    Same,
    /// The share grew. The client learns it from the next `RESPONSE` or the
    /// idle `STATUS` timer — a wider window costs nothing to learn late.
    Raised,
    /// The share shrank. The client MUST be told before it sends into the
    /// window the edge no longer has.
    Lowered,
}

impl Conn {
    pub fn ceiling(&self) -> u32 {
        self.ceiling.load(Ordering::Acquire)
    }

    /// Set this connection's share of the edge's budget.
    ///
    /// A **reduction** clamps the live grant down immediately — the whole
    /// point is that the edge stops admitting into a window it cannot honour
    /// at the same moment the client is told about it, not one round trip
    /// later. An **increase** is applied to the live grant only when the
    /// connection is not squeezed; a squeezed connection climbs back through
    /// [`Conn::relax`], which now aims at this ceiling on its own, so a
    /// backpressure episode is not erased by an unrelated disconnect.
    pub fn set_ceiling(&self, ceiling: u32) -> CeilingChange {
        let ceiling = ceiling.max(1);
        let prev = self.ceiling.swap(ceiling, Ordering::AcqRel);
        if ceiling == prev {
            return CeilingChange::Same;
        }
        if ceiling < prev {
            let _ = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                if c > ceiling { Some(ceiling) } else { None }
            });
            CeilingChange::Lowered
        } else {
            if !self.squeezed.load(Ordering::Acquire) {
                let _ = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                    if c < ceiling { Some(ceiling) } else { None }
                });
                self.notify_gate();
            }
            CeilingChange::Raised
        }
    }

    /// `true` exactly once: the first call marks this connection as counted in
    /// the edge's `live` tally.
    pub fn mark_counted(&self) -> bool {
        !self.counted.swap(true, Ordering::AcqRel)
    }

    /// `true` exactly once: the first call after [`Conn::mark_counted`] takes
    /// it back out.
    pub fn clear_counted(&self) -> bool {
        self.counted.swap(false, Ordering::AcqRel)
    }
}
```

- [ ] Make `relax` read its own ceiling. In `conn.rs` change the signature and body head:

```rust
    /// Double the grant back towards the connection's current ceiling after a
    /// completion, but only if it was ever squeezed. Returns `true` if credits
    /// increased, which is what obliges the caller to tell the client promptly.
    ///
    /// The ceiling is the connection's live share of the edge's budget
    /// ([`Conn::set_ceiling`]), not the config constant: a connection that
    /// relaxes while five others are connected must not climb past the share
    /// those five leave it.
    ///
    /// Same CAS discipline as [`Conn::squeeze`], and for the same reason —
    /// these two are the pair that races.
    pub fn relax(&self) -> bool {
        if !self.squeezed.load(Ordering::Acquire) {
            return false;
        }
        let ceiling = self.ceiling();
```

  (the rest of the body is unchanged — it already uses the local `ceiling`).

- [ ] Update the `conn.rs` unit tests that called `relax(8)`:

```rust
    #[test]
    fn credits_halve_under_pressure_and_climb_back_to_the_ceiling() {
        let c = a_conn(0, 8);
        assert!(!c.relax(), "never squeezed: nothing to relax");
        c.squeeze();
        c.squeeze();
        assert_eq!(c.credits(), 2);
        assert!(c.relax());
        assert_eq!(c.credits(), 4);
        assert!(c.relax());
        assert_eq!(c.credits(), 8);
        assert!(!c.relax(), "at the ceiling the squeeze flag clears");
    }

    #[test]
    fn a_lowered_ceiling_clamps_the_live_grant_at_once() {
        let c = a_conn(0, 32);
        assert_eq!(c.set_ceiling(32), CeilingChange::Same);
        assert_eq!(c.set_ceiling(28), CeilingChange::Lowered);
        assert_eq!(c.credits(), 28, "the edge stops admitting the moment the share shrinks");
        assert_eq!(c.set_ceiling(32), CeilingChange::Raised);
        assert_eq!(c.credits(), 32, "an unsqueezed connection takes its share back at once");
    }

    #[test]
    fn a_raised_ceiling_does_not_erase_a_squeeze() {
        let c = a_conn(0, 8);
        c.squeeze();
        assert_eq!(c.credits(), 4);
        assert_eq!(c.set_ceiling(16), CeilingChange::Raised);
        assert_eq!(c.credits(), 4, "a squeezed connection climbs back through relax, not here");
        assert!(c.relax());
        assert_eq!(c.credits(), 8, "…and relax now aims at the NEW ceiling");
    }

    #[test]
    fn a_connection_is_counted_into_the_budget_exactly_once() {
        let c = a_conn(0, 4);
        assert!(c.mark_counted());
        assert!(!c.mark_counted(), "joining twice must not double-count");
        assert!(c.clear_counted());
        assert!(!c.clear_counted(), "leaving twice must not double-discount");
    }
```

- [ ] Implement the `Shared` half. In `uc_gateway/src/edge.rs`, add to `struct Shared` after `max_payload`:

```rust
    /// The total outstanding grant this edge will hand out across every
    /// connection — [`budget_for`] of the `Engine` window. Fixed at start;
    /// what moves is how it is divided.
    budget: u32,
    /// Connections counted into the budget: handshaken, not yet departed.
    live: AtomicU32,
    /// A connect or a disconnect has changed the share; the driver's next
    /// pass republishes it. A flag rather than a queue because the work is
    /// idempotent — recompute every connection's share from `live` — so
    /// coalescing two triggers into one pass is correct, not a shortcut.
    regrant: AtomicBool,
    /// Bumped by the driver after each pass that has published the current
    /// share to every ready connection. A handshaking reader waits for it to
    /// move before granting its own connection anything, which is what makes
    /// "the sum of grants is at most the budget" true at every instant rather
    /// than eventually. See [`Shared::await_settled`].
    grant_gen: AtomicU64,
```

  and to the `Shared { … }` literal in `Edge::start` (after `max_payload,`):

```rust
            budget,
            live: AtomicU32::new(0),
            regrant: AtomicBool::new(false),
            grant_gen: AtomicU64::new(0),
```

  with, immediately before `let members = …` in `Edge::start`:

```rust
        let budget = budget_for(cfg.max_inflight);
```

- [ ] Add the budget methods to `impl Shared` (after `is_faulted`), plus the settle constant next to the other constants:

```rust
/// How long a handshaking reader waits for the driver to publish the smaller
/// share to the connections already here, before granting its own connection
/// anything.
///
/// Bounded, and short: the driver's idle park is capped at [`DRIVER_PARK`]
/// (1 ms), so this is normally microseconds. On the timeout path the new
/// connection is granted its share anyway and the momentary over-promise
/// lands in the budget's headroom — which is what the headroom is for.
const GRANT_SETTLE_TIMEOUT: Duration = Duration::from_millis(250);
```

```rust
    /// Every ready connection's current share.
    fn current_grant(&self) -> u32 {
        grant_for(self.live.load(Ordering::Acquire), self.budget, self.cfg.per_conn_inflight)
    }

    /// Ask the driver to republish the share. Idempotent and free.
    fn request_regrant(&self) {
        self.regrant.store(true, Ordering::Release);
    }

    /// Count a handshaken connection into the budget and ask for a
    /// republication, so the connections already here give up their part of
    /// its share.
    fn join(&self, conn: &Conn) {
        if conn.mark_counted() {
            self.live.fetch_add(1, Ordering::AcqRel);
            self.request_regrant();
        }
    }

    /// Take a departed connection back out. Its share is freed, so the
    /// remaining connections are owed a bigger one — an increase, which rides
    /// their next `RESPONSE` or the idle `STATUS` timer rather than a push.
    fn leave(&self, conn: &Conn) {
        if conn.clear_counted() {
            self.live.fetch_sub(1, Ordering::AcqRel);
            self.request_regrant();
        }
    }

    /// Wait (briefly, boundedly) until the driver has published a share at
    /// least as new as `from_gen`.
    fn await_settled(&self, from_gen: u64) {
        let deadline = Instant::now() + GRANT_SETTLE_TIMEOUT;
        while self.grant_gen.load(Ordering::Acquire) == from_gen {
            if self.stopping() || Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    /// Recompute every ready connection's share and tell the ones whose window
    /// SHRANK, right away. Driver thread only — it is the only thread that may
    /// write on a connection other than its own.
    fn push_grants(&self) {
        let grant = self.current_grant();
        let mut dead: Vec<u32> = Vec::new();
        self.table.for_each(|c| {
            if !c.is_ready() {
                return;
            }
            match c.set_ceiling(grant) {
                CeilingChange::Same => {}
                CeilingChange::Raised => {
                    self.stats.grant_changes.fetch_add(1, Ordering::Relaxed);
                }
                CeilingChange::Lowered => {
                    self.stats.grant_changes.fetch_add(1, Ordering::Relaxed);
                    if !self.write_status(c) {
                        dead.push(c.idx);
                    }
                }
            }
        });
        for idx in dead {
            self.table.remove(idx);
        }
        self.grant_gen.fetch_add(1, Ordering::Release);
    }
```

- [ ] `write_status` must report a dead socket. Change its signature and tail in `edge.rs`:

```rust
    /// Write a standalone `STATUS`. Silently does nothing on a connection whose
    /// handshake has not completed — an unsolicited frame before `HELLO_OK`
    /// would fail the peer's dial (see `Conn::ready`). Returns `false` if the
    /// connection died on the write.
    fn write_status(&self, conn: &Conn) -> bool {
        if !conn.is_ready() {
            return true;
        }
        let mut out = Vec::new();
        Status { acked_seq: conn.acked_seq(), credits: conn.credits() }.encode(&mut out);
        self.stats.status_frames.fetch_add(1, Ordering::Relaxed);
        conn.write(conn.hdr(FrameType::Status, 0, 0), &out, self.now_ns())
    }
```

  and in `periodic` change `shared.write_status(c);` to `let _ = shared.write_status(c);`.

- [ ] Wire the connect path. In `handshake`, replace the `HelloOk` block (currently `let leader = send.leader_hint(); … conn.set_ready(); true`) with:

```rust
    // Join the budget BEFORE granting anything, and wait for the driver to
    // have taken this connection's share back off the connections already
    // here. Only then is a grant computed and put on the wire — so a client
    // is never told a number the edge has not already made room for, and the
    // sum of outstanding grants never exceeds the budget even for an instant.
    let gen = shared.grant_gen.load(Ordering::Acquire);
    shared.join(conn);
    shared.await_settled(gen);
    let grant = shared.current_grant();
    conn.set_ceiling(grant);

    let leader = send.leader_hint();
    let leader_addr = leader.and_then(|id| shared.gateway_of(id)).unwrap_or("");
    let mut out = Vec::new();
    HelloOk { credits: grant, leader, leader_addr }.encode(&mut out);
    if !conn.write(conn.hdr(FrameType::HelloOk, 0, h.seq), &out, shared.now_ns()) {
        return false;
    }
    // Only now may the edge write on its own initiative (the STATUS timer),
    // and only now does this connection's grant count towards the budget as
    // far as `grants_for_tests` and `push_grants` are concerned.
    conn.set_ready();
    true
```

- [ ] Wire the disconnect path. In `reader`, replace the final `shared.table.remove(conn.idx);` with:

```rust
    // Order matters: out of the table FIRST, so this connection's grant is
    // invisible before `leave` lets the survivors grow into its share — the
    // reverse order would over-promise the budget for as long as the driver's
    // republication took.
    shared.table.remove(conn.idx);
    shared.leave(&conn);
```

- [ ] Fix the `relax` call site in `complete` (`edge.rs` ~1328):

```rust
    let credits_up = conn.relax();
```

- [ ] Import `CeilingChange` in `edge.rs`: change `use crate::conn::{Conn, ConnTable};` to `use crate::conn::{CeilingChange, Conn, ConnTable};`.

- [ ] Add the test hook to `impl Edge`, next to `fault_for_tests`:

```rust
    /// Every **ready** connection's `(idx, grant)` right now, sorted by index.
    ///
    /// "Grant" is the connection's live credit figure — what the client is
    /// actually allowed to have outstanding — not its ceiling, so a squeezed
    /// connection reports the smaller number. The sum of these is the quantity
    /// the budget bounds.
    ///
    /// Behind `test-util` for the same reason as [`Edge::fault_for_tests`]:
    /// hiding a method from rustdoc does not stop anything calling it, and the
    /// build, not the documentation, is what says who may.
    #[cfg(feature = "test-util")]
    pub fn grants_for_tests(&self) -> Vec<(u32, u32)> {
        let mut v = Vec::new();
        self.shared.table.for_each(|c| {
            if c.is_ready() {
                v.push((c.idx, c.credits()));
            }
        });
        v.sort_unstable();
        v
    }
```

- [ ] Add the (still unpublished) `grant_changes` cell so this task compiles: in `StatCells` add `grant_changes: AtomicU64,`, in `snapshot()` add `grant_changes: self.grant_changes.load(Ordering::Relaxed),`, and in `EdgeStats` add:

```rust
    /// Times a connection's grant was recomputed to a **different** value —
    /// the edge redividing its budget as connections come and go. Counted per
    /// connection per change, both directions.
    pub grant_changes: u64,
```

- [ ] Drive it from the driver. In `driver`, add after each `drain_once` outcome — replace the two lines noted:

```rust
        let n = drain_once(&shared, &send, &mut poll);
        if n > 0 {
            idle = 0;
            regrant_tick(&shared);
```

  and, on the idle path, immediately after `leader_tick(&shared, &send, &mut watch);`:

```rust
        regrant_tick(&shared);
```

  with the helper beside `leader_tick`:

```rust
/// Republish the grant share if a connect or a disconnect has changed it.
///
/// Cheap by construction on the no-change path — one atomic swap — which is
/// every pass but the ones right after a connection arrives or leaves. Runs
/// ungated on both the busy and the idle path so a waiting handshake is
/// released within one driver iteration (bounded by [`DRIVER_PARK`]).
fn regrant_tick(shared: &Arc<Shared>) {
    if shared.regrant.swap(false, Ordering::AcqRel) {
        shared.push_grants();
    }
}
```

- [ ] Run: `cargo test -p uc_gateway` → expect all green, including `the_sum_of_grants_never_exceeds_the_edges_budget` and the four new `conn.rs` unit tests.
- [ ] Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings` → expect no warnings.
- [ ] Commit:

```sh
git add uc_gateway/src/conn.rs uc_gateway/src/edge.rs uc_gateway/tests/
git commit -m "feat(gateway): global outstanding-grant budget at the edge (spec §5.1)

Shared gains budget (Engine window - 1/8) + live; each connection gets
clamp(budget/live, 1, per_conn_inflight) as a dynamic Conn::ceiling that
relax() now aims at. A handshake joins the budget and WAITS for the driver
to take its share back off the connections already here before HELLO_OK
carries a grant, so the sum never exceeds the budget even transiently;
a disconnect leaves the table before it leaves the budget, for the same
reason. Test: 6 connections, sampled every 200us, sum <= 56."
```

---

### Task 3: publish a reduction as `STATUS`, and send one on `squeeze`

**Files:**
- Modify `uc_gateway/src/edge.rs` — `dispatch`'s `Backpressure` arm (~1090-1096).
- Modify `uc_gateway/tests/credits.rs` — test (ii).

**Interfaces:**
- Consumes: `Shared::write_status`, `Shared::push_grants` (Task 2).
- Produces: no new signatures; a new `STATUS` call site.

**Steps:**

- [ ] Failing test first — append to `uc_gateway/tests/credits.rs`:

```rust
/// (spec §5.4, ii) A connect shrinks everyone's grant, and the client learns
/// the smaller number from a standalone `STATUS` — **before** any `RESPONSE`
/// would have carried it. Driven raw because the property is about which
/// frame arrives when, on a connection that is deliberately idle: a
/// `RemoteClient` would only report "my window changed", never "it changed
/// without me asking anything".
#[test]
fn a_new_connection_shrinks_the_grant_and_status_says_so_unprompted() {
    use uc_remote::frame::{FrameType, Status};

    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc_service::ServiceBuilder::new(
        uc_service::ServiceConfig::new(&dir, common::APP),
        uc_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    // budget 56, cap 32: alone the first connection holds 32, with a second
    // one 28 each.
    let edge = Edge::start(edge_config(&dir, 64, 32)).unwrap();

    let mut first = common::dial_raw(edge.local_addr());
    common::send_hello(&mut first, 0xAAAA, common::APP);
    let (_, hello_ok) = common::read_until_frame(&mut first, FrameType::HelloOk,
                                                 Duration::from_secs(5))
        .expect("HELLO_OK");
    let granted = uc_remote::frame::HelloOk::decode(&hello_ok).unwrap().credits;
    assert_eq!(granted, 32, "the only connection gets the whole budget, capped at per_conn");

    // The first connection sends NOTHING from here on. Whatever it hears next
    // is unprompted.
    let mut second = common::dial_raw(edge.local_addr());
    common::send_hello(&mut second, 0xBBBB, common::APP);
    assert!(common::read_until(&mut second, FrameType::HelloOk, Duration::from_secs(5)).is_some());
    let (_, body) = common::read_until_frame(&mut first, FrameType::Status,
                                             Duration::from_secs(5))
        .expect("no STATUS reached the idle client after its share shrank");
    let st = Status::decode(&body).unwrap();
    assert_eq!(st.credits, 28, "the STATUS must carry the SMALLER absolute grant");
    assert!(edge.stats().grant_changes >= 1, "stats: {:?}", edge.stats());

    drop(first);
    drop(second);
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
```

- [ ] Run: `cargo test -p uc_gateway --test credits -- a_new_connection_shrinks` → expect PASS already if Task 2 is complete (`push_grants` writes the STATUS). **If it fails**, the cause is `write_status` not being reached — diagnose before touching the squeeze path.

- [ ] Now the squeeze call site (spec §5.2: "STATUS is also sent on `squeeze` — today a reduction reaches the client only on the next RESPONSE; `edge.rs` has no call site"). In `dispatch`'s `Err(SubmitError::Backpressure)` arm, extend the `if !squeezed` block:

```rust
            Err(SubmitError::Backpressure) => {
                if !squeezed {
                    squeezed = true;
                    shared.stats.backpressure_events.fetch_add(1, Ordering::Relaxed);
                    conn.squeeze();
                    // Tell the client its window just halved, rather than
                    // letting it find out from the next RESPONSE — by which
                    // point it has already sent into a window the edge cannot
                    // honour. This is the reader writing on its OWN
                    // connection, so it can neither stall the driver nor
                    // touch anyone else's socket. Once per request, not once
                    // per ladder turn: `squeezed` gates both.
                    if !shared.write_status(conn) {
                        return false;
                    }
                }
```

- [ ] Add the regression that the squeeze path really writes one — append to `uc_gateway/tests/credits.rs`, inside `a_squeezed_window_still_resolves_every_request`, replacing its `let es = edge.stats();` block tail:

```rust
    let es = edge.stats();
    assert_eq!(es.responses, 2 * PER_CLIENT, "every request resolved: {es:?}");
    assert_eq!(es.retries, 0, "backpressure is absorbed by credits, never bounced: {es:?}");
    // Not asserted as > 0 — whether the shared window actually fills is a
    // scheduling race (see the sibling test). What IS asserted: if it fired,
    // the client was told, unprompted.
    if es.backpressure_events > 0 {
        assert!(
            es.status_frames >= es.backpressure_events,
            "every squeeze owes the client a STATUS: {es:?}"
        );
    }
```

- [ ] Run: `cargo test -p uc_gateway --test credits` → expect all green.
- [ ] Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings` → expect no warnings.
- [ ] Commit:

```sh
git add uc_gateway/src/edge.rs uc_gateway/tests/credits.rs
git commit -m "feat(gateway): push a shrinking grant as STATUS, including on squeeze

A reduction reaches every ready connection through the driver's regrant
pass (Task 2) and, for the reactive ladder, from the reader on its own
socket the moment Conn::squeeze fires — the call site spec M12 §4.2 asked
for and edge.rs never had. Test: an idle raw client hears STATUS{28}
unprompted when a second connection joins."
```

---

### Task 4: `HELLO_OK` carries the live grant; a disconnect gives the share back

**Files:**
- Modify `uc_gateway/tests/credits.rs` — tests (iii) and (iv).

**Interfaces:** none new — this task is the acceptance evidence for Task 2's connect/disconnect wiring.

**Steps:**

- [ ] Failing test first — append to `uc_gateway/tests/credits.rs`:

```rust
/// (spec §5.4, iii) A disconnect gives its share back to the survivors, and
/// `HELLO_OK` on a later dial carries the *current* division of the budget,
/// never the config constant.
#[test]
fn a_disconnect_gives_its_share_back_and_hello_ok_carries_the_live_grant() {
    use uc_remote::frame::{FrameType, HelloOk};

    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc_service::ServiceBuilder::new(
        uc_service::ServiceConfig::new(&dir, common::APP),
        uc_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    // budget 56, cap 32: 1 -> 32, 2 -> 28, 4 -> 14.
    let edge = Edge::start(edge_config(&dir, 64, 32)).unwrap();
    let budget = uc_gateway::budget_for(64);

    let mut held = Vec::new();
    for i in 0..3u64 {
        let mut c = common::dial_raw(edge.local_addr());
        common::send_hello(&mut c, 0xC000 + i, common::APP);
        assert!(common::read_until(&mut c, FrameType::HelloOk, Duration::from_secs(5)).is_some());
        held.push(c);
    }
    // The FOURTH dial must be told 14, not the 32 the config would grant.
    let mut fourth = common::dial_raw(edge.local_addr());
    common::send_hello(&mut fourth, 0xC0FF, common::APP);
    let (_, body) =
        common::read_until_frame(&mut fourth, FrameType::HelloOk, Duration::from_secs(5))
            .expect("HELLO_OK");
    let credits = HelloOk::decode(&body).unwrap().credits;
    assert_eq!(credits, uc_gateway::grant_for(4, budget, 32), "HELLO_OK must carry the live share");
    assert_eq!(credits, 14);

    // Three of them go away; the survivor must climb back to the whole cap.
    drop(fourth);
    held.truncate(1);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let g = edge.grants_for_tests();
        if g.len() == 1 && g[0].1 == 32 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the survivor never got its share back: {g:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
    // …and the increase reaches the (idle) client on the STATUS timer, with no
    // request of its own.
    let (_, body) = common::read_until_frame(&mut held[0], FrameType::Status,
                                             Duration::from_secs(5))
        .expect("no STATUS carried the widened window");
    assert_eq!(uc_remote::frame::Status::decode(&body).unwrap().credits, 32);

    drop(held);
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
```

- [ ] Run: `cargo test -p uc_gateway --test credits -- a_disconnect_gives` → expect PASS (Task 2 built the mechanism; this is its acceptance). A failure here means `leave` is not reached — check the `reader` tail order.

- [ ] (iv) Tighten the existing two-client test. In `two_clients_stay_inside_the_credits_the_edge_grants`, replace the informational `println!` and the ceiling assertion:

```rust
    for (who, s) in [("a", a), ("b", b)] {
        // budget = 8 - 8/8 = 7, two connections -> 3 each (capped by 4).
        assert!(
            (1..=3).contains(&s.max_credits_seen),
            "client {who} was advertised {} credits; two connections share a budget of 7",
            s.max_credits_seen
        );
        assert_eq!(s.reconnects, 0, "client {who} should not have had to fail over: {s:?}");
        assert_eq!(s.unknown, 0, "client {who}: {s:?}");
    }

    let es = edge.stats();
    assert_eq!(es.connections, 2);
    assert_eq!(es.submits, 2 * PER_CLIENT, "every write reached the ring exactly once: {es:?}");
    assert_eq!(es.responses, 2 * PER_CLIENT, "one RESPONSE each: {es:?}");
    assert_eq!((es.redirects, es.unknown), (0, 0), "a healthy single-node leader: {es:?}");
    // THE POINT OF THE BUDGET. Before it, two connections were each granted
    // the full `per_conn_inflight` against a window that could not hold both,
    // and the reactive halve/relax ladder was the only thing keeping them
    // apart. With grants summing to 6 against a window of 8, the ladder is the
    // exception path and never runs at all here.
    assert_eq!(
        es.backpressure_events, 0,
        "grants sum inside the Engine window, so nothing may hit Backpressure: {es:?}"
    );
```

- [ ] Run: `cargo test -p uc_gateway --test credits` → expect all five tests green.
- [ ] Run: `cargo test -p uc_gateway` → expect the whole gateway suite green (`credits_wire.rs` included, on the moved helpers).
- [ ] Commit:

```sh
git add uc_gateway/tests/credits.rs
git commit -m "test(gateway): HELLO_OK grants the live share; a disconnect returns it

Plus the acceptance the budget exists for: the two-client test now asserts
backpressure_events == 0 at equal inflight — grants that sum inside the
Engine window mean the reactive halve/relax ladder never runs."
```

---

### Task 5: config validation — `per_conn_inflight <= budget`, and a warning past it

**Files:**
- Modify `uc_gateway/src/config.rs` — `ConfigError` (~39-64), `validate` (~136-178), tests (~233-241).
- Modify `uc_gateway/examples/hop_bench/main.rs` — `EdgeArgs::per_conn_inflight` default (~84).
- Modify `docs/reference/gateway-config.md` — `[limits]` table + sizing section.
- Modify `packaging/gateway.example.toml` — `[limits]` comments.

**Interfaces:**
- Produces: `ConfigError::PerConnExceedsBudget { per_conn: u32, budget: u32, max_inflight: u32 }`; `EdgeConfig::warnings(&self) -> Vec<String>`.
- Consumes: `budget_for` (Task 1).

**Steps:**

- [ ] Failing test first — append to `uc_gateway/src/config.rs`'s `mod tests`:

```rust
    #[test]
    fn per_conn_credits_may_not_exceed_the_grant_budget() {
        // The budget is the Engine window less its 1/8 headroom, so a
        // per-connection cap between the two is refused BY NAME rather than
        // silently over-promising the window on the very first connection.
        let c = EdgeConfig { max_inflight: 4096, per_conn_inflight: 4096, ..ok() };
        assert_eq!(
            c.validate(),
            Err(ConfigError::PerConnExceedsBudget {
                per_conn: 4096,
                budget: 3584,
                max_inflight: 4096
            })
        );
        let c = EdgeConfig { max_inflight: 4096, per_conn_inflight: 3584, ..ok() };
        assert_eq!(c.validate(), Ok(()), "exactly the budget is grantable");
        // The pre-existing, coarser check still fires first for a value over
        // the window itself.
        let c = EdgeConfig { max_inflight: 8, per_conn_inflight: 9, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::PerConnExceedsMax { per_conn: 9, max: 8 }));
    }

    #[test]
    fn a_connection_ceiling_above_the_budget_is_warned_about_not_refused() {
        // Legal — the grant simply floors at 1 for the connections past the
        // budget — but almost certainly not what the operator meant.
        let c = EdgeConfig { max_inflight: 64, per_conn_inflight: 8, max_connections: 4096, ..ok() };
        assert_eq!(c.validate(), Ok(()));
        let w = c.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(w[0].contains("max_connections"), "{}", w[0]);
        assert!(w[0].contains("56"), "the warning must state the budget: {}", w[0]);

        let quiet = EdgeConfig { max_connections: 16, ..c };
        assert!(quiet.warnings().is_empty());
        assert!(ok().warnings().is_empty(), "the defaults must not warn");
    }
```

- [ ] Run: `cargo test -p uc_gateway --lib` → expect failure: `no variant or associated item named 'PerConnExceedsBudget'`.

- [ ] Implement. Add to `ConfigError`:

```rust
    #[error("per_conn_inflight ({per_conn}) exceeds the edge's grant budget ({budget} = \
             max_inflight {max_inflight} less its 1/8 headroom): a single connection could \
             promise more than the Engine window can honour")]
    PerConnExceedsBudget { per_conn: u32, budget: u32, max_inflight: u32 },
```

  and to `validate`, immediately after the existing `PerConnExceedsMax` check:

```rust
        let budget = crate::budget_for(self.max_inflight);
        if self.per_conn_inflight > budget {
            return Err(ConfigError::PerConnExceedsBudget {
                per_conn: self.per_conn_inflight,
                budget,
                max_inflight: self.max_inflight,
            });
        }
```

  and the advisory, after `validate`:

```rust
    /// Configuration that is legal but probably not what was meant. Unlike
    /// [`EdgeConfig::validate`] these never refuse a start — they are printed
    /// once by the `uc2-gateway` binary and are otherwise inert.
    pub fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        let budget = crate::budget_for(self.max_inflight);
        if self.max_connections > budget {
            out.push(format!(
                "max_connections ({}) is above the edge's grant budget ({budget} = max_inflight \
                 {} less its 1/8 headroom): past {budget} simultaneous connections each one is \
                 granted the floor of 1 credit and the sum stops fitting the Engine window. \
                 Raise max_inflight or lower max_connections.",
                self.max_connections, self.max_inflight
            ));
        }
        out
    }
```

- [ ] Print the warnings once at startup. In `uc_gateway/src/bin/uc2-gateway.rs`, immediately before `Edge::start(...)`:

```rust
    for w in cfg.warnings() {
        eprintln!("uc2-gateway: warning: {w}");
    }
```

- [ ] Unblock the bench harness, which currently defaults `per_conn_inflight` to the whole window. In `uc_gateway/examples/hop_bench/main.rs`:

```rust
    /// Credits granted to every connection at HELLO_OK (`per_conn_inflight`).
    /// Must be at or under the edge's grant budget — `max_inflight` less its
    /// 1/8 headroom — or `Edge::start` refuses by name.
    #[arg(long, default_value_t = 1024)]
    pub per_conn_inflight: u32,
```

- [ ] Run: `cargo test -p uc_gateway --lib` → expect green. Run `cargo build -p uc_gateway --example hop_bench --release` → expect a clean build.
- [ ] Smoke the refusal end to end:

```sh
cargo run -q -p uc_gateway --example hop_bench --release -- edge \
  --instance-dir /home/claude/nonexistent --listen 127.0.0.1:0 \
  --max-inflight 4096 --per-conn-inflight 4096
```

  → expect the process to fail with `per_conn_inflight (4096) exceeds the edge's grant budget (3584 …)`, **not** an attach error (validation runs first in `Edge::start`).

- [ ] Update `docs/reference/gateway-config.md`. Replace the `per_conn_inflight` row's Refusal cell with:

```
| `per_conn_inflight` | u32 | `256` | credits granted to each connection at `HELLO_OK` — **an equal share of the edge's budget**, capped at this value; shrinks under backpressure and relaxes back up to the current share | `0` → `ZeroPerConnInflight`; greater than `max_inflight` → `PerConnExceedsMax { per_conn, max }`; greater than the **grant budget** (`max_inflight` less its 1/8 headroom) → `PerConnExceedsBudget { per_conn, budget, max_inflight }` |
```

  and replace the whole "### Sizing `per_conn_inflight` and `max_connections` (2.6.0)" section with:

```markdown
### The grant budget (2.7.0)

The edge holds **one** `Engine` inflight window (`max_inflight`) and divides
it across its connections instead of promising each one the same constant.
Two derived numbers:

- **budget** = `max_inflight` − `max_inflight / 8`. The 1/8 headroom is not a
  tuning dial and is not configurable: it is the slack that absorbs frames
  already on the wire when a grant shrinks.
- **grant** = `clamp(budget / live_connections, 1, per_conn_inflight)`, where
  `live_connections` counts the handshaken connections on this edge.

At the defaults that is a budget of `3584` and a grant of `256` (the cap)
for the first fourteen connections, `255` at fifteen, and so on down. A
connection is told its grant in `HELLO_OK`, and every later change reaches
it as an absolute `credits` value: a **reduction** is pushed as a standalone
`STATUS` before it can send into the smaller window, an **increase** rides
the next `RESPONSE` or the idle `STATUS` tick. `uc_gateway::budget_for` and
`uc_gateway::grant_for` are public if you would rather compute than read.

**What this replaces.** In `2.6.0` every connection was granted
`per_conn_inflight` in full and nothing counted the sum, so N connections
could promise N × 256 against a 4096-slot window and the only arbiter was
the `Engine` refusing submits — the reactive halve/relax ladder, per
connection, uncoordinated. That gap is closed; see
[the correction note](../notes/uc2-m12a-edge-flow-control-gap.md).

**The one case the budget does not cover:** past `live > budget` every grant
floors at `1` and the sum exceeds the budget again. `validate` **warns**
(it does not refuse — a floor of 1 still works, it is just miserable) when
`max_connections > budget`; the binary prints the warning at startup. Size
`max_connections` at or under the budget, or raise `max_inflight`.
```

  Delete the two link definitions (`[edgesat]`, `[cleanrun]`) at the end of that section — nothing references them any more.

- [ ] Update `packaging/gateway.example.toml`'s `[limits]` block:

```toml
[limits]
# The local Engine's inflight window, shared across every connection. The edge
# holds back 1/8 of it as headroom and divides the rest — the "grant budget" —
# equally across its live connections.
max_inflight = 4096
# CEILING on one connection's credits. The actual grant is
# clamp(budget / live_connections, 1, per_conn_inflight), where budget is
# max_inflight less its 1/8 headroom (3584 at the default above), so this
# value binds only while few connections are attached. A value above the
# budget is a startup REFUSAL (PerConnExceedsBudget); max_connections above
# the budget is a startup WARNING (every connection past it floors at 1
# credit). See docs/reference/gateway-config.md, "The grant budget".
per_conn_inflight = 256
```

- [ ] Run: `cargo test -p uc_gateway && cargo clippy -p uc_gateway --all-targets -- -D warnings` → expect green, no warnings.
- [ ] Commit:

```sh
git add uc_gateway/src/config.rs uc_gateway/src/bin/uc2-gateway.rs \
        uc_gateway/examples/hop_bench/main.rs \
        docs/reference/gateway-config.md packaging/gateway.example.toml
git commit -m "feat(gateway): refuse per_conn_inflight above the budget, warn past max_connections

PerConnExceedsBudget is a named startup refusal (a single connection must be
grantable in full); max_connections above the budget is a printed warning,
not a refusal — the grant floors at 1 there, which works and is miserable.
hop_bench's edge default drops 4096 -> 1024 so the harness stays startable.
Docs: gateway-config.md 'The grant budget', gateway.example.toml."
```

---

### Task 6: `grant_changes` in the stats line and the operator docs

**Files:**
- Modify `uc_gateway/src/bin/uc2-gateway.rs` — stats line (~93-108).
- Modify `docs/how-to/run-a-gateway.md` — "Stats line" section (~280-296).
- Modify `uc_gateway/examples/hop_bench/main.rs` — the per-second `edge:` line (~123-132).

**Interfaces:**
- Consumes: `EdgeStats::grant_changes` (added in Task 2).

**Steps:**

- [ ] Failing test first — append to `uc_gateway/src/edge.rs`'s `mod tests`:

```rust
    /// `grant_changes` counts redivisions, in both directions, per connection.
    /// A stat nobody can reach from `EdgeStats` is a stat nobody will read.
    #[test]
    fn the_stats_snapshot_exposes_grant_changes() {
        let s = EdgeStats::default();
        assert_eq!(s.grant_changes, 0);
        let cells = StatCells::default();
        cells.grant_changes.fetch_add(3, Ordering::Relaxed);
        assert_eq!(cells.snapshot().grant_changes, 3);
    }
```

- [ ] Run: `cargo test -p uc_gateway --lib -- the_stats_snapshot_exposes_grant_changes` → expect PASS if Task 2 added the field; a failure means the field or the snapshot line is missing.

- [ ] Add it to the binary's stats line:

```rust
            eprintln!(
                "uc2-gateway: conns={} submits={} queries={} responses={} redirects={} \
                 retries={} unknown={} backpressure={} grant_changes={} leader_changes={} \
                 status={} refused_busy={}",
                s.connections,
                s.submits,
                s.queries,
                s.responses,
                s.redirects,
                s.retries,
                s.unknown,
                s.backpressure_events,
                s.grant_changes,
                s.leader_changes,
                s.status_frames,
                s.refused_busy,
            );
```

- [ ] Add it to `hop_bench`'s per-second edge line, so a gate rung shows the budget moving:

```rust
        println!(
            "edge: conns={} submits/s={} responses/s={} backpressure/s={} retries/s={} \
             unknown/s={} status/s={} grants/s={}",
            s.connections,
            s.submits - last.submits,
            s.responses - last.responses,
            s.backpressure_events - last.backpressure_events,
            s.retries - last.retries,
            s.unknown - last.unknown,
            s.status_frames - last.status_frames,
            s.grant_changes - last.grant_changes,
        );
```

- [ ] Update `docs/how-to/run-a-gateway.md`'s "Stats line" section, replacing its first paragraph:

```markdown
`uc2-gateway` prints one stats line to stderr every 10 s (100 ticks of the
main loop's 100 ms polling interval), exactly these fields in order:
`conns` (connections accepted), `submits`, `queries`, `responses`,
`redirects`, `retries`, `unknown`, `backpressure` (squeeze events),
`grant_changes` (times a connection's share of the budget was recomputed to
a different value — it moves when connections arrive and leave, and should
be quiet otherwise), `leader_changes` (observed leader-watch transitions),
`status` (standalone `STATUS` frames written), and `refused_busy` (dials
turned away at the `max_connections` ceiling). `EdgeStats` also tracks
`leader_changed_frames` (`LEADER_CHANGED` frames actually written, which can
differ from `leader_changes` — a transition to an unresolvable leader hint
is observed but not announced) but the reference binary does not print it;
read it via `Edge::stats()` if you embed the library yourself.
```

- [ ] Run: `cargo test -p uc_gateway && cargo build -p uc_gateway --example hop_bench --release && cargo clippy -p uc_gateway --all-targets -- -D warnings` → expect green.
- [ ] Commit:

```sh
git add uc_gateway/src/edge.rs uc_gateway/src/bin/uc2-gateway.rs \
        uc_gateway/examples/hop_bench/main.rs docs/how-to/run-a-gateway.md
git commit -m "feat(gateway): export grant_changes in the stats line

EdgeStats.grant_changes counts budget redivisions; the daemon's stats line
and hop_bench's per-second edge line both print it, and the how-to says what
a quiet number means."
```

---

### Task 7: the M13 gate doc — bars a–f, pre-committed, results empty

**Files:**
- Create `docs/benchmarks/uc2-m13-gate-2026-08-24.md`.

**Interfaces:** none — a document. Its bars are copied **verbatim** from spec §2 and may never be edited to match a result.

**Steps:**

- [ ] Create `docs/benchmarks/uc2-m13-gate-2026-08-24.md` with exactly this content:

```markdown
# uc2 M13 gate — remote path performance & flow control

**Date:** 2026-08-24

> **Decide rule committed before any run.** The bar table below is copied
> verbatim from the design spec's §2
> (`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`), which
> was written and reviewed before a line of M13 code existed. This document's
> own commit — the bar, with every result cell empty — lands **before** the
> commit that produces any result, per the honest-failure protocol carried
> forward from M7/M9/M10/M11/M12. Nothing in the bar may be edited to match a
> result: a run that misses the bar is recorded as a FAIL and keeps the bar.

## What the gate measures

Spec: `docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`
(§1 the three defects, §3 the client, §4 the ring, §5 the edge budget).
Baseline and root causes: `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`
(the per-hop isolation bench) and
`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`.

M13 makes the remote path — `client → TCP → Edge → shmem Engine → node` —
run at the cluster's speed and **degrade rather than collapse** under more
connections than the host has cores. Three fixes, measured together: a
rebuilt `uc_remote` client (track B), per-record commit in
`uc_protocol::ring::mpsc` (track A), and a global outstanding-grant budget
at the edge (track C). No consensus change, no node↔node wire change, no
cnc-page change; the remote wire protocol stays v1.

M12 gate **row 2 is closed by reference to row b** below: the 0.8× bar it
carried compared one TCP client to one shmem client, which the hop bench
showed to be the wrong comparison (see that document's §"What this means for
M13", point 5).

## The bar

Pre-committed, verbatim from spec §2. Rows a–d are **fleet only** (4×
`c6id.2xlarge`, `hop_bench` + `bench-infra/scripts/m13_hop_bench.py`); rows
e and f are local/CI.

| row | measure | bar | result |
|---|---|---|---|
| a | ONE new client connection through the real edge into the real 3-node cluster (`hop_bench remote-load` rebuilt on the new client, conns=1, inflight 1024) vs the direct `Engine` arm on the same generation | **≥ 0.5× direct** resp/s | *not yet run* |
| b | N-connection aggregate through the edge (new client), best rung ≤ 16 | **≥ 0.75× direct** | *not yet run* |
| c | Ladder N = 1,2,4,8,16 at 1024 inflight on the co-located host, with the ring fix + edge budget | **monotone, no collapse**: 0 lost, p99 bounded (< 1 s at every rung), no rung > 20% below the previous rung | *not yet run* |
| d | N local `Engine`s into one node on an oversubscribed host (`engine-load --engines 1,2,4,8` on the 8-vCPU server host, and locally on 4 vCPU) | **≤ linear degradation**: resp/s at N engines ≥ (cores / busy threads) × single-engine resp/s × 0.5, never below 10% of single-engine | *not yet run* |
| e | Ring correctness: existing 73 `uc_protocol` tests + new preemption test + loom model | green | *not yet run* |
| f | Correctness capstones on the new client: `remote_lin` (envelope on/off), `uc_gateway` tests, `client_fake_edge` suite ported | green | *not yet run* |

Reference numbers from the hop bench, for scale (they are **not** bars):
direct arm 1.9–2.6 M/s; raw client through edge+cluster 1.14 M/s (0.6×) at
N = 1 and 1.43 M/s (0.75×) at N = 2.

### Reading row d's rule

`busy threads` is `2N + 1`: one submitter and one poll thread per `Engine`,
plus the sink's single busy-poll drain thread. The scaling factor
`cores / busy threads` is **clamped at 1.0** — at N = 1 on an 8-vCPU host it
would otherwise read `8/3 = 2.67`, i.e. demand that one engine go 33% faster
than itself. The rule is therefore: at every rung,
`rps(N) ≥ min(1, cores / (2N+1)) × rps(1) × 0.5` **and** `rps(N) ≥ 0.10 × rps(1)`.

## How it is run

Rows a–d, one command, on a provisioned 4-host fleet (`bench-infra/`):

```bash
python3 bench-infra/scripts/m13_hop_bench.py --fleet --arms gate --secs 10
```

Topology, unchanged from the hop bench: `hosts[0..3]` are the real 3-voter
cluster (raw state machine, envelope off, admission 256 KiB) plus the client
host `hosts[3]`; the `hop_bench` edge runs on the elected leader. Rows a, b
and c come from one `remote-load` ladder (N = 1,2,4,8,16 at inflight 1024)
against one `engine-load` direct-arm reference (G) taken on the **same
cluster generation and the same leader**; row d runs afterwards on the server
host alone, `engine-load --engines 1,2,4,8` into `dummy-node`.

Every point prints one `HOP-JSON {…}` line; every row prints one
`GATE-JSON {…}` line and one `[PASS]/[FAIL] <row> — <detail>` line, and the
process exits non-zero if any adjudicated row failed — a green terminal is
not a PASS, the exit code is. The row arithmetic is separately unit-tested
against canned points:

```bash
python3 bench-infra/scripts/m13_hop_bench.py --selftest
```

Rows e and f are local/CI, and the driver prints these commands rather than
running them:

```bash
cargo test -p uc_protocol                                   # row e: ring, incl. the preemption test
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release loom_   # row e: the loom model
cargo test -p uc_remote                                    # row f: client_fake_edge, ported
cargo test -p uc_gateway                                   # row f: edge incl. the grant budget
cargo test -p uc_node --test remote_lin                    # row f: the capstone, envelope on/off
```

The dev-box smoke (`cargo run -p uc_gateway --release --example hop_bench --
local --secs 3 --conns 1,4`) is unchanged and is **not** a gate row: this box
is 4 vCPU and every arm contends for the same cores (see "Dev box is not a
bench" below).

## Dev box is not a bench

Carried forward verbatim from M12's gate doc, and binding here: the
development box is small, shared and oversubscribed. Local runs of any arm in
this document are **smoke** — they prove a harness runs and an ordering
holds, never a rate. Every rate bar above is adjudicated on the fleet and
nowhere else, and no bar moves because a local run went red.

## Honest-failure protocol

Adopted verbatim from M7/M9/M10/M11/M12:

- The driver prints the bar and exits non-zero on FAIL. A green terminal is
  not a PASS; the exit code is.
- Bar and result are recorded in **separate commits, bar first** — this
  document, with empty result cells, before the harness that fills them.
- A FAIL is diagnosed before it is re-run. Harness defects and genuine
  product properties are separated, and **both** are recorded here.
- Rows a–d are not locally adjudicable. Local numbers, where printed at all,
  are smoke and are labelled as such.
- `v2.7.0` tags only when rows a–f are recorded here with their real results
  — a separate, user-approved step, not performed by this document or by
  landing any M13 branch.

## Results

*Not yet run.* Each row's result lands here as its own section, with the raw
`GATE-JSON`/`HOP-JSON` lines, the fleet's instance types and region, and the
`terraform destroy` confirmation, exactly as the M12 rows record theirs.

## Links

- Design spec: `docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`
- Baseline measurement: `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`
- The convoy, explained: `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`
- The M12a flow-control gap this milestone closes:
  `docs/notes/uc2-m12a-edge-flow-control-gap.md`
- M12 gate (row 2, closed by reference to row b):
  `docs/benchmarks/uc2-m12-gate-2026-08-22.md`
```

- [ ] Verify no result was smuggled in: `grep -n "resp/s\|M/s" docs/benchmarks/uc2-m13-gate-2026-08-24.md` → every hit must be inside the bar text or the explicitly-labelled reference line, and every `result` cell must read `*not yet run*`.
- [ ] Commit **before any harness change**, per the protocol:

```sh
git add docs/benchmarks/uc2-m13-gate-2026-08-24.md
git commit -m "docs(gate): M13 gate doc — bars a-f pre-committed, results empty

Copied verbatim from spec §2 before the harness that adjudicates them
exists, per the honest-failure protocol. M12 row 2 is closed by reference
to row b. Rows a-d are fleet-only; e/f are cargo test."
```

---

### Task 8: `m13_hop_bench.py --arms gate` (and `--selftest` for its arithmetic)

**Files:**
- Modify `bench-infra/scripts/m13_hop_bench.py` — module doc (~19-33), imports (~46-49), new `arm_gate` + four pure verdict functions + `selftest`, `main` (~413-486).

**Interfaces:**
- Consumes: `m12_fleet_gate.Verdict`, `m12.start_cluster`, `m12.stop_cluster`, `m12.wipe_dirs`, `m6.wait_leader`, `run_point`, `start_hop_edge`, `start_dummy_node`, `kill_unit`, `ssh`.
- Produces: `verdict_row_a/b/c/d(points, …) -> Verdict`, `busy_threads(n) -> int`, `arm_gate(m12hosts, hophosts, a, points, verdicts)`, `selftest() -> int`.

**Steps:**

- [ ] Failing test first — add the selftest at the bottom of `bench-infra/scripts/m13_hop_bench.py`, above `main`:

```python
# ---------------------------------------------------------------- selftest

def _pt(hop, n, rps, p99=1.0, lost=0, ok=True):
    return {"label": f"{hop} n={n}", "arm": "remote", "ok": ok, "hop": hop, "n": n,
            "rps": rps, "p50_ms": 0.1, "p95_ms": p99 / 2, "p99_ms": p99, "lost": lost,
            "retried": 0, "sends": int(rps), "inflight": 1024,
            "server_host_cpu_pct": None, "server_proc_cpu_pct": None,
            "client_host_cpu_pct": None, "client_proc_cpu_pct": None}


def selftest():
    """Feed canned HOP-JSON points through the row arithmetic and check the
    verdicts. Runs nowhere near a fleet; `--selftest` is the whole invocation.
    """
    fails = []

    def check(name, got, want):
        if got != want:
            fails.append(f"{name}: verdict {got}, expected {want}")

    direct = 2_000_000.0
    ladder_ok = [_pt("gate-c", 1, 1_100_000.0), _pt("gate-c", 2, 1_500_000.0),
                 _pt("gate-c", 4, 1_400_000.0), _pt("gate-c", 8, 1_200_000.0),
                 _pt("gate-c", 16, 1_000_000.0)]

    # --- row a: one connection vs the direct arm, bar 0.5x
    check("a pass", verdict_row_a(ladder_ok, direct).passed, True)
    check("a fail", verdict_row_a([_pt("gate-c", 1, 900_000.0)], direct).passed, False)
    check("a missing", verdict_row_a([], direct).passed, False)
    check("a no direct", verdict_row_a(ladder_ok, 0.0).passed, False)

    # --- row b: best rung vs the direct arm, bar 0.75x
    check("b pass", verdict_row_b(ladder_ok, direct).passed, True)      # 1.5M/2.0M = 0.75
    check("b fail", verdict_row_b([_pt("gate-c", 2, 1_400_000.0)], direct).passed, False)

    # --- row c: 0 lost, p99 < 1000 ms, no rung > 20% below the previous
    check("c pass", verdict_row_c(ladder_ok).passed, True)
    check("c lost", verdict_row_c(ladder_ok[:-1] + [_pt("gate-c", 16, 1_000_000.0, lost=3)]).passed,
          False)
    check("c p99", verdict_row_c(ladder_ok[:-1] + [_pt("gate-c", 16, 1_000_000.0, p99=1500.0)]).passed,
          False)
    collapse = ladder_ok[:-1] + [_pt("gate-c", 16, 900_000.0)]          # 0.75x of the 8 rung
    check("c collapse", verdict_row_c(collapse).passed, False)
    check("c one rung", verdict_row_c([_pt("gate-c", 1, 1_000_000.0)]).passed, True)
    check("c empty", verdict_row_c([]).passed, False)

    # --- row d: <= linear on an 8-core host, busy = 2N+1, factor clamped at 1
    d_ok = [_pt("gate-d", 1, 2_800_000.0), _pt("gate-d", 2, 2_240_000.0),
            _pt("gate-d", 4, 1_400_000.0), _pt("gate-d", 8, 950_000.0)]
    check("d pass", verdict_row_d(d_ok, cores=8).passed, True)
    d_convoy = d_ok[:-1] + [_pt("gate-d", 8, 5_000.0)]                  # the M12 collapse
    check("d collapse", verdict_row_d(d_convoy, cores=8).passed, False)
    d_floor = d_ok[:-1] + [_pt("gate-d", 8, 200_000.0)]                 # 7.1% of single
    check("d floor", verdict_row_d(d_floor, cores=8).passed, False)
    check("d no base", verdict_row_d(d_ok[1:], cores=8).passed, False)
    check("busy", busy_threads(4), 9)

    for f in fails:
        print(f"SELFTEST FAIL {f}")
    print(f"SELFTEST: {len(fails)} failure(s)")
    return 1 if fails else 0
```

- [ ] Run: `python3 bench-infra/scripts/m13_hop_bench.py --selftest` → expect `NameError: name 'verdict_row_a' is not defined` (and no `--selftest` flag yet — an argparse error first).

- [ ] Implement the pure verdict functions, above the selftest:

```python
# --------------------------------------------------------------- gate rows
#
# Every row is a PURE function of the points list, so `--selftest` can
# adjudicate canned numbers without a fleet. The bars are spec §2's, copied
# into the defaults below and NOT overridable from the command line — a bar
# you can move from the shell is not a bar.

BAR_A_RATIO = 0.5      # one connection through the edge vs the direct arm
BAR_B_RATIO = 0.75     # best N-connection aggregate vs the direct arm
BAR_C_P99_MS = 1000.0  # p99 bound at every rung
BAR_C_RUNG = 0.8       # no rung more than 20% below the previous one
BAR_D_FRAC = 0.5       # row d's "x 0.5" term
BAR_D_FLOOR = 0.10     # …and its never-below-10%-of-single-engine floor


def busy_threads(n):
    """Busy threads at N engines: one submitter and one poll thread each, plus
    the sink's single busy-poll drain thread."""
    return 2 * n + 1


def _ladder(points, hop):
    """The usable rungs of one ladder, keyed by N, lowest first."""
    by_n = {}
    for p in points:
        if p.get("hop") == hop and p.get("ok") and p.get("rps"):
            by_n[p["n"]] = p
    return [by_n[n] for n in sorted(by_n)]


def verdict_row_a(points, direct_rps):
    row = "a one connection through the edge vs direct Engine"
    if not direct_rps:
        return Verdict(row, False, "no direct-arm reference (G) was measured")
    rungs = [p for p in _ladder(points, "gate-c") if p["n"] == 1]
    if not rungs:
        return Verdict(row, False, "no single-connection rung was measured")
    rps = rungs[0]["rps"]
    ratio = rps / direct_rps
    return Verdict(row, ratio >= BAR_A_RATIO,
                   f"{rps:.0f}/s vs direct {direct_rps:.0f}/s = {ratio:.3f}x "
                   f"(bar >= {BAR_A_RATIO}x)")


def verdict_row_b(points, direct_rps):
    row = "b N-connection aggregate vs direct Engine"
    if not direct_rps:
        return Verdict(row, False, "no direct-arm reference (G) was measured")
    rungs = _ladder(points, "gate-c")
    if not rungs:
        return Verdict(row, False, "no ladder rung was measured")
    best = max(rungs, key=lambda p: p["rps"])
    ratio = best["rps"] / direct_rps
    return Verdict(row, ratio >= BAR_B_RATIO,
                   f"best rung N={best['n']} {best['rps']:.0f}/s vs direct "
                   f"{direct_rps:.0f}/s = {ratio:.3f}x (bar >= {BAR_B_RATIO}x)")


def verdict_row_c(points):
    row = "c ladder monotone, no collapse"
    rungs = _ladder(points, "gate-c")
    if not rungs:
        return Verdict(row, False, "no ladder rung was measured")
    bad = []
    lost = 0
    for p in rungs:
        lost += p.get("lost") or 0
        if (p.get("lost") or 0) > 0:
            bad.append(f"N={p['n']} lost {p['lost']}")
        p99 = p.get("p99_ms")
        if p99 is None or p99 >= BAR_C_P99_MS:
            bad.append(f"N={p['n']} p99 {p99} ms (bar < {BAR_C_P99_MS})")
    for prev, cur in zip(rungs, rungs[1:]):
        if cur["rps"] < BAR_C_RUNG * prev["rps"]:
            bad.append(f"N={cur['n']} {cur['rps']:.0f}/s is "
                       f"{cur['rps'] / prev['rps']:.2f}x of N={prev['n']} "
                       f"(bar >= {BAR_C_RUNG})")
    detail = (f"{len(rungs)} rung(s) N={[p['n'] for p in rungs]}, lost {lost}, "
              f"p99 max {max((p.get('p99_ms') or 0) for p in rungs):.1f} ms")
    if bad:
        detail += " — " + "; ".join(bad)
    return Verdict(row, not bad, detail)


def verdict_row_d(points, cores):
    row = "d N engines on an oversubscribed host"
    rungs = _ladder(points, "gate-d")
    base = next((p for p in rungs if p["n"] == 1), None)
    if base is None:
        return Verdict(row, False, "no single-engine reference rung was measured")
    b = base["rps"]
    bad = []
    for p in rungs:
        # Clamped at 1.0: at N=1 on 8 cores the raw factor is 8/3, which would
        # demand one engine outrun itself. See the gate doc, "Reading row d".
        factor = min(1.0, cores / busy_threads(p["n"]))
        expect = factor * b * BAR_D_FRAC
        floor = b * BAR_D_FLOOR
        if p["rps"] < expect or p["rps"] < floor:
            bad.append(f"N={p['n']} {p['rps']:.0f}/s < max(linear {expect:.0f}, "
                       f"floor {floor:.0f})")
    detail = (f"{cores} cores, single-engine {b:.0f}/s, rungs "
              + ", ".join(f"N={p['n']}:{p['rps']:.0f}" for p in rungs))
    if bad:
        detail += " — " + "; ".join(bad)
    return Verdict(row, not bad, detail)
```

- [ ] Add `Verdict` to the `m12_fleet_gate` import block (~line 46):

```python
from m12_fleet_gate import (  # noqa: E402
    ssh, start_unit, kill_unit, tail_log, parse_result, echo,
    sample_cpu_concurrently, wait_units_done, unit_log, Verdict,
)
```

- [ ] Run: `python3 bench-infra/scripts/m13_hop_bench.py --selftest` → still argparse-fails; that is expected until the flag lands two steps below.

- [ ] Implement the gate arm, after `arm_full`:

```python
def detect_cores(host, override):
    if override:
        return override
    r = ssh(host, "nproc", label="nproc")
    try:
        return int((r.stdout or "").strip().splitlines()[0])
    except (ValueError, IndexError):
        raise RuntimeError(f"could not read nproc on {host.public_ip}: {r.stdout!r}")


def arm_gate(m12hosts, hophosts, a, points, verdicts):
    """The M13 gate (spec §2, doc docs/benchmarks/uc2-m13-gate-2026-08-24.md).

    Rows a, b and c all come from ONE `remote-load` ladder through the real
    edge into the real 3-voter cluster, against ONE `engine-load` direct-arm
    reference taken on the SAME cluster generation and the same leader — the
    comparison M12's row 2 got wrong. Row d then runs on the server host
    alone against `dummy-node`. Rows e and f are cargo tests; their commands
    are printed, not run.
    """
    node_hosts = m12hosts[:3]
    C = hophosts[3]
    cores = detect_cores(hophosts[0], a.gate_cores)
    direct_rps = 0.0
    m12.wipe_dirs(node_hosts)
    m12.start_cluster(node_hosts, a, "off", raw_sm=True)
    try:
        leader = m6.wait_leader(node_hosts, list(range(3)), m12.LEADER_WAIT_SECS)
        if leader is None:
            raise RuntimeError("no serving leader in the real cluster")
        L, LH = node_hosts[leader], hophosts[leader]
        print(f"INFO gate: real cluster leader = n{leader} ({L.public_ip}), "
              f"{cores} cores on the server host", flush=True)

        # G — the direct arm, same generation, same leader. Rows a and b are
        # ratios against this and nothing else.
        g = run_point(
            f"GATE G direct engine→REAL cluster inflight={a.conn_inflight}", LH, LH,
            ["engine-load", "--instance-dir", L.dir, "--app-id", m12.APP,
             "--secs", str(a.secs), "--payload", str(a.payload),
             "--inflight", str(a.conn_inflight), "--engines", "1"],
            "engine", a.secs, a, edge_unit="node",
            extra={"hop": "gate-g", "inflight": a.conn_inflight, "n": 1})
        points.append(g)
        direct_rps = g.get("rps") or 0.0

        # Rows a/b/c — one remote-load ladder through the edge on the leader.
        members = ",".join(f"{i}@{h.private_ip}:{EDGE_PORT}" for i, h in enumerate(node_hosts))
        start_hop_edge(LH, L.dir, m12.APP, a.gate_edge_inflight, a.gate_edge_per_conn,
                       members=members)
        gw = f"{L.private_ip}:{EDGE_PORT}"
        for n in ladder(a.gate_conns):
            points.append(run_point(
                f"GATE abc remote→edge→REAL cluster conns={n} inflight={a.conn_inflight}",
                LH, C,
                ["remote-load", "--gateways", gw, "--app-id", m12.APP,
                 "--secs", str(a.secs), "--payload", str(a.payload),
                 "--inflight", str(a.conn_inflight), "--conns", str(n)],
                "remote", a.secs, a, edge_unit="hb-edge",
                extra={"hop": "gate-c", "inflight": a.conn_inflight, "n": n,
                       "driver": "remote-load"}))
        kill_unit(LH, "hb-edge")
    finally:
        for h in hophosts[:3]:
            kill_unit(h, "hb-edge")
        m12.stop_cluster(node_hosts)

    # Row d — N engines into one dummy node on the server host.
    S = hophosts[0]
    start_dummy_node(S, a)
    try:
        for n in ladder(a.gate_engines):
            points.append(run_point(
                f"GATE d engine→dummy-node engines={n} inflight={a.conn_inflight}", S, S,
                ["engine-load", "--instance-dir", hop_dir(S), "--app-id", HOP_APP,
                 "--secs", str(a.secs), "--payload", str(a.payload),
                 "--inflight", str(a.conn_inflight), "--engines", str(n)],
                "engine", a.secs, a, edge_unit="hb-dnode",
                extra={"hop": "gate-d", "inflight": a.conn_inflight, "n": n}))
    finally:
        kill_unit(S, "hb-dnode")

    verdicts.append(verdict_row_a(points, direct_rps))
    verdicts.append(verdict_row_b(points, direct_rps))
    verdicts.append(verdict_row_c(points))
    verdicts.append(verdict_row_d(points, cores))
    for v in verdicts:
        print("GATE-JSON " + json.dumps(
            {"row": v.row, "passed": v.passed, "detail": v.detail}), flush=True)
    print("\nGATE rows e and f are local/CI — run them where the code is:", flush=True)
    for c in ("cargo test -p uc_protocol",
              'RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release loom_',
              "cargo test -p uc_remote",
              "cargo test -p uc_gateway",
              "cargo test -p uc_node --test remote_lin"):
        print(f"  {c}", flush=True)
```

- [ ] Wire it into `main`. Add the arguments after `--diag-rungs`:

```python
    ap.add_argument("--selftest", action="store_true",
                    help="adjudicate canned points through the row arithmetic and exit "
                         "(no fleet, no ssh)")
    ap.add_argument("--gate-conns", default="1,2,4,8,16", help="gate rows a/b/c: connection ladder")
    ap.add_argument("--gate-engines", default="1,2,4,8", help="gate row d: engine ladder")
    ap.add_argument("--gate-edge-inflight", type=int, default=65536,
                    help="gate rows a/b/c: the edge's Engine window")
    ap.add_argument("--gate-edge-per-conn", type=int, default=1024,
                    help="gate rows a/b/c: the edge's per-connection credit ceiling "
                         "(must be <= the grant budget, i.e. 7/8 of --gate-edge-inflight)")
    ap.add_argument("--gate-cores", type=int, default=0,
                    help="gate row d: cores on the server host (0 = detect with nproc)")
```

  make `--fleet` optional and check the pair, replacing its line:

```python
    ap.add_argument("--fleet", action="store_true")
```

  and, immediately after `a = ap.parse_args()`:

```python
    if a.selftest:
        sys.exit(selftest())
    if not a.fleet:
        ap.error("one of --fleet or --selftest is required")
```

  add the arm to the dispatch:

```python
        if "gate" in arms:
            arm_gate(m12_hosts, hop_hosts, a, points, verdicts)
```

  with `verdicts = []` beside `points = []`, and replace the tail of `main`:

```python
    missing = [p["label"] for p in points if not p["ok"]]
    if not points:
        print("RESULT: FAIL (infrastructure) — no points measured")
        sys.exit(1)
    if verdicts:
        print("\nM13 gate — FLEET")
        for v in verdicts:
            print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
        failed = [v for v in verdicts if not v.passed]
        if failed:
            print(f"RESULT: FAIL (honest) — {len(failed)} of {len(verdicts)} adjudicated "
                  f"row(s) missed their bar: {[v.row for v in failed]}")
            sys.exit(1)
        print(f"RESULT: PASS — {len(verdicts)} adjudicated row(s), "
              f"{len(missing)} point(s) without a RESULT line")
        sys.exit(0)
    print(f"RESULT: MEASURED {len(points)} points, {len(missing)} without a RESULT line"
          + (f": {missing}" if missing else ""))
    sys.exit(0)
```

  and extend the `--arms` help: `help="subset of: 1,3,2,full,diag,gate"`.

- [ ] Update the module doc's matrix block (~line 22-33) by appending, after the `G direct` line:

```
  gate         the M13 gate (spec §2): rows a/b/c from ONE remote-load ladder
               through the edge into the real cluster against ONE same-generation
               engine-load reference, row d from engine-load → dummy-node on the
               server host. Prints GATE-JSON + PASS/FAIL per row and exits
               non-zero if any bar was missed. Rows e/f are cargo tests, printed.
```

  and change the closing sentence of the doc from "This is a MEASUREMENT driver — it has no bar" to:

```
Output: one `HOP-JSON {...}` line per point, a `HOP-TABLE` at the end. Every
arm but `gate` is a MEASUREMENT with no bar and exits 0 unless the
infrastructure failed; `--arms gate` adjudicates the pre-committed bars in
`docs/benchmarks/uc2-m13-gate-2026-08-24.md` and exits non-zero on a miss.
```

- [ ] Run: `python3 bench-infra/scripts/m13_hop_bench.py --selftest` → expect `SELFTEST: 0 failure(s)` and exit 0.
- [ ] Run: `python3 bench-infra/scripts/m13_hop_bench.py --help` → expect `--arms` help to list `gate` and no import errors.
- [ ] Run: `python3 -c "import ast,sys; ast.parse(open('bench-infra/scripts/m13_hop_bench.py').read())"` → expect no output.
- [ ] Prove the selftest is not vacuous: temporarily change `BAR_B_RATIO` to `0.9`, re-run `--selftest`, expect `SELFTEST: 1 failure(s)` and exit 1, then restore `0.75` and confirm 0 again.
- [ ] Commit:

```sh
git add bench-infra/scripts/m13_hop_bench.py
git commit -m "feat(gate): m13_hop_bench.py --arms gate — adjudicate M13 rows a-d

Rows a/b/c come from one remote-load ladder through the edge into the real
cluster against one same-generation engine-load reference (the comparison
M12 row 2 got wrong); row d is engine-load -> dummy-node on the server host
with the linear rule clamped at 1.0. Bars are constants, not flags. The row
arithmetic is pure and unit-tested by --selftest against canned points;
rows e/f are cargo tests the driver prints. Exit code is the verdict."
```

---

### Task 9: release checklist (docs only — run at the END of M13, not before)

**Do not start this task until tracks A, B and C are all merged and the gate doc's rows a–f carry real results.** It is listed here so the text exists and nothing is invented at tag time.

**Files:**
- Modify `docs/how-to/run-a-gateway.md` — the operating-envelope section (~30-78) and the "When an edge is full" pointer (~237-240).
- Modify `packaging/systemd/uc2-gateway.service` — the `CPUQuota` comment (~16-21).
- Modify `docs/notes/uc2-m12a-edge-flow-control-gap.md` — a correction paragraph.
- Modify `docs/benchmarks/uc2-m12-gate-2026-08-22.md` — row 2's result cell.
- Modify `RELEASES.md`, `docs/releases.md` — the `v2.7.0` section.
- Modify `Cargo.toml` + every intra-workspace `version = "2.6.0"` pin — the lockstep bump.
- **Reference only, do not edit:** `docs/reference/remote-protocol.md` §6 (track B owns the two clarifications), `docs/how-to/upgrade-a-cluster.md`'s ring-format note (track A owns it).

**Steps:**

- [ ] Replace `docs/how-to/run-a-gateway.md`'s entire "## Operating envelope (2.6.0)" section (lines 30-78, through the two link definitions) with **exactly**:

```markdown
## Operating envelope (2.7.0)

**The rule is connections versus cores.** Everything else the edge used to
need sizing against is now arithmetic it does itself.

`2.7.0` gives the edge a **global grant budget**: it holds one `Engine`
inflight window, keeps an eighth of it back as headroom, and divides the
rest equally across its live connections —
`grant = clamp(budget / connections, 1, per_conn_inflight)`. A connection is
told its grant in `HELLO_OK`; when a new connection shrinks everyone's share
the smaller number is pushed as a `STATUS` **before** the affected clients
can send into it, and when a connection leaves the survivors grow back into
its share. So the sum of what this edge has promised never exceeds what its
node can accept, and the reactive halve-on-backpressure ladder — which in
`2.6.0` was the only cross-connection arbiter there was — is now the
exception path. See [the grant budget](../reference/gateway-config.md#the-grant-budget-270)
for the numbers and the two startup checks that go with it
(`per_conn_inflight` above the budget is a refusal; `max_connections` above
it is a warning).

**The 2.6.0 collapse is gone.** That release's envelope warned that N
connections past the node's admission window did not degrade but collapsed —
~30× down, second-scale p95, lost responses, the edge burning seven of eight
cores. The [hop bench](../benchmarks/uc2-m13-hop-bench-2026-08-24.md) located
the cause, and it was not the credit design: it was a convoy in the
shared-memory MPSC ingress ring, where producers published in claim order and
spun on their predecessor, so one preempted producer stalled all of them.
`2.7.0` replaces that with per-record commit (no producer ever waits for
another), which is why the rule below is about **cores**, not about inflight
arithmetic. The [correction note](../notes/uc2-m12a-edge-flow-control-gap.md)
records what the `2.6.0` diagnosis got right and what it got wrong.

**What still needs your attention:**

1. **Count threads against cores.** One edge costs one acceptor, one driver,
   and **one reader thread per connection**; the co-located node runs four
   busy-spin agents and the service one more. On an 8-vCPU host, an edge with
   more than a handful of *busy* connections is oversubscribed, and past that
   point more connections buy latency, not throughput. The measured shape:
   one connection through the edge relays at the cluster's own commit
   ceiling, the aggregate knee is around two connections (the single driver
   thread saturates), and past the knee the curve is flat, then slowly down —
   degradation, not a cliff. `max_connections` (default `1024`) bounds
   threads and sockets; it is not a capacity number and never was.
2. **Bound the gateway's CPU only if you have a reason to.** With the convoy
   gone, `CPUQuota=` is a policy choice about who owns the host's cores, not
   a protection against collapse — and it was never protection: throttling
   the edge made the convoy *worse*, because it starved the preempted
   producer harder.
3. **Size `per_conn_inflight` as a ceiling, not an allocation.** It caps what
   one connection may hold while few are attached; the budget takes over as
   more arrive. A value above the budget is refused at startup by name.

The signals worth watching are the stats line's `backpressure` (should be
near zero — grants that fit the window do not hit it), `grant_changes` (moves
when connections come and go, quiet otherwise), and the node's
`Uc2AdmissionSaturated` alert ([Monitor a cluster](monitor-a-cluster.md)).

[gate]: ../benchmarks/uc2-m13-gate-2026-08-24.md
```

- [ ] In the same file, update the "When an edge is full" pointer (its last paragraph):

```markdown
**`max_connections` is not a capacity number.** It bounds threads and
sockets, not throughput: one reader thread per connection is a real cost on a
host that is also running a node and a service. Size it against the host's
cores — see [Operating envelope (2.7.0)](#operating-envelope-270) — and note
that a value above the edge's grant budget is a startup warning, because
every connection past the budget is granted the floor of one credit.
```

- [ ] Fix the two remaining `#operating-envelope-260` anchors in that file (`grep -n "operating-envelope-260" docs/how-to/run-a-gateway.md`) to `#operating-envelope-270`.

- [ ] Replace the `CPUQuota` comment in `packaging/systemd/uc2-gateway.service` with **exactly**:

```
# Optional: bound the gateway's CPU when it shares a host with its node (the
# supported topology). This is a policy choice about who owns the host's
# cores — it is NOT protection against overload. Before 2.7.0 this comment
# claimed it was; that was wrong twice over, because the 2.6.0 collapse was a
# convoy in the shared ingress ring (fixed in 2.7.0) and throttling the edge
# made that convoy WORSE, not better, by starving the preempted producer.
# Size it to leave the node and service their cores if you want the guarantee
# — see docs/how-to/run-a-gateway.md#operating-envelope-270
# CPUQuota=200%
```

- [ ] Add the correction to `docs/notes/uc2-m12a-edge-flow-control-gap.md`. Replace its italic status line (lines 3-6) with:

```markdown
*Written 2026-08-24, after the M12 edge-saturation fleet ladders.*

> **CORRECTION (2026-08-24, superseding this note's own diagnosis).** The gap
> described below is **real and is now fixed** in `2.7.0`: the edge has a
> global outstanding-grant budget (spec
> `2026-08-24-uc2-m13-remote-path-design.md` §5), so the sum of credits it
> promises fits the `Engine` window and a reduction reaches a client before
> it can send into it. **But this note's "How it collapses" section is
> wrong about the cause of the fleet collapse.** The per-hop bench
> (`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`) reproduced the identical
> 30× collapse with the edge's window at 65536 *and* at 4096, with a raw
> client *and* with `RemoteClient`, against a dummy sink with no admission
> window at all, and — decisively — at 8 connections × 256 inflight, i.e.
> 2,048 outstanding, comfortably *inside* the envelope this note prescribes.
> The trigger was the **number of connections**, and the mechanism was a
> convoy in `uc_protocol::ring::mpsc`: producers published in claim order and
> spun on their predecessor, so one preempted producer stalled every producer
> behind it, on the very cores it needed to make progress. Read the chain in
> "How it collapses" as *a plausible story that the measurement refuted*, and
> `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` for the one that
> survived it. The operating-envelope rule this note recommended
> ("sum of client inflight < the admission window") did not protect against
> the real fault, and the CPU-containment advice made it worse.

Status: the gap is **closed in `2.7.0`**; the collapse it was blamed for had
a different cause, also fixed in `2.7.0`. Nothing here changes consensus, the
node↔node wire, or the cnc page.
```

  and replace its final "## Fix direction (next milestone)" heading with `## Fix direction — built in 2.7.0`, appending after that section:

```markdown
**As built** (`uc_gateway/src/edge.rs`, `conn.rs`): `Shared` carries
`budget = max_inflight − max_inflight/8` and a `live` count;
`grant_for(live, budget, per_conn)` is the share; `Conn::ceiling` is dynamic
and `relax` climbs to it; a handshake joins the budget and waits for the
driver to publish the smaller share to everyone already attached **before**
`HELLO_OK` names a grant, which is what makes "the sum never exceeds the
budget" true at every instant and not merely eventually; a reduction is
pushed as `STATUS`, including on `Conn::squeeze`, which had no call site at
all in `2.6.0`. `SendHalf::inflight()` is still not read — the budget is
sized from the window, not sampled from it, which needs no per-request
atomic load and cannot oscillate.
```

- [ ] Close M12 gate row 2 by reference. In `docs/benchmarks/uc2-m12-gate-2026-08-22.md`, append to row 2's **result** cell (the one beginning "**FAIL vs the 0.8× bar…**"), before the closing `|`:

```
 **CLOSED 2026-08-24 by reference to M13 gate row b** (`docs/benchmarks/uc2-m13-gate-2026-08-24.md`), which is this row re-specified as the hop bench recommended: aggregate through the edge vs the direct arm on the same cluster generation, bar ≥ 0.75×. The 0.1× recorded here was the client, not the edge — the per-hop bench measured `RemoteClient` at 171k/s against a sink that answers instantly, and a raw client through the *same shipped edge* into the *same shipped cluster* at 1.14M/s on one connection. This row is not re-run under M12.
```

- [ ] Bump the workspace version to `2.7.0`: `Cargo.toml`'s `[workspace.package] version`, every intra-workspace `version = "2.6.0"` pin (`grep -rln 'version = "2.6.0"' --include=Cargo.toml .`), and the literal strings `cargo package` cannot see:

```sh
grep -rn "2\.6\.0" packaging/ docs/QUICKSTART.md docs/how-to/run-a-cluster.md
```

  Update each hit per `docs/how-to/cut-a-release.md` §1. Then `cargo package --workspace --allow-dirty` (or CI's `publish-check`) to catch a straggler.

- [ ] Add the new section at the **top** of `RELEASES.md`, directly under the intro paragraph and above `## v2.6.0`, **exactly**:

```markdown
## v2.7.0 — the remote path at the cluster's speed (M13)

The remote path — `client → TCP → gateway → shared memory → node` — now runs
at the backend's own rate and **degrades instead of collapsing** when a host
has more connections than cores. Three defects, located by a per-hop
isolation bench that measured every hop alone
([the bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)) and fixed
together. Nothing here touches consensus, the node-to-node wire protocol, or
the cnc page; the remote wire protocol stays v1. Proof record, row by row:
[M13 gate](docs/benchmarks/uc2-m13-gate-2026-08-24.md).

- **A rebuilt remote client** (`uc_remote`): the same blocking
  `RemoteClient::submit` / `Ticket::wait` surface, over an `Engine`-shaped
  split — a submitter that encodes straight into a preallocated outgoing
  ring, a writer thread that coalesces whatever is queued into one `write`,
  a reader that resolves completions without a lock, and a poll half for
  callers that want batches instead of tickets. The old client paid one
  `write` and about seven futex operations **per request**; it capped at
  ~171k responses/s against a sink that answered instantly, while a raw
  client through the same shipped gateway into the same shipped cluster did
  1.14M/s. That gap was the remote path's bottleneck, by 7×. →
  [Remote protocol](docs/reference/remote-protocol.md) ·
  [the hop bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)
- **A shared-memory ingress ring that cannot convoy**
  (`uc_protocol::ring::mpsc`): producers now commit their own record and no
  producer ever waits for another; the single consumer walks records in claim
  order and stops at the first uncommitted one. A producer that is preempted
  mid-record costs one consumer stall, not a pile-up of every other producer
  spinning on it. A producer that *dies* mid-record leaves a hole the
  consumer skips after `hole_timeout`, counted and logged, instead of
  wedging every producer forever. →
  [The MPSC publish convoy, explained](docs/notes/uc2-m13-mpsc-publish-convoy-explained.md)
- **A global credit budget at the gateway**: the edge holds one `Engine`
  window, keeps an eighth back as headroom, and divides the rest equally
  across its live connections instead of promising each one the same
  constant. A shrinking share is pushed as a `STATUS` before the client can
  send into it; a growing one rides the next response. Two new startup
  checks come with it — `per_conn_inflight` above the budget is a named
  refusal, `max_connections` above it a printed warning. The old
  halve-on-backpressure ladder is still there and is now the exception path.
  →
  [The grant budget](docs/reference/gateway-config.md#the-grant-budget-270) ·
  [Run a gateway](docs/how-to/run-a-gateway.md#operating-envelope-270)
- **Fixed:** the `2.6.0` gateway collapse — ~30× throughput loss, second-scale
  p95 and lost responses past eight connections on an eight-core host — is
  gone, and its diagnosis is corrected. It was **not** the missing credit
  budget the `2.6.0` envelope blamed: it reproduced at 2,048 outstanding
  requests, well inside that envelope, against a sink with no admission
  window at all. It was the ingress ring's publish convoy. The `2.6.0`
  operating envelope and the `CPUQuota=` advice that went with it are both
  retired — CPU containment made the convoy *worse*. →
  [the correction](docs/notes/uc2-m12a-edge-flow-control-gap.md) ·
  [M12 gate row 2, closed](docs/benchmarks/uc2-m12-gate-2026-08-22.md)
- **Performance:** measured on a 4× `c6id.2xlarge` fleet, the gate's
  adjudicated rows — one connection through the gateway against the direct
  shared-memory arm on the same cluster generation, the N-connection
  aggregate against the same reference, the 1→16 connection ladder for
  monotonicity, and N shared-memory engines on an oversubscribed host. →
  [M13 gate](docs/benchmarks/uc2-m13-gate-2026-08-24.md) ·
  [per-hop bench](docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md)

**Upgrade consequence.** The ingress ring's on-disk header changed, so its
magic is bumped and a stale attach is refused by name. **Restart the node,
the service, the gateway and every local client on a host together** — this
is a same-host restart, not a cluster flag day: nodes on different hosts do
not talk to each other through this ring, and the node-to-node wire protocol
is untouched. A gateway `[limits]` section with `per_conn_inflight` above the
grant budget (`max_inflight` less an eighth) now refuses to start, by name. →
[Upgrade a cluster](docs/how-to/upgrade-a-cluster.md)
```

- [ ] Add the matching engineering entry at the top of `docs/releases.md`, above `## v2.6.0`:

```markdown
# ultima_cluster releases

## v2.7.0 — M13 remote path: performance and flow control

**Three defects, one milestone.** Located by
`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`, a per-hop isolation bench
that put a dummy sink behind each hop and a minimal driver in front of it, so
the bottleneck was found by subtraction rather than inferred from an
end-to-end number. Spec:
`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`. Gate:
`docs/benchmarks/uc2-m13-gate-2026-08-24.md`.

**Nothing in this release touches consensus, the node-to-node wire protocol,
or the cnc page layout.** `uc_protocol::version::CURRENT` stays `0.5.0`. The
remote wire protocol stays v1, with two clarifications to its reference (a
`credits` value MAY decrease and is honoured immediately for new seqs;
`STATUS` MAY be sent at any time) that describe behaviour the client already
had.

### The client (`uc_remote`)

*Filled in by track B at merge: the split halves, what moved from the old
client unchanged, and the ported `client_fake_edge` scenarios.*

### The ring (`uc_protocol::ring::mpsc`)

*Filled in by track A at merge: per-record commit, the lap-stamped commit
word, the dead-producer hole and its `IngressRingWedged` residual, the loom
model, and the ring-magic bump.*

### The edge budget (`uc_gateway`)

`Shared` carries `budget = max_inflight − max_inflight / 8` and a `live`
count of handshaken connections; a connection's grant is
`clamp(budget / live, 1, per_conn_inflight)`, exported as
`uc_gateway::budget_for` / `uc_gateway::grant_for`. `Conn` gains a dynamic
`ceiling` that `relax` climbs towards, so a connection that relaxes after a
backpressure episode cannot climb past the share its neighbours leave it.

Two ordering decisions carry the invariant "the sum of grants never exceeds
the budget", and they are the whole of the design:

- **On connect**, a handshake counts itself into `live`, asks for a
  republication, and **waits** (bounded, `GRANT_SETTLE_TIMEOUT` = 250 ms,
  normally microseconds) for the driver to have pushed the smaller share to
  every connection already attached — *then* computes its own grant and puts
  it in `HELLO_OK`. Granting first and shrinking the others afterwards would
  over-promise the window for as long as the republication took.
- **On disconnect**, the connection leaves the table *before* it leaves the
  budget. The reverse order would let the survivors grow into a share the
  departing connection still nominally held.

The reduction itself is written by the **driver** thread, never by a
handshaking reader: the driver is the only thread allowed to write on a
connection other than its own, and a reader that took another connection's
writer lock could stall for the socket write timeout. A reduction is also
pushed from the reader on its *own* connection the moment `Conn::squeeze`
fires — the call site M12's §4.2 asked for and `edge.rs` never had.

`EdgeStats` gains `grant_changes`. `EdgeConfig::validate` gains
`PerConnExceedsBudget` (a named refusal: one connection must be grantable in
full) and `EdgeConfig::warnings()` gains a `max_connections > budget`
advisory, printed once by the daemon — past that point every grant floors at
1 and the sum stops fitting the window, which is legal and miserable.

### What the 2.6.0 collapse actually was

`docs/notes/uc2-m12a-edge-flow-control-gap.md` blamed the missing budget. The
bench refuted it: the collapse reproduced with the edge's window at 65536 and
at 4096, with a raw client and with `RemoteClient`, against a sink with no
admission window, and at 2,048 total outstanding — inside the envelope that
note prescribed. The cause was the ingress ring's publish convoy. That note
now carries a correction paragraph; `run-a-gateway.md`'s operating envelope
and the `CPUQuota=` advice in the systemd unit are retired, the latter
because CPU containment starved the preempted producer and made the convoy
worse.

### M12 gate row 2

Closed by reference to M13 gate row b. Row 2 compared one TCP client to one
shared-memory client at equal inflight, which the bench showed to be the
wrong comparison; row b is the re-specification the bench recommended.
```

- [ ] Verify every link in both files resolves:

```sh
grep -oE '\(docs/[^)]+\)|\(\.\./[^)]+\)|\(#[a-z0-9-]+\)' RELEASES.md docs/how-to/run-a-gateway.md \
  | sed 's/.*(\(.*\))/\1/' | sort -u
```

  → check each path exists (anchors by eye against the target headings).

- [ ] Run the full local proof stack before the tag: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Commit:

```sh
git add -A
git commit -m "docs(release): v2.7.0 writeup — remote path at the cluster's speed

RELEASES.md + docs/releases.md sections (features, the corrected collapse
diagnosis, the ring-magic same-host restart note); run-a-gateway.md's
operating envelope rewritten for the budget and for cores-not-inflight;
the systemd CPUQuota comment corrected (containment made the convoy WORSE);
the M12a gap note carries its correction; M12 gate row 2 closed by
reference to M13 row b; workspace version 2.6.0 -> 2.7.0 in lockstep."
```

- [ ] **Stop here.** The fleet gate run is a separate, user-approved step. Print the command and hand it over:

```bash
cd bench-infra && make apply          # 4x c6id.2xlarge, us-east-1
python3 bench-infra/scripts/m13_hop_bench.py --fleet --arms gate --secs 10 \
  2>&1 | tee /home/claude/m13-gate-fleet.log
cd bench-infra && make destroy && terraform -chdir=bench-infra/terraform state list   # must be empty
```

  Record the results in `docs/benchmarks/uc2-m13-gate-2026-08-24.md`'s "Results"
  section and the row cells, in a **separate commit** from this one, whatever
  they say.

---

## Self-review

Performed against the spec, the prompt's pinned interfaces, and the code as it stands on `main`.

**Spec §5 coverage.** §5.1: `budget = max_inflight − headroom` (Task 1, `BUDGET_HEADROOM_DIV = 8`, a documented constant with no config key, as ruled); `live: AtomicU32` and `grant = clamp(budget/live, 1, per_conn_inflight)` (Tasks 1–2); recomputed on every connect and disconnect (Task 2); a reduction pushed immediately as `STATUS{acked_seq, credits}` (Tasks 2–3); an increase riding the next `RESPONSE`/timer (Task 2, `CeilingChange::Raised` deliberately writes nothing); `HELLO_OK` carrying the current grant, not the config constant (Tasks 2 and 4). §5.2: `squeeze`/`relax` unchanged in shape, `relax` retargeted at the dynamic ceiling, `STATUS` on `squeeze` added (Task 3); the dispatch ladder becomes the exception path, asserted as `backpressure_events == 0` (Task 4). §5.3: equal shares only; no demand weighting anywhere. §5.4: all four tests plus the config refusal — (i) Task 2, (ii) Task 3, (iii) Task 4, (iv) Task 4, (v) Task 5.

**Spec §2 coverage.** The gate doc (Task 7) copies rows a–f verbatim, keeps every result cell at *not yet run*, has a "How it is run" section, and carries the honest-failure protocol in the M11 gate doc's form. The driver (Task 8) composes rows a–d exactly as the prompt specifies, prints `GATE-JSON` per row plus a `Verdict` list, and exits like `m12_fleet_gate` does. Rows e/f are printed commands, not runs. `hop_bench local` is untouched.

**Spec §7 coverage.** Task 9 covers the release-time list item by item: the envelope rewrite, the `CPUQuota` comment, the §6 clarifications (referenced to track B, not edited here), the M12a note's correction, `gateway-config.md`'s budget semantics (moved earlier, to Task 5, because the refusal it documents ships in that task), `RELEASES.md` + `docs/releases.md`, the `2.7.0` lockstep bump, and M12 row 2 closed by reference.

**Placeholder scan.** Every code block is real Rust or real Python against the actual line ranges and identifiers in the tree. The only intentionally-empty text is `docs/releases.md`'s two *"Filled in by track A/B at merge"* subsections — that is a track boundary, marked as such, in a task that runs after those tracks merge — and the gate doc's result cells, which the honest-failure protocol requires to be empty at this commit.

**Signature consistency.** `RemoteClient::connect/submit/Ticket::wait/stats/shutdown` and `RemoteStats.max_credits_seen` are used by the existing gateway tests and by the new tests only through that surface; nothing here reaches into `uc_remote` internals. `hop_bench remote-load`'s CLI (`--gateways --secs --payload --inflight --conns`) is what `arm_gate` invokes, and the `RESULT {arm:"remote",…}` line is what `parse_result(out, "remote")` consumes — both are track B's pinned contract. `Engine`'s API is untouched (track A's promise). `Conn::relax`'s signature changes from `relax(&self, ceiling: u32)` to `relax(&self)` — it is `pub(crate)` inside `uc_gateway` with exactly two call sites (`edge.rs:1328` and `conn.rs`'s unit tests), both updated in Task 2. `Shared::write_status` changes from `()` to `bool`, with both call sites updated.

**Two traps found and handled while writing this.** (1) The naive ordering — grant the new connection first, shrink the others afterwards — over-promises the budget by up to `budget/2` for as long as the driver takes to republish, which would make test (i) flaky rather than false; hence the connect-side settle wait and the remove-then-leave order on disconnect, both called out in the code comments. (2) `hop_bench`'s `edge` role defaults to `per_conn_inflight = 4096` against `max_inflight = 4096`, which Task 5's new refusal would reject — the whole bench harness would stop starting. Task 5 lowers that default to 1024 and the gate arm passes `--gate-edge-per-conn 1024` explicitly; note that at the gate's `--gate-edge-inflight 65536` the budget (57344) never binds at 16 × 1024, so the budget's own proof is rows e/f, not row c, and the gate doc says so implicitly by scoping row c to "monotone, no collapse".
