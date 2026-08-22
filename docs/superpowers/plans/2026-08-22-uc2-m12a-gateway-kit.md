# M12a — Gateway kit + two-tier state-machine contract: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remote clients can reach a UC cluster through a stateless gateway that follows the leader across failover with zero acked-write loss, and a state machine may take its commands as raw bytes (zero decode, zero allocation) or as typed serde values — the user's choice.

**Architecture:** (1) `uc2_service` gains `RawStateMachine` (`apply(&mut self, pos, cmd: &[u8], out: &mut Vec<u8>)`) as the core contract; today's typed `StateMachine` is blanket-implemented onto it (`impl<S: StateMachine> RawStateMachine for S`) doing exactly today's bincode-standard codec, so every existing state machine compiles and produces byte-identical wire. `Sessioned<S>` wraps any `RawStateMachine` with Raft-paper client-session dedup (16-byte envelope, bounded per-client response window, snapshot-composed). (2) `uc2_remote` is the framed-TCP protocol (v1) + the Rust `RemoteClient` (pipelined, receiver-driven credits, redirect-following, ordered re-send). (3) `uc2_gateway` is the edge library over `uc2_client::Engine` — one `Engine` per edge, per-connection reader threads, one poll driver, a leader watch off the cnc page, a static member map — plus the `uc2-gateway` reference binary. Proof: a hard-crash lincheck capstone with three edges in the loop; measurement: `m12_gate` (gateway vs direct `Engine`) and `m5_gate` with `apply-profile` (codec share).

**Tech Stack:** Rust edition 2024 workspace; `std::net` TCP + threads (no tokio on the edge hot path); `bincode` v2 standard config for the typed tier; `serde`; `clap`/`toml` for the binary (same as `uc2-node`); `uc-lincheck` for the capstone; `uc2-crashtest` harness for SIGKILL.

**Spec:** `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` (§3.1, §4) — read it first. Production-readiness umbrella: `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §7.

## Global Constraints

- No consensus change, no wire-protocol (`version::CURRENT`) change, no cnc layout change outside the reserved band, no new remote-admin surface (spec §2). The gateway carries no admin ops.
- The typed trait's signature is unchanged: `fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response`; wire bytes for typed state machines are **byte-identical** to `v2.5.0` (test in Task 1 guards it).
- Apply stays sync, deterministic, no I/O. `Sessioned` eviction and snapshot encoding must be deterministic (`BTreeMap`, position-based LRU — never time).
- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean after every task; `cargo test` (default) must stay green after every task.
- No new heavy deps: `uc2_remote` depends on `bytes`, `thiserror` only (+ `serde`/`bincode` as dev-deps for tests); `uc2_gateway` on `uc2_client`, `uc2_remote`, `uc_protocol`, `uc2_log`, `serde`/`toml`/`clap`/`anyhow` for the bin. No tokio on the edge.
- Never write heavy artifacts to `/tmp`: tests use `tempdir()` under `CARGO_TARGET_TMPDIR` exactly like `uc2_node/tests/lin_v2.rs`.
- Bars are fleet-only; local `m5_gate`/`m12_gate` runs are smoke and are labelled so in any doc.
- Commit after every task; plan commits on branch `uc2/m12-adoptable`.

## File structure (what each new/changed file owns)

| Path | Responsibility |
|---|---|
| `uc2_service/src/traits.rs` | `RawStateMachine`, blanket impl for `StateMachine`, `RawOutputHandler`, `TypedOutput<O>`, `SnapshotStateMachine: RawStateMachine` |
| `uc2_service/src/apply.rs`, `replay.rs`, `output.rs`, `egress.rs`, `lib.rs` | pass frame bytes through; reusable response buffer; builder generics |
| `uc2_service/src/session.rs` | `Sessioned<S>`, `SessionConfig`, envelope constants, response tag |
| `uc2_remote/src/{lib,frame,client,error}.rs` | protocol v1 codec; `RemoteClient`; errors |
| `uc2_gateway/src/{lib,config,edge,conn,watch}.rs` | `EdgeConfig`; `Edge` (attach/accept/driver); per-connection state; leader watch |
| `uc2_gateway/src/bin/uc2-gateway.rs` | reference binary: args, TOML, named refusals, SIGTERM |
| `uc2_gateway/examples/m12_gate.rs` | gateway-vs-direct measurement harness |
| `examples/uc2-crashtest/src/bin/uc2-crashtest-gateway.rs`, `tests/remote_lin.rs` | capstone |
| `packaging/systemd/uc2-gateway.service`, `packaging/gateway.example.toml` | packaging |
| `docs/how-to/run-a-gateway.md`, `docs/reference/gateway-config.md`, `docs/reference/remote-protocol.md`, `docs/reference/state-machine-contract.md`, `docs/notes/uc2-gateway-shapes-and-flow-control.md` | docs |

---

### Task 1: `RawStateMachine` + blanket typed adapter (traits only, byte-identity test)

**Files:**
- Modify: `uc2_service/src/traits.rs`
- Modify: `uc2_service/src/lib.rs:62-65` (exports)
- Test: `uc2_service/tests/raw_contract.rs` (new)

**Interfaces:**
- Produces (used by every later task):
  ```rust
  pub trait RawStateMachine: Send + 'static {
      fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
      fn query(&self, q: &[u8], out: &mut Vec<u8>);
      fn last_applied(&self) -> Option<u64>;
  }
  impl<S: StateMachine> RawStateMachine for S { /* bincode standard */ }
  pub trait SnapshotStateMachine: RawStateMachine { /* unchanged body */ }
  pub trait RawOutputHandler<S: RawStateMachine>: Send + 'static {
      async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError>;
  }
  pub struct TypedOutput<O>(pub O);   // RawOutputHandler<S> for S: StateMachine, O: OutputHandler<S>
  impl<S: RawStateMachine> RawOutputHandler<S> for NoopOutput
  ```

- [ ] **Step 1: Write the failing test** — `uc2_service/tests/raw_contract.rs`:

```rust
//! The raw contract and its blanket typed adapter produce the same bytes the
//! v2.5.0 framework produced (position prefix ++ bincode(resp)) — clients
//! built against 2.5.0 keep decoding responses unchanged.
use uc2_service::{RawStateMachine, StateMachine};

#[derive(serde::Serialize, serde::Deserialize)]
enum Cmd { Add(i64) }
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Resp { value: i64, position: u64 }
#[derive(serde::Serialize, serde::Deserialize)]
enum Q { Value }

#[derive(Default)]
struct Counter { v: i64, last: Option<u64> }
impl StateMachine for Counter {
    type Command = Cmd; type Response = Resp; type Query = Q; type QueryResponse = i64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> Resp {
        match cmd { Cmd::Add(n) => self.v += n }
        self.last = Some(position);
        Resp { value: self.v, position }
    }
    fn query(&self, _q: Q) -> i64 { self.v }
    fn last_applied(&self) -> Option<u64> { self.last }
}

#[test]
fn typed_sm_is_a_raw_sm_with_byte_identical_wire() {
    let mut sm = Counter::default();
    let cmd_bytes = bincode::serde::encode_to_vec(&Cmd::Add(5), bincode::config::standard()).unwrap();
    let mut out = Vec::new();
    RawStateMachine::apply(&mut sm, 4096, &cmd_bytes, &mut out);
    // exactly what v2.5.0's egress encoded after the 8-byte position prefix
    let expected = bincode::serde::encode_to_vec(&Resp { value: 5, position: 4096 }, bincode::config::standard()).unwrap();
    assert_eq!(out, expected);
    assert_eq!(RawStateMachine::last_applied(&sm), Some(4096));

    let q = bincode::serde::encode_to_vec(&Q::Value, bincode::config::standard()).unwrap();
    out.clear();
    RawStateMachine::query(&sm, &q, &mut out);
    assert_eq!(out, bincode::serde::encode_to_vec(&5i64, bincode::config::standard()).unwrap());
}

struct Echo { last: Option<u64> }
impl RawStateMachine for Echo {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) { self.last = Some(position); out.extend_from_slice(cmd); }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) { out.extend_from_slice(q); }
    fn last_applied(&self) -> Option<u64> { self.last }
}

#[test]
fn raw_sm_sees_the_bytes_untouched() {
    let mut sm = Echo { last: None };
    let mut out = Vec::new();
    RawStateMachine::apply(&mut sm, 7, b"\x00\x01raw", &mut out);
    assert_eq!(out, b"\x00\x01raw");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc2_service --test raw_contract`
Expected: FAIL — `unresolved import uc2_service::RawStateMachine`.

- [ ] **Step 3: Implement in `traits.rs`** — add after the `StateMachine` trait (keep `StateMachine` verbatim):

```rust
/// The core state-machine contract: bytes in, bytes out. The framework hands
/// `apply` the committed frame payload exactly as it sits in the log buffer
/// and reuses `out` across calls — no decode, no allocation in steady state.
/// Implement this directly for SBE / flatbuffers / hand-laid frames; or
/// implement [`StateMachine`] (typed, serde + bincode) and get this for free
/// via the blanket impl below. A type implements ONE of the two.
pub trait RawStateMachine: Send + 'static {
    /// Apply the committed command at `position` (the absolute log byte
    /// offset, the idempotency key). Write the response bytes into `out`
    /// (cleared by the caller). Deterministic, sync, no I/O.
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
    /// Answer a read. `out` is cleared by the caller.
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    /// Highest position applied so far (`None` before the first).
    fn last_applied(&self) -> Option<u64>;
}

/// Every typed state machine is a raw one: decode with bincode-standard,
/// apply, encode the response with bincode-standard — exactly the codec the
/// framework used through v2.5.0, so the wire is byte-identical.
impl<S: StateMachine> RawStateMachine for S {
    #[inline]
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        let resp = StateMachine::apply(self, position, cmd);
        bincode::serde::encode_into_std_write(&resp, out, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
    }
    #[inline]
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        let (q, _) = bincode::serde::decode_from_slice::<S::Query, _>(q, bincode::config::standard())
            .expect("corrupt query frame (fail-stop)");
        let qr = StateMachine::query(self, q);
        bincode::serde::encode_into_std_write(&qr, out, bincode::config::standard())
            .expect("query-response bincode-encode (fail-stop)");
    }
    #[inline]
    fn last_applied(&self) -> Option<u64> { StateMachine::last_applied(self) }
}
```
Change the `SnapshotStateMachine` supertrait line to `pub trait SnapshotStateMachine: RawStateMachine {` (body unchanged). Then add after `NoopOutput`:

```rust
/// Raw-tier output handler: sees the committed command bytes. The typed
/// [`OutputHandler`] is adapted onto this by [`TypedOutput`].
#[allow(async_fn_in_trait)]
pub trait RawOutputHandler<S: RawStateMachine>: Send + 'static {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError>;
}

impl<S: RawStateMachine> RawOutputHandler<S> for NoopOutput {
    async fn on_committed(&self, _position: u64, _cmd: &[u8], _state: &S) -> Result<(), OutputError> { Ok(()) }
}

/// Adapts a typed [`OutputHandler`] to the raw tier (one bincode decode per
/// committed command, as the output agent did through v2.5.0).
pub struct TypedOutput<O>(pub O);

impl<S: StateMachine, O: OutputHandler<S>> RawOutputHandler<S> for TypedOutput<O> {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError> {
        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
            .expect("corrupt committed frame (fail-stop)");
        self.0.on_committed(position, &cmd, state).await
    }
}
```
Remove the old `impl<S: StateMachine> OutputHandler<S> for NoopOutput` ONLY IF it conflicts (it does not — keep it; both impls coexist). Export in `lib.rs`: `pub use crate::traits::{NoopOutput, OutputError, OutputHandler, RawOutputHandler, RawStateMachine, SnapshotStateMachine, StateMachine, TypedOutput};`.

- [ ] **Step 4: Run the test + the whole crate** — `cargo test -p uc2_service --test raw_contract` → PASS; `cargo build --workspace` → compiles (nothing else uses the new items yet; `SnapshotStateMachine: RawStateMachine` is satisfied by existing typed impls through the blanket).
- [ ] **Step 5: Commit** — `git commit -am "feat(service): RawStateMachine core contract + blanket typed adapter (byte-identical wire)"`.

---

### Task 2: Apply, replay, and query paths pass bytes through; reusable response buffer

**Files:**
- Modify: `uc2_service/src/apply.rs` (apply loop ~311-343; `drain_queries` ~520-548; generics `S: StateMachine` → `S: RawStateMachine` throughout the file)
- Modify: `uc2_service/src/replay.rs` (~140-175 and its generics)
- Modify: `uc2_service/src/egress.rs` (`publish`, `publish_query_answer`)
- Modify: `uc2_service/src/attach.rs` (generics), `uc2_service/src/lib.rs` (generics on `ServiceBuilder`, `Service`; `start`, `start_with_snapshots`; `Service::query`)

**Interfaces:**
- Consumes: Task 1 traits.
- Produces: `Egress::publish(&mut self, session_id: u64, correlation_id: u64, position: u64, resp: &[u8])`, `Egress::publish_query_answer(&mut self, header_extra: [u8; 8], resp: &[u8])`; `ApplyState<S: RawStateMachine>` gains `resp_buf: Vec<u8>`; `ServiceBuilder<S: RawStateMachine, O: RawOutputHandler<S> = NoopOutput>`; `Service<S: RawStateMachine>`; `Service::query_raw(&self, q: &[u8], out: &mut Vec<u8>)` plus the existing `#[doc(hidden)] query` kept under `where S: StateMachine`.

- [ ] **Step 1: The existing suites are the failing tests** — no new test file; the change is a refactor guarded by `cargo test -p uc2_service` (apply/query/reconstruction/snapshot suites) and Task 1's byte-identity test. Run `cargo test -p uc2_service` once now and note it is green (baseline).

- [ ] **Step 2: `egress.rs`** — replace the two generic methods:

```rust
/// Publish a submit response: `position LE ++ resp` on the egress broadcast,
/// keyed for the client matcher. `resp` is the state machine's response
/// bytes (typed tier: bincode; raw tier: whatever it wrote). One memcpy into
/// the reused scratch, then the ring write — no allocation in steady state.
pub(crate) fn publish(&mut self, session_id: u64, correlation_id: u64, position: u64, resp: &[u8]) {
    self.scratch.clear();
    self.scratch.extend_from_slice(&position.to_le_bytes());
    self.scratch.extend_from_slice(resp);
    let extra = extra_client(session_id as u32, correlation_id as u32);
    let _ = self.producer.write(MSG_V2_RESPONSE, 0, extra, &self.scratch);
}

pub(crate) fn publish_query_answer(&mut self, header_extra: [u8; 8], resp: &[u8]) {
    self.scratch.clear();
    self.scratch.extend_from_slice(&0u64.to_le_bytes());
    self.scratch.extend_from_slice(resp);
    let _ = self.producer.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, header_extra, &self.scratch);
}
```
Add `scratch: Vec<u8>` to the `Egress` struct, initialised `Vec::with_capacity(8 + 256)` in its constructor. Keep the `#[cfg(feature = "apply-profile")]` ENCODE probe out of egress now (the encode moved into the blanket impl; the `decode` probe in apply.rs now wraps the whole `sm.apply` call — see Step 3 — and the profile report's "decode" column becomes "apply incl. codec"; rename the field label in `profile::report` to `sm_apply`).

- [ ] **Step 3: `apply.rs` apply loop** — replace lines 311-343's body with:

```rust
if Some(pos) <= sm.last_applied() {
    continue;
}
#[cfg(feature = "apply-profile")]
let t0 = profile::now();
// Bytes straight from the frame to the state machine. Typed SMs decode
// inside their blanket RawStateMachine impl; raw SMs see the slice.
st.resp_buf.clear();
sm.apply(pos, payload, &mut st.resp_buf);
#[cfg(feature = "apply-profile")]
let t1 = profile::now();
if is_leader {
    st.egress.publish(hdr.session_id, hdr.correlation_id, pos, &st.resp_buf);
}
#[cfg(feature = "apply-profile")]
{
    let t2 = profile::now();
    pf_frames += 1;
    pf_dec += t1 - t0;      // now "apply incl. codec"
    pf_pub += t2 - t1;
    pf_bytes += payload.len() as u64;
}
```
(`pf_app` and the `APPLY` static go away; adjust `profile::add`/`report` accordingly — keep the feature compiling: `cargo clippy -p uc2_service --features apply-profile --all-targets -- -D warnings`.) Add `pub(crate) resp_buf: Vec<u8>` to `ApplyState` and initialise it in `attach.rs` with `Vec::with_capacity(256)`. Change every `S: StateMachine` bound in `apply.rs` to `S: RawStateMachine` (import it). In `drain_queries` replace the decode/query/answer lines with:

```rust
st.resp_buf.clear();
st.sm.lock().unwrap().query(&buf[8..], &mut st.resp_buf);
st.egress.publish_query_answer(rec.header_extra, &st.resp_buf);
```

- [ ] **Step 4: `replay.rs`** — replace the decode+apply with:

```rust
if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > guard.last_applied() {
    scratch.clear();
    guard.apply(pos, &payload[off + HEADER_LEN..off + total], &mut scratch);
}
```
where `let mut scratch = Vec::with_capacity(256);` is declared once before the scan closure. The `decode_error` path no longer exists for the raw tier (a typed SM's blanket impl fail-stops on corrupt bytes exactly as before, via `expect`); remove the now-unused `decode_error` plumbing and the `Err` arm, keep the rest. Generics → `S: RawStateMachine`.

- [ ] **Step 5: `lib.rs` / `attach.rs` generics** — `ServiceBuilder<S: RawStateMachine, O: RawOutputHandler<S> = NoopOutput>`; `impl<S: RawStateMachine> ServiceBuilder<S, NoopOutput> { pub fn new(..) }`; `impl<S: RawStateMachine, O: RawOutputHandler<S>> ServiceBuilder<S, O>`; `start_with_snapshots` keeps `where S: SnapshotStateMachine`; `Service<S: RawStateMachine>`; add

```rust
/// Raw-tier direct query (embedded/test path). `out` is cleared first.
#[doc(hidden)]
pub fn query_raw(&self, q: &[u8], out: &mut Vec<u8>) {
    out.clear();
    self.sm.lock().unwrap().query(q, out);
}
/// Typed convenience over [`Self::query_raw`].
#[doc(hidden)]
pub fn query(&self, q: S::Query) -> S::QueryResponse where S: StateMachine {
    let q = bincode::serde::encode_to_vec(&q, bincode::config::standard()).expect("encode");
    let mut out = Vec::new();
    self.query_raw(&q, &mut out);
    bincode::serde::decode_from_slice(&out, bincode::config::standard()).expect("decode").0
}
```
`output_handler<O2>` will change in Task 3 — for now leave `output.rs` compiling by bounding the builder's `start`/`start_with_snapshots` output wiring on the old trait through a temporary `where S: StateMachine, O: OutputHandler<S>` on those two methods ONLY if the compiler forces it; Task 3 removes it. Prefer doing Tasks 2 and 3 in one sitting if that temporary bound is awkward.

- [ ] **Step 6: Run** — `cargo test -p uc2_service` (all six suites + `raw_contract`) → PASS; `cargo test -p uc2_node --test lin_v2` → PASS (typed `RegisterSm` through the blanket); `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- [ ] **Step 7: Commit** — `git commit -am "refactor(service): apply/replay/query pass frame bytes through; reusable response buffer"`.

---

### Task 3: Output agent on the raw tier; `ServiceBuilder::output_handler` adapts typed handlers

**Files:**
- Modify: `uc2_service/src/output.rs` (decode sites ~170-200 and ~350-375; `deliver` signature; generics)
- Modify: `uc2_service/src/lib.rs:91-99` (`output_handler`)
- Test: existing `uc2_service/tests/output.rs` (unchanged — it is the guard)

**Interfaces:**
- Produces: `ServiceBuilder::output_handler<O2: OutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, TypedOutput<O2>> where S: StateMachine` (API unchanged for users) and `ServiceBuilder::raw_output_handler<O2: RawOutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, O2>`.

- [ ] **Step 1: Baseline** — `cargo test -p uc2_service --test output` green before touching anything.
- [ ] **Step 2: `output.rs`** — make `OutputState<S, O>` generic over `S: RawStateMachine, O: RawOutputHandler<S>`; change `deliver(sm, cnc, handler, rt, pos, frame_end, cmd: &[u8]) -> bool` to take the payload slice; at the live path replace the decode with passing `payload` straight: `if !deliver(&st.sm, &st.cnc, &st.handler, &st.rt, pos, frame_end, payload) { ... }`; at the replay-degrade path pass `&payload[off + HEADER_LEN..off + total]`. Inside `deliver`, the handler call becomes `handler.on_committed(pos, cmd, &*sm_guard).await` with `cmd: &[u8]`.
- [ ] **Step 3: `lib.rs`** —

```rust
/// Install a typed output handler (adapted onto the raw tier; one bincode
/// decode per committed command on the output thread, as before).
pub fn output_handler<O2: OutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, TypedOutput<O2>>
where S: StateMachine {
    ServiceBuilder { cfg: self.cfg, sm: self.sm, output: TypedOutput(h) }
}
/// Install a raw output handler (sees the committed command bytes).
pub fn raw_output_handler<O2: RawOutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, O2> {
    ServiceBuilder { cfg: self.cfg, sm: self.sm, output: h }
}
```
Remove any temporary `where` bounds left from Task 2.
- [ ] **Step 4: Run** — `cargo test -p uc2_service` → PASS; `cargo test --workspace` → PASS (counter example, crashtest bins compile); `cargo test -p uc2_service --features ultima_db` → PASS; clippy clean (both feature sets).
- [ ] **Step 5: Commit** — `git commit -am "refactor(service): output agent on the raw tier; typed handlers adapted via TypedOutput"`.

---

### Task 4: `m5_gate` raw `CountSm` arm (apply-profile smoke) + docs for the two-tier contract

**Files:**
- Modify: `uc2_node/examples/m5_gate.rs` (`CountSm` ~210-235; a `--raw-sm` flag on `node`/`all`)
- Create: `docs/reference/state-machine-contract.md`
- Modify: `docs/reference/configuration.md` or wherever `StateMachine` is documented (grep `StateMachine` under `docs/` and add a pointer)

- [ ] **Step 1: Add the raw arm** — keep `CountSm` (typed, `Command = Vec<u8>`) and add:

```rust
/// Raw-tier twin of `CountSm`: sees the frame bytes, decodes nothing.
#[derive(Default)]
struct RawCountSm { count: u64, last_applied: Option<u64> }
impl uc2_service::RawStateMachine for RawCountSm {
    fn apply(&mut self, position: u64, _cmd: &[u8], out: &mut Vec<u8>) {
        self.count += 1;
        self.last_applied = Some(position);
        out.extend_from_slice(&self.count.to_le_bytes());
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) { out.extend_from_slice(&self.count.to_le_bytes()); }
    fn last_applied(&self) -> Option<u64> { self.last_applied }
}
```
Add `#[arg(long)] raw_sm: bool` to the node-role and all-role arg structs; where the service is built (`ServiceBuilder::new(cfg, CountSm::default()).start()`), branch: `if raw_sm { ServiceBuilder::new(cfg, RawCountSm::default()).start()? } else { ... }` (two `Service<_>` types — hold them in an enum or `Box<dyn Any>`-free way: simplest is two `Option<Service<..>>` locals kept alive until the end). The client side is unchanged (it sends bincode(`Vec<u8>`) = varint len + bytes; the raw SM ignores them; the response is 8 bytes either way — the client's matcher only counts completions).
- [ ] **Step 2: Smoke** — `cargo run -p uc2_node --release --example m5_gate --features uc2_service/apply-profile -- all --secs 6 --payload 512` and again with `--raw-sm`; record the two `apply-profile[final]` lines in the gate-doc skeleton created in Task 12 as **smoke** (dev box). Expected shape: raw arm `sm_apply` ≈ a few ns, typed arm ≈ 800 ns at 512 B (see `docs/notes/2026-08-22-codec-budget-spike.md`).
- [ ] **Step 3: Write `docs/reference/state-machine-contract.md`** — sections: the two tiers (when to pick which), the exact trait signatures (copy from Task 1), "a type implements one of the two", byte-identity promise for typed, `bytes::Bytes`/`serde_bytes` advice for blobs (cite the spike note), the `out` buffer discipline (cleared by the caller; write only your response), `RawOutputHandler` vs `OutputHandler`, how `Sessioned` wraps either (forward pointer to Task 5's section), payload ceiling (~1.3 KB, one datagram). Link from the existing docs page that documents `StateMachine`.
- [ ] **Step 4: Commit** — `git commit -am "docs+gate: raw CountSm arm for m5_gate; state-machine contract reference"`.

---

### Task 5: `Sessioned<S>` — exactly-once at the raw layer

**Files:**
- Create: `uc2_service/src/session.rs`
- Modify: `uc2_service/src/lib.rs` (`mod session; pub use session::{Sessioned, SessionConfig, SESSION_HEADER_LEN, TAG_FRESH, TAG_REPLAYED, TAG_EXPIRED};`)
- Test: `uc2_service/tests/session.rs` (new)

**Interfaces:**
- Produces:
  ```rust
  pub const SESSION_HEADER_LEN: usize = 16;           // client_id u64 LE ++ seq u64 LE
  pub const TAG_FRESH: u8 = 0; pub const TAG_REPLAYED: u8 = 1; pub const TAG_EXPIRED: u8 = 2;
  pub struct SessionConfig { pub window: usize /* default 4096 */, pub max_clients: usize /* default 65536 */ }
  pub struct Sessioned<S> { .. }
  impl<S: RawStateMachine> Sessioned<S> { pub fn new(inner: S, cfg: SessionConfig) -> Self; pub fn inner(&self) -> &S; pub fn inner_mut(&mut self) -> &mut S; }
  impl<S: RawStateMachine> RawStateMachine for Sessioned<S>
  impl<S: SnapshotStateMachine> SnapshotStateMachine for Sessioned<S>
  ```
  Response bytes = `tag u8 ++ inner response` (`TAG_EXPIRED` carries no inner bytes). A command shorter than 16 bytes → `TAG_EXPIRED` (malformed envelope is treated as unanswerable, never panics the apply thread).

- [ ] **Step 1: Write the failing tests** — `uc2_service/tests/session.rs`:

```rust
use uc2_service::{RawStateMachine, SessionConfig, Sessioned, SnapshotStateMachine, SESSION_HEADER_LEN, TAG_EXPIRED, TAG_FRESH, TAG_REPLAYED};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

fn env(client: u64, seq: u64, cmd: &Cmd) -> Vec<u8> {
    let mut v = Vec::with_capacity(SESSION_HEADER_LEN + 16);
    v.extend_from_slice(&client.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    bincode::serde::encode_into_std_write(cmd, &mut v, bincode::config::standard()).unwrap();
    v
}
fn resp(out: &[u8]) -> (u8, Option<CmdResp>) {
    let tag = out[0];
    if tag == TAG_EXPIRED { return (tag, None); }
    let (r, _) = bincode::serde::decode_from_slice(&out[1..], bincode::config::standard()).unwrap();
    (tag, Some(r))
}
fn sm(window: usize, max_clients: usize) -> Sessioned<RegisterSm> {
    Sessioned::new(RegisterSm::default(), SessionConfig { window, max_clients })
}

#[test]
fn fresh_then_replayed_then_expired() {
    let mut s = sm(2, 16);
    let mut out = Vec::new();
    s.apply(100, &env(7, 1, &Cmd::Write(10)), &mut out);
    assert_eq!(resp(&out), (TAG_FRESH, Some(CmdResp::WriteAck)));
    out.clear(); s.apply(200, &env(7, 2, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_FRESH, Some(CmdResp::CasResult(true))));
    // retry of seq 2: replayed, NOT re-applied (a second CAS 10->11 would be false)
    out.clear(); s.apply(300, &env(7, 2, &Cmd::Cas { old: 10, new: 11 }), &mut out);
    assert_eq!(resp(&out), (TAG_REPLAYED, Some(CmdResp::CasResult(true))));
    out.clear(); s.apply(400, &env(7, 3, &Cmd::Write(1)), &mut out);
    // window = 2 holds seqs 2,3 now; seq 1 fell out
    out.clear(); s.apply(500, &env(7, 1, &Cmd::Write(10)), &mut out);
    assert_eq!(resp(&out), (TAG_EXPIRED, None));
    assert_eq!(s.last_applied(), Some(500));
}

#[test]
fn a_gap_is_applied_fresh_and_lower_unseen_is_expired() {
    let mut s = sm(8, 16);
    let mut out = Vec::new();
    s.apply(1, &env(1, 5, &Cmd::Write(5)), &mut out);
    assert_eq!(resp(&out).0, TAG_FRESH);
    out.clear(); s.apply(2, &env(1, 3, &Cmd::Write(3)), &mut out);
    assert_eq!(resp(&out).0, TAG_EXPIRED);
}

#[test]
fn clients_are_evicted_by_oldest_position_deterministically() {
    let mut s = sm(4, 2);
    let mut out = Vec::new();
    s.apply(10, &env(1, 1, &Cmd::Write(1)), &mut out);
    out.clear(); s.apply(20, &env(2, 1, &Cmd::Write(2)), &mut out);
    out.clear(); s.apply(30, &env(3, 1, &Cmd::Write(3)), &mut out); // evicts client 1 (seen at 10)
    out.clear(); s.apply(40, &env(1, 1, &Cmd::Write(1)), &mut out);
    assert_eq!(resp(&out).0, TAG_FRESH, "evicted client starts over: its retry is applied fresh");
    out.clear(); s.apply(50, &env(2, 1, &Cmd::Write(2)), &mut out);
    // client 2 (seen at 20) was evicted when client 1 came back at 40 (client 3 seen at 30 is newer)
    assert_eq!(resp(&out).0, TAG_FRESH);
}

#[test]
fn malformed_envelope_is_expired_not_a_panic() {
    let mut s = sm(4, 4);
    let mut out = Vec::new();
    s.apply(1, b"short", &mut out);
    assert_eq!(out, vec![TAG_EXPIRED]);
}

#[test]
fn snapshot_round_trip_carries_the_dedup_table() {
    let mut s = sm(4, 16);
    let mut out = Vec::new();
    s.apply(100, &env(9, 1, &Cmd::Write(42)), &mut out);
    out.clear(); s.apply(200, &env(9, 2, &Cmd::Cas { old: 42, new: 43 }), &mut out);
    let (handle, pos) = s.freeze().unwrap();
    assert_eq!(pos, 200);
    let mut img = Vec::new();
    <Sessioned<RegisterSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut img).unwrap();
    let mut fresh = sm(4, 16);
    let got = fresh.install_snapshot(200, &mut img.as_slice()).unwrap();
    assert_eq!(got, 200);
    out.clear(); fresh.apply(300, &env(9, 2, &Cmd::Cas { old: 42, new: 43 }), &mut out);
    assert_eq!(resp(&out), (TAG_REPLAYED, Some(CmdResp::CasResult(true))), "dedup survived the snapshot");
    out.clear(); fresh.query(&bincode::serde::encode_to_vec(&uc_lincheck::register::Query::Read, bincode::config::standard()).unwrap(), &mut out);
}
```
(If `RegisterSm`'s query type is not `Query::Read`, read `uc-lincheck/src/register.rs` and use its real query enum; drop the last two lines if it has none.)

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc2_service --test session` → FAIL (unresolved imports).
- [ ] **Step 3: Implement `session.rs`:**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Sessioned<S>`: exactly-once over a remote hop, the Raft-paper
//! client-session model at the raw layer. Each command carries a 16-byte
//! envelope `client_id ++ seq`; a retry inside the per-client window replays
//! the cached response instead of re-applying. Deterministic by construction
//! (BTreeMap, position-based eviction) so every replica's table agrees, and
//! snapshot-composed so it survives restarts.

use std::collections::{BTreeMap, VecDeque};
use crate::config::SnapshotError;
use crate::traits::{RawStateMachine, SnapshotStateMachine};

pub const SESSION_HEADER_LEN: usize = 16;
pub const TAG_FRESH: u8 = 0;
pub const TAG_REPLAYED: u8 = 1;
pub const TAG_EXPIRED: u8 = 2;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Responses remembered per client (a retry older than this is `EXPIRED`).
    pub window: usize,
    /// Clients remembered; the client least recently seen (by log position) is evicted.
    pub max_clients: usize,
}
impl Default for SessionConfig {
    fn default() -> Self { Self { window: 4096, max_clients: 65_536 } }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct ClientState {
    highest_seq: u64,
    last_seen_pos: u64,
    /// (seq, response bytes) oldest-first, len <= window.
    window: VecDeque<(u64, Vec<u8>)>,
}

pub struct Sessioned<S> {
    inner: S,
    cfg: SessionConfig,
    clients: BTreeMap<u64, ClientState>,
}

impl<S: RawStateMachine> Sessioned<S> {
    pub fn new(inner: S, cfg: SessionConfig) -> Self { Self { inner, cfg, clients: BTreeMap::new() } }
    pub fn inner(&self) -> &S { &self.inner }
    pub fn inner_mut(&mut self) -> &mut S { &mut self.inner }

    fn evict_if_needed(&mut self) {
        while self.clients.len() > self.cfg.max_clients {
            // Deterministic: oldest last_seen_pos, ties by smallest client_id
            // (BTreeMap iteration order).
            let victim = self.clients.iter().min_by_key(|(id, c)| (c.last_seen_pos, **id)).map(|(id, _)| *id);
            match victim { Some(id) => { self.clients.remove(&id); } None => break }
        }
    }
}

impl<S: RawStateMachine> RawStateMachine for Sessioned<S> {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
        if cmd.len() < SESSION_HEADER_LEN { out.push(TAG_EXPIRED); return; }
        let client_id = u64::from_le_bytes(cmd[0..8].try_into().unwrap());
        let seq = u64::from_le_bytes(cmd[8..16].try_into().unwrap());
        let body = &cmd[SESSION_HEADER_LEN..];
        let window = self.cfg.window;
        let st = self.clients.entry(client_id).or_default();
        st.last_seen_pos = position;
        if st.highest_seq != 0 && seq <= st.highest_seq {
            if let Some((_, cached)) = st.window.iter().find(|(s, _)| *s == seq) {
                out.push(TAG_REPLAYED);
                out.extend_from_slice(cached);
            } else {
                out.push(TAG_EXPIRED);
            }
            // Note: the inner SM did NOT apply anything, but this frame still
            // advanced our position; last_applied() reports the inner's, and
            // the framework's idempotent re-entry keys on it — see the note
            // in `last_applied` below.
            return;
        }
        // fresh (seq > highest, including gaps)
        out.push(TAG_FRESH);
        let start = out.len();
        self.inner.apply(position, body, out);
        let resp = out[start..].to_vec();
        let st = self.clients.get_mut(&client_id).expect("entry above");
        st.highest_seq = seq;
        st.window.push_back((seq, resp));
        while st.window.len() > window { st.window.pop_front(); }
        self.evict_if_needed();
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) { self.inner.query(q, out) }
    fn last_applied(&self) -> Option<u64> { self.inner.last_applied() }
}
```
**Careful — `last_applied` semantics.** The framework skips frames at or below `last_applied()` (idempotent re-entry) and the inner SM does not advance on a replayed/expired frame. That is correct: re-entry would re-run `Sessioned::apply` for the replayed frame, which is itself idempotent (it replays again). But the dedup table must not be *behind* the inner SM after a restart: a replay from the journal re-runs every frame through `Sessioned::apply`, rebuilding the table — so the table is always a function of the applied prefix. Keep a `max_pos_seen: Option<u64>` field updated on every call and document that `last_applied()` deliberately returns the inner's (the framework contract), with the dedup table reconstructed by replay. Snapshot:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct TableImage { window: usize, max_clients: usize, clients: BTreeMap<u64, ClientState> }

impl<S: SnapshotStateMachine> SnapshotStateMachine for Sessioned<S> {
    type SnapshotHandle = (Vec<u8>, S::SnapshotHandle);
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let (inner_handle, pos) = self.inner.freeze()?;
        let img = TableImage { window: self.cfg.window, max_clients: self.cfg.max_clients, clients: self.clients.clone() };
        let blob = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| SnapshotError::Build(format!("session table encode: {e}")))?;
        Ok(((blob, inner_handle), pos))
    }
    fn stream_snapshot(handle: Self::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        let (blob, inner) = handle;
        dst.write_all(&(blob.len() as u64).to_le_bytes()).map_err(|e| SnapshotError::Build(e.to_string()))?;
        dst.write_all(&blob).map_err(|e| SnapshotError::Build(e.to_string()))?;
        S::stream_snapshot(inner, dst)
    }
    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> {
        let mut len = [0u8; 8];
        src.read_exact(&mut len).map_err(|e| SnapshotError::Install(e.to_string()))?;
        let mut blob = vec![0u8; u64::from_le_bytes(len) as usize];
        src.read_exact(&mut blob).map_err(|e| SnapshotError::Install(e.to_string()))?;
        let (img, _): (TableImage, _) = bincode::serde::decode_from_slice(&blob, bincode::config::standard())
            .map_err(|e| SnapshotError::Install(format!("session table decode: {e}")))?;
        let got = self.inner.install_snapshot(position, src)?;
        self.clients = img.clients;
        Ok(got)
    }
}
```
Use the real `SnapshotError` variant names from `uc2_service/src/config.rs` (read it; if the variants are e.g. `Io`/`Corrupt`, map accordingly). Add `serde` derive on `ClientState` (crate already depends on `serde` with `derive`).

- [ ] **Step 4: Run** — `cargo test -p uc2_service --test session` → PASS; `cargo test -p uc2_service` → PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(service): Sessioned<S> exactly-once adapter (envelope, window, deterministic eviction, snapshot-composed)"`.

---

### Task 6: `uc2_remote` crate — protocol v1 frame codec

**Files:**
- Create: `uc2_remote/Cargo.toml`, `uc2_remote/src/lib.rs`, `uc2_remote/src/frame.rs`, `uc2_remote/src/error.rs`
- Modify: `Cargo.toml` (workspace members: add `"uc2_remote"`, `"uc2_gateway"` now so Task 8 needs no workspace edit)
- Test: `uc2_remote/src/frame.rs` unit tests + `uc2_remote/tests/codec.rs`

**Interfaces:**
- Produces (used by Tasks 7–11):
  ```rust
  pub const PROTOCOL_VERSION: u16 = 1;
  pub const HEADER_LEN: usize = 24;            // len u32 | ty u8 | flags u8 | version u16 | client_id u64 | seq u64
  pub const MAX_FRAME_LEN: u32 = 1 << 20;      // refuse anything larger at the door
  #[repr(u8)] #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub enum FrameType { Hello = 1, HelloOk = 2, HelloRefused = 3, Submit = 4, Query = 5, Response = 6, Status = 7,
                       Redirect = 8, Retry = 9, Unknown = 10, LeaderChanged = 11, Ping = 12, Pong = 13 }
  // flags
  pub const FLAG_LINEARIZABLE: u8 = 0x01;      // QUERY: linearizable (else snapshot)
  pub const FLAG_IS_QUERY: u8 = 0x02;          // RESPONSE answers a QUERY
  pub const FLAG_REPLAYED: u8 = 0x04;          // RESPONSE: Sessioned replay
  pub const FLAG_EXPIRED: u8 = 0x08;           // RESPONSE: Sessioned expired (no payload)
  pub const FLAG_ENVELOPED: u8 = 0x10;         // RESPONSE: tag was present (session_envelope on)
  #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub struct Header { pub ty: FrameType, pub flags: u8, pub version: u16, pub client_id: u64, pub seq: u64 }
  pub fn encode_frame(out: &mut Vec<u8>, h: Header, payload: &[u8]);        // appends one frame
  pub fn decode_header(buf: &[u8]) -> Result<(Header, usize /*payload len*/), FrameError>;   // needs >= HEADER_LEN
  // typed payload helpers (all little-endian, fixed layouts — documented in docs/reference/remote-protocol.md):
  pub struct Hello<'a> { pub app_id: &'a str }                     // u16 len ++ bytes
  pub struct HelloOk<'a> { pub credits: u32, pub leader: Option<u32>, pub leader_addr: &'a str }  // u32 ++ u32(u32::MAX=None) ++ u16 len ++ bytes
  pub struct HelloRefused<'a> { pub reason: u8, pub detail: &'a str } // u8 ++ u16 len ++ bytes
  pub struct ResponseMeta { pub credits: u32, pub acked_seq: u64, pub position: u64 } // 20 bytes, then the response bytes
  pub struct Status { pub acked_seq: u64, pub credits: u32 }       // 12 bytes
  pub struct Leader<'a> { pub node_id: u32, pub addr: &'a str }    // REDIRECT / LEADER_CHANGED: u32 ++ u16 len ++ bytes
  pub struct Retry { pub reason: u8, pub retry_after_us: u32 }     // 5 bytes; reasons:
  pub const RETRY_NOT_SERVING: u8 = 1; pub const RETRY_INSTANCE_RESTART: u8 = 2; pub const RETRY_SERVICE_UNAVAILABLE: u8 = 3;
  pub const HELLO_REFUSED_APP_ID: u8 = 1; pub const HELLO_REFUSED_VERSION: u8 = 2;
  // each helper has encode(&self, out: &mut Vec<u8>) and decode(buf: &[u8]) -> Result<Self, FrameError>
  pub enum FrameError { Short { need: usize, have: usize }, TooLong(u32), BadType(u8), BadVersion(u16), BadPayload(&'static str) }
  ```

- [ ] **Step 1: Crate skeleton** — `uc2_remote/Cargo.toml`:

```toml
[package]
name = "uc2_remote"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "ultima_cluster remote protocol v1 (framed TCP) and the Rust remote client"

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
serde = { workspace = true }
bincode = { workspace = true }
```
Add `"uc2_remote", "uc2_gateway"` to the workspace `members` (create `uc2_gateway/Cargo.toml` + empty `src/lib.rs` now with `[package] name = "uc2_gateway" … [dependencies] uc2_remote = { path = "../uc2_remote" }` so the workspace resolves; Task 8 fills it).

- [ ] **Step 2: Write the failing tests** — `uc2_remote/tests/codec.rs`:

```rust
use uc2_remote::frame::*;

#[test]
fn header_round_trip_and_length_prefix() {
    let h = Header { ty: FrameType::Submit, flags: 0, version: PROTOCOL_VERSION, client_id: 0xC11E, seq: 42 };
    let mut buf = Vec::new();
    encode_frame(&mut buf, h, b"payload");
    assert_eq!(buf.len(), HEADER_LEN + 7);
    let (got, plen) = decode_header(&buf).unwrap();
    assert_eq!(got, h);
    assert_eq!(plen, 7);
    assert_eq!(&buf[HEADER_LEN..], b"payload");
}

#[test]
fn short_and_oversized_and_bad_type_are_errors() {
    assert!(matches!(decode_header(&[0u8; 3]), Err(FrameError::Short { .. })));
    let mut buf = Vec::new();
    encode_frame(&mut buf, Header { ty: FrameType::Ping, flags: 0, version: PROTOCOL_VERSION, client_id: 1, seq: 1 }, &[]);
    buf[4] = 0xEE;
    assert!(matches!(decode_header(&buf), Err(FrameError::BadType(0xEE))));
    let mut big = buf.clone();
    big[0..4].copy_from_slice(&(MAX_FRAME_LEN + 1).to_le_bytes());
    assert!(matches!(decode_header(&big), Err(FrameError::TooLong(_))));
}

#[test]
fn typed_payloads_round_trip() {
    let mut out = Vec::new();
    HelloOk { credits: 512, leader: Some(2), leader_addr: "10.0.0.2:9100" }.encode(&mut out);
    let h = HelloOk::decode(&out).unwrap();
    assert_eq!((h.credits, h.leader, h.leader_addr), (512, Some(2), "10.0.0.2:9100"));
    out.clear();
    HelloOk { credits: 1, leader: None, leader_addr: "" }.encode(&mut out);
    assert_eq!(HelloOk::decode(&out).unwrap().leader, None);
    out.clear();
    ResponseMeta { credits: 7, acked_seq: 9, position: 4096 }.encode(&mut out);
    assert_eq!(out.len(), 20);
    let m = ResponseMeta::decode(&out).unwrap();
    assert_eq!((m.credits, m.acked_seq, m.position), (7, 9, 4096));
    out.clear();
    Retry { reason: RETRY_NOT_SERVING, retry_after_us: 250_000 }.encode(&mut out);
    assert_eq!(Retry::decode(&out).unwrap().retry_after_us, 250_000);
    out.clear();
    Leader { node_id: 3, addr: "h3:9100" }.encode(&mut out);
    assert_eq!(Leader::decode(&out).unwrap().addr, "h3:9100");
    assert!(matches!(Leader::decode(&out[..3]), Err(FrameError::Short { .. })));
}
```

- [ ] **Step 3: Run to verify it fails** — `cargo test -p uc2_remote` → compile error (no `frame` module).
- [ ] **Step 4: Implement `frame.rs`** — fixed LE layouts; `decode_header` checks `have >= HEADER_LEN`, reads `len`, refuses `len > MAX_FRAME_LEN` and `len < HEADER_LEN`, maps `ty` via a `match` (any other byte → `BadType`), returns `(Header, len as usize - HEADER_LEN)`; `encode_frame` reserves, writes the header with `len = HEADER_LEN + payload.len()`, appends payload. String fields: `u16` length prefix, refuse non-UTF-8 with `BadPayload("utf8")`. Each typed helper is a plain struct with `encode`/`decode`. `lib.rs`: `pub mod frame; pub mod error; pub use error::RemoteError;` (`error.rs` holds `FrameError` + the client's `RemoteError`, defined fully in Task 7 — for now `RemoteError` has `Frame(#[from] FrameError)` and `Io(#[from] std::io::Error)`).
- [ ] **Step 5: Run** — `cargo test -p uc2_remote` → PASS; clippy clean.
- [ ] **Step 6: Commit** — `git commit -am "feat(remote): uc2_remote crate — protocol v1 frame codec"`.

---

### Task 7: `uc2_remote::RemoteClient` — pipelined, credit-gated, redirect-following

**Files:**
- Create: `uc2_remote/src/client.rs`, `uc2_remote/src/conn.rs` (framed reader/writer over `TcpStream`)
- Modify: `uc2_remote/src/lib.rs`, `uc2_remote/src/error.rs`
- Test: `uc2_remote/tests/client_fake_edge.rs` (an in-process fake edge speaking the protocol — no cluster needed)

**Interfaces:**
- Produces:
  ```rust
  pub struct RemoteConfig { pub app_id: String, pub members: Vec<String> /* "host:port" */, pub client_id: Option<u64> /* None = random */,
                            pub max_inflight: u32 /* local cap, default 1024 */, pub request_timeout: Duration /* default 10 s */,
                            pub connect_timeout: Duration /* 2 s */, pub resend_on_unknown: bool /* default true */ }
  pub struct RemoteClient { .. }                        // Send + Sync; clone-free handle, Arc inside
  impl RemoteClient {
      pub fn connect(cfg: RemoteConfig) -> Result<Self, RemoteError>;     // tries members in order, follows REDIRECT at HELLO
      pub fn submit(&self, cmd: &[u8]) -> Result<Ticket, RemoteError>;    // blocks while credits are exhausted
      pub fn query(&self, q: &[u8], linearizable: bool) -> Result<Ticket, RemoteError>;
      pub fn client_id(&self) -> u64;
      pub fn leader(&self) -> Option<(u32, String)>;
      pub fn stats(&self) -> RemoteStats;                                  // redirects, leader_changes, reconnects, resends, retries, unknown, expired
      pub fn shutdown(self);
  }
  pub struct Ticket { .. }
  impl Ticket { pub fn wait(self) -> Result<RemoteResponse, RemoteError>; pub fn wait_timeout(self, d: Duration) -> Result<RemoteResponse, RemoteError>; }
  pub struct RemoteResponse { pub position: u64, pub bytes: bytes::Bytes, pub replayed: bool }
  pub enum RemoteError { Io(std::io::Error), Frame(FrameError), HelloRefused { reason: u8, detail: String }, Expired /* Sessioned window */,
                         Unknown /* resend_on_unknown=false, or envelope off */, TimedOut, NoMembersReachable, Closed }
  ```
  Behaviour contract (spec §4.5): every submit ends in exactly one of `Ok(RemoteResponse)` / `Err(Expired|Unknown|TimedOut|Closed)`; REDIRECT / LEADER_CHANGED / connection loss are handled internally (reconnect, re-HELLO, re-send every unanswered `seq` in order); RETRY honoured with `retry_after` + jitter; credits: never more than `credits` unanswered seqs beyond `acked_seq`.

- [ ] **Step 1: Write the failing test** — `uc2_remote/tests/client_fake_edge.rs` builds a minimal fake edge thread (accept, read frames with `decode_header`, answer HELLO with `HelloOk{credits: 2, leader: Some(1), ..}`, answer SUBMIT seq N with `RESPONSE{meta{credits: 2, acked_seq: N, position: N*64}, payload = cmd reversed}`; on a flag, answer the first SUBMIT with `REDIRECT{node 2, addr of a second fake edge}`; on another flag answer with `RETRY{NOT_SERVING, 1000}` once; on another, drop the connection after HELLO once):

```rust
// Key assertions:
#[test] fn submit_pipelined_under_credits() { /* credits=2: issue 6 submits from one thread; all 6 resolve; fake edge asserts it never saw >2 unanswered */ }
#[test] fn redirect_is_followed_and_pending_resent_in_order() { /* edge A redirects on first SUBMIT; client reconnects to B; B sees seqs 1..=3 in order; all tickets resolve */ }
#[test] fn retry_is_honoured_with_hint() { /* edge answers RETRY once; ticket resolves after ~1 ms; stats.retries == 1 */ }
#[test] fn connection_loss_resends_unanswered() { /* edge drops after accepting seq 1 (no response); client reconnects (same addr), resends seq 1; resolves; stats.reconnects == 1, resends == 1 */ }
#[test] fn expired_surfaces_as_error() { /* edge answers with FLAG_EXPIRED|FLAG_ENVELOPED; ticket.wait() == Err(RemoteError::Expired) */ }
```
Write the fake edge as a reusable `tests/common/fake_edge.rs` (`FakeEdge::spawn(behaviour) -> (addr, JoinHandle, Arc<Observed>)`).

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc2_remote --test client_fake_edge` → compile error.
- [ ] **Step 3: Implement** — `conn.rs`: `FramedConn { stream: TcpStream, rd: Vec<u8>, rd_len: usize }` with `read_frame(&mut self) -> io::Result<(Header, Bytes)>` (reads until `HEADER_LEN`, decodes, reads payload) and `write_frame(&mut self, h: Header, payload: &[u8]) -> io::Result<()>` (`encode_frame` into a scratch `Vec` then `write_all`); `set_nodelay(true)`. `client.rs`: shared state under `Arc<Inner>`:

```rust
struct Inner {
    cfg: RemoteConfig,
    client_id: u64,
    next_seq: AtomicU64,
    state: Mutex<State>,            // conn writer, credits, acked_seq, pending: BTreeMap<u64, Pending>, leader, closed
    cv: Condvar,                    // credits/slot availability
    stats: Stats,                   // atomics
}
struct Pending { kind: Kind /*Submit|Query{lin}*/, payload: Bytes, tx: mpsc::SyncSender<Result<RemoteResponse, RemoteError>>, sent_at: Instant }
```
`connect`: for each member in order: TCP connect (timeout) → send HELLO → expect `HELLO_OK` (record credits/leader) or `HELLO_REFUSED` (return error) or `REDIRECT` (go to that addr next); spawn the **reader thread** (owns the read half via `try_clone`) which loops on `read_frame` and dispatches: `RESPONSE` → pop `pending[seq]`, send `Ok(RemoteResponse)` (or `Err(Expired)`), update `credits/acked_seq`, `cv.notify_all()`; `STATUS` → update + notify; `RETRY` → sleep `retry_after + jitter`, re-send that seq; `UNKNOWN` → if `resend_on_unknown` re-send else resolve `Err(Unknown)`; `REDIRECT`/`LEADER_CHANGED` → `reconnect_to(addr)`; read error → `reconnect_round_robin()`. `reconnect_*`: under the state lock, open a new conn, HELLO, replace writer, reset `credits` from `HELLO_OK`, then re-send every `pending` in `seq` order (credit-gated). `submit`: `seq = next_seq.fetch_add(1)+1`; wait on `cv` while `pending.len() >= min(cfg.max_inflight, credits)`; insert `Pending`; write frame; return `Ticket{rx}`. `Ticket::wait` = `rx.recv()`; `wait_timeout` maps to `TimedOut`. A **sweeper** in the reader thread's idle path (or `wait_timeout`) fails pendings older than `request_timeout` with `TimedOut` and removes them. `shutdown` sets `closed`, drops the writer, fails all pendings with `Closed`, joins the reader.

- [ ] **Step 4: Run** — `cargo test -p uc2_remote` → PASS; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(remote): RemoteClient — pipelined, credit-gated, redirect-following, ordered resend"`.

---

### Task 8: `uc2_gateway::Edge` core — attach, accept, submit/query round-trip

**Files:**
- Create: `uc2_gateway/src/lib.rs`, `uc2_gateway/src/config.rs`, `uc2_gateway/src/edge.rs`, `uc2_gateway/src/conn.rs`
- Modify: `uc2_gateway/Cargo.toml`
- Test: `uc2_gateway/tests/roundtrip.rs` (in-process 1-node cluster + `RegisterSm` service + `Edge` + `RemoteClient`)

**Interfaces:**
- Produces:
  ```rust
  // config.rs
  pub struct Member { pub node_id: u32, pub gateway: String }
  pub struct EdgeConfig { pub instance_dir: PathBuf, pub app_id: String, pub listen: SocketAddr, pub members: Vec<Member>,
                          pub session_envelope: bool /* true */, pub max_inflight: u32 /* Engine window, 4096 */,
                          pub per_conn_inflight: u32 /* initial credits, 256 */, pub status_interval: Duration /* 200 ms */,
                          pub request_timeout: Duration /* 10 s */ }
  impl EdgeConfig { pub fn validate(&self) -> Result<(), ConfigError> }   // listen parses, members non-empty & unique ids, per_conn <= max
  // edge.rs
  pub struct Edge { .. }
  impl Edge { pub fn start(cfg: EdgeConfig) -> Result<Edge, EdgeError>; pub fn local_addr(&self) -> SocketAddr;
              pub fn stats(&self) -> EdgeStats; pub fn stop(self); }
  pub enum EdgeError { Config(ConfigError), Attach(uc2_client::ClientError), Bind(std::io::Error) }
  pub struct EdgeStats { pub connections: u64, pub submits: u64, pub queries: u64, pub responses: u64, pub redirects: u64,
                         pub retries: u64, pub unknown: u64, pub backpressure_events: u64, pub leader_changes: u64 }
  ```

- [ ] **Step 1: Write the failing test** — `uc2_gateway/tests/roundtrip.rs`:

```rust
//! One node, one typed RegisterSm service wrapped in Sessioned, one Edge,
//! one RemoteClient: writes, CAS, a linearizable read and a snapshot read
//! all round-trip through the framed protocol with the envelope on.
use std::time::Duration;
use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::{RemoteClient, RemoteConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, Sessioned, SessionConfig};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

mod common; // tempdir() + start_single_node(root) -> (uc2_node::Node, PathBuf) using the lin_v2 NodeConfig shape with n=1

#[test]
fn write_cas_read_round_trip_through_the_edge() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(ServiceConfig::new(&dir, common::APP), Sessioned::new(RegisterSm::default(), SessionConfig::default())).start().unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(EdgeConfig { instance_dir: dir.clone(), app_id: common::APP.into(), listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member { node_id: 0, gateway: "127.0.0.1:0".into() }], ..EdgeConfig::defaults() }).unwrap();
    let client = RemoteClient::connect(RemoteConfig { app_id: common::APP.into(), members: vec![edge.local_addr().to_string()], ..RemoteConfig::defaults() }).unwrap();
    let enc = |c: &Cmd| bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap();
    let dec = |b: &[u8]| -> CmdResp { bincode::serde::decode_from_slice(b, bincode::config::standard()).unwrap().0 };
    let r = client.submit(&enc(&Cmd::Write(7))).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
    assert!(!r.replayed);
    let r = client.submit(&enc(&Cmd::Cas { old: 7, new: 8 })).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::CasResult(true));
    let q = bincode::serde::encode_to_vec(&uc_lincheck::register::Query::Read, bincode::config::standard()).unwrap(); // use RegisterSm's real query type
    let r = client.query(&q, true).unwrap().wait().unwrap();
    let v: Option<u64> = bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard()).unwrap().0;
    assert_eq!(v, Some(8));
    assert!(client.query(&q, false).unwrap().wait().is_ok());
    let s = edge.stats();
    assert_eq!((s.submits, s.queries), (2, 2));
    client.shutdown(); edge.stop(); svc.stop(); node.stop();
}
```
(`common/mod.rs`: copy the `tempdir()` helper and a one-node `NodeConfig` builder from `uc2_node/tests/lincheck_v2/mod.rs:221-260`; `await_serving` polls `node.is_serving_leader()`.) Add to `uc2_gateway/Cargo.toml`: `[dependencies] uc2_client, uc2_remote, uc_protocol, uc2_log (paths), bytes, thiserror, parking_lot (workspace); [dev-dependencies] uc2_node, uc2_service, uc2_net, uc-lincheck (default-features=false, features=["v2"]), serde, bincode, tempfile, rand (workspace)`.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc2_gateway --test roundtrip` → compile error.
- [ ] **Step 3: Implement** —
  - `config.rs` as in Interfaces (+ `EdgeConfig::defaults()` returning the documented defaults with empty `instance_dir`/`members` — only for tests/builders; `validate` refuses empties).
  - `conn.rs`: `pub(crate) struct Conn { idx: u32, writer: Mutex<TcpStream>, client_id: AtomicU64, credits: AtomicU32, inflight: AtomicU32, acked_seq: AtomicU64, corr_to_seq: Mutex<HashMap<u32, (u64, bool /*is_query*/)>>, next_corr: AtomicU32, gate: (Mutex<()>, Condvar) }` and `ConnTable { slots: RwLock<Vec<Option<Arc<Conn>>>> }` with `insert -> idx`, `get(idx)`, `remove(idx)`, `for_each`.
  - `edge.rs`: `Edge::start`: `cfg.validate()`; `Engine::attach(&cfg.instance_dir, &cfg.app_id, EngineConfig { max_inflight: cfg.max_inflight, request_timeout: cfg.request_timeout, serving_gate: false, ..Default::default() })` → `(send, poll)` (`serving_gate: false` — the edge decides itself via `can_serve()` so it can send REDIRECT, not `NotServing`); `TcpListener::bind(cfg.listen)`; spawn `acceptor` thread: for each stream → `ConnTable::insert` → spawn `reader(conn, send.clone(), ..)`; spawn `driver(poll, table, ..)`.
  - `reader`: `FramedConn::read_frame` loop. First frame must be `HELLO` with matching `app_id` and `version == PROTOCOL_VERSION`, else `HELLO_REFUSED{reason}` and close. Reply `HELLO_OK{credits: per_conn_inflight, leader, leader_addr}` (leader from `send.leader_hint()` + member map). Then per frame: `SUBMIT` → (credit gate: `while inflight >= credits { wait on gate }`) → if `!send.can_serve()`: write `REDIRECT{members[leader_hint]}` or `RETRY{NOT_SERVING, election_timeout_hint=300_000}` and continue; else build payload (`session_envelope` → `16-byte header ++ cmd`), `corr = next_corr++`, record `corr_to_seq[corr] = (seq, false)`, `user_data = (idx as u64) << 32 | corr`, loop `send.try_submit(user_data, &payload)`: `Ok` → `inflight += 1`; `Err(Backpressure)` → `stats.backpressure += 1`, halve `credits` (min 1), `thread::yield_now()` and retry (the socket is not read meanwhile — TCP backstop); `Err(NotServing)` → REDIRECT/RETRY as above; `Err(PayloadTooLarge)` → `RETRY{SERVICE_UNAVAILABLE}`? No — refuse loudly: write `RESPONSE` with `FLAG_EXPIRED`? Use `HELLO_REFUSED`-style close: write `RETRY{reason: RETRY_SERVICE_UNAVAILABLE}` and log; document "payload > max_payload is a client bug". `Err(InstanceRestart)` → write `LEADER_CHANGED{leader: None}` to all conns (via table) and close them. `QUERY` → same but `send.try_query(user_data, q, if flags & FLAG_LINEARIZABLE { Linearizable } else { Snapshot })` (no envelope on queries). `PING` → `PONG`. Connection EOF/error → `table.remove(idx)` (in-flight completions for a removed conn are dropped on arrival).
  - `driver`: loop `poll.poll(|c| …)`: `idx = (c.user_data >> 32) as u32; corr = c.user_data as u32;` look up conn + `(seq, is_query)` (remove the map entry, `inflight -= 1`, `acked_seq = max(acked_seq, seq)` for submits, `credits = min(credits * 2, per_conn_inflight)` on a successful completion after a squeeze, notify gate). Map `Outcome`: `Response(bytes)` → if `session_envelope && !is_query`: `tag = bytes[0]`, rest = `bytes[1..]`, flags `FLAG_ENVELOPED | (REPLAYED|EXPIRED per tag)`; write `RESPONSE{meta{credits, acked_seq, position: c.position.unwrap_or(0)}} ++ rest`; `NotLeader{hint}` → `REDIRECT{members[hint]}` (or `RETRY{NOT_SERVING}`); `Retry` → `RETRY{SERVICE_UNAVAILABLE, 1000}`; `TimedOut` → `UNKNOWN`; `InstanceRestart` → `LEADER_CHANGED{None}` to all + close all. Between polls (every 64 iterations or when `poll` returned 0): the **status timer** — for each conn with no write in `status_interval`, write `STATUS{acked_seq, credits}`; and the leader watch (Task 9). Idle strategy: `poll.wait_handle()` park with a 1 ms cap (copy `PipelinedClient`'s driver ladder in `uc2_client/src/pipelined.rs`).
  - `Edge::stop`: set `stop` flag, shutdown listener (connect-to-self trick or `set_nonblocking` + poll), join threads, close conns.

- [ ] **Step 4: Run** — `cargo test -p uc2_gateway --test roundtrip` → PASS (both envelope on; add a second test fn with `session_envelope: false` and a plain `RegisterSm` service asserting `replayed == false` and the bytes decode). Clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(gateway): uc2_gateway Edge — attach, accept, credit-gated submit/query round-trip over uc2_remote"`.

---

### Task 9: Leader watch, redirect across failover, credits under backpressure

**Files:**
- Create: `uc2_gateway/src/watch.rs`
- Modify: `uc2_gateway/src/edge.rs` (driver hook), `uc2_gateway/src/conn.rs`
- Test: `uc2_gateway/tests/failover.rs` (3 in-process nodes, 3 edges, leader crash → `LEADER_CHANGED` → client resubmits with `replayed`/`fresh` accounting), `uc2_gateway/tests/credits.rs`

**Interfaces:**
- Produces: `pub(crate) struct LeaderWatch { last: (bool, Option<u32>) }` with `fn poll(&mut self, send: &SendHalf) -> Option<(bool /*can_serve*/, Option<u32>)>` returning `Some` on transition; `EdgeStats.leader_changes`.

- [ ] **Step 1: Write the failing tests** — `failover.rs` (uses a 3-node `common::start_cluster(root, 3)` built from the `lincheck_v2` `make_config` shape, each node with its own `Sessioned<RegisterSm>` service and its own `Edge`; members map = all three edge addrs keyed by node id):

```rust
#[test]
fn leader_crash_redirects_and_resend_is_deduped() {
    // 1. connect RemoteClient to the FOLLOWER's edge: first submit must arrive as a REDIRECT
    //    (assert client.stats().redirects == 1 and client.leader() == Some(leader_id)) and then succeed.
    // 2. pipeline 200 Write(i) submits; after 100 have been issued, node.crash() the leader (node first, then its service);
    //    keep issuing; all 200 tickets must resolve Ok or Err(Expired) (never Unknown/TimedOut — resend_on_unknown=true);
    //    assert every Ok response's CmdResp == WriteAck and the final linearizable read equals the highest i whose ticket was Ok
    //    (monotone writes, so the register holds the last successful one).
    // 3. assert client.stats().leader_changes >= 1 and edge(follower).stats().redirects >= 1.
}
```
`credits.rs`: with `per_conn_inflight = 4` and an edge whose `max_inflight` is 8, two clients each pipeline 50 submits; assert each client's observed `credits` never exceeds 4 (expose `RemoteClient::stats().max_credits_seen`), everything resolves, and `edge.stats().backpressure_events` is reported (≥ 0; the assertion is on correctness, not on forcing backpressure). Add a unit test in `watch.rs` for transition detection (`None` on no change, `Some` on can_serve flip and on hint change).

- [ ] **Step 2: Run to verify it fails** — compile errors / assertion on missing `leader_changes`.
- [ ] **Step 3: Implement** — `watch.rs` as above; the driver calls `watch.poll(&send)` every 64 poll iterations (≈ sub-millisecond at load, ≤ 1 ms idle): on transition, `stats.leader_changes += 1`, compute `(leader_id, addr)` from the member map, write `LEADER_CHANGED` to every conn (a conn whose write fails is removed). Credits: `Conn::squeeze()` halves (min 1) and `Conn::relax()` doubles (max `per_conn_inflight`) — relax runs in the driver on each successful completion after a squeeze; both trigger an immediate `STATUS` write when credits *increase* (so a throttled client learns it may send again without waiting for a response).
- [ ] **Step 4: Run** — `cargo test -p uc2_gateway` → PASS; `cargo clippy -p uc2_gateway --all-targets -- -D warnings` → clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(gateway): leader watch + LEADER_CHANGED, redirect across failover, AIMD credits under backpressure"`.

---

### Task 10: `uc2-gateway` binary, `gateway.toml`, packaging

**Files:**
- Create: `uc2_gateway/src/bin/uc2-gateway.rs`, `uc2_gateway/src/config_file.rs`
- Create: `packaging/gateway.example.toml`, `packaging/systemd/uc2-gateway.service`
- Modify: `uc2_gateway/Cargo.toml` (`[[bin]] name = "uc2-gateway"`; deps `clap`, `toml`, `serde`, `anyhow`, `signal-hook` — the same versions `uc2_node`'s bin uses; keep them out of the library's hot path: they are fine as crate deps since the lib is not dependency-minimal-by-contract — `uc2_remote` is)
- Test: `uc2_gateway/tests/config_file.rs`

**Interfaces:**
- Produces: `pub fn load_from_path(path: &Path) -> Result<EdgeConfig, ConfigFileError>` with `ConfigFileError { Read{path, source}, Parse{path, source: toml::de::Error}, Invalid(ConfigError) }`; binary exit codes: `2` = config refusal (systemd `RestartPreventExitStatus=2`), `1` = runtime start failure, `0` = clean stop on SIGTERM/SIGINT.

- [ ] **Step 1: Write the failing test** — `config_file.rs` test: a minimal TOML parses to the expected `EdgeConfig` (defaults filled), an unknown key is refused by name (`deny_unknown_fields`), a duplicate `node_id` is refused, `per_conn_inflight > max_inflight` is refused.

```toml
# packaging/gateway.example.toml (also the test fixture's shape)
# ---------------------------------------------------------------- required
[local]
instance_dir = "/srv/uc2/n0"          # the co-located node's instance directory
app_id = "myapp"
listen = "0.0.0.0:9200"               # where remote clients connect

# Every member's gateway address, keyed by node id — how REDIRECT and
# LEADER_CHANGED tell clients where the leader is. Keep it identical on every host.
[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
[[members]]
node_id = 1
gateway = "10.0.0.11:9200"
[[members]]
node_id = 2
gateway = "10.0.0.12:9200"

# ---------------------------------------------------------------- optional
[limits]
max_inflight = 4096            # Engine window shared by all connections
per_conn_inflight = 256        # initial credits per connection
request_timeout_ms = 10000
status_interval_ms = 200

[session]
envelope = true                # prepend client_id/seq for Sessioned<S> services; false = raw pass-through
```

- [ ] **Step 2: Run to verify it fails**, **Step 3: Implement** `config_file.rs` (serde structs mirroring the TOML with `#[serde(deny_unknown_fields)]` per section, then `EdgeConfig::validate`) and the binary (copy `uc2_node/src/bin/uc2-node.rs:33-93`'s shape: `Args { --config }`, load → exit 2 on error; `Edge::start` → exit 1 on error; `signal_hook::flag::register` for SIGTERM/SIGINT; loop sleeping 100 ms printing a one-line stats summary every 10 s to stderr; on stop `edge.stop()`; exit 0). `uc2-gateway.service`:

```
[Unit]
Description=ultima_cluster gateway
After=uc2-node.service
BindsTo=uc2-node.service

[Service]
Type=simple
ExecStart=/usr/local/bin/uc2-gateway --config /etc/uc2/gateway.toml
Restart=on-failure
RestartSec=1
RestartPreventExitStatus=2
TimeoutStopSec=5
KillSignal=SIGTERM
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```
- [ ] **Step 4: Run** — `cargo test -p uc2_gateway` → PASS; `cargo run -p uc2_gateway --bin uc2-gateway -- --config /nonexistent` → exits 2 with a named error; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "feat(gateway): uc2-gateway binary, gateway.toml, systemd unit"`.

---

### Task 11: Remote lincheck capstone (hard-crash, three edges in the loop)

**Files:**
- Modify: `examples/uc2-crashtest/Cargo.toml` (deps `uc2_gateway`, `uc2_remote`; new bin `uc2-crashtest-gateway`)
- Modify: `examples/uc2-crashtest/src/bin/uc2-crashtest-service.rs` (`--sessioned` flag → `Sessioned<RegisterSm>`)
- Create: `examples/uc2-crashtest/src/bin/uc2-crashtest-gateway.rs` (thin: `--instance-dir --app-id --listen --members id@addr,… [--no-envelope]` → `Edge::start`, run until SIGTERM/SIGKILL)
- Create: `examples/uc2-crashtest/tests/remote_lin.rs` (feature `hard-crash-tests`)
- Modify: `.github/workflows/nightly.yml` (`crashtest` job already runs `cargo test -p uc2-crashtest --features hard-crash-tests` — the new test is picked up; add `timeout-minutes` headroom if needed)

**Interfaces:**
- Consumes: `common/mod.rs` helpers (`NODE_BIN`, `SERVICE_BIN`, `Reap`, `spawn_node`, `spawn_service`, `wait_for_ready`, `wait_for_fresh_instance`, `tempdir`); `uc_lincheck::history::{History, Outcome}`, `checker::check_register`, `register::{Cmd, CmdResp, RegisterSm}`.

- [ ] **Step 1: Write the failing test** — `remote_lin.rs`:

```rust
#![cfg(feature = "hard-crash-tests")]
//! Three node procs, three Sessioned services, three gateway procs; remote
//! clients pipeline Write/Cas/Read through uc2_remote; the leader NODE is
//! SIGKILLed and respawned repeatedly. Assert: linearizable, and no acked
//! write lost (every Ok(WriteAck) is visible to the checker as Ok).
mod common;
use common::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use uc_lincheck::{checker::{check_register, Verdict}, history::{History, Outcome}, register::{Cmd, CmdResp}};
use uc2_remote::{RemoteClient, RemoteConfig};

const GATEWAY_BIN: &str = env!("CARGO_BIN_EXE_uc2-crashtest-gateway");

fn remote_lin_once(seed: u64, envelope: bool) {
    let root = tempdir();
    // 1. three instance dirs, three bound-port members (127.0.0.1:0 bound then released, like hard_crash.rs)
    // 2. spawn 3 nodes (Reap), wait_for_ready each; spawn 3 services with --sessioned iff envelope; spawn 3 gateways
    //    (listen 127.0.0.1:<port_i>, --members "0@127.0.0.1:p0,1@…,2@…", --no-envelope iff !envelope)
    // 3. workers: 4 threads, each a RemoteClient over ALL THREE gateway addrs (members), client_id fixed per worker,
    //    loop for 20 s: pick Write(rand)/Cas{old,new}/Read(linearizable); inv = history.invoke();
    //    submit → ticket.wait(): Ok(resp) → Outcome::Ok(decode), Err(Expired|Unknown|TimedOut|Closed) → Outcome::Indeterminate
    // 4. chaos thread: every 3 s pick the CURRENT leader (read each instance dir's cnc via uc2_log::cnc::CncPage::open + NODE_FLAG_LEADER),
    //    SIGKILL that node proc (Reap drop) and its service proc, respawn both (node first; wait_for_fresh_instance); gateway procs
    //    are NOT restarted — they must survive the InstanceRestart and keep serving after reconnects.
    // 5. stop, join, entries = history.into_entries(); assert ok*100 >= len*70 (remote path is slower: lower liveness bar than lin_v2's 80);
    //    match check_register(&entries) { Linearizable => {}, Violation => dump+panic, Inconclusive => panic }
}

#[test] fn remote_lin_envelope_on()  { remote_lin_once(1, true); }
#[test] fn remote_lin_envelope_off() { remote_lin_once(2, false); }
```
Write the body fully (the shape above is the contract; copy the spawn/kill/readiness helpers from `hard_crash.rs` and the worker op selection from `lincheck_v2/mod.rs:1247`). With the envelope **on**, additionally assert the "no acked write lost" property directly: maintain a per-worker set of `(value, position)` for every `Ok(WriteAck)`; at the end read the register and assert it equals the `value` of the highest `position` across all workers' Ok writes (positions are total-order, so the last acked write must be the visible one unless a later Ok write/CAS superseded it — compute the expected value by replaying Ok outcomes in position order: `Write(v)` sets v, `Cas{old,new}` with `CasResult(true)` sets new).

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc2-crashtest --features hard-crash-tests --test remote_lin` → compile error (bins missing).
- [ ] **Step 3: Implement** the two bins (service: `if args.sessioned { ServiceBuilder::new(cfg, Sessioned::new(RegisterSm::default(), SessionConfig::default())).start() } else { ServiceBuilder::new(cfg, RegisterSm::default()).start() }` held in an enum/`Box<dyn Any>`-free pair of `Option`s; gateway bin as described) and fill the test body.
- [ ] **Step 4: Run** — `cargo test -p uc2-crashtest --features hard-crash-tests --test remote_lin -- --nocapture` → PASS (both variants; each ≤ 90 s locally). Then the full `cargo test -p uc2-crashtest --features hard-crash-tests` → PASS. Check `nightly.yml` `crashtest` `timeout-minutes` (60) still has headroom; bump to 75 if the run is > 40 min.
- [ ] **Step 5: Commit** — `git commit -am "test(crashtest): remote lincheck capstone — three gateways in the loop, leader SIGKILL, zero acked-write loss"`.

---

### Task 12: `m12_gate` (gateway vs direct `Engine`) + gate-doc skeleton

**Files:**
- Create: `uc2_gateway/examples/m12_gate.rs`
- Create: `docs/benchmarks/uc2-m12-gate-2026-08-22.md` (skeleton with every row from spec §8, local smoke numbers labelled as such, fleet cells empty)

- [ ] **Step 1: Write the harness** — reuse `m5_gate`'s `all` shape (3 in-process nodes, `CountSm` services — the typed one, so the two arms share the service) with two arms selected by `--arm direct|gateway`: `direct` = `m5_gate`'s `run_client_measurement` copied verbatim (Engine on the leader's instance dir); `gateway` = one `Edge` per node + `RemoteClient` connected to the leader's edge, same payload/inflight/secs, one sender thread + ticket-wait threads, the same `ClientStats`/`print_report`. Print both arms' `responses/s`, `p50`, `p99` and the ratio `gateway/direct`. Args: `--secs 6 --payload 64 --inflight 4096 --envelope {on,off}`.
- [ ] **Step 2: Smoke** — `cargo run -p uc2_gateway --release --example m12_gate -- --secs 6 --payload 64` on the dev box; record in the gate doc as **smoke (dev box, 4 vCPU, not a gate number)**.
- [ ] **Step 3: Gate doc skeleton** — table with spec §8's rows; fill: remote capstone (CI, PASS once Task 11 is green in nightly — leave "pending nightly" until then), codec share (Task 4's smoke lines), gateway cost (Task 12 smoke), remaining rows "M12b/c/d". State the proposed bar (≥ 0.8× direct-Engine at equal inflight) and that it is fleet-only.
- [ ] **Step 4: Commit** — `git commit -am "gate(m12a): m12_gate harness + gate doc skeleton (smoke numbers labelled)"`.

---

### Task 13: Docs, spec amendment, CI touch-ups

**Files:**
- Create: `docs/how-to/run-a-gateway.md`, `docs/reference/gateway-config.md`, `docs/reference/remote-protocol.md`, `docs/notes/uc2-gateway-shapes-and-flow-control.md`
- Modify: `docs/reference/state-machine-contract.md` (Task 4; add the `Sessioned` section), `docs/QUICKSTART.md` (one "remote clients" pointer), `docs/ops/uc2-runbook.md` (gateway ops: start/stop, what `REDIRECT`/`LEADER_CHANGED` look like, stats line), `README.md` crate table (+`uc2_remote`, `uc2_gateway`), `CLAUDE.md` workspace crate list (+ the two crates and the `Sessioned`/`RawStateMachine` one-liners), `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §3.1 (blanket impl instead of `Typed<S>`, 1-byte tag, `FLAG_*` names — amend, do not rewrite)
- Modify: `.github/workflows/ci.yml` — nothing required (`cargo test --workspace --exclude uc2_node` picks up `uc2_remote`/`uc2_gateway`); add `cargo clippy -p uc2_service --features apply-profile --all-targets -- -D warnings` next to the `ultima_db` clippy line so the probe never rots.

- [ ] **Step 1: `docs/reference/remote-protocol.md`** — the frame layout byte-for-byte, every type's payload layout (copy Task 6's Interfaces block as tables), flags, the credit rule ("at most `credits` unanswered seqs beyond `acked_seq`"), the `RETRY` reasons and that it is a state signal, the failover promises (spec §4.5) — this is the page a non-Rust port implements from.
- [ ] **Step 2: `docs/reference/gateway-config.md`** — every `gateway.toml` key, default, refusal; `docs/how-to/run-a-gateway.md` — one edge per node host, the member map must be identical everywhere, systemd unit, what a client sees on failover, when to use `envelope = false`.
- [ ] **Step 3: `docs/notes/uc2-gateway-shapes-and-flow-control.md`** — the A/B/C/D comparison (from the design conversation; C chosen, D the end-state), why redirect not forward, why credits not TCP, Aeron parallels (`REDIRECT`, `NewLeaderEvent`, Status Messages).
- [ ] **Step 4: Spec amendment + README/CLAUDE.md/QUICKSTART/runbook edits** as listed.
- [ ] **Step 5: Run everything** — `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `cargo test -p uc2_service --features ultima_db`; `cargo test -p uc2_node --test lin_v2`; `cargo doc --workspace --no-deps --lib` (the `docs.yml` link-drift guard must still pass).
- [ ] **Step 6: Commit** — `git commit -am "docs(m12a): gateway how-to/reference, remote protocol spec page, explainer note, spec §3.1 amendment"`.

---

## Self-review against spec §3.1 / §4

- §3.1 two tiers → Tasks 1–4 (blanket impl instead of `Typed<S>` — amended in Task 13; `egress` no longer allocates — Task 2; `apply-profile` kept — Task 2/4). ✔
- §4.1 crates → Tasks 6–8 (`uc2_remote`, `uc2_gateway`, `uc2_service::session`). ✔
- §4.2 protocol incl. credits/`STATUS`/`RETRY{reason, hint}`/`UNKNOWN`/`LEADER_CHANGED` → Tasks 6–9. ✔
- §4.3 edge (Engine per edge, conn table, leader watch, static member map, redirect, envelope, errors, `gateway.toml`, packaging) → Tasks 8–10. ✔
- §4.4 `Sessioned` (window, LRU by position, gap-as-fresh, snapshot composition, works over typed) → Task 5. ✔
- §4.5 failover promises → Task 7 (client) + Task 11 (proof). ✔
- §4.6 tests/gate: capstone (11), `Sessioned` tests (5), credits/redirect (9), byte-identity (1), codec share + gateway cost (4, 12), packaging (10). ✔
- Type names used consistently: `RawStateMachine`, `RawOutputHandler`, `TypedOutput`, `Sessioned`/`SessionConfig`/`SESSION_HEADER_LEN`/`TAG_*`, `Header`/`FrameType`/`FLAG_*`/`HelloOk`/`ResponseMeta`/`Status`/`Leader`/`Retry`, `RemoteClient`/`RemoteConfig`/`Ticket`/`RemoteResponse`/`RemoteError`, `Edge`/`EdgeConfig`/`Member`/`EdgeStats`/`EdgeError`. ✔
