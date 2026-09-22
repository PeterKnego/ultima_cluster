# `uc2-diffreplay pin-verify` — reconstruction mode part 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the diff-replay harness a black-box mode that proves, on a real node with the app's real service binaries, that S4's pin does what §6.2 part 2 asks — the stale binary is **refused by name** after the pin, and the new binary's live state is the **artifact path's**, never the genesis counterfactual — for both the empty and the durable state-machine shapes.

**Architecture:** A new `uc2-diffreplay pin-verify` subcommand runs an in-process single-voter `uc_node` in a scratch instance dir, spawns the OLD service binary (black-box: `<bin> [args] --instance-dir D --app-id A`, the same serve form every service binary in the tree already has), builds a history by re-submitting the recorded `MESSAGE` frames of a caller-supplied corpus through the raw client engine (so the harness never learns the app's command encoding), commands an instant at P, lets OLD run on to a frontier X > P, stops it, places a real `uc2ctl upgrade pin` (admin op 10 through the cnc admin band), and then runs two arms: **refusal** (OLD re-attaches → must exit non-zero naming the pin) and **swap** (NEW attaches → must reach X, after which the harness takes a second instant Q, projects the live artifact with `NEW project`, exports a corpus `[P, Q)` from the same instance dir and compares three projections: live, `NEW replay` from the artifact, `NEW replay --from-genesis`). PASS = refusal held ∧ live == artifact path; the artifact-vs-genesis comparison is the mode's teeth (DIVERGE) or its honesty note (INCONCLUSIVE — the change had no semantic effect on this span). The durable shape is exercised by `uc_lincheck`'s new `Durable<S>` fixture, a register whose `(value, last_applied)` persists to a file in the instance dir, served by `register-replay serve --durable`.

**Tech Stack:** Rust 2024 / MSRV 1.89; `uc_node` (in-process node), `uc_client::Engine` (raw-bytes submit/poll), `uc_log::cnc` (the row's `applied` and `version` words), `uc_journal::TailReader` + `uc_protocol::v2::frame` (walking the corpus span), `libc` (SIGTERM), `clap`, `serde_json`.

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` — §6.2 (three modes; "part 2 — verify that the system refuses the wrong one … for both the empty and the durable-SM shapes"), §3 S4 (steps 4–5 and its "Errata (plan B2, as built)"), §2.3 (the counterfactual and its DEMONSTRATED evidence note), §1.4 (why the assertion is refusal, not agreement), §6.3 (black-box first), §11 item 8. Plans A (`2026-09-20-uc2-diff-replay-harness.md`, Task 10 = part 1) and B2 (`2026-09-21-uc2-pinned-install-at-attach.md`, Task 5 = the in-process real-pin proof) are the substrate; B3 (`2026-09-21-uc2-live-snapshot-reports.md`, Task 5) is why a service now waits out `NodeBooting`.

## Global Constraints

- **Black-box stays black-box.** The harness runs the app's binaries and reads UC's own pages/files; it never links the app, never decodes its commands, never asks it for anything beyond the README's CLI contract (`replay`, `project`, and — new here — the serve form `<bin> [args] --instance-dir D --app-id A`, exit 0 on a clean SIGTERM stop, non-zero with the error on stderr when attach fails). No new obligation on the app beyond that serve form, which `examples/kv`, `examples/counter` and `uc_crashtest-service` already meet.
- **The pin is real.** `pin-verify` places the pin the way an operator does: staged `<instance_dir>/upgrade.pending` + admin op `ADMIN_OP_UPGRADE_PIN` with `uc_node::staged_digest`'s `(id, ip, port)`, exactly `uc_diffreplay/tests/common/mod.rs`'s `pin_row` (which moves into the library). Refusal reasons are B1's (52–59) and are reported by number and name, never retried except the two documented races (status 2; reason 54 `pin_no_set`).
- **The refusal arm's evidence is the SDK's own text.** A refused re-attach exits non-zero and its stderr carries `ServiceError::PinnedVersionMismatch`'s Display, whose stable phrase is `is pinned to version` (`uc_service/src/config.rs` ~175). The harness matches that phrase; a test pins the phrase to the Display so a reword fails loudly.
- **Apply stays deterministic.** `Durable<S>` persists AFTER `apply` returns and restores at construction; it is a `uc_lincheck` **test fixture**, documented "not a pattern for a user state machine" like `DoublingRegisterSm`. Nothing in `uc_service` changes.
- **Same flag day, no wire/cnc change.** 2.13.0 (wire `0.9.0`, cnc `3.3`) is unshipped; this plan adds no wire kind, no cnc word, no refusal number.
- **Timeouts are bounds, not measurements.** Every wait in the harness is a bounded poll (`--timeout-secs`, default 60) with the condition named in its failure; no sleeps-and-hope; no dev-box timing is asserted as a property.
- `cargo fmt --all -- --check`; workspace clippy `-D warnings` + the feature-gated runs (`uc_crashtest --features hard-crash-tests`, **`uc_lincheck --features replay-bin`**, `uc_service --features apply-profile`, `uc_gateway --features test-util`); the fixture build `cargo build -p uc_lincheck --features replay-bin --bin register-replay` and `cargo build -p uc_diffreplay` precede the tests (CI already does both). Frozen strings and numbers get tests. **No `RELEASES.md`/`docs/releases.md`/`CLAUDE.md` edits** (plan D). No `git stash`. Scratch under `$HOME/scratch/`. Tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`. Use a private `CARGO_TARGET_DIR` for every cargo command.

### Errata against the spec text (decided while planning; Task 5 records them under §6.2)

1. **The history is the corpus's own commands, re-submitted.** §6.2 gives `reconstruction` "one build, two start states" over a corpus; part 2 needs a LIVE history with a pin in it, and a black-box harness cannot generate app commands. So the corpus is the workload: its `MESSAGE` frames are re-submitted verbatim through the raw client engine, first half before the instant, second half after. `TIMER` frames cannot be re-submitted (a node mints them) and are skipped and counted. The corpus's ARTIFACT is not used — the fresh node's history starts empty under OLD; the corpus is a source of realistic bytes, not of state.
2. **`--to` is an input.** A black-box harness cannot read the NEW binary's `VERSION` without attaching it, and attaching it before the pin would run its `apply` on the live history. `--to` is required, in the form `uc2ctl upgrade pin --to` accepts; the harness verifies it after the swap arm against the row's attached-version word and reports a mismatch as a FAIL that names both numbers.
3. **Empty vs. durable is the app's property, exercised by the harness's sequence, proven by the fixture.** Stopping OLD at X > P before the pin is what makes a durable SM attach with `last_applied() = X`; an in-memory SM attaches empty. The outcome check is the same for both (live == artifact path), and the report records X and P but cannot say which shape the app has. The two shapes are each proven on the harness's own fixture: `register-replay serve` (empty) and `register-replay serve --durable` (durable, via `uc_lincheck::register::Durable<S>`).
4. **INCONCLUSIVE is an outcome, not a failure.** If NEW's artifact-path and genesis-path projections agree over the span, the run cannot show the counterfactual (the change did not alter the semantics of any command in the span); the refusal arm and the live-equals-artifact check still hold and are still judged. Exit 0 with the note, so a CI job can require PASS-or-INCONCLUSIVE while a human reads the note.
5. **`upgrade show` is not consulted.** The record for an instant lands one instant behind the artifact `upgrade show` reads (B3's documented lag); the harness reads the pin from the row's cnc words (`PinRead::Pinned`) and the state from artifacts and projections, none of which lag.
6. **A same-version run is refused, not judged** (ruling R-C-1, found in Task 3's smoke). The refusal arm re-attaches OLD after a pin to `--to`; if the row already runs `--to`, OLD is the pinned version and the arm cannot hold. The mode refuses before placing the pin, naming both numbers.
7. **The demonstration needs a state-dependent tail** (ruling R-C-1). For a last-write-wins FSM a write-only span makes the artifact and genesis paths agree at the end (INCONCLUSIVE by design); the harness's own e2e uses a CAS chain whose outcome depends on the state at P. An app's corpus should likewise carry commands whose result depends on prior state, or the run cannot show the counterfactual — the how-to says so.

---

## File structure

| file | responsibility |
|---|---|
| `uc_lincheck/src/register.rs` | `Durable<S>` fixture: a `StateMachine` + `SnapshotStateMachine` wrapper that persists `(value, last_applied)` to `<dir>/register.state` after every apply/install and restores it at construction |
| `uc_lincheck/src/bin/register-replay.rs` | gains `serve --instance-dir D --app-id A [--double] [--durable]`: attach, supervise, SIGTERM-stop; exit 1 on attach failure with the error on stderr |
| `uc_lincheck/Cargo.toml` | `replay-bin` gains `signal-hook` (already a workspace dep) for the serve form |
| `uc_diffreplay/src/live.rs` (new, feature `pin-verify`) | the rig: single-voter node config/start, `command_instant`, `pin_row` (moved from `tests/common`), `AppProcess` (spawn / wait-attached / stop), `replay_span` (raw re-submit of a corpus's MESSAGE frames), cnc readers |
| `uc_diffreplay/src/pinverify.rs` (new, feature `pin-verify`) | the sequence itself and `PinVerifyReport` (JSON + text, verdict) — pure enough to unit-test the verdict table |
| `uc_diffreplay/src/bin/uc2-diffreplay.rs` | `Sub::PinVerify { .. }` → `pinverify::run` |
| `uc_diffreplay/src/lib.rs`, `Cargo.toml` | `pub mod live; pub mod pinverify;` behind `pin-verify = ["dep:uc_node", "dep:uc_net", "dep:uc_log", "dep:uc_client", "dep:libc"]`, `default = ["export", "pin-verify"]` |
| `uc_diffreplay/tests/common/mod.rs` | thins to re-exports of `live::{node_config, command_instant, pin_row, wait_for}` |
| `uc_diffreplay/tests/pin_verify.rs` (new) | the e2e: empty PASS+DIVERGE, durable PASS+DIVERGE, stale-NEW FAIL, same-version INCONCLUSIVE |
| `uc_diffreplay/README.md`, `docs/how-to/diff-replay.md`, `docs/how-to/upgrade-an-application.md`, `docs/VERIFICATION.md`, the spec's §6.2 errata block, `docs/BACKLOG.md` | Task 5 |

---

### Task 1: `Durable<S>` and `register-replay serve`

**Files:**
- Modify: `uc_lincheck/src/register.rs` (append after `DoublingRegisterSm`'s impls, ~line 200)
- Modify: `uc_lincheck/src/bin/register-replay.rs`
- Modify: `uc_lincheck/Cargo.toml` (`replay-bin` feature deps)
- Test: `uc_lincheck/src/register.rs` (unit, `#[cfg(all(test, feature = "v2"))]`)

**Interfaces:**
- Consumes: `RegisterSm` / `DoublingRegisterSm` (`freeze() -> (Vec<u8>, u64)`, `install_snapshot(position, src) -> Result<u64>`, both with `SnapshotHandle = Vec<u8>` = bincode of `(Option<u64>, Option<u64>)`); `uc_service::{ServiceBuilder, ServiceConfig, StateMachine, SnapshotStateMachine, ApplyCtx}`.
- Produces: `pub struct Durable<S> { inner: S, path: PathBuf }` with `pub fn open(inner: S, instance_dir: &Path) -> std::io::Result<Durable<S>>` (restores if `<dir>/register.state` exists) and `pub const STATE_FILE: &str = "register.state"`; `register-replay serve --instance-dir <D> --app-id <A> [--double] [--durable]` (exit 0 on SIGTERM stop; exit 1 on attach failure with `Error: <ServiceError Display>` on stderr — `anyhow`'s default main rendering; exit 1 with `apply agent fail-stopped` if `is_alive()` drops).

- [ ] **Step 1: Write the failing unit test**

Append to `uc_lincheck/src/register.rs`:

```rust
#[cfg(all(test, feature = "v2"))]
mod durable_tests {
    use super::*;
    use uc_service::{SnapshotStateMachine, StateMachine};

    fn ctx(pos: u64) -> uc_service::ApplyCtx {
        uc_service::ApplyCtx::new(pos, <RegisterSm as uc_service::RawStateMachine>::IDENTITY)
    }

    #[test]
    fn a_durable_register_restores_value_and_last_applied_from_its_file() {
        let dir = tempfile::Builder::new()
            .prefix("durable-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap();
        {
            let mut d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
            assert_eq!(StateMachine::last_applied(&d), None, "fresh: nothing persisted yet");
            let _ = StateMachine::apply(&mut d, &mut ctx(64), Cmd::Write(7));
            let _ = StateMachine::apply(&mut d, &mut ctx(128), Cmd::Write(9));
        }
        let d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
        assert_eq!(StateMachine::query(&d, ()), Some(9));
        assert_eq!(StateMachine::last_applied(&d), Some(128));
        assert!(dir.path().join(Durable::<RegisterSm>::STATE_FILE).is_file());
    }

    #[test]
    fn a_durable_register_persists_an_install_too() {
        let dir = tempfile::Builder::new()
            .prefix("durable-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap();
        let (image, _) = {
            let mut src = RegisterSm::default();
            let _ = StateMachine::apply(&mut src, &mut ctx(32), Cmd::Write(5));
            SnapshotStateMachine::freeze(&src).unwrap()
        };
        {
            let mut d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
            let got = SnapshotStateMachine::install_snapshot(&mut d, 64, &mut &image[..]).unwrap();
            assert_eq!(got, 64);
        }
        let d = Durable::open(RegisterSm::default(), dir.path()).unwrap();
        assert_eq!(StateMachine::query(&d, ()), Some(5));
        assert_eq!(StateMachine::last_applied(&d), Some(32), "the image's recorded cursor, not the tag");
    }
}
```

(`tempfile` is already a dev-dependency of `uc_lincheck`; if it is not, add `tempfile = { workspace = true }` under `[dev-dependencies]`.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_lincheck --lib durable_tests`
Expected: compile error — `Durable` not found.

- [ ] **Step 3: Implement `Durable<S>`**

Append to `uc_lincheck/src/register.rs` (before the test module):

```rust
// ------------------------------------------------------------ durable fixture

/// A diff-replay / pin-verify **test fixture** — the "durable state machine"
/// shape spec §2.3 says decided Q2: one whose `last_applied()` is non-`None`
/// on attach because it persisted. Wraps [`RegisterSm`] or
/// [`DoublingRegisterSm`] and writes their snapshot image (`(value,
/// last_applied)`) to `<instance_dir>/register.state` after every `apply`
/// and every `install_snapshot`, restoring it at [`Durable::open`]. The
/// write is `rename`-atomic so a killed process leaves the previous image,
/// never a torn one. **Not a pattern for a user state machine**: it does
/// file I/O inside `apply`, which is only acceptable because the I/O IS the
/// durability under test and it is deterministic (same inputs, same file).
#[cfg(feature = "v2")]
pub struct Durable<S> {
    inner: S,
    path: std::path::PathBuf,
}

#[cfg(feature = "v2")]
impl<S> Durable<S>
where
    S: uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    pub const STATE_FILE: &'static str = "register.state";

    /// Wrap `inner`, restoring the persisted image if `<instance_dir>/register.state`
    /// exists. The restore goes through `inner.install_snapshot(la, image)` with
    /// `la` = the image's own recorded cursor, which the register's install
    /// accepts (payload position ≤ tag).
    pub fn open(mut inner: S, instance_dir: &std::path::Path) -> std::io::Result<Durable<S>> {
        let path = instance_dir.join(Self::STATE_FILE);
        if path.is_file() {
            let image = std::fs::read(&path)?;
            let ((_, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
                &image,
                bincode::config::standard(),
            )
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            inner
                .install_snapshot(la.unwrap_or(0), &mut &image[..])
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(Durable { inner, path })
    }

    fn persist(&self) {
        // Failure here is a fixture defect, not a state-machine outcome:
        // panic, so the apply thread fail-stops and the test names it.
        let (image, _) = self.inner.freeze().expect("durable fixture: freeze");
        let tmp = self.path.with_extension("state.tmp");
        std::fs::write(&tmp, &image).expect("durable fixture: write");
        std::fs::rename(&tmp, &self.path).expect("durable fixture: rename");
    }
}

#[cfg(feature = "v2")]
impl<S> uc_service::StateMachine for Durable<S>
where
    S: uc_service::StateMachine + uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;
    type Command = S::Command;
    type Response = S::Response;
    type Query = S::Query;
    type QueryResponse = S::QueryResponse;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: S::Command) -> S::Response {
        let r = self.inner.apply(ctx, cmd);
        self.persist();
        r
    }
    fn query(&self, q: S::Query) -> S::QueryResponse {
        self.inner.query(q)
    }
    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied()
    }
}

#[cfg(feature = "v2")]
impl<S> uc_service::SnapshotStateMachine for Durable<S>
where
    S: uc_service::StateMachine + uc_service::SnapshotStateMachine<SnapshotHandle = Vec<u8>>,
{
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        self.inner.freeze()
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        S::stream_snapshot(handle, dst)
    }
    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, uc_service::SnapshotError> {
        let got = self.inner.install_snapshot(position, src)?;
        self.persist();
        Ok(got)
    }
    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
        self.inner.project(out)
    }
}
```

If `StateMachine` has further provided methods that `RegisterSm` overrides (check the trait in `uc_service/src/traits.rs`: `on_timer`, `on_committed`-adjacent hooks), forward them the same way — the wrapper must be behaviourally transparent. `DoublingRegisterSm`'s `SnapshotHandle` must be `Vec<u8>` for the bound to hold; if it is a different type, add `type SnapshotHandle = Vec<u8>` conformance there (read its impl at ~line 173 first).

- [ ] **Step 4: Run the unit tests**

Run: `cargo test -p uc_lincheck --lib durable_tests`
Expected: 2 passed.

- [ ] **Step 5: Add `serve` to `register-replay`**

`uc_lincheck/Cargo.toml`: add `signal-hook = { workspace = true, optional = true }` under `[dependencies]` and `"dep:signal-hook"` to the `replay-bin` feature list (workspace already pins `signal-hook = "0.3"`).

`uc_lincheck/src/bin/register-replay.rs` — extend `Sub`:

```rust
    /// The serve form of the diff-replay CLI contract: attach to a running
    /// node and apply until SIGTERM. `pin-verify` runs this for both eras.
    Serve {
        #[arg(long)]
        instance_dir: PathBuf,
        #[arg(long, default_value = "register")]
        app_id: String,
        /// The "v2" build: `Write(v)` stores `2·v` (`DoublingRegisterSm`).
        #[arg(long)]
        double: bool,
        /// Persist `(value, last_applied)` in the instance dir — the durable
        /// state-machine shape (spec §2.3's third path).
        #[arg(long)]
        durable: bool,
    },
```

and the arm (model: `examples/counter/src/bin/counter-service.rs`'s supervise loop):

```rust
        Sub::Serve {
            instance_dir,
            app_id,
            double,
            durable,
        } => {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
                signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
            }
            let cfg = || uc_service::ServiceConfig::new(instance_dir.clone(), app_id.clone());
            // Four combinations, one `Service` type each; `supervise` is
            // generic over the state machine so the loop is written once.
            match (double, durable) {
                (false, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), RegisterSm::default()).start_with_snapshots()?,
                    &stop,
                ),
                (true, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), DoublingRegisterSm::default()).start_with_snapshots()?,
                    &stop,
                ),
                (false, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(RegisterSm::default(), &instance_dir)?,
                    )
                    .start_with_snapshots()?,
                    &stop,
                ),
                (true, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(DoublingRegisterSm::default(), &instance_dir)?,
                    )
                    .start_with_snapshots()?,
                    &stop,
                ),
            }
        }
```

with

```rust
/// The template every service binary follows (`docs/how-to/write-a-service-binary.md`):
/// poll `is_alive`, exit 1 if the apply agent fail-stopped, stop cleanly on
/// the signal flag. `attach` errors propagate through `main`'s `?`, so a
/// refused attach exits 1 with `Error: <ServiceError>` on stderr — which is
/// what `pin-verify`'s refusal arm reads.
fn supervise<S: uc_service::RawStateMachine>(
    service: uc_service::Service<S>,
    stop: &std::sync::atomic::AtomicBool,
) -> anyhow::Result<()> {
    eprintln!(
        "register-replay: attached row={} pinned={:?}",
        service.service_id(),
        service.pinned()
    );
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        if !service.is_alive() {
            anyhow::bail!("apply agent fail-stopped");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    service.stop();
    Ok(())
}
```

`Service<S>`'s bound: use whatever `ServiceBuilder::start_with_snapshots` returns (`Service<S>` with `S: SnapshotStateMachine` in scope); `service_id()` and `pinned()` are on `Service` (`uc_service/src/lib.rs` ~487). The `pinned=` line is the fixture's own record of which path ran — the harness does NOT parse it (black-box), tests may.

- [ ] **Step 6: Build, lint, smoke the serve form by hand against a node**

Run: `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo clippy -p uc_lincheck --features replay-bin --all-targets -- -D warnings`
Expected: clean.

Smoke (no node running): `target/debug/register-replay serve --instance-dir /nonexistent --app-id x; echo "exit=$?"`
Expected: a `ServiceError` on stderr (the cnc page cannot be opened) and `exit=1` — proves the refusal path renders through `main`.

- [ ] **Step 7: Commit**

```bash
git add uc_lincheck/src/register.rs uc_lincheck/src/bin/register-replay.rs uc_lincheck/Cargo.toml
git commit -m "uc_lincheck: Durable<S> register fixture and register-replay serve (plan C T1)"
```

---

### Task 2: `uc_diffreplay::live` — the rig, the app process, the span replay

**Files:**
- Create: `uc_diffreplay/src/live.rs`
- Modify: `uc_diffreplay/src/lib.rs` (`#[cfg(feature = "pin-verify")] pub mod live;`), `uc_diffreplay/Cargo.toml` (feature + optional deps), `uc_diffreplay/tests/common/mod.rs` (thin to re-exports)
- Test: `uc_diffreplay/tests/live.rs` (new)

**Interfaces:**
- Consumes: `uc_node::{Node, NodeConfig, PurgePolicy, CryptoConfig, ServicesConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, SnapshotRefusal, staged_digest, UPGRADE_PENDING_FILE, REASON_PIN_NO_SET}`, `uc_net::fault::FaultConfig`, `uc_log::cnc::{CncPage, AdminReq, AdminResp}`, `uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN`, `uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin}`, `uc_client::{Engine, EngineConfig}` (`Engine::attach(dir, app_id, cfg) -> (SendHalf, PollHalf)`, `SendHalf::try_submit(user_data, bytes) -> Result<(), SubmitError>`, `PollHalf::poll(|Completion { user_data, position, outcome }|) -> usize`, `SubmitError::{Backpressure, NotServing, ..}`, `Outcome::{Response, Responses, NotLeader{..}, Retry, BadService{..}, TimedOut, InstanceRestart{..}}`), `uc_journal::TailReader::{open, scan_from}`, `uc_protocol::v2::frame::{read_header, align_frame_len, HEADER_LEN, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER}`, `Corpus::{open, journal_dir, manifest}`.
- Produces (all `pub`, `#[cfg(feature = "pin-verify")]`):
  - `pub fn node_config(dir: &Path, app_id: &str, fsm: &str) -> NodeConfig` (verbatim from `tests/common`), `pub fn start_node(dir: &Path, app_id: &str, fsm: &str, timeout: Duration) -> anyhow::Result<Node>` (start + wait `can_serve`).
  - `pub fn wait_for(f: impl FnMut() -> bool, timeout: Duration) -> bool`.
  - `pub fn command_instant(node: &Node, timeout: Duration) -> anyhow::Result<u64>`; `pub fn artifact_path(dir: &Path, row: u8, p: u64) -> PathBuf` (= `dir/snapshots/<row>/snap-<p>.ultsnap`).
  - `pub fn pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64, timeout: Duration) -> anyhow::Result<AdminResp>` — returns `Ok(resp)` with `resp.status == 0`, or `Err` naming `status`/`reason` (the two races are retried until `timeout`, everything else is immediate).
  - `pub struct AppProcess { child: Child, stderr_path: PathBuf }`, `pub fn spawn_app(bin: &Path, args: &[String], dir: &Path, app_id: &str, stderr_path: &Path) -> anyhow::Result<AppProcess>` (argv = `bin args... --instance-dir dir --app-id app_id`; stderr redirected to the file), `pub enum AttachOutcome { Attached, Exited { code: Option<i32>, stderr: String } }`, `pub fn incarnation(cnc: &CncPage, row: u8) -> u32` (the slot status word's incarnation field, via `uc_log::cnc::unpack_service_status(cnc.service_slot(row).status.load_acquire())`), `impl AppProcess { pub fn wait_attached(&mut self, cnc: &CncPage, row: u8, before: u32, timeout: Duration) -> AttachOutcome` (Attached once the status word's ATTACHED bit is set AND its incarnation differs from `before` — the incarnation captured by the caller right before `spawn_app`; the version word is NOT usable here because `RegisterSm::VERSION` is the trait default `0`, and the bit alone is not enough because a stopped process may leave it set), `pub fn wait_applied(&mut self, cnc, row, at_least: u64, timeout) -> AttachOutcome` ("caught up" = `service_slot(row).applied.load_acquire() >= at_least`); `pub fn stop(mut self, timeout: Duration) -> anyhow::Result<std::process::ExitStatus>` (SIGTERM via `libc::kill(pid, libc::SIGTERM)`, wait up to `timeout`, then `kill()` and report), `pub fn stderr(&self) -> String` }`.
  - `pub struct SpanReplay { pub submitted: u64, pub skipped_timers: u64, pub last_position: u64 }`, `pub fn replay_span(corpus: &Corpus, dir: &Path, app_id: &str, row: u8, range: std::ops::Range<usize>, timeout: Duration) -> anyhow::Result<SpanReplay>` — re-submits the corpus's MESSAGE frames with index in `range` (0-based over MESSAGE frames only), one in flight, to `row`; `pub fn message_frames(corpus: &Corpus) -> anyhow::Result<Vec<Vec<u8>>>` (the payload bytes of every MESSAGE frame in `[origin, end)`, in order — what `replay_span` indexes).

- [ ] **Step 1: Feature and deps**

`uc_diffreplay/Cargo.toml`:

```toml
[features]
default = ["export", "pin-verify"]
export = ["dep:uc_node"]
# `pin-verify` runs a real node, a real client engine and reads the cnc
# page: everything an app that only embeds the replay driver must NOT link
# (`examples/kv` takes `default-features = false`).
pin-verify = ["dep:uc_node", "dep:uc_net", "dep:uc_log", "dep:uc_client", "dep:libc"]

[dependencies]
# … existing …
uc_net = { path = "../uc_net", version = "2.12.0", optional = true }
uc_log = { path = "../uc_log", version = "2.12.0", optional = true }
uc_client = { path = "../uc_client", version = "2.12.0", optional = true }
libc = { workspace = true, optional = true }
```

Keep the `[dev-dependencies]` block as is (tests link the lib with default features, so `uc_node`/`uc_net`/`uc_log`/`uc_client` are present either way; the dev-dep lines may stay for the reason their comment gives).

- [ ] **Step 2: Write the failing test**

`uc_diffreplay/tests/live.rs`:

```rust
//! The `live` rig on its own: a node, the register fixture's serve form, an
//! attach observed through the cnc page, a clean SIGTERM stop, and a refused
//! attach observed as a non-zero exit with the error on stderr.
mod common;
use std::time::Duration;

use uc_diffreplay::live::{AttachOutcome, artifact_path, command_instant, spawn_app, start_node};
use uc_log::cnc::CncPage;

const T: Duration = Duration::from_secs(60);

#[test]
fn the_serve_form_attaches_and_stops_cleanly() {
    let inst = common::tempdir();
    let dir = inst.path();
    let node = start_node(dir, "live1", common::register_name(), T).unwrap();
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), "live1").unwrap();
    let before = uc_diffreplay::live::incarnation(&cnc, 0);
    let mut app = spawn_app(
        &common::register_replay_bin(),
        &["serve".to_string()],
        dir,
        "live1",
        &dir.join("old.stderr"),
    )
    .unwrap();
    assert!(matches!(app.wait_attached(&cnc, 0, before, T), AttachOutcome::Attached), "{}", app.stderr());
    // An instant completes for the row: the artifact appears.
    let p = command_instant(&node, T).unwrap();
    assert!(
        uc_diffreplay::live::wait_for(|| artifact_path(dir, 0, p).is_file(), T),
        "row 0 never published snap-{p}"
    );
    let st = app.stop(T).unwrap();
    assert!(st.success(), "clean stop must exit 0: {st}");
    node.stop();
}

#[test]
fn a_refused_attach_is_a_nonzero_exit_with_the_error_on_stderr() {
    let inst = common::tempdir();
    let dir = inst.path();
    let node = start_node(dir, "live2", common::register_name(), T).unwrap();
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), "live2").unwrap();
    let before = uc_diffreplay::live::incarnation(&cnc, 0);
    // Wrong app id: the page refuses the attach by name.
    let mut app = spawn_app(
        &common::register_replay_bin(),
        &["serve".to_string()],
        dir,
        "not-live2",
        &dir.join("bad.stderr"),
    )
    .unwrap();
    match app.wait_attached(&cnc, 0, before, T) {
        AttachOutcome::Exited { code, stderr } => {
            assert_ne!(code, Some(0));
            assert!(stderr.contains("Error:"), "stderr: {stderr}");
        }
        AttachOutcome::Attached => panic!("a wrong app id must not attach"),
    }
    node.stop();
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo test -p uc_diffreplay --test live`
Expected: compile error — `uc_diffreplay::live` not found.

- [ ] **Step 4: Implement `live.rs`**

`uc_diffreplay/src/live.rs`:

```rust
//! The live rig behind `uc2-diffreplay pin-verify` (spec §6.2 part 2): an
//! in-process single-voter node, the app's service binary as a black-box
//! child process, a real `uc2ctl upgrade pin`, and a corpus's recorded
//! commands re-submitted through the raw client engine. Everything here
//! reads UC's own surfaces (the cnc page, the snapshot dir, the journal) and
//! never the app's.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use uc_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc_journal::TailReader;
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, HEADER_LEN, align_frame_len};
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};

use crate::corpus::Corpus;

/// Verbatim `uc_diffreplay/tests/common/mod.rs`'s single-voter config: a
/// 1 MiB ring, 256 B payload cap, purge OFF (the shipped default — the
/// counterfactual path §2.3 warns about is the one this rig must leave
/// open so the harness can show the pin closing it).
pub fn node_config(dir: &Path, app_id: &str, fsm: &str) -> NodeConfig {
    let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: uc_net::fault::FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(fsm),
    }
}

pub fn wait_for(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// Start the node and wait until it serves (a single voter elects itself;
/// the first submit races that without the wait).
pub fn start_node(dir: &Path, app_id: &str, fsm: &str, timeout: Duration) -> anyhow::Result<Node> {
    let node = Node::start(node_config(dir, app_id, fsm)).map_err(|e| anyhow::anyhow!("node start: {e}"))?;
    if !wait_for(|| node.can_serve(), timeout) {
        node.stop();
        bail!("the node never became a serving leader within {timeout:?}");
    }
    Ok(node)
}

/// `uc2ctl snapshot` in process: command an instant, return its position P.
pub fn command_instant(node: &Node, timeout: Duration) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return Ok(p),
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => bail!("uc2ctl snapshot refused: {e}"),
        }
    }
}

pub fn artifact_path(dir: &Path, row: u8, p: u64) -> PathBuf {
    dir.join("snapshots").join(row.to_string()).join(format!("snap-{p}.ultsnap"))
}

/// The slot status word's incarnation field — what `AppProcess::wait_attached`
/// compares against, so a re-attach is told apart from a stale bit.
pub fn incarnation(cnc: &CncPage, row: u8) -> u32 {
    uc_log::cnc::unpack_service_status(cnc.service_slot(row as usize).status.load_acquire()).2
}

/// `<instance_dir>/upgrade.pending`, written the way `uc2ctl` writes it:
/// 0600, fsync'd, renamed into place.
fn stage_upgrade_pin(dir: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let pending = dir.join(uc_node::UPGRADE_PENDING_FILE);
    let tmp = dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &pending)?;
    Ok(())
}

/// The `uc2ctl` mutating-command flow, minus the bin (`uc_node/tests/reconfig.rs`'s
/// `admin_request`): write a fresh request at `seq + 1`, poll for the echo.
fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16, timeout: Duration) -> anyhow::Result<AdminResp> {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(seq);
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return Ok(resp);
        }
        if Instant::now() >= deadline {
            bail!("admin response timed out for seq {seq}");
        }
        std::thread::yield_now();
    }
}

/// `uc2ctl upgrade pin --row <row> --from <from> --to <to> --origin <origin>`
/// in process (spec §2.5, plan B1). Exactly two answers are RACES against
/// this rig and are retried until `timeout`: status 2 (single-in-flight) and
/// reason 54 `pin_no_set` (the newest complete set is published a moment
/// after the row's artifact appears). Every other refusal returns `Err`
/// immediately, naming status and reason.
pub fn pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64, timeout: Duration) -> anyhow::Result<AdminResp> {
    let mut bytes = Vec::new();
    encode_upgrade_pin(&UpgradePin { row, from, to, origin }, &mut bytes);
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let deadline = Instant::now() + timeout;
    loop {
        stage_upgrade_pin(dir, &bytes)?;
        let resp = admin_request(cnc, ADMIN_OP_UPGRADE_PIN, id, ip, port, timeout)?;
        if resp.status == 0 {
            return Ok(resp);
        }
        let racy = resp.status == 2 || resp.reason == uc_node::REASON_PIN_NO_SET;
        if !racy || Instant::now() >= deadline {
            bail!("uc2ctl upgrade pin refused: status={} reason={}", resp.status, resp.reason);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The app's service binary as a black-box child: `<bin> <args…> --instance-dir
/// <dir> --app-id <app_id>`, stderr captured to a file (evidence on failure).
pub struct AppProcess {
    child: Child,
    stderr_path: PathBuf,
}

#[derive(Debug)]
pub enum AttachOutcome {
    Attached,
    Exited { code: Option<i32>, stderr: String },
}

pub fn spawn_app(bin: &Path, args: &[String], dir: &Path, app_id: &str, stderr_path: &Path) -> anyhow::Result<AppProcess> {
    let stderr = std::fs::File::create(stderr_path)?;
    let child = Command::new(bin)
        .args(args)
        .arg("--instance-dir")
        .arg(dir)
        .arg("--app-id")
        .arg(app_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .with_context(|| format!("spawn {}", bin.display()))?;
    Ok(AppProcess { child, stderr_path: stderr_path.to_path_buf() })
}

impl AppProcess {
    pub fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    /// Poll `cond` against the page until it holds (→ `Attached`) or the
    /// child exits first (→ `Exited`), bounded by `timeout` (→ `Exited` with
    /// the child killed and `code: None`).
    fn wait_cond(&mut self, mut cond: impl FnMut() -> bool, timeout: Duration) -> AttachOutcome {
        let deadline = Instant::now() + timeout;
        loop {
            if cond() {
                return AttachOutcome::Attached;
            }
            if let Ok(Some(st)) = self.child.try_wait() {
                return AttachOutcome::Exited { code: st.code(), stderr: self.stderr() };
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return AttachOutcome::Exited { code: None, stderr: self.stderr() };
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// "Attached" = the row's status word has its ATTACHED bit set under an
    /// incarnation that differs from `before` (captured by the caller just
    /// before `spawn_app`). Not the version word — `RegisterSm::VERSION` is
    /// the trait default `0` — and not the bit alone, which a stopped process
    /// may leave set.
    pub fn wait_attached(&mut self, cnc: &CncPage, row: u8, before: u32, timeout: Duration) -> AttachOutcome {
        self.wait_cond(
            || {
                let (_, attached, inc) = uc_log::cnc::unpack_service_status(
                    cnc.service_slot(row as usize).status.load_acquire(),
                );
                attached && inc != before
            },
            timeout,
        )
    }

    /// "Caught up" = the row's published `applied` frontier ≥ `at_least`.
    pub fn wait_applied(&mut self, cnc: &CncPage, row: u8, at_least: u64, timeout: Duration) -> AttachOutcome {
        self.wait_cond(|| cnc.service_slot(row as usize).applied.load_acquire() >= at_least, timeout)
    }

    /// SIGTERM, wait up to `timeout`, else SIGKILL — and say which.
    pub fn stop(mut self, timeout: Duration) -> anyhow::Result<std::process::ExitStatus> {
        // SAFETY: a pid we spawned and still own; `kill` with SIGTERM has no
        // memory-safety preconditions.
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(st) = self.child.try_wait()? {
                return Ok(st);
            }
            if Instant::now() >= deadline {
                self.child.kill()?;
                let st = self.child.wait()?;
                bail!("the service did not stop within {timeout:?} after SIGTERM; killed ({st})");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// The payload bytes of every MESSAGE frame in the corpus's `[origin, end)`,
/// in log order — the app's own commands, opaque to us. TIMER frames are
/// counted, not returned (a node mints them; they cannot be re-submitted).
pub fn message_frames(corpus: &Corpus) -> anyhow::Result<(Vec<Vec<u8>>, u64)> {
    let m = &corpus.manifest;
    let reader = TailReader::open(&corpus.journal_dir())?;
    let mut frames = Vec::new();
    let mut timers = 0u64;
    let end = m.end;
    reader.scan_from(m.origin, |_seq, base, block| {
        let mut off = 0usize;
        while off + HEADER_LEN <= block.len() {
            let hdr = frame::read_header(&block[off..]);
            let total = hdr.length as usize;
            let aligned = align_frame_len(total);
            if total < HEADER_LEN || off + aligned > block.len() {
                break;
            }
            let pos = base + off as u64;
            if pos.saturating_add(aligned as u64) > end {
                return false;
            }
            if pos >= m.origin {
                match hdr.frame_type {
                    FRAME_TYPE_MESSAGE => frames.push(block[off + HEADER_LEN..off + total].to_vec()),
                    FRAME_TYPE_TIMER => timers += 1,
                    _ => {}
                }
            }
            off += aligned;
        }
        true
    })?;
    Ok((frames, timers))
}

pub struct SpanReplay {
    pub submitted: u64,
    /// The log position the LAST submitted command's response named — the
    /// frontier the app has applied to once the response arrived.
    pub last_position: u64,
}

/// Re-submit `frames[range]` to `row`, one in flight, through the raw engine.
/// Backpressure and `Retry`/`NotLeader` are waited out (bounded by
/// `timeout` per command); any other outcome is an error naming the index.
pub fn replay_span(frames: &[Vec<u8>], dir: &Path, app_id: &str, row: u8, range: std::ops::Range<usize>, timeout: Duration) -> anyhow::Result<SpanReplay> {
    let (send, mut poll) = Engine::attach(dir, app_id, EngineConfig::default())
        .map_err(|e| anyhow::anyhow!("engine attach: {e}"))?;
    let mut last_position = 0u64;
    let mut submitted = 0u64;
    for i in range {
        let bytes = &frames[i];
        let deadline = Instant::now() + timeout;
        // Submit (waiting out backpressure / not-serving), then wait for this
        // one command's completion.
        loop {
            match send.try_submit_to(i as u64, row, bytes) {
                Ok(()) => break,
                Err(SubmitError::Backpressure) | Err(SubmitError::NotServing) => {
                    poll.poll(|_| {});
                    if Instant::now() >= deadline {
                        bail!("command #{i}: could not submit within {timeout:?}");
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => bail!("command #{i}: {e}"),
            }
        }
        let mut done: Option<anyhow::Result<u64>> = None;
        while done.is_none() {
            poll.poll(|c| {
                if c.user_data != i as u64 {
                    return;
                }
                done = Some(match c.outcome {
                    Outcome::Response(_) | Outcome::Responses(_) => Ok(c.position.unwrap_or(0)),
                    Outcome::Retry | Outcome::NotLeader { .. } => Err(anyhow::anyhow!("retry")),
                    other => Err(anyhow::anyhow!("command #{i}: {other:?}")),
                });
            });
            if done.is_none() && Instant::now() >= deadline {
                bail!("command #{i}: no completion within {timeout:?}");
            }
            if done.is_none() {
                std::thread::yield_now();
            }
        }
        match done.unwrap() {
            Ok(p) => {
                last_position = p;
                submitted += 1;
            }
            Err(e) if e.to_string() == "retry" => {
                // Re-submit the same command: leadership settled or the
                // node asked for a retry. Bounded by the same deadline.
                if Instant::now() >= deadline {
                    bail!("command #{i}: retried past {timeout:?}");
                }
                std::thread::sleep(Duration::from_millis(5));
                // (loop body re-entered by re-running this index)
                return replay_span(frames, dir, app_id, row, i..range_end_placeholder(), timeout)
                    .map(|r| SpanReplay { submitted: submitted + r.submitted, last_position: r.last_position });
            }
            Err(e) => return Err(e),
        }
    }
    Ok(SpanReplay { submitted, last_position })
}
```

**The `Retry` branch above is a sketch the implementer must finish properly** — write the per-index loop as `let mut i = range.start; while i < range.end { … on Retry: continue without incrementing i … i += 1; }` so a retry re-submits the same index in place (no recursion, no placeholder function). `try_submit_to(user_data, row, bytes)`'s exact parameter order is at `uc_client/src/engine.rs:715` — read it; if `row` is a `u8` "service id" there, pass `row`. `c.position` is `Option<u64>`: the position the response names — if the raw tier's `Response` completion carries `None` for a plain response, fall back to reading the row's `applied` word after the loop for `last_position` and say so in a comment.

- [ ] **Step 5: Wire the module and thin `tests/common`**

`uc_diffreplay/src/lib.rs`: add `#[cfg(feature = "pin-verify")] pub mod live;` (and, from Task 3, `pub mod pinverify;`).

`uc_diffreplay/tests/common/mod.rs`: delete its own `node_config`, `start_single_node`, `wait_for`, `command_instant`, `pin_row`, `stage_upgrade_pin`, `admin_request` bodies and replace with:

```rust
pub use uc_diffreplay::live::{artifact_path, node_config, wait_for};

pub fn start_single_node(dir: &Path, app_id: &str, fsm: &str) -> Node {
    uc_diffreplay::live::start_node(dir, app_id, fsm, Duration::from_secs(60)).unwrap()
}
pub fn command_instant(node: &Node) -> u64 {
    uc_diffreplay::live::command_instant(node, Duration::from_secs(10)).unwrap()
}
pub fn pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64) -> AdminResp {
    uc_diffreplay::live::pin_row(dir, cnc, row, from, to, origin, Duration::from_secs(30)).unwrap()
}
```

keeping `tempdir`, `wait_until`, `register_replay_bin`, `register_name`, `build_register_history` as they are. Drop the now-unused imports. Every existing test in `uc_diffreplay/tests/` must still pass unchanged.

- [ ] **Step 6: Run the tests, lint**

Run: `cargo test -p uc_diffreplay --test live && cargo test -p uc_diffreplay && cargo clippy -p uc_diffreplay --all-targets -- -D warnings && cargo clippy -p uc_diffreplay --no-default-features --all-targets -- -D warnings`
Expected: `live` 2 passed; every other `uc_diffreplay` test still green (reconstruction, upgrade_e2e, corpus, drive, diff_attribute_confirm); clippy clean with and without the feature (the `examples/kv` shape, `default-features = false`, must still build: `cargo build -p kv_store`).

- [ ] **Step 7: Commit**

```bash
git add uc_diffreplay/src/live.rs uc_diffreplay/src/lib.rs uc_diffreplay/Cargo.toml uc_diffreplay/tests/common/mod.rs uc_diffreplay/tests/live.rs Cargo.lock
git commit -m "uc_diffreplay: the live rig — node, app process, real pin, raw span replay (plan C T2)"
```

---

### Task 3: `pin-verify` — the sequence, the report, the verdict

**Files:**
- Create: `uc_diffreplay/src/pinverify.rs`
- Modify: `uc_diffreplay/src/bin/uc2-diffreplay.rs` (`Sub::PinVerify`), `uc_diffreplay/src/lib.rs`
- Test: `uc_diffreplay/src/pinverify.rs` (unit: the verdict table and the JSON round-trip)

**Interfaces:**
- Consumes: everything Task 2 produces; `Corpus::{open, export}`; `crate::trace::Trace::read_json`; the app's `replay`/`project` CLI forms (the same `replay()` helper the binary already has — move it from the bin into `pinverify.rs` as `pub fn run_replay(bin, args, corpus, out, from_genesis) -> anyhow::Result<Trace>` and have the bin call it, so `upgrade`/`determinism`/`reconstruction` keep working; `pub fn run_project(bin, args, artifact, position) -> anyhow::Result<String>`).
- Produces:

```rust
pub struct PinVerifyArgs {
    pub corpus: PathBuf,
    pub old: PathBuf, pub old_args: Vec<String>,
    pub new: PathBuf, pub new_args: Vec<String>,
    pub app_id: String,
    pub fsm: String,          // the row's FSM NAME (node.toml `[services] names`)
    pub row: u8,              // default 0
    pub to: u32,              // packed version the pin names
    pub split: Option<usize>, // MESSAGE frames before the instant; default = half
    pub timeout: Duration,    // default 60 s
    pub scratch: Option<PathBuf>, // instance dir root; default = tempdir beside the report
    pub report: PathBuf,
}
pub fn run(a: &PinVerifyArgs) -> anyhow::Result<PinVerifyReport>;

#[derive(Serialize, Deserialize)]
pub struct PinVerifyReport {
    pub mode: String,                 // "pin-verify"
    pub corpus: PathBuf,
    pub frames: u64, pub skipped_timers: u64,
    pub origin: u64,                  // P
    pub frontier: u64,                // X (OLD's last applied before the stop)
    pub end: u64,                     // Q
    pub from: u32, pub to: u32,
    pub pin: PinArm,                  // { status, reason }
    pub refusal: RefusalArm,          // { exited: bool, code, matched: bool, stderr_excerpt }
    pub swap: SwapArm,                // { attached: bool, version_seen: u32, caught_up: bool, live_eq_artifact: Option<bool>, artifact_eq_genesis: Option<bool>, live, artifact, genesis: Option<String> }
    pub verdict: Verdict,             // Pass | Inconclusive | Fail
    pub notes: Vec<String>,
}
pub enum Verdict { Pass, Inconclusive, Fail }
pub fn verdict(refusal_held: bool, live_eq_artifact: Option<bool>, artifact_eq_genesis: Option<bool>) -> Verdict;
impl PinVerifyReport { pub fn write_json(&self, w) ; pub fn write_text(&self, w) ; pub fn failed(&self) -> bool }
```

- [ ] **Step 1: Write the failing unit tests**

In `uc_diffreplay/src/pinverify.rs` (bottom):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_verdict_table() {
        // refusal held, live == artifact, artifact != genesis: the full demonstration
        assert_eq!(verdict(true, Some(true), Some(false)), Verdict::Pass);
        // … but the change had no effect on this span: honest, not a pass
        assert_eq!(verdict(true, Some(true), Some(true)), Verdict::Inconclusive);
        // the live state is the counterfactual: the system took the wrong path
        assert_eq!(verdict(true, Some(false), Some(false)), Verdict::Fail);
        // the stale binary attached: the door is open
        assert_eq!(verdict(false, Some(true), Some(false)), Verdict::Fail);
        // the swap arm never produced a comparison (attach failed, timeout)
        assert_eq!(verdict(true, None, None), Verdict::Fail);
        // live == artifact but genesis unknown (a replay failed): still not a pass
        assert_eq!(verdict(true, Some(true), None), Verdict::Fail);
    }

    #[test]
    fn the_refusal_marker_is_the_sdks_own_text() {
        let e = uc_service::ServiceError::PinnedVersionMismatch {
            name: "register".into(), row: 0, origin: 4096, pinned: 2, mine: 1,
        };
        assert!(e.to_string().contains(REFUSAL_MARKER), "{e}");
    }

    #[test]
    fn the_report_round_trips_json() {
        let r = PinVerifyReport::empty(PathBuf::from("c"), 1, 2);
        let mut buf = Vec::new();
        r.write_json(&mut buf).unwrap();
        let back: PinVerifyReport = serde_json::from_slice(&buf).unwrap();
        assert_eq!(back.mode, "pin-verify");
        assert_eq!(back.verdict, Verdict::Fail, "an empty report is a failure until the arms fill it");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_diffreplay --lib pinverify`
Expected: compile error — module missing.

- [ ] **Step 3: Implement `pinverify.rs`**

```rust
//! `uc2-diffreplay pin-verify` — spec §6.2 part 2. See the plan's
//! Architecture paragraph for the sequence; each phase below names the spec
//! step it stands for.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use uc_log::cnc::{CncPage, PinRead};

use crate::corpus::Corpus;
use crate::live::{self, AttachOutcome};
use crate::trace::Trace;

/// The stable phrase of `ServiceError::PinnedVersionMismatch`'s Display
/// (`uc_service/src/config.rs`): what the refusal arm looks for on the
/// stale binary's stderr. Pinned by `the_refusal_marker_is_the_sdks_own_text`.
pub const REFUSAL_MARKER: &str = "is pinned to version";

pub struct PinVerifyArgs { /* as in Interfaces */ }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict { Pass, Inconclusive, Fail }

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PinArm { pub status: u32, pub reason: u32 }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RefusalArm { pub exited: bool, pub code: Option<i32>, pub matched: bool, pub stderr_excerpt: String }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SwapArm {
    pub attached: bool, pub version_seen: u32, pub caught_up: bool,
    pub live_eq_artifact: Option<bool>, pub artifact_eq_genesis: Option<bool>,
    pub live: Option<String>, pub artifact: Option<String>, pub genesis: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PinVerifyReport { /* as in Interfaces */ }

pub fn verdict(refusal_held: bool, live_eq_artifact: Option<bool>, artifact_eq_genesis: Option<bool>) -> Verdict {
    match (refusal_held, live_eq_artifact, artifact_eq_genesis) {
        (true, Some(true), Some(false)) => Verdict::Pass,
        (true, Some(true), Some(true)) => Verdict::Inconclusive,
        _ => Verdict::Fail,
    }
}

impl PinVerifyReport {
    pub fn empty(corpus: PathBuf, from: u32, to: u32) -> PinVerifyReport { /* all arms default, verdict Fail */ }
    pub fn failed(&self) -> bool { self.verdict == Verdict::Fail }
    pub fn write_json(&self, w: impl std::io::Write) -> anyhow::Result<()> { Ok(serde_json::to_writer_pretty(w, self)?) }
    pub fn write_text(&self, mut w: impl std::io::Write) -> anyhow::Result<()> {
        // One line per phase, the verdict last, the notes after it. Model
        // `report.rs`'s `write_text`. Include: origin/frontier/end, the pin
        // response, the refusal arm (exit code, matched), the swap arm
        // (version seen vs --to, caught up, the two comparisons), then
        // `verdict: PASS | INCONCLUSIVE | FAIL`.
        todo_write_text(self, &mut w)
    }
}

/// Run the app's `replay` form (moved here from the bin, unchanged).
pub fn run_replay(bin: &Path, args: &[String], corpus: &Path, out: &Path, from_genesis: bool) -> anyhow::Result<Trace> { /* the bin's `replay()` verbatim, `args` prepended */ }
/// Run the app's `project` form and return the projection text.
pub fn run_project(bin: &Path, args: &[String], artifact: &Path, position: u64) -> anyhow::Result<String> {
    let out = Command::new(bin).args(args).arg("project").arg("--artifact").arg(artifact).arg("--position").arg(position.to_string()).output()
        .with_context(|| format!("spawn {}", bin.display()))?;
    if !out.status.success() { bail!("{} project exited {}: {}", bin.display(), out.status, String::from_utf8_lossy(&out.stderr)); }
    Ok(String::from_utf8(out.stdout)?)
}

pub fn run(a: &PinVerifyArgs) -> anyhow::Result<PinVerifyReport> {
    let corpus = Corpus::open(&a.corpus)?;
    let (frames, skipped_timers) = live::message_frames(&corpus)?;
    if frames.is_empty() { bail!("the corpus has no MESSAGE frames in [{}, {}) — nothing to replay", corpus.manifest.origin, corpus.manifest.end); }
    let split = a.split.unwrap_or(frames.len() / 2).clamp(1, frames.len().saturating_sub(1).max(1));
    let scratch = /* a.scratch or tempfile beside the report: `<report>.pinverify/` */;
    let dir = scratch.join("instance");
    std::fs::create_dir_all(&dir)?;
    let mut r = PinVerifyReport::empty(a.corpus.clone(), 0, a.to);
    r.frames = frames.len() as u64; r.skipped_timers = skipped_timers;

    // ---- Phase 1: history under OLD (spec S1–S3 have happened; this is the
    // cluster's life before the upgrade) ----
    let node = live::start_node(&dir, &a.app_id, &a.fsm, a.timeout)?;
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &a.app_id)?;
    let before = live::incarnation(&cnc, a.row);
    let mut old = live::spawn_app(&a.old, &a.old_args, &dir, &a.app_id, &scratch.join("old.stderr"))?;
    match old.wait_attached(&cnc, a.row, before, a.timeout) {
        AttachOutcome::Attached => {}
        AttachOutcome::Exited { code, stderr } => { node.stop(); bail!("OLD never attached (exit {code:?}): {stderr}"); }
    }
    r.from = cnc.service_slot(a.row as usize).status.version();   // the version the row is attached at = the pin's `from`
    live::replay_span(&frames, &dir, &a.app_id, a.row, 0..split, a.timeout)?;
    // ---- S4 step 1: the instant at P ----
    let p = live::command_instant(&node, a.timeout)?;
    if !live::wait_for(|| live::artifact_path(&dir, a.row, p).is_file(), a.timeout) { bail!("row {} never published snap-{p}", a.row); }
    r.origin = p;
    // OLD runs on past P — the durable shape's precondition (an in-memory SM
    // simply attaches empty later; the check is the same).
    let tail = live::replay_span(&frames, &dir, &a.app_id, a.row, split..frames.len(), a.timeout)?;
    // The frontier X: OLD's published `applied` once the last response is in.
    let x = cnc.service_slot(a.row as usize).applied.load_acquire().max(tail.last_position);
    r.frontier = x;
    let st = old.stop(a.timeout)?;
    r.notes.push(format!("OLD stopped at X={x} with exit {st}"));

    // ---- S4 step 2: the pin, exactly as an operator places it ----
    match live::pin_row(&dir, &cnc, a.row, r.from, a.to, p, a.timeout) {
        Ok(resp) => { r.pin = PinArm { status: resp.status, reason: resp.reason }; }
        Err(e) => { r.notes.push(format!("pin refused: {e}")); node.stop(); return Ok(r); }  // verdict stays Fail
    }
    if !live::wait_for(|| matches!(cnc.service_slot(a.row as usize).status.pin(), PinRead::Pinned { origin, .. } if origin == p), a.timeout) {
        node.stop(); bail!("the pin never reached row {}'s slot words: {:?}", a.row, cnc.service_slot(a.row as usize).status.pin());
    }

    // ---- S4 step 5: the refusal arm — the stale binary must not rejoin ----
    let before = live::incarnation(&cnc, a.row);
    let mut stale = live::spawn_app(&a.old, &a.old_args, &dir, &a.app_id, &scratch.join("old-after-pin.stderr"))?;
    let refusal_held = match stale.wait_attached(&cnc, a.row, before, a.timeout) {
        AttachOutcome::Exited { code, stderr } => {
            let matched = stderr.contains(REFUSAL_MARKER);
            r.refusal = RefusalArm { exited: true, code, matched, stderr_excerpt: last_lines(&stderr, 5) };
            code != Some(0) && matched
        }
        AttachOutcome::Attached => {
            r.refusal = RefusalArm { exited: false, code: None, matched: false, stderr_excerpt: last_lines(&stale.stderr(), 5) };
            let _ = stale.stop(a.timeout);
            false
        }
    };

    // ---- S4 step 4: the swap arm — NEW attaches, installs the origin, recomputes (P, X] ----
    let before = live::incarnation(&cnc, a.row);
    let mut new = live::spawn_app(&a.new, &a.new_args, &dir, &a.app_id, &scratch.join("new.stderr"))?;
    match new.wait_attached(&cnc, a.row, before, a.timeout) {
        AttachOutcome::Exited { code, stderr } => { r.notes.push(format!("NEW did not attach (exit {code:?}): {}", last_lines(&stderr, 5))); node.stop(); r.verdict = verdict(refusal_held, None, None); return Ok(r); }
        AttachOutcome::Attached => { r.swap.attached = true; }
    }
    r.swap.version_seen = cnc.service_slot(a.row as usize).status.version();
    if r.swap.version_seen != a.to { r.notes.push(format!("NEW attached as version {:#010x} but --to named {:#010x}", r.swap.version_seen, a.to)); }
    r.swap.caught_up = matches!(new.wait_applied(&cnc, a.row, x, a.timeout), AttachOutcome::Attached);
    if !r.swap.caught_up { r.notes.push(format!("NEW never reached X={x}: {}", last_lines(&new.stderr(), 5))); let _ = new.stop(a.timeout); node.stop(); r.verdict = verdict(refusal_held, None, None); return Ok(r); }
    // A second instant at Q, so the live state is an artifact NEW can project.
    let q = live::command_instant(&node, a.timeout)?;
    if !live::wait_for(|| live::artifact_path(&dir, a.row, q).is_file(), a.timeout) { bail!("row {} never published snap-{q}", a.row); }
    r.end = q;
    let _ = new.stop(a.timeout)?;

    // ---- The three projections ----
    let live_proj = run_project(&a.new, &a.new_args, &live::artifact_path(&dir, a.row, q), q)?;
    let exported = scratch.join("corpus");
    Corpus::export(&dir, &a.app_id, a.row, p, q, r.from, &exported)?;
    let art = run_replay(&a.new, &a.new_args, &exported, &scratch.join("artifact.json"), false)?;
    let gen = run_replay(&a.new, &a.new_args, &exported, &scratch.join("genesis.json"), true)?;
    node.stop();
    let (art_p, gen_p) = (art.projection_at_end.clone(), gen.projection_at_end.clone());
    r.swap.live_eq_artifact = art_p.as_ref().map(|s| *s == live_proj);
    r.swap.artifact_eq_genesis = match (&art_p, &gen_p) { (Some(x), Some(y)) => Some(x == y), _ => None };
    r.swap.live = Some(live_proj); r.swap.artifact = art_p; r.swap.genesis = gen_p;
    r.verdict = verdict(refusal_held && r.swap.version_seen == a.to, r.swap.live_eq_artifact, r.swap.artifact_eq_genesis);
    if r.verdict == Verdict::Inconclusive { r.notes.push("NEW's artifact-path and genesis-path projections agree over this span: the change did not alter any replayed command's semantics, so this run cannot show the counterfactual; the refusal arm and live==artifact still hold".into()); }
    Ok(r)
}

fn last_lines(s: &str, n: usize) -> String { s.lines().rev().take(n).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n") }
```

Replace the `todo_write_text` placeholder with the real renderer (the plan forbids placeholders in the delivered code; it is named here only to keep this block readable — the renderer is ~25 lines of `writeln!`). **The genesis-path replay** walks the exported corpus's journal from position 0 (`Origin::Genesis`), which exists because the rig's purge is OFF — assert in `run` that the exported corpus's journal `first_meta()` is 0 (`uc_journal::TailReader`) and put the failure in `notes` if not; without that, "genesis" would silently be "from the first retained block".

- [ ] **Step 4: The subcommand**

`uc_diffreplay/src/bin/uc2-diffreplay.rs`:

```rust
    /// Spec §6.2 part 2: on a real node, with the app's real binaries, prove
    /// the pin refuses the stale binary and steers the new one onto the
    /// artifact path (never the genesis counterfactual).
    #[cfg(feature = "pin-verify")]
    PinVerify {
        #[arg(long)] corpus: PathBuf,
        /// The OLD service binary (the version running before the upgrade).
        #[arg(long)] old: PathBuf,
        /// Extra arguments placed BEFORE `--instance-dir`/`--app-id` when running OLD (repeatable).
        #[arg(long = "old-arg")] old_args: Vec<String>,
        #[arg(long)] new: PathBuf,
        #[arg(long = "new-arg")] new_args: Vec<String>,
        #[arg(long)] app_id: String,
        /// The row's FSM name, as `[services] names` declares it.
        #[arg(long)] fsm: String,
        #[arg(long, default_value_t = 0)] row: u8,
        /// The packed version the pin names — what `uc2ctl upgrade pin --to` takes.
        #[arg(long, value_parser = parse_version)] to: u32,
        /// MESSAGE frames re-submitted before the instant (default: half).
        #[arg(long)] split: Option<usize>,
        #[arg(long, default_value_t = 60)] timeout_secs: u64,
        /// Where the scratch instance dir and traces go (default: `<report>.pinverify/`).
        #[arg(long)] scratch: Option<PathBuf>,
        #[arg(long)] report: PathBuf,
    },
```

`parse_version` accepts what `uc2ctl upgrade pin --to` accepts — read `uc_ctl/src/upgrade.rs`'s parser and copy its rule (a decimal/hex packed `u32` and/or `major.minor.patch` → `pack_version`); pin it with a unit test in the bin (`#[cfg(test)]`). The arm: build `PinVerifyArgs`, `let r = pinverify::run(&a)?; r.write_json(File::create(&report)?)?; r.write_text(stdout())?; if r.failed() { exit(1) }` — on PASS/INCONCLUSIVE remove the scratch dir unless `--scratch` was given (evidence stays on FAIL, like `clear_traces`).

- [ ] **Step 5: Run unit tests, lint**

Run: `cargo test -p uc_diffreplay --lib pinverify && cargo test -p uc_diffreplay --bin uc2-diffreplay && cargo clippy -p uc_diffreplay --all-targets -- -D warnings && cargo clippy -p uc_diffreplay --no-default-features --all-targets -- -D warnings`
Expected: 3 + the parser test passed; clippy clean both ways; the three existing modes' e2e (`upgrade_e2e.rs`, `reconstruction.rs`) still pass after the `replay()` move: `cargo test -p uc_diffreplay`.

- [ ] **Step 6: Commit**

```bash
git add uc_diffreplay/src/pinverify.rs uc_diffreplay/src/lib.rs uc_diffreplay/src/bin/uc2-diffreplay.rs
git commit -m "uc_diffreplay: pin-verify — the S4 refusal and swap arms on a real node, judged (plan C T3)"
```

---

### Task 4: End to end — empty, durable, a stale NEW, a no-op change, a same-version refusal

**Files:**
- Create: `uc_diffreplay/tests/pin_verify.rs`
- Modify: `uc_diffreplay/tests/common/mod.rs` (`build_register_history_with`), `uc_diffreplay/src/pinverify.rs` (the up-front same-version refusal, ruling R-C-1)
- Test: `uc_diffreplay/tests/pin_verify.rs`

**Interfaces:**
- Consumes: `register-replay serve [--double] [--durable]` (T1/T3: `--old-arg`/`--new-arg` carry the serve form's argv INCLUDING the `serve` verb; `pinverify::app_knobs` strips the verb for the `replay`/`project` forms), `uc2-diffreplay pin-verify` (T3, via `env!("CARGO_BIN_EXE_uc2-diffreplay")`), `Corpus::export`, `uc_lincheck::register::{Cmd, CmdResp}` (`Cmd::Write(u64)`, `Cmd::Cas { old, new }` → `CmdResp::CasResult(bool)`).
- Produces: `common::build_register_history_with(dir, app_id, before: &[Cmd], after: &[Cmd]) -> u64` (P); `pinverify::run` refuses before the pin when the row's attached version equals `--to`.

**Why the corpus needs a CAS tail (ruling R-C-1).** `RegisterSm` is last-write-wins: a `Write` tail lands on the same final value under every start state, so NEW's artifact-path and genesis-path projections AGREE over a pure-write span and the run is INCONCLUSIVE — true, but not the demonstration. A `Cas { old, new }` whose `old` is the value at P succeeds on the artifact path (state at P is v1's) and fails on the genesis path (state at P is the doubled counterfactual), so the two paths diverge by construction. The pure-write corpus is kept as the INCONCLUSIVE case.

**Why a same-version run is refused (ruling R-C-1).** The refusal arm re-attaches OLD after a pin to `--to`; if the row already runs `--to`, OLD IS the pinned version, attaches, and the arm cannot hold — a FAIL that would mislead. The mode therefore refuses before placing the pin, naming both numbers.

- [ ] **Step 1: The up-front refusal (RED first)**

In `uc_diffreplay/src/pinverify.rs::run`, immediately after `r.from` is read from the attached word and BEFORE the first `replay_span`:

```rust
    if r.from == a.to {
        let _ = old.stop(a.timeout);
        node.stop();
        bail!(
            "row {} already runs version {:#010x}, which --to also names; pin-verify needs a \
             version change (a same-version pin cannot hold the refusal arm: the \"stale\" \
             binary IS the pinned version)",
            a.row, r.from
        );
    }
```

RED: write test (e) below first and watch it fail (the run proceeds and reports FAIL instead of refusing); then add the check; GREEN.

- [ ] **Step 2: The corpus builders**

`uc_diffreplay/tests/common/mod.rs`:

```rust
/// Drive a single node with RegisterSm: `before`, an instant at P, `after`.
/// Returns P. `build_register_history` is the all-writes special case.
pub fn build_register_history_with(dir: &Path, app_id: &str, before: &[Cmd], after: &[Cmd]) -> u64 {
    let node = start_single_node(dir, app_id, register_name());
    let cfg = ServiceConfig::new(dir.to_path_buf(), app_id.to_string());
    let svc = ServiceBuilder::new(cfg, RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for c in before {
        let _: CmdResp = client.submit(c).unwrap();
    }
    let p = command_instant(&node);
    let art = artifact_path(dir, 0, p);
    wait_until(|| art.is_file());
    for c in after {
        let _: CmdResp = client.submit(c).unwrap();
    }
    client.shutdown();
    svc.stop();
    node.stop();
    p
}

pub fn build_register_history(dir: &Path, app_id: &str, n: u64, m: u64) -> (u64, u64) {
    let before: Vec<Cmd> = (0..n).map(Cmd::Write).collect();
    let after: Vec<Cmd> = (n..n + m).map(Cmd::Write).collect();
    (build_register_history_with(dir, app_id, &before, &after), u64::MAX)
}
```

- [ ] **Step 3: The test file**

```rust
//! `uc2-diffreplay pin-verify` end to end on the register fixture (spec
//! §6.2 part 2): the empty and the durable state-machine shapes both PASS
//! with the counterfactual DEMONSTRATED on a corpus whose tail is a CAS
//! chain (ruling R-C-1); a stale NEW is a FAIL (the swap arm is refused by
//! name); a pure-write corpus is INCONCLUSIVE, not a pass; a same-version
//! run is refused up front.
mod common;
use std::path::{Path, PathBuf};
use std::process::Command;

use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::pinverify::{PinVerifyReport, Verdict};
use uc_lincheck::register::Cmd;

const WRITES: u64 = 200;
const CAS_TAIL: u64 = 50;

/// `WRITES` writes, an instant at P, then a CAS chain that starts from the
/// value v1 holds at P (`WRITES - 1`) — the tail whose outcome depends on
/// the state at P.
fn cas_corpus(app_id: &str) -> (tempfile::TempDir, PathBuf) {
    let inst = common::tempdir();
    let before: Vec<Cmd> = (0..WRITES).map(Cmd::Write).collect();
    let after: Vec<Cmd> = (0..CAS_TAIL)
        .map(|k| Cmd::Cas { old: WRITES - 1 + k, new: WRITES + k })
        .collect();
    let p = common::build_register_history_with(inst.path(), app_id, &before, &after);
    let out = inst.path().join("corpus");
    Corpus::export(inst.path(), app_id, 0, p, u64::MAX, 0, &out).unwrap();
    (inst, out)
}

/// The all-writes corpus: last-write-wins makes both paths agree.
fn write_corpus(app_id: &str) -> (tempfile::TempDir, PathBuf) {
    let inst = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), app_id, WRITES, WRITES);
    let out = inst.path().join("corpus");
    Corpus::export(inst.path(), app_id, 0, p, u64::MAX, 0, &out).unwrap();
    (inst, out)
}

struct Run { status: std::process::ExitStatus, report: Option<PinVerifyReport>, stdout: String, stderr: String }

fn pin_verify(corpus: &Path, old_args: &[&str], new_args: &[&str], to: &str, app_id: &str) -> Run {
    let bin = common::register_replay_bin();
    let report = corpus.parent().unwrap().join(format!("{app_id}.json"));
    let mut c = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    c.arg("pin-verify").arg("--corpus").arg(corpus)
        .arg("--old").arg(&bin).arg("--new").arg(&bin)
        .arg("--app-id").arg(app_id).arg("--fsm").arg(common::register_name())
        .arg("--to").arg(to).arg("--report").arg(&report);
    for a in old_args { c.arg("--old-arg").arg(a); }
    for a in new_args { c.arg("--new-arg").arg(a); }
    let out = c.output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    eprintln!("{stdout}{stderr}");
    let report = std::fs::File::open(&report).ok().map(|f| serde_json::from_reader(f).expect("report parses"));
    Run { status: out.status, report, stdout, stderr }
}

/// (a) The empty shape: an in-memory register. OLD = plain, NEW = `--double`.
#[test]
fn an_in_memory_register_passes_and_demonstrates_the_counterfactual() {
    let (_inst, corpus) = cas_corpus("pv-empty");
    let r = pin_verify(&corpus, &["serve"], &["serve", "--double"], "2", "pv-empty");
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Pass);
    assert!(rep.refusal.matched, "the stale binary must be refused BY NAME");
    assert_eq!(rep.swap.live_eq_artifact, Some(true));
    assert_eq!(rep.swap.artifact_eq_genesis, Some(false), "the CAS tail makes the paths diverge");
    assert!(rep.frontier > rep.origin, "OLD must have run past P before the stop");
}

/// (b) The durable shape: `--durable` persists `(value, last_applied)`, so
/// NEW attaches with `last_applied() = X > P` and MUST be rewound to P by
/// the pinned install (spec §2.3's third path, S4 step 4).
#[test]
fn a_durable_register_is_rewound_to_the_origin_and_passes() {
    let (_inst, corpus) = cas_corpus("pv-durable");
    let r = pin_verify(&corpus, &["serve", "--durable"], &["serve", "--double", "--durable"], "2", "pv-durable");
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Pass);
    assert_eq!(rep.swap.live_eq_artifact, Some(true), "a durable SM that was NOT rewound would carry OLD's (P, X] and differ here");
    assert_eq!(rep.swap.artifact_eq_genesis, Some(false));
}

/// (c) Teeth for the swap arm: NEW is the OLD binary (version 0) while the
/// pin names 2 — the SDK refuses it at attach, and the mode must FAIL.
#[test]
fn a_new_binary_that_is_not_the_pinned_version_is_a_fail() {
    let (_inst, corpus) = cas_corpus("pv-stale");
    let r = pin_verify(&corpus, &["serve"], &["serve"], "2", "pv-stale");
    let rep = r.report.expect("report");
    assert!(!r.status.success());
    assert_eq!(rep.verdict, Verdict::Fail);
    assert!(rep.refusal.matched, "the refusal arm itself still holds");
    assert!(!rep.swap.attached, "{}", r.stdout);
}

/// (d) No state-dependent command in the tail: last-write-wins makes NEW's
/// artifact-path and genesis-path projections agree, so the run is
/// INCONCLUSIVE — exit 0 with the note, never PASS.
#[test]
fn a_pure_write_corpus_is_inconclusive_not_a_pass() {
    let (_inst, corpus) = write_corpus("pv-writes");
    let r = pin_verify(&corpus, &["serve"], &["serve", "--double"], "2", "pv-writes");
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Inconclusive);
    assert!(rep.refusal.matched);
    assert_eq!(rep.swap.live_eq_artifact, Some(true));
    assert_eq!(rep.swap.artifact_eq_genesis, Some(true));
    assert!(rep.notes.iter().any(|n| n.contains("cannot show the counterfactual")), "{:?}", rep.notes);
}

/// (e) A same-version "upgrade" cannot hold the refusal arm; the mode says
/// so before placing a pin (ruling R-C-1). No report is written.
#[test]
fn a_same_version_run_is_refused_before_the_pin() {
    let (_inst, corpus) = write_corpus("pv-same");
    let r = pin_verify(&corpus, &["serve", "--double"], &["serve", "--double"], "2", "pv-same");
    assert!(!r.status.success(), "{}", r.stdout);
    assert!(r.report.is_none(), "refused before any arm ran: no report");
    assert!(r.stderr.contains("already runs version"), "{}", r.stderr);
}
```

(`serde_json` and `tempfile` are already available to `uc_diffreplay`'s integration tests as a dependency / dev-dependency; `uc_lincheck` is a dev-dependency.)

- [ ] **Step 4: Run RED, then GREEN**

RED for (e): before Step 1's check exists, the run proceeds and (e) fails on `r.report.is_none()` — record it. RED for the verdict teeth: with `verdict` temporarily returning `Verdict::Pass` unconditionally (a one-line local mutation, not committed), (c) must fail on its `Verdict::Fail` assertion — record it. Then restore.
Run: `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`
Expected: 5 passed (single-threaded: five in-process nodes plus register processes at once would only add noise). Then run the file three more times and report the tally.

- [ ] **Step 5: Commit**

```bash
git add uc_diffreplay/tests/pin_verify.rs uc_diffreplay/tests/common/mod.rs uc_diffreplay/src/pinverify.rs
git commit -m "uc_diffreplay: pin-verify e2e — empty and durable PASS with the counterfactual shown on a CAS tail, a stale NEW FAILs, a pure-write span is INCONCLUSIVE, a same-version run is refused (plan C T4)"
```

---

### Task 5: Docs, spec errata, proof stack

**Files:**
- Modify: `uc_diffreplay/README.md` (§ "CLI contract for app binaries": the serve form and exit codes; a `pin-verify` paragraph), `docs/how-to/diff-replay.md` (a new § "4. Verify the pin live" with the command, the three-projection logic, PASS/INCONCLUSIVE/FAIL, the `--to`/`--fsm` inputs, what the report's `frontier` means), `docs/how-to/upgrade-an-application.md` (S4's rehearsal step points at `pin-verify` as the pre-flag-day check; the two shapes named), `docs/VERIFICATION.md` (the `pin_verify.rs` suite beside the reconstruction entry; what it does NOT verify: a multi-node cluster, the app's own durability), the spec's `#### Errata (plan C, as built)` block under §6.2 (the five errata above plus anything execution added), `docs/BACKLOG.md` (line for "pin-verify on a multi-node rig / against a running cluster" and any leftover).
- Proof stack (paste every tail): fmt; workspace clippy + the four feature-gated runs; `cargo build -p uc_lincheck --features replay-bin --bin register-replay`; `cargo build -p uc_diffreplay`; `cargo build -p kv_store` (the `default-features = false` consumer); `cargo test --workspace`; `cargo test -p uc_node --test lin_v2`; `cargo test -p uc_crashtest --features hard-crash-tests`; `(cd fuzz && cargo +nightly fuzz build)`.
- Commit `docs: pin-verify — reconstruction mode part 2, the serve-form contract, spec errata as built (plan C T5)`.

---

## Self-review

**Spec coverage.** §6.2 part 2 "verify that the system refuses the wrong one" → T3's refusal arm (S4 step 5) and swap arm (S4 step 4, live == artifact ≠ genesis), on a real node with real binaries (§6.3 black-box). "For both the empty and the durable-SM shapes" → T1's `Durable<S>` + T4's two PASS tests; erratum 3 records that the harness cannot tell the shapes apart on an arbitrary app. §1.4 (refusal, not agreement) → the verdict table: agreement of live with the ARTIFACT path is required, agreement with genesis is what INCONCLUSIVE names. §2.3's "counterfactual engages within ring size" → the rig's 1 MiB ring with 400 writes may NOT scroll — **T4 must assert, as `reconstruction.rs` does, that the genesis-path replay actually differs from the artifact path** (it does by construction: the driver's `--from-genesis` walks the exported journal from 0 regardless of the ring, so the ring size is irrelevant to the DRIVER comparison; the LIVE path's correctness is what the pin guarantees and what `live_eq_artifact` checks). §11 item 8 → this plan. Items 9 (skill), 1–2 (docs) remain plan D's.

**Placeholder scan.** `todo_write_text` and the recursive `Retry` sketch in T2/T3 are called out as things the implementer replaces with the named real code; no "TBD"/"similar to". `parse_version` points at the exact source to copy.

**Type consistency.** `AttachOutcome::{Attached, Exited{code, stderr}}` (T2) is matched in T3; `live::pin_row(.., timeout)` returns `anyhow::Result<AdminResp>` (T2) and T3 maps `Err` into the report; `run_replay`/`run_project` (T3) take `(bin, args, …)` and T4 passes `--old-arg`/`--new-arg` through `PinVerifyArgs::{old_args, new_args}`; `Durable::open(inner, &Path) -> io::Result` (T1) is what T1's `serve` arm calls with `?` (io::Error → anyhow); `PinVerifyReport.verdict: Verdict` with `PartialEq` (T3) is what T4 asserts on; `message_frames -> (Vec<Vec<u8>>, u64)` (T2) is destructured in T3.
