# Pinned install at attach, attach refusal and the `ULTSNAP2` envelope (plan B2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the pin plan B1 records *bite* on the service side: a service attaching to a pinned row installs `snap-<origin>` unconditionally when its `VERSION` equals the pin's `to` (rewinding a durable state machine that is above the origin), and is refused by name when it does not; every row artifact carries the version that built it (`ULTSNAP2 ‖ P ‖ version`), which both install paths cross-check; and the three `uc_service` carries plan A left open (a public sched-record accessor, an `ids()` call count, an `on_committed` recorder) land in the diff-replay driver.

**Architecture:** The cnc status line gains a fourth pin word, `pinned_from` (+40), under the same seqlock as the other three, and `ServiceStatusLine::pin()` becomes tri-state (`NoPin` / `Pinned{origin, from, to}` / `Contended`) so the attach path can fail CLOSED. `attach()` reads it after finding its row and before publishing any slot word: a `to` that is not `S::VERSION` refuses; `Contended` refuses; a match on a row that cannot install (plain `start()`) refuses; otherwise the artifact at `origin` is opened, its `ULTSNAP2` envelope must name `origin` and the pin's `from`, and `install_snapshot(origin)` runs before the follower is created — so `applied` is published at `origin` and the live path continues from there. The gap guard's opportunistic install (an *unpinned* row falling below the purge floor) now requires the artifact's version to equal `S::VERSION`, which turns the §2.3 silent counterfactual into a named refusal. The envelope grows from 16 to 24 bytes; `ULTSNAP1` artifacts are refused by name, so 2.13.0 requires one wipe of `snapshots/<row>/` per node (the cluster artifact is unaffected). Plan C extends `uc2-diffreplay reconstruction` to exercise these shapes; plan B3 adds the live hash reports.

**Tech Stack:** Rust 1.96 (MSRV 1.89); crates `uc_log`, `uc_protocol`, `uc_service`, `uc_node`, `uc_ctl`, `uc_diffreplay`; `cargo-fuzz` (nightly).

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` — §3 S4 steps 4–5, §9.1 (2), §2.3 (the counterfactual this closes), §6.2 (the shapes plan C will verify). Plan B1 (`docs/superpowers/plans/2026-09-20-uc2-upgrade-pin-and-snapshot-report.md`) is the substrate; its ledger's B2 carries: "`pin()`'s `None` is fail-open for the attach refusal — treat exhaustion distinctly"; "B2 reads the pair only via `pin()`".

## Global Constraints

- **cnc 3.3 is unshipped on `main` (B1 merged 2026-09-21 as `18ba553`, no tag), so the fourth word rides the same version: `CNC_V2_VERSION` stays `(3 << 24) | (3 << 16)`.** Status-line layout after this plan: `+0 status`, `+8 version`, `+16 upgrade_origin`, `+24 pinned_version`, `+32 pin_seq`, **`+40 pinned_from`**, `_pad: [u64; 2]`; 64 B; pinned in BOTH `uc_protocol/src/v2/cnc.rs` (constant + frozen test + layout comment) and `uc_log/src/cnc.rs` (`offset_of!`).
- **Seqlock unchanged:** `store_pin` brackets ALL data words (version, from, origin) between the odd and even `pin_seq` bumps; the reader is the classic seqlock (B1's proof stands — more data words change nothing).
- **`pin()` is tri-state:** `PinRead::NoPin` (origin 0), `PinRead::Pinned { origin, from, to }`, `PinRead::Contended` (64 spins exhausted). Metrics and `uc2ctl status` render `Contended` as zeros; **attach refuses it by name** (fail-closed, retryable).
- **`ULTSNAP2` envelope, 24 B exactly:** `b"ULTSNAP2" @0 ‖ position u64 @8 ‖ version u32 @16 ‖ reserved u32 @20 (zero)`. `SNAPSHOT_ENVELOPE_LEN = 24`. `ULTSNAP1` is refused by name (`EnvelopeError::Legacy`); every other magic is `BadMagic`; a non-zero reserved word is `BadMagic` too (the codec is total on any slice — the fuzz target keeps that property).
- **Version cross-checks:** the gap guard's unpinned install requires `artifact.version == S::VERSION`; the pinned install at attach requires `artifact.version == pin.from`. The diff-replay driver passes `None` (upgrade mode installs v_old's artifact into v_new by design) and records the artifact's version in the trace.
- **Attach order (S4 step 4–5):** row found → pin read → refusals → pinned install → `applied` published at `origin` → status/version stored → epoch bumped. Nothing is written to the slot before the pin decision. The harness page (no names, `services_declared == 0`) reads `NoPin` and is unaffected.
- **Refusals are `ServiceError` variants, by name:** `PinnedVersionMismatch { name, row, origin, pinned, mine }`, `PinUnreadable { row }`, `PinRequiresSnapshots { name, row, origin }`, `PinnedArtifactMissing { row, origin, path }`, and the existing `MistaggedSnapshot` carrying `EnvelopeError::{VersionMismatch, Legacy, …}`.
- **Unconditional means unconditional:** a pinned, version-matching row installs `snap-<origin>` even when `last_applied() == Some(origin)` or above it. `install_snapshot` returns `origin` (exclusive frontier) and the SM's cursor must be strictly below it afterwards (the same two checks `uc_diffreplay::drive::install_from` makes).
- **Apply stays sync/deterministic;** nothing in this plan touches the apply hot loop except `ApplyCtx::ids()` gaining one `u32` increment (measure nothing — it is a counter on a path that allocates an `IdGen`).
- `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings` plus the feature-gated clippy runs (`uc_crashtest --features hard-crash-tests`, `uc_lincheck --features replay-bin`, `uc_service --features apply-profile`, `uc_gateway --features test-util`); fuzz crate builds; frozen numbers get a test.
- **No `RELEASES.md`/`docs/releases.md`/`CLAUDE.md` edits** — plan D. No `git stash` (shared stack). Scratch under `$HOME/scratch/`.

### Errata against the spec text (decided while planning; Task 7 records them under §3 S4 / §9.1)

1. **A fourth cnc word, `pinned_from` (+40).** §9.1 says the stamp "lets `install_snapshot` cross-check that the artifact … was built by the version the pin says was in effect at P" — that version is the pin's `from`, which the service could not read from two words. The FSM already holds it; the agent republishes it.
2. **Tri-state `pin()`.** B1's `Option` collapsed "contended" into "no pin"; attach must not fail open on a torn read.
3. **The install happens in `attach`, not in the gap guard.** A durable SM above the origin on an unscrolled ring never enters replay, so the gap guard cannot be the hook. `attach` takes an optional install capability from `start_with_snapshots`; plain `start()` on a pinned row refuses.
4. **Two cross-checks, not one:** unpinned install = same version; pinned install = the pin's `from`. Spec §9.1 named only the pinned one.
5. **`ULTSNAP2` closes B1's open question:** 2.13.0 requires clearing `snapshots/<row>/` once per node (the how-to's OPEN marker becomes a step); `snapshots/cluster/` is untouched.
6. **The driver's `on_committed` recorder records the framework's call, not the handler's effects:** per applied frame, `Ok`/`Retryable`/`Permanent` from a handler the caller supplies, run on a current-thread runtime. What a handler *does* is outside the trace.

---

## File structure

| file | responsibility |
|---|---|
| `uc_protocol/src/v2/cnc.rs` | `CNC_SVC_OFF_PINNED_FROM = 40`, layout comment, frozen test |
| `uc_log/src/cnc.rs` | `pinned_from` word, `PinRead`, `store_pin(origin, from, to)`, `pin()` |
| `uc_node/src/cluster_agent.rs`, `obs/metrics.rs`; `uc_ctl/src/main.rs` | pass `p.from`; render tri-state |
| `uc_service/src/snapshots.rs` | `ULTSNAP2` codec, `Envelope`, `Legacy`/`VersionMismatch`, `publish(pos, version, …)` |
| `uc_service/src/builder_agent.rs`, `lib.rs` | builder stamps `S::VERSION` |
| `uc_service/src/replay.rs` | gap-guard install cross-checks `S::VERSION` |
| `uc_service/src/attach.rs`, `apply.rs`, `config.rs`, `lib.rs` | pin read, refusals, pinned install, `Attached.pin` |
| `uc_service/src/traits.rs`, `ids.rs` | `take_sched_records` public; `ids_calls` |
| `uc_diffreplay/src/{drive,trace,diff}.rs` | `artifact_version`, `ids_calls`, `output` surfaces |
| `uc_node/src/backup.rs`, `uc_ctl/src/snapshot.rs`, `examples/kv/tests/cluster.rs`, `examples/uc_adjudicate/src/{adapter,diverge}.rs`, `fuzz/fuzz_targets/uc_service_snapshot_envelope.rs` | envelope readers |
| `uc_service/tests/pinned_attach.rs` (**new**), `uc_diffreplay/tests/reconstruction.rs` | the proofs |
| docs | `state-machine-contract.md`, `instance-directory.md`, `cnc-page.md`, `upgrade-a-cluster.md`, `back-up-a-cluster.md`, `diff-replay.md`, `limits.md`, `uc2ctl.md`, `monitor-a-cluster.md`, the explainer, the spec errata |

---

### Task 1: The fourth pin word and the tri-state reader

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (after `CNC_SVC_OFF_PIN_SEQ`; the per-slot layout comment; the frozen-offsets test), `uc_log/src/cnc.rs` (`ServiceStatusLine`, `store_pin`, `pin`, tests), `uc_node/src/cluster_agent.rs` (`publish_view`'s `store_pin` call), `uc_node/src/obs/metrics.rs` (the `pin()` read in the `ServiceRow` builder), `uc_ctl/src/main.rs` (`status`'s per-row line), `docs/reference/cnc-page.md` (the `+40` row)
- Test: both cnc test modules; `uc_node --lib cluster_agent` and `obs::metrics`; `uc_ctl`

**Interfaces:**
- Produces: `uc_protocol::v2::cnc::CNC_SVC_OFF_PINNED_FROM: usize = 40`; in `uc_log::cnc`:
  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum PinRead {
      NoPin,
      Pinned { origin: u64, from: u32, to: u32 },
      Contended,
  }
  impl ServiceStatusLine {
      pub fn store_pin(&self, origin: u64, from: u32, to: u32);   // was (origin, version)
      pub fn pin(&self) -> PinRead;                                // was Option<(u64, u32)>
      pub fn pinned_from(&self) -> u32;
      // upgrade_origin(), pinned_version(), pin_seq() unchanged
  }
  ```
- Consumes: B1's `pin_seq` seqlock (`uc_log/src/cnc.rs` ~245-281), `ClusterState::pin_for(row) -> Option<&UpgradePin { row, from, to, origin }>`.

- [ ] **Step 1: Write the failing tests**

`uc_protocol/src/v2/cnc.rs`, in the slot-offset test beside the `CNC_SVC_OFF_PIN_SEQ` assertions:

```rust
        // plan B2: the pin's `from` — the version the artifact at `origin`
        // was built by — fourth word under the same seqlock.
        assert_eq!(CNC_SVC_OFF_PINNED_FROM, 40);
        assert_eq!(CNC_SVC_OFF_PINNED_FROM, CNC_SVC_OFF_PIN_SEQ + 8);
        const { assert!(CNC_SVC_OFF_PINNED_FROM + 8 <= 64, "inside the status line") };
```

`uc_log/src/cnc.rs` — rewrite the three B1 pin tests to the new shape and add one:

```rust
    #[test]
    fn pin_words_are_zero_at_init_and_store_publishes_all_three() {
        let page = /* the heap page the neighbouring tests build */;
        let s = &page.service_slot(2).status;
        assert_eq!(s.pin(), PinRead::NoPin);
        assert_eq!((s.upgrade_origin(), s.pinned_from(), s.pinned_version(), s.pin_seq()), (0, 0, 0, 0));
        s.store_pin(8192, 0x0100_0000, 0x0101_0000);
        assert_eq!(s.pin(), PinRead::Pinned { origin: 8192, from: 0x0100_0000, to: 0x0101_0000 });
        assert_eq!(s.pin_seq() & 1, 0, "even after a complete store");
        assert_eq!(page.service_slot(1).status.pin(), PinRead::NoPin, "slots are independent");
    }

    #[test]
    fn pin_reads_the_triple_together_across_a_repin() {
        let page = /* as above */;
        let s = &page.service_slot(0).status;
        s.store_pin(8192, 1, 2);
        s.store_pin(9000, 2, 3);
        assert_eq!(s.pin(), PinRead::Pinned { origin: 9000, from: 2, to: 3 });
        assert_eq!(s.pin_seq(), 4);
    }

    #[test]
    fn pin_is_contended_while_a_store_is_in_flight() {
        let page = /* as above */;
        let s = &page.service_slot(0).status;
        s.store_pin(8192, 1, 2);
        s.store_pin_begin_for_test(2, 3); // odd bump + version + from stored, origin NOT yet
        assert_eq!(s.pin_seq() & 1, 1);
        assert_eq!(s.upgrade_origin(), 8192, "origin has NOT moved yet");
        assert_eq!(s.pinned_version(), 3, "version already has");
        assert_eq!(s.pin(), PinRead::Contended, "mid-store: named, never a fabricated triple");
        s.store_pin_finish_for_test(9000);
        assert_eq!(s.pin(), PinRead::Pinned { origin: 9000, from: 2, to: 3 });
    }
```

(B1's `store_pin_begin_for_test` performs the odd bump and the version store; extend it to `(version, from)` and add `store_pin_finish_for_test(origin)` = origin store + even bump, so the test does not reach into private fields.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_protocol cnc 2>&1 | tail -4 && cargo test -p uc_log pin 2>&1 | tail -6`
Expected: compile errors (`CNC_SVC_OFF_PINNED_FROM`, `PinRead`, the new signatures).

- [ ] **Step 3: Implement**

`uc_protocol/src/v2/cnc.rs`:

```rust
/// Plan B2: the version the artifact at `upgrade_origin` was BUILT by — the
/// pin's `from`. A service attaching under the pin cross-checks the
/// `ULTSNAP2` envelope's version against this word (spec §9.1), which is why
/// it rides the same seqlock as the other three: a torn `(from, origin)`
/// would refuse a correct artifact or accept a wrong one.
pub const CNC_SVC_OFF_PINNED_FROM: usize = 40;
```

and the layout-comment row `+40 pinned_from u64 (low 32 = packed version the pin's origin was built by)  writer: node (cluster agent)`.

`uc_log/src/cnc.rs`:

```rust
#[repr(C)]
pub struct ServiceStatusLine {
    status: AtomicU64,
    version: AtomicU64,
    upgrade_origin: AtomicU64,
    pinned_version: AtomicU64,
    pin_seq: AtomicU64,
    pinned_from: AtomicU64,
    _pad: [u64; 2],
}

/// What a consistent read of the pin words found. `Contended` is the
/// 64-spin exhaustion of the seqlock read — with the single `uc2-cluster`
/// writer that is effectively unreachable, but a reader that must DECIDE
/// something (attach, plan B2) treats it as "could not read", never as
/// "no pin"; `/metrics` and `uc2ctl status` render it as zeros.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinRead {
    NoPin,
    Pinned { origin: u64, from: u32, to: u32 },
    Contended,
}

impl ServiceStatusLine {
    pub fn pinned_from(&self) -> u32 {
        self.pinned_from.load(Ordering::Acquire) as u32
    }
    /// SINGLE WRITER (the `uc2-cluster` agent). Odd bump, then every data
    /// word, then the even bump — all `Release`.
    pub fn store_pin(&self, origin: u64, from: u32, to: u32) {
        self.pin_seq.fetch_add(1, Ordering::Release);
        self.pinned_version.store(to as u64, Ordering::Release);
        self.pinned_from.store(from as u64, Ordering::Release);
        self.upgrade_origin.store(origin, Ordering::Release);
        self.pin_seq.fetch_add(1, Ordering::Release);
    }
    pub fn pin(&self) -> PinRead {
        for _ in 0..64 {
            let s1 = self.pin_seq.load(Ordering::Acquire);
            if s1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let origin = self.upgrade_origin.load(Ordering::Acquire);
            let to = self.pinned_version.load(Ordering::Acquire) as u32;
            let from = self.pinned_from.load(Ordering::Acquire) as u32;
            let s2 = self.pin_seq.load(Ordering::Acquire);
            if s1 == s2 {
                return if origin == 0 {
                    PinRead::NoPin
                } else {
                    PinRead::Pinned { origin, from, to }
                };
            }
            std::hint::spin_loop();
        }
        PinRead::Contended
    }
}
const _: () = assert!(std::mem::offset_of!(ServiceStatusLine, pinned_from) == cnc::CNC_SVC_OFF_PINNED_FROM);
```

Keep the B1 doc comments' seqlock argument; update them to name three data words. `#[cfg(test)] store_pin_begin_for_test(to, from)` / `store_pin_finish_for_test(origin)`.

`uc_node/src/cluster_agent.rs` `publish_view`: `status.store_pin(p.origin, p.from, p.to)`. `uc_node/src/obs/metrics.rs`: the `ServiceRow` reads become `match slot.status.pin() { PinRead::Pinned { origin, to, .. } => (origin, to as u64), _ => (0, 0) }` (a `Contended` scrape renders zeros — keep the gauge HELP's "0 = no pin or unreadable"). `uc_ctl/src/main.rs` `status`: the same match, and print ` pinned_from={}` after `pinned=` (`VersionDisplay(from)`), `unversioned` on zeros. `docs/reference/cnc-page.md`: the `| 40 | pinned_from … |` row after `pin_seq`'s.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_protocol cnc 2>&1 | tail -3 && cargo test -p uc_log 2>&1 | tail -3 && cargo test -p uc_node --lib cluster_agent 2>&1 | tail -3 && cargo test -p uc_node --lib obs::metrics 2>&1 | tail -3 && cargo test -p uc_ctl 2>&1 | tail -3 && cargo build --workspace 2>&1 | tail -1`
Expected: all pass; the B1 agent test `an_applied_pin_is_written_to_the_rows_status_line_and_survives_recovery` now asserts `PinRead::Pinned { origin: 4096, from: 0x0100_0000, to: 0x0101_0000 }` (update it).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs uc_node/src/cluster_agent.rs uc_node/src/obs/metrics.rs uc_ctl/src/main.rs docs/reference/cnc-page.md
git commit -m "cnc: pinned_from at status-line +40 under the pin seqlock; PinRead tri-state reader (plan B2 T1)"
```

---

### Task 2: The `ULTSNAP2` envelope

**Files:**
- Modify: `uc_service/src/snapshots.rs` (module doc, constants, `EnvelopeError`, `decode_/write_/verify_snapshot_envelope`, `SnapshotStore::publish`, tests), `uc_service/src/builder_agent.rs` (`BuilderState.version`, the `publish` call, tests), `uc_service/src/lib.rs` (`BuilderState { version: S::VERSION, .. }`), `uc_service/src/replay.rs:243` (call shape only — the cross-check is Task 3), `uc_diffreplay/src/drive.rs` (`install_from`, `Trace.artifact_version`), `uc_diffreplay/src/trace.rs`, `uc_node/src/backup.rs:~918-940`, `uc_ctl/src/snapshot.rs:36` (doc), `examples/kv/tests/cluster.rs:176-180`, `examples/uc_adjudicate/src/diverge.rs:9,90-97` + `adapter.rs:141`, `uc_service/tests/reconstruction.rs:531,595,985`, `fuzz/fuzz_targets/uc_service_snapshot_envelope.rs`
- Test: `uc_service/src/snapshots.rs` tests; the fuzz target builds

**Interfaces:**
- Produces, in `uc_service::snapshots`:
  ```rust
  pub const SNAPSHOT_ENVELOPE_LEN: usize = 24;
  pub const SNAPSHOT_ENVELOPE_MAGIC: &[u8; 8] = b"ULTSNAP2";
  pub const SNAPSHOT_ENVELOPE_MAGIC_V1: &[u8; 8] = b"ULTSNAP1";   // refused by name
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct Envelope { pub position: u64, pub version: u32 }
  pub enum EnvelopeError { Short(usize), BadMagic([u8; 8]), Legacy, Mistagged { built: u64, presented: u64 }, VersionMismatch { built: u32, expected: u32 } }
  pub fn decode_snapshot_envelope(bytes: &[u8]) -> Result<Envelope, EnvelopeError>;
  pub fn write_snapshot_envelope(dst: &mut dyn Write, pos: u64, version: u32) -> io::Result<()>;
  pub fn verify_snapshot_envelope(src: &mut dyn Read, expected: u64, expected_version: Option<u32>) -> Result<Envelope, EnvelopeError>;
  impl SnapshotStore { pub fn publish(&self, pos: u64, version: u32, write: impl FnOnce(&mut dyn Write) -> Result<(), SnapshotError>) -> Result<PathBuf, SnapshotError>; }
  ```
- `Trace` gains `pub artifact_version: Option<u32>` (the installed artifact's stamp; `None` for `Origin::Genesis`).

- [ ] **Step 1: Write the failing tests** (in `snapshots.rs`'s test module; keep every existing test, updating call shapes)

```rust
    #[test]
    fn envelope_v2_layout_is_frozen() {
        let mut v = Vec::new();
        write_snapshot_envelope(&mut v, 4096, 0x0102_0003).unwrap();
        assert_eq!(v.len(), SNAPSHOT_ENVELOPE_LEN);
        assert_eq!(SNAPSHOT_ENVELOPE_LEN, 24);
        assert_eq!(&v[..8], b"ULTSNAP2");
        assert_eq!(&v[8..16], &4096u64.to_le_bytes());
        assert_eq!(&v[16..20], &0x0102_0003u32.to_le_bytes());
        assert_eq!(&v[20..24], &[0, 0, 0, 0], "reserved written as zero");
        assert_eq!(decode_snapshot_envelope(&v), Ok(Envelope { position: 4096, version: 0x0102_0003 }));
    }

    #[test]
    fn a_v1_envelope_is_refused_by_name_and_other_refusals_are_unchanged() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(b"ULTSNAP1");
        v1.extend_from_slice(&4096u64.to_le_bytes());
        v1.extend_from_slice(&[0; 8]);
        assert_eq!(decode_snapshot_envelope(&v1), Err(EnvelopeError::Legacy));
        assert_eq!(decode_snapshot_envelope(&v1[..12]), Err(EnvelopeError::Short(12)));
        let mut junk = v1.clone();
        junk[..8].copy_from_slice(b"NOTASNAP");
        assert!(matches!(decode_snapshot_envelope(&junk), Err(EnvelopeError::BadMagic(_))));
        let mut reserved = Vec::new();
        write_snapshot_envelope(&mut reserved, 1, 1).unwrap();
        reserved[20] = 1;
        assert!(matches!(decode_snapshot_envelope(&reserved), Err(EnvelopeError::BadMagic(_))), "non-zero reserved is not a v2 envelope");
    }

    #[test]
    fn verify_checks_position_always_and_version_only_when_asked() {
        let mut v = Vec::new();
        write_snapshot_envelope(&mut v, 4096, 7).unwrap();
        v.extend_from_slice(b"payload");
        let mut r = &v[..];
        assert_eq!(verify_snapshot_envelope(&mut r, 4096, None), Ok(Envelope { position: 4096, version: 7 }));
        assert_eq!(r, b"payload", "positioned at the payload");
        let mut r = &v[..];
        assert_eq!(verify_snapshot_envelope(&mut r, 4096, Some(7)).map(|e| e.version), Ok(7));
        let mut r = &v[..];
        assert_eq!(verify_snapshot_envelope(&mut r, 4096, Some(8)), Err(EnvelopeError::VersionMismatch { built: 7, expected: 8 }));
        let mut r = &v[..];
        assert_eq!(verify_snapshot_envelope(&mut r, 5000, Some(7)), Err(EnvelopeError::Mistagged { built: 4096, presented: 5000 }), "position is checked before version");
    }

    #[test]
    fn publish_stamps_the_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        store.publish(4096, 0x0100_0000, |w| { w.write_all(b"x")?; Ok(()) }).unwrap();
        let raw = std::fs::read(store.path_for(4096)).unwrap();
        assert_eq!(decode_snapshot_envelope(&raw), Ok(Envelope { position: 4096, version: 0x0100_0000 }));
        assert_eq!(&raw[SNAPSHOT_ENVELOPE_LEN..], b"x");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_service snapshots 2>&1 | tail -6` — Expected: compile errors.

- [ ] **Step 3: Implement**

`snapshots.rs`:

```rust
pub const SNAPSHOT_ENVELOPE_LEN: usize = 24;
/// `2` since plan B2: the envelope now carries the packed `S::VERSION` that
/// BUILT the artifact (spec §9.1). A `ULTSNAP1` file is refused by name —
/// clear `snapshots/<row>/` once when moving to 2.13.0.
pub const SNAPSHOT_ENVELOPE_MAGIC: &[u8; 8] = b"ULTSNAP2";
pub const SNAPSHOT_ENVELOPE_MAGIC_V1: &[u8; 8] = b"ULTSNAP1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    pub position: u64,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    #[error("truncated artifact: {0} bytes, need {len} for the envelope", len = SNAPSHOT_ENVELOPE_LEN)]
    Short(usize),
    #[error("not a UC snapshot artifact: magic {0:02x?}, expected {SNAPSHOT_ENVELOPE_MAGIC:?}")]
    BadMagic([u8; 8]),
    /// A 2.11.0/2.12.0 (`ULTSNAP1`) artifact: it carries no version, so no
    /// install path can cross-check it. Refused; the operator clears the row's
    /// `snapshots/<row>/` once and the next instant rebuilds it.
    #[error("pre-2.13.0 artifact (ULTSNAP1): carries no version stamp — clear snapshots/<row>/ once and take a new instant")]
    Legacy,
    #[error("artifact was built at position {built} but is presented as {presented}")]
    Mistagged { built: u64, presented: u64 },
    /// The artifact was built by a different `VERSION` than the caller
    /// requires: an UNPINNED install by a newer binary (the §2.3
    /// counterfactual, now refused), or a pinned install whose artifact is
    /// not the one the pin's `from` built.
    #[error("artifact was built by version {built:#010x} but {expected:#010x} is required here")]
    VersionMismatch { built: u32, expected: u32 },
}

pub fn decode_snapshot_envelope(bytes: &[u8]) -> Result<Envelope, EnvelopeError> {
    let Some(head) = bytes.get(..SNAPSHOT_ENVELOPE_LEN) else {
        // A v1 header is 16 bytes: name it if the magic says so, even short.
        if bytes.len() >= 8 && &bytes[..8] == SNAPSHOT_ENVELOPE_MAGIC_V1 {
            return Err(EnvelopeError::Legacy);
        }
        return Err(EnvelopeError::Short(bytes.len()));
    };
    let magic: [u8; 8] = head[..8].try_into().expect("8 bytes");
    if &magic == SNAPSHOT_ENVELOPE_MAGIC_V1 {
        return Err(EnvelopeError::Legacy);
    }
    if &magic != SNAPSHOT_ENVELOPE_MAGIC || head[20..24] != [0, 0, 0, 0] {
        return Err(EnvelopeError::BadMagic(magic));
    }
    Ok(Envelope {
        position: u64::from_le_bytes(head[8..16].try_into().expect("8 bytes")),
        version: u32::from_le_bytes(head[16..20].try_into().expect("4 bytes")),
    })
}

pub fn write_snapshot_envelope(dst: &mut dyn Write, pos: u64, version: u32) -> io::Result<()> {
    dst.write_all(SNAPSHOT_ENVELOPE_MAGIC)?;
    dst.write_all(&pos.to_le_bytes())?;
    dst.write_all(&version.to_le_bytes())?;
    dst.write_all(&[0; 4])
}

pub fn verify_snapshot_envelope(src: &mut dyn Read, expected: u64, expected_version: Option<u32>) -> Result<Envelope, EnvelopeError> {
    /* the existing short-read loop over a [0u8; SNAPSHOT_ENVELOPE_LEN] buffer, unchanged */
    let env = decode_snapshot_envelope(&buf[..n])?;
    if env.position != expected {
        return Err(EnvelopeError::Mistagged { built: env.position, presented: expected });
    }
    if let Some(want) = expected_version && env.version != want {
        return Err(EnvelopeError::VersionMismatch { built: env.version, expected: want });
    }
    Ok(env)
}
```

`SnapshotStore::publish(pos, version, write)` writes `write_snapshot_envelope(&mut f, pos, version)`. `BuilderState` gains `pub(crate) version: u32`; `builder_cycle` calls `st.store.publish(pos, st.version, job)`; `lib.rs` sets `version: S::VERSION`; the builder tests set `version: 0` and assert the decoded `Envelope`. `replay.rs:243`: `verify_snapshot_envelope(&mut file, s_pos, None)` for now (Task 3 supplies `Some(S::VERSION)`), binding the returned envelope with `let _env`. `uc_diffreplay::drive::install_from`: `let env = verify_snapshot_envelope(f, position, None)…?; … Ok((got, env.version))` and `install`/`drive` thread `artifact_version` into `Trace` (`Some(env.version)` on `Origin::Artifact`, `None` on `Genesis`); the driver's test helper that writes an envelope passes a version. `backup.rs:918-940`: buffer `[0u8; SNAPSHOT_ENVELOPE_LEN]`, match `Ok(env) if env.position == pos`, and the `Ok(env)` mismatch arm names `env.position`; a `Legacy` error surfaces through the existing `Err(e) => … e.to_string()` arm (so `uc2ctl verify-backup` names a pre-2.13.0 artifact). `uc_ctl/src/snapshot.rs:36` doc: `ULTSNAP2`. `examples/kv/tests/cluster.rs:176-180`: `b"ULTSNAP2"` and the prefix length 24. `examples/uc_adjudicate/src/diverge.rs`: `ENVELOPE = b"ULTSNAP2"`, the slice offset 24, the message "no ULTSNAP2 envelope (pre-2.13.0 artifact or not an artifact)"; `adapter.rs:141` doc. `uc_service/tests/reconstruction.rs:531,595` pass `None`; `:985` matches `Ok(Envelope { position, .. })`. Fuzz target: `verify_snapshot_envelope(&mut src, expected, None)`, and the tail assertion uses the new `SNAPSHOT_ENVELOPE_LEN`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc_service 2>&1 | grep -E "^test result|FAILED" | sort | uniq -c && cargo test -p uc_diffreplay 2>&1 | grep -E "^test result|FAILED" | sort | uniq -c && cargo test -p uc_node --lib backup 2>&1 | tail -3 && cargo test -p kv_store --test cluster 2>&1 | tail -3 && cargo build -p uc_adjudicate 2>&1 | tail -1 && (cd fuzz && cargo +nightly fuzz build uc_service_snapshot_envelope 2>&1 | tail -1)`
Expected: all green (`uc_diffreplay`'s tests need the fixture binaries: `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p kv_store --bin kv-service` first).

- [ ] **Step 5: Commit**

```bash
git add -A uc_service uc_diffreplay uc_node/src/backup.rs uc_ctl/src/snapshot.rs examples/kv/tests/cluster.rs examples/uc_adjudicate fuzz/fuzz_targets/uc_service_snapshot_envelope.rs
git commit -m "snapshots: ULTSNAP2 envelope carries the builder's VERSION; ULTSNAP1 refused by name (plan B2 T2)"
```

---

### Task 3: The gap guard's install cross-checks the version

**Files:**
- Modify: `uc_service/src/replay.rs` (~240-262), `uc_service/src/config.rs` (doc on `MistaggedSnapshot`)
- Test: `uc_service/tests/reconstruction.rs` (a new test beside `fresh_service_below_purge_floor_installs_snapshot_then_tail_replays`)

**Interfaces:**
- Consumes: Task 2's `verify_snapshot_envelope(src, pos, Some(S::VERSION))`.
- Produces: an unpinned gap-guard install of an artifact built by another version is refused as `ServiceError::MistaggedSnapshot { path, source: EnvelopeError::VersionMismatch { .. } }`; obs event `snapshot_installed` (info: `row`, `position`, `version`) on success.

- [ ] **Step 1: Write the failing test**

In `uc_service/tests/reconstruction.rs`, modelled on `fresh_service_below_purge_floor_installs_snapshot_then_tail_replays` (which builds a purged node with a snapshotting `RegisterSm` and re-attaches a fresh one): after the purge, re-attach `DoublingRegisterSm` (`uc_lincheck::register::DoublingRegisterSm`, `VERSION = 2`) instead — WITHOUT a pin — and assert the apply thread poisons with `MistaggedSnapshot` whose source is `VersionMismatch { built: 1, expected: 2 }` (read the poison the way `gap_without_snapshot_capability_fails_stop_with_named_contract` does). Name it `an_unpinned_newer_binary_cannot_install_an_older_versions_artifact`.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p uc_service --test reconstruction an_unpinned_newer 2>&1 | tail -6`
Expected: the service installs the v1 artifact and the assertion on the poison fails (the counterfactual, today's behaviour).

- [ ] **Step 3: Implement**

`replay.rs`, at the install arm:

```rust
                let env = crate::snapshots::verify_snapshot_envelope(&mut file, s_pos, Some(S::VERSION))
                    .map_err(|e| ServiceError::MistaggedSnapshot {
                        path: path.display().to_string(),
                        source: e,
                    })?;
                let installed = (r.install)(&mut guard, s_pos, &mut file)
                    .map_err(|e| ServiceError::Replay(format!("snapshot install: {e}")))?;
                crate::obs_event!(Info, "snapshot_installed", row = ..., position = s_pos, version = env.version as u64);
```

(Use whatever event macro/level `uc_service` already uses for its structured events — grep `obs_event!` in the crate; if it has none, use the crate's existing `eprintln!`-style reporting for reconstruction milestones and say so.) Extend the comment: "Plan B2: an UNPINNED install must be same-version. A newer binary installing an older artifact and tail-replaying under its own `apply` is the §2.3 counterfactual; the pinned path (`attach`) is the sanctioned way across a version boundary and checks against the pin's `from` instead." Update `MistaggedSnapshot`'s doc in `config.rs` to name the version case.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_service --test reconstruction 2>&1 | tail -4` — Expected: all pass, including the existing install test (same-version).

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/replay.rs uc_service/src/config.rs uc_service/tests/reconstruction.rs
git commit -m "uc_service: an unpinned snapshot install must be same-version — the §2.3 counterfactual is refused by name (plan B2 T3)"
```

---

### Task 4: Attach — read the pin, refuse by name, install unconditionally

**Files:**
- Modify: `uc_service/src/attach.rs` (signature; after row discovery ~line 100 and before step 4 ~line 190; `Attached`), `uc_service/src/apply.rs` (`InstallFn` is already `pub(crate)`), `uc_service/src/config.rs` (four variants), `uc_service/src/lib.rs` (`start` passes `None`, `start_with_snapshots` passes `Some(install)` and reuses the returned one)
- Create: `uc_service/tests/pinned_attach.rs`
- Test: the new suite

**Interfaces:**
- `pub(crate) fn attach<S: RawStateMachine>(cfg, sm, install: Option<InstallFn<S>>) -> Result<Attached<S>, ServiceError>`; `Attached` gains `pub(crate) install: Option<InstallFn<S>>` (handed back) and `pub(crate) pin: Option<(u64, u32, u32)>` (origin, from, to — `Some` only when installed).
- `ServiceError` gains:
  ```rust
  #[error("FSM {name:?} at row {row} is pinned to version {pinned:#010x} from origin {origin}, but this binary is {mine:#010x}; a stale binary cannot rejoin after `uc2ctl upgrade pin`")]
  PinnedVersionMismatch { name: String, row: u8, origin: u64, pinned: u32, mine: u32 },
  #[error("row {row}'s pin words could not be read consistently (the uc2-cluster agent is mid-publish); retry the attach")]
  PinUnreadable { row: u8 },
  #[error("FSM {name:?} at row {row} is pinned to origin {origin} but was started with start(); a pinned row must install snap-{origin} and needs start_with_snapshots()")]
  PinRequiresSnapshots { name: String, row: u8, origin: u64 },
  #[error("row {row} is pinned to origin {origin} but {path} does not exist on this node — the set at the origin was pruned or never fetched; take `uc2ctl snapshot fetch` or re-pin at a retained instant")]
  PinnedArtifactMissing { row: u8, origin: u64, path: String },
  ```

- [ ] **Step 1: Write the failing tests** — `uc_service/tests/pinned_attach.rs`

Build on `uc_service/tests/reconstruction.rs`'s helpers (`start_single_node_with_buffer`, `cfg`, `open_cnc`, `write_reg`, `query_reg`, `command_instant`, `wait_service_caught_up` — copy the ones you need into this file or a shared `tests/common` module; do not `include!` another test file). The pin words are written DIRECTLY on the page in these tests (`cnc.service_slot(0).status.store_pin(origin, from, to)`) — the node's own agent never pins here, so the single-writer rule holds and no admin request is needed. Each test: one node (`PurgePolicy::Disabled`, a small ring so the counterfactual is reachable — see `uc_diffreplay/tests/reconstruction.rs` for `BUFFER_BYTES`/`SEGMENT_BYTES`), `RegisterSm` (v1, `VERSION = 1`) writes `0..5`, `command_instant` → P, wait for `snap-<P>.ultsnap`, stop the service.

```rust
#[test]
fn a_stale_binary_is_refused_by_name_after_the_pin() {
    // pin row 0: from = 1 (RegisterSm), to = 2 (DoublingRegisterSm), origin = P
    cnc.service_slot(0).status.store_pin(p, 1, 2);
    let err = ServiceBuilder::new(cfg(dir, app), RegisterSm::default()).start_with_snapshots().err().expect("refused");
    assert!(matches!(err, ServiceError::PinnedVersionMismatch { row: 0, pinned: 2, mine: 1, .. }), "{err}");
    assert_eq!(cnc.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_ATTACHED, 0, "nothing was written to the slot");
}

#[test]
fn the_pinned_version_installs_the_origin_unconditionally_and_recomputes_the_tail() {
    // more writes AFTER P so the tail is non-empty, then pin and swap.
    cnc.service_slot(0).status.store_pin(p, 1, 2);
    let svc2 = ServiceBuilder::new(cfg(dir, app), DoublingRegisterSm::default()).start_with_snapshots().unwrap();
    wait_service_caught_up(&cnc);
    // v1's state at P (Some(4)), then the post-P writes applied under v2's doubling apply.
    assert_eq!(query_reg2(&svc2), Some(2 * LAST_WRITE_AFTER_P));
    // and the artifact really was installed: `applied` was published at P before the tail
    // (observe via the obs event or by checking the SM's last_applied through a query if exposed)
}

#[test]
fn a_durable_sm_above_the_origin_is_rewound_to_it() {
    // A persistent SM stand-in: install v2 with a `DoublingRegisterSm` whose `last_applied()` is
    // preset ABOVE P (construct it with the v1 tail already applied, e.g. by running v2 unpinned to the
    // end first, stopping it, THEN pinning and re-attaching the same SM instance — `Service::stop`
    // returns/keeps the SM? if not, drive the SM through `uc_diffreplay`-style in-process replay to X > P
    // and hand THAT instance to the builder). Assert the value after reattach equals the pinned-path
    // value from the previous test, not the unpinned one.
}

#[test]
fn a_pinned_row_started_without_snapshots_is_refused() {
    cnc.service_slot(0).status.store_pin(p, 1, 2);
    let err = ServiceBuilder::new(cfg(dir, app), DoublingRegisterSm::default()).start().err().unwrap();
    assert!(matches!(err, ServiceError::PinRequiresSnapshots { row: 0, .. }), "{err}");
}

#[test]
fn a_pinned_origin_with_no_artifact_is_refused() {
    std::fs::remove_file(dir.join("snapshots/0").join(format!("snap-{p}.ultsnap"))).unwrap();
    cnc.service_slot(0).status.store_pin(p, 1, 2);
    let err = ServiceBuilder::new(cfg(dir, app), DoublingRegisterSm::default()).start_with_snapshots().err().unwrap();
    assert!(matches!(err, ServiceError::PinnedArtifactMissing { row: 0, .. }), "{err}");
}

#[test]
fn a_pinned_artifact_built_by_the_wrong_version_is_refused() {
    // pin says from = 3, but snap-<P> was built by version 1
    cnc.service_slot(0).status.store_pin(p, 3, 2);
    let err = ServiceBuilder::new(cfg(dir, app), DoublingRegisterSm::default()).start_with_snapshots().err().unwrap();
    assert!(matches!(err, ServiceError::MistaggedSnapshot { source: EnvelopeError::VersionMismatch { built: 1, expected: 3 }, .. }), "{err}");
}

#[test]
fn a_contended_pin_read_is_refused_not_ignored() {
    cnc.service_slot(0).status.store_pin_begin_for_test(2, 1); // needs a pub #[doc(hidden)] variant reachable from an integration test
    let err = ServiceBuilder::new(cfg(dir, app), DoublingRegisterSm::default()).start_with_snapshots().err().unwrap();
    assert!(matches!(err, ServiceError::PinUnreadable { row: 0 }), "{err}");
}
```

For the third test, read `uc_service/src/lib.rs`'s `Service::stop` to see whether the SM comes back; if it does not, build the durable shape the way the sketch's second alternative says. For the last test, the in-flight helper must be reachable from an integration test: make `store_pin_begin_for_test` `pub` + `#[doc(hidden)]` (the pattern `take_sched_records_for_test` used), NOT `#[cfg(test)]`. `query_reg2` is `query_reg` for the doubling type.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_service --test pinned_attach 2>&1 | tail -12`
Expected: compile errors on the new variants; then, once they compile, the refusal tests fail (attach succeeds today).

- [ ] **Step 3: Implement**

`attach.rs`, immediately after `row` is known and `declared`/`lag_mode` computed, BEFORE the lock (so a refused attach never takes `service.<row>.lock` — check whether the lock should come first to serialise with a concurrent legit attach; the lock is per-row exclusive and released on drop, so taking it first and returning the error is also fine — choose taking the lock first, then the pin decision, so two racing attaches cannot both pass the pin read; document the order):

```rust
    // Plan B2 (spec §3 S4 steps 4-5): the row's pin, read through the
    // seqlock reader ONLY. Decided BEFORE any slot word is written.
    let s = slot(&cnc, row);
    let pin = match s.status.pin() {
        PinRead::NoPin => None,
        PinRead::Contended => return Err(ServiceError::PinUnreadable { row }),
        PinRead::Pinned { origin, from, to } => {
            if to != S::VERSION {
                return Err(ServiceError::PinnedVersionMismatch {
                    name: S::IDENTITY.name.as_str().to_string(), row, origin, pinned: to, mine: S::VERSION,
                });
            }
            Some((origin, from, to))
        }
    };
    // Unconditional install (step 4): the artifact at the origin, built by
    // the pin's `from`, replaces whatever state this SM holds — a durable SM
    // above the origin is rewound and recomputes the tail under THIS version,
    // exactly as its fresh peers do.
    let mut sm = sm;
    if let Some((origin, from, _)) = pin {
        let Some(install_fn) = install.as_ref() else {
            return Err(ServiceError::PinRequiresSnapshots { name: ..., row, origin });
        };
        let store = SnapshotStore::open(dir, row)?;
        let path = store.path_for(origin);
        let mut file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ServiceError::PinnedArtifactMissing { row, origin, path: path.display().to_string() });
            }
            Err(e) => return Err(e.into()),
        };
        let env = crate::snapshots::verify_snapshot_envelope(&mut file, origin, Some(from))
            .map_err(|e| ServiceError::MistaggedSnapshot { path: path.display().to_string(), source: e })?;
        let installed = install_fn(&mut sm, origin, &mut file)
            .map_err(|e| ServiceError::Replay(format!("pinned install at {origin}: {e}")))?;
        if installed != origin || sm.last_applied() >= Some(origin) {
            return Err(ServiceError::Replay(format!(
                "pinned install at {origin} left the state machine at {:?} (returned {installed}); \
                 install_snapshot must land at the tag with its cursor strictly below it", sm.last_applied())));
        }
        // obs: pinned_install { row, origin, from, to, artifact_version = env.version }
    }
```

Then the existing step 4 (`last_applied`/drift/`applied` store) runs on the installed SM — `start_pos` is therefore `sm.last_applied()` = the artifact's cursor below `origin`, and the idempotent-skip re-walks up to it; the drift bound still holds (`origin ≤ durable`, the set was committed by construction). `snapshot_capable` = `install.is_some()`. `Attached { install, pin, .. }`. `lib.rs`: `start` → `attach(&cfg, sm, None)`; `start_with_snapshots` → build `install` first, `attach(&cfg, sm, Some(install))`, take `attached.install.expect(..)` for `SnapshotRestore`. Add the four `ServiceError` variants with the texts above.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p uc_service --test pinned_attach 2>&1 | tail -10 && cargo test -p uc_service 2>&1 | grep -E "^test result|FAILED" | sort | uniq -c && cargo clippy -p uc_service --all-targets -- -D warnings 2>&1 | tail -2`
Expected: 7/7 new tests pass; every other suite unchanged.

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/attach.rs uc_service/src/apply.rs uc_service/src/config.rs uc_service/src/lib.rs uc_service/src/snapshots.rs uc_service/tests/pinned_attach.rs
git commit -m "uc_service: pinned install at attach — refuse a stale binary, an unreadable pin, a non-installing start, a missing or wrong-version origin; install unconditionally otherwise (plan B2 T4)"
```

---

### Task 5: The end-to-end proof through a real pin

**Files:**
- Modify: `uc_diffreplay/tests/reconstruction.rs` (`v2_after_swap` gains a `pinned: bool` arm; a new test), `uc_diffreplay/tests/common/mod.rs` (`pin_row`)
- Test: that file

**Interfaces:**
- `common::pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64) -> AdminResp`: stage `upgrade.pending` with `uc_protocol::v2::upgrade::encode_upgrade_pin`, compute `uc_node::staged_digest`, send admin op `ADMIN_OP_UPGRADE_PIN` through `cnc.write_admin_req`/`read_admin_resp` exactly as `uc_node/tests/reconfig.rs:169`'s `admin_request` does (the test node's admin policy is filesystem, so no auth line), assert `status == 0`.

- [ ] **Step 1: Write the failing test**

Extend `v2_after_swap(purge, app_id)` to `v2_after_swap(purge, pinned: bool, app_id)`: after the v1 service stops and before the swap, when `pinned`: `common::pin_row(dir, &cnc, 0, 1, 2, p)`, then wait until `cnc.service_slot(0).status.pin() == PinRead::Pinned { origin: p, from: 1, to: 2 }` (the agent republishes at commit). Add:

```rust
/// Spec §3 S4 end to end: with purge DISABLED (the shipped default) the
/// unpinned swap computes the counterfactual `Some(8)` (plan A's finding);
/// the same swap under a real `uc2ctl upgrade pin` installs v1's artifact
/// at P and answers v1's true state `Some(4)`.
#[test]
fn a_real_pin_makes_the_default_purge_off_swap_install_the_origin() {
    assert_eq!(v2_after_swap(uc_node::PurgePolicy::Disabled, false, "pin-off"), Some(8));
    assert_eq!(v2_after_swap(uc_node::PurgePolicy::Disabled, true, "pin-on"), Some(4));
}
```

Also assert, in the pinned arm, that the v1 service's own re-attach after the pin fails with `PinnedVersionMismatch` (one extra `ServiceBuilder::new(.., RegisterSm::default()).start_with_snapshots()` between the pin and the swap).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_diffreplay --test reconstruction a_real_pin 2>&1 | tail -8` — Expected: the pinned arm answers `Some(8)` until Task 4's code is present (it is — so this test should PASS once written if Tasks 1–4 are committed; verify the RED by temporarily asserting `Some(9)` and watching it fail, then restore — say in the report that you did).

- [ ] **Step 3–4: Implement the helper, run**

Run: `cargo test -p uc_diffreplay --test reconstruction 2>&1 | tail -6` — Expected: all pass (the plan-A tests included; they use the `Origin::Artifact` install with `expected_version = None`).

- [ ] **Step 5: Commit**

```bash
git add uc_diffreplay/tests
git commit -m "uc_diffreplay: end-to-end — a real upgrade pin makes the purge-off swap install the origin (Some(4), not Some(8)) (plan B2 T5)"
```

---

### Task 6: The plan-A carries — sched accessor, `ids()` call count, `on_committed` recorder

**Files:**
- Modify: `uc_service/src/traits.rs` (`take_sched_records` public; `ids_calls`), `uc_service/tests/timed.rs:107,117`, `uc_diffreplay/src/drive.rs` (`sched_of`; `ids_calls`; `DriveOptions` with an output handler), `uc_diffreplay/src/trace.rs` (`Entry.ids_calls: u32`, `Entry.output: Option<String>`), `uc_diffreplay/src/diff.rs` (surfaces `ids` and `output`), `uc_diffreplay/Cargo.toml` (`tokio` with `rt` if not already transitively enabled), `uc_diffreplay/README.md` + `docs/how-to/diff-replay.md` ("What this does not compare" shrinks)
- Test: `uc_service` (traits unit tests), `uc_diffreplay/tests/drive.rs`, `diff_attribute_confirm.rs`

**Interfaces:**
- `impl ApplyCtx { pub fn take_sched_records(&mut self) -> Vec<SchedRecord>; pub fn ids_calls(&self) -> u32; }` — `take_sched_records_for_test` removed (both callers updated). `ids()` increments a `u32` on `ApplyCtx` (reset in `rebind`).
- `uc_diffreplay::drive::DriveOptions<S> { pub output: Option<Box<dyn RawOutputHandler<S>>> }` and `pub fn drive_with<S>(sm, corpus, origin, opts: DriveOptions<S>) -> Trace` (`drive` = `drive_with(.., DriveOptions::default())`). Per applied MESSAGE frame the driver calls `handler.on_committed(pos, cmd, &sm)` on a `tokio` current-thread runtime and records `Entry.output = Some("ok" | "retryable: <msg>" | "permanent: <msg>")`; `None` without a handler.
- `diff`: `Surface::Ids` compares `ids_calls`; `Surface::Output` compares `output`.

- [ ] **Step 1: Write the failing tests**

`uc_service/src/traits.rs` tests: `ids_calls_counts_generator_requests_per_apply` — `ApplyCtx::new(..)`, `ctx.ids()` twice → `ids_calls() == 2`; after `rebind` → 0. `uc_diffreplay/tests/drive.rs`: a corpus whose SM calls `ctx.ids()` once per `Write` produces entries with `ids_calls == 1`; a recording handler (a test type implementing `RawOutputHandler<S>` that returns `Ok` for even positions and `Err(OutputError::Permanent(..))` for odd) yields the matching `output` strings; without a handler `output` is `None`. `diff_attribute_confirm.rs`: two traces differing only in `ids_calls` produce a `Surface::Ids` finding; only in `output`, a `Surface::Output` finding.

- [ ] **Step 2–4: fail, implement, pass**

`ids()` takes `&self` today; make it `&mut self`? — `ApplyCtx::ids(&self) -> IdGen` is a public SDK method; changing to `&mut self` breaks no caller (every caller holds `&mut ApplyCtx`) but grep `examples/`, `uc_lincheck`, `testing/` and update them. Alternative: a `Cell<u32>`; prefer the `&mut self` change (no interior mutability on the hot path) and say which you chose.

Run: `cargo test -p uc_service traits 2>&1 | tail -3 && cargo test -p uc_service --test timed 2>&1 | tail -3 && cargo test -p uc_diffreplay 2>&1 | grep -E "^test result|FAILED" | sort | uniq -c && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1`

- [ ] **Step 5: Commit**

```bash
git add uc_service uc_diffreplay docs/how-to/diff-replay.md
git commit -m "uc_service + uc_diffreplay: public take_sched_records, ids() call count and an on_committed recorder — the three plan-A trace carries (plan B2 T6)"
```

---

### Task 7: Docs, the spec's errata, the proof stack

**Files:**
- Modify: `docs/reference/state-machine-contract.md` (§ "Snapshots: the instant, the envelope…" ~142-192: 24-byte `ULTSNAP2 ‖ P ‖ version`, the two cross-checks, the pinned install at attach and its refusals), `docs/reference/instance-directory.md` (the envelope section; `ULTSNAP1` → refused), `docs/reference/cnc-page.md` (done in T1 — verify), `docs/reference/limits.md`, `docs/reference/uc2ctl.md` (`status` prints `pinned_from=`; `verify-backup` names a legacy artifact), `docs/how-to/upgrade-a-cluster.md` (the 2.13.0 section: replace the OPEN marker with the step "clear `snapshots/<row>/` on every node once — `ULTSNAP1` artifacts are refused by name; `snapshots/cluster/` is untouched" and the S4 sequence `snapshot → pin → stop/swap/start`, what each refusal means), `docs/how-to/back-up-a-cluster.md` (verify-backup's legacy refusal), `docs/how-to/diff-replay.md` (T6 did the surfaces; add `artifact_version`), `docs/how-to/monitor-a-cluster.md` (events `pinned_install`, `snapshot_installed`; gauge HELP "0 = no pin or unreadable"), `docs/notes/uc2-cluster-fsm-explained.md` ("Pins and reports": the fourth word, tri-state, what attach does), `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` (an "Errata (plan B2, as built)" block under §3 S4 with the six errata above; §9.1's one-liner noting the two cross-checks), `docs/VERIFICATION.md` (the `pinned_attach` suite and the e2e test), `fuzz/README.md` (the envelope target's description)
- Test: the proof stack

- [ ] **Step 1: The sweep**

`grep -rn "ULTSNAP1\|16-byte\|16 B envelope\|SNAPSHOT_ENVELOPE_LEN" docs README.md QUICKSTART.md examples/*/README.md` — rewrite every current-state hit (history stays). The `upgrade-a-cluster.md` OPEN marker must be gone.

- [ ] **Step 2: The proof stack** (paste every tail)

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings
cargo clippy -p uc_lincheck --features replay-bin --all-targets -- -D warnings
cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings
cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings
cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p kv_store --bin kv-service
cargo test --workspace 2>&1 | grep -E "^test result|FAILED|panicked" | sort | uniq -c
cargo test -p uc_node --test lin_v2
cargo test -p uc_crashtest --features hard-crash-tests
(cd fuzz && cargo +nightly fuzz build)
```

The crashtest suite is in the list because the crashtest service uses `start_with_snapshots` and real `kill -9` restarts — the attach path changed. A known flake (`uc_node` log-sink capture) is re-run alone and reported both ways.

- [ ] **Step 3: Commit**

```bash
git add docs fuzz/README.md
git commit -m "docs: ULTSNAP2, pinned install at attach and its refusals, the 2.13.0 snapshots/<row>/ wipe step, spec errata as built (plan B2 T7)"
```

---

## Self-review

**Spec coverage.** S4 step 4 (unconditional install, durable rewind): T4 + T5. S4 step 5 (attach refusal): T4 + T5's re-attach arm. §9.1 (2) (`ULTSNAP2`, pre-envelope refused, install cross-check): T2 + T3 (unpinned) + T4 (pinned, against `from` — erratum 1/4). §2.3 counterfactual: T3 turns it into a named refusal on the unpinned path; T5 demonstrates the pinned path answering v1's state on the default purge policy. §6.2 part 2 (the CLI mode verifying refusal/empty/durable shapes): plan C — the mechanisms and their service-level tests land here. B1 carries (tri-state `pin()`, readers via `pin()`): T1 + T4. Plan-A carries: T6. §6.5.2 (live hashes): plan B3.

**Placeholder scan.** T1 and T4 tests say "the heap page the neighbouring tests build" / "copy the helpers you need" — each names the exact source test. T4's durable-rewind test gives two constructions and tells the implementer to read `Service::stop` to pick one; T5's RED instruction is explicit. T3's event macro: the implementer greps and reports. No "TBD"/"similar to".

**Type consistency.** `store_pin(origin: u64, from: u32, to: u32)` (T1) ↔ `store_pin(p.origin, p.from, p.to)` (T1 agent) ↔ tests `store_pin(p, 1, 2)` (T4/T5). `PinRead::Pinned { origin, from, to }` everywhere. `verify_snapshot_envelope(src, expected: u64, expected_version: Option<u32>) -> Result<Envelope, _>` (T2) ↔ `Some(S::VERSION)` (T3) ↔ `Some(from)` (T4) ↔ `None` (driver). `publish(pos, version, write)` (T2) ↔ `st.store.publish(pos, st.version, job)`. `attach(cfg, sm, install: Option<InstallFn<S>>)` ↔ `start` passes `None`, `start_with_snapshots` passes `Some`. `Entry.ids_calls: u32` / `Entry.output: Option<String>` (T6) ↔ `Surface::Ids` / `Surface::Output`.
