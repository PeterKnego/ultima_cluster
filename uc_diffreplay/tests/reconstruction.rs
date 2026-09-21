// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The harness's first teeth-check (spec §6.2 part 1): the §2.3
//! counterfactual, demonstrated twice.
//!
//! §2.3 claims that after a binary swap a service that replays the journal
//! from GENESIS under the new build computes a state that never existed,
//! while one that installs the pinned artifact carries the true history.
//! The claim was derived from reading code; nothing demonstrated it. These
//! tests do, at both levels:
//!
//! * [`genesis_to_p_under_v2_is_not_v1s_state_at_p`] — the driver level, via
//!   the `register-replay` fixture binary: the same frames `[0, P)`, one run
//!   from the artifact and one from genesis, disagree about the state at P.
//! * [`real_attach_genesis_replay_computes_the_counterfactual_and_install_does_not`]
//!   — UC's OWN reconstruction path (`uc_service::replay`'s gap guard) with
//!   an in-process node: swap `RegisterSm` for `DoublingRegisterSm` on a live
//!   instance dir and read the register back. UNPINNED, that swap either
//!   computes the counterfactual (genesis replay) or is refused by name
//!   (the gap guard's same-version rule, plan B2 T3).
//! * [`a_real_pin_makes_the_default_purge_off_swap_install_the_origin`] — the
//!   same swap under a real `uc2ctl upgrade pin`: the origin is installed and
//!   the row carries v1's true history, which is the whole point of the pin.
//!
//! [`a_real_divergence_is_detected_and_attributed`] closes the loop the other
//! way: the diff → attribute → confirm chain over a divergence that is really
//! there, which is the only end-to-end evidence that the harness reports a
//! genuine change rather than only a declared-but-absent one.

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, DoublingRegisterSm, RegisterSm};
use uc_log::cnc::{CncPage, PinRead};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

use common::{register_replay_bin, wait_for};
use uc_diffreplay::attribute::{Attribution, Declaration, attribute};
use uc_diffreplay::confirm::{Verdict, confirm};
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::diff::{Surface, diff};
use uc_diffreplay::trace::Trace;

/// The reattach experiments start a real node, purge a journal prefix and
/// wait for a v2 service to walk it: 30 s, not [`common::wait_until`]'s 10,
/// and non-panicking so the `Stop` guards run before the assertion.
const REATTACH_TIMEOUT: Duration = Duration::from_secs(30);

/// `<bin> replay --corpus … --out …` with the given knobs, then read the
/// trace back.
fn replay(corpus: &Path, out: &Path, double: bool, from_genesis: bool) -> Trace {
    let mut c = Command::new(register_replay_bin());
    c.arg("replay")
        .arg("--corpus")
        .arg(corpus)
        .arg("--out")
        .arg(out);
    if double {
        c.arg("--double");
    }
    if from_genesis {
        c.arg("--from-genesis");
    }
    let st = c.status().unwrap();
    assert!(st.success(), "register-replay failed: {st}");
    Trace::read_json(std::fs::File::open(out).unwrap()).unwrap()
}

// ------------------------------------------------------- 1. driver level

/// Spec §2.3 at the driver level. `RegisterSm` (v1) writes `0..5`, so its
/// artifact at **P** holds `value=Some(4)`. `DoublingRegisterSm` (v2, same
/// row, `Write(v)` stores `2·v`) replaying the SAME frames `[0, P)` from
/// genesis holds `value=Some(8)` — a state that never existed on any replica.
///
/// The two runs are compared at the SAME position on purpose: the corpus's
/// `end` is P itself, so the genesis run stops exactly where the artifact was
/// taken. (Comparing at the end of a longer span would not show it — the
/// register keeps only the last write, and `2·7` is `2·7` either way.)
#[test]
fn genesis_to_p_under_v2_is_not_v1s_state_at_p() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "rc2", 5, 0);
    let c = Corpus::export(inst.path(), "rc2", 0, p, p, 0, out.path()).unwrap();

    let from_genesis = replay(&c.dir, &out.path().join("gen.json"), true, true);
    let at_end = from_genesis.projection_at_end.as_deref().unwrap();
    assert!(
        at_end.starts_with("value=Some(8)\n"),
        "v2 from genesis must hold the doubled counterfactual, got {at_end:?}"
    );
    // The genesis run installs no artifact, so it has no origin projection —
    // the artifact's own state is read directly instead.
    assert!(
        from_genesis.projection_at_origin.is_none(),
        "a genesis run installs nothing, so it has no origin projection: {:?}",
        from_genesis.projection_at_origin
    );
    assert_eq!(from_genesis.origin, 0);

    let art = uc_diffreplay::drive::project_artifact(RegisterSm::default(), &c.artifact(), p)
        .expect("project v1's artifact at P");
    assert!(
        art.starts_with("value=Some(4)\n"),
        "v1's artifact at P must hold the true history, got {art:?}"
    );
}

// ------------------------------- 2. a real divergence, named and confirmed

/// The other half of the teeth-check: a divergence that is really there is
/// observed, attributed to a touched arm, and confirmed against the
/// declaration — `Pass`, with nothing `Absent` and nothing `Unexplained`.
///
/// Both runs start from the SAME artifact (`Origin::Artifact`), so the only
/// thing that differs is what the two builds do with the tail above P.
/// `RegisterSm` ends at the last written value, `DoublingRegisterSm` at twice
/// it, and the `projection_end` surface carries the difference.
///
/// `Cmd::Write`'s responses do NOT diverge — both builds ack a write the same
/// way — so the declaration expects only `projection_end`, and the profile is
/// asserted to carry no per-entry divergence at all.
#[test]
fn a_real_divergence_is_detected_and_attributed() {
    let inst = common::tempdir();
    let out = common::tempdir();
    // 5 writes, an instant at P, then 3 more: the tail above P is what the
    // two builds disagree about.
    let (p, _) = common::build_register_history(inst.path(), "rdiv", 5, 3);
    let c = Corpus::export(inst.path(), "rdiv", 0, p, u64::MAX, 0, out.path()).unwrap();

    let a = replay(&c.dir, &out.path().join("v1.json"), false, false);
    let b = replay(&c.dir, &out.path().join("v2.json"), true, false);

    // Both installed the artifact at P, so both have an origin projection and
    // the two agree there: the change is in `apply`, not in the image.
    assert_eq!(a.projection_at_origin, b.projection_at_origin);
    assert!(
        a.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(7)\n"),
        "{:?}",
        a.projection_at_end
    );
    assert!(
        b.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(14)\n"),
        "{:?}",
        b.projection_at_end
    );
    // Both walked the same three tail frames.
    assert_eq!(a.entries.len(), 3, "{:?}", a.entries);
    assert_eq!(b.entries.len(), 3, "{:?}", b.entries);

    // The declaration's `"00"` tag is the leading byte of an encoded
    // `Cmd::Write`, in the codec the client actually submits with
    // (`uc_client::pipelined`'s bincode-standard) — pinned here rather than
    // assumed.
    let encoded = bincode::serde::encode_to_vec(Cmd::Write(1), bincode::config::standard())
        .expect("encode Cmd::Write");
    assert_eq!(encoded[0], 0x00, "Cmd::Write's tag byte: {encoded:?}");
    for e in &a.entries {
        assert_eq!(e.tag.first(), Some(&0x00), "traced tag: {:?}", e.tag);
    }

    let profile = diff(&a, &b).unwrap();
    // The responses are identical (`CmdResp::WriteAck` either way) and neither
    // build schedules, so the ONLY surface that diverges is the end state.
    assert!(
        profile.entries.is_empty(),
        "no per-entry divergence expected, got {:?}",
        profile.entries
    );
    assert!(
        profile.projection_origin.is_empty(),
        "both installed the same artifact, so the origin states must match: {:?}",
        profile.projection_origin
    );
    assert!(
        !profile.projection_end.is_empty(),
        "the end states must differ (Some(7) vs Some(14))"
    );
    assert!(
        profile.only_in_a.is_empty() && profile.only_in_b.is_empty(),
        "both builds walked the same positions: only_in_a={:?} only_in_b={:?}",
        profile.only_in_a,
        profile.only_in_b
    );

    let decl = Declaration::from_toml(
        "[tags]\n\
         \"00\" = \"write\"\n\
         [touched]\n\
         arms = [\"write\"]\n\
         [[expect]]\n\
         surface = \"projection_end\"\n\
         note = \"values doubled\"\n",
    )
    .unwrap();
    let att = attribute(&profile, &decl);
    assert!(
        att.entries.is_empty(),
        "nothing per-entry to attribute: {:?}",
        att.entries
    );
    assert_eq!(att.projection_origin, None);
    // A projection is one comparison over the whole state: it is attributed
    // to the change's touched set as a whole, never to one arm of it.
    assert_eq!(att.projection_end, Some(Attribution::Touched));

    let v = confirm(&att, &decl);
    assert_eq!(v.findings.len(), 1, "{:?}", v.findings);
    let f = &v.findings[0];
    assert_eq!(f.surface, Surface::ProjectionEnd);
    assert_eq!(f.verdict, Verdict::Pass);
    // …and the finding names no arm, matching the arm-less `[[expect]]`.
    assert_eq!(f.arm, None);
    assert_eq!(f.note, "values doubled");
    assert!(
        !v.findings
            .iter()
            .any(|f| matches!(f.verdict, Verdict::Absent | Verdict::Unexplained)),
        "{:?}",
        v.findings
    );
    assert!(
        !v.failed(),
        "a declared, observed, attributed change must not fail: {:?}",
        v.findings
    );
}

// --------------------------------------------------- 3. the real attach path

/// Stop `T` when the binding goes out of scope, however it goes out of scope.
/// A failed wait in [`v2_after_swap`] must not leave a busy-spinning node or a
/// service thread behind for the rest of this binary, and `Node::stop` /
/// `Service::stop` both consume `self`, so neither can be called from a plain
/// `Drop` impl on the value itself.
struct Stop<T>(Option<T>, fn(T));

impl<T> Stop<T> {
    fn new(v: T, stop: fn(T)) -> Stop<T> {
        Stop(Some(v), stop)
    }
    fn get(&self) -> &T {
        self.0.as_ref().expect("live")
    }
}

impl<T> Drop for Stop<T> {
    fn drop(&mut self) {
        if let Some(v) = self.0.take() {
            (self.1)(v);
        }
    }
}

/// Writes submitted in the v1 era. Two jobs, both structural:
///
/// * **scroll the ring.** The reattaching service starts at cursor 0, and
///   `LogFollower` only reports `Overrun` — the one door into
///   `uc_service::replay` — once the appender is more than `buffer_bytes`
///   ahead of it. Below that it reads the live buffer from 0 and replays from
///   genesis no matter what the journal looks like, so BOTH arms would be the
///   counterfactual and the test would prove nothing.
/// * **roll journal segments.** `Journal::purge_before` drops whole
///   non-active segments, so a purge only moves `first_meta()` off 0 when
///   several segments lie below the instant.
///
/// At 2 payload bytes per `Cmd::Write` each frame occupies the 64-byte
/// minimum slot, so this is ~128 KiB of log against a 64 KiB ring and 16 KiB
/// segments.
const WRITES: u64 = 2000;
/// Ring capacity for the swap test — small on purpose (see [`WRITES`]).
const BUFFER_BYTES: usize = 1 << 16;
/// Journal segment size — small on purpose (see [`WRITES`]).
const SEGMENT_BYTES: u64 = 16 * 1024;
/// The values written cycle `0..MODULUS`, so the LAST one is fixed and the
/// register's state at P is known without counting frames.
const MODULUS: u64 = 5;
/// v1's register at P, and the value the artifact carries.
const LAST_WRITE: u64 = (WRITES - 1) % MODULUS;

/// `RegisterSm` takes the trait's default `VERSION`; `DoublingRegisterSm`
/// declares 2. Read from the types rather than written as literals, so a
/// change to either is a compile-time relocation and not a silently wrong
/// pin.
const V1: u32 = <RegisterSm as StateMachine>::VERSION;
const V2: u32 = <DoublingRegisterSm as StateMachine>::VERSION;

/// What the swapped-in v2 service did with the instance dir.
#[derive(Debug, PartialEq, Eq)]
enum Swap {
    /// It reconstructed up to P, and answers this.
    CaughtUp(Option<u64>),
    /// Its published applied frontier never reached P within
    /// [`REATTACH_TIMEOUT`] — what a fail-stopped apply thread looks like
    /// from outside the service.
    Stalled,
}

/// v1 (`RegisterSm`) writes, takes a coordinated instant at **P**, and stops.
/// Then v2 (`DoublingRegisterSm`, same row name, `Write(v)` stores `2·v`)
/// attaches to the SAME instance dir behind the SAME running node, and the
/// register is read back once it has caught up to P.
///
/// Two knobs, and between them they name every path §2.3 talks about:
///
/// * `purge` — with [`uc_node::PurgePolicy::Disabled`] (the shipped default)
///   the journal still holds `[0, P)` and the gap guard never fires, so an
///   unpinned v2 replays from genesis and computes the counterfactual; with
///   `BelowSnapshot` the prefix below P is gone and the gap guard must find a
///   covering artifact — v1's, which an UNPINNED v2 is refused by name (plan
///   B2 T3).
/// * `pinned` — a real `uc2ctl upgrade pin` (admin op 10, through the cnc
///   admin band and the cluster FSM) between the two eras. The pinned attach
///   installs v1's artifact at P unconditionally, so v2 carries v1's true
///   history instead of recomputing it.
fn swap_to_v2(purge: uc_node::PurgePolicy, pinned: bool, app_id: &str) -> Swap {
    let inst = common::tempdir();
    let dir = inst.path();
    let purging = !matches!(purge, uc_node::PurgePolicy::Disabled);

    // --- v1 era ---
    let mut cfg = common::node_config(dir, app_id, common::register_name());
    cfg.purge = purge;
    cfg.buffer_bytes = BUFFER_BYTES;
    cfg.journal_segment_bytes = SEGMENT_BYTES;
    let node = Stop::new(uc_node::Node::start(cfg).unwrap(), uc_node::Node::stop);
    // Task 2: the first submit races leader election without this and fails
    // with `NotLeader`.
    assert!(
        wait_for(|| node.get().can_serve(), REATTACH_TIMEOUT),
        "node never served"
    );

    let p = {
        let _svc = Stop::new(
            ServiceBuilder::new(
                ServiceConfig::new(dir.to_path_buf(), app_id.to_string()),
                RegisterSm::default(),
            )
            .start_with_snapshots()
            .unwrap(),
            uc_service::Service::<RegisterSm>::stop,
        );
        let client = Stop::new(Client::connect(dir, app_id).unwrap(), Client::shutdown);
        for v in 0..WRITES {
            let _: CmdResp = client.get().submit(&Cmd::Write(v % MODULUS)).unwrap();
        }
        let p = common::command_instant(node.get());
        // The instant completes for this row when its artifact appears.
        let art = dir
            .join("snapshots")
            .join("0")
            .join(format!("snap-{p}.ultsnap"));
        assert!(
            wait_for(|| art.is_file(), REATTACH_TIMEOUT),
            "row 0 never published snap-{p}.ultsnap"
        );
        // The precondition the whole experiment rests on: the appender is more
        // than a ring capacity ahead of position 0, so the reattaching service
        // CANNOT read `[0, …)` out of the live buffer and must go through
        // `uc_service::replay`. Without it both arms replay from genesis and
        // the test proves nothing (measured: at `buffer_bytes = 1 MiB` both
        // arms answer `Some(8)`).
        let append = node.get().counters().append.load_acquire();
        assert!(
            append > BUFFER_BYTES as u64,
            "the ring must have scrolled past 0: append={append}, capacity={BUFFER_BYTES}"
        );
        let jr = uc_journal::TailReader::open(&dir.join("journal")).unwrap();
        if purging {
            // The complete set at P moves the node's durable snapshot floor,
            // which commands the purge; the archive acks by advancing
            // `archive_first_base` (`uc_node::Node::archive_first_base`, the
            // same observable `tests/purge_safety.rs` uses). The floor persist
            // is throttled to 100 ms, so this is a wait, not a poll.
            assert!(
                wait_for(|| node.get().archive_first_base() > 0, REATTACH_TIMEOUT),
                "purge never advanced the archive floor below P={p}"
            );
            // And the journal's own lowest replayable position is what the
            // service's gap guard reads (`replay.rs`: `first > needed`).
            let first = jr.first_meta().unwrap().unwrap_or(0);
            assert!(
                first > 0 && first <= p,
                "purged journal must start inside (0, {p}], got {first}"
            );
        } else {
            // The other arm's premise, asserted rather than assumed: nothing
            // was purged, so the gap guard never fires and the tail alone
            // rebuilds the state — from genesis, under v2's `apply`.
            assert_eq!(
                jr.first_meta().unwrap().unwrap_or(0),
                0,
                "purge is disabled: the journal must still cover [0, P)"
            );
        }
        p
    }; // client shuts down, then the v1 service stops — the node keeps running.

    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap();

    // --- the operator's pin, between the two eras (spec §3 S4 step 2) ---
    if pinned {
        common::pin_row(dir, &cnc, 0, V1, V2, p);
        // The pin is cluster data: it reaches this row's slot words only once
        // the `uc2-cluster` agent has APPLIED the command at commit and
        // republished its view. Attaching before that would read `NoPin` and
        // prove nothing.
        assert!(
            wait_for(
                || cnc.service_slot(0).status.pin()
                    == PinRead::Pinned {
                        origin: p,
                        from: V1,
                        to: V2,
                    },
                REATTACH_TIMEOUT
            ),
            "the pin never reached row 0's slot words: {:?}",
            cnc.service_slot(0).status.pin()
        );
        // And the pin is a one-way door: the version it names is the only one
        // that may serve this row from here on, so v1's own re-attach — the
        // operator's rollback reflex — is refused BY NAME rather than
        // silently carrying on.
        let err = ServiceBuilder::new(
            ServiceConfig::new(dir.to_path_buf(), app_id.to_string()),
            RegisterSm::default(),
        )
        .start_with_snapshots()
        .err()
        .expect("a pinned row must refuse the old binary");
        assert!(
            matches!(
                err,
                uc_service::ServiceError::PinnedVersionMismatch {
                    row: 0,
                    pinned: V2,
                    mine: V1,
                    ..
                }
            ),
            "{err}"
        );
    }

    // --- flag day: swap the service binary against the same instance dir ---
    let svc2 = ServiceBuilder::new(
        ServiceConfig::new(dir.to_path_buf(), app_id.to_string()),
        DoublingRegisterSm::default(),
    )
    .start_with_snapshots()
    .unwrap();
    // Reconstruction is finished when the row's published applied frontier has
    // reached P — the slot the apply loop stores after every batch and after
    // every replay pass. `attach` reset it to 0 before `start_with_snapshots`
    // returned and v1 is stopped, so only v2 can raise it. (No sleep: a sleep
    // would be a guess at how long a journal walk takes.)
    let caught_up = wait_for(
        || cnc.service_slot(0).applied.load_acquire() >= p,
        REATTACH_TIMEOUT,
    );
    let out = if caught_up {
        Swap::CaughtUp(svc2.query(()))
    } else {
        Swap::Stalled
    };
    // A stalled service's apply thread has fail-stopped, taking the state
    // machine's lock down with it: `stop()` re-raises that panic in teardown
    // and `query` would meet a poisoned mutex, so a stall is torn down with
    // `crash()` (which joins and swallows) and is never queried.
    if caught_up {
        svc2.stop();
    } else {
        svc2.crash();
    }
    drop(node);
    out
}

/// [`swap_to_v2`] for the arms that are expected to converge.
fn v2_after_swap(purge: uc_node::PurgePolicy, pinned: bool, app_id: &str) -> Option<u64> {
    match swap_to_v2(purge, pinned, app_id) {
        Swap::CaughtUp(v) => v,
        Swap::Stalled => panic!("v2 never reconstructed up to P (app_id={app_id})"),
    }
}

/// A capture buffer for the fail-stop arm below: the apply thread's
/// fail-stop panic unwinds a BACKGROUND thread, so it never fails the test
/// thread directly and has to be recorded by a scoped panic hook
/// (`uc_service/tests/reconstruction.rs`'s two fail-stop tests, verbatim).
static PANIC_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// `PANIC_LOG` and `set_hook`/`take_hook` are process-global. Held for the
/// whole hook-owning section so a sibling test cannot interleave with the
/// swapped hook, and poison-tolerant for
/// `uc_service/tests/reconstruction.rs`'s reason: a genuine regression that
/// panics while this lock is held would otherwise make whichever test runs
/// next fail on an unrelated poison error instead of its own assertion.
static PANIC_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Own the panic hook for as long as this value lives, and restore the
/// previous one however the scope ends — including an unwind THROUGH it. A
/// bare `take_hook` / `set_hook(prev)` pair leaks the capture hook onto every
/// later test in this binary the moment anything between them panics, which
/// is exactly when the messages are wanted on stderr.
/// What `std::panic::take_hook` hands back.
type Hook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

struct HookGuard {
    prev: Option<Hook>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl HookGuard {
    fn capture() -> HookGuard {
        let _lock = PANIC_HOOK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        PANIC_LOG.lock().unwrap_or_else(|e| e.into_inner()).clear();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|info| {
            PANIC_LOG
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(info.to_string());
        }));
        HookGuard {
            prev: Some(prev),
            _lock,
        }
    }

    /// Everything the hook captured so far.
    fn captured() -> Vec<String> {
        PANIC_LOG.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Drop for HookGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            std::panic::set_hook(prev);
        }
    }
}

/// Spec §2.3 through UC's own attach path, UNPINNED — the two things an
/// unpinned swap can do with a purge floor, and neither is v1's history.
/// Same instance dir, same node, same v2 binary; only the journal's purge
/// floor differs.
///
/// * purge DISABLED (the shipped default) — the journal still covers
///   `[0, P)`, so v2 replays every old write through its own `apply` and
///   doubles them: `Some(8)`, a state that never existed on any replica.
/// * purge BELOW the complete set — the prefix is gone, so the gap guard
///   needs a covering artifact and the only one is v1's. Since plan B2 T3 an
///   unpinned install must be same-version, so it is REFUSED BY NAME and the
///   apply thread fail-stops. (Before T3 it installed and the row answered
///   `Some(4)`; that is now the PINNED path's answer — see
///   [`a_real_pin_makes_the_default_purge_off_swap_install_the_origin`].)
///
/// So the §2.3 demonstration reads, today: *unpinned, genesis replay computes
/// the counterfactual or the install is refused by name; pinned, the origin
/// is installed.*
#[test]
fn real_attach_genesis_replay_computes_the_counterfactual_and_install_does_not() {
    let genesis = v2_after_swap(uc_node::PurgePolicy::Disabled, false, "ra1");

    // Own the panic hook for the purge-on arm only. The guard restores the
    // previous hook when this scope ends, an unwind from inside the arm
    // included, and holds `PANIC_HOOK_LOCK` for as long as it lives.
    let (purged, seen) = {
        let _hook = HookGuard::capture();
        let purged = swap_to_v2(
            uc_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 },
            false,
            "ra2",
        );
        (purged, HookGuard::captured())
    };
    let refusal =
        format!("artifact was built by version {V1:#010x} but {V2:#010x} is required here");
    let fired = seen
        .iter()
        .any(|m| m.contains("MistaggedSnapshot") && m.contains(&refusal));

    // The §2.3 evidence note says this demonstration is missing; print it so
    // a `--nocapture` run IS the record.
    eprintln!("§2.3 (unpinned): genesis-replay={genesis:?}  purged={purged:?}");
    assert_eq!(
        genesis,
        Some(2 * LAST_WRITE),
        "genesis path under v2 is the counterfactual"
    );
    assert_eq!(
        purged,
        Swap::Stalled,
        "an unpinned cross-version install must not converge"
    );
    assert!(
        fired,
        "the apply agent must fail-stop by name (MistaggedSnapshot/VersionMismatch \
         {V1:#010x} vs {V2:#010x}); panics seen: {seen:?}"
    );
}

/// Spec §3 S4 end to end, through a REAL `uc2ctl upgrade pin`: the same
/// purge-off swap that computes the counterfactual unpinned (plan A's
/// finding, the first arm) installs v1's artifact at P once the cluster has
/// pinned the row, and answers v1's true state `Some(4)`.
///
/// The pin goes in the way an operator's does — staged `upgrade.pending`,
/// admin op 10, through the cluster FSM — and the arm also asserts the door
/// it closes: v1's own re-attach after the pin is refused by name (inside
/// [`swap_to_v2`], which is where the two eras meet).
#[test]
fn a_real_pin_makes_the_default_purge_off_swap_install_the_origin() {
    assert_eq!(
        v2_after_swap(uc_node::PurgePolicy::Disabled, false, "pin-off"),
        Some(2 * LAST_WRITE),
        "unpinned, the purge-off swap replays from genesis under v2's apply"
    );
    assert_eq!(
        v2_after_swap(uc_node::PurgePolicy::Disabled, true, "pin-on"),
        Some(LAST_WRITE),
        "pinned, the SAME swap installs v1's artifact at P and carries its state"
    );
}
