//! `uc2-diffreplay pin-verify` end to end on the register fixture (spec
//! §6.2 part 2): the empty and the durable state-machine shapes both PASS
//! with the counterfactual DEMONSTRATED on a corpus whose tail is a CAS
//! chain (ruling R-C-1); a stale NEW is a FAIL (the swap arm is refused by
//! name); a pure-write corpus is INCONCLUSIVE, not a pass; a same-version
//! run is refused up front.
//!
//! **Why the durable case runs `--double-cas` and the empty one does not**
//! (final review C1, ruling R-C-3). The two cases have different wrong
//! paths. An EMPTY state machine that ignored the pin would replay from
//! GENESIS, which is the counterfactual `artifact_eq_genesis` already
//! computes — so `--double` has teeth there. A DURABLE one that ignored the
//! pin would CONTINUE FROM X: it would keep the `(P, X]` it persisted. With
//! `--double` (which rewrites `Cmd::Write` only) OLD and NEW compute the CAS
//! tail identically — 249 either way — so the artifact path and
//! continue-from-X land on the same value and the case could not fail on the
//! rewind it is named for. `--double-cas` (`DoublingCasRegisterSm`, VERSION
//! 3) also doubles `Cas.new`, which makes the tail's RESULT depend on the
//! version while its OUTCOME still depends on the state at P: artifact/live
//! = 400, continue-from-X = 249, genesis = 398, three distinct values. Both
//! cases additionally require `install_logged` — the SDK's own
//! `pinned install of snap-P` line — so a skipped install fails the run even
//! on a span that could not tell the paths apart.
mod common;
use std::path::{Path, PathBuf};
use std::process::Command;

use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::pinverify::{PinVerifyReport, Verdict};
use uc_lincheck::register::Cmd;

const WRITES: u64 = 200;
const CAS_TAIL: u64 = 50;

/// `WRITES` writes followed by a CAS chain that starts from the value the
/// writes leave behind (`WRITES - 1`) — the tail whose outcome depends on
/// the state the run's own instant froze.
///
/// BOTH halves are in `after`, so both land inside the exported `[P, …)`
/// span: `pin-verify` re-submits the corpus's frames onto a FRESH node and
/// takes its OWN instant at `--split`, so a command that is not in the
/// corpus is a command the counterfactual never sees. (A corpus whose
/// writes sat in `before` exports 50 frames, not 250: the CAS chain then
/// starts from an empty register, every CAS fails under both builds, and
/// the run is INCONCLUSIVE.) `before` is only what the corpus's own
/// artifact at P holds, and one write is enough to anchor it.
///
/// The split must fall exactly on the write/CAS boundary, which is why the
/// caller passes `Some(WRITES)` rather than taking the default half: with
/// any write in the TAIL, last-write-wins lands both paths on the same
/// final value again.
fn cas_corpus(app_id: &str) -> (tempfile::TempDir, PathBuf) {
    let inst = common::tempdir();
    let after: Vec<Cmd> = (0..WRITES)
        .map(Cmd::Write)
        .chain((0..CAS_TAIL).map(|k| Cmd::Cas {
            old: WRITES - 1 + k,
            new: WRITES + k,
        }))
        .collect();
    let p = common::build_register_history_with(inst.path(), app_id, &[Cmd::Write(0)], &after);
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

struct Run {
    status: std::process::ExitStatus,
    report: Option<PinVerifyReport>,
    stdout: String,
    stderr: String,
}

/// `split` is `pin-verify`'s own `--split`: how many of the corpus's
/// MESSAGE frames go in before the run's instant at P. `None` takes the
/// mode's default (half).
fn pin_verify(
    corpus: &Path,
    old_args: &[&str],
    new_args: &[&str],
    to: &str,
    app_id: &str,
    split: Option<u64>,
) -> Run {
    let bin = common::register_replay_bin();
    let report = corpus.parent().unwrap().join(format!("{app_id}.json"));
    let mut c = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    c.arg("pin-verify")
        .arg("--corpus")
        .arg(corpus)
        .arg("--old")
        .arg(&bin)
        .arg("--new")
        .arg(&bin)
        .arg("--app-id")
        .arg(app_id)
        .arg("--fsm")
        .arg(common::register_name())
        .arg("--to")
        .arg(to)
        .arg("--report")
        .arg(&report);
    if let Some(n) = split {
        c.arg("--split").arg(n.to_string());
    }
    for a in old_args {
        c.arg("--old-arg").arg(a);
    }
    for a in new_args {
        c.arg("--new-arg").arg(a);
    }
    let out = c.output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    eprintln!("{stdout}{stderr}");
    let report = std::fs::File::open(&report)
        .ok()
        .map(|f| serde_json::from_reader(f).expect("report parses"));
    Run {
        status: out.status,
        report,
        stdout,
        stderr,
    }
}

/// (a) The empty shape: an in-memory register. OLD = plain, NEW = `--double`.
#[test]
fn an_in_memory_register_passes_and_demonstrates_the_counterfactual() {
    let (_inst, corpus) = cas_corpus("pv-empty");
    let r = pin_verify(
        &corpus,
        &["serve"],
        &["serve", "--double"],
        "2",
        "pv-empty",
        Some(WRITES),
    );
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Pass);
    assert!(
        rep.refusal.matched,
        "the stale binary must be refused BY NAME"
    );
    assert_eq!(rep.swap.live_eq_artifact, Some(true));
    assert_eq!(
        rep.swap.artifact_eq_genesis,
        Some(false),
        "the CAS tail makes the paths diverge"
    );
    assert!(
        rep.swap.install_logged,
        "NEW must have SAID it ran the pinned install of snap-{}: {}",
        rep.origin, r.stdout
    );
    assert!(
        rep.frontier > rep.origin,
        "OLD must have run past P before the stop"
    );
}

/// (b) The durable shape: `--durable` persists `(value, last_applied)`, so
/// NEW attaches with `last_applied() = X > P` and MUST be rewound to P by
/// the pinned install (spec §2.3's third path, S4 step 4).
///
/// NEW is `--double-cas`, not `--double`, and the difference is the whole
/// tooth — see the module doc. The three paths over this corpus, all three
/// arithmetics written out because the case is worthless if they coincide:
///
/// - OLD (plain) applies `Write(0..200)` below P, so the artifact at P holds
///   `199`; above P its CAS chain fires all the way, `199 → 249`, and that
///   `249` is what `--durable` persisted when OLD was stopped at X.
/// - **Artifact path** (what the pin is for): install the artifact at P
///   (`199`), then recompute `(P, X]` under `--double-cas`. The first CAS is
///   `{old: 199, new: 2·200 = 400}` — `old` is NOT rewritten, so it still
///   matches — and stores **400**; every later CAS then compares against a
///   value that is no longer in the chain and fails. Live state = 400.
/// - **Continue-from-X** (what a NEW that skipped the pinned install would
///   have): OLD's persisted **249**, untouched.
/// - **Genesis path**: every write doubled, so the register sits at
///   `2·199 = 398` when the chain starts, `{old: 199, …}` never matches, and
///   nothing fires. **398**.
///
/// 400 ≠ 249 ≠ 398: `live == artifact` now genuinely excludes the skipped
/// install, and `artifact != genesis` still excludes the genesis replay.
#[test]
fn a_durable_register_is_rewound_to_the_origin_and_passes() {
    let (_inst, corpus) = cas_corpus("pv-durable");
    let r = pin_verify(
        &corpus,
        &["serve", "--durable"],
        &["serve", "--double-cas", "--durable"],
        "3",
        "pv-durable",
        Some(WRITES),
    );
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Pass);
    assert!(
        rep.swap.install_logged,
        "NEW must have SAID it ran the pinned install of snap-{} — the observation that a \
         skipped install cannot fake: {}",
        rep.origin, r.stdout
    );
    assert_eq!(
        rep.swap.live_eq_artifact,
        Some(true),
        "live={:?} artifact={:?}",
        rep.swap.live,
        rep.swap.artifact
    );
    assert_eq!(
        rep.swap.artifact_eq_genesis,
        Some(false),
        "artifact={:?} genesis={:?}",
        rep.swap.artifact,
        rep.swap.genesis
    );
    // The point of `--double-cas`: the value NEW ended on is the one the
    // ARTIFACT path computes (2·WRITES), which a NEW that had continued from
    // OLD's persisted state (WRITES + CAS_TAIL - 1) could not have produced,
    // and which is not the genesis value (2·(WRITES - 1)) either.
    let live = rep.swap.live.as_deref().unwrap_or_default();
    assert!(
        live.contains(&format!("value=Some({})", 2 * WRITES)),
        "the artifact path stores 2·{WRITES}={} on the first CAS; continue-from-X would read \
         value=Some({}) and genesis value=Some({}). live was: {live:?}",
        2 * WRITES,
        WRITES + CAS_TAIL - 1,
        2 * (WRITES - 1)
    );
    let genesis = rep.swap.genesis.as_deref().unwrap_or_default();
    assert!(
        genesis.contains(&format!("value=Some({})", 2 * (WRITES - 1))),
        "genesis doubles every write and fires no CAS: {genesis:?}"
    );
}

/// (c) Teeth for the swap arm: NEW is the OLD binary (version 0) while the
/// pin names 2 — the SDK refuses it at attach, and the mode must FAIL.
#[test]
fn a_new_binary_that_is_not_the_pinned_version_is_a_fail() {
    let (_inst, corpus) = cas_corpus("pv-stale");
    let r = pin_verify(
        &corpus,
        &["serve"],
        &["serve"],
        "2",
        "pv-stale",
        Some(WRITES),
    );
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
    let r = pin_verify(
        &corpus,
        &["serve"],
        &["serve", "--double"],
        "2",
        "pv-writes",
        None,
    );
    let rep = r.report.expect("report");
    assert!(r.status.success(), "{}", r.stdout);
    assert_eq!(rep.verdict, Verdict::Inconclusive);
    assert!(rep.refusal.matched);
    assert_eq!(rep.swap.live_eq_artifact, Some(true));
    assert_eq!(rep.swap.artifact_eq_genesis, Some(true));
    assert!(
        rep.notes
            .iter()
            .any(|n| n.contains("cannot show the counterfactual")),
        "{:?}",
        rep.notes
    );
}

/// (e) A same-version "upgrade" cannot hold the refusal arm; the mode says
/// so before placing a pin (ruling R-C-1). No report is written.
#[test]
fn a_same_version_run_is_refused_before_the_pin() {
    let (_inst, corpus) = write_corpus("pv-same");
    let r = pin_verify(
        &corpus,
        &["serve", "--double"],
        &["serve", "--double"],
        "2",
        "pv-same",
        None,
    );
    assert!(!r.status.success(), "{}", r.stdout);
    assert!(r.report.is_none(), "refused before any arm ran: no report");
    assert!(r.stderr.contains("already runs version"), "{}", r.stderr);
}
