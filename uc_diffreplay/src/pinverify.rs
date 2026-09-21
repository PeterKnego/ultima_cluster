// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2-diffreplay pin-verify` — spec §6.2 part 2: on a real node, with the
//! app's real binaries, prove that a committed `uc2ctl upgrade pin` refuses
//! the stale binary and steers the new one onto the ARTIFACT path — never the
//! genesis counterfactual §2.3 warns about.
//!
//! The sequence [`run`] drives, each phase named for the spec step it stands
//! for:
//!
//! 1. **History under OLD** (S1–S3 have happened): the node starts, OLD
//!    attaches, and the corpus's recorded commands are re-submitted — the
//!    first `--split` of them, then the coordinated instant at **P**, then
//!    the rest. OLD is stopped at the frontier **X**.
//! 2. **The pin** (S4 step 2): a real `uc2ctl upgrade pin --row R --from
//!    <the attached version> --to <--to> --origin P`, waited for at the
//!    row's own slot words.
//! 3. **The refusal arm** (S4 step 5): OLD is started again. It must NOT
//!    rejoin — the SDK refuses it by name, and the arm holds only on a
//!    non-zero exit whose stderr carries [`REFUSAL_MARKER`].
//! 4. **The swap arm** (S4 step 4): NEW attaches, installs the artifact at
//!    P, recomputes (P, X], and a second instant at **Q** turns its live
//!    state into an artifact anyone can project.
//! 5. **The three projections**: NEW's live state at Q, NEW replaying the
//!    exported corpus from the artifact at P, and NEW replaying it from
//!    genesis. `live == artifact` says the pin steered the swap correctly;
//!    `artifact != genesis` says the span could tell the two paths apart at
//!    all — without it the run demonstrates nothing and is INCONCLUSIVE, not
//!    a pass ([`verdict`]).
//!
//! Everything here is black-box: the app is only ever RUN (`serve`,
//! `replay`, `project`), and every observation comes from UC's own surfaces
//! — the cnc page, the snapshot directory, the journal, the child's exit
//! status and its stderr.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use uc_protocol::identity::VersionDisplay;

use crate::trace::Trace;

/// The stable phrase of `ServiceError::PinnedVersionMismatch`'s Display
/// (`uc_service/src/config.rs`): what the refusal arm looks for on the stale
/// binary's stderr. Pinned by `the_refusal_marker_is_the_sdks_own_text`.
pub const REFUSAL_MARKER: &str = "is pinned to version";

/// Where a run's scratch lands by default: `<report>.pinverify/`, beside the
/// report itself — the same rule the diff modes' `<report>.traces/` follows,
/// so a failing run's evidence sits next to the report that names it.
pub fn scratch_dir_of(report: &Path) -> PathBuf {
    report.with_extension("pinverify")
}

/// What the run proved. Only [`Verdict::Pass`] is a demonstration; the other
/// two are honest about what was and was not shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Pass,
    Inconclusive,
    Fail,
}

/// The admin answer to `uc2ctl upgrade pin` (`status`/`reason` as the cnc
/// admin band carries them; `0`/`0` is accepted).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinArm {
    pub status: u32,
    pub reason: u32,
}

/// The stale binary's fate after the pin. The arm HOLDS only when the child
/// exited non-zero AND said why — an exit alone could be any startup failure.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RefusalArm {
    pub exited: bool,
    pub code: Option<i32>,
    pub matched: bool,
    pub stderr_excerpt: String,
}

/// The new binary's fate, and the three projections it produced.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SwapArm {
    pub attached: bool,
    pub version_seen: u32,
    pub caught_up: bool,
    /// `None` = never compared (a projection or a replay did not happen),
    /// never confused with a comparison that came out false.
    pub live_eq_artifact: Option<bool>,
    pub artifact_eq_genesis: Option<bool>,
    pub live: Option<String>,
    pub artifact: Option<String>,
    pub genesis: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinVerifyReport {
    pub mode: String,
    pub corpus: PathBuf,
    pub frames: u64,
    pub skipped_timers: u64,
    /// P — the coordinated instant the pin names as its origin.
    pub origin: u64,
    /// X — OLD's applied frontier when it was stopped.
    pub frontier: u64,
    /// Q — the second instant, taken once NEW had caught up.
    pub end: u64,
    pub from: u32,
    pub to: u32,
    pub pin: PinArm,
    pub refusal: RefusalArm,
    pub swap: SwapArm,
    pub verdict: Verdict,
    pub notes: Vec<String>,
}

/// The verdict table (spec §6.2 part 2).
///
/// A PASS needs all three: the pin refused the stale binary, NEW's live
/// state is the one the ARTIFACT path computes, and the two paths could be
/// told apart on this span. `live == artifact` with the paths AGREEING is
/// [`Verdict::Inconclusive`] — everything held, but nothing was shown, so
/// calling it a pass would overstate the evidence. Anything else fails,
/// including an arm that never produced a comparison at all.
pub fn verdict(
    refusal_held: bool,
    live_eq_artifact: Option<bool>,
    artifact_eq_genesis: Option<bool>,
) -> Verdict {
    match (refusal_held, live_eq_artifact, artifact_eq_genesis) {
        (true, Some(true), Some(false)) => Verdict::Pass,
        (true, Some(true), Some(true)) => Verdict::Inconclusive,
        _ => Verdict::Fail,
    }
}

impl PinVerifyReport {
    /// A report with nothing proved yet: every arm empty and the verdict
    /// [`Verdict::Fail`], so a run that returns early — for any reason,
    /// including one nobody anticipated — reports a failure rather than a
    /// vacuous pass.
    pub fn empty(corpus: PathBuf, from: u32, to: u32) -> PinVerifyReport {
        PinVerifyReport {
            mode: "pin-verify".into(),
            corpus,
            frames: 0,
            skipped_timers: 0,
            origin: 0,
            frontier: 0,
            end: 0,
            from,
            to,
            pin: PinArm::default(),
            refusal: RefusalArm::default(),
            swap: SwapArm::default(),
            verdict: Verdict::Fail,
            notes: Vec::new(),
        }
    }

    pub fn failed(&self) -> bool {
        self.verdict == Verdict::Fail
    }

    pub fn write_json(&self, w: impl std::io::Write) -> anyhow::Result<()> {
        Ok(serde_json::to_writer_pretty(w, self)?)
    }

    /// One line per phase, the verdict last, the notes after it.
    pub fn write_text(&self, mut w: impl std::io::Write) -> anyhow::Result<()> {
        writeln!(w, "{} — corpus {}", self.mode, self.corpus.display())?;
        writeln!(
            w,
            "  span: {} MESSAGE frames ({} TIMER frames skipped), origin P={} frontier X={} end Q={}",
            self.frames, self.skipped_timers, self.origin, self.frontier, self.end
        )?;
        writeln!(
            w,
            "  pin: {} → {} at origin {} — admin status={} reason={}",
            VersionDisplay(self.from),
            VersionDisplay(self.to),
            self.origin,
            self.pin.status,
            self.pin.reason
        )?;
        writeln!(
            w,
            "  refusal arm: exited={} code={} matched={:?}",
            self.refusal.exited,
            self.refusal
                .code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".into()),
            self.refusal.matched
        )?;
        for line in self.refusal.stderr_excerpt.lines() {
            writeln!(w, "    | {line}")?;
        }
        writeln!(
            w,
            "  swap arm: attached={} version={} (--to {}) caught_up={}",
            self.swap.attached,
            VersionDisplay(self.swap.version_seen),
            VersionDisplay(self.to),
            self.swap.caught_up
        )?;
        writeln!(
            w,
            "  live == artifact: {}",
            yes_no(self.swap.live_eq_artifact)
        )?;
        writeln!(
            w,
            "  artifact == genesis: {} (a PASS needs them to DIFFER — that is the counterfactual)",
            yes_no(self.swap.artifact_eq_genesis)
        )?;
        writeln!(
            w,
            "  verdict: {}",
            match self.verdict {
                Verdict::Pass => "PASS",
                Verdict::Inconclusive => "INCONCLUSIVE",
                Verdict::Fail => "FAIL",
            }
        )?;
        for n in &self.notes {
            writeln!(w, "  note: {n}")?;
        }
        Ok(())
    }
}

fn yes_no(b: Option<bool>) -> &'static str {
    match b {
        Some(true) => "yes",
        Some(false) => "no",
        None => "not compared",
    }
}

/// The app's own knobs, for a form other than `serve`.
///
/// The app-binary CLI contract (crate README) is `<bin> <verb> <flags…>`, so
/// an app's knobs ride AFTER the verb — which means the serve form's argv
/// cannot simply be handed to `replay` or `project`. `--old-arg`/`--new-arg`
/// give the serve form's argv (the app's serve verb, then its knobs), and
/// this drops that leading verb so the other two forms can put their own in
/// front of the same knobs. A first argument that begins with `-` is a knob,
/// not a verb, and is kept.
pub fn app_knobs(serve_args: &[String]) -> &[String] {
    match serve_args.first() {
        Some(first) if !first.starts_with('-') => &serve_args[1..],
        _ => serve_args,
    }
}

/// Run the app's `replay` form: `<bin> replay <args…> --corpus C --out T
/// [--from-genesis]`, then read the trace back. Moved here from the binary
/// so `pin-verify` and the three diff modes run the app the same way.
pub fn run_replay(
    bin: &Path,
    args: &[String],
    corpus: &Path,
    out: &Path,
    from_genesis: bool,
) -> anyhow::Result<Trace> {
    let mut c = Command::new(bin);
    c.arg("replay")
        .args(args)
        .arg("--corpus")
        .arg(corpus)
        .arg("--out")
        .arg(out);
    if from_genesis {
        c.arg("--from-genesis");
    }
    let st = c
        .status()
        .with_context(|| format!("spawn {}", bin.display()))?;
    if !st.success() {
        bail!("{} replay exited {st}", bin.display());
    }
    Trace::read_json(std::fs::File::open(out)?)
}

/// Run the app's `project` form and return the projection text.
pub fn run_project(
    bin: &Path,
    args: &[String],
    artifact: &Path,
    position: u64,
) -> anyhow::Result<String> {
    let out = Command::new(bin)
        .arg("project")
        .args(args)
        .arg("--artifact")
        .arg(artifact)
        .arg("--position")
        .arg(position.to_string())
        .output()
        .with_context(|| format!("spawn {}", bin.display()))?;
    if !out.status.success() {
        bail!(
            "{} project exited {}: {}",
            bin.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)?)
}

/// The live half: the sequence itself, and everything that needs a running
/// node. Behind `pin-verify`, so a service binary that embeds only the
/// replay driver (`examples/kv`, `default-features = false`) still gets the
/// report types and [`run_replay`]/[`run_project`] above without linking
/// `uc_node`.
#[cfg(feature = "pin-verify")]
mod sequence {
    use std::path::Path;
    use std::time::Duration;

    use anyhow::{Context, bail};
    use uc_journal::TailReader;
    use uc_log::cnc::{CncPage, PinRead};
    use uc_node::Node;
    use uc_protocol::identity::VersionDisplay;

    use super::*;
    use crate::corpus::Corpus;
    use crate::live::{self, AttachOutcome};

    /// Everything one `pin-verify` run needs. `old_args`/`new_args` are the
    /// app's own arguments for its SERVE form, exactly as
    /// [`crate::live::spawn_app`] takes them (the app's serve verb followed by
    /// its knobs); see [`app_knobs`] for how the `replay` and `project` forms
    /// reuse them.
    #[derive(Debug, Clone)]
    pub struct PinVerifyArgs {
        pub corpus: PathBuf,
        /// The OLD service binary — the version running before the upgrade.
        pub old: PathBuf,
        pub old_args: Vec<String>,
        /// The NEW service binary — the version the pin names.
        pub new: PathBuf,
        pub new_args: Vec<String>,
        pub app_id: String,
        /// The row's FSM NAME, as `node.toml`'s `[services] names` declares it.
        pub fsm: String,
        pub row: u8,
        /// The packed version the pin names.
        pub to: u32,
        /// MESSAGE frames re-submitted before the instant; default = half.
        pub split: Option<usize>,
        pub timeout: Duration,
        /// Scratch root (instance dir, stderr files, traces); default =
        /// [`scratch_dir_of`] the report.
        pub scratch: Option<PathBuf>,
        pub report: PathBuf,
    }

    /// The node for the length of a run, stopped on EVERY exit path — including
    /// the early returns and the `bail!`s that report a broken rig. A leaked
    /// node keeps five busy-spin agents running for the rest of the process.
    struct NodeGuard(Option<Node>);

    impl NodeGuard {
        fn start(
            dir: &Path,
            app_id: &str,
            fsm: &str,
            timeout: Duration,
        ) -> anyhow::Result<NodeGuard> {
            Ok(NodeGuard(Some(live::start_node(
                dir, app_id, fsm, timeout,
            )?)))
        }
        fn node(&self) -> &Node {
            self.0.as_ref().expect("the node is taken only on drop")
        }
    }

    impl Drop for NodeGuard {
        fn drop(&mut self) {
            if let Some(n) = self.0.take() {
                n.stop();
            }
        }
    }

    /// The last `n` lines of a child's stderr — the app's own words, which is
    /// all the report ever quotes.
    fn last_lines(s: &str, n: usize) -> String {
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    /// A bounded wait's outcome as a sentence, for a note.
    fn describe(outcome: &AttachOutcome) -> String {
        match outcome {
            AttachOutcome::Attached => "attached".into(),
            AttachOutcome::Exited { code, stderr } => {
                format!("exited (code {code:?}): {}", last_lines(stderr, 5))
            }
            AttachOutcome::TimedOut { stderr } => {
                format!("timed out, killed: {}", last_lines(stderr, 5))
            }
        }
    }

    /// Record a step's failure as a note and return `None`, so the comparison it
    /// would have fed stays `None` ("not compared") rather than the run dying
    /// with no report at all. The verdict table treats a missing comparison as a
    /// failure, so nothing is hidden by continuing.
    fn note_err<T>(notes: &mut Vec<String>, what: &str, res: anyhow::Result<T>) -> Option<T> {
        match res {
            Ok(v) => Some(v),
            Err(e) => {
                notes.push(format!("{what} failed: {e:#}"));
                None
            }
        }
    }

    pub fn run(a: &PinVerifyArgs) -> anyhow::Result<PinVerifyReport> {
        let corpus = Corpus::open(&a.corpus)?;
        let (frames, skipped_timers) = live::message_frames(&corpus)?;
        if frames.is_empty() {
            bail!(
                "the corpus has no MESSAGE frames in [{}, {}) — nothing to replay",
                corpus.manifest.origin,
                corpus.manifest.end
            );
        }
        let split = a
            .split
            .unwrap_or(frames.len() / 2)
            .clamp(1, frames.len().saturating_sub(1).max(1));
        let scratch = a
            .scratch
            .clone()
            .unwrap_or_else(|| scratch_dir_of(&a.report));
        let dir = scratch.join("instance");
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut r = PinVerifyReport::empty(a.corpus.clone(), 0, a.to);
        r.frames = frames.len() as u64;
        r.skipped_timers = skipped_timers;
        if skipped_timers > 0 {
            r.notes.push(format!(
                "{skipped_timers} TIMER frames in the recorded span were NOT re-submitted — a node \
                 mints those, so this run's span is the corpus's MESSAGE frames only"
            ));
        }
        // ---- Phase 1: history under OLD (spec S1–S3 have happened; this is the
        // cluster's life before the upgrade) ----
        let node = NodeGuard::start(&dir, &a.app_id, &a.fsm, a.timeout)?;
        let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &a.app_id)
            .map_err(|e| anyhow::anyhow!("opening the cnc page: {e}"))?;
        let before = live::incarnation(&cnc, a.row);
        let mut old = live::spawn_app(
            &a.old,
            &a.old_args,
            &dir,
            &a.app_id,
            &scratch.join("old.stderr"),
        )?;
        match old.wait_attached(&cnc, a.row, before, a.timeout) {
            AttachOutcome::Attached => {}
            other => bail!("OLD never attached: {}", describe(&other)),
        }
        // The version the row is attached AT is the pin's `from`.
        r.from = cnc.service_slot(a.row as usize).status.version();
        live::replay_span(&frames, &dir, &a.app_id, a.row, 0..split, a.timeout)?;

        // ---- S4 step 1: the instant at P ----
        let p = live::command_instant(node.node(), a.timeout)?;
        if !live::wait_for(|| live::artifact_path(&dir, a.row, p).is_file(), a.timeout) {
            bail!("row {} never published snap-{p}", a.row);
        }
        r.origin = p;
        // OLD runs on past P — the durable shape's precondition (an in-memory SM
        // simply attaches empty later; the check is the same).
        let tail = live::replay_span(
            &frames,
            &dir,
            &a.app_id,
            a.row,
            split..frames.len(),
            a.timeout,
        )?;
        // The frontier X is the row's published `applied` CURSOR — the position
        // after the last applied frame — never the last response's position,
        // which is that frame's START (`live::SpanReplay::last_position`).
        let applied = || cnc.service_slot(a.row as usize).applied.load_acquire();
        if tail.submitted > 0 && !live::wait_for(|| applied() > tail.last_position, a.timeout) {
            bail!(
                "row {}'s applied cursor ({}) never passed the tail's last frame at {}",
                a.row,
                applied(),
                tail.last_position
            );
        }
        let x = applied();
        r.frontier = x;
        match old.stop(a.timeout) {
            Ok(st) => r.notes.push(format!("OLD stopped at X={x} with exit {st}")),
            Err(e) => r
                .notes
                .push(format!("OLD did not stop cleanly at X={x}: {e}")),
        }

        // ---- S4 step 2: the pin, exactly as an operator places it ----
        match live::pin_row(&dir, &cnc, a.row, r.from, a.to, p, a.timeout) {
            Ok(resp) => {
                r.pin = PinArm {
                    status: resp.status,
                    reason: resp.reason,
                }
            }
            Err(e) => {
                // The verdict stays Fail: nothing downstream of an unplaced pin
                // would mean anything.
                r.notes.push(format!("pin refused: {e}"));
                return Ok(r);
            }
        }
        if !live::wait_for(
            || matches!(cnc.service_slot(a.row as usize).status.pin(), PinRead::Pinned { origin, .. } if origin == p),
            a.timeout,
        ) {
            bail!(
                "the pin never reached row {}'s slot words: {:?}",
                a.row,
                cnc.service_slot(a.row as usize).status.pin()
            );
        }

        // ---- S4 step 5: the refusal arm — the stale binary must not rejoin ----
        if r.from == a.to {
            r.notes.push(format!(
                "OLD already runs the pinned version ({}), so the refusal arm cannot refuse it — \
                 a pin that names the running version closes no door",
                VersionDisplay(a.to)
            ));
        }
        let before = live::incarnation(&cnc, a.row);
        let mut stale = live::spawn_app(
            &a.old,
            &a.old_args,
            &dir,
            &a.app_id,
            &scratch.join("old-after-pin.stderr"),
        )?;
        let refusal_held = match stale.wait_attached(&cnc, a.row, before, a.timeout) {
            AttachOutcome::Exited { code, stderr } => {
                let matched = stderr.contains(REFUSAL_MARKER);
                r.refusal = RefusalArm {
                    exited: true,
                    code,
                    matched,
                    stderr_excerpt: last_lines(&stderr, 5),
                };
                // A refusal is an exit that SAID why: a non-zero code alone could
                // be any startup failure at all.
                code != Some(0) && matched
            }
            other => {
                let attached = matches!(other, AttachOutcome::Attached);
                r.refusal = RefusalArm {
                    exited: false,
                    code: None,
                    matched: false,
                    stderr_excerpt: last_lines(&stale.stderr(), 5),
                };
                r.notes.push(format!(
                    "the stale binary was not refused: {}",
                    describe(&other)
                ));
                if attached {
                    let _ = stale.stop(a.timeout);
                }
                false
            }
        };

        // ---- S4 step 4: the swap arm — NEW attaches, installs the origin,
        // recomputes (P, X] ----
        let before = live::incarnation(&cnc, a.row);
        let mut new = live::spawn_app(
            &a.new,
            &a.new_args,
            &dir,
            &a.app_id,
            &scratch.join("new.stderr"),
        )?;
        match new.wait_attached(&cnc, a.row, before, a.timeout) {
            AttachOutcome::Attached => r.swap.attached = true,
            other => {
                r.notes
                    .push(format!("NEW did not attach: {}", describe(&other)));
                r.verdict = verdict(refusal_held, None, None);
                return Ok(r);
            }
        }
        r.swap.version_seen = cnc.service_slot(a.row as usize).status.version();
        if r.swap.version_seen != a.to {
            r.notes.push(format!(
                "NEW attached as version {:#010x} but --to named {:#010x}",
                r.swap.version_seen, a.to
            ));
        }
        r.swap.caught_up = matches!(
            new.wait_applied(&cnc, a.row, x, a.timeout),
            AttachOutcome::Attached
        );
        if !r.swap.caught_up {
            r.notes.push(format!(
                "NEW never reached X={x}: {}",
                last_lines(&new.stderr(), 5)
            ));
            let _ = new.stop(a.timeout);
            r.verdict = verdict(refusal_held, None, None);
            return Ok(r);
        }
        // A second instant at Q, so the live state is an artifact NEW can project.
        let q = live::command_instant(node.node(), a.timeout)?;
        if !live::wait_for(|| live::artifact_path(&dir, a.row, q).is_file(), a.timeout) {
            bail!("row {} never published snap-{q}", a.row);
        }
        r.end = q;
        match new.stop(a.timeout) {
            Ok(st) => r.notes.push(format!("NEW stopped at Q={q} with exit {st}")),
            Err(e) => r
                .notes
                .push(format!("NEW did not stop cleanly at Q={q}: {e}")),
        }

        // ---- The three projections ----
        // The app's knobs, with each form's own verb in front (`app_knobs`).
        let knobs = app_knobs(&a.new_args);
        let live_proj = note_err(
            &mut r.notes,
            "the live projection",
            run_project(&a.new, knobs, &live::artifact_path(&dir, a.row, q), q),
        );
        let exported = scratch.join("corpus");
        let ec = match note_err(
            &mut r.notes,
            "exporting the corpus",
            Corpus::export(&dir, &a.app_id, a.row, p, q, r.from, &exported),
        ) {
            Some(c) => c,
            None => return Ok(r),
        };
        // The instance dir has been read for the last time; stop spinning.
        drop(node);
        // "Genesis" must really BE genesis: `--from-genesis` walks the exported
        // journal from its first retained block, which is position 0 only while
        // the rig's purge is OFF. If it is not, the genesis path would silently
        // be "from the first retained block" — a different claim — so the
        // comparison is left uncompared instead.
        let first_meta = match note_err(
            &mut r.notes,
            "reading the exported journal's first block",
            TailReader::open(&ec.journal_dir())
                .and_then(|t| t.first_meta())
                .map_err(|e| anyhow::anyhow!("{e}")),
        ) {
            Some(m) => m.unwrap_or(0),
            None => u64::MAX,
        };
        let art = note_err(
            &mut r.notes,
            "NEW's artifact-path replay",
            run_replay(
                &a.new,
                knobs,
                &exported,
                &scratch.join("artifact.json"),
                false,
            ),
        );
        let genesis = if first_meta == 0 {
            note_err(
                &mut r.notes,
                "NEW's genesis-path replay",
                run_replay(
                    &a.new,
                    knobs,
                    &exported,
                    &scratch.join("genesis.json"),
                    true,
                ),
            )
        } else {
            r.notes.push(format!(
                "the exported journal starts at position {first_meta}, not 0, so a `--from-genesis` \
                 replay would not be a genesis replay; the counterfactual was NOT computed"
            ));
            None
        };
        let art_p = art.and_then(|t| t.projection_at_end);
        let gen_p = genesis.and_then(|t| t.projection_at_end);
        r.swap.live_eq_artifact = match (&art_p, &live_proj) {
            (Some(x), Some(y)) => Some(x == y),
            _ => None,
        };
        r.swap.artifact_eq_genesis = match (&art_p, &gen_p) {
            (Some(x), Some(y)) => Some(x == y),
            _ => None,
        };
        r.swap.live = live_proj;
        r.swap.artifact = art_p;
        r.swap.genesis = gen_p;
        r.verdict = verdict(
            refusal_held && r.swap.version_seen == a.to,
            r.swap.live_eq_artifact,
            r.swap.artifact_eq_genesis,
        );
        if r.verdict == Verdict::Inconclusive {
            r.notes.push(
                "NEW's artifact-path and genesis-path projections agree over this span: the change \
                 did not alter any replayed command's semantics, so this run cannot show the \
                 counterfactual; the refusal arm and live==artifact still hold"
                    .into(),
            );
        }
        Ok(r)
    }
}

#[cfg(feature = "pin-verify")]
pub use sequence::{PinVerifyArgs, run};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
            name: "register".into(),
            row: 0,
            origin: 4096,
            pinned: 2,
            mine: 1,
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
        assert_eq!(
            back.verdict,
            Verdict::Fail,
            "an empty report is a failure until the arms fill it"
        );
    }

    /// The serve form's argv carries the app's verb; every other form
    /// supplies its own and reuses the knobs.
    #[test]
    fn app_knobs_drops_the_serve_verb_and_keeps_the_knobs() {
        let s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(app_knobs(&s(&["serve", "--double"])), &s(&["--double"])[..]);
        assert_eq!(app_knobs(&s(&["serve"])), &[] as &[String]);
        assert_eq!(app_knobs(&s(&[])), &[] as &[String]);
        // No verb to drop: a binary whose serve form takes only flags.
        assert_eq!(
            app_knobs(&s(&["--double", "--durable"])),
            &s(&["--double", "--durable"])[..]
        );
    }

    /// The text renderer says every phase, ends with the verdict, and leaves
    /// no placeholder behind.
    #[test]
    fn the_text_report_names_every_phase() {
        let mut r = PinVerifyReport::empty(PathBuf::from("c"), 0, 2);
        r.notes.push("a note".into());
        let mut buf = Vec::new();
        r.write_text(&mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        for phrase in [
            "pin-verify",
            "span:",
            "pin:",
            "refusal arm:",
            "swap arm:",
            "live == artifact:",
            "artifact == genesis:",
            "verdict: FAIL",
            "note: a note",
        ] {
            assert!(text.contains(phrase), "{phrase:?} missing from:\n{text}");
        }
        assert!(!text.contains("todo"), "{text}");
    }
}
