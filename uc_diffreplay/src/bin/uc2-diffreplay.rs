// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2-diffreplay`: orchestrate diff replay over app binaries that
//! implement the `replay` contract (README). Exit 1 when the report fails.

use std::path::{Path, PathBuf};

#[cfg(feature = "export")]
use anyhow::bail;
use clap::{Parser, Subcommand};
use uc_diffreplay::attribute::{Declaration, attribute};
use uc_diffreplay::confirm::{Verdicts, confirm};
#[cfg(feature = "export")]
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::diff::{Profile, diff};
use uc_diffreplay::pinverify;
use uc_diffreplay::report::Report;
use uc_diffreplay::trace::Trace;

#[derive(Parser)]
#[command(name = "uc2-diffreplay", version)]
struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Export a corpus from an instance directory.
    #[cfg(feature = "export")]
    Corpus {
        #[command(subcommand)]
        cmd: CorpusSub,
    },
    /// v_old vs v_new over one corpus, judged against a declaration.
    Upgrade {
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long)]
        old: PathBuf,
        #[arg(long)]
        new: PathBuf,
        #[arg(long)]
        declare: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
    /// One binary, two processes: the profile must be empty.
    Determinism {
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long)]
        bin: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
    /// One binary, two origins (artifact vs genesis): demonstrates §2.3.
    Reconstruction {
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long)]
        bin: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
    /// Spec §6.2 part 2: on a real node, with the app's real binaries, prove
    /// the pin refuses the stale binary and steers the new one onto the
    /// artifact path (never the genesis counterfactual).
    #[cfg(feature = "pin-verify")]
    PinVerify {
        #[arg(long)]
        corpus: PathBuf,
        /// The OLD service binary (the version running before the upgrade).
        #[arg(long)]
        old: PathBuf,
        /// The app's own arguments for its SERVE form — its serve verb, then
        /// its knobs (repeatable, and each one may itself begin with `-`).
        /// The `replay`/`project` forms reuse the knobs behind their own verb
        /// (`uc_diffreplay::pinverify::app_knobs`).
        #[arg(long = "old-arg", allow_hyphen_values = true)]
        old_args: Vec<String>,
        #[arg(long)]
        new: PathBuf,
        #[arg(long = "new-arg", allow_hyphen_values = true)]
        new_args: Vec<String>,
        #[arg(long)]
        app_id: String,
        /// The row's FSM name, as `[services] names` declares it.
        #[arg(long)]
        fsm: String,
        #[arg(long, default_value_t = 0)]
        row: u8,
        /// The packed version the pin names — what `uc2ctl upgrade pin --to`
        /// takes (`MAJOR.MINOR.PATCH`), or a raw packed integer.
        #[arg(long, value_parser = parse_version)]
        to: u32,
        /// MESSAGE frames re-submitted before the instant (default: half).
        #[arg(long)]
        split: Option<usize>,
        #[arg(long, default_value_t = 60)]
        timeout_secs: u64,
        /// Where the scratch instance dir and traces go (default:
        /// `<report>.pinverify/`). Given explicitly, it is never removed.
        #[arg(long)]
        scratch: Option<PathBuf>,
        #[arg(long)]
        report: PathBuf,
    },
}

/// What `--to` accepts: the `MAJOR.MINOR.PATCH` form `uc2ctl upgrade pin
/// --to` takes (`uc_ctl::upgrade::parse_semver`'s rule, copied rather than
/// depended on — this binary must not link the admin CLI), or a raw packed
/// `u32` (decimal, or `0x`-prefixed hex) for a state machine whose `VERSION`
/// is a bare integer rather than a packed semver — the harness's own
/// `DoublingRegisterSm::VERSION = 2` is one.
///
/// Packed `0` is the "unversioned" sentinel — what an unversioned row's cnc
/// word already reads — so a pin naming it could not be told from "no pin".
/// Refused in BOTH forms, with the same message `uc2ctl` gives.
#[cfg(feature = "pin-verify")]
fn parse_version(s: &str) -> Result<u32, String> {
    let v = if s.contains('.') {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(format!("version {s:?}: expected MAJOR.MINOR.PATCH"));
        }
        let major: u8 = parts[0]
            .parse()
            .map_err(|_| format!("version {s:?}: major must be 0..=255"))?;
        let minor: u8 = parts[1]
            .parse()
            .map_err(|_| format!("version {s:?}: minor must be 0..=255"))?;
        let patch: u16 = parts[2]
            .parse()
            .map_err(|_| format!("version {s:?}: patch must be 0..=65535"))?;
        uc_protocol::identity::pack_version(major, minor, patch)
    } else if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u32::from_str_radix(hex, 16).map_err(|_| format!("version {s:?}: not a packed u32"))?
    } else {
        s.parse::<u32>()
            .map_err(|_| format!("version {s:?}: not MAJOR.MINOR.PATCH and not a packed u32"))?
    };
    if v == 0 {
        return Err(format!(
            "version {s:?} packs to 0, the \"unversioned\" sentinel: --to must name a real version"
        ));
    }
    Ok(v)
}

#[cfg(feature = "export")]
#[derive(Subcommand)]
enum CorpusSub {
    Export {
        #[arg(long)]
        instance_dir: PathBuf,
        #[arg(long)]
        app_id: String,
        #[arg(long)]
        row: u8,
        #[arg(long)]
        from: Option<u64>,
        #[arg(long)]
        to: Option<u64>,
        #[arg(long)]
        around: Option<u64>,
        #[arg(long, default_value_t = 0)]
        version: u32,
        #[arg(long)]
        out: PathBuf,
    },
}

/// Run `<bin> replay --corpus C --out T [--from-genesis]` and read the trace
/// back. The three diff modes give the app no arguments of their own; only
/// `pin-verify` does (it runs one app binary in three forms), which is why
/// the helper itself lives in [`uc_diffreplay::pinverify`].
fn replay(bin: &Path, corpus: &Path, out: &Path, from_genesis: bool) -> anyhow::Result<Trace> {
    pinverify::run_replay(bin, &[], corpus, out, from_genesis)
}

fn finish(report: Report, path: &Path) -> anyhow::Result<()> {
    report.write_json(std::fs::File::create(path)?)?;
    report.write_text(std::io::stdout())?;
    if report.failed() {
        std::process::exit(1);
    }
    clear_traces(path);
    Ok(())
}

/// Drop the per-run traces on a PASS. They are EVIDENCE, so a failing run
/// keeps them (and a leftover directory is itself the signal that something
/// failed or was killed); a passing one has nothing to say and should leave
/// nothing behind.
fn clear_traces(report: &Path) {
    let _ = std::fs::remove_dir_all(traces_dir_of(report));
}

/// The declaration every non-`upgrade` mode judges against: EMPTY by
/// definition, so any divergence at all is a defect.
fn empty_declaration() -> anyhow::Result<Declaration> {
    Declaration::from_toml("[touched]\narms = []\n")
}

/// `diff → attribute → confirm`, the sequence every mode runs once it has
/// its two traces. `adjust` runs between `diff` and `attribute`, so a mode
/// can reshape the freshly computed profile before it is judged —
/// `reconstruction` uses it to blank a surface it deliberately did not
/// compare (see that arm) rather than letting an untouched `LineDiff`
/// silently read as "compared and equal".
fn judge(
    a: &Trace,
    b: &Trace,
    d: &Declaration,
    adjust: impl FnOnce(&mut Profile),
) -> anyhow::Result<(Profile, Verdicts)> {
    let mut profile = diff(a, b)?;
    adjust(&mut profile);
    let verdicts = confirm(&attribute(&profile, d), d);
    Ok((profile, verdicts))
}

fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        #[cfg(feature = "export")]
        Sub::Corpus {
            cmd:
                CorpusSub::Export {
                    instance_dir,
                    app_id,
                    row,
                    from,
                    to,
                    around,
                    version,
                    out,
                },
        } => {
            let c = match (from, to, around) {
                (Some(p), to, None) => Corpus::export(
                    &instance_dir,
                    &app_id,
                    row,
                    p,
                    to.unwrap_or(u64::MAX),
                    version,
                    &out,
                )?,
                (None, None, Some(pos)) => {
                    Corpus::export_around(&instance_dir, &app_id, row, pos, version, &out)?
                }
                _ => bail!("give --from [--to] or --around, not both"),
            };
            println!(
                "corpus at {} — row {} origin {} end {}",
                c.dir.display(),
                c.manifest.row,
                c.manifest.origin,
                c.manifest.end
            );
            Ok(())
        }
        Sub::Upgrade {
            corpus,
            old,
            new,
            declare,
            report,
        } => {
            let tmp = traces_dir_beside(&report)?;
            let a = replay(&old, &corpus, &tmp.join("old.json"), false)?;
            let b = replay(&new, &corpus, &tmp.join("new.json"), false)?;
            let d = Declaration::from_toml(&std::fs::read_to_string(&declare)?)?;
            let (profile, verdicts) = judge(&a, &b, &d, |_| {})?;
            finish(Report::new("upgrade", corpus, profile, verdicts), &report)
        }
        Sub::Determinism {
            corpus,
            bin,
            report,
        } => {
            let tmp = traces_dir_beside(&report)?;
            let a = replay(&bin, &corpus, &tmp.join("run1.json"), false)?;
            let b = replay(&bin, &corpus, &tmp.join("run2.json"), false)?;
            let d = empty_declaration()?;
            let (profile, verdicts) = judge(&a, &b, &d, |_| {})?;
            finish(
                Report::new("determinism", corpus, profile, verdicts),
                &report,
            )
        }
        Sub::Reconstruction {
            corpus,
            bin,
            report,
        } => {
            let tmp = traces_dir_beside(&report)?;
            let art = replay(&bin, &corpus, &tmp.join("artifact.json"), false)?;
            let mut genesis = replay(&bin, &corpus, &tmp.join("genesis.json"), true)?;
            // Align the spans for diff() (same origin/end/row) — but the
            // genesis run installs no artifact, so it has no origin state to
            // compare. Do NOT fake one by copying the artifact run's
            // projection over: leave it `None` and blank the resulting
            // `projection_origin` diff below, so an empty diff there reads
            // as "not applicable", never as "compared and found equal"
            // (spec §6.4 — the report is designed to be read).
            genesis.origin = art.origin;
            genesis.projection_at_origin = None;
            genesis.entries.retain(|e| e.pos >= art.origin);
            let d = empty_declaration()?;
            let (profile, verdicts) = judge(&art, &genesis, &d, |p| {
                p.projection_origin = Default::default();
            })?;
            let r = Report::new("reconstruction", corpus, profile, verdicts).with_note(
                "projection_origin: not applicable in reconstruction mode — the genesis run \
                 installs no artifact, so there is no origin state to compare",
            );
            // In this mode a NON-empty end-projection diff is the expected
            // demonstration (spec §6.2 part 1); report it, exit 0 either way.
            r.write_json(std::fs::File::create(&report)?)?;
            r.write_text(std::io::stdout())?;
            if !r.failed() {
                clear_traces(&report);
            }
            println!(
                "reconstruction: end projections {}",
                if r.profile.projection_end.is_empty() {
                    "AGREE (no semantic change below P)"
                } else {
                    "DIVERGE — the §2.3 counterfactual"
                }
            );
            Ok(())
        }
        #[cfg(feature = "pin-verify")]
        Sub::PinVerify {
            corpus,
            old,
            old_args,
            new,
            new_args,
            app_id,
            fsm,
            row,
            to,
            split,
            timeout_secs,
            scratch,
            report,
        } => {
            // An explicit `--scratch` is the operator's directory: never
            // removed, whatever the verdict. The default one is this run's
            // own, and it is EVIDENCE — kept on a FAIL (like `clear_traces`),
            // swept on a PASS or an INCONCLUSIVE.
            let keep = scratch.is_some();
            let a = pinverify::PinVerifyArgs {
                corpus,
                old,
                old_args,
                new,
                new_args,
                app_id,
                fsm,
                row,
                to,
                split,
                timeout: std::time::Duration::from_secs(timeout_secs),
                scratch,
                report: report.clone(),
            };
            let r = pinverify::run(&a)?;
            r.write_json(std::fs::File::create(&report)?)?;
            r.write_text(std::io::stdout())?;
            if r.failed() {
                std::process::exit(1);
            }
            if !keep {
                let _ = std::fs::remove_dir_all(pinverify::scratch_dir_of(&report));
            }
            Ok(())
        }
    }
}

/// Where a run's per-binary traces land: `<report>.traces/`, beside the
/// report itself, so the evidence for a failing run sits next to the report
/// that names it.
fn traces_dir_of(report: &Path) -> PathBuf {
    report.with_extension("traces")
}

fn traces_dir_beside(report: &Path) -> anyhow::Result<PathBuf> {
    let d = traces_dir_of(report);
    std::fs::create_dir_all(&d)?;
    Ok(d)
}

#[cfg(all(test, feature = "pin-verify"))]
mod tests {
    use super::parse_version;

    /// `--to` takes both spellings a real pin is written in: the
    /// `MAJOR.MINOR.PATCH` one `uc2ctl upgrade pin` takes, and the raw packed
    /// integer a state machine's `const VERSION` may be (the harness's
    /// `DoublingRegisterSm::VERSION = 2`). Packed `0` is the "unversioned"
    /// sentinel and is refused in BOTH spellings — `0.0.0` packs to the very
    /// word a bare `0` names.
    #[test]
    fn to_accepts_semver_and_packed_integers_but_never_zero() {
        assert_eq!(parse_version("1.2.3"), Ok(0x0102_0003));
        assert_eq!(parse_version("0.0.2"), Ok(2));
        assert_eq!(parse_version("2"), Ok(2));
        assert_eq!(parse_version("0x00010203"), Ok(0x0001_0203));
        assert_eq!(parse_version("4294967295"), Ok(u32::MAX));
        for zero in ["0.0.0", "0", "0x0"] {
            let e = parse_version(zero).unwrap_err();
            assert!(e.contains("unversioned"), "{zero}: {e}");
        }
        for bad in [
            "1.2",
            "1.2.3.4",
            "nope",
            "256.0.0",
            "0.0.65536",
            "-1",
            "0xzz",
        ] {
            assert!(parse_version(bad).is_err(), "{bad} should not parse");
        }
    }
}
