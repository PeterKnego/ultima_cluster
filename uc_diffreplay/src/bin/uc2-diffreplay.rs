// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2-diffreplay`: orchestrate diff replay over app binaries that
//! implement the `replay` contract (README). Exit 1 when the report fails.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use uc_diffreplay::attribute::{Declaration, attribute};
use uc_diffreplay::confirm::{Verdicts, confirm};
#[cfg(feature = "export")]
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::diff::{Profile, diff};
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

/// Run `<bin> replay --corpus C --out T [--from-genesis] [extra…]` and read the trace back.
fn replay(
    bin: &Path,
    corpus: &Path,
    out: &Path,
    from_genesis: bool,
    extra: &[&str],
) -> anyhow::Result<Trace> {
    let mut c = Command::new(bin);
    c.arg("replay")
        .arg("--corpus")
        .arg(corpus)
        .arg("--out")
        .arg(out);
    if from_genesis {
        c.arg("--from-genesis");
    }
    c.args(extra);
    let st = c
        .status()
        .with_context(|| format!("spawn {}", bin.display()))?;
    if !st.success() {
        bail!("{} replay exited {st}", bin.display());
    }
    Trace::read_json(std::fs::File::open(out)?)
}

fn finish(report: Report, path: &Path) -> anyhow::Result<()> {
    report.write_json(std::fs::File::create(path)?)?;
    report.write_text(std::io::stdout())?;
    if report.failed() {
        std::process::exit(1);
    }
    Ok(())
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
            let tmp = tempfile_dir(&report)?;
            let a = replay(&old, &corpus, &tmp.join("old.json"), false, &[])?;
            let b = replay(&new, &corpus, &tmp.join("new.json"), false, &[])?;
            let d = Declaration::from_toml(&std::fs::read_to_string(&declare)?)?;
            let (profile, verdicts) = judge(&a, &b, &d, |_| {})?;
            finish(Report::new("upgrade", corpus, profile, verdicts), &report)
        }
        Sub::Determinism {
            corpus,
            bin,
            report,
        } => {
            let tmp = tempfile_dir(&report)?;
            let a = replay(&bin, &corpus, &tmp.join("run1.json"), false, &[])?;
            let b = replay(&bin, &corpus, &tmp.join("run2.json"), false, &[])?;
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
            let tmp = tempfile_dir(&report)?;
            let art = replay(&bin, &corpus, &tmp.join("artifact.json"), false, &[])?;
            let mut genesis = replay(&bin, &corpus, &tmp.join("genesis.json"), true, &[])?;
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
    }
}

fn tempfile_dir(beside: &Path) -> anyhow::Result<PathBuf> {
    let d = beside.with_extension("traces");
    std::fs::create_dir_all(&d)?;
    Ok(d)
}
