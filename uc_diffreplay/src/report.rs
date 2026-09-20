// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The attributed diff report (spec §6.4): designed to be read, not just
//! checked. JSON for the skill; text for a human.

use std::io::Write;
use std::path::PathBuf;

use serde::Serialize;

use crate::confirm::{Finding, Verdict, Verdicts};
use crate::diff::Profile;

#[derive(Serialize, Debug, Clone, Default)]
pub struct Summary {
    pub pass: usize,
    pub undeclared: usize,
    pub unexplained: usize,
    pub absent: usize,
}

#[derive(Serialize, Debug, Clone)]
pub struct Report {
    pub mode: String,
    pub corpus: PathBuf,
    pub profile: Profile,
    pub findings: Vec<Finding>,
    pub summary: Summary,
}

impl Report {
    pub fn new(mode: &str, corpus: PathBuf, profile: Profile, verdicts: Verdicts) -> Report {
        let mut s = Summary::default();
        for f in &verdicts.findings {
            match f.verdict {
                Verdict::Pass => s.pass += 1,
                Verdict::Undeclared => s.undeclared += 1,
                Verdict::Unexplained => s.unexplained += 1,
                Verdict::Absent => s.absent += 1,
            }
        }
        Report {
            mode: mode.into(),
            corpus,
            profile,
            findings: verdicts.findings,
            summary: s,
        }
    }
    pub fn failed(&self) -> bool {
        self.summary.undeclared + self.summary.unexplained + self.summary.absent > 0
    }
    pub fn write_json(&self, w: impl Write) -> anyhow::Result<()> {
        Ok(serde_json::to_writer_pretty(w, self)?)
    }
    pub fn write_text(&self, mut w: impl Write) -> anyhow::Result<()> {
        writeln!(
            w,
            "diff replay — {} — corpus {}",
            self.mode,
            self.corpus.display()
        )?;
        writeln!(
            w,
            "  divergences: {} entries, origin projection {}−/{}+, end projection {}−/{}+",
            self.profile.entries.len(),
            self.profile.projection_origin.removed.len(),
            self.profile.projection_origin.added.len(),
            self.profile.projection_end.removed.len(),
            self.profile.projection_end.added.len()
        )?;
        for f in &self.findings {
            writeln!(
                w,
                "  {:<11} {:<18} arm={:<10} pos={:<8} {}",
                format!("{:?}", f.verdict),
                format!("{:?}", f.surface),
                f.arm.as_deref().unwrap_or("-"),
                f.pos.map(|p| p.to_string()).unwrap_or_else(|| "-".into()),
                f.note
            )?;
        }
        writeln!(
            w,
            "  {} pass, {} undeclared, {} unexplained, {} absent → {}",
            self.summary.pass,
            self.summary.undeclared,
            self.summary.unexplained,
            self.summary.absent,
            if self.failed() { "FAIL" } else { "PASS" }
        )?;
        Ok(())
    }
}
