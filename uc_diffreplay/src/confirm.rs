// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Spec §4.6: observed × attributed × declared → a verdict per entry.
//! A failure here is a FINDING, not a verdict on the change (S5).

use serde::Serialize;

use crate::attribute::{Attributed, Attribution, Declaration, Expect};
use crate::diff::Surface;

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Undeclared,
    Unexplained,
    Absent,
}

#[derive(Serialize, Debug, Clone)]
pub struct Finding {
    pub surface: Surface,
    pub arm: Option<String>,
    pub pos: Option<u64>,
    pub verdict: Verdict,
    pub note: String,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct Verdicts {
    pub findings: Vec<Finding>,
}

impl Verdicts {
    pub fn failed(&self) -> bool {
        self.findings.iter().any(|f| f.verdict != Verdict::Pass)
    }
}

fn matches(e: &Expect, s: Surface, arm: Option<&str>) -> bool {
    e.surface == s.name() && (e.arm.is_none() || e.arm.as_deref() == arm)
}

/// The most-specific `Expect` matching `(surface, arm)`: an entry whose
/// `arm` equals the observed arm beats a wildcard (`arm = None`) entry on
/// the same surface, regardless of declaration order — so a wildcard
/// declared before a specific entry never steals its match. Among
/// equally-specific candidates, prefer one not yet `satisfied`; otherwise
/// the first in declaration order.
fn best_expect(
    d: &Declaration,
    satisfied: &[bool],
    surface: Surface,
    arm: Option<&str>,
) -> Option<usize> {
    let candidates: Vec<usize> = d
        .expect
        .iter()
        .enumerate()
        .filter(|(_, e)| matches(e, surface, arm))
        .map(|(i, _)| i)
        .collect();
    let most_specific = candidates.iter().any(|&i| d.expect[i].arm.is_some());
    let ranked: Vec<usize> = candidates
        .into_iter()
        .filter(|&i| d.expect[i].arm.is_some() == most_specific)
        .collect();
    ranked
        .iter()
        .copied()
        .find(|&i| !satisfied[i])
        .or_else(|| ranked.first().copied())
}

pub fn confirm(att: &Attributed, d: &Declaration) -> Verdicts {
    let mut v = Verdicts::default();
    let mut satisfied = vec![false; d.expect.len()];

    let mut judge = |surface: Surface, pos: Option<u64>, a: &Attribution, v: &mut Verdicts| {
        let arm = match a {
            Attribution::Arm(s) => Some(s.as_str()),
            Attribution::Migration => Some("migration"),
            // The touched set as a whole names no single arm, so it matches
            // only an `[[expect]]` that names none either — which
            // `Declaration::from_toml` makes the only legal shape on a
            // projection surface.
            Attribution::Touched => None,
            Attribution::Unexplained => {
                v.findings.push(Finding {
                    surface,
                    arm: None,
                    pos,
                    verdict: Verdict::Unexplained,
                    note: "no touched arm explains this".into(),
                });
                return;
            }
        };
        match best_expect(d, &satisfied, surface, arm) {
            Some(i) => {
                satisfied[i] = true;
                v.findings.push(Finding {
                    surface,
                    arm: arm.map(String::from),
                    pos,
                    verdict: Verdict::Pass,
                    note: d.expect[i].note.clone(),
                });
            }
            None => v.findings.push(Finding {
                surface,
                arm: arm.map(String::from),
                pos,
                verdict: Verdict::Undeclared,
                note: "observed and attributed, but not declared".into(),
            }),
        }
    };

    for (div, a) in &att.entries {
        judge(div.surface, Some(div.pos), a, &mut v);
    }
    // A position only one build dispatched is not a value difference on a
    // surface — it is a disagreement about WHICH frames the FSM saw, which
    // no arm can explain. Report one finding per position so the diff is
    // never silently narrower than the profile (spec §6.4).
    for (positions, side) in [(&att.only_in_a, "only_in_a"), (&att.only_in_b, "only_in_b")] {
        for &pos in positions {
            v.findings.push(Finding {
                surface: Surface::Response,
                arm: None,
                pos: Some(pos),
                verdict: Verdict::Unexplained,
                note: format!("position dispatched by one build only ({side})"),
            });
        }
    }
    if let Some(a) = &att.projection_origin {
        judge(Surface::ProjectionOrigin, None, a, &mut v);
    }
    if let Some(a) = &att.projection_end {
        judge(Surface::ProjectionEnd, None, a, &mut v);
    }
    for (i, e) in d.expect.iter().enumerate() {
        if !satisfied[i] {
            let surface = Surface::parse(&e.surface).expect(
                "declaration validated at parse time: every Expect.surface is one of \
                 response | sched | projection_origin | projection_end | ids | output",
            );
            v.findings.push(Finding {
                surface,
                arm: e.arm.clone(),
                pos: None,
                verdict: Verdict::Absent,
                note: format!("declared but not observed: {}", e.note),
            });
        }
    }
    v
}
