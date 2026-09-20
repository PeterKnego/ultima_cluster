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

fn surface_name(s: Surface) -> &'static str {
    match s {
        Surface::Response => "response",
        Surface::Sched => "sched",
        Surface::ProjectionOrigin => "projection_origin",
        Surface::ProjectionEnd => "projection_end",
    }
}

fn matches(e: &Expect, s: Surface, arm: Option<&str>) -> bool {
    e.surface == surface_name(s) && (e.arm.is_none() || e.arm.as_deref() == arm)
}

pub fn confirm(att: &Attributed, d: &Declaration) -> Verdicts {
    let mut v = Verdicts::default();
    let mut satisfied = vec![false; d.expect.len()];

    let mut judge = |surface: Surface, pos: Option<u64>, a: &Attribution, v: &mut Verdicts| {
        let arm = match a {
            Attribution::Arm(s) => Some(s.as_str()),
            Attribution::Migration => Some("migration"),
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
        match d.expect.iter().position(|e| matches(e, surface, arm)) {
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
    if let Some(a) = &att.projection_origin {
        judge(Surface::ProjectionOrigin, None, a, &mut v);
    }
    if let Some(a) = &att.projection_end {
        judge(Surface::ProjectionEnd, None, a, &mut v);
    }
    for (i, e) in d.expect.iter().enumerate() {
        if !satisfied[i] {
            let surface = match e.surface.as_str() {
                "response" => Surface::Response,
                "sched" => Surface::Sched,
                "projection_origin" => Surface::ProjectionOrigin,
                _ => Surface::ProjectionEnd,
            };
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
