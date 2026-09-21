// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Two traces → a divergence profile (spec §4.3): every position and surface
//! on which the two runs disagreed, and what each produced. Never a boolean.

use std::collections::BTreeMap;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::trace::{Entry, Trace};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Surface {
    Response,
    Sched,
    ProjectionOrigin,
    ProjectionEnd,
    /// `ApplyCtx::ids_calls` — how many times this frame's apply/on_timer
    /// minted a generator (plan B2 T6, spec §8.1's determinism hazard).
    Ids,
    /// The `on_committed` result the driver recorded for this frame, when
    /// run with an output handler (plan B2 T6).
    Output,
}

impl Surface {
    /// The declaration-file spelling used in `[[expect]] surface = "..."`.
    /// `None` for anything else — callers validate at the parse boundary
    /// (`Declaration::from_toml`) rather than defaulting silently.
    pub fn parse(s: &str) -> Option<Surface> {
        Some(match s {
            "response" => Surface::Response,
            "sched" => Surface::Sched,
            "projection_origin" => Surface::ProjectionOrigin,
            "projection_end" => Surface::ProjectionEnd,
            "ids" => Surface::Ids,
            "output" => Surface::Output,
            _ => return None,
        })
    }

    /// The inverse of [`Surface::parse`], and the ONE spelling used
    /// everywhere a surface is written out: the declaration match in
    /// `confirm`, the text report, and the JSON (`rename_all =
    /// "snake_case"`). A round-trip test pins `parse(name()) == Some(self)`
    /// over all six, so the two can never drift apart.
    pub fn name(&self) -> &'static str {
        match self {
            Surface::Response => "response",
            Surface::Sched => "sched",
            Surface::ProjectionOrigin => "projection_origin",
            Surface::ProjectionEnd => "projection_end",
            Surface::Ids => "ids",
            Surface::Output => "output",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    pub pos: u64,
    pub tag: Vec<u8>,
    /// The timer id, when the entry at this position was a TIMER frame
    /// rather than a client command. A timer frame carries no application
    /// payload, so its `tag` is empty and `[tags]` can never name it —
    /// `[timers]` maps the id to an arm instead (`attribute`).
    #[serde(default)]
    pub timer_id: Option<u64>,
    pub surface: Surface,
    pub a: Vec<u8>,
    pub b: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct LineDiff {
    pub removed: Vec<String>,
    pub added: Vec<String>,
}

impl LineDiff {
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty()
    }

    /// Set difference over lines — projections are canonical (sorted, one
    /// record per line), so order carries no information and a multiset diff
    /// is exact.
    fn of(a: Option<&str>, b: Option<&str>) -> LineDiff {
        fn count(s: Option<&str>) -> BTreeMap<&str, usize> {
            let mut m: BTreeMap<&str, usize> = BTreeMap::new();
            for l in s.unwrap_or("").lines() {
                *m.entry(l).or_default() += 1;
            }
            m
        }
        let (ma, mb) = (count(a), count(b));
        let mut d = LineDiff::default();
        for (l, &na) in &ma {
            let nb = mb.get(l).copied().unwrap_or(0);
            for _ in nb..na {
                d.removed.push((*l).to_string());
            }
        }
        for (l, &nb) in &mb {
            let na = ma.get(l).copied().unwrap_or(0);
            for _ in na..nb {
                d.added.push((*l).to_string());
            }
        }
        d
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Profile {
    pub entries: Vec<Divergence>,
    pub projection_origin: LineDiff,
    pub projection_end: LineDiff,
    /// Positions one side dispatched and the other did not — a frontier or
    /// identity disagreement, always worth a look.
    pub only_in_a: Vec<u64>,
    pub only_in_b: Vec<u64>,
}

impl Profile {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.projection_origin.is_empty()
            && self.projection_end.is_empty()
            && self.only_in_a.is_empty()
            && self.only_in_b.is_empty()
    }
}

pub fn diff(a: &Trace, b: &Trace) -> anyhow::Result<Profile> {
    if (a.origin, a.end, a.row) != (b.origin, b.end, b.row) {
        bail!(
            "traces cover different spans: a=[{}, {}) row {} vs b=[{}, {}) row {}",
            a.origin,
            a.end,
            a.row,
            b.origin,
            b.end,
            b.row
        );
    }
    fn by_pos(t: &Trace) -> BTreeMap<u64, &Entry> {
        t.entries.iter().map(|e| (e.pos, e)).collect()
    }
    fn timer_id(e: &Entry) -> Option<u64> {
        match e.kind {
            crate::trace::EntryKind::Timer { id, .. } => Some(id),
            crate::trace::EntryKind::Message => None,
        }
    }
    let (ma, mb) = (by_pos(a), by_pos(b));
    let mut p = Profile {
        projection_origin: LineDiff::of(
            a.projection_at_origin.as_deref(),
            b.projection_at_origin.as_deref(),
        ),
        projection_end: LineDiff::of(
            a.projection_at_end.as_deref(),
            b.projection_at_end.as_deref(),
        ),
        ..Default::default()
    };
    for (&pos, ea) in &ma {
        let Some(eb) = mb.get(&pos) else {
            p.only_in_a.push(pos);
            continue;
        };
        if ea.response != eb.response {
            p.entries.push(Divergence {
                pos,
                tag: ea.tag.clone(),
                timer_id: timer_id(ea),
                surface: Surface::Response,
                a: ea.response.clone(),
                b: eb.response.clone(),
            });
        }
        if ea.sched != eb.sched {
            let enc = |s: &[crate::trace::Sched]| serde_json::to_vec(s).unwrap_or_default();
            p.entries.push(Divergence {
                pos,
                tag: ea.tag.clone(),
                timer_id: timer_id(ea),
                surface: Surface::Sched,
                a: enc(&ea.sched),
                b: enc(&eb.sched),
            });
        }
        if ea.ids_calls != eb.ids_calls {
            p.entries.push(Divergence {
                pos,
                tag: ea.tag.clone(),
                timer_id: timer_id(ea),
                surface: Surface::Ids,
                a: ea.ids_calls.to_string().into_bytes(),
                b: eb.ids_calls.to_string().into_bytes(),
            });
        }
        if ea.output != eb.output {
            let enc = |o: &Option<String>| serde_json::to_vec(o).unwrap_or_default();
            p.entries.push(Divergence {
                pos,
                tag: ea.tag.clone(),
                timer_id: timer_id(ea),
                surface: Surface::Output,
                a: enc(&ea.output),
                b: enc(&eb.output),
            });
        }
    }
    for &pos in mb.keys() {
        if !ma.contains_key(&pos) {
            p.only_in_b.push(pos);
        }
    }
    Ok(p)
}
