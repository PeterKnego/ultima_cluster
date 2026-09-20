// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Spec §4.5, the MECHANICAL attribution pass: partition the profile by
//! command tag and surface, the code change by touched arm (from the
//! declaration — drafted by the skill, owned by the developer), and
//! cross-tabulate. What this cannot name is `Unexplained`, and goes to the
//! semantic pass (the skill).

use std::collections::BTreeMap;

use anyhow::Context;
use serde::Deserialize;

use crate::diff::{Divergence, Profile, Surface};

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    /// Hex of the command payload's leading bytes → arm name. Keys are
    /// lowercased at parse (`from_toml`), so `"0A"` and `"0a"` name the
    /// same byte.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Timer id (decimal, as a string — TOML keys are strings) → arm name,
    /// e.g. `[timers] "9" = "reaper"`. A TIMER frame carries no application
    /// payload, so `[tags]` cannot reach it and a timer divergence would
    /// otherwise be permanently `Unexplained`.
    #[serde(default)]
    pub timers: BTreeMap<String, String>,
    /// How many leading tag bytes are FRAMEWORK envelope rather than
    /// application bytes — dropped before the hex prefixes in `[tags]` are
    /// matched. A `Sessioned<S>` service puts its 16-byte `client_id ‖ seq`
    /// envelope ahead of the app's own frame (`uc_service::session`,
    /// `SESSION_HEADER_LEN`), so `tag_offset = 16` is what makes `[tags]`
    /// name the app's op bytes instead of a client id. Default 0: a raw SM
    /// whose payload IS the application frame.
    #[serde(default)]
    pub tag_offset: usize,
    pub touched: Touched,
    #[serde(default)]
    pub expect: Vec<Expect>,
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Touched {
    #[serde(default)]
    pub arms: Vec<String>,
    /// The state diff at the origin is expected to be non-empty (an image
    /// migration, spec §4.4).
    #[serde(default)]
    pub migration: bool,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    /// `response` | `sched` | `projection_origin` | `projection_end`
    pub surface: String,
    #[serde(default)]
    pub arm: Option<String>,
    #[serde(default)]
    pub note: String,
}

impl Declaration {
    pub fn from_toml(s: &str) -> anyhow::Result<Declaration> {
        let mut d: Declaration = toml::from_str(s).context("declaration TOML")?;
        // `arm_of` formats tag bytes as lowercase hex, so an uppercase key
        // would silently never match. Normalise once, here, rather than at
        // every lookup.
        d.tags = d
            .tags
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        for e in &d.expect {
            let Some(surface) = Surface::parse(&e.surface) else {
                anyhow::bail!(
                    "declaration: unknown surface \"{}\" in [[expect]]; expected \
                     response | sched | projection_origin | projection_end",
                    e.surface
                );
            };
            // A projection is one whole-state comparison, attributed to the
            // change's touched set as a whole (`Attribution::Touched`) —
            // there is no per-arm projection to name, so an `arm` here would
            // read as a promise the harness cannot keep.
            if matches!(surface, Surface::ProjectionOrigin | Surface::ProjectionEnd)
                && e.arm.is_some()
            {
                anyhow::bail!(
                    "declaration: [[expect]] surface = \"{}\" takes no arm — projections are \
                     attributed to the touched set as a whole",
                    e.surface
                );
            }
        }
        Ok(d)
    }

    /// Longest hex prefix of `tag` that has a mapping wins, so `"01"` and
    /// `"0102"` can coexist. The leading `tag_offset` bytes are dropped
    /// first (the framework envelope, if any); an offset past the tag's end
    /// leaves nothing to match, which is `None` rather than a panic.
    pub fn arm_of(&self, tag: &[u8]) -> Option<&str> {
        let hex: String = tag[self.tag_offset.min(tag.len())..]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        (1..=hex.len())
            .rev()
            .filter(|n| n % 2 == 0)
            .find_map(|n| self.tags.get(&hex[..n]).map(String::as_str))
    }

    /// The arm a TIMER frame belongs to, from `[timers]`. The id is matched
    /// as its decimal spelling, which is how a TOML key can carry an
    /// integer.
    pub fn arm_of_timer(&self, id: u64) -> Option<&str> {
        self.timers.get(&id.to_string()).map(String::as_str)
    }

    fn touched(&self, arm: &str) -> bool {
        self.touched.arms.iter().any(|a| a == arm)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribution {
    Arm(String),
    /// The change's touched set AS A WHOLE. A projection is one comparison
    /// over the entire state — no single arm owns it, and picking one (the
    /// first declared, say) would name an arm the diff never implicated.
    Touched,
    Migration,
    Unexplained,
}

#[derive(Debug, Clone, Default)]
pub struct Attributed {
    pub entries: Vec<(Divergence, Attribution)>,
    pub projection_origin: Option<Attribution>,
    pub projection_end: Option<Attribution>,
    /// Carried through from the [`Profile`] so `confirm` can report them:
    /// a position one build dispatched and the other did not is a frontier
    /// or identity disagreement, never explained by an arm.
    pub only_in_a: Vec<u64>,
    pub only_in_b: Vec<u64>,
}

pub fn attribute(p: &Profile, d: &Declaration) -> Attributed {
    let mut out = Attributed {
        only_in_a: p.only_in_a.clone(),
        only_in_b: p.only_in_b.clone(),
        ..Default::default()
    };
    for div in &p.entries {
        // A timer frame has no application payload to tag, so its arm comes
        // from `[timers]` keyed on the timer id; everything else is tagged.
        let arm = match div.timer_id {
            Some(id) => d.arm_of_timer(id),
            None => d.arm_of(&div.tag),
        };
        let att = match arm {
            Some(arm) if d.touched(arm) => Attribution::Arm(arm.to_string()),
            _ => Attribution::Unexplained,
        };
        out.entries.push((div.clone(), att));
    }
    if !p.projection_origin.is_empty() {
        out.projection_origin = Some(if d.touched.migration {
            Attribution::Migration
        } else {
            Attribution::Unexplained
        });
    }
    if !p.projection_end.is_empty() {
        out.projection_end = Some(if !d.touched.arms.is_empty() {
            Attribution::Touched
        } else if d.touched.migration {
            Attribution::Migration
        } else {
            Attribution::Unexplained
        });
    }
    out
}
