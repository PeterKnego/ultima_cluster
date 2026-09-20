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

use crate::diff::{Divergence, Profile};

#[derive(Deserialize, Debug, Clone)]
pub struct Declaration {
    /// Hex of the command payload's leading bytes → arm name.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub touched: Touched,
    #[serde(default)]
    pub expect: Vec<Expect>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Touched {
    #[serde(default)]
    pub arms: Vec<String>,
    /// The state diff at the origin is expected to be non-empty (an image
    /// migration, spec §4.4).
    #[serde(default)]
    pub migration: bool,
}

#[derive(Deserialize, Debug, Clone)]
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
        toml::from_str(s).context("declaration TOML")
    }

    /// Longest hex prefix of `tag` that has a mapping wins, so `"01"` and
    /// `"0102"` can coexist.
    pub fn arm_of(&self, tag: &[u8]) -> Option<&str> {
        let hex: String = tag.iter().map(|b| format!("{b:02x}")).collect();
        (1..=hex.len())
            .rev()
            .filter(|n| n % 2 == 0)
            .find_map(|n| self.tags.get(&hex[..n]).map(String::as_str))
    }

    fn touched(&self, arm: &str) -> bool {
        self.touched.arms.iter().any(|a| a == arm)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribution {
    Arm(String),
    Migration,
    Unexplained,
}

#[derive(Debug, Clone, Default)]
pub struct Attributed {
    pub entries: Vec<(Divergence, Attribution)>,
    pub projection_origin: Option<Attribution>,
    pub projection_end: Option<Attribution>,
}

pub fn attribute(p: &Profile, d: &Declaration) -> Attributed {
    let mut out = Attributed::default();
    for div in &p.entries {
        let att = match d.arm_of(&div.tag) {
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
        out.projection_end = Some(match d.touched.arms.first() {
            Some(arm) => Attribution::Arm(arm.clone()),
            None if d.touched.migration => Attribution::Migration,
            None => Attribution::Unexplained,
        });
    }
    out
}
