//! Top-K leaderboard with diversity-aware sampling.

use crate::task::Direction;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub variant_id: String,
    /// In Direction units (i.e. raw measured value; sort direction interpreted on insert).
    pub primary_metric: f64,
    pub diversity_tag: [u8; 32],
    pub hypothesis: String,
}

impl Eq for Entry {}

/// Top-K best entries (K = capacity), sorted best-first.
pub struct Leaderboard {
    entries: Vec<Entry>,
    cap: usize,
    dir: Direction,
}

impl Leaderboard {
    pub fn new(cap: usize, dir: Direction) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
            cap,
            dir,
        }
    }

    /// True iff `a` is strictly better than `b` under the configured direction.
    #[allow(dead_code)]
    fn better(&self, a: f64, b: f64) -> bool {
        match self.dir {
            Direction::Minimize => a < b,
            Direction::Maximize => a > b,
        }
    }

    pub fn insert(&mut self, e: Entry) {
        self.entries.push(e);
        let dir = self.dir;
        self.entries.sort_by(|a, b| match dir {
            Direction::Minimize => a.primary_metric.partial_cmp(&b.primary_metric).unwrap(),
            Direction::Maximize => b.primary_metric.partial_cmp(&a.primary_metric).unwrap(),
        });
        self.entries.truncate(self.cap);
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn best(&self) -> Option<&Entry> {
        self.entries.first()
    }

    /// Pick `n` entries from the leaderboard excluding `exclude_id`, preferring
    /// entries with *different* diversity tags from the best entry. Deterministic:
    /// scans top-down, skips entries sharing the best's tag until we run out of
    /// distinct-tag entries, then falls back to including same-tag ones.
    pub fn diverse_pick(&self, exclude_id: &str, n: usize) -> Vec<Entry> {
        let best_tag = self
            .entries
            .iter()
            .find(|e| e.variant_id == exclude_id)
            .map(|e| e.diversity_tag);
        let mut distinct = Vec::new();
        let mut same = Vec::new();
        for e in &self.entries {
            if e.variant_id == exclude_id {
                continue;
            }
            match best_tag {
                Some(t) if e.diversity_tag == t => same.push(e.clone()),
                _ => distinct.push(e.clone()),
            }
        }
        distinct.into_iter().chain(same).take(n).collect()
    }
}

/// Normalize Rust source for diversity hashing:
/// - strip line comments (`//...`) — does not handle string-literal `//` correctly,
///   but for diversity-tagging this is good enough.
/// - strip block comments (`/* ... */`) likewise approximate.
/// - collapse all whitespace runs to nothing (so `let a=1` == `let a = 1` == `let a =\n1`).
pub fn normalize_source(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    let mut in_block = false;
    let mut in_line = false;
    while let Some(c) = chars.next() {
        if in_line {
            if c == '\n' {
                in_line = false;
            }
            continue;
        }
        if in_block {
            if c == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block = false;
            }
            continue;
        }
        if c == '/' {
            match chars.peek() {
                Some('/') => {
                    chars.next();
                    in_line = true;
                    continue;
                }
                Some('*') => {
                    chars.next();
                    in_block = true;
                    continue;
                }
                _ => {}
            }
        }
        if !c.is_whitespace() {
            out.push(c);
        }
    }
    out
}

pub fn diversity_hash(src: &str) -> [u8; 32] {
    let n = normalize_source(src);
    let mut h = Sha256::new();
    h.update(n.as_bytes());
    h.finalize().into()
}
