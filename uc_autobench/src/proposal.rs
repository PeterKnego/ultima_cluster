//! Variant proposal type + static checks.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantProposal {
    pub hypothesis: String,
    pub rationale: String,
    #[serde(default)]
    pub expected_outcome: serde_json::Value,
    pub risk_notes: String,
    /// Full file contents keyed by repo-relative path.
    pub files: BTreeMap<PathBuf, String>,
}

#[derive(Debug)]
pub enum StaticCheckResult {
    Ok,
    Reject { reason: String },
}

/// Verify the proposal only touches paths in `mutable` and never touches paths
/// in `frozen`. Empty file maps are rejected.
pub fn static_checks(
    proposal: &VariantProposal,
    mutable: &[PathBuf],
    frozen: &[PathBuf],
) -> StaticCheckResult {
    if proposal.files.is_empty() {
        return StaticCheckResult::Reject {
            reason: "proposal touches zero files".into(),
        };
    }
    let mutable_set: std::collections::HashSet<&Path> =
        mutable.iter().map(|p| p.as_path()).collect();
    let frozen_set: std::collections::HashSet<&Path> =
        frozen.iter().map(|p| p.as_path()).collect();
    for path in proposal.files.keys() {
        if frozen_set.contains(path.as_path()) {
            return StaticCheckResult::Reject {
                reason: format!("touched frozen path: {}", path.display()),
            };
        }
        if !mutable_set.contains(path.as_path()) {
            return StaticCheckResult::Reject {
                reason: format!("touched non-mutable path: {}", path.display()),
            };
        }
    }
    StaticCheckResult::Ok
}
