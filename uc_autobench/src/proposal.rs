//! Variant proposal type + static checks.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
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
    let frozen_set: std::collections::HashSet<&Path> = frozen.iter().map(|p| p.as_path()).collect();
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

/// Captured pre-patch state. `None` = file did not exist before.
pub struct FileSnapshot {
    pub files: HashMap<PathBuf, Option<String>>,
}

pub fn snapshot_files(root: &Path, paths: &[PathBuf]) -> anyhow::Result<FileSnapshot> {
    let mut files = HashMap::new();
    for rel in paths {
        let abs = root.join(rel);
        let content = match fs::read_to_string(&abs) {
            Ok(s) => Some(s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        files.insert(rel.clone(), content);
    }
    Ok(FileSnapshot { files })
}

pub fn restore_snapshot(root: &Path, snap: &FileSnapshot) -> anyhow::Result<()> {
    for (rel, original) in &snap.files {
        let abs = root.join(rel);
        match original {
            Some(content) => fs::write(&abs, content)?,
            None => {
                if abs.exists() {
                    fs::remove_file(&abs)?;
                }
            }
        }
    }
    Ok(())
}

/// Apply the proposal's files to disk, overwriting in place. Caller is
/// responsible for snapshotting first if revert may be needed.
pub fn apply_patch(root: &Path, proposal: &VariantProposal) -> anyhow::Result<()> {
    for (rel, content) in &proposal.files {
        let abs = root.join(rel);
        if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&abs, content)?;
    }
    Ok(())
}
