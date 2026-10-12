//! Backup-tier 3b spec §6.1: the read-only handle a node holds on a tier it
//! may fetch snapshot sets from (`[tier_source]`, or its own `[backup]`
//! target). It knows the tier is this cluster's and which committed `through`
//! the cluster has registered for the tier's id; it never writes.

use super::TierError;
use super::target::{BackupTarget, FsTarget, read_tier};
use std::path::Path;
use std::sync::Arc;
use uc_protocol::v2::backup::BackupTierEntry;

pub struct TierSource {
    target: Arc<FsTarget>,
    app_id: String,
}

impl TierSource {
    /// Opens `TIER`: a present `TIER` of another `app_id` is
    /// `Err(SourceForeign)` (the startup refusal `tier_source_foreign`);
    /// absent or unreadable is `Ok` — the tier id is learned lazily.
    pub fn open(path: &Path, app_id: &str) -> Result<TierSource, TierError> {
        let target = Arc::new(FsTarget::new(path));
        if let Ok(Some(info)) = read_tier(target.as_ref())
            && info.app_id != app_id
        {
            return Err(TierError::SourceForeign {
                path: path.display().to_string(),
                tier_app_id: info.app_id,
                app_id: app_id.to_string(),
            });
        }
        Ok(TierSource {
            target,
            app_id: app_id.to_string(),
        })
    }

    /// Why this source cannot serve right now (no `TIER` yet, or unreadable);
    /// `None` when `TIER` reads and is this cluster's.
    pub fn unavailable(&self) -> Option<String> {
        match read_tier(self.target.as_ref()) {
            Ok(Some(i)) if i.app_id == self.app_id => None,
            Ok(Some(_)) => Some("TIER names another app_id".into()),
            Ok(None) => Some("no TIER on the target".into()),
            Err(e) => Some(e.to_string()),
        }
    }

    /// The committed `through` for this tier, or `None` when `TIER` is
    /// unreadable, foreign, or its id is not registered in `backups`.
    /// Re-reads `TIER` on each call, so a tier mounted later works.
    pub fn registered_through(&self, backups: &[BackupTierEntry]) -> Option<u64> {
        let info = read_tier(self.target.as_ref()).ok().flatten()?;
        if info.app_id != self.app_id {
            return None;
        }
        backups
            .iter()
            .find(|e| e.tier_id == info.tier_id)
            .map(|e| e.through)
    }

    pub fn target(&self) -> &dyn BackupTarget {
        self.target.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tier::layout::{TIER_FILE, TierInfo};

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .unwrap()
    }

    fn put_tier(dir: &Path, app: &str, id: u64) {
        let t = TierInfo {
            app_id: app.into(),
            tier_id: id,
            created_ns: 1,
        };
        std::fs::write(dir.join(TIER_FILE), t.render()).unwrap();
    }

    fn entry(tier_id: u64, through: u64) -> BackupTierEntry {
        BackupTierEntry {
            tier_id,
            oldest_set: 0,
            newest_set: 0,
            through,
            reporter: 1,
            updated_pos: 0,
        }
    }

    #[test]
    fn open_refuses_a_foreign_tier_and_tolerates_an_absent_one() {
        let d = tmp();
        let s = TierSource::open(d.path(), "mine").expect("absent TIER is tolerated");
        assert!(s.unavailable().is_some());
        assert_eq!(s.registered_through(&[entry(7, 100)]), None);
        // A tier mounted later works without reopening.
        put_tier(d.path(), "mine", 7);
        assert_eq!(s.unavailable(), None);
        assert_eq!(s.registered_through(&[entry(7, 100)]), Some(100));
        let f = tmp();
        put_tier(f.path(), "other", 7);
        let e = TierSource::open(f.path(), "mine").err().expect("foreign");
        assert!(e.to_string().contains("tier_source_foreign"), "{e}");
        assert!(!e.to_string().contains("backup_target_foreign"), "{e}");
        // A nonexistent path is also tolerated.
        TierSource::open(&d.path().join("nope"), "mine").unwrap();
    }

    #[test]
    fn registered_through_is_none_for_an_unregistered_tier() {
        let d = tmp();
        put_tier(d.path(), "mine", 7);
        let s = TierSource::open(d.path(), "mine").unwrap();
        assert_eq!(s.registered_through(&[]), None);
        assert_eq!(s.registered_through(&[entry(9, 5)]), None);
        assert_eq!(s.registered_through(&[entry(9, 5), entry(7, 42)]), Some(42));
    }
}
