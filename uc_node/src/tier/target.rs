//! The backup target: a write-once object store. `FsTarget` is the
//! filesystem implementation (a mounted path); the trait is the seam for
//! object stores.
use super::TierError;
use super::layout::{TIER_FILE, TierInfo};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub enum Put {
    Written(u64),
    Exists,
}

pub trait BackupTarget: Send + Sync {
    /// Write `path` iff absent. The callback fills a scratch file and returns
    /// the stored length.
    fn put_if_absent(
        &self,
        path: &str,
        write: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
    ) -> Result<Put, TierError>;
    /// Entry names (not paths), sorted ascending; an absent dir is empty.
    fn list(&self, dir: &str) -> Result<Vec<String>, TierError>;
    /// Absent is `None`.
    fn open(&self, path: &str) -> Result<Option<File>, TierError>;
    /// Absent is `Ok`.
    fn delete(&self, path: &str) -> Result<(), TierError>;
    fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError>;
}

pub struct FsTarget {
    root: PathBuf,
}

const TMP: &str = ".tmp";

fn fsync_dir(p: &Path) -> Result<(), TierError> {
    File::open(p)?.sync_all()?;
    Ok(())
}

impl FsTarget {
    pub fn new(root: impl Into<PathBuf>) -> FsTarget {
        FsTarget { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Remove `.tmp` entries last modified more than `older_than` ago (crash
    /// residue); `Duration::ZERO` removes all. Returns how many.
    pub fn clean_tmp(&self, older_than: Duration) -> Result<usize, TierError> {
        let rd = match std::fs::read_dir(self.root.join(TMP)) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        let mut n = 0;
        for ent in rd {
            let ent = ent?;
            let age = ent.metadata()?.modified()?.elapsed().unwrap_or_default();
            if age >= older_than {
                match std::fs::remove_file(ent.path()) {
                    Ok(()) => n += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(n)
    }
}

impl BackupTarget for FsTarget {
    fn put_if_absent(
        &self,
        path: &str,
        write: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
    ) -> Result<Put, TierError> {
        let tmp_dir = self.root.join(TMP);
        std::fs::create_dir_all(&tmp_dir)?;
        let tmp = tmp_dir.join(format!("{:016x}", rand::random::<u64>()));
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let res = write(&mut f).and_then(|stored| {
            f.sync_all()?;
            Ok(stored)
        });
        let stored = match res {
            Ok(s) => s,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        };
        drop(f);
        let fin = self.root.join(path);
        let parent = fin.parent().unwrap_or(&self.root).to_path_buf();
        std::fs::create_dir_all(&parent)?;
        // hard_link fails with AlreadyExists when the final name exists: the
        // atomic "iff absent" (rename would silently replace).
        let linked = std::fs::hard_link(&tmp, &fin);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                fsync_dir(&parent)?;
                Ok(Put::Written(stored))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(Put::Exists),
            Err(e) => Err(e.into()),
        }
    }

    fn list(&self, dir: &str) -> Result<Vec<String>, TierError> {
        let rd = match std::fs::read_dir(self.root.join(dir)) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut v = Vec::new();
        for ent in rd {
            v.push(ent?.file_name().to_string_lossy().into_owned());
        }
        v.sort();
        Ok(v)
    }

    fn open(&self, path: &str) -> Result<Option<File>, TierError> {
        match File::open(self.root.join(path)) {
            Ok(f) => Ok(Some(f)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn delete(&self, path: &str) -> Result<(), TierError> {
        let p = self.root.join(path);
        match std::fs::remove_file(&p) {
            Ok(()) => {
                if let Some(parent) = p.parent() {
                    let _ = fsync_dir(parent);
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError> {
        match std::fs::remove_dir(self.root.join(dir)) {
            Ok(()) => Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// The tier's identity file, or `None` on a fresh target.
pub fn read_tier(t: &dyn BackupTarget) -> Result<Option<TierInfo>, TierError> {
    let Some(mut f) = t.open(TIER_FILE)? else {
        return Ok(None);
    };
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    TierInfo::parse(&s).map(Some)
}

/// Read the target's `TIER`, creating it (write-once, so two racing writers
/// agree) when absent. A different `app_id` is refused by name.
pub fn open_or_create_tier(
    t: &dyn BackupTarget,
    app_id: &str,
    now_ns: u64,
) -> Result<TierInfo, TierError> {
    let check = |info: TierInfo| {
        if info.app_id == app_id {
            Ok(info)
        } else {
            Err(TierError::Foreign(
                TIER_FILE.to_string(),
                info.app_id,
                app_id.to_string(),
            ))
        }
    };
    if let Some(info) = read_tier(t)? {
        return check(info);
    }
    let text = TierInfo {
        app_id: app_id.to_string(),
        tier_id: rand::random::<u64>() | 1,
        created_ns: now_ns,
    }
    .render();
    t.put_if_absent(TIER_FILE, &mut |f| {
        f.write_all(text.as_bytes())?;
        Ok(text.len() as u64)
    })?;
    // Whoever won the race, the file on the target is the truth.
    let info = read_tier(t)?.ok_or_else(|| TierError::Corrupt {
        what: TIER_FILE.to_string(),
        detail: "vanished after create".to_string(),
    })?;
    check(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    #[test]
    fn put_if_absent_is_write_once_and_crash_residue_is_cleaned() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        let w = |b: &'static [u8]| {
            move |f: &mut File| {
                f.write_all(b)?;
                Ok(b.len() as u64)
            }
        };
        assert!(matches!(
            t.put_if_absent("sets/1/x", &mut w(b"one")).unwrap(),
            Put::Written(3)
        ));
        assert!(matches!(
            t.put_if_absent("sets/1/x", &mut w(b"two")).unwrap(),
            Put::Exists
        ));
        assert_eq!(std::fs::read(d.path().join("sets/1/x")).unwrap(), b"one");
        assert!(t.list(".tmp").unwrap().is_empty(), "Exists removes its tmp");
        std::fs::write(d.path().join(".tmp/stale"), b"x").unwrap();
        assert_eq!(t.clean_tmp(Duration::ZERO).unwrap(), 1);
    }

    #[test]
    fn a_foreign_tier_is_refused_by_name() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        let a = open_or_create_tier(&t, "kv", 1).unwrap();
        assert_eq!(
            open_or_create_tier(&t, "kv", 2).unwrap(),
            a,
            "second writer reads it"
        );
        let e = open_or_create_tier(&t, "other", 3).unwrap_err();
        assert!(e.to_string().contains("backup_target_foreign"), "{e}");
    }
}
