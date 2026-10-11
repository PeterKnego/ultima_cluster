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
    /// Remove `.tmp` entries older than `older_than` (crash residue, spec
    /// §4.4); returns how many. A target with no scratch area keeps the
    /// default, which removes nothing.
    fn clean_tmp(&self, older_than: Duration) -> Result<usize, TierError> {
        let _ = older_than;
        Ok(0)
    }
    /// How long ago anything in `dir` last changed — the newest
    /// modification time over the directory itself and its entries — or
    /// `None` when absent or unknown. Retention removes an incomplete set
    /// directory only when this is old (spec §7.2), so an in-flight copy is
    /// never raced; a target that cannot tell keeps the default, and
    /// retention then never removes one.
    fn newest_entry_age(&self, dir: &str) -> Result<Option<Duration>, TierError> {
        let _ = dir;
        Ok(None)
    }
}

pub struct FsTarget {
    root: PathBuf,
}

const TMP: &str = ".tmp";

fn fsync_dir(p: &Path) -> Result<(), TierError> {
    File::open(p)?.sync_all()?;
    Ok(())
}

/// C5: `link(2)` failures that mean the filesystem cannot hard-link at all
/// (SMB/CIFS and some FUSE mounts answer `EPERM`/`EOPNOTSUPP`; `.tmp` on
/// another mount than the object answers `EXDEV`) — never a transient.
fn no_hardlinks(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EPERM) | Some(libc::EXDEV) | Some(libc::EOPNOTSUPP)
    )
}

impl FsTarget {
    pub fn new(root: impl Into<PathBuf>) -> FsTarget {
        FsTarget { root: root.into() }
    }

    /// `create_dir_all` may have made several ancestors: fsync every
    /// directory from `leaf` up to the root so none of the new entries can
    /// be lost to a crash.
    fn fsync_up(&self, leaf: &Path) -> Result<(), TierError> {
        let mut d = leaf;
        loop {
            fsync_dir(d)?;
            if d == self.root {
                break;
            }
            match d.parent() {
                Some(p) if p.starts_with(&self.root) => d = p,
                _ => break,
            }
        }
        Ok(())
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
        let placed = std::fs::create_dir_all(&parent).and_then(|()| {
            // hard_link fails with AlreadyExists when the final name exists:
            // the atomic "iff absent" (rename would silently replace).
            std::fs::hard_link(&tmp, &fin)
        });
        let _ = std::fs::remove_file(&tmp);
        match placed {
            Ok(()) => {
                self.fsync_up(&parent)?;
                Ok(Put::Written(stored))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // C1: the entry may be another writer's that is not durable
                // yet (it linked it and has not fsynced the directories).
                // `Exists` lets this writer advance its progress — and so its
                // node's purge cap — on that entry, so make it durable first.
                self.fsync_up(&parent)?;
                Ok(Put::Exists)
            }
            Err(e) if no_hardlinks(&e) => Err(TierError::NoHardlinks(
                self.root.display().to_string(),
                e.to_string(),
            )),
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

    fn clean_tmp(&self, older_than: Duration) -> Result<usize, TierError> {
        FsTarget::clean_tmp(self, older_than)
    }

    fn newest_entry_age(&self, dir: &str) -> Result<Option<Duration>, TierError> {
        let path = self.root.join(dir);
        let mut newest = match std::fs::metadata(&path) {
            Ok(m) => m.modified()?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Another writer's retention may remove the directory between the
        // two calls: that is "absent", not a failed pass.
        let rd = match std::fs::read_dir(&path) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        for ent in rd {
            let m = match ent?.metadata() {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            newest = newest.max(m.modified()?);
        }
        // A clock that reads before the mtime: treat as just changed.
        Ok(Some(newest.elapsed().unwrap_or_default()))
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
    fn a_failing_callback_leaves_nothing_behind() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        let e = t
            .put_if_absent("sets/1/x", &mut |f: &mut File| {
                f.write_all(b"partial")?;
                Err(TierError::Refused("boom".into()))
            })
            .map(|_| ())
            .unwrap_err();
        assert!(e.to_string().contains("boom"));
        assert!(!d.path().join("sets/1/x").exists());
        assert!(t.list(".tmp").unwrap().is_empty());
    }

    #[test]
    fn two_racing_creators_agree_on_one_tier() {
        let d = tmp();
        let t = std::sync::Arc::new(FsTarget::new(d.path()));
        let gate = std::sync::Arc::new(std::sync::Barrier::new(2));
        let hs: Vec<_> = (1..=2u64)
            .map(|i| {
                let (t, gate) = (t.clone(), gate.clone());
                std::thread::spawn(move || {
                    gate.wait();
                    open_or_create_tier(&*t, "kv", i).unwrap()
                })
            })
            .collect();
        let a: Vec<_> = hs.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(a[0], a[1]);
        assert_eq!(read_tier(&*t).unwrap().unwrap(), a[0]);
    }

    /// C5: the `link(2)` errors that mean "this filesystem cannot
    /// hard-link" are named; anything else stays a generic I/O failure.
    #[test]
    fn hard_link_refusals_are_named_no_hardlinks() {
        let e = |n| std::io::Error::from_raw_os_error(n);
        for n in [libc::EPERM, libc::EXDEV, libc::EOPNOTSUPP, libc::ENOTSUP] {
            assert!(no_hardlinks(&e(n)), "{n}");
        }
        for n in [libc::EACCES, libc::ENOSPC, libc::EIO] {
            assert!(!no_hardlinks(&e(n)), "{n}");
        }
        let msg = TierError::NoHardlinks("t".into(), "x".into()).to_string();
        assert!(msg.contains("target_no_hardlinks"), "{msg}");
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
