// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The node's on-disk instance directory (M5 spec §7): the exclusive-flock'd
//! root under which one node keeps its cnc v2 page, file-backed log buffer,
//! journal, durable state, and the shared-memory IPC ring files.
//!
//! One node per instance dir — enforced by an exclusive `instance.lock`
//! (`fs2::try_lock_exclusive`) held for the node's whole life. A service or a
//! client attaches by opening the well-known paths this type vends (the cnc
//! page carries the fresh per-boot `instance_id` that invalidates any stale
//! attachment). The lock is the single hard gate; every other file is
//! re-created or size-checked at boot.

use std::path::{Path, PathBuf};

use fs2::FileExt;

/// Why an instance dir could not be acquired (or an IPC file could not be
/// materialized). `AlreadyRunning` is the flock-contended case — a live node
/// already owns this dir.
#[derive(thiserror::Error, Debug)]
pub enum IpcError {
    #[error("AlreadyRunning: another node holds the instance lock at {0}")]
    AlreadyRunning(PathBuf),
    #[error("instance dir io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Cnc(#[from] uc_log::cnc::CncError),
}

/// A held instance directory: `root` plus the exclusive lock file kept open for
/// the node's life (dropping it releases the flock). The path accessors are the
/// contract every attaching party (service, clients) resolves against.
pub struct InstanceDir {
    pub root: PathBuf,
    // Held open (and flock'd) for the lifetime of the node; released on drop.
    _lock: std::fs::File,
}

impl InstanceDir {
    /// Create/open the dir, take `instance.lock` EXCLUSIVELY (fs2
    /// `try_lock_exclusive` → [`IpcError::AlreadyRunning`] on contention), and
    /// materialize the durable subdirs (`journal/`, `state/`). This is boot
    /// step 1 — nothing else touches the dir until the lock is held.
    pub fn acquire(root: &Path) -> Result<InstanceDir, IpcError> {
        std::fs::create_dir_all(root)?;
        let lock_path = root.join("instance.lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        // Non-blocking: a contended lock means a live node already owns the dir.
        FileExt::try_lock_exclusive(&lock)
            .map_err(|_| IpcError::AlreadyRunning(root.to_path_buf()))?;
        std::fs::create_dir_all(root.join("journal"))?;
        std::fs::create_dir_all(root.join("state"))?;
        Ok(InstanceDir {
            root: root.to_path_buf(),
            _lock: lock,
        })
    }

    pub fn cnc_path(&self) -> PathBuf {
        self.root.join("cnc2.dat")
    }
    pub fn log_path(&self) -> PathBuf {
        self.root.join("log.buf")
    }
    pub fn journal_dir(&self) -> PathBuf {
        self.root.join("journal")
    }
    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }
    pub fn ingress_ring(&self) -> PathBuf {
        self.root.join("ingress.ring")
    }
    pub fn query_ring(&self) -> PathBuf {
        self.root.join("query.ring")
    }
    pub fn egress_node(&self) -> PathBuf {
        self.root.join("egress_node.broadcast")
    }
    /// M14a: the node→service query ring for service `id`.
    pub fn svc_query_ring_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("svc_query.{id}.ring"))
    }
    /// M14a: service `id`'s response broadcast (service → clients).
    pub fn egress_service_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("egress_service.{id}.broadcast"))
    }
    /// Time-and-timers §4.4: the service→node schedule ring for row `id`.
    pub fn svc_sched_ring_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("svc_sched.{id}.ring"))
    }
    /// M14a: service `id`'s snapshot directory (`snapshots/<id>/`).
    pub fn snapshot_dir_for(&self, id: u8) -> PathBuf {
        self.root.join("snapshots").join(id.to_string())
    }
    /// M14c: the snapshots ROOT (`snapshots/`), which holds one `<id>/`
    /// directory per declared FSM. The inbound snapshot intake is wired to this
    /// and picks the per-id subdirectory from each `SNAP_BEGIN`.
    pub fn snapshot_root(&self) -> PathBuf {
        self.root.join("snapshots")
    }
    /// M14a: the exclusive flock a service process takes for its id.
    pub fn service_lock_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("service.{id}.lock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_holds_exclusive_lock_and_refuses_second() {
        let dir = tempfile::tempdir().unwrap();
        let held = InstanceDir::acquire(dir.path()).unwrap();
        // subdirs materialized
        assert!(dir.path().join("journal").is_dir());
        assert!(dir.path().join("state").is_dir());
        // second acquire on the same dir is refused while the first is held
        assert!(matches!(
            InstanceDir::acquire(dir.path()),
            Err(IpcError::AlreadyRunning(_))
        ));
        // releasing the first lets a fresh acquire succeed
        drop(held);
        let _again = InstanceDir::acquire(dir.path()).unwrap();
    }

    #[test]
    fn path_accessors_are_rooted() {
        let dir = tempfile::tempdir().unwrap();
        let d = InstanceDir::acquire(dir.path()).unwrap();
        assert_eq!(d.cnc_path(), dir.path().join("cnc2.dat"));
        assert_eq!(d.log_path(), dir.path().join("log.buf"));
        assert_eq!(d.ingress_ring(), dir.path().join("ingress.ring"));
        assert_eq!(d.egress_node(), dir.path().join("egress_node.broadcast"));
        assert_eq!(d.svc_query_ring_for(0), dir.path().join("svc_query.0.ring"));
        assert_eq!(d.svc_query_ring_for(7), dir.path().join("svc_query.7.ring"));
        assert_eq!(d.svc_sched_ring_for(0), dir.path().join("svc_sched.0.ring"));
        assert_eq!(d.svc_sched_ring_for(7), dir.path().join("svc_sched.7.ring"));
        assert_eq!(
            d.egress_service_for(3),
            dir.path().join("egress_service.3.broadcast")
        );
        assert_eq!(
            d.snapshot_dir_for(1),
            dir.path().join("snapshots").join("1")
        );
        assert_eq!(d.service_lock_for(2), dir.path().join("service.2.lock"));
    }
}
