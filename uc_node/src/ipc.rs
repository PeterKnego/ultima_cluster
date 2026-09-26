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

/// How long [`InstanceDir::acquire`] keeps retrying a CONTENDED instance
/// lock before calling it `AlreadyRunning`: long enough to outlast any
/// [`probe_instance_lock`] (a try-lock and an unlock), short enough that a
/// second node on a live dir still refuses at once to a human.
const ACQUIRE_PROBE_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

/// `fs2`'s "somebody else holds it" error, as opposed to a lock the
/// filesystem could not take at all (`ENOLCK`, NFS's `EBADF` on a read-only
/// fd) — the two mean different things to both callers.
fn is_contended(e: &std::io::Error) -> bool {
    e.raw_os_error() == fs2::lock_contended_error().raw_os_error()
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
        // Non-blocking: a contended lock means a live node already owns the dir
        // — unless it stays free again within `ACQUIRE_PROBE_GRACE`. A
        // [`probe_instance_lock`] (`uc2ctl status`, `backup`) holds the same
        // exclusive lock for microseconds, and `status` is polled at boot (the
        // compose healthcheck, the flag-day script), so a single try would let
        // a probe make a booting node refuse to start as `AlreadyRunning`
        // (#35 review). A real node holds the lock for its whole life, so the
        // grace only delays that refusal, never hides it.
        let deadline = std::time::Instant::now() + ACQUIRE_PROBE_GRACE;
        loop {
            match FileExt::try_lock_exclusive(&lock) {
                Ok(()) => break,
                Err(e) if is_contended(&e) && std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(_) => return Err(IpcError::AlreadyRunning(root.to_path_buf())),
            }
        }
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
    /// Cluster-FSM spec §4.7: the `uc2-cluster` agent's own snapshot
    /// directory, alongside the per-service `snapshots/<id>/` directories but
    /// keyed by name (the cluster FSM has no cnc slot / row id).
    pub fn cluster_snapshot_dir(&self) -> PathBuf {
        self.root.join("snapshots").join("cluster")
    }
}

/// What [`probe_instance_lock`] found at `<root>/instance.lock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceLock {
    /// A process holds the flock: a node owns this dir right now.
    Held,
    /// The file exists but nothing holds it — a stopped (or killed) node's
    /// leftover; shutdown never deletes it, and the OS drops the flock on
    /// any exit, `SIGKILL` included.
    Free,
    /// No `instance.lock` at all: no node has ever booted here (or this is a
    /// backup artifact, which never carries one).
    Absent,
}

/// Probe whether a node currently owns `root`, without holding the lock
/// beyond the probe itself: a non-blocking exclusive try-lock — the same
/// primitive [`InstanceDir::acquire`] uses to enforce one node per dir —
/// released again at once. Unlike a heartbeat this cannot be fooled by a
/// frozen page: the OS releases the flock the instant the node's process
/// dies. It is a same-host answer only, which is all an instance directory
/// ever is.
///
/// Opened read-only: `flock` does not care about the open mode on a local
/// filesystem, and a reader that may not write the lock file can still ask.
///
/// Sound only while the node is the ONE process that ever locks
/// `instance.lock`: a service, client or gateway taking even a shared lock on
/// it would read as `Held` after the node died. They lock
/// `service.<row>.lock` instead — keep it that way.
///
/// The probe briefly holds the exclusive lock itself;
/// [`InstanceDir::acquire`] retries a contended lock for
/// `ACQUIRE_PROBE_GRACE` so a probe can never make a booting node refuse.
pub fn probe_instance_lock(root: &Path) -> std::io::Result<InstanceLock> {
    let lock = match std::fs::File::open(root.join("instance.lock")) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(InstanceLock::Absent),
        Err(e) => return Err(e),
    };
    match FileExt::try_lock_exclusive(&lock) {
        Ok(()) => {
            // Dropping `lock` would release it too; unlocking first just
            // makes the hold as short as it can be.
            let _ = FileExt::unlock(&lock);
            Ok(InstanceLock::Free)
        }
        Err(e) if is_contended(&e) => Ok(InstanceLock::Held),
        // The filesystem could not answer (no flock support, NFS's EBADF on a
        // read-only fd): say so rather than guess either way.
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_instance_lock_tells_held_from_free_from_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            probe_instance_lock(dir.path()).unwrap(),
            InstanceLock::Absent
        );
        let held = InstanceDir::acquire(dir.path()).unwrap();
        assert_eq!(probe_instance_lock(dir.path()).unwrap(), InstanceLock::Held);
        drop(held);
        assert_eq!(probe_instance_lock(dir.path()).unwrap(), InstanceLock::Free);
        // The probe released what it took: the dir is still acquirable.
        let _again = InstanceDir::acquire(dir.path()).unwrap();
    }

    /// #35 review: a probe's momentary hold must not make a booting node
    /// refuse. Stand in for a probe that holds the lock for 20 ms (far longer
    /// than a real one) while the node acquires.
    #[test]
    fn acquire_outlasts_a_momentary_probe_hold() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("instance.lock"), b"").unwrap();
        let probe = std::fs::File::open(dir.path().join("instance.lock")).unwrap();
        FileExt::try_lock_exclusive(&probe).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            drop(probe);
        });
        let held = InstanceDir::acquire(dir.path());
        release.join().unwrap();
        assert!(held.is_ok(), "{:?}", held.err());
    }

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
        assert_eq!(
            d.cluster_snapshot_dir(),
            dir.path().join("snapshots").join("cluster")
        );
        // PINNED equal to the lock-free reader `uc2ctl` uses: the two build
        // the same path, and a change to either without the other would make
        // `schedule show` read an empty directory beside a running node.
        assert_eq!(
            d.cluster_snapshot_dir(),
            crate::cluster_agent::snapshot_dir_of(dir.path())
        );
    }
}
