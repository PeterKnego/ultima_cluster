//! Node-owned instance directory: takes the exclusive `instance.lock`
//! flock, creates `cnc.dat`, generates the `instance_id`, exposes the
//! cnc mapping for later sub-ring construction.

use std::path::{Path, PathBuf};

use fs2::FileExt;
use memmap2::MmapMut;
use thiserror::Error;
use uc_protocol::cnc::{cnc_file_size, init_cnc};
use uc_protocol::ring::RingError;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("instance already running at {0}")]
    AlreadyRunning(PathBuf),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring: {0}")]
    Ring(#[from] RingError),
}

/// Node-side instance handle. Holds:
///   * `_lock_file` — keeps the exclusive flock alive (Drop unlocks).
///   * `cnc_mmap` — owns the cnc.dat mapping; sub-buffer offsets in the
///     header tell us where `NodeStatus` / `ServiceStatus` / control rings
///     live within it.
///   * `instance_id` — the randomly-generated 128-bit id stamped into the
///     header. Stable for the lifetime of this `Instance`; a node restart
///     produces a fresh value.
pub struct Instance {
    pub instance_dir: PathBuf,
    pub instance_id: u128,
    pub app_id: String,
    pub node_id: u64,
    pub cnc_mmap: MmapMut,
    _lock_file: std::fs::File,
}

impl Instance {
    /// Create a fresh instance directory: acquire `instance.lock`, write a
    /// new `cnc.dat`, create the `service/` subdir for the SPSC ring files
    /// that [`super::service_link::ServiceLink`] will populate.
    pub fn create(instance_dir: &Path, app_id: &str, node_id: u64) -> Result<Self, IpcError> {
        std::fs::create_dir_all(instance_dir)?;
        std::fs::create_dir_all(instance_dir.join("service"))?;
        std::fs::create_dir_all(instance_dir.join("clients"))?;

        // Exclusive flock — one node per instance directory.
        let lock_path = instance_dir.join("instance.lock");
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|_| IpcError::AlreadyRunning(instance_dir.to_owned()))?;

        // cnc.dat: truncate-create, mmap, init.
        let cnc_path = instance_dir.join("cnc.dat");
        let cnc_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cnc_path)?;
        let file_size = cnc_file_size();
        cnc_file.set_len(file_size as u64)?;
        // SAFETY: just-created file we hold the exclusive flock on; no
        // other process can observe the mmap during init.
        let mut cnc_mmap = unsafe { MmapMut::map_mut(&cnc_file)? };

        let instance_id: u128 = rand::random();
        init_cnc(&mut cnc_mmap[..], app_id, node_id, instance_id)?;

        Ok(Instance {
            instance_dir: instance_dir.to_owned(),
            instance_id,
            app_id: app_id.to_string(),
            node_id,
            cnc_mmap,
            _lock_file: lock_file,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::cnc::validate_cnc;

    #[test]
    fn create_writes_cnc_and_locks() {
        let tmp = tempfile::tempdir().unwrap();
        let inst = Instance::create(tmp.path(), "kv-app", 7).expect("create");
        assert_eq!(inst.app_id, "kv-app");
        assert_eq!(inst.node_id, 7);
        assert_ne!(inst.instance_id, 0);

        // cnc.dat exists and validates.
        let cnc_path = tmp.path().join("cnc.dat");
        assert!(cnc_path.exists());
        let cnc_bytes = std::fs::read(&cnc_path).unwrap();
        let header = validate_cnc(&cnc_bytes).expect("validate");
        assert_eq!(header.app_id_str(), "kv-app");
        assert_eq!(header.node_id, 7);
        assert_eq!(header.instance_id(), inst.instance_id);

        // service/ and clients/ subdirs exist.
        assert!(tmp.path().join("service").is_dir());
        assert!(tmp.path().join("clients").is_dir());
        // instance.lock exists.
        assert!(tmp.path().join("instance.lock").exists());
    }

    #[test]
    fn second_create_in_same_dir_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let _first = Instance::create(tmp.path(), "app", 1).expect("first create");
        let second = Instance::create(tmp.path(), "app", 1);
        assert!(matches!(second, Err(IpcError::AlreadyRunning(_))));
    }

    #[test]
    fn create_after_drop_is_allowed() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let _first = Instance::create(tmp.path(), "app", 1).expect("first");
            // drop here releases the flock.
        }
        let _second = Instance::create(tmp.path(), "app", 2).expect("second");
    }

    #[test]
    fn each_create_gets_a_fresh_instance_id() {
        // Two separate dirs; check that the random instance ids differ.
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let a = Instance::create(t1.path(), "app", 1).unwrap();
        let b = Instance::create(t2.path(), "app", 1).unwrap();
        assert_ne!(a.instance_id, b.instance_id);
    }
}
