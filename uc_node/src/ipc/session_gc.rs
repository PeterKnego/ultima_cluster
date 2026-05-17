//! Node-side session-file garbage collector for `clients/sessions.dir/`.
//!
//! Each connected client owns one file `{client_id}.session`, 64 bytes:
//!
//! ```text
//!  0..8   heartbeat_seq      AtomicU64  (client ticks)
//!  8..16  heartbeat_at_ns    AtomicU64  (wall time of the last tick)
//! 16..20  client_id_check    u32        (== filename's u32; sanity)
//! 20..64  padding (zero)
//! ```
//!
//! The GC task wakes every [`GC_TICK`], reads each file once, and runs a
//! [`HeartbeatWatcher`] against the heartbeat counters. A file whose
//! `heartbeat_seq` has not advanced for [`STALE_AFTER`] (5 s by default)
//! is unlinked. No further node-side state needs cleanup — in-flight
//! broadcasts for the dead client are no-ops (no consumer reads them).

#![allow(dead_code)] // wired into NodeBuilder in Task 3.4

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use memmap2::Mmap;
use tokio::task::JoinHandle;

use uc_protocol::liveness::HeartbeatWatcher;

pub const SESSION_FILE_LEN: usize = 64;
pub const GC_TICK: Duration = Duration::from_secs(2);
pub const STALE_AFTER: Duration = Duration::from_secs(5);

#[repr(C, align(8))]
pub struct SessionFile {
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub client_id_check: u32,
    pub _pad: [u8; 44],
}

const _: () = {
    assert!(std::mem::size_of::<SessionFile>() == SESSION_FILE_LEN);
};

pub struct SessionGcHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_session_gc(sessions_dir: PathBuf) -> SessionGcHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut watchers: HashMap<u32, HeartbeatWatcher> = HashMap::new();

        while !stop_for_task.load(Ordering::Relaxed) {
            sweep(&sessions_dir, &mut watchers);
            tokio::time::sleep(GC_TICK).await;
        }
    });

    SessionGcHandle { join, stop }
}

fn sweep(sessions_dir: &std::path::Path, watchers: &mut HashMap<u32, HeartbeatWatcher>) {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(it) => it,
        Err(e) => {
            tracing::warn!(?e, "session_gc: read_dir failed");
            return;
        }
    };

    let now_ns = now_ns();
    let timeout_ns = STALE_AFTER.as_nanos() as u64;
    let mut live_ids: std::collections::HashSet<u32> = Default::default();

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(stem) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if path.extension().and_then(|e| e.to_str()) != Some("session") {
            continue;
        }
        live_ids.insert(stem);

        let f = match std::fs::OpenOptions::new().read(true).open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        // SAFETY: read-only mmap of a 64-byte file we just opened.
        let mmap = match unsafe { Mmap::map(&f) } {
            Ok(m) => m,
            Err(_) => continue,
        };
        if mmap.len() < SESSION_FILE_LEN {
            continue;
        }
        // SAFETY: file len ≥ SESSION_FILE_LEN; mmap is page-aligned.
        let sess = unsafe { &*mmap.as_ptr().cast::<SessionFile>() };
        let seq = sess.heartbeat_seq.load(Ordering::Relaxed);
        let watcher = watchers
            .entry(stem)
            .or_insert_with(|| HeartbeatWatcher::new(seq, now_ns));

        let alive = if seq != watcher.last_seq() {
            *watcher = HeartbeatWatcher::new(seq, now_ns);
            true
        } else {
            now_ns.saturating_sub(watcher.last_seen_ns()) < timeout_ns
        };

        if !alive {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(?e, client_id = stem, "session_gc: unlink failed");
            } else {
                tracing::info!(client_id = stem, "session_gc: unlinked stale session");
            }
            watchers.remove(&stem);
        }
    }

    watchers.retain(|id, _| live_ids.contains(id));
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;

    fn make_session_file(dir: &std::path::Path, client_id: u32, seq: u64) -> std::path::PathBuf {
        let path = dir.join(format!("{client_id}.session"));
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut bytes = vec![0u8; SESSION_FILE_LEN];
        bytes[0..8].copy_from_slice(&seq.to_le_bytes());
        bytes[16..20].copy_from_slice(&client_id.to_le_bytes());
        f.write_all(&bytes).unwrap();
        path
    }

    #[tokio::test]
    async fn stale_session_is_unlinked() {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_session_file(tmp.path(), 42, 0);

        let handle = spawn_session_gc(tmp.path().to_path_buf());
        tokio::time::sleep(Duration::from_secs(8)).await;
        handle.stop.store(true, Ordering::Relaxed);
        let _ = handle.join.await;
        assert!(!path.exists(), "stale session should have been unlinked");
    }

    #[tokio::test]
    async fn live_session_is_preserved() {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_session_file(tmp.path(), 7, 0);

        let writer_stop = Arc::new(AtomicBool::new(false));
        let ws = Arc::clone(&writer_stop);
        let p_for_writer = path.clone();
        let writer = tokio::spawn(async move {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&p_for_writer)
                .unwrap();
            let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
            let mut seq: u64 = 0;
            while !ws.load(Ordering::Relaxed) {
                seq += 1;
                mmap[0..8].copy_from_slice(&seq.to_le_bytes());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let handle = spawn_session_gc(tmp.path().to_path_buf());
        tokio::time::sleep(Duration::from_secs(8)).await;
        handle.stop.store(true, Ordering::Relaxed);
        let _ = handle.join.await;
        writer_stop.store(true, Ordering::Relaxed);
        let _ = writer.await;
        assert!(path.exists(), "live session should not be unlinked");
    }
}
