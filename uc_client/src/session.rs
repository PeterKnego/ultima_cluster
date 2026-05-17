//! Client-side session file under `clients/sessions.dir/{client_id}.session`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;
use tokio::task::JoinHandle;

use crate::ClientError;

pub const SESSION_FILE_LEN: usize = 64;
const TICK_PERIOD: Duration = Duration::from_millis(100);

#[repr(C, align(8))]
struct SessionFile {
    heartbeat_seq: AtomicU64,
    heartbeat_at_ns: AtomicU64,
    client_id_check: u32,
    _pad: [u8; 44],
}

const _: () = {
    assert!(std::mem::size_of::<SessionFile>() == SESSION_FILE_LEN);
};

pub struct SessionHandle {
    pub path: PathBuf,
    /// Keeps the mmap alive while the ticker runs.
    _mmap: Arc<MmapHolder>,
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

struct MmapHolder(MmapMut);
// SAFETY: SessionFile is all-atomic; concurrent access through the raw
// ptr is sound across threads.
unsafe impl Send for MmapHolder {}
unsafe impl Sync for MmapHolder {}

impl SessionHandle {
    pub fn create(sessions_dir: &Path, client_id: u32) -> Result<Self, ClientError> {
        let path = sessions_dir.join(format!("{client_id}.session"));
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        f.set_len(SESSION_FILE_LEN as u64)?;
        // SAFETY: just-created file we own; no other process maps it
        // until the GC sweep reads it later (read-only).
        let mut mmap = unsafe { MmapMut::map_mut(&f)? };

        // Init: zero, then write client_id_check.
        mmap[..SESSION_FILE_LEN].fill(0);
        mmap[16..20].copy_from_slice(&client_id.to_le_bytes());

        let holder = Arc::new(MmapHolder(mmap));
        let stop = Arc::new(AtomicBool::new(false));

        let holder_for_task = Arc::clone(&holder);
        let stop_for_task = Arc::clone(&stop);

        // Obtain &'static references to the two atomic fields before spawning.
        // SAFETY: SessionFile is repr(C, align(8)); the holder Arc keeps the
        // mmap alive for the task's lifetime; only this task writes these
        // fields; AtomicU64 is Send + Sync.
        let seq_ref: &'static AtomicU64 = unsafe {
            let base = holder_for_task.0.as_ptr() as *const SessionFile;
            &(*base).heartbeat_seq
        };
        let at_ref: &'static AtomicU64 = unsafe {
            let base = holder_for_task.0.as_ptr() as *const SessionFile;
            &(*base).heartbeat_at_ns
        };

        let join = tokio::spawn(async move {
            let _holder = holder_for_task; // keep mmap alive
            while !stop_for_task.load(Ordering::Relaxed) {
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                seq_ref.fetch_add(1, Ordering::Relaxed);
                at_ref.store(now_ns, Ordering::Relaxed);
                tokio::time::sleep(TICK_PERIOD).await;
            }
        });

        Ok(SessionHandle {
            path,
            _mmap: holder,
            join,
            stop,
        })
    }
}
