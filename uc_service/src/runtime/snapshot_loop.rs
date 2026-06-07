//! Service-side snapshot control loop (Phase 2a). Consumes `snapshot.ring`:
//! BUILD_SNAPSHOT → build into snapshot.region; INSTALL_SNAPSHOT → install from it.
//! Blocking build/install under the SM RwLock (2a; Phase 2b makes build async).

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use uc_protocol::frames::snapshot::{
    MSG_TYPE_BUILD_SNAPSHOT, MSG_TYPE_INSTALL_SNAPSHOT, MSG_TYPE_SNAPSHOT_BUILT,
    MSG_TYPE_SNAPSHOT_INSTALLED, encode_extra_snapshot_built, encode_extra_snapshot_installed,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use uc_protocol::snapshot_region;

use crate::StateMachine;

const IDLE_BACKOFF: Duration = Duration::from_millis(2);
const ERROR_BACKOFF: Duration = Duration::from_millis(10);

pub struct SnapshotLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_snapshot_loop<S>(
    sm: Arc<RwLock<S>>,
    consumer: SpscConsumer,
    resp_producer: SpscProducer,
    region_path: PathBuf,
) -> SnapshotLoopHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let join = tokio::spawn(snapshot_task_body::<S>(
        sm,
        consumer,
        resp_producer,
        region_path,
        stop_for_task,
    ));
    SnapshotLoopHandle { join, stop }
}

async fn snapshot_task_body<S>(
    sm: Arc<RwLock<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
    region_path: PathBuf,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(64);
    while !stop.load(Ordering::Relaxed) {
        match consumer.try_read(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_BUILD_SNAPSHOT => {
                let (built_index, bytes) = {
                    let guard = sm.read().await;
                    let mut buf: Vec<u8> = Vec::new();
                    match guard.build_snapshot(&mut buf) {
                        Ok(idx) => (idx, buf),
                        Err(e) => {
                            tracing::error!(error = %e, "snapshot build failed");
                            continue;
                        }
                    }
                };
                if let Err(e) = snapshot_region::write(&region_path, built_index, &bytes) {
                    tracing::error!(error = %e, "snapshot.region write failed");
                    continue;
                }
                publish_resp(
                    &mut resp_producer,
                    MSG_TYPE_SNAPSHOT_BUILT,
                    encode_extra_snapshot_built(built_index),
                    &stop,
                );
            }
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_INSTALL_SNAPSHOT => {
                let bytes = match snapshot_region::read(&region_path) {
                    Ok((_idx, b)) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "snapshot.region read failed");
                        continue;
                    }
                };
                let new_last_applied = {
                    let mut guard = sm.write().await;
                    match guard.install_snapshot(&mut Cursor::new(bytes)) {
                        Ok(li) => li,
                        Err(e) => {
                            tracing::error!(error = %e, "snapshot install failed");
                            continue;
                        }
                    }
                };
                publish_resp(
                    &mut resp_producer,
                    MSG_TYPE_SNAPSHOT_INSTALLED,
                    encode_extra_snapshot_installed(new_last_applied),
                    &stop,
                );
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "snapshot ring: unexpected frame");
            }
            Ok(None) => tokio::time::sleep(IDLE_BACKOFF).await,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot ring read error");
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

fn publish_resp(
    producer: &mut SpscProducer,
    msg_type: u16,
    extra: [u8; 8],
    stop: &AtomicBool,
) {
    loop {
        match producer.try_write(msg_type, 0, extra, &[]) {
            Ok(()) => return,
            Err(RingError::Full) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::yield_now();
            }
            Err(e) => panic!("snapshot_resp write: {e}"),
        }
    }
}
