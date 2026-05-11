//! Heartbeat helpers.
//!
//! Each side (node, service) increments its own `heartbeat_seq` and bumps
//! `heartbeat_at_ns` on a fixed tick (typically 100ms). Watchers track the
//! last observed `heartbeat_seq` and consider the peer dead if the seq has
//! not advanced within `liveness_timeout_ns`.
//!
//! Seq + wall-time are both useful: seq detects "the peer is alive and
//! ticking," wall-time detects "the watcher itself just woke up after a
//! long pause and shouldn't immediately declare death."

use std::sync::atomic::Ordering;

use crate::cnc::{NodeStatus, ServiceStatus};

#[inline]
pub fn tick_node(status: &NodeStatus, now_ns: u64) {
    status.heartbeat_seq.fetch_add(1, Ordering::Relaxed);
    status.heartbeat_at_ns.store(now_ns, Ordering::Relaxed);
}

#[inline]
pub fn tick_service(status: &ServiceStatus, now_ns: u64) {
    status.heartbeat_seq.fetch_add(1, Ordering::Relaxed);
    status.heartbeat_at_ns.store(now_ns, Ordering::Relaxed);
}

/// Per-watcher state. Embed one of these in each side's runtime to detect
/// peer death.
#[derive(Debug, Copy, Clone)]
pub struct HeartbeatWatcher {
    last_seq: u64,
    last_seen_ns: u64,
}

impl HeartbeatWatcher {
    pub fn new(current_seq: u64, now_ns: u64) -> Self {
        Self {
            last_seq: current_seq,
            last_seen_ns: now_ns,
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
    pub fn last_seen_ns(&self) -> u64 {
        self.last_seen_ns
    }

    /// Poll a `NodeStatus`. Returns `true` if the peer is still considered
    /// alive (seq advanced since last poll, or time since last seen <
    /// timeout).
    pub fn poll_node(&mut self, status: &NodeStatus, now_ns: u64, timeout_ns: u64) -> bool {
        let seq = status.heartbeat_seq.load(Ordering::Relaxed);
        self.poll_inner(seq, now_ns, timeout_ns)
    }

    pub fn poll_service(&mut self, status: &ServiceStatus, now_ns: u64, timeout_ns: u64) -> bool {
        let seq = status.heartbeat_seq.load(Ordering::Relaxed);
        self.poll_inner(seq, now_ns, timeout_ns)
    }

    fn poll_inner(&mut self, seq: u64, now_ns: u64, timeout_ns: u64) -> bool {
        if seq != self.last_seq {
            self.last_seq = seq;
            self.last_seen_ns = now_ns;
            true
        } else {
            now_ns.saturating_sub(self.last_seen_ns) < timeout_ns
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cnc::{cnc_file_size, init_cnc, sub};
    use memmap2::MmapMut;
    use tempfile::NamedTempFile;

    /// Returns `(mmap, &'static NodeStatus, &'static ServiceStatus)` —
    /// references are pinned to the mmap, which is kept alive by the caller.
    fn cnc_with_statuses() -> (MmapMut, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        let size = cnc_file_size();
        f.set_len(size as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        init_cnc(&mut mmap[..], "test", 1, 1).unwrap();
        (mmap, tmp)
    }

    fn node_status(mmap: &MmapMut) -> &NodeStatus {
        let header = crate::cnc::validate_cnc(&mmap[..]).unwrap();
        let off = header.sub_buffer_offsets[sub::NODE_STATUS] as usize;
        // SAFETY: cnc is properly aligned + sized; offset validated in init_cnc.
        unsafe { &*mmap[off..].as_ptr().cast::<NodeStatus>() }
    }

    fn service_status(mmap: &MmapMut) -> &ServiceStatus {
        let header = crate::cnc::validate_cnc(&mmap[..]).unwrap();
        let off = header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize;
        unsafe { &*mmap[off..].as_ptr().cast::<ServiceStatus>() }
    }

    #[test]
    fn tick_increments_seq_and_bumps_time() {
        let (mmap, _t) = cnc_with_statuses();
        let ns = node_status(&mmap);
        assert_eq!(ns.heartbeat_seq.load(Ordering::Relaxed), 0);
        tick_node(ns, 1_000);
        assert_eq!(ns.heartbeat_seq.load(Ordering::Relaxed), 1);
        assert_eq!(ns.heartbeat_at_ns.load(Ordering::Relaxed), 1_000);
        tick_node(ns, 2_000);
        assert_eq!(ns.heartbeat_seq.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn watcher_alive_when_seq_advances() {
        let (mmap, _t) = cnc_with_statuses();
        let ss = service_status(&mmap);
        let mut w = HeartbeatWatcher::new(0, 1_000);
        tick_service(ss, 2_000);
        assert!(w.poll_service(ss, 3_000, 1_000));
        assert_eq!(w.last_seq(), 1);
        assert_eq!(w.last_seen_ns(), 3_000);
    }

    #[test]
    fn watcher_dead_after_timeout() {
        let (mmap, _t) = cnc_with_statuses();
        let ns = node_status(&mmap);
        let mut w = HeartbeatWatcher::new(0, 1_000);
        // No tick — poll twice with growing now_ns.
        assert!(w.poll_node(ns, 1_500, 1_000)); // 500ns elapsed, alive
        assert!(!w.poll_node(ns, 2_500, 1_000)); // 1500ns elapsed, dead
    }
}
