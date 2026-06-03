//! Feature-gated commit-path latency probes. See
//! docs/superpowers/specs/2026-06-03-unified-benchmark-harness-design.md §3.
//!
//! Callers in uc_node / uc_service / uc_client invoke `stamp_*`/`bridge`
//! unconditionally. Without the `uc-bench-probes` feature the bodies are empty
//! `#[inline(always)]` no-ops. With the feature, timestamps land in a
//! process-local sink keyed by the correlation ids that already flow through
//! the system — valid only for the single-process in-process fixture.

/// Commit-path checkpoints, in path order. Used to index a per-request row.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Checkpoint {
    Submit = 0,
    NodeDequeue = 1,
    JournalAppended = 2,
    JournalFsynced = 3,
    ApplyEnqueue = 4,
    ApplyStart = 5,
    ApplyDone = 6,
    RespDequeue = 7,
    Broadcast = 8,
    ClientRecv = 9,
}

/// Number of checkpoints (length of a per-request stamp row).
pub const N_CHECKPOINTS: usize = 10;

#[cfg(not(feature = "uc-bench-probes"))]
mod imp {
    use super::Checkpoint;
    #[inline(always)]
    pub fn stamp_client(_client_id: u32, _local_seq: u32, _cp: Checkpoint) {}
    #[inline(always)]
    pub fn stamp_log(_log_index: u64, _cp: Checkpoint) {}
    #[inline(always)]
    pub fn bridge(_client_id: u32, _local_seq: u32, _log_index: u64) {}
}

#[cfg(feature = "uc-bench-probes")]
mod imp {
    use super::{Checkpoint, N_CHECKPOINTS};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Instant;

    type Row = [Option<u64>; N_CHECKPOINTS];

    struct Sink {
        base: Instant,
        /// Keyed by (client_id<<32 | local_seq): Submit/NodeDequeue/Broadcast/ClientRecv.
        client_rows: Mutex<HashMap<u64, Row>>,
        /// Keyed by log_index: the journal + apply stages.
        log_rows: Mutex<HashMap<u64, Row>>,
        /// client-key -> log_index, recorded at the dispatcher once both are known.
        bridge: Mutex<HashMap<u64, u64>>,
    }

    static SINK: OnceLock<Sink> = OnceLock::new();

    fn sink() -> &'static Sink {
        SINK.get_or_init(|| Sink {
            base: Instant::now(),
            client_rows: Mutex::new(HashMap::new()),
            log_rows: Mutex::new(HashMap::new()),
            bridge: Mutex::new(HashMap::new()),
        })
    }

    fn now_ns(s: &Sink) -> u64 {
        s.base.elapsed().as_nanos() as u64
    }

    fn client_key(client_id: u32, local_seq: u32) -> u64 {
        ((client_id as u64) << 32) | (local_seq as u64)
    }

    pub fn stamp_client(client_id: u32, local_seq: u32, cp: Checkpoint) {
        let s = sink();
        let t = now_ns(s);
        s.client_rows
            .lock()
            .entry(client_key(client_id, local_seq))
            .or_insert([None; N_CHECKPOINTS])[cp as usize] = Some(t);
    }

    pub fn stamp_log(log_index: u64, cp: Checkpoint) {
        let s = sink();
        let t = now_ns(s);
        s.log_rows
            .lock()
            .entry(log_index)
            .or_insert([None; N_CHECKPOINTS])[cp as usize] = Some(t);
    }

    pub fn bridge(client_id: u32, local_seq: u32, log_index: u64) {
        let s = sink();
        s.bridge
            .lock()
            .insert(client_key(client_id, local_seq), log_index);
    }

    /// Clear all captured stamps. Call before a measured run.
    pub fn reset() {
        let s = sink();
        s.client_rows.lock().clear();
        s.log_rows.lock().clear();
        s.bridge.lock().clear();
    }

    /// Drain and join client-keyed + log-keyed rows into one row per request.
    /// Requests missing a bridge entry or a matching log row are dropped.
    pub fn drain_joined() -> Vec<Row> {
        let s = sink();
        let client_rows = std::mem::take(&mut *s.client_rows.lock());
        let log_rows = std::mem::take(&mut *s.log_rows.lock());
        let bridge = std::mem::take(&mut *s.bridge.lock());
        let mut out = Vec::new();
        for (ckey, crow) in client_rows {
            let Some(&li) = bridge.get(&ckey) else { continue };
            let Some(lrow) = log_rows.get(&li) else { continue };
            let mut merged = crow;
            for i in 0..N_CHECKPOINTS {
                if merged[i].is_none() {
                    merged[i] = lrow[i];
                }
            }
            out.push(merged);
        }
        out
    }

    /// Named per-stage deltas (ns) for one joined row. Stages whose endpoints
    /// are missing, or where end < start, are omitted.
    pub fn stage_deltas(row: &Row) -> Vec<(&'static str, u64)> {
        use Checkpoint::*;
        const STAGES: &[(&str, Checkpoint, Checkpoint)] = &[
            ("submit_to_node", Submit, NodeDequeue),
            ("node_to_append", NodeDequeue, JournalAppended),
            ("journal_fsync", JournalAppended, JournalFsynced),
            ("commit_to_apply_enq", JournalFsynced, ApplyEnqueue),
            ("apply_ring", ApplyEnqueue, ApplyStart),
            ("apply", ApplyStart, ApplyDone),
            ("resp_ring", ApplyDone, RespDequeue),
            ("resp_to_broadcast", RespDequeue, Broadcast),
            ("broadcast_to_client", Broadcast, ClientRecv),
            ("total", Submit, ClientRecv),
        ];
        let mut out = Vec::with_capacity(STAGES.len());
        for (name, a, b) in STAGES {
            if let (Some(ta), Some(tb)) = (row[*a as usize], row[*b as usize])
                && tb >= ta
            {
                out.push((*name, tb - ta));
            }
        }
        out
    }
}

pub use imp::{bridge, stamp_client, stamp_log};

#[cfg(feature = "uc-bench-probes")]
pub use imp::{drain_joined, reset, stage_deltas};

#[cfg(all(test, feature = "uc-bench-probes"))]
mod tests {
    use super::*;

    // The probe sink is a process-global (`OnceLock<Sink>`); cargo runs unit
    // tests multi-threaded by default, so without serialization one test's
    // `reset()` can wipe another's stamps mid-flight. Hold this across each
    // test body. `unwrap_or_else(into_inner)` tolerates poisoning from a prior
    // failing test so we still see the real assertion, not a cascade.
    static SINK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn join_and_stage_deltas_cover_full_path() {
        let _serial = SINK_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset();
        // One request: client_id=7, local_seq=0, log_index=100.
        stamp_client(7, 0, Checkpoint::Submit);
        stamp_client(7, 0, Checkpoint::NodeDequeue);
        bridge(7, 0, 100);
        stamp_log(100, Checkpoint::JournalAppended);
        stamp_log(100, Checkpoint::JournalFsynced);
        stamp_log(100, Checkpoint::ApplyEnqueue);
        stamp_log(100, Checkpoint::ApplyStart);
        stamp_log(100, Checkpoint::ApplyDone);
        stamp_log(100, Checkpoint::RespDequeue);
        stamp_client(7, 0, Checkpoint::Broadcast);
        stamp_client(7, 0, Checkpoint::ClientRecv);

        let rows = drain_joined();
        assert_eq!(rows.len(), 1, "one joined request");
        let row = &rows[0];
        for (i, cp) in row.iter().enumerate() {
            assert!(cp.is_some(), "checkpoint {i} present after join");
        }
        let deltas = stage_deltas(row);
        let names: Vec<&str> = deltas.iter().map(|(n, _)| *n).collect();
        assert!(names.contains(&"journal_fsync"));
        assert!(names.contains(&"apply"));
        assert!(names.contains(&"total"));
        // total spans the whole path: >= every sub-stage individually.
        let total = deltas.iter().find(|(n, _)| *n == "total").unwrap().1;
        for (name, d) in &deltas {
            if *name != "total" {
                assert!(*d <= total, "{name} delta {d} <= total {total}");
            }
        }
    }

    #[test]
    fn request_without_bridge_is_dropped() {
        let _serial = SINK_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset();
        stamp_client(9, 1, Checkpoint::Submit);
        stamp_client(9, 1, Checkpoint::ClientRecv);
        // No bridge, no log row.
        assert!(drain_joined().is_empty());
    }
}
