//! Operation history recording. Pure types + a thread-safe recorder. The
//! real-time order is captured by a global monotonic sequence stamped at
//! invoke and at return: op A precedes B iff `A.ret < B.invoke`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub use crate::model::{Op, RegResp};

/// Observed outcome of one operation.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Committed and the response was observed; must linearize with this response.
    Ok(RegResp),
    /// May or may not have committed; response not observed.
    Indeterminate,
}

#[derive(Clone, Debug)]
pub struct Entry {
    #[allow(dead_code)]
    pub client: u32,
    pub op: Op,
    pub invoke: u64,
    pub ret: u64,
    pub outcome: Outcome,
}

/// Records entries from concurrent workers and stamps the global sequence.
pub struct History {
    seq: AtomicU64,
    entries: Mutex<Vec<Entry>>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl History {
    /// Stamp an invoke; call right before firing the op.
    pub fn invoke(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
    /// Stamp a return and record the completed entry.
    pub fn record(&self, client: u32, op: Op, invoke: u64, outcome: Outcome) {
        let ret = self.seq.fetch_add(1, Ordering::Relaxed);
        self.entries.lock().unwrap().push(Entry {
            client,
            op,
            invoke,
            ret,
            outcome,
        });
    }
    /// Consume the recorded entries.
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries.into_inner().unwrap()
    }
    /// Clone the entries recorded so far (for the in-progress liveness check,
    /// which must read the history without consuming it).
    pub fn snapshot(&self) -> Vec<Entry> {
        self.entries.lock().unwrap().clone()
    }
    /// Count of Ok outcomes (for the liveness gate).
    #[allow(dead_code)]
    pub fn ok_count(entries: &[Entry]) -> usize {
        entries
            .iter()
            .filter(|e| matches!(e.outcome, Outcome::Ok(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_return_ordering() {
        let h = History::default();
        let i1 = h.invoke();
        h.record(0, Op::Write(1), i1, Outcome::Ok(RegResp::Ack));
        let i2 = h.invoke();
        h.record(0, Op::Read, i2, Outcome::Ok(RegResp::Value(Some(1))));
        let es = h.into_entries();
        assert_eq!(es.len(), 2);
        // First op returned (ret) before second op was invoked.
        assert!(es[0].ret < es[1].invoke);
    }
}
