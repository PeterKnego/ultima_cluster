// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle EDN history recording (design spec 2026-07-15). Pure std, hand-formatted
//! EDN — one map per line, the exact encoding ultima_db's `elle-history` driver
//! emits and the vendored elle-cli consumes. Singleton txns only (one op per
//! `:value`). Jepsen process semantics: after an `:info` (maybe-committed)
//! outcome a process id is RETIRED — `retire()` hands out a fresh one and the
//! old id must never record again (the driver asserts this).

use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Debug)]
pub enum EdnOp {
    Append { key: u32, val: u64 },
    Read { key: u32, result: Option<Vec<u64>> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdnType {
    Invoke,
    Ok,
    Fail,
    Info,
}

/// Format one elle event line. `:f :txn` and a single-op `:value` vector —
/// the singleton-txn list-append encoding.
pub fn edn_line(index: u64, typ: EdnType, process: u64, time_ns: u64, op: &EdnOp) -> String {
    let typ = match typ {
        EdnType::Invoke => ":invoke",
        EdnType::Ok => ":ok",
        EdnType::Fail => ":fail",
        EdnType::Info => ":info",
    };
    let mut v = String::new();
    match op {
        EdnOp::Append { key, val } => write!(v, "[:append {key} {val}]").unwrap(),
        EdnOp::Read { key, result: None } => write!(v, "[:r {key} nil]").unwrap(),
        EdnOp::Read { key, result: Some(list) } => {
            write!(v, "[:r {key} [").unwrap();
            for (j, x) in list.iter().enumerate() {
                if j > 0 {
                    v.push(' ');
                }
                write!(v, "{x}").unwrap();
            }
            v.push_str("]]");
        }
    }
    format!("{{:index {index}, :type {typ}, :f :txn, :process {process}, :time {time_ns}, :value [{v}]}}")
}

/// Thread-safe event recorder: a global monotonic `:index`, `:time` in ns since
/// construction, fresh-forever process ids, and Ok/completed counters for the
/// driver's liveness gate.
pub struct EdnRecorder {
    start: Instant,
    index: AtomicU64,
    next_process: AtomicU64,
    ok: AtomicU64,
    completed: AtomicU64,
    lines: Mutex<Vec<String>>,
}

impl EdnRecorder {
    pub fn new(initial_processes: u64) -> Self {
        Self {
            start: Instant::now(),
            index: AtomicU64::new(0),
            next_process: AtomicU64::new(initial_processes),
            ok: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            lines: Mutex::new(Vec::new()),
        }
    }

    /// A fresh, never-before-used process id (call after an `:info`).
    pub fn retire(&self) -> u64 {
        self.next_process.fetch_add(1, Ordering::Relaxed)
    }

    /// Stamp index + time and append the formatted line.
    pub fn record(&self, typ: EdnType, process: u64, op: &EdnOp) {
        match typ {
            EdnType::Invoke => {}
            EdnType::Ok => {
                self.ok.fetch_add(1, Ordering::Relaxed);
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            EdnType::Fail | EdnType::Info => {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
        let time_ns = self.start.elapsed().as_nanos() as u64;
        let mut lines = self.lines.lock().unwrap();
        // Index is taken under the lock so line order == index order in the file.
        let index = self.index.fetch_add(1, Ordering::Relaxed);
        lines.push(edn_line(index, typ, process, time_ns, op));
    }

    pub fn ok_count(&self) -> u64 {
        self.ok.load(Ordering::Relaxed)
    }
    /// Ok + Fail + Info (i.e. ops that finished one way or another).
    pub fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn write_to(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lines = self.lines.lock().unwrap();
        std::fs::write(path, lines.join("\n") + "\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edn_line_append_invoke() {
        let op = EdnOp::Append { key: 7, val: 42 };
        assert_eq!(
            edn_line(0, EdnType::Invoke, 3, 1200, &op),
            "{:index 0, :type :invoke, :f :txn, :process 3, :time 1200, :value [[:append 7 42]]}"
        );
    }

    #[test]
    fn edn_line_read_nil_and_lists() {
        assert_eq!(
            edn_line(1, EdnType::Invoke, 0, 5, &EdnOp::Read { key: 1, result: None }),
            "{:index 1, :type :invoke, :f :txn, :process 0, :time 5, :value [[:r 1 nil]]}"
        );
        assert_eq!(
            edn_line(9, EdnType::Ok, 0, 5, &EdnOp::Read { key: 1, result: Some(vec![12, 42]) }),
            "{:index 9, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 [12 42]]]}"
        );
        assert_eq!(
            edn_line(2, EdnType::Ok, 0, 5, &EdnOp::Read { key: 1, result: Some(vec![]) }),
            "{:index 2, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 []]]}"
        );
    }

    #[test]
    fn recorder_indexes_counts_and_retires() {
        let r = EdnRecorder::new(2); // processes 0 and 1 pre-allocated
        let op = EdnOp::Append { key: 1, val: 1 };
        r.record(EdnType::Invoke, 0, &op);
        r.record(EdnType::Ok, 0, &op);
        r.record(EdnType::Invoke, 1, &op);
        r.record(EdnType::Info, 1, &op);
        // Fresh ids start above the pre-allocated range and are never reused.
        assert_eq!(r.retire(), 2);
        assert_eq!(r.retire(), 3);
        assert_eq!(r.ok_count(), 1);
        assert_eq!(r.completed_count(), 2); // Ok + Info (invokes don't count)

        let dir = std::env::temp_dir().join(format!("edn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("history.edn");
        r.write_to(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4);
        // Global :index stamps are the emission order.
        assert!(lines[0].starts_with("{:index 0, :type :invoke"));
        assert!(lines[3].starts_with("{:index 3, :type :info"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
