// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Single-writer polling agents (spec §3.1): a duty-cycle closure on a
//! dedicated thread with a configurable idle strategy. No pools, no async.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleStrategy {
    /// Never park; lowest latency, pegs the core.
    BusySpin,
    /// Yield to the OS scheduler between empty cycles.
    Yield,
    /// Sleep between empty cycles (background-grade agents).
    Sleep(Duration),
}

impl IdleStrategy {
    #[inline]
    pub fn idle(&self) {
        match self {
            IdleStrategy::BusySpin => std::hint::spin_loop(),
            IdleStrategy::Yield => std::thread::yield_now(),
            IdleStrategy::Sleep(d) => std::thread::sleep(*d),
        }
    }
}

pub struct AgentRunner {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl AgentRunner {
    /// Spawn a named agent thread looping `work()`; when `work` returns
    /// false (no work done), the idle strategy runs.
    pub fn spawn<F>(name: &str, idle: IdleStrategy, mut work: F) -> io::Result<AgentRunner>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new().name(name.to_string()).spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                if !work() {
                    idle.idle();
                }
            }
        })?;
        Ok(AgentRunner { stop, handle })
    }

    /// Signal stop and join; propagates a panic from the work closure.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().expect("agent thread panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn runner_drives_work_and_stops_cleanly() {
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let runner = AgentRunner::spawn("test-agent", IdleStrategy::Yield, move || {
            c.fetch_add(1, Ordering::Relaxed);
            true
        })
        .unwrap();
        while count.load(Ordering::Relaxed) < 1000 {
            std::thread::yield_now();
        }
        runner.stop();
        let n = count.load(Ordering::Relaxed);
        assert!(n >= 1000);
    }
}
