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
    handle: Option<JoinHandle<()>>,
    finished: Arc<AtomicBool>,
}

/// Sets `finished` true on drop — fires whether the worker thread's loop
/// returns cleanly or unwinds from a panic, since the guard lives inside the
/// spawned closure and `Drop::drop` runs during unwind too.
struct FinishedGuard(Arc<AtomicBool>);

impl Drop for FinishedGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl AgentRunner {
    /// Spawn a named agent thread looping `work()`; when `work` returns
    /// false (no work done), the idle strategy runs.
    ///
    /// CONTRACT: `work` is a DUTY CYCLE — it must do a bounded amount of work
    /// per call and return `true` iff it made progress. It must never block
    /// or loop internally waiting for input; that starves the stop flag and
    /// turns the idle strategy into a lie.
    pub fn spawn<F>(name: &str, idle: IdleStrategy, mut work: F) -> io::Result<AgentRunner>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let finished = Arc::new(AtomicBool::new(false));
        let finished_flag = Arc::clone(&finished);
        let handle = std::thread::Builder::new().name(name.to_string()).spawn(move || {
            let _guard = FinishedGuard(finished_flag);
            while !stop_flag.load(Ordering::Relaxed) {
                if !work() {
                    idle.idle();
                }
            }
        })?;
        Ok(AgentRunner { stop, handle: Some(handle), finished })
    }

    /// Shared liveness flag: false while the worker loop runs, set true when
    /// the closure returns *or panics* (a drop-guard inside the spawned
    /// thread sets it during unwind too). Unlike `is_finished()` (which polls
    /// `JoinHandle::is_finished`, itself panic-safe but not cheaply shareable
    /// across threads), this is the Arc a supervisor/observability reader can
    /// clone and poll without borrowing the `AgentRunner`.
    pub fn finished_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.finished)
    }

    /// Has this agent's thread exited? A polling agent runs until stopped, so
    /// `true` before teardown means the work closure PANICKED (a fail-stop:
    /// the service's instance-mismatch or log-rewind contracts, the archive's
    /// journal I/O contract). Supervisors — the production one, and the test
    /// harnesses that stand in for it — poll this to respawn instead of
    /// discovering the death at teardown, when `stop()` re-raises it.
    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| h.is_finished())
    }

    /// Signal stop and join; propagates a panic from the work closure.
    /// Prefer this over `drop` in teardown paths that must observe failures.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().expect("agent thread panicked");
    }
}

/// Dropping without `stop()` still signals and joins (no leaked busy-spinning
/// thread — the v1 SyncCore teardown lesson), but swallows a work-closure
/// panic to avoid a double panic during unwind.
impl Drop for AgentRunner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
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

    #[test]
    fn the_finished_flag_survives_a_panicking_agent() {
        use std::time::{Duration, Instant};
        let r = AgentRunner::spawn("panics", IdleStrategy::Sleep(Duration::from_millis(1)), || {
            panic!("deliberate");
        })
        .unwrap();
        let flag = r.finished_flag();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !flag.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "flag never set");
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(r); // Drop swallows the panic — that behaviour is unchanged
    }

    #[test]
    fn drop_without_stop_signals_and_joins() {
        use std::time::{Duration, Instant};
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let runner = AgentRunner::spawn("drop-agent", IdleStrategy::Yield, move || {
            c.fetch_add(1, Ordering::Relaxed);
            true
        })
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while count.load(Ordering::Relaxed) < 100 {
            assert!(Instant::now() < deadline, "agent never ran");
            std::thread::yield_now();
        }
        drop(runner); // must signal stop AND join — the thread is gone after this
        let n = count.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(count.load(Ordering::Relaxed), n, "agent thread still running after drop");
    }
}
