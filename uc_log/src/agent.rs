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
    /// A ladder over the runner's consecutive-idle streak: the first `spins`
    /// empty cycles spin, the next `yields` yield, every one after that
    /// sleeps `sleep`. A productive cycle resets the streak. The shape is
    /// `uc_client`'s wait ladder; the difference is that the sleep rung does
    /// NOT ramp, so an agent that has been quiet for seconds still wakes
    /// within one `sleep` plus timer slack — the same bound a plain
    /// [`IdleStrategy::Sleep`] gives, which is what keeps sparse traffic from
    /// regressing when this replaces one.
    ///
    /// Why it exists (2026-09-16 service-time record): a flat 50 µs sleep on
    /// the service's apply agent landed on roughly half of all responses at
    /// low load as a second mode ~100 µs above the first. The ladder keeps
    /// the agent awake across one client round trip after each frame, so a
    /// steady low-rate stream never meets the sleep, while a truly idle
    /// service falls back to sleeping within a millisecond or so.
    Backoff {
        spins: u32,
        yields: u32,
        sleep: Duration,
    },
}

impl IdleStrategy {
    /// One idle step. `streak` is the number of consecutive empty cycles so
    /// far, counting this one from 1; only [`IdleStrategy::Backoff`] reads it.
    #[inline]
    pub fn idle(&self, streak: u64) {
        match self {
            IdleStrategy::BusySpin => std::hint::spin_loop(),
            IdleStrategy::Yield => std::thread::yield_now(),
            IdleStrategy::Sleep(d) => std::thread::sleep(*d),
            IdleStrategy::Backoff { sleep, .. } => match self.backoff_rung(streak) {
                BackoffRung::Spin => std::hint::spin_loop(),
                BackoffRung::Yield => std::thread::yield_now(),
                BackoffRung::Sleep => std::thread::sleep(*sleep),
            },
        }
    }

    /// Which rung of a [`IdleStrategy::Backoff`] ladder `streak` lands on.
    /// Pure, so the boundaries are unit-testable; the other variants report
    /// their own single behaviour.
    #[inline]
    pub fn backoff_rung(&self, streak: u64) -> BackoffRung {
        match self {
            IdleStrategy::BusySpin => BackoffRung::Spin,
            IdleStrategy::Yield => BackoffRung::Yield,
            IdleStrategy::Sleep(_) => BackoffRung::Sleep,
            IdleStrategy::Backoff { spins, yields, .. } => {
                if streak <= u64::from(*spins) {
                    BackoffRung::Spin
                } else if streak <= u64::from(*spins) + u64::from(*yields) {
                    BackoffRung::Yield
                } else {
                    BackoffRung::Sleep
                }
            }
        }
    }
}

/// The three behaviours an idle step can take (see
/// [`IdleStrategy::backoff_rung`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffRung {
    Spin,
    Yield,
    Sleep,
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
        let handle = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let _guard = FinishedGuard(finished_flag);
                // Consecutive empty cycles; only `Backoff` reads it. One
                // register op per cycle, outside the work closure's body.
                let mut streak: u64 = 0;
                while !stop_flag.load(Ordering::Relaxed) {
                    if work() {
                        streak = 0;
                    } else {
                        streak = streak.saturating_add(1);
                        idle.idle(streak);
                    }
                }
            })?;
        Ok(AgentRunner {
            stop,
            handle: Some(handle),
            finished,
        })
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
        self.handle
            .take()
            .unwrap()
            .join()
            .expect("agent thread panicked");
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
    fn backoff_rungs_follow_the_streak_and_reset_on_work() {
        let b = IdleStrategy::Backoff {
            spins: 3,
            yields: 2,
            sleep: Duration::from_micros(50),
        };
        let rungs: Vec<_> = (1..=7).map(|s| b.backoff_rung(s)).collect();
        assert_eq!(
            rungs,
            [
                BackoffRung::Spin,
                BackoffRung::Spin,
                BackoffRung::Spin,
                BackoffRung::Yield,
                BackoffRung::Yield,
                BackoffRung::Sleep,
                BackoffRung::Sleep,
            ]
        );
        // The runner resets the streak on a productive cycle: model that by
        // asking rung 1 again after a "work" — it is a spin, not a sleep.
        assert_eq!(b.backoff_rung(1), BackoffRung::Spin);
        // Zero-width rungs collapse cleanly.
        let no_spin = IdleStrategy::Backoff {
            spins: 0,
            yields: 1,
            sleep: Duration::from_micros(1),
        };
        assert_eq!(no_spin.backoff_rung(1), BackoffRung::Yield);
        assert_eq!(no_spin.backoff_rung(2), BackoffRung::Sleep);
    }

    #[test]
    fn non_backoff_variants_ignore_the_streak() {
        for s in [1, 10, u64::MAX] {
            assert_eq!(IdleStrategy::BusySpin.backoff_rung(s), BackoffRung::Spin);
            assert_eq!(IdleStrategy::Yield.backoff_rung(s), BackoffRung::Yield);
            assert_eq!(
                IdleStrategy::Sleep(Duration::from_micros(1)).backoff_rung(s),
                BackoffRung::Sleep
            );
        }
    }

    #[test]
    fn a_backoff_runner_reaches_its_sleep_rung_and_still_stops() {
        // Spawn a runner whose work never makes progress, with a ladder short
        // enough to fall through to the sleep rung within the test, and
        // prove it stops cleanly from there (the sleep must not starve the
        // stop flag).
        let cycles = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&cycles);
        let r = AgentRunner::spawn(
            "backoff-idle",
            IdleStrategy::Backoff {
                spins: 2,
                yields: 2,
                sleep: Duration::from_micros(200),
            },
            move || {
                c.fetch_add(1, Ordering::Relaxed);
                false
            },
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while cycles.load(Ordering::Relaxed) < 20 {
            assert!(std::time::Instant::now() < deadline, "runner never cycled");
            std::thread::sleep(Duration::from_millis(1));
        }
        r.stop();
    }

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
        let r = AgentRunner::spawn(
            "panics",
            IdleStrategy::Sleep(Duration::from_millis(1)),
            || {
                panic!("deliberate");
            },
        )
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
        assert_eq!(
            count.load(Ordering::Relaxed),
            n,
            "agent thread still running after drop"
        );
    }
}
