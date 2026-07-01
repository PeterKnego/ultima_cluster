//! `UcBusySpinRuntime` — a never-park, CPU-busy-spin `AsyncRuntime` backend for
//! openraft, built for `ultima_cluster`.
//!
//! # Why
//!
//! The commit-floor decomposition attributed ~73% of UC's commit floor to
//! openraft's async *choreography* — many tokio task hops per commit, each a
//! ~8.8 µs cross-thread futex park/unpark. openraft routes every internal hop
//! through the [`AsyncRuntime`] seam (spawn, the `Mpsc`/`Watch`/`Oneshot`/`Mutex`
//! channels, and timers), and its core has no direct tokio dependency, so the
//! scheduler can be replaced wholesale without forking consensus logic.
//!
//! This runtime swaps tokio's parking multi-thread scheduler for a busy-poll
//! executor: worker threads re-poll their tasks in a tight loop and never park,
//! so an internal hop becomes a same-loop re-poll (~ns) instead of a futex wake.
//!
//! # Design (skeleton)
//!
//! * **Executor** (`executor`): fixed pool of busy-spin worker threads. Custom.
//! * **Timers** (`timer`): poll-based `Instant`/`Sleep`/`Timeout` — no reactor.
//! * **Channels / mutex**: *reused* from `openraft-rt-tokio`. tokio's `sync`
//!   primitives are runtime-agnostic (need no tokio reactor) and waker-correct,
//!   which is required at the openraft<->tokio API boundary. Replacing the hot
//!   internal channels with `Send` busy-spin rings is a later, boundary-aware
//!   optimization — the executor already delivers the futex-elimination win.
//!
//! Conformance: passes openraft's `AsyncRuntime` test [`Suite`] (see tests).
//!
//! # Boundary note
//!
//! openraft awaits the application's network/storage futures on this runtime.
//! Anything that needs a tokio *reactor* (quinn I/O, `tokio::fs`, `tokio::time`)
//! must either run on a co-resident tokio reactor (entered `Handle`) or be
//! decoupled behind a ring. A single-node M1 boot needs none of that (no peer
//! RPC; the journal is synchronous), which is why it is the first boot target.

mod executor;
mod timer;

use std::future::Future;
use std::time::Duration;

use openraft_rt::{AsyncRuntime, OptionalSend};
use openraft_rt_tokio::{TokioMpsc, TokioMutex, TokioOneshot, TokioWatch};

pub use executor::{JoinError, JoinHandle};
pub use timer::{BusyInstant, BusySleep, BusyTimeout, Elapsed};

/// Busy-spin `AsyncRuntime` for openraft. Zero-sized handle; the worker pool is
/// a process-global started on first use (or via [`AsyncRuntime::new`]).
#[derive(Debug, Default, Clone, Copy)]
pub struct UcBusySpinRuntime;

impl AsyncRuntime for UcBusySpinRuntime {
    type JoinError = JoinError;
    type JoinHandle<T: OptionalSend + 'static> = JoinHandle<T>;
    type Sleep = BusySleep;
    type Instant = BusyInstant;
    type TimeoutError = Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = BusyTimeout<T>;
    type ThreadLocalRng = rand::rngs::ThreadRng;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        executor::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        BusySleep::after(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        BusySleep::until(deadline)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        BusyTimeout::after(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        BusyTimeout::until(deadline, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        rand::rng()
    }

    type Mpsc = TokioMpsc;
    type Watch = TokioWatch;
    type Oneshot = TokioOneshot;
    type Mutex<T: OptionalSend + 'static> = TokioMutex<T>;

    fn new(threads: usize) -> Self {
        executor::ensure_started(threads);
        UcBusySpinRuntime
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
        T: OptionalSend,
    {
        executor::block_on(future)
    }
}

#[cfg(test)]
mod conformance {
    use openraft_rt::testing::Suite;

    use super::UcBusySpinRuntime;

    /// openraft's own conformance suite for an `AsyncRuntime` implementation:
    /// spawn/join, sleep/timeout, instant arithmetic, and the full mpsc/watch
    /// channel contracts. Green here = the runtime is openraft-conformant.
    #[test]
    fn async_runtime_conformance() {
        Suite::<UcBusySpinRuntime>::test_all();
    }
}
