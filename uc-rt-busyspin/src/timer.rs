//! Poll-based timers for the busy-spin runtime.
//!
//! No timer wheel, no reactor: `Sleep`/`Timeout` simply compare the monotonic
//! clock against a deadline on each poll. Under the busy-spin executor (which
//! re-polls unconditionally) this is a tight clock check; no waker is needed.
//! This keeps the runtime free of any tokio time-driver / reactor dependency.

use std::future::Future;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant as StdInstant};

/// Monotonic instant backing the runtime, wrapping `std::time::Instant`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BusyInstant(pub StdInstant);

impl Add<Duration> for BusyInstant {
    type Output = Self;
    fn add(self, d: Duration) -> Self {
        BusyInstant(self.0 + d)
    }
}
impl AddAssign<Duration> for BusyInstant {
    fn add_assign(&mut self, d: Duration) {
        self.0 += d;
    }
}
impl Sub<Duration> for BusyInstant {
    type Output = Self;
    fn sub(self, d: Duration) -> Self {
        BusyInstant(self.0 - d)
    }
}
impl SubAssign<Duration> for BusyInstant {
    fn sub_assign(&mut self, d: Duration) {
        self.0 -= d;
    }
}
impl Sub<BusyInstant> for BusyInstant {
    type Output = Duration;
    fn sub(self, other: BusyInstant) -> Duration {
        self.0 - other.0
    }
}

impl openraft_rt::Instant for BusyInstant {
    fn now() -> Self {
        BusyInstant(StdInstant::now())
    }
}

/// Poll-based sleep: ready once the deadline passes.
pub struct BusySleep {
    deadline: StdInstant,
}

impl BusySleep {
    pub fn until(deadline: BusyInstant) -> Self {
        Self { deadline: deadline.0 }
    }
    pub fn after(d: Duration) -> Self {
        Self { deadline: StdInstant::now() + d }
    }
}

impl Future for BusySleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if StdInstant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Timeout error returned by `BusyTimeout` when the deadline is reached first.
#[derive(Debug)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

/// Poll-based timeout: races the inner future against a deadline.
///
/// The inner future is boxed-and-pinned so `BusyTimeout` is always `Unpin`
/// (avoids a pin-projection dependency); allocation cost is irrelevant on the
/// election/heartbeat paths where openraft uses timeouts.
pub struct BusyTimeout<F> {
    deadline: StdInstant,
    fut: Pin<Box<F>>,
}

impl<F> BusyTimeout<F> {
    pub fn until(deadline: BusyInstant, fut: F) -> Self {
        Self { deadline: deadline.0, fut: Box::pin(fut) }
    }
    pub fn after(d: Duration, fut: F) -> Self {
        Self { deadline: StdInstant::now() + d, fut: Box::pin(fut) }
    }
}

impl<F: Future> Future for BusyTimeout<F> {
    type Output = Result<F::Output, Elapsed>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `Self: Unpin` (Pin<Box<F>> + Copy fields), so `get_mut` is safe.
        let this = self.get_mut();
        if let Poll::Ready(v) = this.fut.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        if StdInstant::now() >= this.deadline {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    }
}
