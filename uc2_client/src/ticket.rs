// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Ticket<R>: a blocking handle that is also a Future (spec §7, M5 Task 5).

use crate::ClientError;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Internal state of a single-response slot.
struct State {
    /// None = waiting; Some(result) = resolved. Position is discarded at this layer.
    done: Option<Result<(u64, Vec<u8>), ClientError>>,
    /// Waker to notify on resolution (Future path).
    waker: Option<Waker>,
}

/// The internal core backing a Ticket (consumed from Task 6).
pub(crate) struct TicketCore {
    inner: Mutex<State>,
    cv: Condvar,
}

impl TicketCore {
    /// Create a new unresolved ticket core (consumed from Task 6).
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        TicketCore {
            inner: Mutex::new(State {
                done: None,
                waker: None,
            }),
            cv: Condvar::new(),
        }
    }

    /// Resolve with bytes (or an error). First resolution wins; later calls are ignored (consumed from Task 6).
    #[allow(dead_code)]
    pub(crate) fn resolve(&self, r: Result<(u64, Vec<u8>), ClientError>) {
        let mut state = self.inner.lock().unwrap();
        if state.done.is_some() {
            return; // Already resolved; ignore.
        }
        state.done = Some(r);
        let waker = state.waker.take();
        drop(state);
        self.cv.notify_all();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// A blocking handle that is also a Future, typed for response R.
pub struct Ticket<R> {
    core: Arc<TicketCore>,
    _phantom: std::marker::PhantomData<fn() -> R>,
}

impl<R: serde::de::DeserializeOwned> Ticket<R> {
    /// Block until resolved, then decode. Returns `Err(Timeout(d))` if timeout elapses.
    pub fn wait(self) -> Result<R, ClientError> {
        let mut state = self.core.inner.lock().unwrap();
        loop {
            if let Some(result) = state.done.take() {
                return match result {
                    Ok((_, bytes)) => {
                        bincode::serde::decode_from_slice::<R, _>(
                            &bytes,
                            bincode::config::standard(),
                        )
                        .map(|(v, _)| v)
                        .map_err(|e| ClientError::Decode(e.to_string()))
                    }
                    Err(e) => Err(e),
                };
            }
            state = self.core.cv.wait(state).unwrap();
        }
    }

    /// Block until resolved or timeout, then decode. Returns `Err(Timeout(d))` on timeout.
    pub fn wait_timeout(self, d: Duration) -> Result<R, ClientError> {
        let mut state = self.core.inner.lock().unwrap();
        loop {
            if let Some(result) = state.done.take() {
                return match result {
                    Ok((_, bytes)) => {
                        bincode::serde::decode_from_slice::<R, _>(
                            &bytes,
                            bincode::config::standard(),
                        )
                        .map(|(v, _)| v)
                        .map_err(|e| ClientError::Decode(e.to_string()))
                    }
                    Err(e) => Err(e),
                };
            }
            let (new_state, timed_out) = self.core.cv.wait_timeout(state, d).unwrap();
            state = new_state;
            if timed_out.timed_out() {
                return Err(ClientError::Timeout(d));
            }
        }
    }
}

impl<R: serde::de::DeserializeOwned> Future for Ticket<R> {
    type Output = Result<R, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.core.inner.lock().unwrap();
        if let Some(result) = state.done.take() {
            let output = match result {
                Ok((_, bytes)) => {
                    bincode::serde::decode_from_slice::<R, _>(
                        &bytes,
                        bincode::config::standard(),
                    )
                    .map(|(v, _)| v)
                    .map_err(|e| ClientError::Decode(e.to_string()))
                }
                Err(e) => Err(e),
            };
            Poll::Ready(output)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Create a (Ticket, TicketCore) pair for pipelining (consumed from Task 6).
#[allow(dead_code)]
pub(crate) fn ticket_pair<R>() -> (Ticket<R>, Arc<TicketCore>) {
    let core = Arc::new(TicketCore::new());
    let ticket = Ticket {
        core: core.clone(),
        _phantom: std::marker::PhantomData,
    };
    (ticket, core)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// Hand-rolled block_on: thread-parker waker, no runtime dep (spec §9).
    fn block_on<F: std::future::Future>(mut fut: F) -> F::Output {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn raw(thread: std::thread::Thread) -> RawWaker {
            fn clone(p: *const ()) -> RawWaker {
                raw(unsafe { (*(p as *const std::thread::Thread)).clone() })
            }
            fn wake(p: *const ()) {
                unsafe { Box::from_raw(p as *mut std::thread::Thread) }.unpark();
            }
            fn wake_by_ref(p: *const ()) {
                unsafe { &*(p as *const std::thread::Thread) }.unpark();
            }
            fn drop_fn(p: *const ()) {
                drop(unsafe { Box::from_raw(p as *mut std::thread::Thread) });
            }
            static VT: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
            RawWaker::new(Box::into_raw(Box::new(thread)) as *const (), &VT)
        }
        let waker = unsafe { Waker::from_raw(raw(std::thread::current())) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::park(),
            }
        }
    }

    fn resolved_bytes(v: u64) -> Result<(u64, Vec<u8>), crate::ClientError> {
        Ok((7, bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap()))
    }

    #[test]
    fn resolve_then_wait_decodes() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(resolved_bytes(42));
        assert_eq!(t.wait().unwrap(), 42);
    }

    #[test]
    fn wait_blocks_until_a_late_resolve() {
        let (t, core) = ticket_pair::<u64>();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            core.resolve(resolved_bytes(9));
        });
        assert_eq!(t.wait().unwrap(), 9);
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_elapses_to_timeout_error() {
        let (t, _core) = ticket_pair::<u64>();
        let err = t.wait_timeout(Duration::from_millis(30)).unwrap_err();
        assert!(matches!(err, crate::ClientError::Timeout(d) if d == Duration::from_millis(30)));
    }

    #[test]
    fn future_resolves_under_hand_rolled_block_on() {
        let (t, core) = ticket_pair::<u64>();
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            core.resolve(resolved_bytes(11));
        });
        assert_eq!(block_on(t).unwrap(), 11);
        h.join().unwrap();
    }

    #[test]
    fn second_resolve_is_ignored() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(resolved_bytes(1));
        core.resolve(Err(crate::ClientError::ShutDown)); // must not clobber
        assert_eq!(t.wait().unwrap(), 1);
    }

    #[test]
    fn error_resolution_surfaces_the_error() {
        let (t, core) = ticket_pair::<u64>();
        core.resolve(Err(crate::ClientError::Retry));
        assert!(matches!(t.wait(), Err(crate::ClientError::Retry)));
    }

    #[test]
    fn dropping_the_ticket_then_resolving_is_harmless() {
        let (t, core) = ticket_pair::<u64>();
        drop(t);
        core.resolve(resolved_bytes(5)); // orphan: no panic, no leak beyond core's Arc
        assert_eq!(Arc::strong_count(&core), 1, "ticket side released its ref");
    }

    #[test]
    fn decode_failure_surfaces_as_decode_error() {
        let (t, core) = ticket_pair::<String>();
        core.resolve(Ok((0, vec![0xFF]))); // truncated bincode varint
        assert!(matches!(t.wait(), Err(crate::ClientError::Decode(_))));
    }
}
