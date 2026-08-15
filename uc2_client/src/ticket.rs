// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Ticket<R>: a blocking handle that is also a Future (spec §7, M5 Task 5).

use crate::ClientError;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// Internal state of a single-response slot.
struct State {
    /// None = waiting; Some(result) = resolved. Position is discarded at this layer.
    done: Option<Result<(u64, Vec<u8>), ClientError>>,
    /// Waker to notify on resolution (Future path).
    waker: Option<Waker>,
}

/// The internal core backing a Ticket. Consumed by `pipelined.rs`'s driver:
/// `user_data` is `Arc::into_raw(core.clone())`, reclaimed by exactly one
/// `Arc::from_raw` per accepted request (completion callback or shutdown
/// drain).
pub(crate) struct TicketCore {
    inner: Mutex<State>,
    cv: Condvar,
}

impl TicketCore {
    /// Create a new unresolved ticket core.
    pub(crate) fn new() -> Self {
        TicketCore {
            inner: Mutex::new(State {
                done: None,
                waker: None,
            }),
            cv: Condvar::new(),
        }
    }

    /// Resolve with bytes (or an error). First resolution wins; later calls are ignored.
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
    /// Tracks if poll() has already returned Ready; polling after completion panics.
    taken: bool,
}

impl<R: serde::de::DeserializeOwned> Ticket<R> {
    /// Block until resolved, then decode. This call itself has no local
    /// timeout — it blocks until the driver resolves the ticket. A
    /// `ClientError::Timeout` CAN still arrive, but only because the
    /// engine's own deadline sweep (`request_timeout`, configured at
    /// attach/connect time) resolved the underlying request as
    /// `Outcome::TimedOut`; `wait()` never imposes one on its own.
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
        let deadline = Instant::now() + d;
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
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ClientError::Timeout(d));
            }
            let (new_state, timed_out) = self.core.cv.wait_timeout(state, remaining).unwrap();
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
        let this = self.get_mut();
        if this.taken {
            panic!("Ticket polled after completion");
        }
        let mut state = this.core.inner.lock().unwrap();
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
            this.taken = true;
            Poll::Ready(output)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Create a (Ticket, TicketCore) pair for pipelining. Consumed by
/// `pipelined.rs`'s `PipelinedClient::dispatch`.
pub(crate) fn ticket_pair<R>() -> (Ticket<R>, Arc<TicketCore>) {
    let core = Arc::new(TicketCore::new());
    let ticket = Ticket {
        core: core.clone(),
        _phantom: std::marker::PhantomData,
        taken: false,
    };
    (ticket, core)
}

/// `decode_response`'s two old byte-level cases (undersized payload, bincode
/// decode failure) moved down here from the deleted `matcher.rs` as `Ticket`
/// decode tests. Only the decode-failure case survives: the "undersized
/// payload" case is now structurally impossible to reach from a `Ticket` —
/// the ENGINE strips and validates the 8-byte `position` prefix
/// (`engine.rs`'s `handle_record`, `MSG_V2_RESPONSE` arm) before a completion
/// is ever produced, so a `Ticket` only ever sees the body bytes underneath.
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

    /// Noop waker for manual polling (no-op wake).
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        fn clone(_: *const ()) -> RawWaker {
            noop_raw()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop_fn(_: *const ()) {}
        fn noop_raw() -> RawWaker {
            static VT: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
            RawWaker::new(std::ptr::null(), &VT)
        }
        unsafe { Waker::from_raw(noop_raw()) }
    }

    #[test]
    fn polling_after_ready_panics() {
        let (mut t, core) = ticket_pair::<u64>();
        core.resolve(resolved_bytes(42));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut t_pin = unsafe { std::pin::Pin::new_unchecked(&mut t) };

        // First poll: should succeed
        match t_pin.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 42),
            _ => panic!("Expected Ready on first poll"),
        }

        // Second poll: should panic with exact message
        // Save original hook once at the top
        let orig_hook = std::panic::take_hook();
        // Install silent hook for the panicking region
        std::panic::set_hook(Box::new(|_| {}));

        // Run catch_unwind and capture result
        let second_poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut t_pin = unsafe { std::pin::Pin::new_unchecked(&mut t) };
            let _ = t_pin.as_mut().poll(&mut cx);
        }));

        // Restore hook UNCONDITIONALLY before any assertion
        std::panic::set_hook(orig_hook);

        // Now assert on the captured result
        assert!(
            second_poll_result.is_err(),
            "Expected panic on second poll"
        );

        // Verify exact panic message by downcasting
        let payload = second_poll_result.unwrap_err();
        let msg = payload
            .downcast_ref::<&str>()
            .copied()
            .unwrap_or_else(|| {
                payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .expect("panic payload is a string")
            });
        assert!(
            msg.contains("Ticket polled after completion"),
            "wrong panic message: {msg}"
        );
    }

    #[test]
    fn wait_timeout_respects_budget() {
        let (t, core) = ticket_pair::<u64>();
        let start = Instant::now();

        // Spawn thread that notifies repeatedly WITHOUT resolving
        let core_clone = core.clone();
        let h = std::thread::spawn(move || {
            for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(2));
                core_clone.cv.notify_all();
            }
        });

        // wait_timeout with 100ms budget must respect the real deadline
        let result = t.wait_timeout(Duration::from_millis(100));
        let elapsed = start.elapsed();

        h.join().unwrap();

        // Should timeout
        assert!(matches!(result, Err(crate::ClientError::Timeout(d)) if d == Duration::from_millis(100)));

        // Elapsed must be within budget (under 1s, over 90ms)
        assert!(
            elapsed < Duration::from_secs(1),
            "Elapsed {:?} exceeded generous 1s bound (old code would run ~5s with 50 notifies)",
            elapsed
        );
        assert!(
            elapsed > Duration::from_millis(90),
            "Elapsed {:?} suspiciously low",
            elapsed
        );
    }
}
