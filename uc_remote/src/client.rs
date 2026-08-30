// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`RemoteClient`] — the blocking convenience client, layered on the
//! [`crate::RemoteEngine`] halves the way `uc_client::Client` sits on its
//! `Engine`.
//!
//! # What this is for, and what it is not
//!
//! One request, one [`Ticket`], one blocking `wait`: the shape a CLI
//! (`counter-remote`), a crash test worker, or any caller with a handful of
//! outstanding requests wants. It costs an `Arc` allocation and a condvar
//! wake per request, and a mutex across `try_submit` so the handle can be
//! `Sync`. **It is not the path M13's throughput bars measure** — that is
//! [`crate::RemoteSendHalf::try_submit`] plus [`crate::RemotePollHalf::poll`]
//! on the caller's own threads.
//!
//! # The promise (unchanged)
//!
//! Every `submit`/`query` ends in **exactly one** resolution: `Ok(
//! RemoteResponse)`, or `Err` of [`RemoteError::Expired`] /
//! [`RemoteError::Unknown`] / [`RemoteError::PayloadTooLarge`] /
//! [`RemoteError::TimedOut`] / [`RemoteError::Closed`]. `REDIRECT`,
//! `LEADER_CHANGED`, `RETRY` and connection loss are absorbed by the link.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::engine::{
    Consistency, RemoteCompletion, RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf,
    RemoteResponse, RemoteSendHalf, RemoteStats, RemoteWaitHandle, SubmitError,
};
use crate::error::RemoteError;

/// One outstanding request's resolution cell.
struct TicketCore {
    done: Mutex<Option<Result<RemoteResponse, RemoteError>>>,
    cv: Condvar,
}

impl TicketCore {
    fn new() -> TicketCore {
        TicketCore { done: Mutex::new(None), cv: Condvar::new() }
    }

    fn set(&self, r: Result<RemoteResponse, RemoteError>) {
        let mut g = self.done.lock().unwrap();
        if g.is_none() {
            *g = Some(r);
        }
        drop(g);
        self.cv.notify_all();
    }
}

/// A handle on one outstanding request.
pub struct Ticket {
    core: Arc<TicketCore>,
}

impl Ticket {
    /// Block until the request resolves. Only the client's own
    /// `request_timeout` or `shutdown` can end the wait without an answer.
    pub fn wait(self) -> Result<RemoteResponse, RemoteError> {
        let mut g = self.core.done.lock().unwrap();
        loop {
            if let Some(r) = g.take() {
                return r;
            }
            g = self.core.cv.wait(g).unwrap();
        }
    }

    /// Like [`Ticket::wait`] with a caller-side bound. Giving up here abandons
    /// the request: the link still resolves it, the answer is just dropped.
    pub fn wait_timeout(self, d: Duration) -> Result<RemoteResponse, RemoteError> {
        let deadline = Instant::now() + d;
        let mut g = self.core.done.lock().unwrap();
        loop {
            if let Some(r) = g.take() {
                return r;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RemoteError::TimedOut);
            }
            let (guard, _) = self.core.cv.wait_timeout(g, deadline - now).unwrap();
            g = guard;
        }
    }
}

/// A connected remote client. `Send + Sync`; share it behind an `Arc` or a
/// reference — every method takes `&self`.
pub struct RemoteClient {
    send: Mutex<RemoteSendHalf>,
    wait: RemoteWaitHandle,
    stop: Arc<AtomicBool>,
    poller: Mutex<Option<JoinHandle<()>>>,
    request_timeout: Duration,
}

impl RemoteClient {
    /// Connect (see [`RemoteEngine::connect`] for the error contract) and
    /// start this client's own poller thread.
    pub fn connect(cfg: RemoteConfig) -> Result<Self, RemoteError> {
        let request_timeout = cfg.request_timeout;
        let (send, poll) = RemoteEngine::connect(cfg)?;
        let wait = poll.wait_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let poller = {
            let stop = Arc::clone(&stop);
            let wait = wait.clone();
            std::thread::Builder::new()
                .name("uc2-remote-poll".into())
                .spawn(move || poller_loop(poll, stop, wait))?
        };
        Ok(RemoteClient {
            send: Mutex::new(send),
            wait,
            stop,
            poller: Mutex::new(Some(poller)),
            request_timeout,
        })
    }

    /// Submit a command. Blocks while the edge's credits (or `max_inflight`)
    /// are exhausted, and gives up with [`RemoteError::TimedOut`] if the
    /// window never reopens within `request_timeout`.
    ///
    /// Note that the credit wait is a **separate** `request_timeout` budget
    /// from the one the returned [`Ticket`] then spends, so a caller that
    /// blocks the full wait here and then waits out the request can spend
    /// ~2 x `request_timeout` in total.
    pub fn submit(&self, cmd: &[u8]) -> Result<Ticket, RemoteError> {
        self.enqueue(None, cmd)
    }

    /// Ask a question. Same admission accounting as [`RemoteClient::submit`].
    pub fn query(&self, q: &[u8], consistency: Consistency) -> Result<Ticket, RemoteError> {
        self.enqueue(Some(consistency), q)
    }

    fn enqueue(&self, q: Option<Consistency>, bytes: &[u8]) -> Result<Ticket, RemoteError> {
        let core = Arc::new(TicketCore::new());
        // The engine's `user_data` is an owned reference to the ticket; the
        // completion path (or a refusal below) turns it back with exactly one
        // `Arc::from_raw`.
        let user_data = Arc::into_raw(Arc::clone(&core)) as u64;
        let deadline = Instant::now() + self.request_timeout;
        loop {
            let r = {
                let s = self.send.lock().unwrap();
                match q {
                    None => s.try_submit(user_data, bytes),
                    Some(c) => s.try_query(user_data, c, bytes),
                }
            };
            match r {
                Ok(()) => return Ok(Ticket { core }),
                Err(SubmitError::Backpressure) => {
                    if Instant::now() >= deadline {
                        reclaim(user_data);
                        return Err(RemoteError::TimedOut);
                    }
                    // Park on the completion signal: a completion is exactly
                    // what reopens the window.
                    self.wait.park(Duration::from_micros(200));
                }
                Err(SubmitError::Closed) => {
                    reclaim(user_data);
                    return Err(RemoteError::Closed);
                }
                Err(SubmitError::PayloadTooLarge) => {
                    reclaim(user_data);
                    return Err(RemoteError::PayloadTooLarge);
                }
            }
        }
    }

    pub fn stats(&self) -> RemoteStats {
        self.send.lock().unwrap().stats()
    }

    pub fn leader(&self) -> Option<(u32, String)> {
        self.send.lock().unwrap().leader()
    }

    pub fn is_connected(&self) -> bool {
        self.send.lock().unwrap().is_connected()
    }

    pub fn connected_addr(&self) -> Option<String> {
        self.send.lock().unwrap().connected_addr()
    }

    pub fn client_id(&self) -> u64 {
        self.send.lock().unwrap().client_id()
    }

    /// Close the connection and fail every outstanding request with
    /// [`RemoteError::Closed`]. Idempotent; dropping the client does the same.
    pub fn shutdown(&self) {
        self.send.lock().unwrap().shutdown();
        self.stop.store(true, Ordering::Release);
        self.wait.wake();
        let handle = self.poller.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for RemoteClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.send.lock().unwrap();
        f.debug_struct("RemoteClient")
            .field("client_id", &s.client_id())
            .field("addr", &s.connected_addr())
            .field("credits", &s.credits())
            .field("inflight", &s.inflight())
            .finish()
    }
}

/// Give the ticket reference back without resolving it — the request was
/// refused, so no completion will ever carry this `user_data`.
fn reclaim(user_data: u64) {
    // SAFETY: `user_data` came from `Arc::into_raw::<TicketCore>` in
    // `enqueue`, and this is the ONLY path that reclaims a REFUSED request
    // (an accepted one is reclaimed by `resolve`, exactly once).
    drop(unsafe { Arc::from_raw(user_data as *const TicketCore) });
}

fn resolve(c: RemoteCompletion<'_>) {
    // SAFETY: as `reclaim` — one completion per accepted request, so this
    // runs exactly once for this pointer.
    let core = unsafe { Arc::from_raw(c.user_data as *const TicketCore) };
    let r = match c.outcome {
        RemoteOutcome::Response { body, replayed, expired } => {
            if expired {
                Err(RemoteError::Expired)
            } else {
                Ok(RemoteResponse {
                    position: c.position.unwrap_or(0),
                    bytes: Bytes::copy_from_slice(body),
                    replayed,
                })
            }
        }
        RemoteOutcome::Unknown => Err(RemoteError::Unknown),
        RemoteOutcome::PayloadTooLarge => Err(RemoteError::PayloadTooLarge),
        RemoteOutcome::TimedOut => Err(RemoteError::TimedOut),
        RemoteOutcome::Closed => Err(RemoteError::Closed),
    };
    core.set(r);
}

fn poller_loop(mut poll: RemotePollHalf, stop: Arc<AtomicBool>, wait: RemoteWaitHandle) {
    while !stop.load(Ordering::Acquire) {
        if poll.poll(resolve) == 0 {
            wait.park(Duration::from_millis(1));
        }
    }
    // Final drain: `shutdown` completes every outstanding request with
    // `Closed`, and those completions are queued before the threads stop.
    while poll.poll(resolve) > 0 {}
}
