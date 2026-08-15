// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `PipelinedClient`: an io_uring-shaped submit/poll client over [`Engine`]
//! (spec 2026-08-13 §5/§9). A caller holds a WINDOW of outstanding
//! [`Ticket`]s instead of blocking one round trip per call — `submit`/
//! `try_submit`/`query_*` hand a command to the engine and return
//! immediately; one hand-spawned driver thread polls completions and
//! resolves the matching ticket.
//!
//! ## The Arc-leak/reclaim contract (pins Key Mechanics #1)
//!
//! `user_data` handed to the engine is `Arc::into_raw(core.clone()) as u64`
//! (asserted 64-bit below — the pointer must round-trip through a `u64`
//! losslessly). The engine's central contract (`engine.rs`: "every accepted
//! `try_submit`/`try_query` produces exactly one completion for its
//! `user_data`, in bounded time") makes this leak-free as long as this module
//! honors ITS half: every leaked raw pointer is reclaimed by exactly one
//! `Arc::from_raw`, on exactly one of these paths:
//! - the driver's completion callback (`resolve`, below) — the normal case;
//! - the driver's shutdown drain (`PollHalf::drain_abort`) — inflight
//!   requests still open when `shutdown`/`Drop` fires;
//! - the door (`dispatch`, below) — a submit the ENGINE REFUSED (never
//!   reached the ring, so no completion will ever arrive for it) reclaims
//!   immediately on its own error path.
//!
//! ## Threading (Key Mechanics #2)
//!
//! The driver is a hand-spawned `std::thread`, NOT `uc2_log::agent::AgentRunner`:
//! `AgentRunner`'s contract forbids blocking in the duty cycle, and
//! `WaitStrategy::Park` parks for up to 1ms. `PollHalf` lives and dies on the
//! driver thread — it never crosses threads, including at shutdown: the
//! drain-abort call happens INSIDE the driver thread, right before it
//! returns, not from `shutdown`/`Drop`'s caller thread.
//!
//! ## `SendHalf` is `!Sync` (Key Mechanics #4)
//!
//! `PipelinedClient` holds `Mutex<SendHalf>`; the critical section is
//! claim+try_write (~100ns). Callers chasing max throughput from multiple
//! submitter threads should use [`Engine::attach`] directly and clone
//! `SendHalf` once per thread instead of sharing one `PipelinedClient`.

#[cfg(not(target_pointer_width = "64"))]
compile_error!("uc2_client's pipelined layer requires 64-bit pointers");

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;

use uc_protocol::ring::{RingError, RingWaitHandle};

use crate::ticket::{TicketCore, ticket_pair};
use crate::wait::Idle;
use crate::{
    ClientError, Completion, Consistency, Engine, EngineConfig, EngineStats, Outcome, PollHalf,
    SendHalf, SubmitError, Ticket, WaitStrategy,
};

/// How long `submit`/`query_*` retries `Backpressure`/`NotServing` before
/// giving up (parity with the old `Client`'s `BACKPRESSURE_GRACE`).
const BACKPRESSURE_GRACE: Duration = Duration::from_secs(1);
/// Retry sleep on `SubmitError::Backpressure` (ingress/query ring full or the
/// inflight window is exhausted — expected to clear fast).
const RETRY_BACKPRESSURE: Duration = Duration::from_micros(100);
/// Retry sleep on `SubmitError::NotServing` (elections run 150-300ms; no
/// point retrying at `RETRY_BACKPRESSURE`'s cadence).
const RETRY_NOT_SERVING: Duration = Duration::from_millis(1);

/// [`PipelinedClient::connect`] configuration.
pub struct PipelinedConfig {
    /// How the driver thread waits between empty poll cycles.
    pub driver_wait: WaitStrategy,
    /// Inflight window handed to the underlying [`Engine`].
    pub max_inflight: u32,
    /// Per-request deadline, enforced by the engine's deadline sweep.
    pub request_timeout: Duration,
    /// Refuse submits/queries when the node isn't a serving leader instead of
    /// free-running into a dead/non-leader node.
    pub serving_gate: bool,
}

impl Default for PipelinedConfig {
    fn default() -> Self {
        PipelinedConfig {
            driver_wait: WaitStrategy::Park,
            max_inflight: 4096,
            request_timeout: Duration::from_secs(10),
            serving_gate: true,
        }
    }
}

/// The pipelined client SDK: submit a WINDOW of outstanding requests, each
/// resolved by a [`Ticket`] (blocking `wait()` or `poll()` as a `Future`).
/// `Send + Sync` — share via `Arc<PipelinedClient>`, but see the module docs:
/// the submit-side critical section is a `Mutex<SendHalf>`, so callers
/// chasing max single-thread throughput want `Engine::attach` + a per-thread
/// `SendHalf` clone instead.
pub struct PipelinedClient {
    send: Mutex<SendHalf>,
    stop: Arc<AtomicBool>,
    wait_handle: RingWaitHandle,
    driver: Option<std::thread::JoinHandle<()>>,
}

impl PipelinedClient {
    /// Attach to a running node's instance directory and spawn the driver
    /// thread. See [`Engine::attach`] for the attach-time contract (app_id/
    /// protocol validation, egress subscription order).
    pub fn connect(
        instance_dir: &Path,
        app_id: &str,
        cfg: PipelinedConfig,
    ) -> Result<PipelinedClient, ClientError> {
        let engine_cfg = EngineConfig {
            max_inflight: cfg.max_inflight,
            request_timeout: cfg.request_timeout,
            max_payload: None,
            serving_gate: cfg.serving_gate,
            start_seq: 0,
        };
        let (send, poll) = Engine::attach(instance_dir, app_id, engine_cfg)?;
        let wait_handle = poll.wait_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let driver = spawn_driver(poll, Arc::clone(&stop), cfg.driver_wait, cfg.request_timeout)
            .map_err(RingError::Io)?;
        Ok(PipelinedClient { send: Mutex::new(send), stop, wait_handle, driver: Some(driver) })
    }

    /// Submit a command; nonblocking apart from up to [`BACKPRESSURE_GRACE`]
    /// of retry on transient `Backpressure`/`NotServing` (see the module's
    /// grace-loop docs on [`Self::dispatch`]). Returns a [`Ticket`] the
    /// driver resolves once the engine emits a completion.
    pub fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<Ticket<R>, ClientError> {
        let bytes = encode(cmd)?;
        self.dispatch(&bytes, true, |send, ud, b| send.try_submit(ud, b))
    }

    /// Fail-fast submit: `Backpressure`/`NotServing` map immediately instead
    /// of retrying.
    pub fn try_submit<C: Serialize, R: DeserializeOwned>(
        &self,
        cmd: &C,
    ) -> Result<Ticket<R>, ClientError> {
        let bytes = encode(cmd)?;
        self.dispatch(&bytes, false, |send, ud, b| send.try_submit(ud, b))
    }

    /// Linearizable read (routed through the node's quorum read barrier).
    pub fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, |send, ud, b| {
            send.try_query(ud, b, Consistency::Linearizable)
        })
    }

    /// Snapshot (non-linearizable) read, answered from the local replica.
    pub fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, |send, ud, b| send.try_query(ud, b, Consistency::Snapshot))
    }

    pub fn client_id(&self) -> u32 {
        self.send.lock().unwrap().client_id()
    }

    pub fn instance_id(&self) -> u128 {
        self.send.lock().unwrap().instance_id()
    }

    /// The cnc page's current `leader_hint` (`None` = unknown).
    pub fn leader_hint(&self) -> Option<u32> {
        self.send.lock().unwrap().leader_hint()
    }

    /// A point-in-time snapshot of the underlying engine's counters.
    pub fn stats(&self) -> EngineStats {
        self.send.lock().unwrap().stats()
    }

    /// Stop the driver thread (failing every still-inflight ticket with
    /// [`ClientError::ShutDown`]) and join it. Also runs on `Drop`, so this
    /// is optional — but calling it explicitly lets a caller observe the
    /// join complete before moving on.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    /// Key Mechanics #2: `stop.store(true)`, wake a parked driver, join.
    /// Idempotent — safe to call from both `shutdown` and the `Drop` that
    /// runs immediately after it consumes `self`.
    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.wait_handle.wake(); // interrupt a parked driver promptly
        if let Some(h) = self.driver.take() {
            let _ = h.join();
        }
    }

    /// The shared submit door for `submit`/`try_submit`/`query_*` (Key
    /// Mechanics #1 and #6).
    ///
    /// Leaks exactly one `Arc<TicketCore>` reference into `user_data` up
    /// front. On the happy path (`Ok(())` from `submit_fn`) that leaked
    /// reference is now the driver's obligation — it will be reclaimed by
    /// `resolve`'s `Arc::from_raw` (or the shutdown drain) exactly once. On
    /// every OTHER path — the engine refused the request outright, or the
    /// grace window on a transient refusal expired — this function reclaims
    /// it itself before returning `Err`, because no completion will ever
    /// arrive for a request the engine never accepted.
    ///
    /// `retry`: `true` for `submit`/`query_*` (grace loop, [`BACKPRESSURE_GRACE`]
    /// total, `Backpressure` retried at [`RETRY_BACKPRESSURE`] and
    /// `NotServing` at [`RETRY_NOT_SERVING`]); `false` for `try_submit`
    /// (fail-fast — the first refusal maps immediately).
    fn dispatch<R: DeserializeOwned>(
        &self,
        bytes: &[u8],
        retry: bool,
        submit_fn: impl Fn(&SendHalf, u64, &[u8]) -> Result<(), SubmitError>,
    ) -> Result<Ticket<R>, ClientError> {
        let (ticket, core) = ticket_pair::<R>();
        // Key Mechanics #1: user_data = Arc::into_raw(core.clone()) as u64.
        let user_data = Arc::into_raw(core.clone()) as u64;
        let deadline = Instant::now() + BACKPRESSURE_GRACE;
        loop {
            let outcome = {
                let send = self.send.lock().unwrap();
                submit_fn(&send, user_data, bytes)
            };
            match outcome {
                Ok(()) => return Ok(ticket),
                Err(SubmitError::Backpressure) => {
                    if retry && Instant::now() < deadline {
                        std::thread::sleep(RETRY_BACKPRESSURE);
                        continue;
                    }
                    reclaim(user_data);
                    return Err(ClientError::BackpressureFull);
                }
                Err(SubmitError::NotServing) => {
                    if retry && Instant::now() < deadline {
                        std::thread::sleep(RETRY_NOT_SERVING);
                        continue;
                    }
                    reclaim(user_data);
                    return Err(ClientError::NotLeader { hint: self.leader_hint() });
                }
                Err(SubmitError::PayloadTooLarge { len, max }) => {
                    reclaim(user_data);
                    return Err(ClientError::PayloadTooLarge { len, max });
                }
                Err(SubmitError::InstanceRestart { attached, current }) => {
                    reclaim(user_data);
                    return Err(ClientError::InstanceRestart { attached, current });
                }
                Err(SubmitError::Ring(e)) => {
                    reclaim(user_data);
                    return Err(ClientError::Ring(e));
                }
            }
        }
    }
}

impl Drop for PipelinedClient {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn encode<C: Serialize>(cmd: &C) -> Result<Vec<u8>, ClientError> {
    bincode::serde::encode_to_vec(cmd, bincode::config::standard())
        .map_err(|e| ClientError::Decode(e.to_string()))
}

/// Reclaim a `user_data` raw pointer the engine refused to accept (never
/// handed to the driver, so no completion will ever reclaim it). Only ever
/// called from `dispatch`'s own error arms, immediately after `dispatch`
/// minted `ud` and before it was ever handed to `submit_fn` successfully —
/// `Ok(())` is the only outcome that hands the reclaim obligation onward
/// (to the driver), and this function is never called on that path, so
/// `ud` is guaranteed not yet reclaimed by anything else.
fn reclaim(ud: u64) {
    // SAFETY: `ud` is a value produced by `Arc::into_raw::<TicketCore>` in
    // `dispatch` that has not yet been reclaimed — see the doc comment above.
    drop(unsafe { Arc::from_raw(ud as *const TicketCore) });
}

/// The driver thread: one hand-spawned `std::thread` (not `AgentRunner` — see
/// the module docs) that polls `poll_half` in a loop, resolving each
/// [`Completion`] against the [`TicketCore`] its `user_data` points to, and
/// idles per `ws` between empty poll cycles. `PollHalf` is moved in here and
/// never leaves this thread, including its shutdown drain.
fn spawn_driver(
    mut poll: PollHalf,
    stop: Arc<AtomicBool>,
    ws: WaitStrategy,
    request_timeout: Duration,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name("uc2-pipelined-driver".into()).spawn(move || {
        let wh = poll.wait_handle();
        let mut resolve = |c: Completion<'_>| {
            // SAFETY: user_data is the raw Arc<TicketCore> leaked by
            // `PipelinedClient::dispatch`; the engine emits exactly one
            // completion per accepted request, so this is the one matching
            // `from_raw` for that leak.
            let core = unsafe { Arc::from_raw(c.user_data as *const TicketCore) };
            core.resolve(match c.outcome {
                Outcome::Response(bytes) => Ok((c.position.unwrap_or(0), bytes.to_vec())),
                Outcome::NotLeader { hint } => Err(ClientError::NotLeader { hint }),
                Outcome::Retry => Err(ClientError::Retry),
                Outcome::TimedOut => Err(ClientError::Timeout(request_timeout)),
                Outcome::InstanceRestart { attached, current } => {
                    Err(ClientError::InstanceRestart { attached, current })
                }
            });
        };
        let mut idle = Idle::for_strategy(ws);
        while !stop.load(Ordering::Relaxed) {
            let n = poll.poll(&mut resolve);
            if n > 0 {
                idle = Idle::for_strategy(ws); // progress resets the ladder
                continue;
            }
            match ws {
                WaitStrategy::BusySpin => std::hint::spin_loop(),
                WaitStrategy::BackoffYield | WaitStrategy::Backoff => idle.idle(),
                WaitStrategy::Park => {
                    let seq = wh.current_seq();
                    wh.arm();
                    if poll.poll(&mut resolve) == 0 && !stop.load(Ordering::Relaxed) {
                        wh.park(seq, Duration::from_millis(1));
                    }
                    wh.disarm();
                }
            }
        }
        // Shutdown drain ON this thread — the PollHalf never crosses
        // threads. Key Mechanics #1's third reclaim path: everything still
        // inflight at this point never got (and now never will get) a real
        // completion, so it fails with ShutDown instead of hanging forever.
        poll.drain_abort(|ud| {
            // SAFETY: same contract as `resolve` above — `ud` is a raw
            // Arc<TicketCore> leaked by `dispatch` for a request that was
            // accepted (so ownership passed to the driver) but never
            // completed before shutdown; `drain_abort` yields each such
            // slot's `user_data` exactly once.
            let core = unsafe { Arc::from_raw(ud as *const TicketCore) };
            core.resolve(Err(ClientError::ShutDown));
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The module docs and the task brief both pin `PipelinedClient` as
    /// `Send + Sync` ("share as Arc"); a compile-time check, not a runtime
    /// assertion, so a future field addition that breaks it fails the build
    /// here rather than surfacing as a confusing downstream trait-bound error.
    #[test]
    fn pipelined_client_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PipelinedClient>();
    }
}
