// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared measurement plumbing for every `hop_bench` load role: a per-thread
//! latency histogram, a lock-free send-timestamp slot array (the same
//! `SLOTS`/`SLOT_MASK` correlation `m12_gate` and `m5_gate` use), and the one
//! machine-readable `RESULT {json}` line the orchestrators parse.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

pub const SLOTS: usize = 1 << 20;
pub const SLOT_MASK: usize = SLOTS - 1;
pub const HIST_MAX_NS: u64 = 60_000_000_000;
/// How long a load role waits for its last in-flight requests after the send
/// window closes before declaring the remainder lost.
pub const DRAIN_GRACE: Duration = Duration::from_secs(5);

pub fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram")
}

/// Send-timestamp slots for ONE stream of sequence numbers (one connection,
/// one engine): `stamp(seq)` before the send, `latency_ns(seq, now)` on the
/// response. Wraps at `SLOTS`; a stream never has more than `SLOTS` in flight.
pub struct SendClock {
    t0: Instant,
    send_ns: Box<[AtomicU64]>,
}

impl SendClock {
    pub fn new(t0: Instant) -> Self {
        SendClock {
            t0,
            send_ns: (0..SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
    #[inline]
    pub fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }
    #[inline]
    pub fn stamp(&self, seq: u64) {
        self.send_ns[(seq as usize) & SLOT_MASK].store(self.now_ns(), Ordering::Release);
    }
    #[inline]
    pub fn latency_ns(&self, seq: u64, now_ns: u64) -> u64 {
        now_ns
            .saturating_sub(self.send_ns[(seq as usize) & SLOT_MASK].load(Ordering::Acquire))
            .min(HIST_MAX_NS)
    }
}

/// One stream's tally (a connection, an engine); `merge` folds streams into
/// the process-wide result.
pub struct StreamStats {
    pub sends: u64,
    pub responses: u64,
    pub lost: u64,
    pub retried: u64,
    pub redirected: u64,
    /// Nanoseconds (on the role's `t0` clock) of the last response seen.
    pub last_response_ns: u64,
    /// Nanoseconds (same clock) when the send window closed.
    pub send_window_end_ns: u64,
    pub hist: Histogram<u64>,
}

impl StreamStats {
    pub fn new() -> Self {
        StreamStats {
            sends: 0,
            responses: 0,
            lost: 0,
            retried: 0,
            redirected: 0,
            last_response_ns: 0,
            send_window_end_ns: 0,
            hist: new_hist(),
        }
    }
    pub fn merge(&mut self, o: &StreamStats) {
        self.sends += o.sends;
        self.responses += o.responses;
        self.lost += o.lost;
        self.retried += o.retried;
        self.redirected += o.redirected;
        self.last_response_ns = self.last_response_ns.max(o.last_response_ns);
        self.send_window_end_ns = self.send_window_end_ns.max(o.send_window_end_ns);
        self.hist.add(&o.hist).expect("merge histogram");
    }
    /// Drain-inclusive elapsed: the later of the send window's end and the
    /// last response (the `m5_gate` convention).
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.last_response_ns.max(self.send_window_end_ns))
    }
    pub fn responses_per_sec(&self) -> f64 {
        let s = self.elapsed().as_secs_f64();
        if s > 0.0 {
            self.responses as f64 / s
        } else {
            0.0
        }
    }
    pub fn ms(&self, q: f64) -> f64 {
        self.hist.value_at_quantile(q) as f64 / 1e6
    }
}

impl Default for StreamStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Human report + the ONE machine-readable line. `extra` carries role-specific
/// fields (`conns`, `engines`, …) appended verbatim as `"k":v` pairs — `v`
/// must already be valid JSON (a number or a quoted string).
pub fn report(
    arm: &str,
    s: &StreamStats,
    secs: u64,
    payload: usize,
    inflight: u64,
    extra: &[(&str, String)],
) {
    // A RETRY'd seq is re-sent under the same seq and later answered (or
    // lost), so `retried` is a count of events, not of outstanding requests —
    // it must not be subtracted here.
    let inflight_at_end = s.sends.saturating_sub(s.responses + s.lost);
    println!("== {arm}: {secs} s, payload {payload} B, inflight {inflight}");
    println!(
        "   sends={} responses={} lost={} retried={} redirected={} inflight_at_end={} elapsed={:.3}s",
        s.sends,
        s.responses,
        s.lost,
        s.retried,
        s.redirected,
        inflight_at_end,
        s.elapsed().as_secs_f64()
    );
    println!(
        "   responses/s={:.1}  p50={:.3}ms p90={:.3}ms p95={:.3}ms p99={:.3}ms max={:.3}ms",
        s.responses_per_sec(),
        s.ms(0.50),
        s.ms(0.90),
        s.ms(0.95),
        s.ms(0.99),
        s.hist.max() as f64 / 1e6
    );
    let mut extra_json = String::new();
    for (k, v) in extra {
        extra_json.push_str(&format!(",\"{k}\":{v}"));
    }
    println!(
        "RESULT {{\"arm\":\"{arm}\",\"responses_per_sec\":{:.1},\"payload\":{payload},\
         \"inflight\":{inflight},\"secs\":{secs},\"sends\":{},\"responses\":{},\
         \"lost\":{},\"retried\":{},\"redirected\":{},\"inflight_at_end\":{},\"p50_ms\":{:.3},\
         \"p90_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},\"max_ms\":{:.3},\"elapsed_secs\":{:.3}{extra_json}}}",
        s.responses_per_sec(),
        s.sends,
        s.responses,
        s.lost,
        s.retried,
        s.redirected,
        inflight_at_end,
        s.ms(0.50),
        s.ms(0.90),
        s.ms(0.95),
        s.ms(0.99),
        s.hist.max() as f64 / 1e6,
        s.elapsed().as_secs_f64(),
    );
}
