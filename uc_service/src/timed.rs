// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Timed<S>`: exactly-once timer delivery (time-and-timers spec §4.6).
//!
//! The node fires timers **at least once** (it re-arms in-flight instances on
//! leadership loss). This wrapper keeps the pending set the inner state
//! machine asked for — rebuilt from the log on replay, carried in the
//! snapshot — and delivers a `TIMER` frame to the inner `on_timer` only if
//! its `(id, deadline)` is still pending. Every replica decides identically
//! because the decision reads nothing but committed frames.

use std::collections::BTreeMap;

use crate::config::SnapshotError;
use crate::traits::{ApplyCtx, RawStateMachine, SnapshotStateMachine, TimerEvent, TimerReq};

const MAX_IMAGE_LEN: u64 = 1 << 26;

pub struct Timed<S> {
    inner: S,
    pending: BTreeMap<u64, u64>,
    /// Plan 2: last delivered deadline per table id. Carried in the image now
    /// so the snapshot format does not change when the table lands.
    table_last: BTreeMap<u64, u64>,
    max_pos_seen: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TimedImage {
    pending: BTreeMap<u64, u64>,
    table_last: BTreeMap<u64, u64>,
}

impl<S> Timed<S> {
    pub fn new(inner: S) -> Timed<S> {
        Timed {
            inner,
            pending: BTreeMap::new(),
            table_last: BTreeMap::new(),
            max_pos_seen: None,
        }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    pub fn pending(&self) -> Vec<(u64, u64)> {
        self.pending.iter().map(|(&i, &d)| (i, d)).collect()
    }
    fn absorb(&mut self, reqs: &[TimerReq]) {
        for r in reqs {
            match *r {
                TimerReq::Schedule { id, at_ns } => {
                    self.pending.insert(id, at_ns);
                }
                TimerReq::Cancel { id } => {
                    self.pending.remove(&id);
                }
            }
        }
    }
}

impl<S: RawStateMachine> RawStateMachine for Timed<S> {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.max_pos_seen = Some(ctx.position).max(self.max_pos_seen);
        let before = ctx.timers().len();
        self.inner.apply(ctx, cmd, out);
        let reqs: Vec<TimerReq> = ctx.timers()[before..].to_vec();
        self.absorb(&reqs);
    }

    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.max_pos_seen = Some(ctx.position).max(self.max_pos_seen);
        let deliver = if ev.table {
            self.table_last
                .get(&ev.id)
                .is_none_or(|&last| ev.deadline_ns > last)
        } else {
            self.pending.get(&ev.id) == Some(&ev.deadline_ns)
        };
        if deliver {
            if ev.table {
                self.table_last.insert(ev.id, ev.deadline_ns);
            } else {
                self.pending.remove(&ev.id);
            }
            let before = ctx.timers().len();
            self.inner.on_timer(ctx, ev);
            let reqs: Vec<TimerReq> = ctx.timers()[before..].to_vec();
            self.absorb(&reqs);
        }
        ctx.consumed(ev.id, ev.deadline_ns);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.inner.query(q, out)
    }

    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied().max(self.max_pos_seen)
    }

    fn pending_timers(&self) -> Vec<(u64, u64)> {
        self.pending()
    }
}

impl<S: SnapshotStateMachine> SnapshotStateMachine for Timed<S> {
    type SnapshotHandle = (Vec<u8>, S::SnapshotHandle);

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let (inner_handle, pos) = self.inner.freeze()?;
        let img = TimedImage {
            pending: self.pending.clone(),
            table_last: self.table_last.clone(),
        };
        let blob = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(format!("timed image encode: {e}")))?;
        Ok(((blob, inner_handle), pos))
    }

    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), SnapshotError> {
        let (blob, inner) = handle;
        dst.write_all(&(blob.len() as u64).to_le_bytes())?;
        dst.write_all(&blob)?;
        S::stream_snapshot(inner, dst)
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        use std::io::Read as _;
        let mut len_buf = [0u8; 8];
        src.read_exact(&mut len_buf)?;
        let len = u64::from_le_bytes(len_buf);
        if len > MAX_IMAGE_LEN {
            return Err(SnapshotError::Codec(format!(
                "timed image blob length {len} exceeds the {MAX_IMAGE_LEN}-byte sanity bound"
            )));
        }
        // Grow to what the stream ACTUALLY supplies rather than pre-allocating
        // `len` bytes on the strength of an 8-byte prefix nothing has
        // validated yet. `MAX_IMAGE_LEN` above is still the hard ceiling;
        // `take` makes it a ceiling rather than an instruction, so a stream
        // that claims a large size and supplies few bytes costs few bytes.
        let mut blob = Vec::new();
        src.take(len).read_to_end(&mut blob)?;
        if blob.len() as u64 != len {
            return Err(SnapshotError::Codec(format!(
                "timed image blob truncated: header claims {len} bytes, stream supplied {}",
                blob.len()
            )));
        }
        let (img, _): (TimedImage, _) =
            bincode::serde::decode_from_slice(&blob, bincode::config::standard())
                .map_err(|e| SnapshotError::Codec(format!("timed image decode: {e}")))?;

        let got = self.inner.install_snapshot(position, src)?;
        self.pending = img.pending;
        self.table_last = img.table_last;
        self.max_pos_seen = Some(got);
        Ok(got)
    }
}
