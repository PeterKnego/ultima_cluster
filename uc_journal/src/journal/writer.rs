// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::notifier::Signal;
use crate::{Durability, JournalError};

use super::segment::SegmentFile;

pub(crate) struct AppendRequest {
    pub seq: u64,
    pub meta: u64,
    pub payload: Vec<u8>,
    pub signal: Signal,
    /// `WriterState::truncate_gen` at submission. The writer persists this
    /// request only while `pending[seq]` still carries the same generation —
    /// a `truncate_after` in between bumps the generation, fencing off
    /// queued requests for truncated seqs (they would otherwise resurrect
    /// on disk, or shadow a re-appended entry at the same seq).
    pub generation: u64,
}

// ---------------------------------------------------------------------------
// SeqWatermark — fsync-durable sequence watermark (task28)
// ---------------------------------------------------------------------------

/// A one-shot durability callback, fired with `Ok(())` once the target seq is
/// fsynced, or `Err(_)` if a covering fsync failed / the journal closed first.
type DurabilityCallback = Box<dyn FnOnce(Result<(), JournalError>) + Send>;

/// Tracks the highest seq whose bytes are fsync-durable, and lets a caller wait
/// on (or be notified of) an arbitrary target seq.
///
/// The journal's fsync is a *barrier*: a single `sync_all()` makes every seq
/// written before it durable, not one specific append. So the watermark is
/// advanced to the high-water seq present in the file at fsync time (resolved
/// by the single-threaded writer in [`writer_loop`]), and a parked waiter for
/// seq `s` resolves on the first fsync whose high-water mark reaches `s`.
///
/// Strictly additive: it does not change the existing per-append [`Signal`]
/// semantics (which still fire after the buffered write in Eventual mode).
pub(crate) struct SeqWatermark {
    /// Highest seq known fsync-durable. Advances on publish; pulled back
    /// only by `reset_to` (truncate_after removing durable suffix entries).
    durable_seq: AtomicU64,
    /// Truncation generation (mirrors `WriterState::truncate_gen`). A
    /// publish carries the generation captured when its bytes were written;
    /// a stale-generation publish is dropped — its bytes were truncated
    /// away after the write, so they must not advance the watermark.
    generation: AtomicU64,
    /// Set when the writer thread is gone; releases parked waiters so they
    /// cannot block forever on a seq that will never be reached.
    closed: AtomicBool,
    inner: Mutex<Waiters>,
    condvar: Condvar,
}

#[derive(Default)]
struct Waiters {
    /// Parked callbacks: `(target_seq, callback)`, fired once the watermark
    /// reaches `target_seq` (or an error/close covers it).
    callbacks: Vec<(u64, DurabilityCallback)>,
    /// Sticky fsync error, recorded for the highest seq that failed. Waiters
    /// at/below this seq resolve to `Err`.
    last_error: Option<(u64, JournalError)>,
}

impl SeqWatermark {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            durable_seq: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            inner: Mutex::new(Waiters::default()),
            condvar: Condvar::new(),
        })
    }

    /// Highest seq known to be fsync-durable.
    pub(crate) fn current(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    /// Called by `truncate_after` (under the state lock): pull the watermark
    /// back to `keep_seq` — durable seqs past it no longer exist — and adopt
    /// the new truncation generation so an in-flight publish for bytes
    /// written before the truncate (now removed) is dropped as stale.
    pub(crate) fn reset_to(&self, keep_seq: u64, generation: u64) {
        let _w = self.inner.lock().unwrap();
        if self.durable_seq.load(Ordering::Acquire) > keep_seq {
            self.durable_seq.store(keep_seq, Ordering::Release);
        }
        self.generation.store(generation, Ordering::Release);
    }

    /// Called by the writer after a successful fsync. `seq` is the high-water
    /// mark of everything flushed by that fsync; `gen` is the truncation
    /// generation captured (under the state lock) when those bytes were
    /// written. A stale generation means a truncate removed them — skip.
    pub(crate) fn publish(&self, seq: u64, generation: u64) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            if generation != self.generation.load(Ordering::Acquire) {
                return;
            }
            let new = self.durable_seq.load(Ordering::Acquire).max(seq);
            // Store under the mutex so a concurrent wait/on_complete cannot read
            // the old value and then miss the notify (lost wakeup).
            self.durable_seq.store(new, Ordering::Release);
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, new)
        };
        for cb in ready {
            cb(Ok(()));
        }
    }

    /// Called by the writer when an fsync fails. Waiters at/below `seq` resolve
    /// to `Err`; the watermark is NOT advanced. Stale generations are dropped
    /// like in [`publish`].
    pub(crate) fn publish_error(&self, seq: u64, generation: u64, err: JournalError) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            if generation != self.generation.load(Ordering::Acquire) {
                return;
            }
            match &w.last_error {
                Some((es, _)) if *es >= seq => {}
                _ => w.last_error = Some((seq, clone_err(&err))),
            }
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, seq)
        };
        for cb in ready {
            cb(Err(clone_err(&err)));
        }
    }

    /// Release all parked waiters (the writer thread is gone).
    pub(crate) fn close(&self) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            self.condvar.notify_all();
            std::mem::take(&mut w.callbacks)
        };
        for (_, cb) in ready {
            cb(Err(JournalError::Closed));
        }
    }

    /// Block until `seq` is durable. Returns `Err` if a covering fsync failed
    /// or the journal closed first.
    pub(crate) fn wait(&self, seq: u64) -> Result<(), JournalError> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if self.durable_seq.load(Ordering::Acquire) >= seq {
                return Ok(());
            }
            if let Some((es, err)) = &guard.last_error
                && *es >= seq
            {
                return Err(clone_err(err));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(JournalError::Closed);
            }
            guard = self.condvar.wait(guard).unwrap();
        }
    }

    /// Register `cb` to fire once `seq` is durable. Fires inline (on the calling
    /// thread) if already durable, already errored, or already closed.
    pub(crate) fn on_complete(&self, seq: u64, cb: DurabilityCallback) {
        let mut w = self.inner.lock().unwrap();
        if self.durable_seq.load(Ordering::Acquire) >= seq {
            drop(w);
            cb(Ok(()));
            return;
        }
        if let Some((es, err)) = &w.last_error
            && *es >= seq
        {
            let err = clone_err(err);
            drop(w);
            cb(Err(err));
            return;
        }
        if self.closed.load(Ordering::Acquire) {
            drop(w);
            cb(Err(JournalError::Closed));
            return;
        }
        w.callbacks.push((seq, cb));
    }
}

/// Remove and return every callback whose target seq is `<= seq`. Order is
/// irrelevant (each callback is independent), so `swap_remove` is fine.
fn drain_le(callbacks: &mut Vec<(u64, DurabilityCallback)>, seq: u64) -> Vec<DurabilityCallback> {
    let mut ready = Vec::new();
    let mut i = 0;
    while i < callbacks.len() {
        if callbacks[i].0 <= seq {
            ready.push(callbacks.swap_remove(i).1);
        } else {
            i += 1;
        }
    }
    ready
}

pub(crate) struct WriterState {
    pub dir: PathBuf,
    /// Active only when `preallocate_segments` is set. Supplies preallocated
    /// temp segments for rotation so the zero-fill never hits the commit path.
    pub pipeline: Option<Arc<crate::journal::segment_pipeline::SegmentPipeline>>,
    pub segment_size: u64,
    pub durability: Durability,
    /// See `JournalConfig::eventual_fsync_interval` (Eventual mode only).
    pub eventual_fsync_interval: Duration,
    pub segments: Vec<SegmentFile>,
    pub last_seq: Option<u64>,
    pub first_seq: Option<u64>,
    /// Records that have been `append()`ed but not yet written to a segment by
    /// the bg writer. openraft's `RaftLogStorage::append()` contract requires an
    /// appended entry to be readable the instant `append()` returns — before the
    /// flush callback fires — so `append()` publishes here synchronously and
    /// `read`/`read_range` overlay it on the durable segments. The writer evicts
    /// each seq once it is persisted (both under this same lock, so a seq is in
    /// `pending` XOR a segment, never both and never neither).
    /// seq → (truncate_gen at append, meta, payload).
    pub pending: BTreeMap<u64, (u64, u64, Vec<u8>)>,
    /// Bumped by every `truncate_after`. Stamped onto pending entries and
    /// `AppendRequest`s; the writer only persists a request whose generation
    /// still matches its pending entry (see `AppendRequest::generation`).
    pub truncate_gen: u64,
    /// Highest seq actually written to a segment (0 = none). The Eventual
    /// idle fsync publishes this — NOT `last_seq`, which `append()` advances
    /// for entries still queued and unwritten (publishing those would
    /// over-report durability). Pulled back by `truncate_after`.
    pub persisted_hwm: u64,
    /// True when the ACTIVE (last) segment has bytes written since it was
    /// last fsynced. Set by `flush_run` whenever it actually writes;
    /// cleared by any fsync that covers the active segment
    /// (`fsync_active_segment`'s per-batch/per-idle-tick fsync, or
    /// `write_batch`'s roll-time fsync-on-seal). Lets the roll-time
    /// fsync-on-seal catch bytes left unfsynced by an EARLIER, separate
    /// batch — not just the CURRENT batch's own `run` — which is what
    /// closes the `Durability::Eventual` idle-timer gap (M11 roll-fsync
    /// investigation §1.5): under Eventual a segment can accumulate several
    /// batches' worth of unfsynced bytes (no per-batch fsync) before it
    /// finally rolls off.
    pub active_segment_dirty: bool,
    /// Set (under this lock) by the writer before it halts on an I/O error.
    /// `append`/`truncate_after` refuse with `Closed` once set — fail-stop:
    /// a failed fsync may have dropped dirty pages, so a later "successful"
    /// fsync must never be allowed to claim the lost bytes are durable.
    pub poisoned: bool,
    /// Test-only fault injection: the next segment fsync fails with an I/O
    /// error, exercising the writer's fail-stop path.
    #[cfg(test)]
    pub fail_next_fsync: bool,
    /// Test-only: records the path of every segment `sync_data()`'d by the
    /// fsync-on-seal call in `write_batch`'s roll path (M11 roll-fsync
    /// investigation fix) — lets a test assert precisely which segment was
    /// made durable at a roll, since a successful `sync_data()` has no
    /// other observable effect on file content.
    #[cfg(test)]
    pub seal_fsync_log: Vec<PathBuf>,
}

pub(crate) struct Writer {
    pub tx: Sender<AppendRequest>,
    pub handle: Option<JoinHandle<()>>,
}

impl Writer {
    pub fn spawn(state: Arc<Mutex<WriterState>>, watermark: Arc<SeqWatermark>) -> Self {
        let (tx, rx) = channel::<AppendRequest>();
        let handle = thread::spawn(move || writer_loop(rx, state, watermark));
        Self {
            tx,
            handle: Some(handle),
        }
    }
}

fn writer_loop(
    rx: Receiver<AppendRequest>,
    state: Arc<Mutex<WriterState>>,
    watermark: Arc<SeqWatermark>,
) {
    // Durability is fixed at Journal::open and never changes at runtime.
    let (durability, eventual_interval) = {
        let st = state.lock().unwrap();
        (st.durability, st.eventual_fsync_interval)
    };
    let mut last_eventual_fsync = Instant::now();

    loop {
        let timeout = match durability {
            Durability::Consistent => None,
            Durability::Eventual => {
                let elapsed = last_eventual_fsync.elapsed();
                if elapsed >= eventual_interval {
                    // Snapshot the persisted high-water seq + truncation
                    // generation, fsync, then publish — so the watermark only
                    // advances to seqs whose bytes the fsync definitely
                    // flushed (task28), and never to seqs that `append()`
                    // claimed but the writer hasn't written yet.
                    let (hwm, generation) = {
                        let st = state.lock().unwrap();
                        (st.persisted_hwm, st.truncate_gen)
                    };
                    match fsync_active_segment(&state) {
                        Ok(()) => {
                            if hwm > 0 {
                                watermark.publish(hwm, generation);
                            }
                        }
                        Err(e) => {
                            // Fail-stop (see halt_writer): a failed fsync may
                            // have dropped dirty pages; nothing after it may
                            // be reported durable.
                            if hwm > 0 {
                                watermark.publish_error(hwm, generation, e);
                            }
                            halt_writer(&state, &rx);
                            break;
                        }
                    }
                    last_eventual_fsync = Instant::now();
                }
                // `checked_sub` avoids a panic if the OS preempted us long
                // enough that elapsed > interval between the check above and
                // here. We just fall through with a zero timeout.
                Some(
                    eventual_interval
                        .checked_sub(last_eventual_fsync.elapsed())
                        .unwrap_or(Duration::ZERO),
                )
            }
        };
        let first = match timeout {
            None => match rx.recv() {
                Ok(r) => r,
                Err(_) => break,
            },
            Some(t) => match rx.recv_timeout(t) {
                Ok(r) => r,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            },
        };
        let mut batch = vec![first];
        // Drain anything already queued — this is the group-commit window.
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }
        // Write (no fsync). `write_batch` returns the high-water seq written
        // and the truncation generation captured while writing.
        let write_res = write_batch(&state, &batch);
        // Determine the result reported to per-append Signals, and advance the
        // durable_seq watermark. In Consistent mode the fsync happens here
        // (outside `write_batch`) so we can publish the watermark after it,
        // without holding the state lock during callback fan-out. In Eventual
        // mode the buffered write is signalled immediately and the watermark is
        // advanced later by the idle-timer fsync above.
        let result: Result<(), JournalError> = match durability {
            Durability::Consistent => match write_res {
                Ok((mut hwm, mut generation)) => {
                    // Re-drain window: appends that landed during `write_all`
                    // above missed this batch's `try_recv` drain. Fold them into
                    // the SAME fsync barrier so they don't each wait a full extra
                    // fsync cycle. Bounded (a runaway producer cannot starve the
                    // fsync) and bails the instant the channel is empty (an idle
                    // producer never delays the fsync). Each `write_batch` goes
                    // through the identical truncation/monotonic gating, so a
                    // re-drained request is fenced exactly as in the first pass.
                    const MAX_REDRAIN_ROUNDS: usize = 4;
                    let mut redrain_err: Option<JournalError> = None;
                    for _ in 0..MAX_REDRAIN_ROUNDS {
                        let mut extra = Vec::new();
                        while let Ok(req) = rx.try_recv() {
                            extra.push(req);
                        }
                        if extra.is_empty() {
                            break;
                        }
                        match write_batch(&state, &extra) {
                            // Fold the newcomers' high-water seq (take the max so
                            // a skipped/empty round can't pull it back) and carry
                            // the latest generation captured under the state lock.
                            Ok((extra_hwm, extra_gen)) => {
                                hwm = match (hwm, extra_hwm) {
                                    (Some(a), Some(b)) => Some(a.max(b)),
                                    (a, b) => a.or(b),
                                };
                                generation = extra_gen;
                                batch.append(&mut extra);
                            }
                            Err(e) => {
                                // A re-drain write failed: settle these newcomers
                                // with the same fail-stop error below, alongside
                                // the original batch (none were fsynced).
                                batch.append(&mut extra);
                                redrain_err = Some(e);
                                break;
                            }
                        }
                    }
                    match redrain_err {
                        Some(e) => Err(e),
                        // One fsync covers the original batch plus every
                        // re-drained request; publish the combined watermark once.
                        None => match fsync_active_segment(&state) {
                            Ok(()) => {
                                if let Some(s) = hwm {
                                    watermark.publish(s, generation);
                                }
                                Ok(())
                            }
                            Err(e) => {
                                if let Some(s) = hwm {
                                    watermark.publish_error(s, generation, clone_err(&e));
                                }
                                Err(e)
                            }
                        },
                    }
                }
                Err(e) => Err(e),
            },
            Durability::Eventual => write_res.map(|_| ()),
        };
        let failed = result.is_err();
        for req in batch {
            req.signal
                .complete(result.as_ref().map(|_| ()).map_err(clone_err));
        }
        if failed {
            // Fail-stop: a write or fsync error leaves the on-disk state
            // unknown. Halt instead of continuing — a later "successful"
            // fsync must never advance the watermark past the failure
            // (the failed fsync may have dropped the dirty pages).
            halt_writer(&state, &rx);
            break;
        }
    }
}

/// Poison the journal and drain queued requests before the writer exits on an
/// I/O error. The flag is set under the state lock — `append()` sends while
/// holding that lock, so every request is either drained here or refused with
/// `Closed` at `append()`; none can be left with a forever-pending Notifier.
fn halt_writer(state: &Arc<Mutex<WriterState>>, rx: &Receiver<AppendRequest>) {
    state.lock().unwrap().poisoned = true;
    while let Ok(req) = rx.try_recv() {
        req.signal.complete(Err(JournalError::Closed));
    }
}

fn write_batch(
    state: &Arc<Mutex<WriterState>>,
    batch: &[AppendRequest],
) -> Result<(Option<u64>, u64), JournalError> {
    let mut st = state.lock().unwrap();

    // Records accumulated for the current (last) segment, not yet written.
    let mut run: Vec<(u64, u64, &[u8])> = Vec::new();
    // Projected size of the current segment = on-disk size + bytes buffered in
    // `run`. Drives rotation exactly like the old per-record `seg.size()` check.
    let mut projected: u64 = match st.segments.last() {
        Some(seg) => seg.size()?,
        None => 0,
    };
    // Last seq written so far in THIS batch. `append()` now owns `st.last_seq`
    // (it advances it synchronously, possibly past this batch for entries not yet
    // persisted), so the writer can no longer use `st.last_seq` for its in-batch
    // monotonic guard — it tracks the locally-written high-water instead.
    let mut prev_written: Option<u64> = None;
    for req in batch {
        // Truncation fence: persist this request only if it is still the
        // current owner of its seq — i.e. `pending[seq]` carries the same
        // generation. A `truncate_after` since submission either removed the
        // seq from `pending` (truncated) or a re-append replaced it under a
        // newer generation; writing the stale request would resurrect a
        // truncated record or shadow its replacement. Skipped requests are
        // settled by the caller's signal fan-out (the append was accepted,
        // then explicitly truncated).
        let owns = st
            .pending
            .get(&req.seq)
            .is_some_and(|(generation, _, _)| *generation == req.generation);
        if !owns {
            continue;
        }
        // Monotonic-seq guard (redundant with append()'s enqueue check, kept as
        // defense; the channel is FIFO and append() validates global
        // monotonicity under the state lock, so this only ever guards against an
        // internal batching bug). On failure flush the good prefix first so
        // earlier records are persisted, matching the old per-record
        // write-then-error behavior. A flush error here supersedes the guard
        // error — acceptable, as the segment I/O failure is more fundamental.
        if let Some(last) = prev_written
            && req.seq <= last
        {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::NonMonotonicSeq {
                expected_gt: last,
                got: req.seq,
            });
        }
        let body_len = 16 + req.payload.len();
        let total = (4 + body_len + 4) as u64;
        if total > st.segment_size {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::PayloadTooLargeForSegment {
                segment_size: st.segment_size,
                record_size: total,
            });
        }

        // Rotate when there is no segment yet, or the current one is full.
        let need_new = st.segments.is_empty() || projected >= st.segment_size;
        if need_new {
            // Flush whatever was accumulated for the now-full segment first.
            flush_run(&mut st, &mut run)?;
            // Fsync-on-seal (M11 roll-fsync investigation,
            // roll-fsync-investigation.md): `flush_run` writes into k's page
            // cache but never fsyncs it — every later `fsync_active_segment`
            // targets `segments.last()` only, which becomes k+1 the moment
            // `st.segments.push(seg)` below runs. Without fsyncing k HERE,
            // while it is still `last()`, k's bytes are acked to their
            // callers on the strength of an fdatasync of a DIFFERENT file —
            // silent acked-write loss on power loss, until the kernel's own
            // writeback timer happens to flush k (unbounded, ~0-35s with
            // Linux defaults).
            //
            // `st.active_segment_dirty` (set by `flush_run` whenever it
            // actually writes, cleared by any fsync that covers the active
            // segment) tracks this, and NOT just this call's own `run` —
            // that distinction matters: under `Durability::Eventual` a
            // segment can pick up MULTIPLE records across SEPARATE prior
            // `write_batch` calls (each individually not triggering a roll)
            // with no fsync in between (Eventual only fsyncs on its own idle
            // timer, not per batch), so the batch that finally discovers
            // `need_new` can have an EMPTY `run` of its own while `k` still
            // holds real unfsynced bytes from an earlier batch. Gating only
            // on this call's `run` (the investigation's literal one-line
            // form) misses that inter-batch case; gating on the persistent
            // flag catches it too — while still costing nothing on the
            // current serial path, where every batch's own fsync (Consistent)
            // already clears the flag before the next batch ever runs.
            if st.active_segment_dirty {
                // `active_segment_dirty` can be stale-true with NO current
                // segment: `apply_truncate_to_segments`' None arm (keep_seq
                // below every base_seq) empties `st.segments` without
                // touching the flag, reachable via a live `truncate_after`/
                // `truncate_all` call between two batches — round-3 fix
                // review found this reachable under Eventual (nothing else
                // clears it) and even under Consistent (the window between
                // `write_batch` returning and the caller's
                // `fsync_active_segment` running). An `.expect` here used to
                // panic on that — killing the writer thread MID-BATCH while
                // holding this very lock, poisoning it, and hanging every
                // future `wait()` forever. `if let` degrades to "nothing to
                // seal, just clear the stale flag" instead — safe, since (b)
                // and (c) below (`truncate_after`/`truncate_all`,
                // `fsync_active_segment`) now also clear this flag whenever
                // they legitimately empty or otherwise cover `segments`, so
                // reaching here with `segments` empty should not happen in
                // practice either; this is the fail-safe, not the primary
                // fix.
                if let Some(sealed) = st.segments.last() {
                    // `write_batch` holds `st` for its whole body (unlike
                    // `fsync_active_segment`, which drops the lock before
                    // the syscall) — this fsync therefore briefly blocks
                    // other `append()` callers, but it fires at most once
                    // per `segment_size_bytes` of throughput (a roll),
                    // never per commit, matching the investigation's "at
                    // most one extra fdatasync per segment roll" cost
                    // accounting.
                    sealed.fsync_handle()?.sync_data().map_err(JournalError::Io)?;
                    #[cfg(test)]
                    {
                        let p = sealed.path().to_path_buf();
                        st.seal_fsync_log.push(p);
                    }
                }
                st.active_segment_dirty = false;
            }
            let final_path = st.dir.join(format!("seg-{:020}.log", req.seq));
            let seg = match st.pipeline.clone() {
                Some(pipe) => {
                    // Take a ready preallocated temp and activate it (header +
                    // atomic rename). The zero-fill already happened off the
                    // commit path on the pipeline thread.
                    let temp = pipe.take_ready()?;
                    SegmentFile::activate_prealloc_temp(&temp, &final_path, req.seq)?
                }
                None => SegmentFile::create(&final_path, req.seq)?,
            };
            st.segments.push(seg);
            projected = st.segments.last().unwrap().size()?;
        }

        run.push((req.seq, req.meta, &req.payload));
        projected += total;
        prev_written = Some(req.seq);
    }

    // Flush the final run for the active segment.
    flush_run(&mut st, &mut run)?;
    // The fsync (Consistent) is issued by the caller so the durable_seq watermark
    // can be published after it, outside this lock. Return the high-water seq
    // PERSISTED by this batch — NOT `st.last_seq`, which `append()` may have
    // advanced past this batch for entries not yet written; using it would
    // over-report durability. The truncation generation is captured under this
    // same lock so a truncate that lands after we release it invalidates the
    // eventual watermark publish for these bytes.
    Ok((prev_written, st.truncate_gen))
}

/// Write all accumulated records to the active segment with a single
/// coalesced `write_all`, then clear the run. No-op on an empty run.
fn flush_run(
    st: &mut WriterState,
    run: &mut Vec<(u64, u64, &[u8])>,
) -> Result<(), JournalError> {
    if run.is_empty() {
        return Ok(());
    }
    {
        let seg = st
            .segments
            .last_mut()
            .expect("flush_run called with a non-empty run but no active segment");
        seg.append_records(run.as_slice())?;
    }
    // The active segment now has bytes on disk that no fsync has covered
    // yet — see `active_segment_dirty`'s doc for why the roll-time
    // fsync-on-seal needs this instead of just checking THIS call's `run`.
    st.active_segment_dirty = true;
    // These records are now segment-readable, so evict them from the in-memory
    // overlay `append()` populated — under the same state lock, so a seq is in a
    // segment XOR `pending`, never double-counted by a concurrent read. Only
    // successfully-persisted records are evicted (on an `append_records` error we
    // returned above, leaving them visible-but-unpersisted, as the failed write
    // demands).
    for (seq, _, _) in run.iter() {
        st.pending.remove(seq);
    }
    if let Some((seq, _, _)) = run.last() {
        st.persisted_hwm = st.persisted_hwm.max(*seq);
    }
    run.clear();
    Ok(())
}

fn fsync_active_segment(state: &Arc<Mutex<WriterState>>) -> Result<(), JournalError> {
    // Grab a dup'd fd for the active segment under the lock, then DROP the lock
    // before issuing the fsync — so producers can keep enqueuing (and
    // `write_batch` can buffer the next group-commit window) while the fsync
    // syscall is in flight. The dup shares the same open file description, so it
    // is the same durability barrier over the contiguous written prefix.
    let f = {
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut st = state.lock().unwrap();
        #[cfg(test)]
        if st.fail_next_fsync {
            st.fail_next_fsync = false;
            return Err(JournalError::Io(std::io::Error::other(
                "injected fsync failure",
            )));
        }
        // Preserve the existing "no active segment is a no-op" behavior.
        st.segments.last().map(|s| s.fsync_handle()).transpose()?
    };
    if let Some(f) = f {
        // WAL commit primitive: fdatasync, not full fsync. sync_data flushes
        // the appended data plus the i_size growth needed to read it back,
        // and skips only inode timestamps (mtime/atime) — irrelevant to
        // durability. This preserves Durability::Consistent's power-loss
        // guarantee while dropping the per-commit timestamp write, lowering
        // the submitted->persisted P99 tail (task13 §15). Full sync_all is
        // retained on segment create (segment.rs) where the new directory
        // entry must be made durable.
        f.sync_data().map_err(JournalError::Io)?;
    }
    // Clear the dirty flag REGARDLESS of whether there was an active
    // segment to fsync — not just inside the `if let Some(f)` arm. If
    // `segments` is empty (e.g. a live `truncate_after`/`truncate_all` ran
    // between the write that set the flag and this call — see
    // `apply_truncate_to_segments`' None arm), there is nothing to fsync,
    // but the flag must still not be left stranded `true`: a stale `true`
    // with an empty `segments` is exactly the shape that used to reach
    // `write_batch`'s seal block and panic on a `.expect` there (round-3
    // fix). When there WAS an active segment, this fsync covers whatever
    // it currently is — the writer thread that could have written more to
    // it is the SAME thread executing this fsync, so nothing raced the
    // syscall — so clearing here is correct either way.
    state.lock().unwrap().active_segment_dirty = false;
    Ok(())
}

pub(crate) fn clone_err(e: &JournalError) -> JournalError {
    use JournalError::*;
    match e {
        Io(io) => Io(std::io::Error::new(io.kind(), io.to_string())),
        Corrupted {
            segment,
            offset,
            reason,
        } => Corrupted {
            segment: segment.clone(),
            offset: *offset,
            reason: reason.clone(),
        },
        NonMonotonicSeq { expected_gt, got } => NonMonotonicSeq {
            expected_gt: *expected_gt,
            got: *got,
        },
        SeqOutOfRange => SeqOutOfRange,
        PayloadTooLargeForSegment {
            segment_size,
            record_size,
        } => PayloadTooLargeForSegment {
            segment_size: *segment_size,
            record_size: *record_size,
        },
        Closed => Closed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed fsync poisons waiters at/below the attempted high-water seq
    /// without advancing the watermark; a later success still advances.
    #[test]
    fn publish_error_poisons_without_advancing() {
        let wm = SeqWatermark::new();
        wm.publish_error(5, 0, JournalError::Closed);
        assert_eq!(wm.current(), 0, "watermark must not advance on error");
        assert!(matches!(wm.wait(3), Err(JournalError::Closed)));

        // A later successful fsync advances and resolves higher seqs.
        wm.publish(6, 0);
        assert_eq!(wm.current(), 6);
        wm.wait(6).unwrap();
        // The already-resolved low seq is durable now too (6 >= 3).
        wm.wait(3).unwrap();
    }

    /// `publish` only ever advances the watermark (monotonic), even if called
    /// with a lower seq after a higher one.
    #[test]
    fn publish_is_monotonic() {
        let wm = SeqWatermark::new();
        wm.publish(10, 0);
        wm.publish(4, 0);
        assert_eq!(wm.current(), 10);
    }

    /// A blocked `wait` unblocks with `Err(Closed)` when the watermark closes.
    #[test]
    fn wait_unblocks_on_close() {
        let wm = SeqWatermark::new();
        let wm2 = Arc::clone(&wm);
        let waiter = thread::spawn(move || wm2.wait(999));
        thread::sleep(Duration::from_millis(50));
        wm.close();
        assert!(matches!(waiter.join().unwrap(), Err(JournalError::Closed)));
    }

    // -----------------------------------------------------------------
    // M11 roll-fsync fix (Task 2c): fsync-on-seal regression tests.
    //
    // These drive `write_batch` DIRECTLY rather than going through a real
    // `Journal` + background writer thread, and rather than racing the
    // channel-drain timing that produces a real straddling batch. The
    // roll-fsync investigation's own empirical probe (§3) found the
    // straddle rate under real thread scheduling to be inherently
    // non-deterministic (0-2 straddling rolls per ~120-record trial,
    // varying by run) — a test relying on that race would be a FLAKY
    // regression guard for exactly the bug being pinned here. Calling
    // `write_batch` directly with a hand-built batch exercises the exact
    // same production code path `writer_loop` calls, deterministically,
    // every time.
    // -----------------------------------------------------------------

    fn make_writer_state(dir: &std::path::Path, segment_size: u64) -> Arc<Mutex<WriterState>> {
        Arc::new(Mutex::new(WriterState {
            dir: dir.to_path_buf(),
            pipeline: None,
            segment_size,
            durability: Durability::Consistent,
            eventual_fsync_interval: Duration::from_millis(1),
            segments: Vec::new(),
            last_seq: None,
            first_seq: None,
            pending: BTreeMap::new(),
            truncate_gen: 0,
            persisted_hwm: 0,
            active_segment_dirty: false,
            poisoned: false,
            #[cfg(test)]
            fail_next_fsync: false,
            #[cfg(test)]
            seal_fsync_log: Vec::new(),
        }))
    }

    /// Build a batch of `AppendRequest`s AND seed `state.pending` with a
    /// matching entry for each — `write_batch`'s "ownership" fence
    /// (`writer.rs` ~487: ``owns = st.pending.get(&req.seq)...``) silently
    /// `continue`s past any request whose seq isn't present in `pending`
    /// with the same generation, exactly as it must for a real
    /// `truncate_after` race. In production `Journal::append()` populates
    /// `pending` synchronously before the request ever reaches the writer
    /// channel; a direct `write_batch` test has to replicate that.
    fn make_batch(
        state: &Arc<Mutex<WriterState>>,
        records: &[(u64, u64, &[u8])],
    ) -> Vec<AppendRequest> {
        let mut st = state.lock().unwrap();
        records
            .iter()
            .map(|&(seq, meta, payload)| {
                st.pending.insert(seq, (0, meta, payload.to_vec()));
                let (signal, _notifier) = crate::notifier::Notifier::pending();
                AppendRequest {
                    seq,
                    meta,
                    payload: payload.to_vec(),
                    signal,
                    generation: 0,
                }
            })
            .collect()
    }

    /// The core regression: a batch whose records straddle a segment roll
    /// (some land in the outgoing segment *k* via the `need_new`-triggered
    /// flush, before *k+1* is created) must fsync *k* right there — the
    /// caller's single post-batch fsync only ever targets `segments.last()`,
    /// which becomes *k+1* the instant this call returns. Three records,
    /// 30-byte payloads (54 bytes on disk each) into a 100-byte segment:
    /// record 1 creates segment 0 (32-byte header + 54 = 86 bytes,
    /// projected 86 < 100 → record 2 also lands in `run`, projected 86+54=
    /// 140 → record 3's `need_new` check (140 >= 100) fires WITH `run`
    /// still holding records 1 and 2 — the exact straddle shape.
    #[test]
    fn write_batch_fsyncs_the_outgoing_segment_when_a_batch_straddles_a_roll() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_writer_state(dir.path(), 100);
        let batch = make_batch(
            &state,
            &[(1, 0, &[0u8; 30]), (2, 0, &[0u8; 30]), (3, 0, &[0u8; 30])],
        );
        write_batch(&state, &batch).unwrap();

        let st = state.lock().unwrap();
        assert_eq!(
            st.segments.len(),
            2,
            "the batch must have rolled to a 2nd segment"
        );
        let seg0_path = st.segments[0].path().to_path_buf();
        assert!(
            st.seal_fsync_log.contains(&seg0_path),
            "the outgoing segment (holding records 1 and 2) must be fsynced \
             at the roll, not left for a later fsync that will only ever \
             target the NEW active segment"
        );
    }

    /// A batch whose FIRST request alone triggers `need_new` (an empty
    /// `run`, nothing new for THIS batch to seal into the outgoing segment)
    /// must not fsync anything on the seal path — `k` was already fully
    /// durable from whatever wrote its last record, and firing an extra
    /// fsync here would be the redundant per-boot/per-batch cost the fix is
    /// supposed to avoid on the ordinary serial path.
    #[test]
    fn write_batch_does_not_fsync_on_seal_when_the_run_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_writer_state(dir.path(), 100);
        // record1 alone (54 bytes on disk) fits within segment_size (100) —
        // need_new fires only once, on the FIRST request ever
        // (segments.is_empty()), which is unconditionally exempt (no prior
        // segment to seal); record1 itself is written via the batch's own
        // trailing flush (line ~539), not a need_new-triggered one.
        let batch = make_batch(&state, &[(1, 0, &[0u8; 30])]);
        write_batch(&state, &batch).unwrap();
        assert!(
            state.lock().unwrap().seal_fsync_log.is_empty(),
            "no rotation occurred yet — nothing should be logged by the seal path"
        );
    }

    /// The `Durability::Eventual` variant (M11 roll-fsync investigation
    /// §1.5): under Eventual, `write_batch` never fsyncs on its own — the
    /// idle timer does, on its own schedule, independent of batch/roll
    /// boundaries. So a segment can accumulate bytes across MULTIPLE
    /// separate `write_batch` calls (none of which individually straddle a
    /// roll — each one's own `run` is non-empty only for records that fit,
    /// same as the ordinary non-rotating case) before a LATER, separate
    /// batch's `need_new` finally fires with an EMPTY `run` of its own. A
    /// fix gated only on "this call's run was non-empty" (the
    /// investigation's literal one-liner) would miss this: three separate
    /// `write_batch` calls, records 1 and 2 landing in segment 0 across the
    /// first two (segment 0 not yet full after either), record 3 in the
    /// third finally discovering `projected >= segment_size` — segment 0
    /// must still be fsynced at that roll, from the PERSISTENT
    /// `active_segment_dirty` flag, not the batch-local `run`. This
    /// directly demonstrates the investigation's closure argument
    /// (segment 0 is durable at roll time, before the Eventual idle timer
    /// — which targets `segments.last()` only and would from here on target
    /// segment 1 — ever needs to run) rather than merely asserting it in
    /// prose.
    #[test]
    fn write_batch_closes_the_eventual_inter_batch_gap_via_the_persistent_dirty_flag() {
        let dir = tempfile::tempdir().unwrap();
        let state = make_writer_state(dir.path(), 100);
        state.lock().unwrap().durability = Durability::Eventual;

        // Batch 1: record 1 alone creates segment 0 (86 bytes on disk), no
        // rotation, no fsync of any kind under Eventual.
        let batch1 = make_batch(&state, &[(1, 0, &[0u8; 30])]);
        write_batch(&state, &batch1).unwrap();
        assert!(
            state.lock().unwrap().seal_fsync_log.is_empty(),
            "no rotation yet"
        );

        // Batch 2: record 2 alone. `need_new` compares against segment 0's
        // size BEFORE this record (86 < 100) — still no rotation — but the
        // write pushes segment 0 to 140 bytes, discovered only by the NEXT
        // batch. Still no fsync of any kind under Eventual: segment 0 now
        // holds two unfsynced records from two SEPARATE prior batches.
        let batch2 = make_batch(&state, &[(2, 0, &[0u8; 30])]);
        write_batch(&state, &batch2).unwrap();
        assert!(
            state.lock().unwrap().seal_fsync_log.is_empty(),
            "still no rotation — segment 0 is dirty but not yet rolled off"
        );

        // Batch 3: record 3's `need_new` check (140 >= 100) finally fires,
        // with an EMPTY run of its OWN — the bytes that need fsyncing came
        // from batches 1 and 2, not this one.
        let batch3 = make_batch(&state, &[(3, 0, &[0u8; 30])]);
        write_batch(&state, &batch3).unwrap();

        let st = state.lock().unwrap();
        assert_eq!(st.segments.len(), 2, "batch 3 must have rolled");
        let seg0_path = st.segments[0].path().to_path_buf();
        assert!(
            st.seal_fsync_log.contains(&seg0_path),
            "segment 0 (records 1+2, written across TWO earlier batches) \
             must be fsynced at the roll — the Eventual idle timer that \
             would otherwise cover it may not fire for up to \
             eventual_fsync_interval, or ever, before the next roll"
        );
    }
}
