// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Sessioned<S>`: exactly-once over a remote hop, the Raft-paper
//! client-session model at the raw layer. Each command carries a 16-byte
//! envelope `client_id ++ seq`; a retry inside the per-client window replays
//! the cached response instead of re-applying. Deterministic by construction
//! (`BTreeMap`, position-based eviction) so every replica's table agrees, and
//! snapshot-composed so it survives restarts.
//!
//! **`SessionConfig` is part of the replicated contract.** `window`,
//! `max_clients`, and `max_bytes` all feed directly into the FRESH/REPLAYED/
//! EXPIRED classification and into which clients get evicted — so every
//! replica MUST run with identical `SessionConfig` values, and changing them
//! is a flag day (like changing `apply` itself), never a rolling per-node
//! tweak. `install_snapshot` enforces this: it refuses a snapshot whose
//! embedded config disagrees with the live node's, rather than silently
//! retuning (or silently diverging from) a running node.

use std::collections::{BTreeMap, VecDeque};

use crate::config::SnapshotError;
use crate::traits::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

pub const SESSION_HEADER_LEN: usize = 16;
pub const TAG_FRESH: u8 = 0;
pub const TAG_REPLAYED: u8 = 1;
pub const TAG_EXPIRED: u8 = 2;

/// Sanity bound on the dedup-table blob length prefix read by
/// `install_snapshot` — refuses to `Vec::with_capacity` an attacker- or
/// corruption-controlled size before the length has even been validated.
const MAX_TABLE_BLOB_LEN: u64 = 1 << 30;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Responses remembered per client (a retry older than this is `EXPIRED`).
    pub window: usize,
    /// Clients remembered; the client least recently seen (by log position) is evicted.
    pub max_clients: usize,
    /// Total cached-response bytes across all clients' windows. When
    /// exceeded, whole clients are evicted (oldest `last_seen_pos` first,
    /// same deterministic order as `max_clients` eviction) until back under
    /// budget — see [`Sessioned`]'s module doc. The real memory ceiling is
    /// `min(max_clients * window * avg_response_size, max_bytes)`; this is
    /// what actually bounds it for realistic response sizes at the default
    /// `max_clients`/`window` (which alone would be multi-GB).
    pub max_bytes: usize,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            window: 4096,
            max_clients: 65_536,
            max_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct ClientState {
    /// `None` = this client has never had a fresh frame applied. Must be an
    /// `Option`, not a bare `u64` with `0` as a "never seen" sentinel: a
    /// client's genuine first `seq` can legitimately be `0`, and a sentinel
    /// collision there would make its retry misclassify as FRESH again
    /// (silently re-applying a non-idempotent command — see
    /// `seq_zero_is_deduplicated` in `tests/session.rs`).
    highest_seq: Option<u64>,
    last_seen_pos: u64,
    /// (seq, response bytes) oldest-first, len <= window.
    window: VecDeque<(u64, Vec<u8>)>,
}

impl ClientState {
    fn window_bytes(&self) -> usize {
        self.window.iter().map(|(_, v)| v.len()).sum()
    }
}

/// Exactly-once wrapper. Wraps any [`RawStateMachine`] `S`; if `S` also
/// implements [`SnapshotStateMachine`] the dedup table rides along in the
/// snapshot image so a restore does not lose in-flight retries.
pub struct Sessioned<S> {
    inner: S,
    cfg: SessionConfig,
    clients: BTreeMap<u64, ClientState>,
    /// Sum of `window_bytes()` across all clients — kept incrementally so
    /// budget enforcement never needs a full-table scan on the hot path.
    total_bytes: usize,
    /// Highest position `apply`/`install_snapshot` has ever been called with,
    /// **including** frames that were `REPLAYED` or `EXPIRED` (the inner SM
    /// did not move for those). See `last_applied` below for why this — not
    /// `inner.last_applied()` alone — is what gets reported.
    max_pos_seen: Option<u64>,
}

impl<S: RawStateMachine> Sessioned<S> {
    pub fn new(inner: S, cfg: SessionConfig) -> Self {
        Self {
            inner,
            cfg,
            clients: BTreeMap::new(),
            total_bytes: 0,
            max_pos_seen: None,
        }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Deterministic victim order shared by both eviction paths: smallest
    /// `last_seen_pos`, ties broken by smallest `client_id`. Never wall-clock,
    /// never `HashMap` iteration order.
    fn oldest_client_excluding(&self, excl: Option<u64>) -> Option<u64> {
        self.clients
            .iter()
            .filter(|(id, _)| Some(**id) != excl)
            .min_by_key(|(id, c)| (c.last_seen_pos, **id))
            .map(|(id, _)| *id)
    }

    fn remove_client(&mut self, id: u64) {
        if let Some(st) = self.clients.remove(&id) {
            self.total_bytes -= st.window_bytes();
        }
    }

    fn evict_clients_over_capacity(&mut self) {
        while self.clients.len() > self.cfg.max_clients {
            match self.oldest_client_excluding(None) {
                Some(id) => self.remove_client(id),
                None => break,
            }
        }
    }

    /// Enforces `max_bytes` after a FRESH apply. `just_written` is never
    /// evicted as a *whole client* — it is the frame that just moved the
    /// budget over the line, and evicting it would mean the caller's own
    /// response vanishes before it can ever be replayed. If it ends up the
    /// only client left and the budget is still exceeded, its own window is
    /// trimmed from the front (oldest response first) until back under
    /// budget or empty — the general FIFO trim, just forced harder.
    fn evict_bytes_over_budget(&mut self, just_written: u64) {
        while self.total_bytes > self.cfg.max_bytes {
            match self.oldest_client_excluding(Some(just_written)) {
                Some(id) => self.remove_client(id),
                None => {
                    // Only `just_written` remains (or the table is empty).
                    let Some(st) = self.clients.get_mut(&just_written) else {
                        break;
                    };
                    match st.window.pop_front() {
                        Some((_, old)) => self.total_bytes -= old.len(),
                        None => break,
                    }
                }
            }
        }
    }
}

impl<S: RawStateMachine> RawStateMachine for Sessioned<S> {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        let position = ctx.position;
        // A malformed envelope (shorter than the fixed header) is treated as
        // unanswerable rather than panicking the apply thread — but the frame
        // still occupied a log position, so it still counts toward
        // `max_pos_seen`.
        self.max_pos_seen = Some(position);
        if cmd.len() < SESSION_HEADER_LEN {
            out.push(TAG_EXPIRED);
            return;
        }
        let client_id = u64::from_le_bytes(cmd[0..8].try_into().unwrap());
        let seq = u64::from_le_bytes(cmd[8..16].try_into().unwrap());
        let body = &cmd[SESSION_HEADER_LEN..];
        let window = self.cfg.window;

        let st = self.clients.entry(client_id).or_default();
        // Every frame for this client — fresh, replayed, or expired — moves
        // its last-seen position forward; eviction ranks purely on this.
        st.last_seen_pos = position;
        if let Some(highest) = st.highest_seq
            && seq <= highest
        {
            if let Some((_, cached)) = st.window.iter().find(|(s, _)| *s == seq) {
                out.push(TAG_REPLAYED);
                out.extend_from_slice(cached);
            } else {
                out.push(TAG_EXPIRED);
            }
            // The inner SM did NOT apply anything for this frame. That is
            // fine: `last_applied()` below does not delegate straight to
            // the inner SM, so a restart's replay will not re-derive an
            // inconsistent resume point. If the framework DOES replay
            // this frame again (e.g. after a crash before this call's
            // position was durably recorded elsewhere),
            // `Sessioned::apply` reaches the exact same branch and
            // produces the exact same tagged response — idempotent by
            // construction.
            return;
        }

        // Fresh: seq is new (including a gap — the Raft-paper session model
        // only rejects seqs at or below the highest seen, never gaps; and
        // `None` — no seq seen yet at all — always counts as fresh too).
        // The inner SM is given its OWN buffer, not `out` with the tag byte
        // already in it. `RawStateMachine::apply`'s contract says `out` is
        // "cleared by the caller", and `Sessioned` is a caller: handing the
        // inner a one-byte-long buffer and then recovering the response as
        // `out[start..]` silently required the inner to APPEND. An inner SM
        // that clears `out` first — which the contract invites — truncated
        // the tag away and panicked the apply thread on the slice
        // (M12d finding #1; regression test
        // `an_inner_sm_that_clears_out_does_not_panic_the_apply_thread`).
        //
        // Allocation-neutral, near enough: the old shape allocated once
        // anyway, in `to_vec()`, to put the response in the dedup window;
        // this allocates that same buffer up front and MOVES it into the
        // window instead of copying into it. The one thing that IS new is
        // that a fresh `Vec` starts empty and grows, where the old code
        // appended into `out`'s already-warm capacity — so the capacity is
        // seeded from this client's previous response, which makes the
        // steady state a single exact-size allocation for the overwhelmingly
        // common case of a client whose responses are all about one size.
        let hint = st.window.back().map_or(0, |(_, prev)| prev.len());
        let mut resp = Vec::with_capacity(hint);
        self.inner.apply(ctx, body, &mut resp);
        out.push(TAG_FRESH);
        out.extend_from_slice(&resp);
        let resp_len = resp.len();
        let st = self
            .clients
            .get_mut(&client_id)
            .expect("entry inserted above");
        st.highest_seq = Some(seq);
        st.window.push_back((seq, resp));
        self.total_bytes += resp_len;
        while st.window.len() > window {
            if let Some((_, old)) = st.window.pop_front() {
                self.total_bytes -= old.len();
            }
        }
        self.evict_clients_over_capacity();
        self.evict_bytes_over_budget(client_id);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.inner.query(q, out)
    }

    /// **Deliberately not just `self.inner.last_applied()`.** The inner SM
    /// only advances on `FRESH` frames; a `REPLAYED`/`EXPIRED` frame is a
    /// dedup-table-only operation. If this method reported the inner's
    /// value, the framework's idempotent-replay-from-`last_applied()` would
    /// re-feed every dedup-only frame back through `Sessioned::apply` on
    /// every restart — safe (this type is fully idempotent: replaying a
    /// `REPLAYED`/`EXPIRED` frame reaches the identical branch and produces
    /// the identical tagged output) but needlessly redoes work, and more
    /// importantly the exact resume point (`max_pos_seen`) is a MORE precise
    /// answer, not a less safe one: "under-reporting is safe, over-reporting
    /// above the frontier is refused" (the trait's own contract), and
    /// `max_pos_seen` never exceeds the true applied frontier because it is
    /// exactly the position of the last frame this method saw. The dedup
    /// table itself is always rebuildable by replay regardless of where
    /// resume starts, so nothing here weakens the exactly-once guarantee —
    /// it only changes how much redundant re-processing a restart does.
    fn last_applied(&self) -> Option<u64> {
        self.max_pos_seen.max(self.inner.last_applied())
    }
}

/// The wire image of the dedup table carried inside a `Sessioned` snapshot,
/// ahead of the inner SM's own snapshot bytes. `window`/`max_clients`/
/// `max_bytes` are carried so `install_snapshot` can enforce the replicated
/// `SessionConfig` invariant (see the module doc) — NOT so the live node
/// adopts them; a live node's own `cfg` always governs.
#[derive(serde::Serialize, serde::Deserialize)]
struct TableImage {
    window: usize,
    max_clients: usize,
    max_bytes: usize,
    clients: BTreeMap<u64, ClientState>,
}

impl<S: SnapshotStateMachine> SnapshotStateMachine for Sessioned<S> {
    type SnapshotHandle = (Vec<u8>, S::SnapshotHandle);

    /// Returns the **inner** SM's frozen position, which may be strictly
    /// below `Sessioned::last_applied()` when the most recent frames were
    /// `REPLAYED`/`EXPIRED` (they never move the inner SM). That is safe to
    /// round-trip: a non-FRESH frame only ever bumps `last_seen_pos` and
    /// never evicts anything, so re-feeding the trailing skew through
    /// `apply` again after `install_snapshot` exactly reproduces the
    /// pre-freeze table (see `freeze_with_trailing_replayed_frames_round_trips`).
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let (inner_handle, pos) = self.inner.freeze()?;
        let img = TableImage {
            window: self.cfg.window,
            max_clients: self.cfg.max_clients,
            max_bytes: self.cfg.max_bytes,
            clients: self.clients.clone(),
        };
        let blob = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(format!("session table encode: {e}")))?;
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
        if len > MAX_TABLE_BLOB_LEN {
            return Err(SnapshotError::Codec(format!(
                "session table blob length {len} exceeds the {MAX_TABLE_BLOB_LEN}-byte sanity bound"
            )));
        }
        // Grow to what the stream ACTUALLY supplies rather than pre-allocating
        // `len` bytes on the strength of an 8-byte prefix nothing has
        // validated yet. `MAX_TABLE_BLOB_LEN` above is still the hard ceiling;
        // `take` makes it a ceiling rather than an instruction, so a stream
        // that claims 1 GiB and supplies 40 bytes costs 40 bytes.
        let mut blob = Vec::new();
        src.take(len).read_to_end(&mut blob)?;
        if blob.len() as u64 != len {
            return Err(SnapshotError::Codec(format!(
                "session table blob truncated: header claims {len} bytes, stream supplied {}",
                blob.len()
            )));
        }
        let (img, _): (TableImage, _) =
            bincode::serde::decode_from_slice(&blob, bincode::config::standard())
                .map_err(|e| SnapshotError::Codec(format!("session table decode: {e}")))?;

        // `SessionConfig` is part of the replicated contract (module doc):
        // refuse a config mismatch BEFORE touching the inner SM, so a
        // misconfigured node fails closed rather than silently diverging.
        if img.window != self.cfg.window
            || img.max_clients != self.cfg.max_clients
            || img.max_bytes != self.cfg.max_bytes
        {
            return Err(SnapshotError::Codec(format!(
                "session config mismatch: snapshot {{window={}, max_clients={}, max_bytes={}}} vs live {{window={}, max_clients={}, max_bytes={}}}",
                img.window,
                img.max_clients,
                img.max_bytes,
                self.cfg.window,
                self.cfg.max_clients,
                self.cfg.max_bytes,
            )));
        }

        let got = self.inner.install_snapshot(position, src)?;
        self.total_bytes = img.clients.values().map(ClientState::window_bytes).sum();
        self.clients = img.clients;
        self.max_pos_seen = Some(got);
        Ok(got)
    }
}
