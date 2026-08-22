// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Sessioned<S>`: exactly-once over a remote hop, the Raft-paper
//! client-session model at the raw layer. Each command carries a 16-byte
//! envelope `client_id ++ seq`; a retry inside the per-client window replays
//! the cached response instead of re-applying. Deterministic by construction
//! (`BTreeMap`, position-based eviction) so every replica's table agrees, and
//! snapshot-composed so it survives restarts.

use std::collections::{BTreeMap, VecDeque};

use crate::config::SnapshotError;
use crate::traits::{RawStateMachine, SnapshotStateMachine};

pub const SESSION_HEADER_LEN: usize = 16;
pub const TAG_FRESH: u8 = 0;
pub const TAG_REPLAYED: u8 = 1;
pub const TAG_EXPIRED: u8 = 2;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Responses remembered per client (a retry older than this is `EXPIRED`).
    pub window: usize,
    /// Clients remembered; the client least recently seen (by log position) is evicted.
    pub max_clients: usize,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self { window: 4096, max_clients: 65_536 }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct ClientState {
    highest_seq: u64,
    last_seen_pos: u64,
    /// (seq, response bytes) oldest-first, len <= window.
    window: VecDeque<(u64, Vec<u8>)>,
}

/// Exactly-once wrapper. Wraps any [`RawStateMachine`] `S`; if `S` also
/// implements [`SnapshotStateMachine`] the dedup table rides along in the
/// snapshot image so a restore does not lose in-flight retries.
pub struct Sessioned<S> {
    inner: S,
    cfg: SessionConfig,
    clients: BTreeMap<u64, ClientState>,
    /// Highest position `apply`/`install_snapshot` has ever been called with,
    /// **including** frames that were `REPLAYED` or `EXPIRED` (the inner SM
    /// did not move for those). See `last_applied` below for why this — not
    /// `inner.last_applied()` alone — is what gets reported.
    max_pos_seen: Option<u64>,
}

impl<S: RawStateMachine> Sessioned<S> {
    pub fn new(inner: S, cfg: SessionConfig) -> Self {
        Self { inner, cfg, clients: BTreeMap::new(), max_pos_seen: None }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    fn evict_if_needed(&mut self) {
        while self.clients.len() > self.cfg.max_clients {
            // Deterministic: oldest last_seen_pos, ties by smallest client_id
            // (BTreeMap iteration order — never HashMap, never wall-clock).
            let victim = self
                .clients
                .iter()
                .min_by_key(|(id, c)| (c.last_seen_pos, **id))
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    self.clients.remove(&id);
                }
                None => break,
            }
        }
    }
}

impl<S: RawStateMachine> RawStateMachine for Sessioned<S> {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
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
        if st.highest_seq != 0 && seq <= st.highest_seq {
            if let Some((_, cached)) = st.window.iter().find(|(s, _)| *s == seq) {
                out.push(TAG_REPLAYED);
                out.extend_from_slice(cached);
            } else {
                out.push(TAG_EXPIRED);
            }
            // The inner SM did NOT apply anything for this frame. That is
            // fine: `last_applied()` below does not delegate straight to the
            // inner SM, so a restart's replay will not re-derive an
            // inconsistent resume point. If the framework DOES replay this
            // frame again (e.g. after a crash before this call's position
            // was durably recorded elsewhere), `Sessioned::apply` reaches the
            // exact same branch and produces the exact same tagged response —
            // idempotent by construction.
            return;
        }

        // Fresh: seq is new (including a gap — the Raft-paper session model
        // only rejects seqs at or below the highest seen, never gaps).
        out.push(TAG_FRESH);
        let start = out.len();
        self.inner.apply(position, body, out);
        let resp = out[start..].to_vec();
        let st = self.clients.get_mut(&client_id).expect("entry inserted above");
        st.highest_seq = seq;
        st.window.push_back((seq, resp));
        while st.window.len() > window {
            st.window.pop_front();
        }
        self.evict_if_needed();
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
/// ahead of the inner SM's own snapshot bytes.
#[derive(serde::Serialize, serde::Deserialize)]
struct TableImage {
    window: usize,
    max_clients: usize,
    clients: BTreeMap<u64, ClientState>,
}

impl<S: SnapshotStateMachine> SnapshotStateMachine for Sessioned<S> {
    type SnapshotHandle = (Vec<u8>, S::SnapshotHandle);

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError> {
        let (inner_handle, pos) = self.inner.freeze()?;
        let img = TableImage {
            window: self.cfg.window,
            max_clients: self.cfg.max_clients,
            clients: self.clients.clone(),
        };
        let blob = bincode::serde::encode_to_vec(&img, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(format!("session table encode: {e}")))?;
        Ok(((blob, inner_handle), pos))
    }

    fn stream_snapshot(handle: Self::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        let (blob, inner) = handle;
        dst.write_all(&(blob.len() as u64).to_le_bytes())?;
        dst.write_all(&blob)?;
        S::stream_snapshot(inner, dst)
    }

    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> {
        let mut len = [0u8; 8];
        src.read_exact(&mut len)?;
        let mut blob = vec![0u8; u64::from_le_bytes(len) as usize];
        src.read_exact(&mut blob)?;
        let (img, _): (TableImage, _) = bincode::serde::decode_from_slice(&blob, bincode::config::standard())
            .map_err(|e| SnapshotError::Codec(format!("session table decode: {e}")))?;
        let got = self.inner.install_snapshot(position, src)?;
        // `img.window`/`img.max_clients` are carried for forward-diagnostic
        // value only: this node's own `cfg` (not the snapshot's) governs
        // future eviction/window behavior, so a snapshot taken under a
        // different tuning does not silently retune a live node.
        let _ = (img.window, img.max_clients);
        self.clients = img.clients;
        self.max_pos_seen = Some(got);
        Ok(got)
    }
}
