// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared helpers for the uc2 fuzz targets. Nothing here is shipped.
//!
//! This crate lives OUTSIDE the root workspace on purpose — see
//! `fuzz/README.md` and the `exclude = ["fuzz"]` line in the root
//! `Cargo.toml`. It builds on nightly only.

pub mod seeds;

/// Split `data` into `n` slices at lengths taken from its own leading bytes
/// (so the fuzzer controls the split points). Always returns exactly `n`
/// slices; trailing ones may be empty.
pub fn split(data: &[u8], n: usize) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(n);
    let (lens, mut rest) = data.split_at(data.len().min(n.saturating_sub(1)));
    for &l in lens {
        let take = (l as usize).min(rest.len());
        let (a, b) = rest.split_at(take);
        out.push(a);
        rest = b;
    }
    while out.len() < n {
        out.push(rest);
        rest = &[];
    }
    out
}

/// A state machine that ignores its input — for fuzzing adapters that wrap
/// one (`Sessioned<S>`) without caring what `S` does.
pub struct NoopSm;

impl uc2_service::RawStateMachine for NoopSm {
    fn apply(&mut self, _position: u64, _cmd: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.clear();
    }
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

/// No-op snapshot capability, so `Sessioned<NoopSm>` (which forwards
/// `SnapshotStateMachine` from its inner SM) can itself be fuzzed through the
/// snapshot seam.
impl uc2_service::SnapshotStateMachine for NoopSm {
    type SnapshotHandle = ();

    fn freeze(&self) -> Result<((), u64), uc2_service::SnapshotError> {
        Ok(((), 0))
    }

    fn stream_snapshot(
        _handle: (),
        _dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut sink = std::io::sink();
        let _ = std::io::copy(src, &mut sink);
        Ok(position)
    }
}
