// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Conformance-vector generator for the Lean model (spec
//! `docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md` §5).
//! Emits JSONL `{fn, ..., expect}` where `expect` is THIS implementation's
//! output; `proofs/Conform/Main.lean` replays each line through `Uc2Model`
//! and fails on any divergence. Zero deps: hand-rolled splitmix64 PRNG,
//! hand-rolled JSON (all values are numbers/arrays/bools).
//!
//! Usage: cargo run -p uc_consensus --release --example conform_gen -- \
//!            --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 20260716

use std::fmt::Write as _;
use std::io::Write as _;

use uc_consensus::commit::CommitTracker;
use uc_consensus::election::log_ok_order;
use uc_consensus::reconcile::{MAX_TERM_MAP_WIRE_ENTRIES, Reconcile, reconcile};

/// splitmix64: deterministic, platform-independent, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn json_map(m: &[(u32, u64)]) -> String {
    let mut s = String::from("[");
    for (i, (t, b)) in m.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "[{t},{b}]");
    }
    s.push(']');
    s
}

/// Random ascending map of exactly `n` entries: terms strictly ascending,
/// bases non-strictly.
fn gen_map_len(rng: &mut Rng, n: usize) -> Vec<(u32, u64)> {
    let mut term = 0u32;
    let mut base = 0u64;
    let mut m = Vec::with_capacity(n);
    for _ in 0..n {
        term += 1 + rng.below(3) as u32;
        // occasionally a zero-byte frontier entry (same base as predecessor)
        if rng.below(5) != 0 {
            base += rng.below(1 << 20);
        }
        m.push((term, base));
    }
    m
}

/// Random ascending map of up to `max_entries` entries (see [`gen_map_len`]).
fn gen_map(rng: &mut Rng, max_entries: u64) -> Vec<(u32, u64)> {
    let n = rng.below(max_entries + 1) as usize;
    gen_map_len(rng, n)
}

/// Push `n` fresh entries continuing `m`'s ascending order (a divergent tail
/// one side holds and the other does not).
fn push_tail(rng: &mut Rng, m: &mut Vec<(u32, u64)>, n: u64) {
    for _ in 0..n {
        let (lt, lb) = m.last().copied().unwrap_or((0, 0));
        m.push((lt + 1 + rng.below(2) as u32, lb + rng.below(1 << 19)));
    }
}

fn emit_reconcile(rng: &mut Rng, out: &mut impl std::io::Write) {
    // Correlated pair: leader = base map; own = shared prefix + own suffix,
    // so common prefixes, conflicts, overhangs, and phantom entries all occur.
    // `leader[0]` is always `own[0]` (or `own` is empty) here — the window-
    // aligned `j > 0` branch is `emit_reconcile_windowed`'s job.
    let leader = gen_map(rng, 8);
    let keep = rng.below(leader.len() as u64 + 1) as usize;
    let mut own: Vec<(u32, u64)> = leader[..keep].to_vec();
    let own_tail = rng.below(3);
    push_tail(rng, &mut own, own_tail);
    let own_durable = own.last().map(|&(_, b)| b).unwrap_or(0) + rng.below(1 << 19);
    write_reconcile(out, &own, own_durable, &leader);
}

/// The 2026-08-16 window-alignment regression shape (`reconcile.rs`'s
/// `windowed_*` tests): the cluster's LIFETIME term count exceeds
/// `MAX_TERM_MAP_WIRE_ENTRIES`, so the leader ships only the last 64 entries
/// of its map and `leader[0]` is our entry `j > 0` — or, when our own map
/// stopped short of the window, not our entry at all. Without this arm the
/// rig never produced a single `j > 0` vector (measured 2026-08-29: 0 of
/// 33 334 reconcile vectors under seed 1), so the branch the acked-write-
/// loss fix added was replayed against the model exactly never.
///
/// Arms, on top of a shared history of 65..=96 entries (so the window's
/// start is always strictly inside it, `j >= 1`):
///   - healthy follower (2/3): `own` = the shared history, the leader's
///     window = its tail — each side optionally grows a divergent tail, so
///     the aligned run is clean, or cut at the first mismatch by the own-
///     side or leader-side clamp;
///   - below-window follower (1/3): `own` = `shared[..keep]` with `keep`
///     just below `j`, so `leader[0] ∉ own` — `NoCommonPrefix` when the
///     window opens beyond our bytes, else the cut-at-window-start path,
///     where a divergent own tail reaching the window's first term also
///     fires the same-term/different-base clamp.
///
/// `own_durable` overshoots the last base by up to 2 MiB so it lands on
/// either side of the window's first base.
fn emit_reconcile_windowed(rng: &mut Rng, out: &mut impl std::io::Write) {
    let shared_len = MAX_TERM_MAP_WIRE_ENTRIES + 1 + rng.below(32) as usize;
    let shared = gen_map_len(rng, shared_len);
    let mut leader_full = shared.clone();
    let leader_tail = rng.below(3);
    push_tail(rng, &mut leader_full, leader_tail);
    let j = leader_full.len() - MAX_TERM_MAP_WIRE_ENTRIES;
    debug_assert!(j >= 1 && j < shared.len());
    let leader: Vec<(u32, u64)> = leader_full[j..].to_vec();
    let mut own: Vec<(u32, u64)> = if rng.below(3) != 0 {
        shared.clone()
    } else {
        let keep = j.saturating_sub(rng.below(4) as usize);
        shared[..keep].to_vec()
    };
    let own_tail = rng.below(4);
    push_tail(rng, &mut own, own_tail);
    let own_durable = own.last().map(|&(_, b)| b).unwrap_or(0) + rng.below(1 << 21);
    write_reconcile(out, &own, own_durable, &leader);
}

fn write_reconcile(
    out: &mut impl std::io::Write,
    own: &[(u32, u64)],
    own_durable: u64,
    leader: &[(u32, u64)],
) {
    let expect = match reconcile(own, own_durable, leader) {
        Reconcile::Ok(o) => format!(
            r#"{{"kind":"ok","valid_up_to":{},"new_map":{}}}"#,
            o.valid_up_to,
            json_map(&o.new_map)
        ),
        Reconcile::NoCommonPrefix => r#"{"kind":"no_common_prefix"}"#.into(),
    };
    writeln!(
        out,
        r#"{{"fn":"reconcile","own":{},"own_durable":{},"leader":{},"expect":{}}}"#,
        json_map(own),
        own_durable,
        json_map(leader),
        expect
    )
    .unwrap();
}

fn emit_advance_fold(rng: &mut Rng, out: &mut impl std::io::Write) {
    // Valid config only (the Rust constructor asserts):
    // cluster > followers AND followers + 1 > cluster/2.
    let (n_followers, cluster_size) = loop {
        let c = 2 + rng.below(6) as usize; // 2..=7
        let f = 1 + rng.below(c as u64 - 1) as usize; // 1..c
        if c > f && f + 1 > c / 2 {
            break (f, c);
        }
    };
    let mut t = CommitTracker::new(n_followers, cluster_size);
    let n_ev = 1 + rng.below(48);
    let mut evs = String::from("[");
    let mut own = 0u64;
    for i in 0..n_ev {
        if i > 0 {
            evs.push(',');
        }
        match rng.below(5) {
            0 => {
                t.reset_reports();
                evs.push_str(r#"["reset"]"#);
            }
            1 | 2 => {
                own += rng.below(1 << 18);
                t.advance(own);
                let _ = write!(evs, r#"["advance",{own}]"#);
            }
            _ => {
                let idx = rng.below(n_followers as u64) as usize;
                let d = rng.below(1 << 20);
                t.on_durable(idx, d);
                let _ = write!(evs, r#"["report",{idx},{d}]"#);
            }
        }
    }
    evs.push(']');
    writeln!(
        out,
        r#"{{"fn":"advance_fold","n_followers":{n_followers},"cluster_size":{cluster_size},"events":{evs},"expect":{{"commit":{}}}}}"#,
        t.commit()
    )
    .unwrap();
}

fn emit_log_ok(rng: &mut Rng, out: &mut impl std::io::Write) {
    // Dense around the boundary: equal terms / equal durables are common.
    let ot = rng.below(6) as u32;
    let ct = if rng.below(2) == 0 {
        ot
    } else {
        rng.below(6) as u32
    };
    let od = rng.below(4);
    let cd = if rng.below(2) == 0 { od } else { rng.below(4) };
    writeln!(
        out,
        r#"{{"fn":"log_ok","our_term":{ot},"our_durable":{od},"cand_term":{ct},"cand_durable":{cd},"expect":{}}}"#,
        log_ok_order(ot, od, ct, cd)
    )
    .unwrap();
}

fn main() {
    let mut out_path = String::new();
    let mut count = 100_000u64;
    let mut seed = 20_260_716u64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--out" => out_path = args.next().expect("--out PATH"),
            "--count" => count = args.next().unwrap().parse().unwrap(),
            "--seed" => seed = args.next().unwrap().parse().unwrap(),
            other => panic!("unknown arg {other}"),
        }
    }
    assert!(!out_path.is_empty(), "--out is required");
    if let Some(dir) = std::path::Path::new(&out_path).parent() {
        std::fs::create_dir_all(dir).unwrap();
    }
    let f = std::fs::File::create(&out_path).unwrap();
    let mut w = std::io::BufWriter::new(f);
    let mut rng = Rng(seed);
    for i in 0..count {
        match i % 3 {
            // One reconcile vector in three is the windowed (`j > 0`) shape.
            0 if rng.below(3) == 0 => emit_reconcile_windowed(&mut rng, &mut w),
            0 => emit_reconcile(&mut rng, &mut w),
            1 => emit_advance_fold(&mut rng, &mut w),
            _ => emit_log_ok(&mut rng, &mut w),
        }
    }
    w.flush().unwrap();
    eprintln!("wrote {count} vectors to {out_path}");
}
