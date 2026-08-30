// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![cfg(not(loom))]

//! Post-M7 archive stress: concurrent appender + archiver + replayer,
//! wrap-heavy, with truncation and reopen arms. Repro harness for the
//! once-seen Replay::next OOB panic (M7 ledger open ticket). A structured
//! CorruptBlock/RecorderCorrupt error IS the repro signal — fail the test
//! and print the full error + iteration seed.
//! Budget: UC2_ARCHIVE_STRESS_MS (default 2000 for CI; run 60000+ locally).
//!
//! Topology (mirrors uc_node's four polling agents at the uc_log layer):
//!   * appender thread  — lock-free writes into the shared LogBuffer (the
//!     leader's hot path). Concurrent with the archiver's `recordable_slice`
//!     frame walk: this is the H1 race (torn/immutable-region walk).
//!   * archiver thread  — `Archive::do_work` (recordable_slice -> journal
//!     block -> fdatasync -> advance durable). The `RecorderCorrupt` (H1) and,
//!     downstream, `CorruptBlock` (recorded garbage) surface here.
//!   * replayer thread  — LOCK-FREE `Replay` drain over a `journal_arc()`
//!     clone (`replay_journal_from(random pos in [first_base, durable))`),
//!     WITHOUT the archive mutex: journal READS genuinely race the archiver's
//!     journal APPENDS + fsyncs (the sender-thread NAK shape, and the original
//!     panic's described "scan against a block still being concurrently
//!     written" — H3 by exercise, not only by audit). The `CorruptBlock`
//!     (H1/H2/H3) OOB site.
//!   * reconfig thread  — (arms B/C only) quiesces the appender via an
//!     exclusive `append_gate`, then either `truncate_to(frame boundary)` +
//!     `prime` (election reconciliation, H2) or drop+reopen the Archive +
//!     `prime(recovered)` (crash-restart, H4). Bumps `gen` so the appender
//!     rebuilds its `Appender` at the primed frontier — exactly as uc_node's
//!     `BecomeLeader`/archive-truncate paths do (`close_gate` -> `prime` ->
//!     fresh `Appender`).
//!
//! Lock discipline: `append_gate` -> `topo` (RwLock) -> `archive` (Mutex),
//! strictly in that order for any thread taking more than one. `topo`
//! excludes ONLY topology mutations from in-flight replay drains: purge /
//! truncate / reopen take `topo.write()`, the replayer holds `topo.read()`
//! across a drain — `Replay::next` contract-requires its `[seq, last_seq]`
//! snapshot to stay readable (production `Replay` users are boot-time
//! single-threaded; the production lock-free journal reader, the sender's NAK
//! path, tolerates vanished blocks itself, `sender.rs::serve_nak_from_journal`).
//! Crucially `do_work`'s append+fsync NEVER takes `topo`, so replay reads race
//! block appends for real.
//!
//! The single production caller of `recordable_slice` is `Archive::do_work`,
//! and `Replay::next` is only ever driven by a replay drain, so these
//! three+one agents cover every path to the two structured errors.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use uc_log::archive::{Archive, ArchiveConfig, ArchiveError, find_block, replay_journal_from};
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::cnc::{CncMeta, CncPage};
use uc_log::region::Region;

const CAP: u64 = 1 << 16; // 64 KiB — small so wrap (+ padding frames) is constant
const MAX_PAYLOAD: usize = 4000;
/// Journal slack kept above the purge floor so the on-disk journal stays
/// bounded across a 60 s run (unbounded growth would exhaust tmpfs); purge is
/// realistic (M6) and keeps `first_base` honest for the replayer's pos pick.
const PURGE_SLACK: u64 = 4 * CAP;

#[derive(Clone, Copy)]
struct ArmConfig {
    truncation: bool,
    reopen: bool,
    /// Arm D: run a "nak-server" thread that models
    /// `sender.rs::serve_nak_from_journal` — a LOCK-FREE journal reader that
    /// does NOT take `topo`, so the archiver's `purge_below` drops blocks out
    /// from under its `find_block`/`journal.read(seq)` walk. Unlike the
    /// `replay_journal_from` replayer (which holds `topo.read` to honor
    /// `Replay::next`'s snapshot-readable contract), the production sender path
    /// re-locates each block with `find_block` and TOLERATES a vanished one, so
    /// this thread must never panic under the purge race. Closes t3b.
    nak_server: bool,
}

fn budget_ms() -> u64 {
    std::env::var("UC2_ARCHIVE_STRESS_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(2000)
}

fn run_seed() -> u64 {
    std::env::var("UC2_ARCHIVE_STRESS_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15)
    })
}

/// A journal dir on a real filesystem. `tempfile::tempdir()` lands in $TMPDIR
/// (a RAM tmpfs with a quota on this box); a 60 s run's journal can outgrow it,
/// so honor `UC2_ARCHIVE_STRESS_DIR` (point it at ext4 for the long runs).
fn journal_dir() -> tempfile::TempDir {
    match std::env::var("UC2_ARCHIVE_STRESS_DIR") {
        Ok(base) => tempfile::Builder::new().prefix("uc2-arch-stress-").tempdir_in(base).unwrap(),
        Err(_) => tempfile::tempdir().unwrap(),
    }
}

#[inline]
fn xorshift(s: &mut u64) -> u64 {
    let mut x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    x
}

fn make_buffer() -> (Arc<LogBuffer>, Arc<CncPage>) {
    let cnc = CncPage::heap(&CncMeta {
        node_id: 0,
        instance_id: 0,
        app_id: "stress".into(),
        buffer_bytes: CAP,
        max_payload: MAX_PAYLOAD as u32,
    });
    let buffer =
        Arc::new(LogBuffer::new(Region::heap_zeroed(CAP as usize), Arc::clone(&cnc), MAX_PAYLOAD));
    (buffer, cnc)
}

fn archive_cfg(dir: &std::path::Path) -> ArchiveConfig {
    // Small segments so a long run's on-disk journal stays modest; preallocate
    // as production does.
    ArchiveConfig { segment_size_bytes: 4 * 1024 * 1024, ..ArchiveConfig::new(dir) }
}

/// The two structured corruption errors are the repro. Any other error is
/// unexpected in this harness (we never pick a purged replay position) and is
/// also a failure. Sets the shared stop flag FIRST so the main loop and every
/// sibling thread wind down promptly (instead of sleeping out the full
/// budget before the join re-raises), then panics with the run seed so a hit
/// is reproducible-ish (thread timing is inherently nondeterministic, but the
/// seed pins the payload-size stream).
fn fail_repro(stop: &AtomicBool, seed: u64, ctx: &str, err: &ArchiveError) -> ! {
    stop.store(true, Ordering::Relaxed);
    panic!("ARCHIVE STRESS REPRO (seed={seed}) at {ctx}: {err:?} -- {err}");
}

/// Enumerate a few frame-start positions in `[first, durable)` by replaying,
/// and return one in the middle band (a valid `truncate_to` target). Returns
/// `None` when too few frames exist yet. Propagates a corrupt-block hit as a
/// repro (the replay drain is itself a `Replay::next` exercise).
fn pick_frame_boundary(
    arch: &Archive,
    first: u64,
    durable: u64,
    rng: &mut u64,
    seed: u64,
    stop: &AtomicBool,
) -> Option<u64> {
    let mut r = match arch.replay_from(first) {
        Ok(r) => r,
        Err(ArchiveError::PositionPurged { .. }) => return None,
        Err(e) => fail_repro(stop, seed, "pick_frame_boundary/replay_from", &e),
    };
    let mut positions = Vec::new();
    loop {
        match r.next() {
            Ok(Some(f)) => {
                if f.position >= durable {
                    break;
                }
                positions.push(f.position);
                if positions.len() >= 512 {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => fail_repro(stop, seed, "pick_frame_boundary/next", &e),
        }
    }
    if positions.len() < 4 {
        return None;
    }
    // Middle band: keep enough of a prefix that the cut is meaningful and stays
    // strictly inside (first, durable).
    let base = positions.len() / 3;
    let span = (positions.len() / 3).max(1);
    let idx = base + (xorshift(rng) as usize % span);
    positions.get(idx).copied()
}

fn run_stress(arm: ArmConfig, arm_name: &str) {
    let seed = run_seed();
    let budget = Duration::from_millis(budget_ms());
    println!(
        "archive_stress[{arm_name}] seed={seed} budget_ms={} truncation={} reopen={}",
        budget.as_millis(),
        arm.truncation,
        arm.reopen
    );

    let (buffer, _cnc) = make_buffer();
    let dir = journal_dir();
    let cfg = archive_cfg(dir.path());
    // `Option` so a reopen can drop the old Archive (closing its journal) and
    // open the new one WITHOUT ever releasing the mutex — a crash-restart has
    // exactly one live journal at a time (never two on the same dir). The
    // Archive is always `Some` outside a reopen's held critical section.
    let archive = Arc::new(Mutex::new(Some(Archive::open(cfg.clone()).unwrap())));
    let append_gate = Arc::new(Mutex::new(())); // held by appender; taken exclusively to reconfig
    // Journal-topology lock (see module doc): write = purge/truncate/reopen,
    // read = an in-flight lock-free replay drain. do_work NEVER takes it.
    let topo = Arc::new(RwLock::new(()));
    let generation = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let deadline = Instant::now() + budget;

    // ---- appender ---------------------------------------------------------
    let appender_thread = {
        let buffer = Arc::clone(&buffer);
        let append_gate = Arc::clone(&append_gate);
        let generation = Arc::clone(&generation);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("stress-appender".into())
            .spawn(move || {
                let mut rng = seed ^ 0xA5A5_A5A5;
                let mut term: u32 = 1;
                let mut my_gen = generation.load(Ordering::Acquire);
                let mut appender = Appender::new(Arc::clone(&buffer), term);
                let scratch = vec![0xABu8; MAX_PAYLOAD];
                while !stop.load(Ordering::Relaxed) {
                    let _g = append_gate.lock().unwrap();
                    let cur = generation.load(Ordering::Acquire);
                    if cur != my_gen {
                        // A reconfig primed the counters: rebuild the appender at
                        // the new frontier (uc_node's post-prime `Appender::new`).
                        my_gen = cur;
                        term = term.wrapping_add(1).max(1);
                        appender = Appender::new(Arc::clone(&buffer), term);
                    }
                    for _ in 0..16 {
                        // total frame 100..=4000 B => payload 68..=3968 B
                        let payload_len = 68 + (xorshift(&mut rng) as usize % (3968 - 68));
                        let sid = xorshift(&mut rng);
                        let cid = xorshift(&mut rng);
                        match appender.append(sid, cid, &scratch[..payload_len]) {
                            Ok(_) => {}
                            Err(AppendError::WouldOverrun) => break, // let the archive drain
                            Err(AppendError::PayloadTooLarge) => unreachable!("bounded above"),
                        }
                    }
                    drop(_g);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // ---- archiver ---------------------------------------------------------
    let archiver_thread = {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let topo = Arc::clone(&topo);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("stress-archiver".into())
            .spawn(move || {
                let mut since_purge = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    // Purge drops whole segments, so it is a topology mutation:
                    // take `topo.write()` BEFORE the archive mutex (the one lock
                    // order) so no lock-free replay drain is in flight. do_work
                    // below deliberately does NOT take `topo` — its journal
                    // appends + fsyncs must race the replayer's reads.
                    if since_purge >= 32 {
                        since_purge = 0;
                        let durable = buffer.counters().durable.load_acquire();
                        if durable > PURGE_SLACK {
                            let _t = topo.write().unwrap();
                            let mut guard = archive.lock().unwrap();
                            let arch = guard.as_mut().expect("archive present outside reopen");
                            if let Err(e) = arch.purge_below(durable - PURGE_SLACK) {
                                fail_repro(&stop, seed, "archiver/purge_below", &e);
                            }
                        }
                    }
                    let mut guard = archive.lock().unwrap();
                    let arch = guard.as_mut().expect("archive present outside reopen");
                    match arch.do_work(&buffer) {
                        Ok(true) => {
                            // Drain observations so they don't grow unbounded.
                            arch.take_term_observations();
                            arch.take_config_observations();
                            since_purge += 1;
                        }
                        Ok(false) => {}
                        Err(e @ ArchiveError::RecorderCorrupt(_)) => {
                            fail_repro(&stop, seed, "archiver/do_work", &e)
                        }
                        Err(e @ ArchiveError::CorruptBlock { .. }) => {
                            fail_repro(&stop, seed, "archiver/do_work", &e)
                        }
                        Err(e) => fail_repro(&stop, seed, "archiver/do_work(unexpected)", &e),
                    }
                    drop(guard);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // ---- replayer (LOCK-FREE vs the archiver's appends) --------------------
    let replayer_thread = {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let topo = Arc::clone(&topo);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("stress-replayer".into())
            .spawn(move || {
                let mut rng = seed ^ 0x5555_1234;
                while !stop.load(Ordering::Relaxed) {
                    // `topo.read()` for the whole drain: purge/truncate/reopen
                    // are excluded (Replay::next's snapshot-readable contract),
                    // but do_work is NOT — every journal.read below races the
                    // archiver's journal.append + fdatasync for real. The
                    // archive mutex is held only long enough to snapshot the
                    // journal handle + floor, never across a read.
                    let _t = topo.read().unwrap();
                    let (journal, first) = {
                        let guard = archive.lock().unwrap();
                        let arch = guard.as_ref().expect("archive present outside reopen");
                        (arch.journal_arc(), arch.first_base())
                    };
                    // Read durable AFTER the snapshot: it only grows (do_work
                    // advances it post-append+fsync on this same journal), so
                    // every pos < durable is block-covered; and under
                    // topo.read, first can't grow (no purge) and the handle
                    // can't be swapped (no reopen).
                    let durable = buffer.counters().durable.load_acquire();
                    if durable > first {
                        let pos = first + xorshift(&mut rng) % (durable - first);
                        match replay_journal_from(&journal, pos) {
                            Ok(Some(mut r)) => loop {
                                match r.next() {
                                    Ok(Some(_)) => {}
                                    Ok(None) => break,
                                    Err(e @ ArchiveError::CorruptBlock { .. }) => {
                                        fail_repro(&stop, seed, "replayer/next", &e)
                                    }
                                    Err(e) => {
                                        fail_repro(&stop, seed, "replayer/next(unexpected)", &e)
                                    }
                                }
                            },
                            Ok(None) => {
                                // pos ∈ [first, durable) with purge excluded by
                                // topo.read: a covering block MUST exist.
                                stop.store(true, Ordering::Relaxed);
                                panic!(
                                    "ARCHIVE STRESS REPRO (seed={seed}) at replayer/\
                                     replay_journal_from: pos {pos} in [{first}, {durable}) \
                                     reported below-floor (Ok(None)) with purge excluded"
                                );
                            }
                            Err(e) => {
                                fail_repro(&stop, seed, "replayer/replay_journal_from", &e)
                            }
                        }
                    }
                    drop(_t);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // ---- nak-server (arm D): LOCK-FREE, no topo, races purge --------------
    // Faithful model of sender.rs::serve_nak_from_journal: walk blocks with
    // find_block + journal.read(seq), tolerating a block that a concurrent
    // purge_below drops between locate and read (break, never panic). It holds
    // NO topology lock, so the archiver's purge genuinely races this walk —
    // the exact production interleaving the topo.read replayer above excludes.
    let nak_thread = if arm.nak_server {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let stop = Arc::clone(&stop);
        Some(
            thread::Builder::new()
                .name("stress-nak".into())
                .spawn(move || {
                    let mut rng = seed ^ 0x0FF1_CE05;
                    while !stop.load(Ordering::Relaxed) {
                        // Snapshot the journal handle + floor WITHOUT topo (the
                        // sender holds no topology lock). first/durable may both
                        // move under us; that is the point.
                        let (journal, first) = {
                            let guard = archive.lock().unwrap();
                            let arch = guard.as_ref().expect("archive present outside reopen");
                            (arch.journal_arc(), arch.first_base())
                        };
                        let durable = buffer.counters().durable.load_acquire();
                        if durable > first {
                            // Bias half the picks to the FLOOR itself: `find_block`
                            // then reads `first` right where the purger is dropping
                            // it, maximally exposing the read-after-first_seq TOCTOU
                            // window a purge-race panic lived in (t3b).
                            let pos = if xorshift(&mut rng).is_multiple_of(2) {
                                first
                            } else {
                                first + xorshift(&mut rng) % (durable - first)
                            };
                            let end = durable;
                            // The serve_nak_from_journal loop, verbatim in shape:
                            // re-locate each block; a vanished (purged) block just
                            // ends the walk. NO expect/unwrap that a purge could
                            // trip — a panic here is the repro.
                            let mut p = pos;
                            let mut guard_iters = 0u32;
                            while p < end && guard_iters < 8192 {
                                guard_iters += 1;
                                let Some((seq, base)) =
                                    find_block(&journal, p).ok().flatten()
                                else {
                                    break; // purged/below-floor: tolerated
                                };
                                let Ok(Some((rbase, block))) = journal.read(seq) else {
                                    break; // block dropped between locate and read
                                };
                                // Append-only + front-purge never renumbers a seq,
                                // so an existing seq's base is stable even under
                                // the race (mirrors the production debug_assert).
                                assert_eq!(rbase, base, "seq {seq}: read base disagrees with find_block");
                                let block_end = base + block.len() as u64;
                                if p >= block_end {
                                    break; // at/beyond the durable frontier
                                }
                                p = block_end;
                            }
                        }
                        thread::yield_now();
                    }
                })
                .unwrap(),
        )
    } else {
        None
    };

    // ---- reconfig (arms B/C) ---------------------------------------------
    let reconfig_thread = if arm.truncation || arm.reopen {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let append_gate = Arc::clone(&append_gate);
        let topo = Arc::clone(&topo);
        let generation = Arc::clone(&generation);
        let stop = Arc::clone(&stop);
        let cfg = cfg.clone();
        Some(
            thread::Builder::new()
                .name("stress-reconfig".into())
                .spawn(move || {
                    let mut rng = seed ^ 0xDEAD_BEEF;
                    while !stop.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_millis(2 + xorshift(&mut rng) % 6));
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        // Quiesce the appender, exclude in-flight replay drains
                        // (topology mutation -> topo.write), then take the
                        // archive — the one lock order — and HOLD ALL THREE
                        // across the whole reconfig: no append can race the
                        // prime and no archiver/replayer can touch the journal
                        // (uc_node closes the gate across the counter reset, and
                        // a crash-restart has no concurrent journal user at
                        // all). Releasing any of them mid-reconfig is the
                        // unfaithful interleaving that manufactures a false
                        // repro.
                        let _ag = append_gate.lock().unwrap();
                        let _t = topo.write().unwrap();
                        let mut guard = archive.lock().unwrap();

                        let do_reopen =
                            arm.reopen && (!arm.truncation || xorshift(&mut rng).is_multiple_of(2));
                        if do_reopen {
                            // Crash-restart (H4): drop the OLD Archive (closes its
                            // journal) BEFORE opening the new one — exactly one
                            // live journal on the dir, ever — then recover the
                            // durable frontier and prime the counters there (node
                            // boot). All under the held guard, so no concurrent
                            // writer sees a half-open dir.
                            *guard = None; // drop old Archive => close journal
                            let re = Archive::open(cfg.clone())
                                .expect("reopen archive (journal I/O fail-stop)");
                            let recovered = re.recovered_position();
                            *guard = Some(re);
                            buffer.counters().prime(recovered);
                            generation.fetch_add(1, Ordering::Release);
                        } else if arm.truncation {
                            let arch = guard.as_mut().expect("archive present");
                            let durable = buffer.counters().durable.load_acquire();
                            let first = arch.first_base();
                            if durable > first + PURGE_SLACK / 2
                                && let Some(target) =
                                    pick_frame_boundary(arch, first, durable, &mut rng, seed, &stop)
                            {
                                match arch.truncate_to(target) {
                                    Ok(()) => {
                                        buffer.counters().prime(target);
                                        generation.fetch_add(1, Ordering::Release);
                                    }
                                    Err(e) => fail_repro(&stop, seed, "reconfig/truncate_to", &e),
                                }
                            }
                        }
                        drop(guard);
                        drop(_t);
                        drop(_ag);
                    }
                })
                .unwrap(),
        )
    } else {
        None
    };

    // Let the arms run for the budget.
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);

    // Propagate the first panicking thread (the repro), if any.
    let mut panics = Vec::new();
    for (name, h) in [
        ("appender", appender_thread),
        ("archiver", archiver_thread),
        ("replayer", replayer_thread),
    ] {
        if let Err(p) = h.join() {
            panics.push((name, p));
        }
    }
    if let Some(h) = reconfig_thread
        && let Err(p) = h.join()
    {
        panics.push(("reconfig", p));
    }
    if let Some(h) = nak_thread
        && let Err(p) = h.join()
    {
        panics.push(("nak-server", p));
    }
    if let Some((name, payload)) = panics.into_iter().next() {
        eprintln!("archive_stress[{arm_name}]: thread '{name}' failed (seed={seed})");
        std::panic::resume_unwind(payload);
    }

    // Sanity: the run actually did work (guards against a no-op green).
    let durable = buffer.counters().durable.load_acquire();
    println!(
        "archive_stress[{arm_name}] seed={seed} OK — durable frontier {durable} B, blocks {}",
        archive.lock().unwrap().as_ref().unwrap().blocks_recorded()
    );
    assert!(durable > 0, "harness did no work (durable frontier never advanced)");
}

/// Arm A: concurrent append + archive + replay, wrap-heavy. Exercises H1 (the
/// `recordable_slice` frame walk racing the appender) and the lock-free
/// `Replay::next` drain racing the archiver's journal appends + fsyncs (H3 by
/// exercise).
#[test]
#[cfg_attr(miri, ignore)] // real journal files + fsync + threads
fn stress_append_archive_replay() {
    run_stress(
        ArmConfig { truncation: false, reopen: false, nak_server: false },
        "A/append-archive-replay",
    );
}

/// Arm B: adds periodic `truncate_to(frame boundary)` + `prime` under an
/// append quiesce — the election-reconciliation churn the original panic was
/// seen under (H2: stale pre-truncation lengths at re-used buffer offsets).
#[test]
#[cfg_attr(miri, ignore)]
fn stress_with_truncation() {
    run_stress(ArmConfig { truncation: true, reopen: false, nak_server: false }, "B/truncation");
}

/// Arm C: periodic drop+reopen of the Archive mid-load with counter priming —
/// the crash-restart shape (H4: `prime` leaving `durable`/`append` at a
/// frontier whose buffer offsets hold stale bytes). Truncation churn too, so
/// reopen recovers a truncated frontier.
#[test]
#[cfg_attr(miri, ignore)]
fn stress_reopen() {
    run_stress(ArmConfig { truncation: true, reopen: true, nak_server: false }, "C/reopen");
}

/// Focused t3b repro (find_block purge-race): a TIGHT purge loop (floor chases
/// durable at one-buffer slack, purging every iteration — no `since_purge`
/// gate) while a finder hammers `find_block(&journal, floor)` LOCK-FREE. This
/// hits the read-after-`first_seq()` TOCTOU window far more often than arm D's
/// gated purge. On the pre-fix `find_block` (which `.expect()`s a raced read),
/// a `first block readable` / `block readable` panic is the repro; the tolerant
/// version returns `Ok(None)` and this runs clean. Discriminating guard for the
/// find_block purge-tolerance fix.
#[test]
#[cfg_attr(miri, ignore)]
fn find_block_tolerates_tight_purge_race() {
    let seed = run_seed();
    let budget = Duration::from_millis(budget_ms());
    println!("find_block_tolerates_tight_purge_race seed={seed} budget_ms={}", budget.as_millis());
    // One-buffer slack: the floor stays ~CAP below durable and moves whenever
    // durable advances (i.e. constantly under the appender's load).
    const SMALL_SLACK: u64 = CAP;

    let (buffer, _cnc) = make_buffer();
    let dir = journal_dir();
    let archive = Arc::new(Mutex::new(Archive::open(archive_cfg(dir.path())).unwrap()));
    let append_gate = Arc::new(Mutex::new(()));
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + budget;

    // appender: continuous load so the journal keeps growing (purge has supply).
    let appender_thread = {
        let (buffer, append_gate, stop) =
            (Arc::clone(&buffer), Arc::clone(&append_gate), Arc::clone(&stop));
        thread::Builder::new()
            .name("tight-appender".into())
            .spawn(move || {
                let mut rng = seed ^ 0xA5A5_A5A5;
                let mut appender = Appender::new(Arc::clone(&buffer), 1);
                let scratch = vec![0xABu8; MAX_PAYLOAD];
                while !stop.load(Ordering::Relaxed) {
                    let _g = append_gate.lock().unwrap();
                    for _ in 0..16 {
                        let payload_len = 68 + (xorshift(&mut rng) as usize % (3968 - 68));
                        match appender.append(xorshift(&mut rng), xorshift(&mut rng), &scratch[..payload_len]) {
                            Ok(_) => {}
                            Err(AppendError::WouldOverrun) => break,
                            Err(AppendError::PayloadTooLarge) => unreachable!("bounded above"),
                        }
                    }
                    drop(_g);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // archiver: record blocks (no purge here — the purger owns that).
    let archiver_thread = {
        let (buffer, archive, stop) = (Arc::clone(&buffer), Arc::clone(&archive), Arc::clone(&stop));
        thread::Builder::new()
            .name("tight-archiver".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Err(e) = archive.lock().unwrap().do_work(&buffer) {
                        fail_repro(&stop, seed, "tight/archiver", &e);
                    }
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // purger: TIGHT loop — purge the floor to durable-SMALL_SLACK every
    // iteration. Holds the archive mutex only for the purge call itself.
    let purger_thread = {
        let (buffer, archive, stop) = (Arc::clone(&buffer), Arc::clone(&archive), Arc::clone(&stop));
        thread::Builder::new()
            .name("tight-purger".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let durable = buffer.counters().durable.load_acquire();
                    if durable > SMALL_SLACK
                        && let Err(e) = archive.lock().unwrap().purge_below(durable - SMALL_SLACK)
                    {
                        fail_repro(&stop, seed, "tight/purger", &e);
                    }
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // finder: LOCK-FREE `find_block` hammering the floor, racing the purger.
    let finder_thread = {
        let (archive, stop) = (Arc::clone(&archive), Arc::clone(&stop));
        thread::Builder::new()
            .name("tight-finder".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Snapshot the (stable) journal handle + current floor under a
                    // brief lock, then release BEFORE the lock-free find_block so
                    // the purger genuinely races it.
                    let (journal, first) = {
                        let g = archive.lock().unwrap();
                        (g.journal_arc(), g.first_base())
                    };
                    for _ in 0..256 {
                        match find_block(&journal, first) {
                            Ok(_) => {} // Some or Ok(None) (purged) both fine
                            Err(e) => fail_repro(&stop, seed, "tight/finder", &e),
                        }
                    }
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);

    let mut panics = Vec::new();
    for (name, h) in [
        ("appender", appender_thread),
        ("archiver", archiver_thread),
        ("purger", purger_thread),
        ("finder", finder_thread),
    ] {
        if let Err(p) = h.join() {
            panics.push((name, p));
        }
    }
    if let Some((name, payload)) = panics.into_iter().next() {
        eprintln!("find_block_tolerates_tight_purge_race: thread '{name}' failed (seed={seed})");
        std::panic::resume_unwind(payload);
    }
    let durable = buffer.counters().durable.load_acquire();
    println!("find_block_tolerates_tight_purge_race seed={seed} OK — durable {durable} B");
    assert!(durable > 0, "harness did no work");
}

/// Arm D (t3b): adds a lock-free "nak-server" thread modeling
/// `sender.rs::serve_nak_from_journal` — `find_block` + `journal.read(seq)` per
/// block, holding NO `topo` lock, so the archiver's `purge_below` drops blocks
/// out from under its walk. The production sender path must TOLERATE a vanished
/// block (break, never panic); a panic in this arm (e.g. an internal `.expect`
/// on a read a purge just invalidated) is the repro. Closes the ledger's
/// sender-NAK-vs-purge coverage gap.
#[test]
#[cfg_attr(miri, ignore)]
fn stress_nak_vs_purge() {
    run_stress(
        ArmConfig { truncation: false, reopen: false, nak_server: true },
        "D/nak-vs-purge",
    );
}
