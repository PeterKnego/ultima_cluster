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
//! Topology (mirrors uc2_node's four polling agents at the uc2_log layer):
//!   * appender thread  — lock-free writes into the shared LogBuffer (the
//!     leader's hot path). Concurrent with the archiver's `recordable_slice`
//!     frame walk: this is the H1 race (torn/immutable-region walk).
//!   * archiver thread  — `Archive::do_work` (recordable_slice -> journal
//!     block -> fdatasync -> advance durable). The `RecorderCorrupt` (H1) and,
//!     downstream, `CorruptBlock` (recorded garbage) surface here.
//!   * replayer thread  — `replay_from(random pos in [first_base, durable))`
//!     draining `next()` to the end. The `CorruptBlock` (H1/H2/H3) OOB site.
//!   * reconfig thread  — (arms B/C only) quiesces the appender via an
//!     exclusive `append_gate`, then either `truncate_to(frame boundary)` +
//!     `prime` (election reconciliation, H2) or drop+reopen the Archive +
//!     `prime(recovered)` (crash-restart, H4). Bumps `gen` so the appender
//!     rebuilds its `Appender` at the primed frontier — exactly as uc2_node's
//!     `BecomeLeader`/archive-truncate paths do (`close_gate` -> `prime` ->
//!     fresh `Appender`).
//!
//! The single production caller of `recordable_slice` is `Archive::do_work`,
//! and the single caller of `Replay::next` is a `replay_from` drain, so these
//! three+one agents cover every path to the two structured errors.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use uc2_log::archive::{Archive, ArchiveConfig, ArchiveError};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::cnc::{CncMeta, CncPage};
use uc2_log::region::Region;

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
/// also a failure. Panics with the run seed so a hit is reproducible-ish
/// (thread timing is inherently nondeterministic, but the seed pins the
/// payload-size stream).
fn fail_repro(seed: u64, ctx: &str, err: &ArchiveError) -> ! {
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
) -> Option<u64> {
    let mut r = match arch.replay_from(first) {
        Ok(r) => r,
        Err(ArchiveError::PositionPurged { .. }) => return None,
        Err(e) => fail_repro(seed, "pick_frame_boundary/replay_from", &e),
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
            Err(e) => fail_repro(seed, "pick_frame_boundary/next", &e),
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
                        // the new frontier (uc2_node's post-prime `Appender::new`).
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
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("stress-archiver".into())
            .spawn(move || {
                let mut since_purge = 0u32;
                while !stop.load(Ordering::Relaxed) {
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
                            fail_repro(seed, "archiver/do_work", &e)
                        }
                        Err(e @ ArchiveError::CorruptBlock { .. }) => {
                            fail_repro(seed, "archiver/do_work", &e)
                        }
                        Err(e) => fail_repro(seed, "archiver/do_work(unexpected)", &e),
                    }
                    if since_purge >= 32 {
                        since_purge = 0;
                        let durable = buffer.counters().durable.load_acquire();
                        if durable > PURGE_SLACK
                            && let Err(e) = arch.purge_below(durable - PURGE_SLACK)
                        {
                            fail_repro(seed, "archiver/purge_below", &e);
                        }
                    }
                    drop(guard);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // ---- replayer ---------------------------------------------------------
    let replayer_thread = {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("stress-replayer".into())
            .spawn(move || {
                let mut rng = seed ^ 0x5555_1234;
                while !stop.load(Ordering::Relaxed) {
                    let guard = archive.lock().unwrap();
                    let arch = guard.as_ref().expect("archive present outside reopen");
                    // Holding the archive lock pins durable + first_base + the
                    // journal against the archiver, so [first, durable) is a
                    // consistent, non-purged, contiguous span.
                    let first = arch.first_base();
                    let durable = buffer.counters().durable.load_acquire();
                    if durable > first {
                        let pos = first + xorshift(&mut rng) % (durable - first);
                        let mut r = match arch.replay_from(pos) {
                            Ok(r) => r,
                            Err(e) => fail_repro(seed, "replayer/replay_from", &e),
                        };
                        loop {
                            match r.next() {
                                Ok(Some(_)) => {}
                                Ok(None) => break,
                                Err(e @ ArchiveError::CorruptBlock { .. }) => {
                                    fail_repro(seed, "replayer/next", &e)
                                }
                                Err(e) => fail_repro(seed, "replayer/next(unexpected)", &e),
                            }
                        }
                    }
                    drop(guard);
                    thread::yield_now();
                }
            })
            .unwrap()
    };

    // ---- reconfig (arms B/C) ---------------------------------------------
    let reconfig_thread = if arm.truncation || arm.reopen {
        let buffer = Arc::clone(&buffer);
        let archive = Arc::clone(&archive);
        let append_gate = Arc::clone(&append_gate);
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
                        // Quiesce the appender, then take the archive, and HOLD
                        // BOTH across the whole reconfig: no append can race the
                        // prime and no archiver/replayer can touch the journal
                        // (uc2_node closes the gate across the counter reset, and
                        // a crash-restart has no concurrent journal writer at
                        // all). Releasing either mid-reconfig is the unfaithful
                        // interleaving that manufactures a false repro.
                        let _ag = append_gate.lock().unwrap();
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
                                    pick_frame_boundary(arch, first, durable, &mut rng, seed)
                            {
                                match arch.truncate_to(target) {
                                    Ok(()) => {
                                        buffer.counters().prime(target);
                                        generation.fetch_add(1, Ordering::Release);
                                    }
                                    Err(e) => fail_repro(seed, "reconfig/truncate_to", &e),
                                }
                            }
                        }
                        drop(guard);
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
/// `recordable_slice` frame walk racing the appender) and the `Replay::next`
/// drain over live-recorded blocks.
#[test]
#[cfg_attr(miri, ignore)] // real journal files + fsync + threads
fn stress_append_archive_replay() {
    run_stress(ArmConfig { truncation: false, reopen: false }, "A/append-archive-replay");
}

/// Arm B: adds periodic `truncate_to(frame boundary)` + `prime` under an
/// append quiesce — the election-reconciliation churn the original panic was
/// seen under (H2: stale pre-truncation lengths at re-used buffer offsets).
#[test]
#[cfg_attr(miri, ignore)]
fn stress_with_truncation() {
    run_stress(ArmConfig { truncation: true, reopen: false }, "B/truncation");
}

/// Arm C: periodic drop+reopen of the Archive mid-load with counter priming —
/// the crash-restart shape (H4: `prime` leaving `durable`/`append` at a
/// frontier whose buffer offsets hold stale bytes). Truncation churn too, so
/// reopen recovers a truncated frontier.
#[test]
#[cfg_attr(miri, ignore)]
fn stress_reopen() {
    run_stress(ArmConfig { truncation: true, reopen: true }, "C/reopen");
}
