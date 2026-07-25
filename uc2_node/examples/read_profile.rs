// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Linearizable-read profile harness. See
//! `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`.

use std::path::Path;

/// One agent thread's yield rate over a measurement window.
///
/// **Why yields and not CPU time:** the node's agents idle on
/// `IdleStrategy::Yield` (`uc2_log/src/agent.rs:28` → `std::thread::yield_now()`),
/// so an IDLE agent still burns a core in a yield loop and CPU% is saturated by
/// construction. Each empty duty cycle costs one `sched_yield`, which the kernel
/// counts in `voluntary_ctxt_switches` — so a LOW yield rate means a BUSY agent.
/// This is an ordinal signal (it ranks agents); it is not a duty-cycle percentage.
#[derive(Debug, Clone, PartialEq)]
struct Occupancy {
    pub name: String,
    pub yields_per_sec: f64,
}

/// Read `(thread_name, voluntary_ctxt_switches)` for every thread under a
/// `/proc/<pid>/task` directory. Threads that vanish mid-scan (exited between
/// readdir and read) are skipped rather than failing the sample.
fn sample_yields(task_dir: &Path) -> std::io::Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(task_dir)? {
        let path = entry?.path();
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else { continue };
        let Ok(status) = std::fs::read_to_string(path.join("status")) else { continue };
        let yields = status
            .lines()
            .find_map(|l| l.strip_prefix("voluntary_ctxt_switches:"))
            .and_then(|v| v.trim().parse::<u64>().ok());
        let Some(yields) = yields else { continue };
        out.push((comm.trim().to_string(), yields));
    }
    Ok(out)
}

/// Join two samples by thread name and rank by yield rate ASCENDING — fewest
/// yields first, i.e. busiest agent first. Threads missing from either sample
/// are dropped (they did not exist for the whole window, so their rate is not
/// comparable).
fn occupancy_delta(
    before: &[(String, u64)],
    after: &[(String, u64)],
    secs: f64,
) -> Vec<Occupancy> {
    let mut out: Vec<Occupancy> = after
        .iter()
        .filter_map(|(name, late)| {
            let (_, early) = before.iter().find(|(n, _)| n == name)?;
            Some(Occupancy {
                name: name.clone(),
                yields_per_sec: late.saturating_sub(*early) as f64 / secs,
            })
        })
        .collect();
    out.sort_by(|a, b| a.yields_per_sec.total_cmp(&b.yields_per_sec));
    out
}

fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a synthetic `/proc/<pid>/task` tree: one dir per thread, each
    /// holding a `comm` and a `status` file in the kernel's format.
    fn fake_task_dir(threads: &[(&str, u64)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for (i, (name, yields)) in threads.iter().enumerate() {
            let t = dir.path().join(format!("{}", 1000 + i));
            fs::create_dir(&t).unwrap();
            fs::write(t.join("comm"), format!("{name}\n")).unwrap();
            fs::write(
                t.join("status"),
                format!(
                    "Name:\t{name}\nThreads:\t1\nvoluntary_ctxt_switches:\t{yields}\n\
                     nonvoluntary_ctxt_switches:\t7\n"
                ),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn samples_name_and_yield_count_per_thread() {
        let dir = fake_task_dir(&[("uc2-consensus", 100), ("uc2-sender", 250)]);
        let mut got = sample_yields(dir.path()).expect("sample");
        got.sort();
        assert_eq!(
            got,
            vec![
                ("uc2-consensus".to_string(), 100),
                ("uc2-sender".to_string(), 250)
            ]
        );
    }

    #[test]
    fn skips_threads_missing_files_rather_than_failing() {
        let dir = fake_task_dir(&[("uc2-consensus", 100)]);
        // A thread that exited between readdir and read: dir exists, files don't.
        fs::create_dir(dir.path().join("2000")).unwrap();
        let got = sample_yields(dir.path()).expect("sample");
        assert_eq!(got, vec![("uc2-consensus".to_string(), 100)]);
    }

    #[test]
    fn delta_ranks_busiest_first_and_normalizes_by_time() {
        let before = vec![("uc2-consensus".into(), 100u64), ("uc2-sender".into(), 100)];
        // Over 2 s: consensus yielded 20 times (busy), sender 2000 (idle).
        let after = vec![("uc2-consensus".into(), 120u64), ("uc2-sender".into(), 2100)];
        let got = occupancy_delta(&before, &after, 2.0);
        assert_eq!(got[0].name, "uc2-consensus", "busiest (fewest yields) ranks first");
        assert_eq!(got[0].yields_per_sec, 10.0);
        assert_eq!(got[1].name, "uc2-sender");
        assert_eq!(got[1].yields_per_sec, 1000.0);
    }

    #[test]
    fn delta_ignores_threads_absent_from_either_sample() {
        let before = vec![("uc2-consensus".into(), 100u64), ("gone".into(), 5)];
        let after = vec![("uc2-consensus".into(), 120u64), ("new".into(), 5)];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "uc2-consensus");
    }
}
