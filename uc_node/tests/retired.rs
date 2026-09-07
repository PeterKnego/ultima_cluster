//! Spec §7 / plan 3: the symbols this flag day retired must not come back
//! under their old names. A grep, not a compile check, so a re-introduction
//! in a comment, a doc or a script is caught too.
use std::process::Command;

const RETIRED: &[&str] = &[
    "FRAME_TYPE_CONFIG\\b",
    "FRAME_TYPE_SCHEDULE_TABLE\\b",
    "append_schedule_table",
    "DGRAM_KIND_SNAP_TABLE\\b",
    "SnapTableBody",
    "SNAP_TABLE_FIXED_LEN",
    "SNAP_BEGIN_LAYOUT_V3\\b",
    "schedule_state::",
    "ScheduleRecord\\b",
    "SCHEDULE_STATE_FILE",
    "ScheduleShip",
    "shippable_schedule",
    "known_committed",
    "install_snapshot_table",
    "incoming_snapshot_config",
    "incoming_snapshot_table",
    "SnapshotPolicy\\b",
    "maybe_build_snapshot",
    "rearm_timers",
    "uc2_timers_rearmed_total",
    "timers_rearmed",
    "retain_newest",
    "bridging_trigger",
];

/// Q2 (controller ruling): a hit whose line mentions it is retired/reserved
/// is not a re-introduction — the historical record is allowed to say so in
/// current tense even inside a file that isn't excluded outright.
fn line_is_excused(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.contains("retired") || lower.contains("reserved")
}

#[test]
fn retired_symbols_are_gone_from_the_tree() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    let mut hits = Vec::new();
    for pat in RETIRED {
        let out = Command::new("git")
            .args([
                "grep",
                "-nE",
                pat,
                "--",
                ":!docs/superpowers/",
                ":!docs/benchmarks/",
                ":!docs/releases.md",
                ":!RELEASES.md",
                ":!uc_node/tests/retired.rs",
                ":!docs/notes/",
            ])
            .current_dir(root)
            .output()
            .unwrap();
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout);
            let remaining: Vec<&str> = text.lines().filter(|line| !line_is_excused(line)).collect();
            if !remaining.is_empty() {
                hits.push(format!("{pat}:\n{}", remaining.join("\n")));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "retired symbols still present:\n{}",
        hits.join("\n")
    );
}
