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

/// Q2 (controller ruling), amended by **Q2'**: a hit whose line mentions it
/// is retired/reserved is not a re-introduction — but that excuse applies
/// ONLY to non-`.rs` paths (docs, scripts, YAML, TOML), which are the
/// historical record allowed to say so in current tense. A `.rs` hit is
/// ALWAYS a hit, regardless of its line's content — `let _x =
/// ScheduleRecord::default(); // retired` is still live code using a
/// retired symbol, and a trailing comment must not excuse it. Each `hit_line`
/// is one `git grep -n` output line, shaped `path:lineno:content`.
fn hit_is_excused(hit_line: &str) -> bool {
    let mut parts = hit_line.splitn(3, ':');
    let Some(path) = parts.next() else {
        return false;
    };
    let _lineno = parts.next();
    let content = parts.next().unwrap_or("");
    if path.ends_with(".rs") {
        return false;
    }
    let lower = content.to_ascii_lowercase();
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
            let remaining: Vec<&str> = text.lines().filter(|line| !hit_is_excused(line)).collect();
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

/// Q2': the excuse must never fire on a `.rs` hit, no matter what its line
/// says — only a non-`.rs` (historical-record) hit can be excused by
/// mentioning "retired"/"reserved".
#[test]
fn the_retired_or_reserved_excuse_applies_only_outside_rs_files() {
    assert!(
        !hit_is_excused(
            "uc_node/src/some_file.rs:42:    let _x = ScheduleRecord::default(); // retired"
        ),
        "a .rs hit must never be excused by its own comment"
    );
    assert!(
        hit_is_excused("docs/BACKLOG.md:12:`ScheduleRecord` is retired."),
        "a non-.rs hit naming a retired symbol as retired must still be excused"
    );
}
