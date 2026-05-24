use tempfile::TempDir;
use uc_autobench::outcome::LoopEvent;
use uc_autobench::persist::EventLog;

fn evt(run_id: &str) -> LoopEvent {
    LoopEvent::RunStarted {
        t: "2026-05-24T00:00:00Z".into(),
        run_id: run_id.into(),
        task: "shmem-rings".into(),
        git_head: "abc".into(),
    }
}

#[test]
fn append_then_replay_yields_same_events() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("events.jsonl");

    let mut log = EventLog::open(&path).unwrap();
    log.append(&evt("r1")).unwrap();
    log.append(&evt("r2")).unwrap();
    drop(log);

    let replayed = EventLog::replay(&path).unwrap();
    assert_eq!(replayed.len(), 2);
    if let LoopEvent::RunStarted { run_id, .. } = &replayed[0] {
        assert_eq!(run_id, "r1");
    } else {
        panic!("expected RunStarted");
    }
}

#[test]
fn replay_tolerates_trailing_garbage() {
    // Crash-mid-write may leave a partial last line. Replay must return
    // everything *before* the bad line and not error out.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("events.jsonl");

    let mut log = EventLog::open(&path).unwrap();
    log.append(&evt("r1")).unwrap();
    drop(log);

    // Append garbage at the end.
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    f.write_all(b"{not-json\n").unwrap();
    drop(f);

    let replayed = EventLog::replay(&path).unwrap();
    assert_eq!(replayed.len(), 1);
}

#[test]
fn append_fsyncs_each_line() {
    // We don't assert syscalls, but we do assert that each append is independently
    // readable (i.e. flushed) by reopening between writes.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("events.jsonl");

    let mut log = EventLog::open(&path).unwrap();
    log.append(&evt("r1")).unwrap();
    let mid = EventLog::replay(&path).unwrap();
    assert_eq!(mid.len(), 1);
    log.append(&evt("r2")).unwrap();
    drop(log);
    let final_ = EventLog::replay(&path).unwrap();
    assert_eq!(final_.len(), 2);
}
