use std::time::Duration;
use uc_autobench::sandbox::{SandboxOutcome, run_subprocess};

#[test]
fn fast_command_succeeds_with_captured_stdout() {
    let r = run_subprocess("echo hello && echo world", Duration::from_secs(5)).unwrap();
    match r {
        SandboxOutcome::Completed {
            exit_code,
            stdout,
            stderr: _,
            duration: _,
        } => {
            assert_eq!(exit_code, 0);
            assert!(stdout.contains("hello"));
            assert!(stdout.contains("world"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[test]
fn nonzero_exit_reports_exit_code() {
    let r = run_subprocess("exit 7", Duration::from_secs(5)).unwrap();
    match r {
        SandboxOutcome::Completed { exit_code, .. } => assert_eq!(exit_code, 7),
        other => panic!("expected Completed, got {other:?}"),
    }
}

#[test]
fn slow_command_is_killed_at_timeout() {
    // sleep for 10s but timeout after 1s
    let start = std::time::Instant::now();
    let r = run_subprocess("sleep 10", Duration::from_secs(1)).unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "should have been killed quickly, took {elapsed:?}"
    );
    match r {
        SandboxOutcome::TimedOut { duration } => {
            assert!(duration >= Duration::from_secs(1));
        }
        other => panic!("expected TimedOut, got {other:?}"),
    }
}

#[test]
fn stderr_is_captured_separately() {
    let r = run_subprocess("echo out; echo err 1>&2", Duration::from_secs(5)).unwrap();
    match r {
        SandboxOutcome::Completed { stdout, stderr, .. } => {
            assert!(stdout.contains("out"));
            assert!(stderr.contains("err"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}
