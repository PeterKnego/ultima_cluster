//! Subprocess driver with hard wall-clock timeout.
//!
//! All candidate code (cargo check / test / microbench / e2e) runs through here.
//! On timeout the child is killed with SIGKILL so the OS reclaims any mmap'd
//! shmem files the process held.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum SandboxOutcome {
    Completed {
        exit_code: i32,
        stdout: String,
        stderr: String,
        duration: Duration,
    },
    TimedOut {
        duration: Duration,
    },
}

/// Run `cmd` via `/bin/sh -c`, capturing stdout/stderr. Enforces `timeout`
/// via SIGKILL on overrun.
pub fn run_subprocess(cmd: &str, timeout: Duration) -> anyhow::Result<SandboxOutcome> {
    let start = Instant::now();
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Drain stdout/stderr in background threads so the child doesn't block on
    // a full pipe buffer.
    let mut child_stdout = child.stdout.take().expect("piped");
    let mut child_stderr = child.stderr.take().expect("piped");
    let (out_tx, out_rx) = channel();
    let (err_tx, err_rx) = channel();
    std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = child_stdout.read_to_string(&mut buf);
        let _ = out_tx.send(buf);
    });
    std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = child_stderr.read_to_string(&mut buf);
        let _ = err_tx.send(buf);
    });

    // Poll for child exit with a deadline.
    let deadline = start + timeout;
    loop {
        match child.try_wait()? {
            Some(status) => {
                let duration = start.elapsed();
                let stdout = out_rx
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap_or_default();
                let stderr = err_rx
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap_or_default();
                return Ok(SandboxOutcome::Completed {
                    exit_code: status.code().unwrap_or(-1),
                    stdout,
                    stderr,
                    duration,
                });
            }
            None => {
                if Instant::now() >= deadline {
                    // SIGKILL — `Child::kill` on Unix sends SIGKILL.
                    let _ = child.kill();
                    let _ = child.wait();
                    return Ok(SandboxOutcome::TimedOut {
                        duration: start.elapsed(),
                    });
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
