// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The live rig behind `uc2-diffreplay pin-verify` (spec §6.2 part 2): an
//! in-process single-voter node, the app's service binary as a black-box
//! child process, a real `uc2ctl upgrade pin`, and a corpus's recorded
//! commands re-submitted through the raw client engine. Everything here
//! reads UC's own surfaces (the cnc page, the snapshot dir, the journal) and
//! never the app's — a recorded MESSAGE frame's payload travels back to the
//! node as opaque bytes, exactly as it was captured.
//!
//! Every wait in this module is a bounded poll that names its condition when
//! it expires; nothing here sleeps a fixed interval and hopes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use uc_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc_journal::TailReader;
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::frame::{
    self, FRAME_TYPE_MESSAGE, FRAME_TYPE_TIMER, HEADER_LEN, align_frame_len,
};
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};

use crate::corpus::Corpus;

/// The single-voter config this crate's tests and the `pin-verify` harness
/// both run: a 1 MiB ring, a 256 B payload cap, purge OFF (the shipped
/// default — the counterfactual path spec §2.3 warns about is the one this
/// rig must leave open, so the harness can show the pin closing it).
pub fn node_config(dir: &Path, app_id: &str, fsm: &str) -> NodeConfig {
    let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: uc_net::fault::FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(fsm),
    }
}

/// Poll `f` until it holds or `timeout` elapses. Returns whether it held —
/// the caller decides what a timeout means, so cleanup can run before an
/// assertion fires.
#[must_use]
pub fn wait_for(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// Start the node and wait until it serves (a single voter elects itself;
/// the first submit — and the first service attach, which waits out
/// `NodeBooting` — races that without the wait).
pub fn start_node(dir: &Path, app_id: &str, fsm: &str, timeout: Duration) -> anyhow::Result<Node> {
    let node = Node::start(node_config(dir, app_id, fsm))
        .map_err(|e| anyhow::anyhow!("node start: {e}"))?;
    if !wait_for(|| node.can_serve(), timeout) {
        node.stop();
        bail!("the node never became a serving leader within {timeout:?}");
    }
    Ok(node)
}

/// `uc2ctl snapshot` in process: command an instant, return its position P.
/// `SnapshotRefusal::Retry` is the single-in-flight race and is waited out;
/// every other refusal is a refusal.
pub fn command_instant(node: &Node, timeout: Duration) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return Ok(p),
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => bail!("uc2ctl snapshot refused: {e}"),
        }
    }
}

/// Where a row publishes the artifact for instant `p`.
pub fn artifact_path(dir: &Path, row: u8, p: u64) -> PathBuf {
    dir.join("snapshots")
        .join(row.to_string())
        .join(format!("snap-{p}.ultsnap"))
}

/// The row's slot status word's incarnation field — what
/// [`AppProcess::wait_attached`] compares against, so a fresh attach is told
/// apart from a stale ATTACHED bit left by a stopped process.
pub fn incarnation(cnc: &CncPage, row: u8) -> u32 {
    uc_log::cnc::unpack_service_status(cnc.service_slot(row as usize).status.load_acquire()).2
}

/// `<instance_dir>/upgrade.pending`, written the way `uc2ctl` writes it:
/// 0600, fsync'd, renamed into place, so the node reads a whole record or
/// none of one.
fn stage_upgrade_pin(dir: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let pending = dir.join(uc_node::UPGRADE_PENDING_FILE);
    let tmp = dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("staging {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &pending)?;
    Ok(())
}

/// The `uc2ctl` mutating-command flow, minus the bin
/// (`uc_node/tests/reconfig.rs`'s `admin_request`): read the admin band's
/// current seq, write a fresh request at `seq + 1`, poll the response line
/// for the echoed seq. These rigs run the filesystem admin policy, so there
/// is no auth line to sign.
fn admin_request(
    cnc: &CncPage,
    op: u32,
    id: u32,
    ip: u32,
    port: u16,
    timeout: Duration,
) -> anyhow::Result<AdminResp> {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    // The nonce is the anti-replay field of the SIGNED flow; with the
    // filesystem policy nothing reads it, so a fresh wall-clock reading is
    // enough to keep two requests in one run distinct.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(seq);
    cnc.write_admin_req(&AdminReq {
        seq,
        nonce,
        op,
        id,
        ip,
        port,
    });
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return Ok(resp);
        }
        if Instant::now() >= deadline {
            bail!("admin response timed out for seq {seq}");
        }
        std::thread::yield_now();
    }
}

/// `uc2ctl upgrade pin --row <row> --from <from> --to <to> --origin <origin>`
/// in process (FSM upgrade lifecycle spec §2.5, plan B1): stage the 20-byte
/// `UpgradePin` record, then submit admin op 10 with the staged file's digest
/// in the `id`/`ip`/`port` fields.
///
/// Exactly two answers are RACES against this rig rather than errors in it,
/// and only those two are retried until `timeout`: status 2 (the ordinary
/// single-in-flight retry) and reason 54 `pin_no_set`, which compares
/// `origin` against the node's NEWEST complete set — published by the cluster
/// agent a moment after the row's own artifact appears. Every other refusal
/// returns `Err` immediately, naming its status and reason, instead of being
/// re-sent for the whole timeout and then reported as one.
pub fn pin_row(
    dir: &Path,
    cnc: &CncPage,
    row: u8,
    from: u32,
    to: u32,
    origin: u64,
    timeout: Duration,
) -> anyhow::Result<AdminResp> {
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from,
            to,
            origin,
        },
        &mut bytes,
    );
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let deadline = Instant::now() + timeout;
    loop {
        stage_upgrade_pin(dir, &bytes)?;
        let resp = admin_request(cnc, ADMIN_OP_UPGRADE_PIN, id, ip, port, timeout)?;
        if resp.status == 0 {
            return Ok(resp);
        }
        let racy = resp.status == 2 || resp.reason == uc_node::REASON_PIN_NO_SET;
        if !racy || Instant::now() >= deadline {
            bail!(
                "uc2ctl upgrade pin refused: status={} reason={}",
                resp.status,
                resp.reason
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The app's service binary as a black-box child: `<bin> <args…>
/// --instance-dir <dir> --app-id <app_id>`, stderr captured to a file so a
/// refused attach can be reported with the app's own words.
pub struct AppProcess {
    child: Child,
    stderr_path: PathBuf,
    /// Whether the child has already been waited for — by [`wait_cond`]'s
    /// exit or timeout path. A reaped pid is no longer ours: the kernel may
    /// hand the number to an unrelated process, so [`AppProcess::stop`]'s
    /// raw `libc::kill` must not run once this is set.
    ///
    /// [`wait_cond`]: AppProcess::wait_cond
    reaped: bool,
}

/// What a bounded wait on the child's cnc-page condition saw. The three
/// arms are distinct on purpose: a child that exited on its own has an exit
/// code and said why, while one the wait KILLED never got to — reporting the
/// second as `Exited { code: None }` would read as a signal death.
#[derive(Debug)]
pub enum AttachOutcome {
    Attached,
    Exited {
        code: Option<i32>,
        stderr: String,
    },
    /// The condition never held within the bound; the child was still
    /// running and has been killed.
    TimedOut {
        stderr: String,
    },
}

pub fn spawn_app(
    bin: &Path,
    args: &[String],
    dir: &Path,
    app_id: &str,
    stderr_path: &Path,
) -> anyhow::Result<AppProcess> {
    let stderr = std::fs::File::create(stderr_path)
        .with_context(|| format!("creating {}", stderr_path.display()))?;
    let child = Command::new(bin)
        .args(args)
        .arg("--instance-dir")
        .arg(dir)
        .arg("--app-id")
        .arg(app_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .with_context(|| format!("spawn {}", bin.display()))?;
    Ok(AppProcess {
        child,
        stderr_path: stderr_path.to_path_buf(),
        reaped: false,
    })
}

impl AppProcess {
    /// Everything the child has written to stderr so far — the evidence a
    /// failed wait reports.
    pub fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    /// The child's pid, while it is still ours (see [`AppProcess::stop`]'s
    /// SAFETY note). Exposed so a test can look for the process AFTER the
    /// handle is dropped.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Poll `cond` against the page until it holds (→ `Attached`) or the
    /// child exits first (→ `Exited` with its code and stderr), bounded by
    /// `timeout` (→ `TimedOut`, the child killed and reaped).
    fn wait_cond(&mut self, mut cond: impl FnMut() -> bool, timeout: Duration) -> AttachOutcome {
        let deadline = Instant::now() + timeout;
        loop {
            if cond() {
                return AttachOutcome::Attached;
            }
            // After the exit check, `cond` gets one last look on the next
            // turn only if the child is still alive — so read the page
            // FIRST, above, and let a child that exited having satisfied the
            // condition be reported as attached.
            if let Ok(Some(st)) = self.child.try_wait() {
                self.reaped = true;
                return AttachOutcome::Exited {
                    code: st.code(),
                    stderr: self.stderr(),
                };
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                self.reaped = true;
                return AttachOutcome::TimedOut {
                    stderr: self.stderr(),
                };
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// "Attached" = the row's status word has its ATTACHED bit set under an
    /// incarnation that differs from `before` (captured by the caller just
    /// before [`spawn_app`]). Not the version word — a state machine that
    /// does not override `VERSION` publishes the trait default `0`, so the
    /// word cannot tell two eras apart — and not the bit alone, which a
    /// stopped process may leave set.
    pub fn wait_attached(
        &mut self,
        cnc: &CncPage,
        row: u8,
        before: u32,
        timeout: Duration,
    ) -> AttachOutcome {
        self.wait_cond(
            || {
                let (_, attached, inc) = uc_log::cnc::unpack_service_status(
                    cnc.service_slot(row as usize).status.load_acquire(),
                );
                attached && inc != before
            },
            timeout,
        )
    }

    /// "Caught up" = the row's published `applied` frontier ≥ `at_least`.
    pub fn wait_applied(
        &mut self,
        cnc: &CncPage,
        row: u8,
        at_least: u64,
        timeout: Duration,
    ) -> AttachOutcome {
        self.wait_cond(
            || cnc.service_slot(row as usize).applied.load_acquire() >= at_least,
            timeout,
        )
    }

    /// SIGTERM, wait up to `timeout`, else SIGKILL — and say which happened:
    /// a killed child returns `Err` naming the timeout, never a silent
    /// success that a caller could read as a clean stop.
    pub fn stop(mut self, timeout: Duration) -> anyhow::Result<std::process::ExitStatus> {
        // `self` drops after the call; `stop_inner` has set `reaped`, so
        // `Drop` is a no-op and the child is never signalled twice.
        self.stop_inner(timeout)
    }

    fn stop_inner(&mut self, timeout: Duration) -> anyhow::Result<std::process::ExitStatus> {
        if !self.reaped {
            // SAFETY: `reaped` is false, so this is a pid we spawned and
            // have NOT waited for — the kernel is still holding it for us
            // and cannot have recycled the number. (`kill` itself has no
            // memory-safety preconditions; the pid's ownership is the
            // hazard, and that is what the flag guards.) A child already
            // waited for by `wait_cond` falls through to the cached exit
            // status below instead.
            unsafe {
                libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
            }
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(st) = self.child.try_wait()? {
                self.reaped = true;
                return Ok(st);
            }
            if Instant::now() >= deadline {
                self.child.kill()?;
                let st = self.child.wait()?;
                self.reaped = true;
                bail!("the service did not stop within {timeout:?} after SIGTERM; killed ({st})");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// How long [`AppProcess`]'s `Drop` gives a child to honour SIGTERM before
/// killing it. Short on purpose: the drop path is a rig FAILURE path, where
/// the caller is already unwinding and a clean shutdown is a courtesy, not a
/// result anyone will read.
const DROP_GRACE: Duration = Duration::from_secs(2);

impl Drop for AppProcess {
    /// A child this rig spawned must never outlive the run. [`AppProcess::stop`]
    /// consumes `self` and is the CLEAN path — it reports how the child went;
    /// this covers every other way the handle can go out of scope, which is
    /// every `?` and `bail!` between a successful attach and that call.
    ///
    /// It has to kill AND reap, because an orphaned service does not stop on
    /// its own: `uc_service` fail-stops only when the node's `instance_id`
    /// CHANGES (`uc_service/src/apply.rs`), and an in-process `Node::stop`
    /// leaves the cnc page — id and all — intact, so the apply loop would
    /// busy-spin against a dead node for the life of the process.
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        // SAFETY: `reaped` is false, so the pid is still ours and cannot have
        // been recycled — the same argument `stop_inner` makes.
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + DROP_GRACE;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                // Past the grace, or a `wait` that errored: SIGKILL and reap.
                // Nothing here can report a failure, so nothing here tries.
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break;
                }
            }
        }
        self.reaped = true;
    }
}

/// The payload bytes of every MESSAGE frame in the corpus's `[origin, end)`,
/// in log order — the app's own commands, opaque to us — together with the
/// number of TIMER frames the same span carried. A TIMER frame is counted,
/// never returned: a node mints those, so they cannot be re-submitted by a
/// client, and a caller that wants the span reproduced exactly needs to know
/// how many were dropped.
pub fn message_frames(corpus: &Corpus) -> anyhow::Result<(Vec<Vec<u8>>, u64)> {
    let m = &corpus.manifest;
    let reader = TailReader::open(&corpus.journal_dir())?;
    let mut frames = Vec::new();
    let mut timers = 0u64;
    let end = m.end;
    // The block walk is `drive::walk_block`'s, minus the dispatch: archived
    // blocks lay their frames back to back at the full ALIGNED span, a
    // sub-header or over-running length ends THIS block, and the span bound
    // is the frame's END against the exclusive frontier `end`.
    reader.scan_from(m.origin, |_seq, base, block| {
        let mut off = 0usize;
        while off + HEADER_LEN <= block.len() {
            let hdr = frame::read_header(&block[off..]);
            let total = hdr.length as usize;
            let aligned = align_frame_len(total);
            if total < HEADER_LEN || off + aligned > block.len() {
                break;
            }
            let pos = base + off as u64;
            if pos.saturating_add(aligned as u64) > end {
                return false;
            }
            if pos >= m.origin {
                match hdr.frame_type {
                    FRAME_TYPE_MESSAGE => {
                        frames.push(block[off + HEADER_LEN..off + total].to_vec())
                    }
                    FRAME_TYPE_TIMER => timers += 1,
                    _ => {}
                }
            }
            off += aligned;
        }
        true
    })?;
    Ok((frames, timers))
}

/// What [`replay_span`] did.
#[derive(Debug)]
pub struct SpanReplay {
    /// How many commands were accepted AND completed with a response.
    pub submitted: u64,
    /// The log position the LAST completed command's response named: the
    /// START of that command's frame (`Egress::publish`'s `pos`, which is
    /// [`uc_log`]'s `FrameIter` cursor BEFORE it advances over the frame —
    /// `uc_log/src/reader.rs`). It is NOT an applied frontier, and a reader
    /// must never resume from it: the frame at that very position has
    /// already been applied. The row's own `applied` word — the cursor
    /// AFTER the batch — is the frontier; it is strictly greater than this
    /// once the last command lands.
    ///
    /// The raw tier always carries the position (`Completion::position` is
    /// `Some` for a `Response`/`Responses` outcome), so there is no fallback
    /// read of the row's `applied` word here.
    pub last_position: u64,
}

/// One command's fate, once its completion arrived.
enum Verdict {
    /// Completed at this log position.
    Done(u64),
    /// Pre-side-effect: `Retry`, or a redirect this single-voter rig answers
    /// by re-submitting once leadership settles.
    Again,
}

/// Re-submit `frames[range]` to `row`, one command in flight, through the raw
/// client engine. Backpressure / not-serving on the submit side and
/// `Retry` / `NotLeader` on the completion side are waited out, bounded by
/// `timeout` per command; any other outcome is an error naming the index.
///
/// `range` is 0-based over the MESSAGE frames [`message_frames`] returned —
/// not over log positions.
pub fn replay_span(
    frames: &[Vec<u8>],
    dir: &Path,
    app_id: &str,
    row: u8,
    range: std::ops::Range<usize>,
    timeout: Duration,
) -> anyhow::Result<SpanReplay> {
    if range.end > frames.len() {
        bail!(
            "span {range:?} runs past the corpus's {} MESSAGE frames",
            frames.len()
        );
    }
    // The caller's bound is the binding one: `EngineConfig::default()`'s
    // 10 s `request_timeout` would fail a command the caller was still
    // willing to wait for (a coordinated freeze on this single voter stalls
    // commit for as long as the slowest row's freeze takes).
    let cfg = EngineConfig {
        request_timeout: timeout,
        ..EngineConfig::default()
    };
    let (send, mut poll) =
        Engine::attach(dir, app_id, cfg).map_err(|e| anyhow::anyhow!("engine attach: {e}"))?;
    let mut last_position = 0u64;
    let mut submitted = 0u64;
    let mut i = range.start;
    // One deadline per INDEX, re-armed only when the index advances, so a
    // command that keeps answering `Retry` is bounded too.
    let mut deadline = Instant::now() + timeout;
    while i < range.end {
        let bytes = &frames[i];
        // Submit, waiting out backpressure and a momentarily non-serving node.
        loop {
            match send.try_submit_to(i as u64, row, bytes) {
                Ok(()) => break,
                Err(SubmitError::Backpressure | SubmitError::NotServing) => {
                    // Drain anything outstanding (nothing, on this one-in-flight
                    // path) so backpressure can actually clear.
                    poll.poll(|_| {});
                    if Instant::now() >= deadline {
                        bail!("command #{i}: could not submit within {timeout:?}");
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => bail!("command #{i}: {e}"),
            }
        }
        // Wait for THIS command's completion.
        let mut verdict: Option<anyhow::Result<Verdict>> = None;
        while verdict.is_none() {
            poll.poll(|c| {
                if c.user_data != i as u64 {
                    return;
                }
                verdict = Some(match c.outcome {
                    // A completed response ALWAYS names its position
                    // (`uc_client`'s engine sets it for every `Response` /
                    // `Responses` outcome). Degrading a missing one to 0
                    // would hand the caller a frontier of zero as if it
                    // were a measurement, so say so instead.
                    Outcome::Response(_) | Outcome::Responses(_) => match c.position {
                        Some(p) => Ok(Verdict::Done(p)),
                        None => Err(anyhow::anyhow!(
                            "command #{i}: completed with no log position on the response"
                        )),
                    },
                    Outcome::Retry | Outcome::NotLeader { .. } => Ok(Verdict::Again),
                    ref other => Err(anyhow::anyhow!("command #{i}: {other:?}")),
                });
            });
            if verdict.is_none() {
                if Instant::now() >= deadline {
                    bail!("command #{i}: no completion within {timeout:?}");
                }
                std::thread::yield_now();
            }
        }
        match verdict.expect("the loop exits only once a verdict is set")? {
            Verdict::Done(p) => {
                last_position = p;
                submitted += 1;
                i += 1;
                deadline = Instant::now() + timeout;
            }
            Verdict::Again => {
                // Re-submit the SAME index: the node asked for a retry, or
                // leadership was settling. `i` does not advance and the
                // deadline is not re-armed, so this is bounded.
                if Instant::now() >= deadline {
                    bail!("command #{i}: still retrying after {timeout:?}");
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    Ok(SpanReplay {
        submitted,
        last_position,
    })
}
