// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` node daemon.
//!
//! Starts one node from a TOML config file and runs until signalled. On
//! `SIGTERM`/`SIGINT` it drains the archive to a bounded deadline and stops
//! the agents cleanly, so the restarted node rejoins from the journal instead
//! of paying reconstruction.
//!
//! M12b: `[admin]` is a required, explicit choice (spec §3.3) — this is the
//! one place that turns the config file's `[admin]` section into a live
//! [`AdminPolicy`] and hands it to [`Node::start_with`]. `auth = "none"`
//! loads no keys and prints a boot-time warning (filesystem access on the
//! instance directory is the only admin boundary); `auth = "hmac"` loads
//! every named key file via `AdminKey::load`, and a bad key file (missing,
//! wrong length, group/world-readable) is a named startup refusal — exit 2,
//! same family as every other config refusal this binary already makes.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use uc2_node::config_file::AdminAuthMode;
use uc2_node::obs::http::ObsServer;
use uc2_node::preflight::FsVerdict;
use uc2_node::{AdminKey, AdminPolicy, DrainOutcome, Node, StartOpts, config_file, preflight};

#[derive(Parser)]
#[command(name = "uc2-node", about = "An ultima_cluster node", version)]
struct Args {
    /// Path to the node's TOML configuration file.
    #[arg(long)]
    config: PathBuf,
    /// How long to let the archive drain before stopping anyway.
    #[arg(long, default_value = "5")]
    drain_timeout_secs: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let (cfg, opts) = match config_file::load_from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("uc2-node: {e}");
            return ExitCode::from(2);
        }
    };
    match preflight::check(&cfg, &opts) {
        Ok(FsVerdict::Durable) => {}
        // The override suppresses the refusal, never the notice — and it is
        // announced on EVERY boot, not just the one where it was added. A
        // cluster running on a RAM-backed filesystem must never look quiet.
        Ok(FsVerdict::VolatileOverridden { fs }) => {
            eprintln!(
                "uc2-node: WARNING: starting with the durability check OVERRIDDEN. \
                 Every fsync may be a silent no-op, so this node can lose committed \
                 data on power loss. TEST/DEV ONLY. Detail: {fs}"
            );
        }
        Err(e) => {
            eprintln!("uc2-node: refusing to start: {e}");
            return ExitCode::from(2);
        }
    }
    uc2_node::obs::log::set_level(opts.obs.log_level);

    // M12b: build the live AdminPolicy from `[admin]`. `auth = "none"` never
    // reads a key file — it IS today's pre-M12b posture, and the WARNING
    // fires on every boot (never silenced), same convention as the volatile-
    // fs override above.
    let admin = match opts.admin.auth {
        AdminAuthMode::None => {
            eprintln!(
                "uc2-node: WARNING: [admin] auth = \"none\" — anyone who can write the \
                 instance directory can change cluster membership"
            );
            AdminPolicy::Filesystem
        }
        AdminAuthMode::Hmac => {
            let mut keys = Vec::with_capacity(opts.admin.keys.len());
            for entry in &opts.admin.keys {
                match AdminKey::load(&entry.name, &entry.key_path) {
                    Ok(k) => keys.push(k),
                    Err(e) => {
                        eprintln!(
                            "uc2-node: admin key {} at {}: {e}",
                            entry.name,
                            entry.key_path.display()
                        );
                        return ExitCode::from(2);
                    }
                }
            }
            AdminPolicy::Hmac { keys: Arc::new(keys), ttl: Duration::from_millis(opts.admin.request_ttl_ms) }
        }
    };

    let id = cfg.id;
    let bind = cfg.bind;
    let instance_dir = cfg.instance_dir.clone();
    let node = match Node::start_with(cfg, StartOpts { socket: None, admin }) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("uc2-node: failed to start node {id}: {e}");
            return ExitCode::from(1);
        }
    };
    println!("uc2-node: node {id} listening on {bind}");

    let obs = node.observability();
    let mut srv: Option<ObsServer> = None;
    if let Some(addr) = opts.obs.metrics_bind {
        match ObsServer::serve(obs.clone(), addr) {
            Ok(s) => {
                println!("uc2-node: observability endpoint on http://{}/metrics", s.local_addr());
                srv = Some(s);
            }
            Err(e) => {
                eprintln!("uc2-node: failed to bind observability endpoint {addr}: {e}");
                node.stop();
                return ExitCode::from(1);
            }
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            eprintln!("uc2-node: cannot install signal handler: {e}");
            node.stop();
            return ExitCode::from(1);
        }
    }

    // Derived-events pass: how often (every 10th 100ms tick, ~1s) and how
    // often each event may actually be recorded (rate limit) are two
    // separate cadences. `last_*` tracks the value AS OF THE LAST PASS (so
    // the delta each pass reflects real change since ~1s ago, no matter how
    // recently an event last fired); `last_*_emit` tracks when an event
    // last actually printed a record, independent of the delta cadence.
    const DERIVED_EVENTS_EVERY_N_TICKS: u64 = 10;
    const DERIVED_EVENT_RATE_LIMIT: Duration = Duration::from_secs(10);
    let mut tick: u64 = 0;
    let mut last_naks_dropped = obs.sender.naks_dropped.load(Ordering::Relaxed);
    let mut last_seal_failures = obs.sender.seal_failures.load(Ordering::Relaxed)
        + obs.receiver.seal_failures.load(Ordering::Relaxed);
    // The BASELINE the `seal_failures` record's `count` is measured against
    // — unlike `last_seal_failures` (which advances every pass so the
    // edge-trigger sees real ~1s-window change), this only advances when a
    // record actually fires, so a suppressed (rate-limited) pass's failures
    // are folded into the NEXT record instead of being silently dropped.
    let mut last_emitted_seal_failures = last_seal_failures;
    let mut last_snapshot_pos = obs.cnc.snapshots().service_snapshot_pos.load_acquire();
    let mut last_nak_storm_emit: Option<Instant> = None;
    let mut last_seal_failures_emit: Option<Instant> = None;
    let mut last_snapshot_emit: Option<Instant> = None;
    let mut last_statvfs_warn_emit: Option<Instant> = None;

    let mut was_leader = None;
    while !stop.load(Ordering::Relaxed) {
        let is_leader = node.is_leader();
        if was_leader != Some(is_leader) {
            println!(
                "uc2-node: node {id} is now {} (term {})",
                if is_leader { "LEADER" } else { "follower" },
                node.current_term()
            );
            was_leader = Some(is_leader);
        }

        tick += 1;
        if tick.is_multiple_of(DERIVED_EVENTS_EVERY_N_TICKS) {
            // A dead agent makes everything downstream (the drain, the obs
            // endpoint) meaningless — fail fast so systemd's
            // Restart=on-failure takes over; the restarted node replays its
            // journal. Deliberately skips stop_draining: Node's Drop
            // swallows the panic that killed the agent, but stop_draining
            // would re-raise it.
            if let Some((name, _)) = obs.agents.iter().find(|(_, f)| f.load(Ordering::Acquire)) {
                uc2_node::obs_event!(Error, "agent_failstopped", agent = *name);
                eprintln!("uc2-node: agent {name} fail-stopped; exiting");
                return ExitCode::FAILURE;
            }

            let now = Instant::now();

            let naks_dropped = obs.sender.naks_dropped.load(Ordering::Relaxed);
            let naks_served = obs.sender.naks_served.load(Ordering::Relaxed);
            if naks_dropped > last_naks_dropped
                && last_nak_storm_emit
                    .is_none_or(|t| now.duration_since(t) >= DERIVED_EVENT_RATE_LIMIT)
            {
                uc2_node::obs_event!(
                    Warn,
                    "nak_storm",
                    node = id as u64,
                    naks_dropped = naks_dropped,
                    naks_served = naks_served,
                );
                last_nak_storm_emit = Some(now);
            }
            last_naks_dropped = naks_dropped;

            let seal_failures = obs.sender.seal_failures.load(Ordering::Relaxed)
                + obs.receiver.seal_failures.load(Ordering::Relaxed);
            if seal_failures > last_seal_failures
                && last_seal_failures_emit
                    .is_none_or(|t| now.duration_since(t) >= DERIVED_EVENT_RATE_LIMIT)
            {
                // WINDOW delta since the last EMITTED record, not the
                // cumulative total — a reader must see "how many failed
                // since I last heard about this", or a steady 1/10s trickle
                // reads as an accelerating storm every time it prints.
                uc2_node::obs_event!(
                    Warn,
                    "seal_failures",
                    node = id as u64,
                    count = seal_failures - last_emitted_seal_failures,
                    is_leader = is_leader,
                );
                last_seal_failures_emit = Some(now);
                last_emitted_seal_failures = seal_failures;
            }
            last_seal_failures = seal_failures;

            let snapshot_pos = obs.cnc.snapshots().service_snapshot_pos.load_acquire();
            if snapshot_pos > last_snapshot_pos
                && last_snapshot_emit
                    .is_none_or(|t| now.duration_since(t) >= DERIVED_EVENT_RATE_LIMIT)
            {
                uc2_node::obs_event!(
                    Info,
                    "snapshot_published",
                    node = id as u64,
                    pos = snapshot_pos,
                );
                last_snapshot_emit = Some(now);
            }
            last_snapshot_pos = snapshot_pos;

            // M11 (Task 5): free_disk_bytes — daemon-loop-only writer, no
            // syscall added to any of the four polling agents. On a probe
            // failure, leave the cnc field at its last value (never write a
            // stale-but-plausible 0) and rate-limit the warning the same way
            // seal_failures/nak_storm do above.
            match free_disk_bytes(&instance_dir) {
                Some(bytes) => obs.cnc.store_free_disk_bytes(bytes),
                None => {
                    if last_statvfs_warn_emit
                        .is_none_or(|t| now.duration_since(t) >= DERIVED_EVENT_RATE_LIMIT)
                    {
                        eprintln!(
                            "uc2-node: statvfs({}) failed: {}",
                            instance_dir.display(),
                            std::io::Error::last_os_error()
                        );
                        last_statvfs_warn_emit = Some(now);
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    println!("uc2-node: signalled, draining");
    if let Some(srv) = srv {
        // Scrapes must not race teardown: stop the HTTP thread before the
        // agents it reads through start winding down.
        srv.stop();
    }
    match node.stop_draining(Duration::from_secs(args.drain_timeout_secs)) {
        DrainOutcome::Drained => println!("uc2-node: drained, stopped cleanly"),
        DrainOutcome::DeadlineExpired { append, durable } => eprintln!(
            "uc2-node: drain deadline expired with {} bytes unrecorded \
             (append {append}, durable {durable}); stopped anyway — the restarted \
             node will re-fetch them",
            append.saturating_sub(durable)
        ),
    }
    ExitCode::SUCCESS
}

/// M11 (Task 5): free bytes on the filesystem backing `path`, via `statvfs`
/// (`f_bavail * f_frsize` — bytes an unprivileged process could still write,
/// not the raw free-block count). `None` on a probe failure (bad path,
/// syscall error) — the caller leaves the cnc field at its last value rather
/// than writing a stale-but-plausible 0. Same `CString`/`statfs`-family idiom
/// as `preflight::fs_kind`, just the `statvfs` sibling call.
#[allow(
    clippy::unnecessary_cast,
    reason = "libc::statvfs's f_bavail/f_frsize field types vary by target (not \
              always u64) — the cast is a portability normalization, a no-op only \
              on this specific build target; 1.89 clippy flags it, 1.96 does not"
)]
fn free_disk_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut buf: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path; `buf` is a zeroed statvfs
    // this call owns for the duration of the call.
    if unsafe { libc::statvfs(c.as_ptr(), &mut buf) } != 0 {
        return None;
    }
    Some(buf.f_bavail as u64 * buf.f_frsize as u64)
}
