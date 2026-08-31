// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Service-only reference binary for the v2 multi-process hard-crash test
//! (M5 Task 14, spec §8 L3). Waits for the node's `cnc2.dat` to appear (up
//! to 30s), attaches, runs the non-persisting `RegisterSm`, then supervises
//! it the way the `counter-service` template does: poll `is_alive` and exit
//! non-zero the moment the apply agent fail-stops (instance_id change, log
//! rewind), so a harness acting as the supervisor can wait for the exit and
//! respawn. A hard death is still the test's job (it SIGKILLs mid-apply).
//!
//! `--sessioned` (M12a Task 11) wraps the register in `Sessioned` so the
//! service can sit behind a gateway edge running with its session envelope
//! on — see the flag's own doc for why the two switches must agree.
//!
//! Sync, like the node bin — no tokio.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_service::{
    RawStateMachine, Service, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc_crashtest")]
    app_id: String,
    /// M12a Task 11: wrap the register in `Sessioned` (the exactly-once
    /// dedup layer a gateway's session envelope feeds). MUST match the
    /// edge's `session_envelope`: with the envelope on, every command
    /// carries the 16-byte `client_id ++ seq` header that only `Sessioned`
    /// knows how to strip, and with it off `Sessioned` would read the
    /// application's own first 16 bytes as one. Every replica in a cluster
    /// must agree on this flag AND on `SessionConfig` — it is part of the
    /// replicated contract (see `uc_service::session`).
    #[arg(long, default_value_t = false)]
    sessioned: bool,
    /// Which declared FSM slot this process is (see [services] ids).
    #[arg(long, default_value_t = 0)]
    service_id: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat");
        std::thread::sleep(Duration::from_millis(20));
    }

    let service_id = args.service_id;
    let instance_dir = args.instance_dir.clone();
    let cfg = ServiceConfig::new(args.instance_dir, args.app_id).service_id(service_id);
    // Two separate `start()` calls rather than one over a boxed SM: the
    // builder is generic over the state machine and `Service<S>` carries `S`
    // in its type, so the branch has to happen here. Both arms hand the
    // service to `supervise`, which holds it alive until it fail-stops.
    if args.sessioned {
        let svc = ServiceBuilder::new(
            cfg,
            Sessioned::new(RegisterSm::default(), SessionConfig::default()),
        )
        .start()?;
        println!(
            "service {} attached at {}",
            service_id,
            instance_dir.display()
        );
        supervise(svc)
    } else {
        let svc = ServiceBuilder::new(cfg, RegisterSm::default()).start()?;
        println!(
            "service {} attached at {}",
            service_id,
            instance_dir.display()
        );
        supervise(svc)
    }
}

/// The supervisor half of the v2.0 fail-stop contract, mirroring
/// `counter-service`: hold the service alive, and exit non-zero as soon as an
/// agent has died. This matters since M14a — the apply agent owns the
/// exclusive `service.<id>.lock`, so a process that parked on after its apply
/// thread panicked would sit there as a zombie that looks alive to the test
/// while its lock is already released, and a harness could only "wait for the
/// fail-stop" by racing that unwind (nightly 33184711408). Exiting makes the
/// fail-stop observable as a process exit, which is what a real supervisor
/// (systemd `Restart=on-failure`) keys on.
fn supervise<S: RawStateMachine>(svc: Service<S>) -> anyhow::Result<()> {
    while svc.is_alive() {
        std::thread::sleep(Duration::from_millis(20));
    }
    eprintln!("uc_crashtest-service: an agent fail-stopped; exiting for respawn");
    anyhow::bail!("service agent fail-stopped")
}
