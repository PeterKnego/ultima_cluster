// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Service-only reference binary for the v2 multi-process hard-crash test
//! (M5 Task 14, spec §8 L3). Waits for the node's `cnc2.dat` to appear (up
//! to 30s), attaches, runs the non-persisting `RegisterSm`, then parks until
//! killed (the test SIGKILLs it mid-apply).
//!
//! `--sessioned` (M12a Task 11) wraps the register in `Sessioned` so the
//! service can sit behind a gateway edge running with its session envelope
//! on — see the flag's own doc for why the two switches must agree.
//!
//! Sync, like the node bin — no tokio.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use uc2_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};
use uc_lincheck::register::RegisterSm;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-crashtest")]
    app_id: String,
    /// M12a Task 11: wrap the register in `Sessioned` (the exactly-once
    /// dedup layer a gateway's session envelope feeds). MUST match the
    /// edge's `session_envelope`: with the envelope on, every command
    /// carries the 16-byte `client_id ++ seq` header that only `Sessioned`
    /// knows how to strip, and with it off `Sessioned` would read the
    /// application's own first 16 bytes as one. Every replica in a cluster
    /// must agree on this flag AND on `SessionConfig` — it is part of the
    /// replicated contract (see `uc2_service::session`).
    #[arg(long, default_value_t = false)]
    sessioned: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat");
        std::thread::sleep(Duration::from_millis(20));
    }

    let cfg = ServiceConfig::new(args.instance_dir, args.app_id);
    // Two separate `start()` calls rather than one over a boxed SM: the
    // builder is generic over the state machine and `Service<S>` carries `S`
    // in its type, so the branch has to happen here. Both arms park forever
    // holding their service alive — dropping it would stop the apply agent.
    if args.sessioned {
        let _svc =
            ServiceBuilder::new(cfg, Sessioned::new(RegisterSm::default(), SessionConfig::default()))
                .start()?;
        park_forever();
    } else {
        let _svc = ServiceBuilder::new(cfg, RegisterSm::default()).start()?;
        park_forever();
    }
}

/// Park until the test SIGKILLs this process.
fn park_forever() -> ! {
    loop {
        std::thread::park();
    }
}
