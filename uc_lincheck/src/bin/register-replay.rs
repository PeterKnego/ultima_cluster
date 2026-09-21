// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `RegisterSm` behind the diff-replay app-binary contract — the harness's
//! own end-to-end fixture. Not a pattern to copy; see examples/kv for that.
//!
//! `<bin> replay --corpus DIR --out TRACE.json [--from-genesis] [--double]`,
//! `<bin> project --artifact FILE --position P`, and
//! `<bin> serve --instance-dir D --app-id A [--double] [--durable]` (the
//! serve form of the app-binary contract: attach, supervise, stop on
//! SIGTERM), per the README's CLI contract for app binaries.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uc_lincheck::register::{DoublingRegisterSm, RegisterSm};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    Replay {
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        from_genesis: bool,
        /// Test knob: double every written value (a "v2" with changed semantics).
        #[arg(long)]
        double: bool,
    },
    Project {
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        position: u64,
    },
    /// The serve form of the diff-replay CLI contract: attach to a running
    /// node and apply until SIGTERM. `pin-verify` runs this for both eras.
    Serve {
        #[arg(long)]
        instance_dir: PathBuf,
        #[arg(long, default_value = "register")]
        app_id: String,
        /// The "v2" build: `Write(v)` stores `2·v` (`DoublingRegisterSm`).
        #[arg(long)]
        double: bool,
        /// Persist `(value, last_applied)` in the instance dir — the durable
        /// state-machine shape (spec §2.3's third path).
        #[arg(long)]
        durable: bool,
    },
}

fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        Sub::Replay {
            corpus,
            out,
            from_genesis,
            double,
        } => {
            if double {
                uc_diffreplay::drive::run_replay_cli(
                    DoublingRegisterSm::default(),
                    &corpus,
                    &out,
                    from_genesis,
                )
            } else {
                uc_diffreplay::drive::run_replay_cli(
                    RegisterSm::default(),
                    &corpus,
                    &out,
                    from_genesis,
                )
            }
        }
        Sub::Project { artifact, position } => {
            print!(
                "{}",
                uc_diffreplay::drive::project_artifact(RegisterSm::default(), &artifact, position)?
            );
            Ok(())
        }
        Sub::Serve {
            instance_dir,
            app_id,
            double,
            durable,
        } => {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
                signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
            }
            let cfg = || uc_service::ServiceConfig::new(instance_dir.clone(), app_id.clone());
            // Four combinations, one `Service` type each; `supervise` is
            // generic over the state machine so the loop is written once.
            match (double, durable) {
                (false, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), RegisterSm::default())
                        .start_with_snapshots()?,
                    &stop,
                ),
                (true, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), DoublingRegisterSm::default())
                        .start_with_snapshots()?,
                    &stop,
                ),
                (false, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(RegisterSm::default(), &instance_dir)?,
                    )
                    .start_with_snapshots()?,
                    &stop,
                ),
                (true, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(
                            DoublingRegisterSm::default(),
                            &instance_dir,
                        )?,
                    )
                    .start_with_snapshots()?,
                    &stop,
                ),
            }
        }
    }
}

/// The template every service binary follows (`docs/how-to/write-a-service-binary.md`):
/// poll `is_alive`, exit 1 if the apply agent fail-stopped, stop cleanly on
/// the signal flag. `attach` errors propagate through `main`'s `?`, so a
/// refused attach exits 1 with `Error: <ServiceError>` on stderr — which is
/// what `pin-verify`'s refusal arm reads.
fn supervise<S: uc_service::RawStateMachine>(
    service: uc_service::Service<S>,
    stop: &std::sync::atomic::AtomicBool,
) -> anyhow::Result<()> {
    eprintln!(
        "register-replay: attached row={} pinned={:?}",
        service.service_id(),
        service.pinned()
    );
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        if !service.is_alive() {
            anyhow::bail!("apply agent fail-stopped");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    service.stop();
    Ok(())
}
