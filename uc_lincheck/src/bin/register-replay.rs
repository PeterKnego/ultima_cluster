// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `RegisterSm` behind the diff-replay app-binary contract — the harness's
//! own end-to-end fixture. Not a pattern to copy; see examples/kv for that.
//!
//! `<bin> replay --corpus DIR --out TRACE.json [--from-genesis]
//! [--double|--double-cas] [--durable]`, `<bin> project --artifact FILE
//! --position P [--double|--double-cas] [--durable]`, and
//! `<bin> serve --instance-dir D --app-id A [--double|--double-cas]
//! [--durable]` (the serve form of the app-binary contract: attach,
//! supervise, stop on SIGTERM), per the README's CLI contract for app
//! binaries.
//!
//! Three builds of one FSM row: the plain register (`VERSION` 0), `--double`
//! (`DoublingRegisterSm`, 2) and `--double-cas` (`DoublingCasRegisterSm`, 3).
//! The two doubling knobs name different state machines and are mutually
//! exclusive — clap refuses them together by name.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uc_lincheck::register::{DoublingCasRegisterSm, DoublingRegisterSm, RegisterSm};

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
        /// Test knob: double every written value AND every `Cas`'s `new`
        /// (a "v3" whose change touches a history-PRESERVING command — see
        /// `DoublingCasRegisterSm`). Mutually exclusive with `--double`.
        #[arg(long, conflicts_with = "double")]
        double_cas: bool,
        /// Accepted and IGNORED, so ONE knob list can drive all three forms
        /// (`uc_diffreplay::pinverify::app_knobs`): `replay` drives the state
        /// machine in process from the corpus, and `Durable` only changes
        /// where a SERVING one keeps its state — it delegates `apply`,
        /// `freeze` and `project` to the inner machine — so the durable shape
        /// cannot change what this form computes.
        #[arg(long)]
        durable: bool,
    },
    Project {
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        position: u64,
        /// The "v2" build, as `replay` takes it. `DoublingRegisterSm`
        /// delegates `project` to the inner machine, so the text is the same
        /// either way; the form accepts the knob so the same list works for
        /// every verb.
        #[arg(long)]
        double: bool,
        /// The "v3" build, as `replay` takes it — `project` delegates too,
        /// for the same reason. Mutually exclusive with `--double`.
        #[arg(long, conflicts_with = "double")]
        double_cas: bool,
        /// Accepted and ignored — see `Replay`'s `--durable`.
        #[arg(long)]
        durable: bool,
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
        /// The "v3" build: `Write(v)` stores `2·v` AND `Cas{old, new}` stores
        /// `2·new` (`DoublingCasRegisterSm`). Mutually exclusive with
        /// `--double`.
        #[arg(long, conflicts_with = "double")]
        double_cas: bool,
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
            double_cas,
            durable: _,
        } => match (double, double_cas) {
            (_, true) => uc_diffreplay::drive::run_replay_cli(
                DoublingCasRegisterSm::default(),
                &corpus,
                &out,
                from_genesis,
            ),
            (true, false) => uc_diffreplay::drive::run_replay_cli(
                DoublingRegisterSm::default(),
                &corpus,
                &out,
                from_genesis,
            ),
            (false, false) => uc_diffreplay::drive::run_replay_cli(
                RegisterSm::default(),
                &corpus,
                &out,
                from_genesis,
            ),
        },
        Sub::Project {
            artifact,
            position,
            double,
            double_cas,
            durable: _,
        } => {
            let text = match (double, double_cas) {
                (_, true) => uc_diffreplay::drive::project_artifact(
                    DoublingCasRegisterSm::default(),
                    &artifact,
                    position,
                )?,
                (true, false) => uc_diffreplay::drive::project_artifact(
                    DoublingRegisterSm::default(),
                    &artifact,
                    position,
                )?,
                (false, false) => uc_diffreplay::drive::project_artifact(
                    RegisterSm::default(),
                    &artifact,
                    position,
                )?,
            };
            print!("{text}");
            Ok(())
        }
        Sub::Serve {
            instance_dir,
            app_id,
            double,
            double_cas,
            durable,
        } => {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
                signal_hook::flag::register(sig, std::sync::Arc::clone(&stop))?;
            }
            let cfg = || uc_service::ServiceConfig::new(instance_dir.clone(), app_id.clone());
            // Six combinations — three builds × in-memory/durable — one
            // `Service` type each; `supervise` is generic over the state
            // machine so the loop is written once. `--double` and
            // `--double-cas` are `conflicts_with`, so the `true, true` corner
            // is unreachable and `double_cas` is matched first.
            match (double_cas, double, durable) {
                (false, false, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), RegisterSm::default())
                        .start_with_snapshots()?,
                    &stop,
                ),
                (false, true, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), DoublingRegisterSm::default())
                        .start_with_snapshots()?,
                    &stop,
                ),
                (true, _, false) => supervise(
                    uc_service::ServiceBuilder::new(cfg(), DoublingCasRegisterSm::default())
                        .start_with_snapshots()?,
                    &stop,
                ),
                (false, false, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(RegisterSm::default(), &instance_dir)?,
                    )
                    .start_with_snapshots()?,
                    &stop,
                ),
                (false, true, true) => supervise(
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
                (true, _, true) => supervise(
                    uc_service::ServiceBuilder::new(
                        cfg(),
                        uc_lincheck::register::Durable::open(
                            DoublingCasRegisterSm::default(),
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
