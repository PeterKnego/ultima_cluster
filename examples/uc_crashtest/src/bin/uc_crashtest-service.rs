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
//! `--timer` / `--mixed-register` (time-and-timers T11) are the two halves of
//! a mixed register/timer node: the row-1 `Timed<TimerSm>` and the row-0
//! register on the shared `MixedCmd` command wire. See each flag's doc, and
//! the `uc_lincheck::timer` module doc for why one wire is required.
//!
//! Sync, like the node bin — no tokio.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_lincheck::timer::{MixedRegisterSm, TimerSm};
use uc_service::{
    RawStateMachine, Service, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned,
    StateMachine, Tagged, Timed,
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
    /// FSM identity (Task 5, spec §3.3): absent attaches as bare `RegisterSm`
    /// (row declared under `RegisterSm::NAME`, `"register"`); `--tagged N`
    /// (`N` in `0..8`) wraps it in `Tagged<N, RegisterSm>` (row declared
    /// under `"fsmN"`) — a second FSM on a two-FSM node, Task 9's harness row.
    #[arg(long)]
    tagged: Option<u8>,
    /// Time-and-timers (T11): attach `Timed<TimerSm>` — the timer FSM behind
    /// the exactly-once delivery wrapper — instead of the register.
    /// `TimerSm::NAME` is `"timer"`, so the node must declare that name
    /// (`--services register,timer`). Mutually exclusive with `--tagged` /
    /// `--sessioned`.
    #[arg(long, default_value_t = false)]
    timer: bool,
    /// Time-and-timers (T11): attach `MixedRegisterSm` — the register
    /// transition reached through the shared `MixedCmd` command wire — under
    /// the register's own name. Required for row 0 of a mixed
    /// register/timer node: UC's log is a broadcast, so row 0 is handed the
    /// timer row's frames too and a bare `RegisterSm` would mis-decode them
    /// (see the `uc_lincheck::timer` module doc).
    #[arg(long, default_value_t = false)]
    mixed_register: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat");
        std::thread::sleep(Duration::from_millis(20));
    }

    let instance_dir = args.instance_dir.clone();
    let cfg = ServiceConfig::new(args.instance_dir, args.app_id);

    // Time-and-timers T11: the two mixed-cluster arms, checked before the
    // `(tagged, sessioned)` matrix below — neither composes with it.
    anyhow::ensure!(
        !(args.timer && args.mixed_register),
        "--timer and --mixed-register are mutually exclusive"
    );
    if args.timer || args.mixed_register {
        anyhow::ensure!(
            args.tagged.is_none() && !args.sessioned,
            "--timer/--mixed-register do not compose with --tagged/--sessioned"
        );
        if args.timer {
            // `Timed<S>` is a RAW state machine (no `StateMachine` impl), so
            // its name comes off `RawStateMachine`; it forwards `TimerSm`'s.
            let svc = ServiceBuilder::new(cfg, Timed::new(TimerSm::default())).start()?;
            println!(
                "service {:?} attached at {}",
                <Timed<TimerSm> as RawStateMachine>::NAME,
                instance_dir.display()
            );
            return supervise(svc);
        }
        let svc = ServiceBuilder::new(cfg, MixedRegisterSm::default()).start()?;
        println!(
            "service {:?} attached at {}",
            <MixedRegisterSm as StateMachine>::NAME,
            instance_dir.display()
        );
        return supervise(svc);
    }
    // Every arm hands the service to `supervise`, which holds it alive until
    // it fail-stops; the branch has to happen here because the builder is
    // generic over the state machine and `Service<S>` carries `S` in its
    // type (and `Tagged<N, _>`'s `N` is a const generic, not a runtime
    // value — the `--tagged` arm dispatches through a `match` over `0..8`).
    match (args.tagged, args.sessioned) {
        (None, false) => {
            let svc = ServiceBuilder::new(cfg, RegisterSm::default()).start()?;
            println!(
                "service {:?} attached at {}",
                <RegisterSm as StateMachine>::NAME,
                instance_dir.display()
            );
            supervise(svc)
        }
        (None, true) => {
            let svc = ServiceBuilder::new(
                cfg,
                Sessioned::new(RegisterSm::default(), SessionConfig::default()),
            )
            .start()?;
            println!(
                "service {:?} attached at {}",
                <RegisterSm as StateMachine>::NAME,
                instance_dir.display()
            );
            supervise(svc)
        }
        (Some(row), sessioned) => {
            macro_rules! tagged_arm {
                ($n:literal) => {
                    if sessioned {
                        let svc = ServiceBuilder::new(
                            cfg,
                            Sessioned::new(
                                Tagged::<$n, RegisterSm>::default(),
                                SessionConfig::default(),
                            ),
                        )
                        .start()?;
                        println!(
                            "service {:?} attached at {}",
                            <Tagged<$n, RegisterSm> as StateMachine>::NAME,
                            instance_dir.display()
                        );
                        supervise(svc)
                    } else {
                        let svc = ServiceBuilder::new(cfg, Tagged::<$n, RegisterSm>::default())
                            .start()?;
                        println!(
                            "service {:?} attached at {}",
                            <Tagged<$n, RegisterSm> as StateMachine>::NAME,
                            instance_dir.display()
                        );
                        supervise(svc)
                    }
                };
            }
            match row {
                0 => tagged_arm!(0),
                1 => tagged_arm!(1),
                2 => tagged_arm!(2),
                3 => tagged_arm!(3),
                4 => tagged_arm!(4),
                5 => tagged_arm!(5),
                6 => tagged_arm!(6),
                7 => tagged_arm!(7),
                _ => anyhow::bail!("--tagged must be 0..8, got {row}"),
            }
        }
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
