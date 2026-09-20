// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `RegisterSm` behind the diff-replay app-binary contract — the harness's
//! own end-to-end fixture. Not a pattern to copy; see examples/kv for that.
//!
//! `<bin> replay --corpus DIR --out TRACE.json [--from-genesis] [--double]`
//! and `<bin> project --artifact FILE --position P`, per the README's CLI
//! contract for app binaries.

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
    }
}
