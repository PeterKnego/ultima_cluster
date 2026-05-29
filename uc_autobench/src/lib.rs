//! uc_autobench — Claude-Code-driven autoresearch loop helpers.
//!
//! See `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
//! for the design and `program.md` for the loop the agent executes.
//!
//! This crate exposes only fitness binaries (`shmem-microbench`, `shmem-e2e`),
//! the consolidation helper (`run-iter`), and the frozen `ring_torture`
//! conformance suite. The orchestration loop itself lives in `program.md`
//! and is executed directly by Claude Code.
