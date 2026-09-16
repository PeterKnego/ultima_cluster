//! The dogfood adjudication harness (wayfinder map #16, ticket #20).
//!
//! Maintainer-side test apparatus that decides whether a service binary
//! built by a clean-room *builder* is correct — by the repo's own checkers,
//! never by the builder's tests — treating the binary as a black box behind
//! `uc2-gateway` and the `uc_remote` protocol. Every row of
//! `docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md` § B2 and § B4, and the
//! paired register arm of § B3, is a subcommand of `uc2-adjudicate`.
//!
//! What is fixed and what is plugged in: the cluster rig (release
//! `uc2-node`/`uc2-gateway`/`uc2ctl` from a tarball's `bin/`), the workload
//! generators, the checkers and the oracles are fixed; the service binary's
//! path and an [`adapter::Adapter`] (its wire format, written from the
//! builder's wire-format page) are the parameters.
pub mod adapter;
pub mod diverge;
pub mod elle;
pub mod known_keys;
pub mod kv_v1;
pub mod kv_v2;
pub mod pinned;
pub mod rate;
pub mod register;
pub mod report;
pub mod rig;
pub mod wgl;
