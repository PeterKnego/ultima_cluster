// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12a gate: gateway (`Edge` + `RemoteEngine`) vs direct `Engine` throughput
//! (spec `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.6
//! item 5, §8 row 2).
//!
//! ```text
//! # local smoke — both arms in-process, one after the other (NOT the gate)
//! cargo run -p uc_gateway --release --example m12_gate -- \
//!     [--arm direct|gateway|both|fsms] [--secs 6] [--payload 64] [--inflight 4096] \
//!     [--envelope on|off] [--root DIR]
//!
//! # fleet roles — one process per role per host, driven by
//! # bench-infra/scripts/m12_fleet_gate.py (gate rows 2 and 3) and M14d's driver
//! m12_gate node          --id N --bind A --instance-dir D --members id@addr,… [--admission-kib K] \
//!                         [--services 0,1] [--fsm-lag lockstep|BYTES] [--purge-below-snapshot] \
//!                         [--journal-segment-bytes N]
//! m12_gate service       --instance-dir D [--envelope on|off] \
//!                         [--fsm count|spin|raw|fsm<N>] [--work-spin K] [--snapshot-interval-bytes N]
//! m12_gate edge          --instance-dir D --listen A --members id@gw_addr,… [--envelope on|off] [--inflight N]
//! m12_gate client-direct --instance-dir D --secs S [--payload P] [--inflight N] [--envelope on|off] \
//!                         [--fan-in] [--warmup-secs S] [--measure-secs S] [--timeline]
//! m12_gate client-remote --gateways A,… --secs S [--payload P] [--inflight N]
//! m12_gate check-fsms    --instance-dir D --app-id A [--mode linearizable|snapshot] \
//!                         [--expect N] [--expect-min N] [--settle-secs S]
//! ```
//!
//! The two client roles each print ONE machine-readable
//! `RESULT {"arm":…,"responses_per_sec":…,…}` line. That is the main thing
//! the fleet drivers parse, but not the only one: `client-direct
//! --timeline` also prints one `TL {"sec":…,"unix_ms":…,"responses":…}` line
//! per elapsed second (M14's row d reads its recovery clock off those), the
//! `check-fsms` role prints `FSMS` / `FSMS-OK {"count":…}`, and the `node`
//! role prints a `… stats: reports_unattested=N snap_refusals=(a,b)` line
//! whenever those counters change (M14's rows f and c read it). Everything
//! else these roles print is for a human reading the unit log.
//!
//! **`direct`** is `m5_gate`'s measuring client (`uc_node/examples/m5_gate.rs`)
//! copied verbatim: three in-process nodes + three typed [`CountSm`] services,
//! the client attached straight to the leader's instance dir over the public
//! `uc_client::Engine`.
//!
//! **`gateway`** boots a SEPARATE three-node cluster, one [`Edge`] per node,
//! and connects ONE `RemoteEngine` (split send/poll halves) to the leader's
//! edge over the framed TCP remote protocol. `--envelope on` (the default)
//! runs the service as `Sessioned<CountSm>` and the edge's
//! `session_envelope: true` — the edge
//! prepends the 16-byte `client_id ++ seq` header, the client sends the same
//! raw command bytes the direct arm does. `--envelope off` runs bare
//! [`CountSm`] with `session_envelope: false` (raw pass-through, at-least-once
//! on a re-send — see `Sessioned`'s and `RemoteConfig::resend_on_unknown`'s
//! docs). Both arms use the same [`CountSm`] state machine (`apply` never
//! inspects the bytes; the codec-share A/B is `m5_gate --raw-sm`'s job, not
//! this one's) and the same inflight cap, so the ratio isolates the edge +
//! remote-protocol hop.
//!
//! Both arms print the same [`ClientStats`] shape `m5_gate` uses
//! (responses/s, p50/p90/p99/max, sends/responses/lost/in-flight-at-end); with
//! `--arm both` (the default) the harness also prints `ratio gateway/direct`
//! for responses/s, p50, and p99.
//!
//! **This is a dev-box smoke number, never the gate.** The proposed bar (spec
//! §8: gateway throughput cost vs direct `Engine` ≥ 0.8× at equal inflight) is
//! fleet-only, per the dev-box-is-not-a-bench rule (`CLAUDE.md`). The banner
//! below prints on every run unless `UC2_GATE_FLEET=1` is set (it never is,
//! for a local run — this box has no fleet to be on).

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;

use uc_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc_gateway::{Edge, EdgeConfig, Member};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, ServicesConfig};
use uc_remote::{
    RemoteConfig, RemoteEngine, RemoteOutcome, RemotePollHalf, RemoteSendHalf,
    SubmitError as RemoteSubmitError,
};
use uc_service::{
    ApplyCtx, RawStateMachine, SESSION_HEADER_LEN, Service, ServiceBuilder, ServiceConfig,
    SessionConfig, Sessioned, SnapshotError, SnapshotPolicy, SnapshotStateMachine, StateMachine,
    TAG_FRESH,
};

// --------------------------------------------------------------- CLI shape

#[derive(Parser)]
#[command(
    name = "m12_gate",
    about = "uc2 M12a gate: gateway (Edge + RemoteEngine) vs direct Engine throughput (spec §8 row 2)"
)]
struct Cli {
    /// Fleet per-role subcommand. Omitted = the in-process local smoke below
    /// (`--arm both` and friends), byte-for-byte the pre-M12-fleet behaviour.
    #[command(subcommand)]
    role: Option<Role>,
    /// Which arm(s) to run.
    #[arg(long, value_enum, default_value_t = Arm::Both)]
    arm: Arm,
    #[arg(long, default_value_t = 6)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
    /// Gateway arm only: whether the edge runs the session envelope
    /// (`Sessioned<CountSm>` + `session_envelope: true`) or raw pass-through
    /// (`CountSm` + `session_envelope: false`).
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// Scratch root for the in-process clusters' instance dirs. Defaults to
    /// `$HOME/.cache/cargo-target/m12_gate` (never `/tmp` — see the guard
    /// below; `/tmp` is RAM-backed tmpfs on this box, CLAUDE.md "Local box").
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Arm {
    Direct,
    Gateway,
    Both,
    /// M14d: two FSMs — `CountSm` + `SpinCountSm(2000)` — fan-in load, then
    /// the divergence check; smoke only.
    Fsms,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Envelope {
    On,
    Off,
}

// ------------------------------------------------- fleet per-role subcommands
//
// The in-process arms above build BOTH clusters on loopback inside one
// process, which is what makes the local smoke's ratio uninterpretable (the
// gate doc's "Local smoke numbers" section says so at length: one 4-vCPU box,
// every role oversubscribed). The roles below are the fleet shape — one
// process per role per host, dedicated cores — driven by
// `bench-infra/scripts/m12_fleet_gate.py`. They deliberately reuse the SAME
// `CountSm`/`RawCountSm` pair, the same `Edge`, the same `RemoteEngine` and
// the same two measurement cores as the in-process arms, so the only thing
// that changes between smoke and fleet is where the processes run.

#[derive(Subcommand)]
enum Role {
    /// Cluster-member node (one process per host). Parks until killed.
    Node(NodeArgs),
    /// State-machine service attached to this host's node. Parks until killed.
    Service(ServiceArgs),
    /// Gateway edge over this host's node. Parks until killed.
    Edge(EdgeArgs),
    /// Measuring client over the LOCAL shmem `Engine` — must run on the
    /// leader's host (the direct arm of row 2, and row 3's load generator).
    ClientDirect(ClientDirectArgs),
    /// Measuring client over the framed remote protocol — runs on a host with
    /// no node of its own (the gateway arm of row 2).
    ClientRemote(ClientRemoteArgs),
    /// M14d: every declared FSM's count, checked for agreement (and against
    /// `--expect`/`--expect-min`) — exits 1 on divergence.
    CheckFsms(CheckFsmsArgs),
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: u32,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// Ingress admission window in KiB (`append - commit` backpressure gate).
    #[arg(long, default_value_t = 256)]
    admission_kib: u64,
    /// M14d, FSM identity: declared FSM names, comma-separated (`count,spin`)
    /// — row order is declaration order. Absent is a named refusal, like
    /// `node.toml`'s `[services] names` (there is no default set to fall
    /// back to).
    #[arg(long)]
    services: Option<String>,
    /// M14d: `lockstep` or a byte bound (`65536`, `16MiB`) — the string form
    /// of `[services].fsm_lag`, parsed by the same function.
    #[arg(long)]
    fsm_lag: Option<String>,
    /// M14d row f: `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` (as
    /// `m6_gate`'s node role) so a late joiner is genuinely below the floor.
    #[arg(long, default_value_t = false)]
    purge_below_snapshot: bool,
    /// M14d row f: journal segment size; small (16 KiB, M7's value) so purge
    /// actually drops prefixes inside a 60 s arm.
    #[arg(long, default_value_t = uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES)]
    journal_segment_bytes: u64,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// `on` wraps the state machine in `Sessioned<_>` (exactly-once over the
    /// remote hop). MUST match the edge's `--envelope` and the direct
    /// client's: with `on`, every submitted frame carries the 16-byte
    /// `client_id ++ seq` header, whoever put it there.
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// FSM identity (Task 5): which SM type to attach as — the name it
    /// presents at attach, so the node must have declared the SAME name
    /// (`--services`) or the attach is refused by name (`UnknownFsm`).
    /// `count` -> `CountSm`; `spin` -> `SpinCountSm` (row `"spin"`, paced by
    /// `--work-spin`); `raw` -> `RawCountSm` (row 3's raw-tier twin, no
    /// decode — paired with `--features uc_service/apply-profile` this is
    /// the codec-share A/B); `fsm<N>` (`N` in `0..8`) -> `Tagged<N, CountSm>`
    /// (row `"fsm<N>"` — harness rows for a multi-FSM node, spec §3.3).
    #[arg(long, default_value = "count")]
    fsm: String,
    /// M14d: `> 0` runs `SpinCountSm` with this many LCG rounds per apply —
    /// the deliberately slow FSM. Only valid with `--fsm spin`.
    #[arg(long, default_value_t = 0)]
    work_spin: u64,
    /// M14d row f: `SnapshotPolicy { interval_bytes }` on the service so the
    /// leader has artifacts to ship. `0` = no snapshots — `start()`, byte-for-
    /// byte every prior arm. `> 0` runs `start_with_snapshots()` (typed tier
    /// only: `CountSm`/`SpinCountSm` and their `Sessioned<_>` wrap are all
    /// `SnapshotStateMachine`); paired with `--raw-sm` it is refused by name.
    #[arg(long, default_value_t = 0)]
    snapshot_interval_bytes: u64,
}

#[derive(clap::Args)]
struct EdgeArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// TCP address this edge accepts remote clients on.
    #[arg(long)]
    listen: SocketAddr,
    /// Comma-separated `id@gateway_addr` map — every member's EDGE address
    /// (not its UDP `bind`), used for `REDIRECT`/`LEADER_CHANGED`.
    #[arg(long)]
    members: String,
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// Engine window and per-connection credit ceiling (kept equal, as the
    /// in-process arm does, so the two arms' inflight really is equal).
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
}

#[derive(clap::Args)]
struct ClientDirectArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
    /// Must match the service's. With `on` this client prepends the SAME
    /// 16-byte `client_id ++ seq` envelope the edge would have prepended, so
    /// the two arms submit byte-identical frames to an identical service.
    #[arg(long, value_enum, default_value_t = Envelope::On)]
    envelope: Envelope,
    /// M14d: submit to every declared FSM and count a completion only when
    /// every FSM answered (spec §15.3).
    #[arg(long, default_value_t = false)]
    fan_in: bool,
    /// M14d: steady-window start (seconds after t0). 0 = whole run.
    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,
    /// M14d: steady-window length. 0 = no window (`window_rps` reads 0).
    #[arg(long, default_value_t = 0)]
    measure_secs: u64,
    /// M14d row d: print `TL` per-second completion buckets.
    #[arg(long, default_value_t = false)]
    timeline: bool,
}

#[derive(clap::Args)]
struct ClientRemoteArgs {
    /// Comma-separated gateway addresses; the first is dialled first.
    #[arg(long)]
    gateways: String,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
}

#[derive(clap::Args)]
struct CheckFsmsArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// `linearizable` goes through the leader's quorum barrier (run on the
    /// leader host); `snapshot` reads each FSM's local state (any host).
    #[arg(long, value_enum, default_value_t = CheckMode::Linearizable)]
    mode: CheckMode,
    /// Every FSM's count must equal this exactly (rows a/b/e: the client's
    /// completed ops on this cluster generation).
    #[arg(long)]
    expect: Option<u64>,
    /// Every FSM's count must be at least this (rows d/f: ops the client saw
    /// complete; commands still in flight at a kill may add to it).
    #[arg(long)]
    expect_min: Option<u64>,
    /// Keep re-reading until the counts agree, up to this long — followers
    /// apply asynchronously and a check right after load may catch one
    /// mid-frame.
    #[arg(long, default_value_t = 10)]
    settle_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum CheckMode {
    Linearizable,
    Snapshot,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // A fleet role short-circuits everything below: no in-process cluster, no
    // smoke banner, no ratio — one process, one job.
    if let Some(role) = cli.role {
        return match role {
            Role::Node(a) => run_node_role(a),
            Role::Service(a) => run_service_role(a),
            Role::Edge(a) => run_edge_role(a),
            Role::ClientDirect(a) => run_client_direct_role(a),
            Role::ClientRemote(a) => run_client_remote_role(a),
            Role::CheckFsms(a) => run_check_fsms_role(a),
        };
    }

    let fleet = std::env::var("UC2_GATE_FLEET").as_deref() == Ok("1");
    if !fleet {
        println!("SMOKE (dev box) — not a gate number");
    }

    let root = cli.root.unwrap_or_else(default_root);
    assert!(
        !root.starts_with("/tmp"),
        "m12_gate: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root); // fresh root per run
    std::fs::create_dir_all(&root)?;

    if cli.arm == Arm::Fsms {
        return run_fsms_arm(&root, cli.secs, cli.payload, cli.inflight);
    }

    println!("arm                   : {:?}", cli.arm);
    println!("envelope (gateway arm): {:?}", cli.envelope);
    println!(
        "secs / payload / inflight: {} / {} / {}",
        cli.secs, cli.payload, cli.inflight
    );

    let direct_stats = if cli.arm != Arm::Gateway {
        Some(run_direct_arm(
            &root.join("direct"),
            cli.secs,
            cli.payload,
            cli.inflight,
        ))
    } else {
        None
    };

    let gateway_stats = if cli.arm != Arm::Direct {
        Some(run_gateway_arm(
            &root.join("gateway"),
            cli.secs,
            cli.payload,
            cli.inflight,
            cli.envelope == Envelope::On,
        ))
    } else {
        None
    };

    if let Some(s) = &direct_stats {
        print_report("direct (Engine)", s);
    }
    if let Some(s) = &gateway_stats {
        print_report("gateway (Edge + RemoteEngine)", s);
    }

    if let (Some(d), Some(g)) = (&direct_stats, &gateway_stats) {
        if !fleet {
            println!("SMOKE (dev box) — not a gate number");
        }
        println!("================ ratio gateway/direct (spec §8 row 2) =====================");
        println!(
            "responses/s ratio     : {:.3}  ({:.0} / {:.0})",
            g.responses_per_sec / d.responses_per_sec,
            g.responses_per_sec,
            d.responses_per_sec
        );
        println!(
            "p50 ratio (gw/direct) : {:.3}  ({:.3} ms / {:.3} ms)",
            g.p50_ms / d.p50_ms,
            g.p50_ms,
            d.p50_ms
        );
        println!(
            "p99 ratio (gw/direct) : {:.3}  ({:.3} ms / {:.3} ms)",
            g.p99_ms / d.p99_ms,
            g.p99_ms,
            d.p99_ms
        );
        println!(
            "(proposed bar, fleet-only: responses/s ratio >= 0.8 at equal inflight — \
             docs/benchmarks/uc2-m12-gate-2026-08-22.md)"
        );
        println!("============================================================================");
    }

    Ok(())
}

fn default_root() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".cache/cargo-target/m12_gate")
}

// -------------------------------------------------------------- CountSm

/// The gate's trivial state machine (copied from `m5_gate`): the cheapest
/// possible deterministic `apply` so the measurement isolates the transport
/// path rather than any user business logic. Command bytes are OPAQUE —
/// `apply` never inspects them — and the response is a single `u64`.
#[derive(Default)]
struct CountSm {
    count: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    const NAME: &'static str = "count";

    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: Vec<u8>) -> u64 {
        self.count += 1;
        self.last_applied = Some(ctx.position);
        self.count
    }

    fn query(&self, _q: ()) -> u64 {
        self.count
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// M14d row f: the typed counter can be shipped as a snapshot — 16 bytes,
/// `count ++ last_applied`, position-pinned on install (the `RegSm` shape in
/// `m6_gate.rs`). `Sessioned<CountSm>` inherits it (session.rs:274).
impl SnapshotStateMachine for CountSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let pos = self.last_applied.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        Ok((buf, pos))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        if buf.len() < 16 {
            return Err(SnapshotError::Codec(format!(
                "short snapshot: {} bytes",
                buf.len()
            )));
        }
        let count = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let pos = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if pos != position {
            return Err(SnapshotError::Codec(format!(
                "snapshot payload position {pos} != requested {position}"
            )));
        }
        self.count = count;
        self.last_applied = Some(position);
        Ok(position)
    }
}

/// M14d: `CountSm` with a fixed price per apply. `spin` rounds of an integer
/// LCG, seeded from the position, consumed through `black_box` — so the loop
/// cannot be optimised away and its result never reaches the response.
/// `K` changes cost, not output (spec §15.3); the test above pins that.
#[derive(Default)]
struct SpinCountSm {
    inner: CountSm,
    spin: u64,
}

impl SpinCountSm {
    fn with_spin(spin: u64) -> Self {
        Self {
            inner: CountSm::default(),
            spin,
        }
    }
}

impl StateMachine for SpinCountSm {
    const NAME: &'static str = "spin";

    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Vec<u8>) -> u64 {
        let mut x: u64 = ctx.position ^ 0x9E37_79B9_7F4A_7C15;
        for _ in 0..self.spin {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            x ^= x >> 29;
        }
        std::hint::black_box(x);
        // Both `StateMachine::apply` and the blanket `RawStateMachine::apply`
        // are in scope and CountSm implements both (the latter via the
        // blanket impl), so a bare `self.inner.apply(..)` is ambiguous
        // (E0034) — disambiguate with UFCS.
        StateMachine::apply(&mut self.inner, ctx, cmd)
    }

    fn query(&self, q: ()) -> u64 {
        StateMachine::query(&self.inner, q)
    }

    fn last_applied(&self) -> Option<u64> {
        StateMachine::last_applied(&self.inner)
    }
}

impl SnapshotStateMachine for SpinCountSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        self.inner.freeze()
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        CountSm::stream_snapshot(handle, dst)
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        self.inner.install_snapshot(position, src)
    }
}

// ------------------------------------------------------------ cluster boot

const NODE_BUFFER_BYTES: usize = 64 << 20;
/// See `m5_gate`'s identical constant doc: this door is enforced on
/// `try_submit`'s bincode-ENCODED bytes, not on `--payload` itself.
const NODE_MAX_PAYLOAD: usize = 512;
/// The in-process arms' admission window, unchanged (the fleet `node` role
/// takes `--admission-kib` instead).
const DEFAULT_ADMISSION_BYTES: u64 = 256 * 1024;
/// Log-buffer ring capacity for a FLEET node — the hot window the archive
/// drains, sized like `m5_gate`'s (256 MiB) rather than the in-process smoke's
/// 64 MiB, because a fleet host really does push M5-ladder rates through it.
const FLEET_BUFFER_BYTES: usize = 256 << 20;
const ELECTION_TIMEOUT_MIN_NS: u64 = 150_000_000;
const ELECTION_TIMEOUT_MAX_NS: u64 = 300_000_000;

/// A distinct, index-derived election seed per node so a clean boot elects
/// exactly one leader (m5_gate / lincheck_v2 precedent).
fn seed_for(id: u32) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[allow(clippy::too_many_arguments)]
fn node_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: &str,
    buffer_bytes: usize,
    admission_bytes: u64,
    services: ServicesConfig,
    purge: uc_node::PurgePolicy,
    journal_segment_bytes: u64,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id: app_id.to_string(),
        buffer_bytes,
        max_payload: NODE_MAX_PAYLOAD,
        admission_bytes,
        election_timeout_min_ns: ELECTION_TIMEOUT_MIN_NS,
        election_timeout_max_ns: ELECTION_TIMEOUT_MAX_NS,
        seed: seed_for(id),
        faults: FaultConfig::default(),
        purge,
        learners: Vec::new(),
        journal_segment_bytes,
        crypto: uc_node::CryptoConfig::Disabled,
        services,
    }
}

/// Wait for EXACTLY one serving leader across the live cluster; assert no
/// split-brain (m5_gate precedent). Returns its index.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].can_serve() && nodes[i].is_leader())
            .collect();
        assert!(
            serving.len() <= 1,
            "split-brain in smoke cluster: nodes {serving:?} all serve"
        );
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(
            Instant::now() < deadline,
            "no leader elected within {secs}s"
        );
        thread::yield_now();
    }
}

/// Bring up an `n`-member in-process cluster + one service per member,
/// generic over the state-machine tier so the direct arm's plain [`CountSm`]
/// and the gateway arm's [`CountSm`] / `Sessioned<CountSm>` share this one
/// boot path (mirrors `m5_gate`'s `run_all_generic` dispatch).
fn boot_cluster<S, F>(
    root: &std::path::Path,
    app_id: &str,
    n: usize,
    make_sm: F,
) -> (Vec<Node>, Vec<Service<S>>, Vec<PathBuf>)
where
    S: RawStateMachine,
    F: Fn() -> S,
{
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();

    let mut nodes = Vec::with_capacity(n);
    let mut services = Vec::with_capacity(n);
    let mut dirs = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&instance_dir).expect("instance dir");
        let cfg = node_config(
            i as u32,
            members.clone(),
            addr,
            instance_dir.clone(),
            app_id,
            NODE_BUFFER_BYTES,
            DEFAULT_ADMISSION_BYTES,
            ServicesConfig::single(S::NAME),
            uc_node::PurgePolicy::Disabled,
            uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc = ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id), make_sm())
            .start()
            .expect("service start");
        nodes.push(node);
        services.push(svc);
        dirs.push(instance_dir);
    }
    (nodes, services, dirs)
}

/// M14d: `boot_cluster` for two declared FSMs per node — id 0 `CountSm`,
/// id 1 `SpinCountSm(spin)`. Bounded lag at the node default.
#[allow(clippy::type_complexity)]
fn boot_cluster2(
    root: &std::path::Path,
    app_id: &str,
    n: usize,
    spin: u64,
) -> (
    Vec<Node>,
    Vec<Service<CountSm>>,
    Vec<Service<SpinCountSm>>,
    Vec<PathBuf>,
) {
    let socks: Vec<UdpSocket> = (0..n)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(u32, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u32, s.local_addr().unwrap()))
        .collect();
    let services = ServicesConfig::from_names(
        &[
            <CountSm as StateMachine>::NAME,
            <SpinCountSm as StateMachine>::NAME,
        ],
        None,
    )
    .expect("ids 0,1");
    let (mut nodes, mut s0, mut s1, mut dirs) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&instance_dir).expect("instance dir");
        let cfg = node_config(
            i as u32,
            members.clone(),
            addr,
            instance_dir.clone(),
            app_id,
            NODE_BUFFER_BYTES,
            DEFAULT_ADMISSION_BYTES,
            services,
            uc_node::PurgePolicy::Disabled,
            uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let a = ServiceBuilder::new(
            ServiceConfig::new(&instance_dir, app_id),
            CountSm::default(),
        )
        .start()
        .expect("service 0");
        let b = ServiceBuilder::new(
            ServiceConfig::new(&instance_dir, app_id),
            SpinCountSm::with_spin(spin),
        )
        .start()
        .expect("service 1");
        nodes.push(node);
        s0.push(a);
        s1.push(b);
        dirs.push(instance_dir);
    }
    (nodes, s0, s1, dirs)
}

fn run_fsms_arm(
    root: &std::path::Path,
    secs: u64,
    payload: usize,
    inflight: u64,
) -> anyhow::Result<()> {
    const APP_ID: &str = "uc2-m12-gate-fsms";
    let (nodes, s0, s1, dirs) = boot_cluster2(root, APP_ID, 3, 2_000);
    let leader = await_single_leader(&nodes, 30);
    println!("[fsms] leader elected: n{leader}");
    let opts = MeasureOpts {
        fan_in: true,
        warmup_secs: 1,
        measure_secs: secs.saturating_sub(2),
        timeline: false,
    };
    let stats = run_client_measurement(&dirs[leader], APP_ID, secs, payload, inflight, None, &opts);
    print_report("fsms (fan-in, 2 FSMs)", &stats);
    print_result_json("fsms", &stats, secs, payload, inflight);
    anyhow::ensure!(stats.lost == 0, "{} lost", stats.lost);
    run_check_fsms_role(CheckFsmsArgs {
        instance_dir: dirs[leader].clone(),
        app_id: APP_ID.into(),
        mode: CheckMode::Linearizable,
        expect: Some(stats.responses),
        expect_min: None,
        settle_secs: 10,
    })?;
    for n in nodes {
        n.stop();
    }
    for s in s0 {
        s.stop();
    }
    for s in s1 {
        s.stop();
    }
    Ok(())
}

/// A loopback TCP address nothing is listening on *right now* (bind-then-drop
/// reservation — `uc_gateway/tests/common/mod.rs`'s `free_tcp_addr` trick):
/// every edge needs the whole node-id -> gateway-address map before any of
/// them starts, so the addresses must be chosen up front.
fn free_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    l.local_addr().unwrap()
}

// ----------------------------------------------------------- stats + report

const SLOTS: usize = 1 << 20;
const SLOT_MASK: usize = SLOTS - 1;
const HIST_MAX_NS: u64 = 60_000_000_000;
const DRAIN_GRACE: Duration = Duration::from_secs(5);
const LEADER_WAIT: Duration = Duration::from_secs(30);
/// The M5 bar, reused only as an informational reference line in the printed
/// report — NOT a pass/fail gate for this harness. What this harness reports
/// is the gateway/direct RATIO; see spec §8 row 2 for the (fleet-only)
/// proposed bar on that ratio.
const RESPONSES_PER_SEC_BAR: f64 = 400_000.0;
const P50_MS_BAR: f64 = 1.0;

struct ClientStats {
    sends: u64,
    responses: u64,
    /// Direct arm with the session envelope on: responses NOT tagged
    /// `TAG_FRESH` (see the counter's comment in `run_client_measurement`).
    /// Always 0 for the gateway arm and for the in-process direct arm.
    not_fresh: u64,
    not_leader: u64,
    retried: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    lost: u64,
    elapsed: Duration,
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    responses_per_sec: f64,
    pass: bool,
    /// M14d: completions inside `[warmup_secs, warmup_secs + measure_secs)`.
    window_responses: u64,
    /// M14d: `window_responses / measure_secs`; 0.0 when `measure_secs == 0`.
    window_rps: f64,
    /// M14d: whether this run submitted via `try_submit_all` (fan-in).
    fan_in: bool,
    /// M14d: `Engine::declared()` / `Client::declared()` — the FSM bitmask
    /// this client attached to.
    declared: u64,
}

/// M14d: what the fleet driver varies per arm, beyond `secs/payload/inflight`.
#[derive(Clone, Copy, Debug, Default)]
struct MeasureOpts {
    /// `try_submit_all` (one response per declared FSM, counted as ONE
    /// completed op when every part arrived) instead of `try_submit`.
    fan_in: bool,
    /// Steady window: completions in `[warmup, warmup + measure)` seconds
    /// after t0. `measure == 0` disables the window (`window_rps` = 0).
    warmup_secs: u64,
    measure_secs: u64,
    /// Print one `TL {...}` line per elapsed second (row d's recovery clock).
    /// Independent of `measure_secs`: the timeline is served by a fixed
    /// per-second bucket array, not by the window's completion-timestamp Vec.
    timeline: bool,
}

/// Completions inside `[warmup, warmup + measure)` and their rate. Pure, so
/// the arithmetic is testable; `done_ns` are completion times since t0.
fn window_rate(done_ns: &[u64], warmup_secs: u64, measure_secs: u64) -> (u64, f64) {
    if measure_secs == 0 {
        return (0, 0.0);
    }
    let lo = warmup_secs * 1_000_000_000;
    let hi = lo + measure_secs * 1_000_000_000;
    let n = done_ns.iter().filter(|&&t| t >= lo && t < hi).count() as u64;
    (n, n as f64 / measure_secs as f64)
}

fn print_report(label: &str, s: &ClientStats) {
    println!();
    println!("================ uc2 M12a gate: {label} ================");
    println!("sends                 : {}", s.sends);
    println!("responses             : {}", s.responses);
    println!("not FRESH (envelope)  : {}", s.not_fresh);
    println!("not_leader / retried  : {} / {}", s.not_leader, s.retried);
    println!(
        "dup / overwritten     : {} / {}",
        s.duplicates, s.overwritten
    );
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!("lost (timeout/error)  : {}", s.lost);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("responses/s           : {:.0}", s.responses_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p90                   : {:.3} ms", s.p90_ms);
    println!("p95                   : {:.3} ms", s.p95_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    println!("max                   : {:.3} ms", s.max_ms);
    println!(
        "(m5 reference bar, informational only: responses/s >= {RESPONSES_PER_SEC_BAR:.0} && \
         p50 <= {P50_MS_BAR:.1} ms — this harness's own bar is the gateway/direct RATIO, printed below)"
    );
    println!(
        "{}",
        if s.pass {
            "reference bar: PASS"
        } else {
            "reference bar: FAIL (not this harness's gate)"
        }
    );
    println!("============================================================================");
}

// ----------------------------------------------------------- direct arm

fn run_direct_arm(root: &std::path::Path, secs: u64, payload: usize, inflight: u64) -> ClientStats {
    const APP_ID: &str = "uc2-m12-gate-direct";
    let (nodes, services, dirs) = boot_cluster(root, APP_ID, 3, CountSm::default);
    let leader = await_single_leader(&nodes, 30);
    println!("[direct] leader elected: n{leader}");

    let stats = run_client_measurement(
        &dirs[leader],
        APP_ID,
        secs,
        payload,
        inflight,
        None,
        &MeasureOpts::default(),
    );

    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
    stats
}

/// The measuring client's core loop, copied verbatim (module-for-module) from
/// `uc_node/examples/m5_gate.rs::run_client_measurement` — same public
/// `uc_client::Engine` path, same slot-array latency correlation, same
/// drain-inclusive clock and PASS computation (the PASS bar here is only an
/// informational reference line — see [`ClientStats`]'s doc).
fn run_client_measurement(
    instance_dir: &std::path::Path,
    app_id: &str,
    secs: u64,
    payload_len: usize,
    inflight_cap: u64,
    session_client_id: Option<u64>,
    opts: &MeasureOpts,
) -> ClientStats {
    let (send, mut poll) = Engine::attach(
        instance_dir,
        app_id,
        EngineConfig {
            max_inflight: inflight_cap as u32,
            request_timeout: Duration::from_secs(30),
            max_payload: Some(NODE_MAX_PAYLOAD),
            serving_gate: true,
            ..EngineConfig::default()
        },
    )
    .unwrap_or_else(|e| panic!("engine attach {instance_dir:?}: {e}"));

    let serve_deadline = Instant::now() + LEADER_WAIT;
    while !send.can_serve() {
        assert!(
            Instant::now() < serve_deadline,
            "no serving leader at this instance_dir within {LEADER_WAIT:?} — \
             is this host's node the elected leader?"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let raw_payload = vec![0xABu8; payload_len];
    let cmd_bytes = bincode::serde::encode_to_vec(&raw_payload, bincode::config::standard())
        .expect("encode fixed payload");

    let send_ns: Arc<Box<[AtomicU64]>> = Arc::new(
        (0..SLOTS)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let resolved = Arc::new(AtomicU64::new(0));
    let responses = Arc::new(AtomicU64::new(0));
    let not_leader = Arc::new(AtomicU64::new(0));
    let retried = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    // Anti-vacuity for the session envelope (fleet `client-direct` only). A
    // frame whose 16-byte header this client got WRONG does not fail: it comes
    // back tagged `TAG_EXPIRED`, having never reached the inner state machine
    // at all — i.e. a broken envelope would read as a FASTER direct arm. So
    // every response is checked to carry `TAG_FRESH`, and a nonzero count here
    // fails the role rather than being reported as throughput.
    let not_fresh = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));
    let hist: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(
        Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram"),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let t0_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // M14d: completion timestamps for the steady-window rate, and per-second
    // buckets for `--timeline` — both are the price of a Vec/bucket-array
    // touch per completion, so an unbounded arm (`MeasureOpts::default()`)
    // must not pay for either (ruling: keep the hot loop's body small).
    //
    // `done_ns` is gated on `measure_secs` ALONE. `--timeline` reads the
    // bucket array below, never this Vec (the only reader is `window_rate`,
    // at the end of this function), and the two flags are independent: M14's
    // row d runs a 45 s timeline arm and row f a 90 s untimed one, neither of
    // which reads `window_rps`. At ~1 M ops/s those arms would have grown
    // this Vec to 400-800 MB by DOUBLING inside the poll thread, and a
    // ~200 MB memcpy can land inside the 2 s recovery window row d exists to
    // measure. When the window IS wanted, reserve it up front instead of
    // growing: `measure_secs` seconds at 2 M completions/s (comfortably above
    // anything this harness has measured), capped at 64 Mi entries = 512 MiB
    // so a long `--measure-secs` cannot ask for an unbounded reservation.
    let done_ns: Option<Arc<Mutex<Vec<u64>>>> = (opts.measure_secs > 0).then(|| {
        let cap = (opts.measure_secs as usize)
            .saturating_mul(2_000_000)
            .min(64 << 20);
        Arc::new(Mutex::new(Vec::with_capacity(cap)))
    });
    let buckets: Option<Arc<Box<[AtomicU64]>>> = opts.timeline.then(|| {
        Arc::new(
            (0..secs + 40)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    });

    let matcher = thread::Builder::new()
        .name("m12-gate-poll".into())
        .spawn({
            let send_ns = Arc::clone(&send_ns);
            let resolved = Arc::clone(&resolved);
            let responses = Arc::clone(&responses);
            let not_leader = Arc::clone(&not_leader);
            let retried = Arc::clone(&retried);
            let lost = Arc::clone(&lost);
            let not_fresh = Arc::clone(&not_fresh);
            let last_response_ns = Arc::clone(&last_response_ns);
            let hist = Arc::clone(&hist);
            let stop = Arc::clone(&stop);
            let done_ns = done_ns.clone();
            let buckets = buckets.clone();
            let fan_in = opts.fan_in;
            move || {
                // Common bookkeeping for one completed op (`Response`, or a
                // fan-in `Responses` where every part arrived): latency,
                // count, and — only when the caller asked — the window
                // timestamp and the timeline bucket.
                let record = |user_data: u64| {
                    let idx = (user_data as usize) & SLOT_MASK;
                    let now = t0.elapsed().as_nanos() as u64;
                    let lat = now
                        .saturating_sub(send_ns[idx].load(Ordering::Acquire))
                        .min(HIST_MAX_NS);
                    let _ = hist.lock().unwrap().record(lat);
                    responses.fetch_add(1, Ordering::Relaxed);
                    last_response_ns.fetch_max(now, Ordering::Relaxed);
                    if let Some(d) = &done_ns {
                        d.lock().unwrap().push(now);
                    }
                    if let Some(b) = &buckets {
                        let sec = (now / 1_000_000_000) as usize;
                        if sec < b.len() {
                            b[sec].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                };
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            Outcome::Response(body) => {
                                if session_client_id.is_some() && body.first() != Some(&TAG_FRESH) {
                                    not_fresh.fetch_add(1, Ordering::Relaxed);
                                }
                                record(c.user_data);
                            }
                            Outcome::Responses(parts) => {
                                if fan_in {
                                    if session_client_id.is_some()
                                        && !parts
                                            .iter()
                                            .all(|(_, body)| body.first() == Some(&TAG_FRESH))
                                    {
                                        not_fresh.fetch_add(1, Ordering::Relaxed);
                                    }
                                    record(c.user_data);
                                } else {
                                    // A bench that did not ask for a fan-in
                                    // must not silently receive one.
                                    lost.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Outcome::NotLeader { .. } => {
                                not_leader.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::Retry => {
                                retried.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::TimedOut
                            | Outcome::InstanceRestart { .. }
                            | Outcome::BadService { .. } => {
                                lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    if n == 0 {
                        thread::sleep(Duration::from_micros(20));
                    }
                }
            }
        })
        .expect("spawn poll thread");

    // `session_client_id` is `Some` only for the fleet `client-direct` role
    // running against a `Sessioned<_>` service: it prepends the SAME 16-byte
    // `client_id ++ seq` header the EDGE prepends for the gateway arm, so both
    // arms hand the service byte-identical frames and the ratio is not
    // measuring one arm paying for an envelope the other skipped. `sent_idx`
    // is the seq — strictly increasing, so every frame classifies FRESH.
    // The in-process direct arm passes `None` (unchanged behaviour).
    let mut frame: Vec<u8> = Vec::with_capacity(SESSION_HEADER_LEN + cmd_bytes.len());
    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = (sent_idx as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        let submit_bytes: &[u8] = match session_client_id {
            Some(cid) => {
                frame.clear();
                frame.extend_from_slice(&cid.to_le_bytes());
                frame.extend_from_slice(&sent_idx.to_le_bytes());
                frame.extend_from_slice(&cmd_bytes);
                &frame
            }
            None => &cmd_bytes,
        };
        let r = if opts.fan_in {
            send.try_submit_all(sent_idx, submit_bytes)
        } else {
            send.try_submit(sent_idx, submit_bytes)
        };
        match r {
            Ok(()) => sent_idx += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(),
            Err(SubmitError::NotServing) => thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("try_submit: {e}"),
        }
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent_idx && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("poll thread panicked");

    let sends = sent_idx;
    let resp = responses.load(Ordering::Relaxed);
    let inflight_at_end = send.inflight();
    let elapsed = Duration::from_nanos(
        last_response_ns
            .load(Ordering::Relaxed)
            .max(send_window_end_ns),
    );
    let responses_per_sec = if elapsed.as_secs_f64() > 0.0 {
        resp as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let (p50_ms, p90_ms, p95_ms, p99_ms, max_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(h.value_at_quantile(0.50)),
            ms(h.value_at_quantile(0.90)),
            ms(h.value_at_quantile(0.95)),
            ms(h.value_at_quantile(0.99)),
            ms(h.max()),
        )
    };

    let pass = responses_per_sec >= RESPONSES_PER_SEC_BAR
        && p50_ms <= P50_MS_BAR
        && inflight_at_end == 0
        && lost.load(Ordering::Relaxed) == 0;

    let engine_stats = send.stats();
    let declared = send.declared();

    if let Some(b) = &buckets {
        for (i, c) in b.iter().enumerate() {
            let n = c.load(Ordering::Relaxed);
            let ms = t0_unix_ms + (i as u64) * 1000;
            println!("TL {{\"sec\":{i},\"unix_ms\":{ms},\"responses\":{n}}}");
        }
    }
    let (window_responses, window_rps) = match &done_ns {
        Some(d) => window_rate(&d.lock().unwrap(), opts.warmup_secs, opts.measure_secs),
        None => (0, 0.0),
    };

    ClientStats {
        sends,
        responses: resp,
        not_fresh: not_fresh.load(Ordering::Relaxed),
        not_leader: not_leader.load(Ordering::Relaxed),
        retried: retried.load(Ordering::Relaxed),
        duplicates: engine_stats.duplicates,
        overwritten: engine_stats.overwritten,
        inflight_at_end,
        lost: lost.load(Ordering::Relaxed),
        elapsed,
        p50_ms,
        p90_ms,
        p95_ms,
        p99_ms,
        max_ms,
        responses_per_sec,
        pass,
        window_responses,
        window_rps,
        fan_in: opts.fan_in,
        declared,
    }
}

// ----------------------------------------------------------- gateway arm

fn run_gateway_arm(
    root: &std::path::Path,
    secs: u64,
    payload: usize,
    inflight: u64,
    envelope_on: bool,
) -> ClientStats {
    if envelope_on {
        run_gateway_arm_generic(root, secs, payload, inflight, true, || {
            Sessioned::new(CountSm::default(), SessionConfig::default())
        })
    } else {
        run_gateway_arm_generic(root, secs, payload, inflight, false, CountSm::default)
    }
}

fn run_gateway_arm_generic<S, F>(
    root: &std::path::Path,
    secs: u64,
    payload: usize,
    inflight: u64,
    envelope_on: bool,
    make_sm: F,
) -> ClientStats
where
    S: RawStateMachine,
    F: Fn() -> S,
{
    const APP_ID: &str = "uc2-m12-gate-gateway";
    const N: usize = 3;

    let (nodes, services, dirs) = boot_cluster(root, APP_ID, N, make_sm);
    let leader = await_single_leader(&nodes, 30);
    println!("[gateway] leader elected: n{leader}");

    // One Edge per node, against the shared static node-id -> gateway map
    // (`uc_gateway/tests/failover.rs` precedent).
    let listen: Vec<SocketAddr> = (0..N).map(|_| free_tcp_addr()).collect();
    let members: Vec<Member> = listen
        .iter()
        .enumerate()
        .map(|(i, a)| Member {
            node_id: i as u32,
            gateway: a.to_string(),
        })
        .collect();

    let mut edges = Vec::with_capacity(N);
    for (i, dir) in dirs.iter().enumerate() {
        let edge = Edge::start(EdgeConfig {
            instance_dir: dir.clone(),
            app_id: APP_ID.into(),
            listen: listen[i],
            members: members.clone(),
            session_envelope: envelope_on,
            max_inflight: inflight as u32,
            per_conn_inflight: inflight as u32,
            status_interval: Duration::from_millis(200),
            request_timeout: Duration::from_secs(30),
            ..EdgeConfig::defaults()
        })
        .unwrap_or_else(|e| panic!("edge start n{i}: {e}"));
        edges.push(edge);
    }

    // ONE RemoteEngine connection, connected straight to the leader's edge (this
    // harness measures steady-state throughput, not failover — that is
    // `failover.rs` / `remote_lin.rs`'s job).
    let leader_addr = edges[leader].local_addr();
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        app_id: APP_ID.into(),
        members: vec![leader_addr.to_string()],
        client_id: None,
        max_inflight: inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .unwrap_or_else(|e| panic!("remote connect {leader_addr}: {e}"));

    let stats = run_remote_measurement(&send, &mut poll, secs, payload);
    print_remote_stats(&send);

    send.shutdown();
    for edge in edges {
        edge.stop();
    }
    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
    stats
}

/// `RemoteEngine`-side measurement core: ONE submitter loop calling
/// `try_submit` under the halves' own credit/inflight gating, with an inline
/// poll drain between submits — the same shape as
/// [`run_client_measurement`]'s `Engine` arm, correlating latency through the
/// `user_data` the completion carries. (The old shape — a `Ticket` per
/// request and a pool of waiter threads — was the client's structure, not the
/// cluster's, and is what M13b removed; `RemoteSendHalf` is `!Sync` by
/// design, so there is no second thread here to hand work to — the submitter
/// drains `poll` itself, which is legal because `poll` is nonblocking and
/// this arm drives exactly one connection.)
fn run_remote_measurement(
    send: &RemoteSendHalf,
    poll: &mut RemotePollHalf,
    secs: u64,
    payload_len: usize,
) -> ClientStats {
    let raw_payload = vec![0xABu8; payload_len];
    let cmd_bytes = bincode::serde::encode_to_vec(&raw_payload, bincode::config::standard())
        .expect("encode fixed payload");

    let send_ns: Arc<Box<[AtomicU64]>> = Arc::new(
        (0..SLOTS)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let responses = Arc::new(AtomicU64::new(0));
    let resolved = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));
    let mut hist = Histogram::<u64>::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram");

    let t0 = Instant::now();
    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = sent_idx;
        let slot = (idx as usize) & SLOT_MASK;
        send_ns[slot].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        match send.try_submit(idx, &cmd_bytes) {
            Ok(()) => sent_idx += 1,
            Err(RemoteSubmitError::Backpressure) => thread::yield_now(),
            Err(e) => panic!("try_submit: {e}"),
        }
        drain_remote(
            poll,
            &send_ns,
            &mut hist,
            t0,
            &responses,
            &resolved,
            &lost,
            &last_response_ns,
        );
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    // Nothing else drives `poll` here (single thread, no poll thread — see
    // the function doc): an empty drain means genuinely nothing is ready yet,
    // so park on the link's wait handle instead of bare-spinning a core for
    // up to `DRAIN_GRACE` on a degraded run (`RemotePollHalf::poll`'s own doc;
    // the same pattern as `run_client_measurement`'s poll thread and
    // `hop_bench/remote_load.rs`'s poller). The main submit loop above is
    // unaffected — `try_submit` + drain is its own pacing.
    let wait = poll.wait_handle();
    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent_idx && Instant::now() < drain_deadline {
        let n = drain_remote(
            poll,
            &send_ns,
            &mut hist,
            t0,
            &responses,
            &resolved,
            &lost,
            &last_response_ns,
        );
        if n == 0 {
            wait.park(Duration::from_micros(200));
        }
    }

    let sends = sent_idx;
    let resp = responses.load(Ordering::Relaxed);
    let resolved_total = resolved.load(Ordering::Relaxed);
    let inflight_at_end = sends.saturating_sub(resolved_total);
    let elapsed = Duration::from_nanos(
        last_response_ns
            .load(Ordering::Relaxed)
            .max(send_window_end_ns),
    );
    let responses_per_sec = if elapsed.as_secs_f64() > 0.0 {
        resp as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let (p50_ms, p90_ms, p95_ms, p99_ms, max_ms) = {
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(hist.value_at_quantile(0.50)),
            ms(hist.value_at_quantile(0.90)),
            ms(hist.value_at_quantile(0.95)),
            ms(hist.value_at_quantile(0.99)),
            ms(hist.max()),
        )
    };

    let lost_count = lost.load(Ordering::Relaxed);
    let pass = responses_per_sec >= RESPONSES_PER_SEC_BAR
        && p50_ms <= P50_MS_BAR
        && inflight_at_end == 0
        && lost_count == 0;

    let rs = send.stats();

    ClientStats {
        sends,
        responses: resp,
        not_fresh: 0, // the EDGE owns the envelope on this arm, not the client
        // Mapped from `RemoteStats` (there is no Engine-side "not_leader" /
        // "retried" concept over the remote protocol): see
        // `print_remote_stats` for the full breakdown.
        not_leader: rs.redirects,
        retried: rs.retries,
        duplicates: rs.resends,
        overwritten: 0, // no analog: the edge holds no broadcast ring to overwrite
        inflight_at_end,
        lost: lost_count,
        elapsed,
        p50_ms,
        p90_ms,
        p95_ms,
        p99_ms,
        max_ms,
        responses_per_sec,
        pass,
        // M14d: the fan-in / steady-window / declared-mask fields are a
        // direct-arm (`Engine`) concept — the gateway/remote arm neither
        // fans in nor exposes a declared-FSM bitmask over the wire.
        window_responses: 0,
        window_rps: 0.0,
        fan_in: false,
        declared: 0,
    }
}

/// One nonblocking drain of `poll`, folded into the counters
/// [`run_remote_measurement`] reports from, returning the number of
/// completions drained (0 tells the tail-drain loop it is safe to park).
/// `expired` responses (the edge's dedup window moved past this seq before
/// the answer arrived) carry no body and count as lost, same as
/// `Unknown`/`TimedOut`/`Closed`/`PayloadTooLarge`.
#[allow(clippy::too_many_arguments)]
fn drain_remote(
    poll: &mut RemotePollHalf,
    send_ns: &[AtomicU64],
    hist: &mut Histogram<u64>,
    t0: Instant,
    responses: &AtomicU64,
    resolved: &AtomicU64,
    lost: &AtomicU64,
    last_response_ns: &AtomicU64,
) -> usize {
    poll.poll(|c| {
        resolved.fetch_add(1, Ordering::Relaxed);
        match c.outcome {
            RemoteOutcome::Response { expired: false, .. } => {
                let now = t0.elapsed().as_nanos() as u64;
                let slot = (c.user_data as usize) & SLOT_MASK;
                let lat = now
                    .saturating_sub(send_ns[slot].load(Ordering::Acquire))
                    .min(HIST_MAX_NS);
                let _ = hist.record(lat);
                responses.fetch_add(1, Ordering::Relaxed);
                last_response_ns.fetch_max(now, Ordering::Relaxed);
            }
            _ => {
                lost.fetch_add(1, Ordering::Relaxed);
            }
        }
    })
}

/// The gateway arm's own stats the direct arm has no analog for — printed
/// separately rather than shoehorned into [`ClientStats`]'s field names.
fn print_remote_stats(send: &RemoteSendHalf) {
    let s = send.stats();
    println!("---------------------------- gateway/remote plane -------------------------");
    println!(
        "redirects {} | leader_changes {} | reconnects {} | resends {} | retries {} | \
         unknown {} | expired {} | refused_members {} | max_credits_seen {} | \
         socket_writes {} | frames_written {}",
        s.redirects,
        s.leader_changes,
        s.reconnects,
        s.resends,
        s.retries,
        s.unknown,
        s.expired,
        s.refused_members,
        s.max_credits_seen,
        s.socket_writes,
        s.frames_written
    );
    println!("============================================================================");
}

// ======================================================================
// Fleet roles (rows 2 and 3)
// ======================================================================
//
// Everything below runs ONE role in ONE process. The orchestrator is
// `bench-infra/scripts/m12_fleet_gate.py`; see that file's module doc for the
// topology and for why the two row-2 arms are measured against the SAME
// cluster generation (holding hardware AND leadership constant is exactly
// what the in-process smoke could not do).

/// Raw-tier twin of [`CountSm`], copied from `m5_gate`: sees the frame bytes,
/// decodes nothing. Same deterministic increment and a `u64` response either
/// way — but not the same bytes (8 LE here, bincode varint through the typed
/// tier). Which side of the [`RawStateMachine`] boundary does the (de)coding
/// is precisely what row 3's `apply-profile` A/B measures.
#[derive(Default)]
struct RawCountSm {
    count: u64,
    last_applied: Option<u64>,
}

impl RawStateMachine for RawCountSm {
    const NAME: &'static str = "raw";

    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
        self.count += 1;
        self.last_applied = Some(ctx.position);
        out.extend_from_slice(&self.count.to_le_bytes());
    }

    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.count.to_le_bytes());
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// `id@addr,...` — used for both the node role's UDP member list and the edge
/// role's node-id -> gateway-address map (the two are different addresses for
/// the same ids, which is why the edge takes its own flag).
fn parse_id_addr_list(s: &str) -> Vec<(u32, String)> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|part| {
            let (id, addr) = part
                .trim()
                .split_once('@')
                .unwrap_or_else(|| panic!("bad entry {part:?}, expected id@addr"));
            let id: u32 = id
                .parse()
                .unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            (id, addr.to_string())
        })
        .collect()
}

/// M14d, FSM identity: `--services` / `--fsm-lag` → the `ServicesConfig` a
/// node boots with. `--services` is REQUIRED (no default declared set) —
/// refusals name the flag, the way `node.toml`'s loader names the field
/// (`config_file.rs`'s `services.names` / `services.fsm_lag`).
fn services_from_flags(
    services: Option<&str>,
    fsm_lag: Option<&str>,
) -> anyhow::Result<ServicesConfig> {
    ServicesConfig::from_cli(services, fsm_lag).map_err(anyhow::Error::msg)
}

/// A per-process session identity for the direct arm. Random enough that two
/// successive `client-direct` runs against the same long-lived service do NOT
/// collide on `(client_id, seq)` — a collision would make the second run's
/// frames REPLAYED (served from the dedup cache, never reaching `apply`),
/// which would silently inflate the direct arm's throughput.
fn fresh_client_id() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn assert_durable_dir(dir: &std::path::Path) {
    assert!(
        !dir.starts_with("/tmp"),
        "instance_dir must be on a real filesystem (never /tmp — RAM tmpfs, and \
         fsync there is fiction); got {dir:?}"
    );
}

// ------------------------------------------------------------- node role

fn run_node_role(a: NodeArgs) -> anyhow::Result<()> {
    assert_durable_dir(&a.instance_dir);
    let members: Vec<(u32, SocketAddr)> = parse_id_addr_list(&a.members)
        .into_iter()
        .map(|(id, addr)| {
            (
                id,
                addr.parse()
                    .unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}")),
            )
        })
        .collect();
    let id = a.id;
    let services = services_from_flags(a.services.as_deref(), a.fsm_lag.as_deref())?;
    let purge = if a.purge_below_snapshot {
        uc_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 }
    } else {
        uc_node::PurgePolicy::Disabled
    };
    let cfg = node_config(
        id,
        members,
        a.bind,
        a.instance_dir,
        &a.app_id,
        FLEET_BUFFER_BYTES,
        a.admission_kib * 1024,
        services,
        purge,
        a.journal_segment_bytes,
    );
    let node = Node::start(cfg)?;
    println!(
        "m12_gate node {id} up (services={:#b}); parking (killed externally by the harness)",
        services.declared()
    );
    // Protocol 0.5.0 observability, same as `m5_gate`'s node role: the
    // attestation counter is process-local, so it cannot come out through the
    // cnc page. On a healthy throughput run it must stay 0. M14d adds the
    // snapshot-session refusal counters (below-floor joins the node had to
    // turn away).
    let mut last = (u64::MAX, (u64::MAX, u64::MAX, u64::MAX));
    loop {
        let now = (node.reports_unattested(), node.snapshot_session_refusals());
        if now != last {
            println!(
                "m12_gate node {id} stats: reports_unattested={} snap_refusals=({},{},{})",
                now.0, now.1.0, now.1.1, now.1.2
            );
            last = now;
        }
        thread::sleep(Duration::from_millis(500));
    }
}

// ---------------------------------------------------------- service role

/// M14d T2 fix round 1: `ServiceBuilder::start()` never reads
/// `cfg.snapshot_policy` — only `start_with_snapshots()` spawns the M6
/// builder thread that trips it (`uc_service/src/lib.rs:199-291`, the same
/// method `m6_gate.rs` uses). Shared by the four typed arms below (raw arms
/// keep plain `start()`; `--raw-sm` + `--snapshot-interval-bytes` is refused
/// by name before this is ever reached).
fn start_typed_svc<S: SnapshotStateMachine>(
    b: ServiceBuilder<S>,
    snapshots: bool,
) -> anyhow::Result<Service<S>> {
    if snapshots {
        Ok(b.start_with_snapshots()?)
    } else {
        Ok(b.start()?)
    }
}

/// FSM identity: which SM type `--fsm` selects. `Tagged(n)` is
/// `uc_service::Tagged<n, CountSm>`, `n` in `0..8` — the const generic can't
/// be a runtime value, so callers `match` over it.
enum FsmKind {
    Count,
    Spin,
    Raw,
    Tagged(u8),
}

fn parse_fsm(s: &str) -> anyhow::Result<FsmKind> {
    match s {
        "count" => Ok(FsmKind::Count),
        "spin" => Ok(FsmKind::Spin),
        "raw" => Ok(FsmKind::Raw),
        s => {
            let n: u8 = s
                .strip_prefix("fsm")
                .and_then(|d| d.parse().ok())
                .filter(|&n| n < 8)
                .ok_or_else(|| {
                    anyhow::anyhow!("--fsm must be count|spin|raw|fsm<N> (N in 0..8), got {s:?}")
                })?;
            Ok(FsmKind::Tagged(n))
        }
    }
}

/// Attach `sm` and park forever, printing `what`. One body shared by every
/// typed arm (`CountSm`, `SpinCountSm`, `Tagged<N, CountSm>`, and each
/// `Sessioned<_>` wrap) — they all diverge, so the `Service<_>` types never
/// need to unify at one call site (`m5_gate`'s service role does the same).
fn run_and_park<S: SnapshotStateMachine>(
    cfg: ServiceConfig,
    sm: S,
    snapshots: bool,
    what: String,
) -> anyhow::Result<()> {
    let _svc = start_typed_svc(ServiceBuilder::new(cfg, sm), snapshots)?;
    park_service(&what)
}

fn run_service_role(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for cnc2.dat at {cnc:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let kind = parse_fsm(&a.fsm)?;
    anyhow::ensure!(
        a.work_spin == 0 || matches!(kind, FsmKind::Spin),
        "--work-spin > 0 is only valid with --fsm spin"
    );
    anyhow::ensure!(
        !(matches!(kind, FsmKind::Raw) && a.snapshot_interval_bytes > 0),
        "--fsm raw and --snapshot-interval-bytes are exclusive: RawCountSm is not a SnapshotStateMachine"
    );
    let mut cfg = ServiceConfig::new(&a.instance_dir, &a.app_id);
    if a.snapshot_interval_bytes > 0 {
        cfg = cfg.snapshot_policy(SnapshotPolicy {
            interval_bytes: a.snapshot_interval_bytes,
        });
    }
    let envelope = a.envelope == Envelope::On;
    let snapshots = a.snapshot_interval_bytes > 0;
    let tag = format!(
        "fsm={} spin={} snap={}",
        a.fsm, a.work_spin, a.snapshot_interval_bytes
    );
    match (envelope, kind) {
        (true, FsmKind::Count) => run_and_park(
            cfg,
            Sessioned::new(CountSm::default(), SessionConfig::default()),
            snapshots,
            format!("Sessioned<CountSm> (typed tier, envelope on, {tag})"),
        ),
        (true, FsmKind::Spin) => run_and_park(
            cfg,
            Sessioned::new(
                SpinCountSm::with_spin(a.work_spin),
                SessionConfig::default(),
            ),
            snapshots,
            format!("Sessioned<SpinCountSm> (typed tier, envelope on, {tag})"),
        ),
        (false, FsmKind::Count) => run_and_park(
            cfg,
            CountSm::default(),
            snapshots,
            format!("CountSm (typed tier, envelope off, {tag})"),
        ),
        (false, FsmKind::Spin) => run_and_park(
            cfg,
            SpinCountSm::with_spin(a.work_spin),
            snapshots,
            format!("SpinCountSm (typed tier, envelope off, {tag})"),
        ),
        (true, FsmKind::Raw) => {
            let _svc = ServiceBuilder::new(
                cfg,
                Sessioned::new(RawCountSm::default(), SessionConfig::default()),
            )
            .start()?;
            park_service(&format!(
                "Sessioned<RawCountSm> (raw tier, envelope on, {tag})"
            ))
        }
        (false, FsmKind::Raw) => {
            let _svc = ServiceBuilder::new(cfg, RawCountSm::default()).start()?;
            park_service(&format!("RawCountSm (raw tier, envelope off, {tag})"))
        }
        (true, FsmKind::Tagged(n)) => {
            macro_rules! arm {
                ($n:literal) => {
                    run_and_park(
                        cfg,
                        Sessioned::new(
                            uc_service::Tagged::<$n, CountSm>::default(),
                            SessionConfig::default(),
                        ),
                        snapshots,
                        format!(
                            concat!(
                                "Sessioned<Tagged<",
                                stringify!($n),
                                ", CountSm>> (typed tier, envelope on, {})"
                            ),
                            tag
                        ),
                    )
                };
            }
            match n {
                0 => arm!(0),
                1 => arm!(1),
                2 => arm!(2),
                3 => arm!(3),
                4 => arm!(4),
                5 => arm!(5),
                6 => arm!(6),
                7 => arm!(7),
                _ => unreachable!("parse_fsm bounds N to 0..8"),
            }
        }
        (false, FsmKind::Tagged(n)) => {
            macro_rules! arm {
                ($n:literal) => {
                    run_and_park(
                        cfg,
                        uc_service::Tagged::<$n, CountSm>::default(),
                        snapshots,
                        format!(
                            concat!(
                                "Tagged<",
                                stringify!($n),
                                ", CountSm> (typed tier, envelope off, {})"
                            ),
                            tag
                        ),
                    )
                };
            }
            match n {
                0 => arm!(0),
                1 => arm!(1),
                2 => arm!(2),
                3 => arm!(3),
                4 => arm!(4),
                5 => arm!(5),
                6 => arm!(6),
                7 => arm!(7),
                _ => unreachable!("parse_fsm bounds N to 0..8"),
            }
        }
    }
}

fn park_service(what: &str) -> ! {
    println!("m12_gate service up ({what}); parking (killed externally by the harness)");
    loop {
        thread::park();
    }
}

// ------------------------------------------------------------- edge role

fn run_edge_role(a: EdgeArgs) -> anyhow::Result<()> {
    let members: Vec<Member> = parse_id_addr_list(&a.members)
        .into_iter()
        .map(|(node_id, gateway)| Member { node_id, gateway })
        .collect();
    anyhow::ensure!(!members.is_empty(), "--members must name at least one edge");
    let edge = Edge::start(EdgeConfig {
        instance_dir: a.instance_dir,
        app_id: a.app_id,
        listen: a.listen,
        members,
        session_envelope: a.envelope == Envelope::On,
        // Kept equal to each other and to the client's `--inflight`, exactly
        // as the in-process gateway arm does: the whole point of row 2 is a
        // ratio "at equal inflight".
        max_inflight: a.inflight as u32,
        per_conn_inflight: a.inflight as u32,
        status_interval: Duration::from_millis(200),
        request_timeout: Duration::from_secs(30),
        ..EdgeConfig::defaults()
    })
    .map_err(|e| anyhow::anyhow!("edge start: {e}"))?;
    println!(
        "m12_gate edge up on {}; parking (killed externally by the harness)",
        edge.local_addr()
    );
    loop {
        thread::park();
    }
}

// --------------------------------------------------------- client roles

/// One machine-readable line per measured arm. The orchestrator parses ONLY
/// this line (`RESULT ` + JSON); everything else a client role prints is for
/// a human reading the unit log.
fn print_result_json(arm: &str, s: &ClientStats, secs: u64, payload: usize, inflight: u64) {
    println!(
        "RESULT {{\"arm\":\"{arm}\",\"responses_per_sec\":{:.1},\"payload\":{payload},\
         \"inflight\":{inflight},\"secs\":{secs},\"sends\":{},\"responses\":{},\
         \"lost\":{},\"not_fresh\":{},\"inflight_at_end\":{},\"p50_ms\":{:.3},\
         \"p90_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},\"max_ms\":{:.3},\"elapsed_secs\":{:.3},\
         \"window_rps\":{:.1},\"window_responses\":{},\"fan_in\":{},\"declared\":{}}}",
        s.responses_per_sec,
        s.sends,
        s.responses,
        s.lost,
        s.not_fresh,
        s.inflight_at_end,
        s.p50_ms,
        s.p90_ms,
        s.p95_ms,
        s.p99_ms,
        s.max_ms,
        s.elapsed.as_secs_f64(),
        s.window_rps,
        s.window_responses,
        s.fan_in,
        s.declared,
    );
}

fn run_client_direct_role(a: ClientDirectArgs) -> anyhow::Result<()> {
    assert_durable_dir(&a.instance_dir);
    let envelope_on = a.envelope == Envelope::On;
    // `--payload` is a RAW length; the frame is its bincode encoding (length
    // varint + bytes) plus, with the envelope on, 16 more. The node's
    // `max_payload` door is enforced on the whole frame — refuse up front with
    // the arithmetic rather than letting `try_submit` panic mid-run.
    let encoded_len =
        bincode::serde::encode_to_vec(vec![0xABu8; a.payload], bincode::config::standard())
            .expect("encode fixed payload")
            .len()
            + if envelope_on { SESSION_HEADER_LEN } else { 0 };
    anyhow::ensure!(
        encoded_len <= NODE_MAX_PAYLOAD,
        "--payload {} encodes to {} B{} which exceeds the node's max_payload of {} B",
        a.payload,
        encoded_len,
        if envelope_on {
            " (incl. the 16-byte session envelope)"
        } else {
            ""
        },
        NODE_MAX_PAYLOAD
    );

    let session_client_id = envelope_on.then(fresh_client_id);
    println!(
        "m12_gate client-direct: {} s, payload {}, inflight {}, envelope {}",
        a.secs,
        a.payload,
        a.inflight,
        if envelope_on { "on" } else { "off" }
    );
    let opts = MeasureOpts {
        fan_in: a.fan_in,
        warmup_secs: a.warmup_secs,
        measure_secs: a.measure_secs,
        timeline: a.timeline,
    };
    let stats = run_client_measurement(
        &a.instance_dir,
        &a.app_id,
        a.secs,
        a.payload,
        a.inflight,
        session_client_id,
        &opts,
    );
    print_report("direct (Engine)", &stats);
    print_result_json("direct", &stats, a.secs, a.payload, a.inflight);
    anyhow::ensure!(
        stats.not_fresh == 0,
        "{} of {} responses were not TAG_FRESH — this client's session envelope \
         did not reach the inner state machine, so the measured rate is not a \
         rate for the work the gateway arm does",
        stats.not_fresh,
        stats.responses
    );
    Ok(())
}

fn run_client_remote_role(a: ClientRemoteArgs) -> anyhow::Result<()> {
    let gateways: Vec<String> = a
        .gateways
        .split(',')
        .map(|g| g.trim().to_string())
        .filter(|g| !g.is_empty())
        .collect();
    anyhow::ensure!(
        !gateways.is_empty(),
        "--gateways must name at least one edge address"
    );
    println!(
        "m12_gate client-remote: {} s, payload {}, inflight {}, gateways {:?}",
        a.secs, a.payload, a.inflight, gateways
    );
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        app_id: a.app_id,
        members: gateways,
        client_id: None,
        max_inflight: a.inflight as u32,
        request_timeout: Duration::from_secs(30),
        ..RemoteConfig::default()
    })
    .map_err(|e| anyhow::anyhow!("remote connect: {e}"))?;

    let stats = run_remote_measurement(&send, &mut poll, a.secs, a.payload);
    print_remote_stats(&send);
    send.shutdown();
    print_report("gateway (Edge + RemoteEngine)", &stats);
    print_result_json("gateway", &stats, a.secs, a.payload, a.inflight);
    Ok(())
}

/// M14d fix round 1: the settle loop's per-pass verdict, pulled out pure so
/// it's unit-testable without a cluster. `counts` empty (no successful pass
/// yet, e.g. every attempt hit a query error) is never a pass.
fn fsms_pass_ok(counts: &[(u8, u64)], expect: Option<u64>, expect_min: Option<u64>) -> bool {
    if counts.is_empty() {
        return false;
    }
    let agree = counts.windows(2).all(|w| w[0].1 == w[1].1);
    let n = counts[0].1;
    let vs_expect = match (expect, expect_min) {
        (Some(e), _) => n == e,
        (None, Some(m)) => n >= m,
        (None, None) => true,
    };
    agree && vs_expect
}

/// M14d row c: every declared FSM answers the same count, equal to (or at
/// least) what the client completed. Any mismatch is exit 1 — the row is a
/// consensus/apply defect, not a rate.
fn run_check_fsms_role(a: CheckFsmsArgs) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(a.settle_secs);
    // M14d fix round 1: a node that just restarted (rows d/f, right after a
    // kill or a join) may not have its cnc page ready yet — retry connect
    // the same as the query loop below rather than exiting 1 on the first
    // attempt (the blocking `Client`'s errors, including a bare `NotLeader`
    // during a leader change, propagate immediately with no retry of their
    // own: `uc_client/src/error.rs`).
    let client = loop {
        match uc_client::Client::connect(&a.instance_dir, &a.app_id) {
            Ok(c) => break c,
            Err(e) => {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "connect to {:?} failed after {}s: {e}",
                        a.instance_dir,
                        a.settle_secs,
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    };
    let declared = client.declared();
    let ids: Vec<u8> = (0..8u8).filter(|i| declared & (1u64 << i) != 0).collect();
    anyhow::ensure!(!ids.is_empty(), "no FSM declared on {:?}", a.instance_dir);
    let mut last: Vec<(u8, u64)> = Vec::new();
    let mut last_err: Option<String> = None;
    loop {
        let mut pass: Vec<(u8, u64)> = Vec::with_capacity(ids.len());
        let mut pass_err: Option<String> = None;
        for &id in &ids {
            let r: Result<u64, _> = match a.mode {
                CheckMode::Linearizable => client.query_linearizable_on(id, &()),
                CheckMode::Snapshot => client.query_snapshot_on(id, &()),
            };
            match r {
                Ok(c) => pass.push((id, c)),
                Err(e) => {
                    pass_err = Some(e.to_string());
                    break;
                }
            }
        }
        match pass_err {
            // A query error (e.g. `NotLeader` mid-election, right after a
            // kill or a join) is not a divergence — skip this pass's
            // agreement check and keep the last successful pass's counts.
            Some(e) => last_err = Some(e),
            None => {
                last = pass;
                if fsms_pass_ok(&last, a.expect, a.expect_min) {
                    break;
                }
            }
        }
        if Instant::now() >= deadline {
            for (id, c) in &last {
                println!("FSMS {{\"id\":{id},\"count\":{c}}}");
            }
            anyhow::bail!(
                "divergence after {}s: counts {last:?}, expect {:?}, expect_min {:?}, last_err {last_err:?}",
                a.settle_secs,
                a.expect,
                a.expect_min
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
    for (id, c) in &last {
        println!("FSMS {{\"id\":{id},\"count\":{c}}}");
    }
    println!(
        "FSMS-OK {{\"declared\":{declared},\"count\":{},\"mode\":\"{:?}\"}}",
        last[0].1, a.mode
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_node::FsmLag;

    #[test]
    fn services_from_flags_absent_is_refused() {
        let e = services_from_flags(None, None).unwrap_err().to_string();
        assert!(e.starts_with("--services is required"), "{e}");
    }

    #[test]
    fn services_from_flags_two_names_bounded_and_lockstep() {
        let s = services_from_flags(Some("count,spin"), Some("65536")).unwrap();
        assert_eq!(s.declared(), 0b11);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Bounded(65536));
        let s = services_from_flags(Some("count, spin"), Some("lockstep")).unwrap();
        assert_eq!(s.declared(), 0b11);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Lockstep);
    }

    #[test]
    fn services_from_flags_refuses_by_name() {
        let e = services_from_flags(Some("Count"), None)
            .unwrap_err()
            .to_string();
        assert!(e.contains("--services"), "{e}");
        let e = services_from_flags(Some("count,count"), None)
            .unwrap_err()
            .to_string();
        assert!(e.contains("--services"), "{e}");
        let e = services_from_flags(Some("count"), Some("bogus"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("--fsm-lag"), "{e}");
    }

    fn drive<
        S: StateMachine<Command = Vec<u8>, Response = u64, Query = (), QueryResponse = u64>,
    >(
        sm: &mut S,
    ) -> Vec<u64> {
        (1..=200u64)
            .map(|i| {
                sm.apply(
                    &mut ApplyCtx::new(i * 64, S::IDENTITY),
                    vec![(i & 0xff) as u8; 8],
                )
            })
            .collect()
    }

    #[test]
    fn spin_count_sm_is_count_sm_with_a_price_not_a_different_answer() {
        let mut plain = CountSm::default();
        let mut spin = SpinCountSm::with_spin(5_000);
        assert_eq!(drive(&mut plain), drive(&mut spin));
        // `StateMachine` and the blanket `RawStateMachine` are both in scope
        // and both implemented, so a bare `.query()`/`.last_applied()` on a
        // concrete SM is ambiguous (E0034) — disambiguate with UFCS.
        assert_eq!(
            StateMachine::query(&plain, ()),
            StateMachine::query(&spin, ())
        );
        assert_eq!(
            StateMachine::last_applied(&plain),
            StateMachine::last_applied(&spin)
        );
        // Two different K's, same answers: K prices the apply, it never
        // reaches the response (spec §15.3).
        let mut spin2 = SpinCountSm::with_spin(50);
        assert_eq!(drive(&mut spin2), drive(&mut SpinCountSm::with_spin(0)));
    }

    #[test]
    fn count_sm_snapshot_round_trips_and_pins_the_position() {
        let mut a = SpinCountSm::with_spin(10);
        drive(&mut a);
        let (blob, pos) = a.freeze().unwrap();
        assert_eq!(pos, 200 * 64);
        let mut b = SpinCountSm::with_spin(0);
        let got = b.install_snapshot(pos, &mut &blob[..]).unwrap();
        assert_eq!(got, pos);
        assert_eq!(StateMachine::query(&b, ()), 200);
        assert_eq!(StateMachine::last_applied(&b), Some(pos));
        let err = SpinCountSm::with_spin(0).install_snapshot(pos + 64, &mut &blob[..]);
        assert!(err.is_err(), "a mis-tagged artifact must be refused");
    }

    #[test]
    fn window_rate_counts_only_completions_inside_the_window() {
        // completions at 0.5s, 1.5s, 2.5s, 3.5s, 9.5s, 10.5s, 11.5s with a
        // 2 s warm-up and an 8 s window → the 2.5, 3.5, 9.5 completions.
        let ns = |s: f64| (s * 1e9) as u64;
        let done = [
            ns(0.5),
            ns(1.5),
            ns(2.5),
            ns(3.5),
            ns(9.5),
            ns(10.5),
            ns(11.5),
        ];
        let (n, rps) = window_rate(&done, 2, 8);
        assert_eq!(n, 3);
        assert!((rps - 3.0 / 8.0).abs() < 1e-9, "{rps}");
        assert_eq!(window_rate(&done, 0, 0), (0, 0.0));
    }

    #[test]
    fn fsms_pass_ok_agreement_and_expect() {
        // agree + exact match on `--expect` → pass.
        assert!(fsms_pass_ok(&[(0, 42), (1, 42)], Some(42), None));
        // agree but the exact `--expect` doesn't match → not a pass.
        assert!(!fsms_pass_ok(&[(0, 42), (1, 42)], Some(43), None));
        // `--expect-min` satisfied (agreeing counts at or above it) → pass.
        assert!(fsms_pass_ok(&[(0, 42), (1, 42)], None, Some(40)));
        assert!(fsms_pass_ok(&[(0, 42), (1, 42)], None, Some(42)));
        assert!(!fsms_pass_ok(&[(0, 42), (1, 42)], None, Some(43)));
        // disagreement is never a pass, expect/expect_min notwithstanding.
        assert!(!fsms_pass_ok(&[(0, 42), (1, 41)], None, None));
        assert!(!fsms_pass_ok(&[(0, 42), (1, 41)], Some(42), None));
        // no successful pass yet (every attempt hit a query error) → not a pass.
        assert!(!fsms_pass_ok(&[], None, None));
    }
}
