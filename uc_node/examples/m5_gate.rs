// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M5 gate: client → apply → response measurement (spec §9 headline bar:
//! **≥400,000 msgs/s @ p50 ≤ 1.0 ms, fsync on, 3×c6id**).
//!
//! ```text
//! cargo run -p uc_node --release --example m5_gate -- node    --id N --bind A --members … --instance-dir D [--admission-kib K] [--crypto-key K --crypto-allowlist A]
//! cargo run -p uc_node --release --example m5_gate -- service --instance-dir D [--raw-sm]
//! cargo run -p uc_node --release --example m5_gate -- client  --instance-dir D --secs S [--payload 64] [--inflight 4096]
//! cargo run -p uc_node --release --example m5_gate -- all     --secs S [--crypto] [--raw-sm]   # local smoke, NOT the gate
//! ```
//!
//! **M12a (`--raw-sm`)** swaps the gate's `CountSm` (typed `StateMachine`,
//! bincode) for `RawCountSm` (`RawStateMachine`, bytes-in/bytes-out, no
//! decode) — same increment-and-echo logic, a `u64` response either way
//! (raw: 8 LE bytes; typed: bincode varint, so 1-9), only the codec differs. Paired with `--features uc_service/apply-profile`, running
//! `all` with and without `--raw-sm` is the two-tier codec-share A/B; see
//! `docs/notes/2026-08-22-codec-budget-spike.md` for the background numbers
//! and its "Post-fix smoke" section for this harness's readings.
//!
//! **M8 (`--crypto`)** runs the SAME measurement with wire crypto ON, so the
//! two arms differ in exactly one thing. `all --crypto` generates a fresh
//! X25519 identity per node plus one shared allowlist under the run root and
//! boots every node with `CryptoConfig::Enabled`; the fleet `node` role takes
//! `--crypto-key`/`--crypto-allowlist` instead (an operator's real key
//! material, not generated). The M8 gate doc
//! (`docs/benchmarks/uc2-m8-gate-2026-07-29.md`) pre-commits the decide rule:
//! encrypted `responses/s` ≥ 90% of the cleartext control. Nothing else about
//! the harness changes between arms — same seed, same payload, same inflight,
//! same duration.
//!
//! **`node`/`service`** are thin fleet-role wrappers (one process per host,
//! systemd-run-friendly) over the real M5 SDK stack: `Node::start` for the
//! consensus/log/IPC side, `ServiceBuilder` running a trivial [`CountSm`] for
//! the apply side. Both park forever once started — the harness owns their
//! lifecycle.
//!
//! **`client`** is the measuring role. It runs on the PUBLIC `uc_client`
//! `Engine` (`Engine::attach` -> `SendHalf`/`PollHalf`) — the measured path
//! IS the shipped path. A sender thread calls `try_submit` with `user_data`
//! set to its local send index and stamps a preallocated `Box<[AtomicU64]>`
//! slot array (`send_ns`, indexed `user_data & (SLOTS-1)`) before each send;
//! a poll thread drains `PollHalf::poll` and times each `Outcome::Response`
//! against that slot. Exactly-once resolution, the duplicate-response guard,
//! the serving-gate pause, and NOT_LEADER/RETRY bookkeeping are the engine's
//! job now — the old hand-rolled `owner` array and `poll_egress` matcher are
//! gone.
//!
//! Reported: responses/s (drain-inclusive clock — from the first send to
//! `max(last resolved response, send-window end)`, i.e. it includes the tail
//! drain AND cannot excise a dead tail where responses stopped arriving
//! mid-window), p50/p90/p99/max (an hdrhistogram of `now - send_ns[slot]` per
//! genuine first-arrival response), sends, responses, lost, in-flight-at-end.
//! **PASS iff `responses/s >= 400_000 && p50 <= 1.0 ms && in-flight-at-end ==
//! 0 && lost == 0`** (the run must have COMPLETED its work — see the pass
//! computation for why zero, strictly); otherwise `RESULT: FAIL (honest)` +
//! `exit(1)`.
//!
//! **`all`** boots 3 nodes + 3 services in-process (real file-backed shmem,
//! just not real separate OS processes) under a tempdir guarded OFF `/tmp`
//! (RAM-backed tmpfs — see `run_all`'s guard, m4_gate's precedent), elects a
//! leader, and runs the SAME client measurement core against it. This is
//! **SMOKE ONLY**: one dev box shares its core count across 3 nodes' worth of
//! polling agents (consensus/sender/receiver/archive ×3) + 3 services + the
//! client + its poll thread — nothing like the fleet's one-role-per-host budget.
//! The doc records its numbers as smoke, never as the gate.
//!
//! **Env caps** (sandbox safety, m1–m4 pattern): `UC2_M5_MAX_SECS`,
//! `UC2_M5_MAX_INFLIGHT` clip `--secs`/`--inflight` from ABOVE when set to a
//! nonzero value; unset (the fleet's mode) is a no-op.
//!
//! **Loud disclaimer (repeated in the gate doc and the task report):**
//! completing this binary is NOT the same as passing the M5 gate. The
//! ≥400k @ p50 ≤1 ms bar is only claimable after the real 3×c6id fleet run —
//! a separate, user-approved step (M1–M4 precedent). Until then this file
//! ships the measurement harness + a sandbox smoke number, nothing more.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;

use uc_client::{Engine, EngineConfig, Outcome, SubmitError};
use uc_consensus::election::NodeId;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ApplyCtx, RawStateMachine, ServiceBuilder, ServiceConfig, StateMachine};

// --------------------------------------------------------------- CLI shape

#[derive(Parser)]
#[command(
    name = "m5_gate",
    about = "UC v2 M5 headline gate: client -> apply -> response measurement (spec §9)"
)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Cluster-member node (fleet: one process per host).
    Node(NodeArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// The measuring client (runs on the public uc_client Engine — see module docs).
    Client(ClientArgs),
    /// Local smoke: 3 nodes + 3 services + 1 client, all in-process. NOT the gate.
    All(AllArgs),
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: NodeId,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "uc2-m5-gate")]
    app_id: String,
    /// Ingress admission window in KiB (`append - commit` backpressure gate).
    /// The fleet protocol sweeps 64/128/256.
    #[arg(long, default_value_t = 256)]
    admission_kib: u64,
    /// M8: this node's 32-byte raw X25519 private key file. Supplying it (with
    /// `--crypto-allowlist`) boots the node under `CryptoConfig::Enabled`;
    /// omitting both is the cleartext control arm.
    #[arg(long, requires = "crypto_allowlist")]
    crypto_key: Option<PathBuf>,
    /// M8: the `id base64(public)` allowlist naming every member (see the
    /// runbook's wire-crypto section for the format).
    #[arg(long, requires = "crypto_key")]
    crypto_allowlist: Option<PathBuf>,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-m5-gate")]
    app_id: String,
    /// Run the RAW-tier twin state machine (`RawCountSm`, bytes-in/bytes-out,
    /// no decode) instead of the typed `CountSm` — the apply-profile codec-
    /// share A/B (`docs/notes/2026-08-22-codec-budget-spike.md`).
    #[arg(long, default_value_t = false)]
    raw_sm: bool,
}

#[derive(clap::Args)]
struct ClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-m5-gate")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
}

#[derive(clap::Args)]
struct AllArgs {
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 4096)]
    inflight: u64,
    /// Scratch root for the 3 in-process nodes' instance dirs. Defaults to a
    /// fresh dir under `target/` (never `/tmp` — see the guard in `run_all`).
    #[arg(long)]
    root: Option<PathBuf>,
    /// M8: run the ENCRYPTED arm — generate a fresh X25519 identity per node
    /// plus one shared allowlist under the run root, and boot every node with
    /// `CryptoConfig::Enabled`. Everything else (seed, payload, inflight,
    /// duration) is identical to the cleartext control.
    #[arg(long, default_value_t = false)]
    crypto: bool,
    /// Run the RAW-tier twin state machine (`RawCountSm`, bytes-in/bytes-out,
    /// no decode) instead of the typed `CountSm` — the apply-profile codec-
    /// share A/B (`docs/notes/2026-08-22-codec-budget-spike.md`).
    #[arg(long, default_value_t = false)]
    raw_sm: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.role {
        Role::Node(a) => run_node(a),
        Role::Service(a) => run_service(a),
        Role::Client(a) => run_client_role(a),
        Role::All(a) => run_all(a),
    }
}

// ---------------------------------------------------------------- env caps

/// Sandbox safety cap (m1–m4 pattern): clip `requested` to the env var's value
/// when set and nonzero; unset/zero is a no-op (the fleet's mode).
fn env_cap(var: &str, requested: u64) -> u64 {
    match std::env::var(var).ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(cap) if cap > 0 => requested.min(cap),
        _ => requested,
    }
}

// -------------------------------------------------------------- CountSm

/// The gate's trivial state machine: the cheapest possible deterministic
/// `apply` (an increment) so the measurement isolates the SDK/apply/response
/// pipeline rather than any user business logic. Command payload bytes are
/// OPAQUE — `apply` never inspects them, only counts arrivals — and the
/// response is a single `u64`, about as small as a typed response gets.
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

/// Raw-tier twin of [`CountSm`]: sees the frame bytes, decodes nothing. Same
/// deterministic increment, and a `u64` response either way — but NOT the same
/// bytes: this tier writes it as 8 little-endian bytes, while `CountSm`'s
/// typed `u64` goes out bincode-varint-encoded (1-9 bytes for the same value).
/// The only difference from `CountSm` is which side of the `RawStateMachine`
/// boundary does the (de)coding, which is exactly what the `apply-profile`
/// A/B (`--raw-sm`) measures. See `docs/notes/2026-08-22-codec-budget-spike.md`.
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

// ------------------------------------------------------------ node role

/// A distinct, index-derived election seed so each node's randomized timeout
/// differs — a clean boot then elects exactly one leader (m4_gate /
/// lincheck_v2 precedent).
fn seed_for(id: NodeId) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let id: NodeId = id
                .parse()
                .unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            let addr: SocketAddr = addr
                .parse()
                .unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}"));
            (id, addr)
        })
        .collect()
}

/// Ring capacity for the node's log buffer — a "hot window" the archive
/// continuously drains, not a bound on total data over the test's duration.
/// 256 MiB comfortably covers any realistic append-durable lag at the target
/// rate. Fixed here (not a CLI flag — the brief only exposes `--admission-kib`
/// for the node role).
const NODE_BUFFER_BYTES: usize = 256 << 20;
/// Max single-frame payload the log buffer accepts. Bounded well under the
/// sender's default MTU (`uc_protocol::v2::datagram::MTU_DEFAULT` = 1408 B) —
/// a max-size frame must fit one datagram (`Sender::new`'s hard assert) — while
/// comfortably above the gate's default 64 B payload.
///
/// Gotcha for `--payload`: this door is enforced on `try_submit`'s bincode-
/// ENCODED `cmd_bytes` (`Vec<u8>` length-prefix + raw bytes), not on the
/// `--payload` value itself. A `--payload 512` command becomes 515 B on the
/// wire (the length varint's own 3 B) and is refused; `--payload 509` is the
/// largest value that lands exactly at this cap.
const NODE_MAX_PAYLOAD: usize = 512;
const ELECTION_TIMEOUT_MIN_NS: u64 = 150_000_000;
const ELECTION_TIMEOUT_MAX_NS: u64 = 300_000_000;

// ------------------------------------------------------------ M8 key material
//
// The encrypted arm needs a real X25519 identity per node and a real allowlist
// on disk — `CryptoConfig::Enabled` refuses to boot without them (spec §5's
// fail-closed rule). The `all` smoke generates its own; the fleet `node` role
// is handed an operator's.

/// Deterministic per-node private key. Reproducibility matters for a gate
/// harness — the two arms must differ ONLY in whether crypto is on — and these
/// bytes never leave the run root. Same fixture shape as
/// `uc_node/tests/crypto_cluster.rs`.
fn gate_private_key(i: usize) -> [u8; 32] {
    [0x60u8 + i as u8; 32]
}

/// Standard-alphabet base64 with padding, matching `uc_crypto::identity`'s
/// allowlist parser. Hand-rolled rather than adding a `base64` dependency to
/// `uc_node` for one fixture (the crypto_cluster test does the same).
fn b64_32(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Writes `n` key files plus one shared allowlist naming every node under
/// `dir`, and returns each node's `(key_path, allowlist_path)`. Public keys are
/// DERIVED through `uc_crypto::identity::Identity` rather than hardcoded, so a
/// wrong derivation shows up as a cluster that will not form, not as a silently
/// wrong fixture.
fn write_crypto_material(dir: &Path, n: usize) -> Vec<(PathBuf, PathBuf)> {
    std::fs::create_dir_all(dir).expect("create crypto material dir");
    let mut key_paths = Vec::with_capacity(n);
    let mut publics = Vec::with_capacity(n);
    for i in 0..n {
        let key_path = dir.join(format!("node{i}.key"));
        std::fs::write(&key_path, gate_private_key(i)).expect("write key file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 key file");
        }
        publics.push(
            uc_crypto::identity::Identity::load(&key_path)
                .expect("load identity")
                .public_bytes(),
        );
        key_paths.push(key_path);
    }
    let mut text = String::new();
    for (i, public) in publics.iter().enumerate() {
        text.push_str(&format!("{i} {}\n", b64_32(public)));
    }
    let allow_path = dir.join("allowlist");
    std::fs::write(&allow_path, text).expect("write allowlist");
    key_paths
        .into_iter()
        .map(|k| (k, allow_path.clone()))
        .collect()
}

fn node_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    admission_bytes: u64,
    crypto: uc_node::CryptoConfig,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id,
        buffer_bytes: NODE_BUFFER_BYTES,
        max_payload: NODE_MAX_PAYLOAD,
        admission_bytes,
        election_timeout_min_ns: ELECTION_TIMEOUT_MIN_NS,
        election_timeout_max_ns: ELECTION_TIMEOUT_MAX_NS,
        seed: seed_for(id),
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto,
        services: uc_node::ServicesConfig::default(),
    }
}

fn run_node(a: NodeArgs) -> anyhow::Result<()> {
    assert!(
        !a.instance_dir.starts_with("/tmp"),
        "node instance_dir must be on a real filesystem (never /tmp — RAM tmpfs); \
         the fleet protocol uses NVMe under /opt/bench"
    );
    let id = a.id;
    let members = parse_members(&a.members);
    // clap's `requires` makes these all-or-nothing, so the `Some`/`Some` and
    // `None`/`None` cases are the only reachable ones.
    let crypto = match (a.crypto_key, a.crypto_allowlist) {
        (Some(key_path), Some(allowlist_path)) => uc_node::CryptoConfig::Enabled {
            key_path,
            allowlist_path,
            rotation: uc_crypto::rotation::RotationPolicy::default(),
        },
        _ => uc_node::CryptoConfig::Disabled,
    };
    let cfg = node_config(
        a.id,
        members,
        a.bind,
        a.instance_dir,
        a.app_id,
        a.admission_kib * 1024,
        crypto,
    );
    let node = Node::start(cfg)?;
    println!("m5_gate node {id} up; parking (killed externally by the harness)");
    // Protocol 0.5.0 observability (see m6_gate): the attestation counter is
    // process-local, so it cannot come out through the cnc page. On a quiescent
    // throughput run this must stay 0 — a climb would mean honest reports are
    // being declined, which would also show up as a degraded commit rate.
    let mut last = u64::MAX;
    loop {
        let now = node.reports_unattested();
        if now != last {
            println!("m5_gate node {id} stats: reports_unattested={now}");
            last = now;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

// ---------------------------------------------------------- service role

fn run_service(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for cnc2.dat at {cnc:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let cfg = ServiceConfig::new(a.instance_dir, a.app_id);
    // Each arm diverges (parks forever), so the two `Service<_>` types (one
    // per state-machine tier) never need to unify.
    if a.raw_sm {
        let _svc = ServiceBuilder::new(cfg, RawCountSm::default()).start()?;
        println!(
            "m5_gate service up (raw-tier RawCountSm); parking (killed externally by the harness)"
        );
        loop {
            std::thread::park();
        }
    } else {
        let _svc = ServiceBuilder::new(cfg, CountSm::default()).start()?;
        println!("m5_gate service up (typed CountSm); parking (killed externally by the harness)");
        loop {
            std::thread::park();
        }
    }
}

// ----------------------------------------------------------- client role

/// Slot-array size for the `send_ns` correlation table. Vastly larger than
/// any realistic in-flight window `W`, so a slot is never reused while its
/// previous occupant is still outstanding.
const SLOTS: usize = 1 << 20;
const SLOT_MASK: usize = SLOTS - 1;
/// Histogram ceiling: 60 s. Any latency this bad already fails the gate by a
/// wide margin; the ceiling just bounds the hdrhistogram's memory.
const HIST_MAX_NS: u64 = 60_000_000_000;
/// After the timed send window closes, how long to wait for outstanding
/// requests to resolve before reporting whatever is left as in-flight.
const DRAIN_GRACE: Duration = Duration::from_secs(5);
/// How long the client waits, at attach, for its instance_dir's node to be a
/// serving leader before starting the timed measurement window.
const LEADER_WAIT: Duration = Duration::from_secs(30);

/// The PASS bar (spec §9).
const RESPONSES_PER_SEC_BAR: f64 = 400_000.0;
const P50_MS_BAR: f64 = 1.0;

struct ClientStats {
    sends: u64,
    responses: u64,
    not_leader: u64,
    retried: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    /// `Outcome::TimedOut | InstanceRestart` seen by the poll callback: the
    /// engine's 30s deadline sweep converts a genuinely lost response into a
    /// resolved `TimedOut` completion (or `InstanceRestart` on a node
    /// restart), so `inflight_at_end == 0` alone no longer proves every send
    /// got a real answer — see the PASS computation below.
    lost: u64,
    /// Drain-inclusive: from the first send to the LAST resolved response.
    elapsed: Duration,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    responses_per_sec: f64,
    pass: bool,
}

/// The measuring client's core loop, shared by the standalone `client` role
/// and the in-process `all` smoke (both attach through the SAME real
/// file-backed shmem code path — `all` just happens to run in the same OS
/// process as the node/service it targets). Attaches through the PUBLIC
/// `uc_client::Engine` — the measured path IS the shipped path: the engine
/// owns the slot table's exactly-once resolution, the duplicate-response
/// guard, the serving-gate pause, and NOT_LEADER/RETRY bookkeeping.
fn run_client_measurement(
    instance_dir: &Path,
    app_id: &str,
    secs: u64,
    payload_len: usize,
    inflight_cap: u64,
) -> ClientStats {
    let (send, mut poll) = Engine::attach(
        instance_dir,
        app_id,
        EngineConfig {
            max_inflight: inflight_cap as u32,
            request_timeout: Duration::from_secs(30), // never the limiter in a healthy run
            max_payload: Some(NODE_MAX_PAYLOAD),
            serving_gate: true, // the engine now owns the redirect-flood defense
            ..EngineConfig::default()
        },
    )
    .unwrap_or_else(|e| panic!("engine attach {instance_dir:?}: {e}"));

    // Serving wait: replaces await_serving(&cnc, ..) — the engine exposes the
    // same cnc flag; the client role no longer opens the cnc page itself.
    let serve_deadline = Instant::now() + LEADER_WAIT;
    while !send.can_serve() {
        assert!(
            Instant::now() < serve_deadline,
            "no serving leader at this instance_dir within {LEADER_WAIT:?} — \
             is this host's node the elected leader?"
        );
        thread::sleep(Duration::from_millis(20));
    }

    // One fixed payload, bincode-encoded ONCE and reused verbatim (unchanged
    // rationale: apply never inspects the bytes; identity lives in
    // header_extra). The engine's max_payload door replaces the old manual
    // NODE_MAX_PAYLOAD assert — try_submit fails loud instead.
    let raw_payload = vec![0xABu8; payload_len];
    let cmd_bytes = bincode::serde::encode_to_vec(&raw_payload, bincode::config::standard())
        .expect("encode fixed payload");

    // Timing slots: user_data = send index; SLOTS (1<<20) >> any window, so a
    // slot is never restamped while its request is outstanding. The old
    // `owner` array is GONE — exactly-once resolution is the engine's job now.
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
    let last_response_ns = Arc::new(AtomicU64::new(0));
    let hist: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(
        Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram"),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();

    let matcher = thread::Builder::new()
        .name("m5-gate-poll".into())
        .spawn({
            let send_ns = Arc::clone(&send_ns);
            let resolved = Arc::clone(&resolved);
            let responses = Arc::clone(&responses);
            let not_leader = Arc::clone(&not_leader);
            let retried = Arc::clone(&retried);
            let lost = Arc::clone(&lost);
            let last_response_ns = Arc::clone(&last_response_ns);
            let hist = Arc::clone(&hist);
            let stop = Arc::clone(&stop);
            move || {
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            Outcome::Response(_) => {
                                let idx = (c.user_data as usize) & SLOT_MASK;
                                let now = t0.elapsed().as_nanos() as u64;
                                let lat = now
                                    .saturating_sub(send_ns[idx].load(Ordering::Acquire))
                                    .min(HIST_MAX_NS);
                                let _ = hist.lock().unwrap().record(lat);
                                responses.fetch_add(1, Ordering::Relaxed);
                                last_response_ns.fetch_max(now, Ordering::Relaxed);
                            }
                            Outcome::NotLeader { .. } => {
                                not_leader.fetch_add(1, Ordering::Relaxed);
                            }
                            Outcome::Retry => {
                                retried.fetch_add(1, Ordering::Relaxed);
                            }
                            // The engine's deadline sweep resolves a genuinely
                            // lost response (or a node restart) into one of
                            // these — count it so the PASS bar can't be fooled
                            // by zero in-flight-at-end alone.
                            // A bench never issues a fan-in or names an id.
                            Outcome::TimedOut
                            | Outcome::InstanceRestart { .. }
                            | Outcome::Responses(_)
                            | Outcome::BadService { .. } => {
                                lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    // Old-matcher idle parity: the pre-Engine pump slept 20us between
                    // empty poll cycles, and that idle strategy is what produced the
                    // recorded fleet numbers (1.64M resp/s). A dedicated-core fleet
                    // client can flip this to spin_loop(), but on the oversubscribed
                    // in-process smoke box (3 nodes' agents + services + this thread
                    // sharing a handful of cores) a busy-spinning poll thread steals a
                    // full core from the very consensus/apply agents being measured —
                    // harness parity with the old pump beats dogma here.
                    if n == 0 {
                        thread::sleep(Duration::from_micros(20));
                    }
                }
            }
        })
        .expect("spawn poll thread");

    // Sender loop (this thread): user_data = send index; stamp send_ns
    // BEFORE try_submit (a response cannot arrive before the request is
    // visible). Restamping the same idx on a Backpressure retry is fine —
    // the request was never accepted.
    let mut sent_idx: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let idx = (sent_idx as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        match send.try_submit(sent_idx, &cmd_bytes) {
            Ok(()) => sent_idx += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(), // window OR ring full — engine's door
            Err(SubmitError::NotServing) => thread::sleep(Duration::from_millis(1)), // old serving-gate pause
            Err(e) => panic!("try_submit: {e}"),
        }
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;

    // Drain grace: unchanged bound, resolution counted by the poll thread.
    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent_idx && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("poll thread panicked");

    let sends = sent_idx;
    let resp = responses.load(Ordering::Relaxed);
    // The engine's own live count of claimed-but-unresolved slots — the
    // authoritative "did the run actually finish" signal now that the old
    // sends-minus-resolved bookkeeping lives inside the engine's slot table.
    let inflight_at_end = send.inflight();
    // Drain-inclusive clock, floored at the send window's end:
    // `max(last_response, send_window_end)`. Taking last_response alone would
    // let a run whose responses STOP arriving mid-window (service permanently
    // behind while the node keeps CAN_SERVE — no redirects, serving gate open,
    // last_response frozen early) silently excise its dead tail from the
    // denominator and report the early burst's rate as the run's rate. With
    // the floor, a dead tail depresses responses/s instead of vanishing.
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

    let (p50_ms, p90_ms, p99_ms, max_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (
            ms(h.value_at_quantile(0.50)),
            ms(h.value_at_quantile(0.90)),
            ms(h.value_at_quantile(0.99)),
            ms(h.max()),
        )
    };

    // PASS requires the run to have actually completed its work: zero
    // in-flight at end (strict, no epsilon) AND zero lost responses. A
    // healthy run has DRAIN_GRACE (5 s) to resolve a tail of at most
    // `inflight_cap` outstanding requests — at even 1% of the 400k/s bar that
    // drains in ~milliseconds — so anything left in-flight means responses
    // stopped arriving (service stalled) or were lost (broadcast overwrite).
    // But `inflight_at_end == 0` is NOT sufficient on its own: the engine's
    // 30s deadline sweep, running throughout the whole timed window (not
    // just DRAIN_GRACE), resolves a response that never arrived into
    // `Outcome::TimedOut` (or `InstanceRestart` on a node restart) — a real
    // completion that vacates the slot table, so `inflight()` reads 0 even
    // though the request never got a genuine answer. Without counting those,
    // a long run that silently loses responses throughout — never building
    // up in-flight backlog because the sweep keeps draining the slot table —
    // could read zero in-flight and print PASS despite real loss. `lost`
    // (from the poll callback) closes that gap.
    let pass = responses_per_sec >= RESPONSES_PER_SEC_BAR
        && p50_ms <= P50_MS_BAR
        && inflight_at_end == 0
        && lost.load(Ordering::Relaxed) == 0;

    let engine_stats = send.stats();

    ClientStats {
        sends,
        responses: resp,
        not_leader: not_leader.load(Ordering::Relaxed),
        retried: retried.load(Ordering::Relaxed),
        duplicates: engine_stats.duplicates,
        overwritten: engine_stats.overwritten,
        inflight_at_end,
        lost: lost.load(Ordering::Relaxed),
        elapsed,
        p50_ms,
        p90_ms,
        p99_ms,
        max_ms,
        responses_per_sec,
        pass,
    }
}

fn print_report(s: &ClientStats) {
    println!();
    println!("================ uc2 M5 gate: client -> apply -> response ================");
    println!("sends                 : {}", s.sends);
    println!("responses             : {}", s.responses);
    println!("not_leader redirects  : {}", s.not_leader);
    println!("retries               : {}", s.retried);
    println!("dup responses dropped : {}", s.duplicates);
    println!("broadcast overwritten : {}", s.overwritten);
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!("lost (timeout/restart): {}", s.lost);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("responses/s           : {:.0}", s.responses_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p90                   : {:.3} ms", s.p90_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    println!("max                   : {:.3} ms", s.max_ms);
    println!(
        "bar                   : responses/s >= {RESPONSES_PER_SEC_BAR:.0} && p50 <= {P50_MS_BAR:.1} ms \
         && in-flight at end == 0 && lost == 0"
    );
    println!("============================================================================");
    if s.pass {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL (honest)");
    }
}

/// One node's crypto-plane counters at a point in time — see
/// [`print_crypto_observability`] for what they are for and why the gate reads
/// them as a DELTA across the measurement window rather than as run totals.
#[derive(Clone, Copy, Default)]
struct CryptoSnapshot {
    seals: u64,
    auth_failed: u64,
    replay: u64,
    unknown_peer: u64,
    unknown_epoch: u64,
    cleartext_peer: u64,
    seal_failures: u64,
    hs_failures: u64,
}

fn crypto_snapshot(nodes: &[Node]) -> Vec<CryptoSnapshot> {
    nodes
        .iter()
        .map(|n| {
            let s = n.crypto_stats();
            CryptoSnapshot {
                seals: n.crypto_seal_count(),
                auth_failed: s.dropped_auth_failed.load(Ordering::Relaxed),
                replay: s.dropped_replay.load(Ordering::Relaxed),
                unknown_peer: s.dropped_unknown_peer.load(Ordering::Relaxed),
                unknown_epoch: s.dropped_unknown_epoch.load(Ordering::Relaxed),
                cleartext_peer: s.peer_appears_cleartext.load(Ordering::Relaxed),
                seal_failures: s.seal_failures.load(Ordering::Relaxed),
                hs_failures: n.crypto_handshake_failures(),
            }
        })
        .collect()
}

/// M8 gate evidence (T10 review's standing requirement): show that the measured
/// load actually drove the crypto planes CONCURRENTLY, in both directions, and
/// that nothing was silently failing closed *while the clock was running*.
///
/// A sender-only microbenchmark could not surface the `KeyState` mutex
/// contention at all, so the gate has to demonstrate rather than assert that
/// the load was two-sided. The demonstration is per-node `seals` (every AEAD
/// seal on every path draws exactly one nonce counter value, so this is a
/// count, not an estimate): the leader's seals are its `DATA`/`HEARTBEAT`
/// fan-out plus `COMMIT_POSITION` (group) and `TERM_MAP` (pairwise, one per
/// peer per commit advance); each follower's seals are its `APPEND_POSITION`/
/// `STATUS`/`NAK` reports. Nonzero on BOTH sides means each node was sealing
/// while simultaneously OPENING the other side's traffic — replication could
/// not have converged otherwise.
///
/// **Why the delta and not the total.** Boot totals are dominated by a
/// transient that has nothing to do with steady-state cost: a node's receiver
/// agent busy-spins its `STATUS`/`APPEND_POSITION` reports from the instant it
/// starts, but the Noise-IK sessions those reports seal under take a few
/// round trips to establish, so every report staged in that window fails to
/// seal with `NoSession` and is dropped (fail-closed, no cleartext fallback —
/// which is the correct behaviour). Those drops cost nothing: the reports are
/// periodic and self-healing, and the position they carry is monotone, so the
/// next one that lands supersedes every one that did not. The delta ACROSS THE
/// MEASURED WINDOW, printed below, discards that boot transient.
///
/// **The leader's in-window `seal_failures` are expected and benign — read the
/// other counters, not that one, for degradation.** A node reports its own
/// durable/append position to `cfg.leader`, and on the leader that address is
/// *itself*. A node holds no pairwise Noise session with itself, so every one
/// of the leader's self-addressed `APPEND_POSITION`/`STATUS` reports fails to
/// seal with `NoSession` and is counted — continuously, not just at boot. This
/// is a **pre-existing v2 self-send** (the receiver has always addressed those
/// reports to `cfg.leader` in cleartext too); wire crypto did not introduce it,
/// it merely made it *visible* by counting the fast-fail. It is harmless: the
/// leader's own position reaches its commit ranking in memory, never over the
/// wire, so nothing is lost — the linearizability capstones (T15) pass with
/// crypto on. It is also symmetric across the two gate arms (cleartext sends
/// the same self-addressed datagram, just without a counter), so it does not
/// move the throughput ratio. Suppressing the self-addressed report is a small
/// efficiency win filed as a post-M8 follow-up, not a correctness fix.
///
/// So the fail-closed check the gate actually reads is: **zero in-window
/// `auth_failed` / `replay` / `unknown_epoch` on every node, and zero in-window
/// `seal_failures` on the FOLLOWERS.** A nonzero there would mean traffic did
/// not make it through the crypto plane and the throughput number describes a
/// degraded system. The leader's `seal_failures` line is annotated below so a
/// reader is not alarmed by the one counter that is supposed to climb.
fn print_crypto_observability(
    nodes: &[Node],
    leader: usize,
    before: &[CryptoSnapshot],
    after: &[CryptoSnapshot],
) {
    println!("---------------------------- M8 crypto plane -----------------------------");
    println!(
        "leader                : n{leader} (group epoch {:?})",
        nodes[leader].crypto_epoch()
    );
    println!("(pre-window totals in parentheses — the boot-transient NoSession window; see");
    println!(" print_crypto_observability's doc. The gate reads the IN-WINDOW delta.)");
    for i in 0..nodes.len() {
        let (b, a) = (before[i], after[i]);
        println!(
            "n{i}{}: seals {:>9} (+{:<7}) | in-window: auth_failed {} | replay {} | \
             unknown_peer {} | unknown_epoch {} | cleartext_peer {} | seal_failures {} | \
             hs_failures {}",
            if i == leader {
                " (leader)"
            } else {
                "         "
            },
            a.seals - b.seals,
            b.seals,
            a.auth_failed - b.auth_failed,
            a.replay - b.replay,
            a.unknown_peer - b.unknown_peer,
            a.unknown_epoch - b.unknown_epoch,
            a.cleartext_peer - b.cleartext_peer,
            a.seal_failures - b.seal_failures,
            a.hs_failures - b.hs_failures,
        );
    }
    println!(
        "(the leader's in-window seal_failures are its own APPEND_POSITION/STATUS reports\n \
         addressed to cfg.leader == itself — a pre-existing v2 self-send, no self-session,\n \
         benign; the followers' seal_failures and everyone's auth_failed/replay/unknown_epoch\n \
         are the fail-closed check and must be 0.)"
    );
    println!("==========================================================================");
}

fn run_client_role(a: ClientArgs) -> anyhow::Result<()> {
    let secs = env_cap("UC2_M5_MAX_SECS", a.secs);
    let inflight = env_cap("UC2_M5_MAX_INFLIGHT", a.inflight);
    let stats = run_client_measurement(&a.instance_dir, &a.app_id, secs, a.payload, inflight);
    print_report(&stats);
    if !stats.pass {
        std::process::exit(1);
    }
    Ok(())
}

// -------------------------------------------------------------- all role

/// Wait for EXACTLY one serving leader across the live cluster; assert no
/// split-brain throughout (m4_gate / lincheck_v2 precedent). Returns its index.
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
        std::thread::yield_now();
    }
}

const ALL_APP_ID: &str = "uc2-m5-gate-smoke";

/// Dispatches to the raw or typed state-machine tier. The two tiers are
/// different `Service<S>` monomorphizations, so the run body is generic over
/// `S` (`run_all_generic`) rather than trying to unify them at one call site
/// (an enum / `Box<dyn Any>` would work too, but a static dispatch on `--raw-sm`
/// is simpler and this harness never mixes tiers within one run).
fn run_all(a: AllArgs) -> anyhow::Result<()> {
    if a.raw_sm {
        run_all_generic::<RawCountSm>(a, "raw (RawCountSm, bytes-in/bytes-out, no decode)")
    } else {
        run_all_generic::<CountSm>(a, "typed (CountSm, StateMachine + bincode)")
    }
}

fn run_all_generic<S: RawStateMachine + Default>(a: AllArgs, sm_label: &str) -> anyhow::Result<()> {
    let root = a
        .root
        .unwrap_or_else(|| PathBuf::from("target/m5_gate_smoke"));
    // Guard off /tmp (m4_gate precedent): /tmp is RAM-backed tmpfs on this dev
    // box and a real filesystem root is required for the journal-bearing
    // instance dirs (3 nodes x 256 MiB ring + journal segments).
    assert!(
        !root.starts_with("/tmp"),
        "m5_gate all: root must be on a real filesystem (never /tmp — RAM tmpfs); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(&root); // fresh root per run
    std::fs::create_dir_all(&root)?;

    println!("== uc2 M5 gate: SMOKE (local, in-process, 3 nodes + 3 services + 1 client) ==");
    println!(
        "*** THIS IS NOT THE M5 GATE *** — one dev box, core-starved across 3 nodes' agents \
         + 3 services + the client; the real bar is only claimable on the 3xc6id fleet run. \
         See docs/benchmarks/uc2-m5-gate-2026-07-12.md."
    );
    println!(
        "arm                   : {}",
        if a.crypto {
            "ENCRYPTED (M8 wire crypto ON)"
        } else {
            "cleartext control"
        }
    );
    println!("state machine         : {sm_label}");

    const N: usize = 3;
    // Generated ONCE for the whole run, before any node boots — every node
    // must see the same allowlist.
    let material = if a.crypto {
        Some(write_crypto_material(&root.join("crypto"), N))
    } else {
        None
    };
    let socks: Vec<UdpSocket> = (0..N)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind"))
        .collect();
    let members: Vec<(NodeId, SocketAddr)> = socks
        .iter()
        .enumerate()
        .map(|(i, s)| (i as NodeId, s.local_addr().unwrap()))
        .collect();

    let mut nodes = Vec::with_capacity(N);
    let mut services = Vec::with_capacity(N);
    let mut dirs = Vec::with_capacity(N);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        let crypto = match &material {
            Some(m) => {
                let (key_path, allowlist_path) = m[i].clone();
                uc_node::CryptoConfig::Enabled {
                    key_path,
                    allowlist_path,
                    rotation: uc_crypto::rotation::RotationPolicy::default(),
                }
            }
            None => uc_node::CryptoConfig::Disabled,
        };
        let cfg = node_config(
            i as NodeId,
            members.clone(),
            addr,
            instance_dir.clone(),
            ALL_APP_ID.into(),
            256 * 1024,
            crypto,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc = ServiceBuilder::new(ServiceConfig::new(&instance_dir, ALL_APP_ID), S::default())
            .start()
            .expect("service start");
        nodes.push(node);
        services.push(svc);
        dirs.push(instance_dir);
    }

    let leader = await_single_leader(&nodes, 30);
    println!("leader elected: n{leader}");

    let secs = env_cap("UC2_M5_MAX_SECS", a.secs);
    let inflight = env_cap("UC2_M5_MAX_INFLIGHT", a.inflight);
    let before = crypto_snapshot(&nodes);
    let stats = run_client_measurement(&dirs[leader], ALL_APP_ID, secs, a.payload, inflight);
    let after = crypto_snapshot(&nodes);
    print_report(&stats);
    if a.crypto {
        print_crypto_observability(&nodes, leader, &before, &after);
    }

    // Node-first-then-service teardown, per slot (v1/lincheck_v2 precedent: a
    // node's shutdown must not wait on a service that tears down first).
    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }

    if !stats.pass {
        println!(
            "(expected on a shared sandbox box: core-starved, NOT a gate failure — \
             see docs/benchmarks/uc2-m5-gate-2026-07-12.md)"
        );
        std::process::exit(1);
    }
    Ok(())
}
