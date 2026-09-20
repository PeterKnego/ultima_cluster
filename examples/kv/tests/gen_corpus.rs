//! Generate the checked-in regression corpora under `tests/corpora/`
//! (spec §5.9). `#[ignore]`d: it is a one-off writer, not a test — it starts
//! a real in-process node, drives the real `Sessioned<KvSm>` service over the
//! real shmem path, and then `uc2ctl backup`s the stopped instance into the
//! repository.
//!
//!     cargo test -p kv_store --test gen_corpus -- --ignored
//!
//! Re-run it after a wire or image format change, then commit the result.
//! The *reader* of what it writes is `regression_corpora.rs`.
//!
//! Why the raw `uc_client::Engine` and not `uc_client::Client`: `Client` is
//! serde-typed (`submit<C: Serialize>`), and `KvSm` is a RAW state machine
//! whose commands are its own byte frames. `Engine` is the bytes-level API —
//! the one the gateway itself relays over — so the bytes that reach `apply`
//! here are exactly the bytes a `kv` CLI request produces: the 16-byte
//! `Sessioned` envelope (`client_id: u64 LE ‖ seq: u64 LE`,
//! `uc_service::session::SESSION_HEADER_LEN`) followed by the KV wire frame.
//! A corpus built with the wrong envelope would replay as garbage on both
//! sides of a determinism run and still "pass", so the generated corpus's
//! content is asserted here AND in `regression_corpora.rs`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kv_store::{KV_VERSION, KvSm, wire};
use uc_client::{Engine, EngineConfig, Outcome, PollHalf, SendHalf};
use uc_diffreplay::corpus::Corpus;
use uc_node::{Node, NodeConfig};
use uc_service::{
    RawStateMachine, SESSION_HEADER_LEN, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned,
    TAG_FRESH,
};

/// The checked-in corpus has to stay small (spec §5.9 — it lives in the
/// repository and every `cargo test` replays it). The node's 64 MiB default
/// segment size is the one knob that could put a large file there, so this
/// generator caps it rather than relying on segment preallocation staying
/// off. What it actually writes today is a 408-byte segment.
const JOURNAL_SEGMENT_BYTES: u64 = 256 * 1024;

const APP_ID: &str = "kv";
/// The one client this corpus has; its envelope's `client_id`.
const CLIENT_ID: u64 = 1;

fn corpus_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpora")
        .join(name)
}

fn node_config(dir: &Path) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: APP_ID.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: uc_net::fault::FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(<KvSm as RawStateMachine>::NAME),
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "{what}: never held");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The bytes a live `kv` request puts on the wire: the `Sessioned` envelope
/// then the app's own frame.
fn enveloped(seq: u64, frame: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(SESSION_HEADER_LEN + frame.len());
    v.extend_from_slice(&CLIENT_ID.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(frame);
    v
}

/// Submit one command and block until its single completion. Returns the
/// response body (the `Sessioned` tag byte first).
fn submit_blocking(send: &SendHalf, poll: &mut PollHalf, seq: u64, frame: &[u8]) -> Vec<u8> {
    let payload = enveloped(seq, frame);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match send.try_submit(seq, &payload) {
            Ok(()) => break,
            Err(e) => {
                assert!(Instant::now() < deadline, "submit seq {seq}: {e}");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
    let mut got: Option<Vec<u8>> = None;
    while got.is_none() {
        assert!(Instant::now() < deadline, "no completion for seq {seq}");
        poll.poll(|c| {
            assert_eq!(c.user_data, seq);
            match c.outcome {
                Outcome::Response(bytes) => got = Some(bytes.to_vec()),
                ref other => panic!("seq {seq}: {other:?}"),
            }
        });
    }
    let resp = got.unwrap();
    assert_eq!(
        resp.first().copied(),
        Some(TAG_FRESH),
        "seq {seq} was not applied fresh: {resp:?}"
    );
    resp
}

/// Copy `src`'s tree into `dst`, creating directories as needed. Files
/// already at `dst` that `src` does not name are left alone — which is how
/// the hand-written `intent.toml` survives a regeneration.
fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let (from, to) = (e.path(), dst.join(e.file_name()));
        if e.file_type().unwrap().is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

/// Two puts below the origin, one delete above it — the shape §5.9 asks a
/// corpus to have: state at P that the span then changes.
#[test]
#[ignore = "one-off corpus writer; run with --ignored and commit the result"]
fn generate_put_then_delete_corpus() {
    let scratch = tempfile::Builder::new()
        .prefix("kv-gen-corpus-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let inst = scratch.path().join("instance");
    std::fs::create_dir_all(&inst).unwrap();

    let node = Node::start(node_config(&inst)).unwrap();
    wait_until("node can_serve", || node.can_serve());

    let cfg = ServiceConfig::new(inst.clone(), APP_ID.to_string());
    let sm = Sessioned::new(KvSm::default(), SessionConfig::default());
    let service = ServiceBuilder::new(cfg, sm).start_with_snapshots().unwrap();

    let (send, mut poll) = Engine::attach(&inst, APP_ID, EngineConfig::default()).unwrap();

    // Below the origin: the two keys the artifact at P must already hold.
    submit_blocking(&send, &mut poll, 0, &wire::encode_put(b"a", b"1"));
    submit_blocking(&send, &mut poll, 1, &wire::encode_put(b"b", b"2"));

    // The origin: `uc2ctl snapshot`, in process.
    let p = {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match node.command_snapshot(false) {
                Ok(p) => break p,
                Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("uc2ctl snapshot refused: {e}"),
            }
        }
    };
    let artifact = inst
        .join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"));
    wait_until("artifact at P", || artifact.is_file());

    // Above the origin: the one command the replayed span performs.
    submit_blocking(&send, &mut poll, 2, &wire::encode_delete(b"a"));

    drop(send);
    service.stop();
    node.stop();

    // Export into scratch first: `backup_instance` refuses a non-empty
    // destination, and the destination in the repository holds the
    // hand-written `intent.toml` (and the reports' `.gitignore`).
    let staged = scratch.path().join("export");
    let c = Corpus::export(&inst, APP_ID, 0, p, u64::MAX, KV_VERSION, &staged).unwrap();
    assert!(c.artifact().is_file());

    let out = corpus_dir("put-then-delete");
    for stale in ["journal", "state", "snapshots"] {
        let _ = std::fs::remove_dir_all(out.join(stale));
    }
    for stale in ["CORPUS", "MANIFEST"] {
        let _ = std::fs::remove_file(out.join(stale));
    }
    copy_tree(&staged, &out);

    // The generated corpus must MEAN something: the artifact at P holds both
    // puts. (`regression_corpora.rs` re-checks this on every `cargo test`,
    // plus that a replay of the span then removes key `a`.)
    let projection = uc_diffreplay::drive::project_artifact(
        Sessioned::new(KvSm::default(), SessionConfig::default()),
        &Corpus::open(&out).unwrap().artifact(),
        p,
    )
    .unwrap();
    assert!(projection.contains("key=61 "), "{projection}");
    assert!(projection.contains("key=62 "), "{projection}");

    eprintln!("wrote {} — origin {p}", out.display());
}
