//! `kv-load` — fill a kv cluster with many keys and read a sample back.
//! Used to show the store holds a few hundred thousand keys and to generate
//! log volume for the snapshot/purge demonstration.
//!
//! This is the **throughput** client, so it drives `uc_remote`'s lock-free
//! `RemoteEngine` halves directly: `try_submit` a window of requests, `poll`
//! their completions, and park only when nothing is ready. It does *not* use
//! the blocking `RemoteClient` that the `kv` CLI uses. The blocking client is
//! the right tool for one request at a time (a CLI, a handler that waits on
//! each call), but it pays a lock, an allocation and a wakeup per request
//! that the engine halves avoid — which only matters when you want the
//! platform's rate rather than one answer. The choice is laid out in
//! `docs/how-to/run-a-gateway.md` § "Which client do I want?".
//!
//! Prints one summary line: keys, bytes, seconds, puts/s, and verifies that
//! a spread of keys reads back linearizably with the value written.

use std::time::{Duration, Instant};

use clap::Parser;
use kv_store::wire::{self, GetReply, WriteReply};
use uc_remote::engine::{
    RemoteEngine, RemoteOutcome, RemotePollHalf, RemoteSendHalf, RemoteWaitHandle, SubmitError,
};
use uc_remote::{Consistency, RemoteConfig};

#[derive(Parser)]
#[command(
    name = "kv-load",
    about = "Loads N keys into a kv cluster through its gateways"
)]
struct Args {
    #[arg(long, value_delimiter = ',', required = true)]
    gateways: Vec<String>,
    #[arg(long, default_value = "kv")]
    app_id: String,
    /// Number of keys to write (keys are `load:<prefix>:<i>`).
    #[arg(long, default_value_t = 100_000)]
    keys: u64,
    /// Value size in bytes (≤ 1024).
    #[arg(long, default_value_t = 256)]
    value_bytes: usize,
    /// Key prefix, so repeated runs write distinct keys.
    #[arg(long, default_value = "a")]
    prefix: String,
    /// Requests kept in flight (the engine's `max_inflight`).
    #[arg(long, default_value_t = 512)]
    window: usize,
    /// Keys to read back and check after the load (linearizable).
    #[arg(long, default_value_t = 200)]
    verify: u64,
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

fn value_for(i: u64, n: usize) -> Vec<u8> {
    // Deterministic per key so verification needs no bookkeeping.
    let seed = i.to_le_bytes();
    (0..n).map(|j| seed[j % 8] ^ (j as u8)).collect()
}

/// How long to park when no completion is ready. `park` returns the moment
/// one lands (a wake between the check and the park is not lost), so this
/// only bounds the cost of a missed wake.
const PARK: Duration = Duration::from_millis(5);

/// Drain every completion currently queued: decode each write reply, count
/// replays, collect failures by key. Returns how many completions were
/// handled, so a caller can park when it is zero.
fn drain_puts(
    poll: &mut RemotePollHalf,
    outstanding: &mut u64,
    replayed: &mut u64,
    errors: &mut Vec<String>,
) -> usize {
    poll.poll(|c| {
        *outstanding -= 1;
        match c.outcome {
            RemoteOutcome::Response {
                body,
                replayed: r,
                expired,
            } => {
                if r {
                    *replayed += 1;
                }
                if expired {
                    // The session dedup window moved past this seq, so the
                    // edge could not say whether it applied. Name that
                    // rather than decoding the (empty) body and blaming
                    // the wire.
                    errors.push(format!(
                        "key {}: expired — the dedup window moved past this seq, outcome unknowable",
                        c.user_data
                    ));
                } else {
                    match wire::decode_write_reply(body) {
                        Ok(WriteReply::Ok { .. }) => {}
                        Ok(other) => errors
                            .push(format!("key {}: unexpected reply {other:?}", c.user_data)),
                        Err(e) => errors.push(format!("key {}: {e}", c.user_data)),
                    }
                }
            }
            other => errors.push(format!("key {}: {other:?}", c.user_data)),
        }
    })
}

/// One linearizable query, driven to completion. Only used once the put
/// window is fully drained, so the completion it waits for is its own.
fn query_one(
    send: &RemoteSendHalf,
    poll: &mut RemotePollHalf,
    wait: &RemoteWaitHandle,
    user_data: u64,
    q: &[u8],
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    loop {
        match send.try_query(user_data, Consistency::Linearizable, q) {
            Ok(()) => break,
            Err(SubmitError::Backpressure) => {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "query {user_data}: backpressure never cleared"
                );
                wait.park(PARK);
            }
            Err(e) => anyhow::bail!("query {user_data}: {e}"),
        }
    }
    loop {
        let mut got: Option<anyhow::Result<Vec<u8>>> = None;
        poll.poll(|c| {
            if c.user_data == user_data {
                got = Some(match c.outcome {
                    RemoteOutcome::Response { body, .. } => Ok(body.to_vec()),
                    other => Err(anyhow::anyhow!("query {user_data}: {other:?}")),
                });
            }
        });
        if let Some(r) = got {
            return r;
        }
        anyhow::ensure!(Instant::now() < deadline, "query {user_data}: timed out");
        wait.park(PARK);
    }
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    anyhow::ensure!(
        a.value_bytes <= wire::MAX_VALUE,
        "--value-bytes > {}",
        wire::MAX_VALUE
    );
    let timeout = Duration::from_secs(a.timeout_secs);
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        app_id: a.app_id.clone(),
        members: a.gateways.clone(),
        max_inflight: a.window as u32,
        request_timeout: timeout,
        ..Default::default()
    })?;
    let wait = poll.wait_handle();

    let started = Instant::now();
    let mut outstanding = 0u64;
    let mut replayed = 0u64;
    let mut errors: Vec<String> = Vec::new();
    let mut bytes = 0u64;
    for i in 0..a.keys {
        let key = format!("load:{}:{i}", a.prefix);
        let val = value_for(i, a.value_bytes);
        let frame = wire::try_encode_put(key.as_bytes(), &val)?;
        bytes += frame.len() as u64;
        // Submit; on backpressure the request is NOT consumed, so free the
        // window by draining completions (parking only if none are ready)
        // and retry the same frame. No lock, no per-request allocation.
        // The deadline mirrors `query_one`: a conforming edge never grants
        // zero credits, so this only bounds a misconfigured window.
        let deadline = Instant::now() + timeout;
        loop {
            match send.try_submit(i, &frame) {
                Ok(()) => {
                    outstanding += 1;
                    break;
                }
                Err(SubmitError::Backpressure) => {
                    anyhow::ensure!(
                        Instant::now() < deadline,
                        "submit key {i}: backpressure never cleared"
                    );
                    if drain_puts(&mut poll, &mut outstanding, &mut replayed, &mut errors) == 0 {
                        wait.park(PARK);
                    }
                }
                Err(e) => anyhow::bail!("submit key {i}: {e}"),
            }
        }
        // Keep completions from piling up behind the window.
        drain_puts(&mut poll, &mut outstanding, &mut replayed, &mut errors);
        if !errors.is_empty() {
            break;
        }
    }
    // Wait out the tail.
    let deadline = Instant::now() + timeout;
    while outstanding > 0 && errors.is_empty() {
        if drain_puts(&mut poll, &mut outstanding, &mut replayed, &mut errors) == 0 {
            anyhow::ensure!(
                Instant::now() < deadline,
                "timed out with {outstanding} puts outstanding"
            );
            wait.park(PARK);
        }
    }
    if let Some(e) = errors.first() {
        anyhow::bail!("{e}");
    }
    let secs = started.elapsed().as_secs_f64();
    println!(
        "loaded keys={} value_bytes={} command_bytes={} secs={:.2} puts_per_s={:.0} replayed={} stats={:?}",
        a.keys,
        a.value_bytes,
        bytes,
        secs,
        a.keys as f64 / secs,
        replayed,
        send.stats()
    );

    // Verify a spread of keys, linearizably.
    let step = (a.keys / a.verify.max(1)).max(1);
    let mut checked = 0u64;
    let mut i = 0;
    while i < a.keys {
        let key = format!("load:{}:{i}", a.prefix);
        let body = query_one(
            &send,
            &mut poll,
            &wait,
            i,
            &wire::encode_get(key.as_bytes()),
            timeout,
        )?;
        match wire::decode_get_reply(&body)? {
            GetReply::Found { value, .. } if value[..] == value_for(i, a.value_bytes)[..] => {
                checked += 1
            }
            other => anyhow::bail!("key {key}: verify failed: {other:?}"),
        }
        i += step;
    }
    println!("verified {checked} keys read back linearizably with the written value");
    send.shutdown();
    Ok(())
}
