//! `kv-load` — fill a kv cluster with many keys and read a sample back.
//! Used to show the store holds a few hundred thousand keys and to generate
//! log volume for the snapshot/purge demonstration. Pipelined: it keeps a
//! window of tickets in flight rather than one round trip per key.
//!
//! Prints one summary line: keys, bytes, seconds, puts/s, and verifies that
//! a random sample of keys reads back with the value written.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use clap::Parser;
use kv_store::wire::{self, GetReply, WriteReply};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, Ticket};

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
    /// Requests kept in flight.
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

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    anyhow::ensure!(
        a.value_bytes <= wire::MAX_VALUE,
        "--value-bytes > {}",
        wire::MAX_VALUE
    );
    let client = RemoteClient::connect(RemoteConfig {
        app_id: a.app_id.clone(),
        members: a.gateways.clone(),
        max_inflight: a.window as u32,
        request_timeout: Duration::from_secs(a.timeout_secs),
        ..Default::default()
    })?;

    let started = Instant::now();
    let mut inflight: VecDeque<(u64, Ticket)> = VecDeque::new();
    let mut bytes = 0u64;
    let mut replayed = 0u64;
    let drain =
        |inflight: &mut VecDeque<(u64, Ticket)>, replayed: &mut u64| -> anyhow::Result<()> {
            if let Some((i, t)) = inflight.pop_front() {
                let r = t.wait()?;
                if r.replayed {
                    *replayed += 1;
                }
                match wire::decode_write_reply(&r.bytes)? {
                    WriteReply::Ok { .. } => {}
                    other => anyhow::bail!("key {i}: unexpected reply {other:?}"),
                }
            }
            Ok(())
        };
    for i in 0..a.keys {
        let key = format!("load:{}:{i}", a.prefix);
        let val = value_for(i, a.value_bytes);
        let frame = wire::try_encode_put(key.as_bytes(), &val)?;
        bytes += frame.len() as u64;
        // `submit` blocks on credits; keep our own window one below max_inflight
        // so the blocking is on the edge's grant, not our cap.
        while inflight.len() >= a.window.saturating_sub(1).max(1) {
            drain(&mut inflight, &mut replayed)?;
        }
        inflight.push_back((i, client.submit(&frame)?));
    }
    while !inflight.is_empty() {
        drain(&mut inflight, &mut replayed)?;
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
        client.stats()
    );

    // Verify a spread of keys, linearizably.
    let step = (a.keys / a.verify.max(1)).max(1);
    let mut checked = 0u64;
    let mut i = 0;
    while i < a.keys {
        let key = format!("load:{}:{i}", a.prefix);
        let r = client
            .query(&wire::encode_get(key.as_bytes()), Consistency::Linearizable)?
            .wait()?;
        match wire::decode_get_reply(&r.bytes)? {
            GetReply::Found { value, .. } if value[..] == value_for(i, a.value_bytes)[..] => {
                checked += 1
            }
            other => anyhow::bail!("key {key}: verify failed: {other:?}"),
        }
        i += step;
    }
    println!("verified {checked} keys read back linearizably with the written value");
    client.shutdown();
    Ok(())
}
