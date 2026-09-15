//! B4.ii — the known-key set: written through the harness BEFORE the
//! application upgrade (each acknowledged write's key, value and version
//! recorded to a file), read back linearizably AFTER it. Every
//! acknowledged value must read back; a changed version is reported (an
//! upgrade must not rewrite history) but is not the bar.
//!
//! File format, one line per acknowledged write: `key<TAB>value<TAB>version`
//! (value and version decimal). Keys are `known:<seed>:<i>`.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

use crate::adapter::{Adapter, KvOp, KvResp};

pub fn client(gateways: &[String], app_id: &str, timeout: Duration) -> Result<RemoteClient> {
    let deadline = Instant::now() + timeout;
    loop {
        match RemoteClient::connect(RemoteConfig {
            app_id: app_id.to_string(),
            members: gateways.to_vec(),
            client_id: Some(rand::random::<u64>() | 1),
            request_timeout: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(2),
            max_inflight: 64,
            ..Default::default()
        }) {
            Ok(c) => return Ok(c),
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(e) => bail!("no gateway reachable within {timeout:?}: {e}"),
        }
    }
}

pub struct WriteReport {
    pub acknowledged: usize,
    pub unacknowledged: usize,
}

pub fn write(
    adapter: &dyn Adapter,
    gateways: &[String],
    app_id: &str,
    set: &Path,
    count: u64,
    seed: u64,
) -> Result<WriteReport> {
    let c = client(gateways, app_id, Duration::from_secs(30))?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut lines = String::new();
    let mut acknowledged = 0;
    let mut unacknowledged = 0;
    for i in 0..count {
        let key = format!("known:{seed}:{i}");
        let value: u64 = rng.random();
        let op = KvOp::Put(value);
        let cmd = adapter.encode(key.as_bytes(), &op, 0);
        let mut done = false;
        for _attempt in 0..5 {
            match c.submit(&cmd).and_then(|t| t.wait()) {
                Ok(resp) => match adapter.decode(&op, &resp.bytes) {
                    Ok(d) if d.resp == KvResp::Ack => {
                        lines.push_str(&format!(
                            "{key}\t{value}\t{}\n",
                            d.version.unwrap_or(resp.position)
                        ));
                        acknowledged += 1;
                        done = true;
                        break;
                    }
                    Ok(d) => bail!("{key}: unexpected reply {:?}", d.resp),
                    Err(e) => bail!("{key}: undecodable reply: {e}"),
                },
                Err(RemoteError::PayloadTooLarge) => bail!("{key}: payload too large"),
                Err(_) => std::thread::sleep(Duration::from_millis(200)),
            }
        }
        if !done {
            unacknowledged += 1;
        }
    }
    std::fs::write(set, lines).with_context(|| format!("write {}", set.display()))?;
    c.shutdown();
    Ok(WriteReport {
        acknowledged,
        unacknowledged,
    })
}

#[derive(Default, Debug)]
pub struct VerifyReport {
    pub checked: usize,
    pub missing: Vec<String>,
    pub wrong_value: Vec<String>,
    pub version_changed: Vec<String>,
    pub unreadable: Vec<String>,
}

pub fn verify(
    adapter: &dyn Adapter,
    gateways: &[String],
    app_id: &str,
    set: &Path,
) -> Result<VerifyReport> {
    let text = std::fs::read_to_string(set).with_context(|| format!("read {}", set.display()))?;
    let c = client(gateways, app_id, Duration::from_secs(30))?;
    let mut r = VerifyReport::default();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let mut it = line.split('\t');
        let (Some(key), Some(value), Some(version)) = (it.next(), it.next(), it.next()) else {
            bail!("malformed set line: {line:?}");
        };
        let value: u64 = value.parse().context("value")?;
        let version: u64 = version.parse().context("version")?;
        let q = adapter.encode(key.as_bytes(), &KvOp::Get, 0);
        let mut got = None;
        for _attempt in 0..10 {
            match c
                .query(&q, Consistency::Linearizable)
                .and_then(|t| t.wait())
            {
                Ok(resp) => {
                    got = Some(adapter.decode(&KvOp::Get, &resp.bytes));
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(300)),
            }
        }
        r.checked += 1;
        match got {
            None => r.unreadable.push(key.to_string()),
            Some(Err(e)) => r.unreadable.push(format!("{key}: {e}")),
            Some(Ok(d)) => match d.resp {
                KvResp::Value(None) => r.missing.push(key.to_string()),
                KvResp::Value(Some(v)) if v != value => r
                    .wrong_value
                    .push(format!("{key}: wrote {value}, read {v}")),
                KvResp::Value(Some(_)) => {
                    if let Some(now) = d.version
                        && now != version
                    {
                        r.version_changed
                            .push(format!("{key}: acknowledged version {version}, now {now}"));
                    }
                }
                other => r.unreadable.push(format!("{key}: {other:?}")),
            },
        }
    }
    c.shutdown();
    Ok(r)
}
