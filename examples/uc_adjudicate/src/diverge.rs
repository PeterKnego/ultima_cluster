//! B4.i — state agreement across replicas, two ways:
//!
//! 1. **Live**, through each gateway: a SNAPSHOT-consistency digest query
//!    pinned to each edge (served by the replica the edge sits on), taken
//!    once every replica reports the same `last_applied` — so the compare
//!    is at one position, not a race. Needs only TCP reachability; this is
//!    the fleet form.
//! 2. **Offline**, from two row-0 snapshot artifacts (`diff-snapshots`):
//!    UC's 16-byte `ULTSNAP1 ‖ P` envelope is checked, the adapter parses
//!    the image, and the key-level difference is listed. This is what says
//!    WHICH keys diverged when the digests disagree.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::adapter::{Adapter, Digest, Image};
use crate::pinned;

pub struct DigestRow {
    pub gateway: String,
    pub digest: Digest,
}

/// Poll every gateway's digest until all `last_applied` agree (and the
/// same value is seen twice in a row), or `timeout`.
pub fn live_digests(
    adapter: &dyn Adapter,
    gateways: &[String],
    app_id: &str,
    timeout: Duration,
) -> Result<Vec<DigestRow>> {
    let q = adapter
        .encode_digest()
        .ok_or_else(|| anyhow::anyhow!("adapter {} has no digest query", adapter.name()))?;
    let deadline = Instant::now() + timeout;
    let mut last_agreed: Option<u64> = None;
    loop {
        let mut rows = Vec::new();
        let mut errs = Vec::new();
        for g in gateways {
            match pinned::snapshot_query(g, app_id, &q, Duration::from_secs(5)) {
                Ok((_meta, body)) => match adapter.decode_digest(&body) {
                    Ok(d) => rows.push(DigestRow {
                        gateway: g.clone(),
                        digest: d,
                    }),
                    Err(e) => errs.push(format!("{g}: {e}")),
                },
                Err(e) => errs.push(format!("{g}: {e:#}")),
            }
        }
        if errs.is_empty() {
            let pos: Vec<u64> = rows.iter().map(|r| r.digest.last_applied).collect();
            let agreed = pos.iter().all(|p| *p == pos[0]);
            if agreed {
                if last_agreed == Some(pos[0]) {
                    return Ok(rows);
                }
                last_agreed = Some(pos[0]);
            } else {
                last_agreed = None;
            }
        }
        if Instant::now() > deadline {
            if !errs.is_empty() {
                bail!(
                    "digest queries failing after {timeout:?}: {}",
                    errs.join("; ")
                );
            }
            bail!(
                "replicas never agreed on last_applied within {timeout:?}: {:?}",
                rows.iter()
                    .map(|r| (r.gateway.clone(), r.digest.last_applied))
                    .collect::<Vec<_>>()
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// `true` iff every row reports the same (count, digest).
pub fn agree(rows: &[DigestRow]) -> bool {
    rows.windows(2)
        .all(|w| w[0].digest.count == w[1].digest.count && w[0].digest.digest == w[1].digest.digest)
}

const ENVELOPE: &[u8; 8] = b"ULTSNAP1";

/// Read a row artifact: check UC's envelope and return (P, image).
pub fn read_artifact(adapter: &dyn Adapter, path: &Path) -> Result<(u64, Image)> {
    let b = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if b.len() < 16 || &b[..8] != ENVELOPE {
        bail!(
            "{}: no ULTSNAP1 envelope (pre-2.11.0 artifact or not an artifact)",
            path.display()
        );
    }
    let p = u64::from_le_bytes(b[8..16].try_into().unwrap());
    // Strip the framework's session-table prefix when the service is
    // `Sessioned`. `uc_service::Sessioned<S>::stream_snapshot`
    // (uc_service/src/session.rs) writes `u64 blob_len ‖ blob` (the
    // replicated dedup table) BEFORE the inner `S::stream_snapshot`. A KV
    // deployed with `[session] envelope = true` — which the builder's README
    // requires for exactly-once — is `Sessioned<KvSm>`, so its on-disk
    // artifact is `envelope ‖ session_blob_len:u64 ‖ session_blob ‖ kv_image`,
    // NOT `envelope ‖ kv_image` as the builder's `WIRE-FORMAT.md` § 5 states.
    // We skip the prefix by its length (exact, never a scan); the blob's
    // bincode `TableImage` contents are UC's, not the adapter's business.
    let payload = &b[16..];
    let image: &[u8] = if adapter.sessioned() {
        let len_bytes = payload.get(..8).ok_or_else(|| {
            anyhow::anyhow!(
                "{}: artifact too short for the session prefix",
                path.display()
            )
        })?;
        let blob_len = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        payload.get(8 + blob_len..).ok_or_else(|| {
            anyhow::anyhow!(
                "{}: session blob claims {blob_len} B, artifact has {}",
                path.display(),
                payload.len().saturating_sub(8)
            )
        })?
    } else {
        payload
    };
    let img = adapter
        .parse_snapshot_image(image)
        .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    Ok((p, img))
}

/// Key-level differences between two images, as printable lines.
pub fn diff(a: &Image, b: &Image) -> Vec<String> {
    let mut out = Vec::new();
    for (k, va) in a {
        match b.get(k) {
            None => out.push(format!("only in A: {} (version {})", hex(k), va.0)),
            Some(vb) if vb != va => out.push(format!(
                "differs: {} A=(version {}, {} B) B=(version {}, {} B)",
                hex(k),
                va.0,
                va.1.len(),
                vb.0,
                vb.1.len()
            )),
            _ => {}
        }
    }
    for k in b.keys() {
        if !a.contains_key(k) {
            out.push(format!("only in B: {} (version {})", hex(k), b[k].0));
        }
    }
    out
}

pub fn hex(b: &[u8]) -> String {
    if let Ok(s) = std::str::from_utf8(b)
        && s.chars().all(|c| c.is_ascii_graphic())
    {
        return s.to_string();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}
