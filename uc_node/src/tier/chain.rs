//! Tier chains (spec §4.5): listing, reach, the watermark, chain planning,
//! streamed range reads and offline verify/show.
use super::TierError;
use super::layout::{
    FENCE_DIR, SET_FILE, SetLabel, TierInfo, chunk_object, cluster_object, fence_object,
    parse_chunk_name, parse_fence_name, parse_set_dir, row_object, set_dir,
};
use super::object::{MAGIC_ARTIFACT, MAGIC_CHUNK, ObjectReader};
use super::target::BackupTarget;
use sha2::Digest;
use std::io::Read;
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len, read_header};
use uc_service::snapshots::{SNAPSHOT_ENVELOPE_LEN, decode_snapshot_envelope};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierListing {
    pub sets: Vec<SetLabel>,
    pub incomplete_sets: Vec<u64>,
    pub chunks: Vec<(u64, u64)>,
    /// F4: sets whose `SET` label exists but does not parse, or names
    /// another position than its directory. Never a start; `verify` reports
    /// each as `tier_set_label_corrupt`; everything else lists on.
    pub corrupt_sets: Vec<u64>,
    /// Ruling RF1: the restore points named by `fence/<Q>` objects. A
    /// writer refuses a fenced target; reads ignore fences.
    pub fences: Vec<u64>,
}

/// Receives `(position, bytes)` pieces of a range read.
pub type RangeSink<'a> = dyn FnMut(u64, &[u8]) -> Result<(), TierError> + 'a;

fn corrupt(what: impl Into<String>, detail: impl Into<String>) -> TierError {
    TierError::Corrupt {
        what: what.into(),
        detail: detail.into(),
    }
}

/// A set's `SET` label: `Ok(None)` when absent (an interrupted write — the
/// label is written last), `Err` when present but unusable (it does not
/// parse, or names another position than its directory).
fn read_label(t: &dyn BackupTarget, p: u64) -> Result<Option<Result<SetLabel, String>>, TierError> {
    let Some(mut f) = t.open(&format!("{}/{SET_FILE}", set_dir(p)))? else {
        return Ok(None);
    };
    let mut s = String::new();
    // A label that is not UTF-8 is as corrupt as one that does not parse.
    if let Err(e) = f.read_to_string(&mut s) {
        if e.kind() == std::io::ErrorKind::InvalidData {
            return Ok(Some(Err("not UTF-8 text".to_string())));
        }
        return Err(e.into());
    }
    Ok(Some(match SetLabel::parse(&s) {
        Err(e) => Err(e.to_string()),
        Ok(l) if l.position != p => Err(format!(
            "position {} disagrees with its directory",
            l.position
        )),
        Ok(l) => Ok(l),
    }))
}

/// A set is complete iff its `SET` label exists and parses; a directory
/// without one is an interrupted write (the label is written last) and
/// never a start. A label that exists but is unusable marks the set
/// corrupt (F4) — listed apart, never a start, and never fatal to the
/// listing: one bad object must not brick the writer, retention or restore.
pub fn list_tier(t: &dyn BackupTarget) -> Result<TierListing, TierError> {
    let mut sets = Vec::new();
    let mut incomplete = Vec::new();
    let mut corrupt_sets = Vec::new();
    for name in t.list("sets")? {
        let Some(p) = parse_set_dir(&name) else {
            continue;
        };
        match read_label(t, p)? {
            None => incomplete.push(p),
            Some(Err(_)) => corrupt_sets.push(p),
            Some(Ok(l)) => sets.push(l),
        }
    }
    sets.sort_by_key(|l| l.position);
    incomplete.sort_unstable();
    corrupt_sets.sort_unstable();
    let mut chunks: Vec<(u64, u64)> = t
        .list("journal")?
        .iter()
        .filter_map(|n| parse_chunk_name(n))
        .collect();
    chunks.sort_unstable();
    let mut fences: Vec<u64> = t
        .list(FENCE_DIR)?
        .iter()
        .filter_map(|n| parse_fence_name(n))
        .collect();
    fences.sort_unstable();
    Ok(TierListing {
        sets,
        incomplete_sets: incomplete,
        chunks,
        corrupt_sets,
        fences,
    })
}

/// Ruling RF1: the restore points of the target's `fence/` objects, sorted.
/// Cheaper than a full [`list_tier`]; a writer checks it when it opens a
/// target.
pub fn list_fences(t: &dyn BackupTarget) -> Result<Vec<u64>, TierError> {
    let mut f: Vec<u64> = t
        .list(FENCE_DIR)?
        .iter()
        .filter_map(|n| parse_fence_name(n))
        .collect();
    f.sort_unstable();
    Ok(f)
}

/// The chunk extending a chain at cursor `c`: `from <= c < to`, greatest `to`
/// (ruling PF1; overlaps are legitimate, spec §4.5.3).
fn step(chunks: &[(u64, u64)], c: u64) -> Option<(u64, u64)> {
    chunks
        .iter()
        .copied()
        .filter(|&(f, t)| f <= c && c < t)
        .max_by_key(|&(_, t)| t)
}

/// Longest gap-free run of chunks from `start` (spec §4.5.4), every step
/// following [`step`] (ruling R3a: a set usually lies INSIDE a chunk).
/// Returns `start` when no chunk covers it.
pub fn reach_from(chunks: &[(u64, u64)], start: u64) -> u64 {
    let mut c = start;
    while let Some((_, t)) = step(chunks, c) {
        c = t;
    }
    c
}

/// `(oldest_set, newest_set, through)`: `through` is the maximal reach over
/// complete sets; oldest is the oldest set reaching it; newest the newest
/// complete set at or before `through`.
pub fn watermark_of(l: &TierListing) -> Option<(u64, u64, u64)> {
    let reaches: Vec<(u64, u64)> = l
        .sets
        .iter()
        .map(|s| (s.position, reach_from(&l.chunks, s.position)))
        .collect();
    let through = reaches.iter().map(|&(_, r)| r).max()?;
    let oldest = reaches.iter().find(|&&(_, r)| r == through)?.0;
    let newest = reaches
        .iter()
        .map(|&(p, _)| p)
        .filter(|&p| p <= through)
        .max()?;
    Some((oldest, newest, through))
}

/// Chunks covering `[from, to)` in order; errors on a gap.
pub fn plan_chain(chunks: &[(u64, u64)], from: u64, to: u64) -> Result<Vec<(u64, u64)>, TierError> {
    let mut plan = Vec::new();
    let mut c = from;
    while c < to {
        let Some(ch) = step(chunks, c) else {
            return Err(TierError::Refused(format!(
                "tier chain has a gap at position {c} (wanted [{from}, {to}))"
            )));
        };
        plan.push(ch);
        c = ch.1;
    }
    Ok(plan)
}

/// Open and stream one chunk: header checks, per-frame walk, optional
/// byte-equality against the overlap with `prev`. `sink` gets every raw byte
/// of the chunk, frame-aligned pieces, with the position of the first byte.
fn walk_chunk(
    t: &dyn BackupTarget,
    ch: (u64, u64),
    prev: Option<(u64, u64)>,
    sink: &mut RangeSink,
) -> Result<(), TierError> {
    let name = chunk_object(ch.0, ch.1);
    let open = |c: (u64, u64)| -> Result<ObjectReader<std::fs::File>, TierError> {
        let n = chunk_object(c.0, c.1);
        let f = t
            .open(&n)?
            .ok_or_else(|| corrupt(n.clone(), "missing (tier_object_missing)"))?;
        let r = ObjectReader::open(f)?;
        let h = r.header();
        if h.magic != MAGIC_CHUNK {
            return Err(corrupt(n, "not a chunk object"));
        }
        if (h.from, h.to) != c || h.raw_len != c.1 - c.0 {
            return Err(corrupt(
                n,
                format!(
                    "header [{}, {}) len {} disagrees with its name",
                    h.from, h.to, h.raw_len
                ),
            ));
        }
        Ok(r)
    };
    let mut r = open(ch)?;
    // The previous chunk's reader, positioned at the start of the overlap.
    let mut other = match prev {
        Some(p) if ch.0 < p.1 && p.0 <= ch.0 => {
            let mut o = open(p)?;
            std::io::copy(&mut (&mut o).take(ch.0 - p.0), &mut std::io::sink())?;
            Some((o, p.1))
        }
        _ => None,
    };
    let mut buf: Vec<u8> = Vec::new();
    let mut base = ch.0; // position of buf[0]
    let mut chunk = vec![0u8; 1 << 20];
    let mut eof = false;
    while !eof || !buf.is_empty() {
        if !eof {
            let n = r.read(&mut chunk)?;
            if n == 0 {
                eof = true;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        // consume whole frames
        let mut off = 0usize;
        loop {
            let rest = &buf[off..];
            if rest.is_empty() {
                break;
            }
            if rest.len() < HEADER_LEN {
                if eof {
                    return Err(corrupt(&name, "frame header runs past the chunk end"));
                }
                break;
            }
            let len = read_header(rest).length as usize;
            if len < HEADER_LEN {
                return Err(corrupt(
                    &name,
                    format!("frame at {} has length {len}", base + off as u64),
                ));
            }
            let stride = align_frame_len(len);
            if stride > rest.len() {
                if eof {
                    return Err(corrupt(&name, "frame runs past the chunk end"));
                }
                break;
            }
            off += stride;
        }
        if off > 0 {
            let piece = &buf[..off];
            if let Some((o, oend)) = other.as_mut()
                && base < *oend
            {
                let n = (piece.len() as u64).min(*oend - base) as usize;
                let mut theirs = vec![0u8; n];
                o.read_exact(&mut theirs)?;
                if let Some(i) = theirs.iter().zip(&piece[..n]).position(|(a, b)| a != b) {
                    return Err(TierError::OverlapMismatch(format!(
                        "{} vs {} at position {}",
                        chunk_object(prev.unwrap().0, prev.unwrap().1),
                        name,
                        base + i as u64
                    )));
                }
            }
            sink(base, piece)?;
            base += off as u64;
            buf.drain(..off);
        } else if eof {
            break;
        }
    }
    if base != ch.1 {
        return Err(corrupt(
            &name,
            format!("frames end at {base}, chunk ends at {}", ch.1),
        ));
    }
    Ok(())
}

/// Streams the raw log bytes `[from, to)` into `sink`, verifying per
/// [`walk_chunk`], and that overlapping chunks agree byte for byte.
pub fn read_range(
    t: &dyn BackupTarget,
    chunks: &[(u64, u64)],
    from: u64,
    to: u64,
    sink: &mut RangeSink,
) -> Result<(), TierError> {
    let plan = plan_chain(chunks, from, to)?;
    let mut frontier = from;
    let mut prev = None;
    for ch in plan {
        walk_chunk(t, ch, prev, &mut |pos, bytes| {
            let end = pos + bytes.len() as u64;
            let (lo, hi) = (pos.max(frontier), end.min(to));
            if lo < hi {
                // Pieces are frame-aligned, not necessarily aligned to
                // `from`/`to`; a mid-frame bound is simply sliced.
                sink(lo, &bytes[(lo - pos) as usize..(hi - pos) as usize])?;
                frontier = hi;
            }
            Ok(())
        })?;
        prev = Some(ch);
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub sets: usize,
    pub chunks: usize,
    pub findings: Vec<String>,
    /// Informational lines that are not findings (ruling RF1: the tier's
    /// restore fences) — `tier verify` prints them and still exits 0.
    pub info: Vec<String>,
}

fn stream_object(
    t: &dyn BackupTarget,
    path: &str,
    deep_hash_skip: Option<usize>,
    deep: bool,
) -> Result<(u64, Option<(u64, u64)>), TierError> {
    // Returns (raw_len, deep: (envelope position, hash)).
    let f = t
        .open(path)?
        .ok_or_else(|| corrupt(path, "missing (tier_object_missing)"))?;
    let mut r = ObjectReader::open(f)?;
    if r.header().magic != MAGIC_ARTIFACT {
        return Err(corrupt(path, "not an artifact object"));
    }
    let raw_len = r.header().raw_len;
    let mut buf = vec![0u8; 1 << 20];
    if !deep {
        while r.read(&mut buf)? > 0 {}
        return Ok((raw_len, None));
    }
    let skip = deep_hash_skip.unwrap_or(0);
    let mut head = vec![0u8; skip];
    r.read_exact(&mut head)
        .map_err(|_| corrupt(path, "shorter than its envelope"))?;
    let pos = if skip > 0 {
        decode_snapshot_envelope(&head)
            .map_err(|e| corrupt(path, format!("envelope: {e}")))?
            .position
    } else {
        0
    };
    let mut h = sha2::Sha256::new();
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    let d = h.finalize();
    Ok((
        raw_len,
        Some((pos, u64::from_le_bytes(d[..8].try_into().unwrap()))),
    ))
}

pub fn verify(t: &dyn BackupTarget, deep: bool) -> Result<VerifyReport, TierError> {
    let l = list_tier(t)?;
    let mut rep = VerifyReport {
        sets: l.sets.len(),
        chunks: l.chunks.len(),
        findings: Vec::new(),
        info: Vec::new(),
    };
    for &q in &l.fences {
        rep.info.push(format!(
            "tier_fenced: {} — a restore rolled this tier's history back to {q}; writers refuse \
             this target (backup_target_fenced), reads and further restores are unaffected",
            fence_object(q)
        ));
    }
    for &p in &l.corrupt_sets {
        let why = match read_label(t, p) {
            Ok(Some(Err(why))) => why,
            Ok(_) => "changed while verifying".to_string(),
            Err(e) => e.to_string(),
        };
        rep.findings.push(format!(
            "tier_set_label_corrupt: sets/{p:020}/{SET_FILE}: {why} (the set is never a restore \
             start)"
        ));
    }
    for p in &l.incomplete_sets {
        rep.findings.push(format!(
            "tier_set_incomplete: sets/{p:020} has no {SET_FILE} label"
        ));
    }
    for s in &l.sets {
        let p = s.position;
        for r in &s.rows {
            let path = row_object(p, r.row);
            match stream_object(t, &path, Some(SNAPSHOT_ENVELOPE_LEN), deep) {
                Err(e) => rep.findings.push(format!("{path}: {e}")),
                Ok((len, d)) => {
                    if len != r.size {
                        rep.findings.push(format!(
                            "{path}: raw length {len} != SET size {} (tier_set_mismatch)",
                            r.size
                        ));
                    }
                    if let Some((ep, hash)) = d {
                        if ep != p {
                            rep.findings.push(format!(
                                "{path}: envelope position {ep} != set {p} (tier_set_mismatch)"
                            ));
                        }
                        if hash != r.hash {
                            rep.findings.push(format!(
                                "{path}: hash {hash:#018x} != SET {:#018x} (tier_set_mismatch)",
                                r.hash
                            ));
                        }
                    }
                }
            }
        }
        let path = cluster_object(p);
        match stream_object(t, &path, None, deep) {
            Err(e) => rep.findings.push(format!("{path}: {e}")),
            Ok((len, d)) => {
                if len != s.cluster_size {
                    rep.findings.push(format!(
                        "{path}: raw length {len} != SET size {} (tier_set_mismatch)",
                        s.cluster_size
                    ));
                }
                if let Some((_, hash)) = d
                    && hash != s.cluster_hash
                {
                    rep.findings.push(format!(
                        "{path}: hash {hash:#018x} != SET {:#018x} (tier_set_mismatch)",
                        s.cluster_hash
                    ));
                }
            }
        }
    }
    let mut bad = std::collections::BTreeSet::new();
    for &ch in &l.chunks {
        if let Err(e) = walk_chunk(t, ch, None, &mut |_, _| Ok(())) {
            rep.findings
                .push(format!("{}: {e}", chunk_object(ch.0, ch.1)));
            bad.insert(ch);
        }
    }
    for (i, &a) in l.chunks.iter().enumerate() {
        for &b in &l.chunks[i + 1..] {
            if b.0 >= a.1 {
                break;
            }
            if bad.contains(&a) || bad.contains(&b) {
                continue;
            }
            if let Err(e) = walk_chunk(t, b, Some(a), &mut |_, _| Ok(())) {
                rep.findings.push(e.to_string());
            }
        }
    }
    Ok(rep)
}

pub fn render_show(info: &TierInfo, l: &TierListing) -> String {
    let mut s = format!(
        "tier {:#018x} app_id={} created_ns={}\n",
        info.tier_id, info.app_id, info.created_ns
    );
    s.push_str(&format!(
        "sets: {} complete, {} incomplete\n",
        l.sets.len(),
        l.incomplete_sets.len()
    ));
    for set in &l.sets {
        s.push_str(&format!(
            "  set {} kind={:?} time_ns={} rows={} reach={}\n",
            set.position,
            set.kind,
            set.time_ns,
            set.rows.len(),
            reach_from(&l.chunks, set.position)
        ));
    }
    for p in &l.incomplete_sets {
        s.push_str(&format!("  set {p} INCOMPLETE (no label)\n"));
    }
    for p in &l.corrupt_sets {
        s.push_str(&format!(
            "  set {p} CORRUPT (its SET label does not parse; tier_set_label_corrupt)\n"
        ));
    }
    s.push_str(&format!("chunks: {}\n", l.chunks.len()));
    for (f, t) in &l.chunks {
        s.push_str(&format!("  [{f}, {t})\n"));
    }
    match watermark_of(l) {
        Some((o, n, th)) => s.push_str(&format!(
            "watermark: oldest_set={o} newest_set={n} through={th}\n"
        )),
        None => s.push_str("watermark: none (no complete set)\n"),
    }
    for q in &l.fences {
        s.push_str(&format!(
            "fence: {q} (a restore rolled the history back to {q}; writers refuse this target — \
             point [backup] target at a new, empty directory)\n"
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tier::layout::{
        RowLabel, TierInfo, chunk_object, cluster_object, row_object, set_label,
    };
    use crate::tier::object::{Codec, MAGIC_ARTIFACT, MAGIC_CHUNK, write_object};
    use crate::tier::target::FsTarget;
    use std::io::Cursor;
    use uc_protocol::v2::catalog::SetKind;
    use uc_protocol::v2::frame::FRAME_TYPE_PADDING;

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    fn label(p: u64) -> SetLabel {
        SetLabel {
            position: p,
            kind: SetKind::Full,
            time_ns: 1,
            rows: vec![],
            cluster_hash: 0,
            cluster_size: 0,
        }
    }

    fn padding_frames(from: u64, to: u64, term: u32) -> Vec<u8> {
        let mut raw = Vec::new();
        for _ in 0..(to - from) / 32 {
            let mut f = [0u8; 32];
            f[0..4].copy_from_slice(&32u32.to_le_bytes());
            f[4] = FRAME_TYPE_PADDING;
            f[8..12].copy_from_slice(&term.to_le_bytes());
            raw.extend_from_slice(&f);
        }
        raw
    }

    fn put_raw_chunk(t: &FsTarget, from: u64, to: u64, raw: Vec<u8>) {
        t.put_if_absent(&chunk_object(from, to), &mut |f| {
            let n = raw.len() as u64;
            write_object(
                f,
                MAGIC_CHUNK,
                Codec::None,
                1,
                from,
                to,
                &mut Cursor::new(&raw),
                n,
            )
        })
        .unwrap();
    }

    fn put_chunk_with_term(t: &FsTarget, from: u64, to: u64, term: u32) {
        put_raw_chunk(t, from, to, padding_frames(from, to, term));
    }

    fn put_chunk(t: &FsTarget, from: u64, to: u64) {
        put_chunk_with_term(t, from, to, 1);
    }

    fn put_set_files_without_label(t: &FsTarget, p: u64) {
        let raw = vec![7u8; 100];
        t.put_if_absent(&row_object(p, 0), &mut |f| {
            write_object(
                f,
                MAGIC_ARTIFACT,
                Codec::None,
                1,
                p,
                p,
                &mut Cursor::new(&raw),
                100,
            )
        })
        .unwrap();
    }

    fn put_label(t: &FsTarget, l: &SetLabel) {
        t.put_if_absent(&set_label(l.position), &mut |f| {
            use std::io::Write;
            let s = l.render();
            f.write_all(s.as_bytes())?;
            Ok(s.len() as u64)
        })
        .unwrap();
    }

    #[test]
    fn reach_stops_at_a_gap_and_a_new_chain_starts_at_its_set() {
        let chunks = vec![(0, 64), (64, 128), (192, 256)];
        assert_eq!(reach_from(&chunks, 0), 128);
        assert_eq!(reach_from(&chunks, 192), 256);
        assert_eq!(reach_from(&chunks, 100), 128);
        assert_eq!(reach_from(&chunks, 130), 130);
        let l = TierListing {
            sets: vec![label(0), label(192)],
            incomplete_sets: vec![],
            corrupt_sets: vec![],
            fences: vec![],
            chunks,
        };
        assert_eq!(
            watermark_of(&l),
            Some((192, 192, 256)),
            "the newer chain reaches further"
        );
    }

    #[test]
    fn a_set_inside_a_chunk_reaches_through_it() {
        let chunks = vec![(0, 4096), (4096, 8192)];
        assert_eq!(reach_from(&chunks, 1000), 8192);
        let l = TierListing {
            sets: vec![label(0), label(1000)],
            incomplete_sets: vec![],
            corrupt_sets: vec![],
            fences: vec![],
            chunks: chunks.clone(),
        };
        assert_eq!(watermark_of(&l), Some((0, 1000, 8192)));
        assert_eq!(
            plan_chain(&chunks, 1000, 8192).unwrap(),
            vec![(0, 4096), (4096, 8192)]
        );
    }

    #[test]
    fn overlapping_chunks_are_planned_greedily_and_must_match() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        put_chunk(&t, 0, 128);
        put_chunk(&t, 64, 256);
        let l = list_tier(&t).unwrap();
        assert_eq!(
            plan_chain(&l.chunks, 0, 256).unwrap(),
            vec![(0, 128), (64, 256)]
        );
        let mut got = 0u64;
        read_range(&t, &l.chunks, 32, 224, &mut |_, b| {
            got += b.len() as u64;
            Ok(())
        })
        .unwrap();
        assert_eq!(got, 192);
        assert!(verify(&t, false).unwrap().findings.is_empty());
    }

    #[test]
    fn an_overlap_that_differs_is_a_named_finding() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        put_chunk(&t, 0, 128);
        put_chunk_with_term(&t, 64, 256, 9);
        let r = verify(&t, false).unwrap();
        assert!(
            r.findings
                .iter()
                .any(|f| f.contains("tier_overlap_mismatch")),
            "{:?}",
            r.findings
        );
        let l = list_tier(&t).unwrap();
        let e = read_range(&t, &l.chunks, 0, 256, &mut |_, _| Ok(())).unwrap_err();
        assert!(matches!(e, TierError::OverlapMismatch(_)), "{e}");
    }

    #[test]
    fn a_set_without_its_label_is_incomplete_and_never_a_start() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        put_set_files_without_label(&t, 64);
        let l = list_tier(&t).unwrap();
        assert!(l.sets.is_empty());
        assert_eq!(l.incomplete_sets, vec![64]);
        assert_eq!(watermark_of(&l), None);
        assert!(!verify(&t, false).unwrap().findings.is_empty());
    }

    /// F4: one SET that does not parse (or names another position) does
    /// not brick the tier: that set is listed as corrupt and left out of
    /// `sets`, every other set and chunk still lists, and verify names it.
    #[test]
    fn an_unparsable_set_label_is_corrupt_not_fatal() {
        let d = tmp();
        let t = FsTarget::new(d.path());
        put_label(&t, &label(64));
        put_chunk(&t, 64, 128);
        put_set_files_without_label(&t, 96);
        t.put_if_absent(&set_label(96), &mut |f| {
            use std::io::Write;
            f.write_all(b"garbage\n")?;
            Ok(8)
        })
        .unwrap();
        let mut liar = label(160);
        liar.position = 161;
        t.put_if_absent(&set_label(160), &mut |f| {
            use std::io::Write;
            let s = liar.render();
            f.write_all(s.as_bytes())?;
            Ok(s.len() as u64)
        })
        .unwrap();
        let l = list_tier(&t).expect("a corrupt SET must not fail the listing");
        assert_eq!(
            l.sets.iter().map(|s| s.position).collect::<Vec<_>>(),
            vec![64]
        );
        assert_eq!(l.corrupt_sets, vec![96, 160]);
        assert_eq!(watermark_of(&l), Some((64, 64, 128)));
        let r = verify(&t, false).unwrap();
        assert_eq!(
            r.findings
                .iter()
                .filter(|f| f.contains("tier_set_label_corrupt"))
                .count(),
            2,
            "{:?}",
            r.findings
        );
        let shown = render_show(
            &TierInfo {
                app_id: "kv".into(),
                tier_id: 1,
                created_ns: 0,
            },
            &l,
        );
        assert!(shown.contains("CORRUPT"), "{shown}");
    }

    #[test]
    fn a_gap_is_an_error_and_a_misaligned_chunk_is_a_finding() {
        assert!(plan_chain(&[(0, 64), (128, 192)], 0, 192).is_err());
        let d = tmp();
        let t = FsTarget::new(d.path());
        // frame claims 64 bytes but the chunk ends after 32
        let mut raw = vec![0u8; 32];
        raw[0..4].copy_from_slice(&64u32.to_le_bytes());
        put_raw_chunk(&t, 0, 32, raw);
        let r = verify(&t, false).unwrap();
        assert!(!r.findings.is_empty(), "{:?}", r.findings);
    }

    #[test]
    fn verify_checks_set_objects_and_deep_rehashes() {
        use sha2::Digest;
        let d = tmp();
        let t = FsTarget::new(d.path());
        let payload = vec![5u8; 300];
        let mut row = Vec::new();
        row.extend_from_slice(b"ULTSNAP2");
        row.extend_from_slice(&64u64.to_le_bytes());
        row.extend_from_slice(&1u32.to_le_bytes());
        row.extend_from_slice(&[0u8; 4]);
        row.extend_from_slice(&payload);
        let h = |b: &[u8]| u64::from_le_bytes(sha2::Sha256::digest(b)[..8].try_into().unwrap());
        let cluster = vec![3u8; 90];
        let put = |path: String, raw: &[u8]| {
            t.put_if_absent(&path, &mut |f| {
                write_object(
                    f,
                    MAGIC_ARTIFACT,
                    Codec::Zstd,
                    1,
                    64,
                    64,
                    &mut Cursor::new(raw),
                    raw.len() as u64,
                )
            })
            .unwrap();
        };
        put(row_object(64, 0), &row);
        put(cluster_object(64), &cluster);
        let mut l = label(64);
        l.rows = vec![RowLabel {
            row: 0,
            hash: h(&payload),
            version: 1,
            size: row.len() as u64,
        }];
        l.cluster_hash = h(&cluster);
        l.cluster_size = cluster.len() as u64;
        put_label(&t, &l);
        let r = verify(&t, true).unwrap();
        assert_eq!((r.sets, r.chunks), (1, 0));
        assert!(r.findings.is_empty(), "{:?}", r.findings);
        // a lying hash is caught only by --deep
        let d2 = tmp();
        let t2 = FsTarget::new(d2.path());
        std::fs::create_dir_all(d2.path().join(crate::tier::layout::set_dir(64))).unwrap();
        for p in [row_object(64, 0), cluster_object(64)] {
            std::fs::copy(d.path().join(&p), d2.path().join(&p)).unwrap();
        }
        l.cluster_hash ^= 1;
        put_label(&t2, &l);
        assert!(verify(&t2, false).unwrap().findings.is_empty());
        assert!(!verify(&t2, true).unwrap().findings.is_empty());
        let l2 = list_tier(&t2).unwrap();
        let s = render_show(
            &crate::tier::layout::TierInfo {
                app_id: "kv".into(),
                tier_id: 1,
                created_ns: 0,
            },
            &l2,
        );
        assert!(s.contains("kv") && s.contains("64"), "{s}");
    }
}
