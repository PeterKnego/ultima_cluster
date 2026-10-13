//! Tier layout (spec §4.1): object names, the `TIER` identity file and the
//! per-set `SET` label.
use super::TierError;
use uc_protocol::v2::catalog::{RowVerdict, SetEntry, SetKind};

pub const TIER_FILE: &str = "TIER";
pub const SET_FILE: &str = "SET";
pub const TIER_FORMAT: &str = "uc2-tier-v1";

pub fn set_dir(p: u64) -> String {
    format!("sets/{p:020}")
}

pub fn row_object(p: u64, row: u8) -> String {
    format!("sets/{p:020}/row-{row}.ultsnap")
}

pub fn cluster_object(p: u64) -> String {
    format!("sets/{p:020}/cluster.ultcluster")
}

pub fn set_label(p: u64) -> String {
    format!("sets/{p:020}/{SET_FILE}")
}

pub fn chunk_object(from: u64, to: u64) -> String {
    format!("journal/{from:020}-{to:020}.ultj")
}

/// The directory of restore fences (ruling RF1).
pub const FENCE_DIR: &str = "fence";

/// Ruling RF1: the write-once object a restore below the tier's reach
/// leaves at `fence/<Q:020>`. Its presence makes every writer refuse the
/// target (`backup_target_fenced`); reads ignore it.
pub fn fence_object(q: u64) -> String {
    format!("{FENCE_DIR}/{q:020}")
}

pub fn parse_fence_name(name: &str) -> Option<u64> {
    parse_pos(name)
}

fn parse_pos(s: &str) -> Option<u64> {
    if s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

pub fn parse_set_dir(name: &str) -> Option<u64> {
    parse_pos(name)
}

pub fn parse_chunk_name(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".ultj")?;
    let (a, b) = stem.split_once('-')?;
    let (from, to) = (parse_pos(a)?, parse_pos(b)?);
    (to > from).then_some((from, to))
}

fn bad(what: &str, detail: impl Into<String>) -> TierError {
    TierError::Refused(format!("tier {what}: {}", detail.into()))
}

/// Split `key=value\n` text into pairs; a line without `=` is refused.
fn pairs<'a>(what: &str, text: &'a str) -> Result<Vec<(&'a str, &'a str)>, TierError> {
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            l.split_once('=')
                .ok_or_else(|| bad(what, format!("malformed line {l:?}")))
        })
        .collect()
}

fn num(what: &str, k: &str, v: &str) -> Result<u64, TierError> {
    let r = match v.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16),
        None => v.parse(),
    };
    r.map_err(|_| bad(what, format!("{k}: bad number {v:?}")))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierInfo {
    pub app_id: String,
    pub tier_id: u64,
    pub created_ns: u64,
}

impl TierInfo {
    pub fn render(&self) -> String {
        format!(
            "format={TIER_FORMAT}\napp_id={}\ntier_id={:#018x}\ncreated_ns={}\n",
            self.app_id, self.tier_id, self.created_ns
        )
    }

    pub fn parse(text: &str) -> Result<TierInfo, TierError> {
        let (mut format, mut app, mut id, mut created) = (None, None, None, None);
        for (k, v) in pairs("TIER", text)? {
            let slot = match k {
                "format" => &mut format,
                "app_id" => &mut app,
                "tier_id" => &mut id,
                "created_ns" => &mut created,
                _ => return Err(bad("TIER", format!("unknown key {k:?}"))),
            };
            if slot.replace(v).is_some() {
                return Err(bad("TIER", format!("duplicate key {k:?}")));
            }
        }
        let miss = |k: &str| bad("TIER", format!("missing key {k:?}"));
        let format = format.ok_or_else(|| miss("format"))?;
        if format != TIER_FORMAT {
            return Err(bad(
                "TIER",
                format!("format {format:?}, expected {TIER_FORMAT:?}"),
            ));
        }
        let tier_id = num("TIER", "tier_id", id.ok_or_else(|| miss("tier_id"))?)?;
        if tier_id == 0 {
            return Err(bad("TIER", "tier_id must be non-zero"));
        }
        Ok(TierInfo {
            app_id: app.ok_or_else(|| miss("app_id"))?.to_string(),
            tier_id,
            created_ns: num(
                "TIER",
                "created_ns",
                created.ok_or_else(|| miss("created_ns"))?,
            )?,
        })
    }
}

/// The body of a `fence/<Q>` object: `key=value` lines. Audit only — the
/// object's NAME is what writers key on; nothing parses the body to decide.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceInfo {
    /// The restore point the tier's history was rolled back to.
    pub q: u64,
    /// The tier's reach when the fence was written (history past `q` up to
    /// here is the abandoned branch).
    pub reach: u64,
    pub created_ns: u64,
    /// The restoring cluster's `app_id` (the tier's own).
    pub app_id: String,
    /// The instance directory the restore built.
    pub restored_into: String,
}

impl FenceInfo {
    pub fn render(&self) -> String {
        // A path with a newline would break the line format: keep it on one.
        let into = self.restored_into.replace(['\n', '\r'], " ");
        format!(
            "q={}\nreach={}\ncreated_ns={}\napp_id={}\nrestored_into={into}\n",
            self.q, self.reach, self.created_ns, self.app_id
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowLabel {
    pub row: u8,
    pub hash: u64,
    pub version: u32,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetLabel {
    pub position: u64,
    pub kind: SetKind,
    pub time_ns: u64,
    pub rows: Vec<RowLabel>,
    pub cluster_hash: u64,
    pub cluster_size: u64,
    /// The cluster's term map strictly below `position`: `(term, base)`.
    pub lineage: Vec<(u32, u64)>,
}

impl SetLabel {
    pub fn render(&self) -> String {
        let kind = match self.kind {
            SetKind::Full => "full",
            SetKind::Standby => "standby",
        };
        let mut s = format!(
            "position={}\nkind={kind}\ntime_ns={}\n",
            self.position, self.time_ns
        );
        s.push_str(&format!("lineage={}\n", render_lineage(&self.lineage)));
        let mut rows = self.rows.clone();
        rows.sort_by_key(|r| r.row);
        for r in rows {
            s.push_str(&format!(
                "row.{0}.hash={1:#018x}\nrow.{0}.version={2:#x}\nrow.{0}.size={3}\n",
                r.row, r.hash, r.version, r.size
            ));
        }
        s.push_str(&format!(
            "cluster.hash={:#018x}\ncluster.size={}\n",
            self.cluster_hash, self.cluster_size
        ));
        s
    }

    pub fn parse(text: &str) -> Result<SetLabel, TierError> {
        let (mut position, mut kind, mut time_ns) = (None, None, None);
        let (mut chash, mut csize) = (None, None);
        let mut lineage: Option<&str> = None;
        // row id -> [hash, version, size]
        let mut rows: std::collections::BTreeMap<u8, [Option<u64>; 3]> = Default::default();
        for (k, v) in pairs("SET", text)? {
            let dup = |present: bool| {
                if present {
                    Err(bad("SET", format!("duplicate key {k:?}")))
                } else {
                    Ok(())
                }
            };
            match k {
                "position" => dup(position.replace(num("SET", k, v)?).is_some())?,
                "time_ns" => dup(time_ns.replace(num("SET", k, v)?).is_some())?,
                "cluster.hash" => dup(chash.replace(num("SET", k, v)?).is_some())?,
                "cluster.size" => dup(csize.replace(num("SET", k, v)?).is_some())?,
                "lineage" => dup(lineage.replace(v).is_some())?,
                "kind" => {
                    let kd = match v {
                        "full" => SetKind::Full,
                        "standby" => SetKind::Standby,
                        _ => return Err(bad("SET", format!("kind {v:?}"))),
                    };
                    dup(kind.replace(kd).is_some())?
                }
                _ => {
                    let rest = k
                        .strip_prefix("row.")
                        .ok_or_else(|| bad("SET", format!("unknown key {k:?}")))?;
                    let (id, field) = rest
                        .split_once('.')
                        .ok_or_else(|| bad("SET", format!("unknown key {k:?}")))?;
                    let id: u8 = id
                        .parse()
                        .map_err(|_| bad("SET", format!("row id in {k:?}")))?;
                    let idx = match field {
                        "hash" => 0,
                        "version" => 1,
                        "size" => 2,
                        _ => return Err(bad("SET", format!("unknown key {k:?}"))),
                    };
                    let slot = &mut rows.entry(id).or_default()[idx];
                    dup(slot.replace(num("SET", k, v)?).is_some())?
                }
            }
        }
        let miss = |k: &str| bad("SET", format!("missing key {k:?}"));
        let position = position.ok_or_else(|| miss("position"))?;
        let lineage = parse_lineage(
            lineage.ok_or_else(|| bad("SET", "set_lineage_missing"))?,
            position,
        )?;
        let mut out = Vec::new();
        for (row, [h, v, s]) in rows {
            let version = u32::try_from(v.ok_or_else(|| miss(&format!("row.{row}.version")))?)
                .map_err(|_| bad("SET", format!("row.{row}.version overflows u32")))?;
            out.push(RowLabel {
                row,
                hash: h.ok_or_else(|| miss(&format!("row.{row}.hash")))?,
                version,
                size: s.ok_or_else(|| miss(&format!("row.{row}.size")))?,
            });
        }
        Ok(SetLabel {
            position,
            kind: kind.ok_or_else(|| miss("kind"))?,
            time_ns: time_ns.ok_or_else(|| miss("time_ns"))?,
            rows: out,
            cluster_hash: chash.ok_or_else(|| miss("cluster.hash"))?,
            cluster_size: csize.ok_or_else(|| miss("cluster.size"))?,
            lineage,
        })
    }

    /// Every field equal except `lineage`, which agrees iff the shorter is a
    /// suffix of the longer (PF1).
    pub fn agrees_with(&self, other: &SetLabel) -> bool {
        self.position == other.position
            && self.kind == other.kind
            && self.time_ns == other.time_ns
            && self.rows == other.rows
            && self.cluster_hash == other.cluster_hash
            && self.cluster_size == other.cluster_size
            && lineage_agrees(&self.lineage, &other.lineage)
    }

    /// The label of a catalog entry: rows whose verdict is `Agreed`, in row
    /// order; the cluster artifact's hash and size from `e.cluster`.
    pub fn from_catalog(e: &SetEntry, lineage: Vec<(u32, u64)>) -> SetLabel {
        SetLabel {
            position: e.position,
            kind: e.kind,
            time_ns: e.time_ns,
            rows: e
                .rows
                .iter()
                .enumerate()
                .filter(|(_, r)| r.verdict == RowVerdict::Agreed)
                .map(|(i, r)| RowLabel {
                    row: i as u8,
                    hash: r.hash,
                    version: r.version,
                    size: r.size,
                })
                .collect(),
            cluster_hash: e.cluster.hash,
            cluster_size: e.cluster.size,
            lineage,
        }
    }
}

/// Two lineages agree iff the shorter is a suffix of the longer; an empty
/// one (never a valid lineage) agrees only with another empty one.
pub fn lineage_agrees(a: &[(u32, u64)], b: &[(u32, u64)]) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if short.is_empty() {
        return long.is_empty();
    }
    long.ends_with(short)
}

/// `t:b,t:b` (decimal).
pub fn render_lineage(l: &[(u32, u64)]) -> String {
    l.iter()
        .map(|(t, b)| format!("{t}:{b}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn parse_lineage(v: &str, position: u64) -> Result<Vec<(u32, u64)>, TierError> {
    let mal = |d: String| bad("SET", format!("set_lineage_malformed: {d}"));
    if v.is_empty() {
        return Err(mal("empty".into()));
    }
    let dec = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let mut out: Vec<(u32, u64)> = Vec::new();
    for item in v.split(',') {
        let (t, b) = item
            .split_once(':')
            .filter(|(t, b)| dec(t) && dec(b))
            .ok_or_else(|| mal(format!("item {item:?} is not term:base")))?;
        let t: u32 = t.parse().map_err(|_| mal(format!("term in {item:?}")))?;
        let b: u64 = b.parse().map_err(|_| mal(format!("base in {item:?}")))?;
        if let Some(&(pt, pb)) = out.last() {
            if t <= pt {
                return Err(mal(format!("terms not strictly increasing at {item:?}")));
            }
            if b <= pb {
                return Err(mal(format!("bases not strictly increasing at {item:?}")));
            }
        }
        out.push((t, b));
    }
    let last = out.last().map_or(0, |&(_, b)| b);
    if last >= position {
        return Err(mal(format!(
            "last base {last} not below position {position}"
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::catalog::SetKind;

    #[test]
    fn names_round_trip_and_sort_by_position() {
        assert_eq!(set_dir(7), "sets/00000000000000000007");
        assert_eq!(parse_set_dir("00000000000000000007"), Some(7));
        assert_eq!(
            parse_chunk_name("00000000000000000064-00000000000000000128.ultj"),
            Some((64, 128))
        );
        assert_eq!(parse_chunk_name("64-128.ultj"), None);
        assert_eq!(
            parse_chunk_name("00000000000000000128-00000000000000000064.ultj"),
            None,
            "to > from"
        );
    }

    #[test]
    fn tier_and_set_labels_round_trip_and_refuse_garbage() {
        let t = TierInfo {
            app_id: "kv".into(),
            tier_id: 0xabc,
            created_ns: 5,
        };
        assert_eq!(TierInfo::parse(&t.render()).unwrap(), t);
        assert!(
            TierInfo::parse("format=uc2-tier-v9\napp_id=kv\ntier_id=1\ncreated_ns=0\n").is_err()
        );
        let s = SetLabel {
            position: 4096,
            kind: SetKind::Full,
            time_ns: 9,
            rows: vec![RowLabel {
                row: 0,
                hash: 1,
                version: 0x10000,
                size: 100,
            }],
            cluster_hash: 2,
            cluster_size: 50,
            lineage: vec![(1, 0)],
        };
        assert_eq!(SetLabel::parse(&s.render()).unwrap(), s);
    }

    #[test]
    fn tier_id_renders_hex_and_zero_is_refused() {
        let t = TierInfo {
            app_id: "kv".into(),
            tier_id: 0xabc,
            created_ns: 5,
        };
        assert!(t.render().contains("tier_id=0x0000000000000abc\n"));
        assert!(
            TierInfo::parse("format=uc2-tier-v1\napp_id=kv\ntier_id=0x0\ncreated_ns=0\n").is_err()
        );
    }

    #[test]
    fn set_label_parse_refuses_duplicate_missing_and_unknown_keys() {
        let s = SetLabel {
            position: 8,
            kind: SetKind::Standby,
            time_ns: 1,
            rows: vec![RowLabel {
                row: 0,
                hash: 1,
                version: 2,
                size: 3,
            }],
            cluster_hash: 4,
            cluster_size: 5,
            lineage: vec![(1, 0)],
        };
        let good = s.render();
        assert_eq!(SetLabel::parse(&good).unwrap(), s);
        assert!(
            SetLabel::parse(&format!("{good}position=9\n")).is_err(),
            "duplicate"
        );
        assert!(
            SetLabel::parse(&format!("{good}row.0.size=9\n")).is_err(),
            "duplicate row key"
        );
        assert!(
            SetLabel::parse(&format!("{good}bogus=1\n")).is_err(),
            "unknown"
        );
        let missing: String = good
            .lines()
            .filter(|l| !l.starts_with("time_ns"))
            .map(|l| format!("{l}\n"))
            .collect();
        assert!(SetLabel::parse(&missing).is_err(), "missing");
        let missing: String = good
            .lines()
            .filter(|l| !l.starts_with("row.0.hash"))
            .map(|l| format!("{l}\n"))
            .collect();
        assert!(SetLabel::parse(&missing).is_err(), "missing row key");
    }

    #[test]
    fn from_catalog_keeps_agreed_rows_by_row_id() {
        use uc_protocol::v2::catalog::RowEntry;
        let mut e = SetEntry::commanded(4096, SetKind::Full, 7);
        let row = |version, hash, size, verdict| RowEntry {
            version,
            hash,
            verdict,
            size,
        };
        e.rows[0] = row(1, 0xa, 100, RowVerdict::Agreed);
        e.rows[1] = row(9, 0xb, 999, RowVerdict::Unreported);
        e.rows[2] = row(3, 0xc, 300, RowVerdict::Agreed);
        e.cluster = row(0, 0xd, 50, RowVerdict::Agreed);
        let l = SetLabel::from_catalog(&e, vec![(1, 0)]);
        assert_eq!(
            l.rows,
            vec![
                RowLabel {
                    row: 0,
                    hash: 0xa,
                    version: 1,
                    size: 100
                },
                RowLabel {
                    row: 2,
                    hash: 0xc,
                    version: 3,
                    size: 300
                },
            ]
        );
        assert_eq!((l.position, l.kind, l.time_ns), (4096, SetKind::Full, 7));
        assert_eq!((l.cluster_hash, l.cluster_size), (0xd, 50));
    }

    fn sample_label() -> SetLabel {
        SetLabel {
            position: 100_000,
            kind: SetKind::Full,
            time_ns: 5,
            rows: vec![
                RowLabel {
                    row: 0,
                    hash: 1,
                    version: 1,
                    size: 10,
                },
                RowLabel {
                    row: 1,
                    hash: 2,
                    version: 2,
                    size: 20,
                },
            ],
            cluster_hash: 3,
            cluster_size: 30,
            lineage: vec![(1, 0)],
        }
    }

    #[test]
    fn set_renders_and_parses_its_lineage() {
        let mut l = sample_label();
        l.lineage = vec![(1, 0), (3, 4096), (4, 90_112)];
        l.position = 100_000;
        let text = l.render();
        assert!(text.contains("\nlineage=1:0,3:4096,4:90112\n"));
        assert_eq!(SetLabel::parse(&text).unwrap(), l);
    }

    #[test]
    fn a_set_without_lineage_is_refused_by_name() {
        let text = sample_label()
            .render()
            .lines()
            .filter(|l| !l.starts_with("lineage="))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let e = SetLabel::parse(&text).unwrap_err().to_string();
        assert!(e.contains("set_lineage_missing"), "{e}");
    }

    #[test]
    fn parse_refuses_a_lineage_at_or_above_the_position_or_out_of_order() {
        for (bad, why) in [
            ("lineage=1:0,2:200000", "above position"),
            ("lineage=1:0,2:100000", "at position - RA1: strictly below"),
            ("lineage=2:0,1:4096", "terms not increasing"),
            ("lineage=1:4096,2:0", "bases not increasing"),
            ("lineage=", "empty"),
            ("lineage=1-0", "shape"),
            ("lineage=1:0x10", "hex is not decimal"),
        ] {
            let mut l = sample_label();
            l.position = 100_000;
            l.lineage = vec![(1, 0)];
            let text = l.render().replace("lineage=1:0", bad);
            let e = SetLabel::parse(&text).unwrap_err().to_string();
            assert!(e.contains("set_lineage_malformed"), "{why}: {e}");
        }
    }

    #[test]
    fn lineage_agreement_is_suffix_agreement() {
        let full = [(1u32, 0u64), (2, 10), (3, 20)];
        assert!(lineage_agrees(&full, &full[1..]));
        assert!(lineage_agrees(&full[1..], &full));
        assert!(!lineage_agrees(&full, &[(1, 0), (3, 20)]));
        assert!(!lineage_agrees(&full, &[(2, 11), (3, 20)]));
        assert!(!lineage_agrees(&full, &[]));
    }
}
