// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl cluster` / `catalog`: the cluster view (storage-service project 4,
//! spec §6.3). Live over `<instance_dir>/obs.sock` when the node runs; else
//! built offline from the newest cluster artifact through the node's own
//! builder (`uc_node::view::offline`), so the two cannot drift.
//!
//! `--json` prints the node's body verbatim (only its `schema` is checked),
//! and `catalog --json` filters that body as a `serde_json::Value`, so a
//! field a newer node adds under the same schema survives either way.

use serde::Serialize;
use serde_json::Value;
use uc_node::view::ClusterViewDoc;
use uc_node::view::build::RowNames;
use uc_node::view::model::Source;

use crate::CommonArgs;

/// The document `cluster`/`catalog`/the `show` commands read.
pub enum Fetched {
    Live(ClusterViewDoc),
    Offline(ClusterViewDoc),
    NoArtifact,
}

/// The same three outcomes, with the live answer kept as the node's own
/// bytes — what `--json` prints.
enum Body {
    Live(String),
    Offline(Box<ClusterViewDoc>),
    NoArtifact,
}

/// Refuse a document under any schema but ours, by name (Review Focus 4).
fn check_schema(v: &Value) -> anyhow::Result<()> {
    let schema = v["schema"].as_str().unwrap_or("<missing>");
    if schema != uc_node::view::SCHEMA {
        anyhow::bail!(
            "the node serves cluster view schema {schema}; this uc2ctl reads {} — use the uc2ctl that matches the node",
            uc_node::view::SCHEMA
        );
    }
    Ok(())
}

/// The body as a JSON value, schema checked.
fn parse_value(body: &str) -> anyhow::Result<Value> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| anyhow::anyhow!("the cluster view is not JSON: {e}"))?;
    check_schema(&v)?;
    Ok(v)
}

/// The body as the typed document, schema checked first.
pub fn parse(body: &str) -> anyhow::Result<ClusterViewDoc> {
    let v = parse_value(body)?;
    serde_json::from_value(v).map_err(|e| anyhow::anyhow!("reading the cluster view: {e}"))
}

/// Row names for the offline view: from a readable (possibly stale)
/// `cnc2.dat`, else none (planning ruling PR13).
fn offline_names(common: &CommonArgs) -> RowNames {
    match crate::open(common) {
        Ok(cnc) => cnc
            .service_names()
            .map(|n| n.map(|n| n.as_str().to_string())),
        Err(_) => Default::default(),
    }
}

fn fetch_body(common: &CommonArgs, require_live_when_held: bool) -> anyhow::Result<Body> {
    use uc_node::ipc::{InstanceLock, probe_instance_lock};
    let dir = &common.instance_dir;
    // Only a held lock means a live node: a socket file a killed node left
    // behind is never dialled (it would refuse anyway).
    if matches!(probe_instance_lock(dir)?, InstanceLock::Held) {
        match uc_node::obs::socket::get(dir, "/cluster") {
            Ok((200, body)) => return Ok(Body::Live(body)),
            Ok((code, _)) => anyhow::bail!("the node answered /cluster with HTTP {code}"),
            Err(e) if require_live_when_held => anyhow::bail!(
                "node running but its obs socket is not bound (starting, or not run by uc2-node): {e}"
            ),
            Err(_) => {}
        }
    }
    let doc = uc_node::view::offline::render_offline(dir, &offline_names(common))
        .map_err(|e| anyhow::anyhow!("reading the cluster artifact: {e}"))?;
    Ok(match doc {
        Some(doc) => Body::Offline(Box::new(doc)),
        None => Body::NoArtifact,
    })
}

/// `require_live_when_held`: `cluster`/`catalog` refuse a held lock with no
/// socket (PR10); the `show` commands pass `false` and fall back offline.
pub fn fetch(common: &CommonArgs, require_live_when_held: bool) -> anyhow::Result<Fetched> {
    Ok(match fetch_body(common, require_live_when_held)? {
        Body::Live(body) => Fetched::Live(parse(&body)?),
        Body::Offline(doc) => Fetched::Offline(*doc),
        Body::NoArtifact => Fetched::NoArtifact,
    })
}

/// The document a `show` command renders (PR9/PR10): live, else the offline
/// one with the silent fallback; `None` when there is neither. An offline
/// document announces itself once on STDERR (P13) so stdout stays exactly
/// what the renderer prints.
pub fn show_doc(common: &CommonArgs) -> anyhow::Result<Option<ClusterViewDoc>> {
    Ok(match fetch(common, false)? {
        Fetched::Live(d) => Some(d),
        Fetched::Offline(d) => {
            eprintln!("offline: newest artifact at {}", d.view_position);
            Some(d)
        }
        Fetched::NoArtifact => None,
    })
}

/// An enum's JSON word (`full`, `no_majority`, `leader`, …) — text output
/// uses the same words the document does (ruling P18), never `Debug`.
fn word<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(Value::String(s)) => s,
        _ => "?".to_string(),
    }
}

/// The attention codes about storage — the subset `catalog` shows.
const STORAGE_CODES: [&str; 6] = [
    "catalog_empty",
    "set_stalled",
    "set_diverged",
    "purge_gate_released",
    "tier_behind",
    "node_stale",
];

/// `catalog --json`: the storage part of the document under the same
/// schema, cut from the JSON value so unknown (newer) fields pass through.
fn catalog_value(v: &Value) -> Value {
    let attention: Vec<Value> = v["attention"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|x| {
                    x["code"]
                        .as_str()
                        .is_some_and(|c| STORAGE_CODES.contains(&c))
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    serde_json::json!({
        "schema": v["schema"],
        "source": v["source"],
        "node_id": v["node_id"],
        "role": v["role"],
        "view_position": v["view_position"],
        "commit": v["commit"],
        "read_at": v["read_at"],
        "catalog": v["cluster"]["catalog"],
        "tiers": v["cluster"]["tiers"],
        "purge_gate": v["cluster"]["purge_gate"],
        "holdings": v["self"]["holdings"],
        "nodes": v["nodes"],
        "attention": attention,
    })
}

fn header(doc: &ClusterViewDoc) -> String {
    match doc.source {
        Source::Live => format!(
            "source=live node={} role={} joined={} view_position={} commit={}",
            doc.node_id.map_or("?".into(), |n| n.to_string()),
            doc.role.as_ref().map_or("?".into(), word),
            doc.joined.map_or("?".into(), |j| j.to_string()),
            doc.view_position,
            doc.commit.map_or("?".into(), |c| c.to_string()),
        ),
        Source::Artifact => format!(
            "source=artifact (offline: newest artifact at {})",
            doc.view_position
        ),
    }
}

fn label(name: &Option<String>, row: u8) -> String {
    name.clone().unwrap_or_else(|| format!("row{row}"))
}

/// The header and the storage part, without attention lines.
fn storage_text(doc: &ClusterViewDoc) -> String {
    use std::fmt::Write;
    let c = &doc.cluster.catalog;
    let mut s = header(doc) + "\n";
    let _ = writeln!(
        s,
        "catalog retain_sets={} empty={} floor={}",
        c.retain_sets,
        // Ruling P32: `null` only offline — the artifact cannot show its own
        // set's completion, so emptiness is unknown unless an earlier set
        // completed.
        c.empty.map_or_else(
            || match doc.source {
                Source::Artifact => "unknown (offline)".into(),
                Source::Live => "unknown".into(),
            },
            |e| e.to_string()
        ),
        c.floor.map_or("none".into(), |f| f.to_string())
    );
    for set in &c.sets {
        let _ = writeln!(
            s,
            "  set={} kind={} state={} agreed={} stalled={} size={} held_by={}{}",
            set.position,
            word(&set.kind),
            word(&set.state),
            set.agreed,
            set.stalled,
            set.size.map_or("unknown".into(), |b| b.to_string()),
            set.held_by
                .as_ref()
                .map_or("(leader only)".into(), |h| format!("{h:?}")),
            // Ruling P28: the artifact at P is cut AT the instant, so it can
            // never record its own set's completion.
            if doc.source == Source::Artifact && set.position == doc.view_position {
                " (cut at this artifact; its completion is not visible offline)"
            } else {
                ""
            },
        );
        for r in set.rows.iter().chain(std::iter::once(&set.cluster)) {
            let _ = writeln!(
                s,
                "    {} verdict={} hash={}",
                label(&r.name, r.row),
                word(&r.verdict),
                r.hash.map_or("-".into(), |h| format!("0x{:016x}", h.0)),
            );
        }
    }
    for t in &doc.cluster.tiers {
        let _ = writeln!(
            s,
            "tier={} oldest_set={} newest_set={} through={} lag_bytes={}",
            t.id,
            t.oldest_set,
            t.newest_set,
            t.through,
            t.lag_bytes.map_or("-".into(), |l| l.to_string())
        );
    }
    let g = &doc.cluster.purge_gate;
    let _ = writeln!(
        s,
        "purge_gate requires_tiers={} position={} tiers_reporting={} released={}",
        g.requires_tiers,
        g.position.map_or("off".into(), |p| p.to_string()),
        g.tiers_reporting,
        g.released.map_or("-".into(), |r| r.to_string())
    );
    if let Some(sp) = &doc.self_part {
        let h = &sp.holdings;
        let _ = writeln!(
            s,
            "self journal_first={} durable={} free_bytes={} journal_bytes={} snapshots_bytes={} sets_held={}",
            h.journal_first,
            h.durable,
            h.free_bytes.map_or("unknown".into(), |b| b.to_string()),
            h.journal_bytes,
            h.snapshots_bytes,
            h.sets_held
                .as_ref()
                .map_or("unknown".into(), |v| format!("{v:?}"))
        );
    }
    for n in doc.nodes.iter().flatten() {
        let _ = writeln!(
            s,
            "node id={} addr={} role={} age_ms={} stale={} durable={}",
            n.id,
            n.addr,
            word(&n.role),
            n.age_ms.map_or("-".into(), |a| a.to_string()),
            n.stale,
            n.holdings
                .as_ref()
                .map_or("-".into(), |h| h.durable.to_string()),
        );
    }
    s
}

fn attention_lines(doc: &ClusterViewDoc, storage_only: bool) -> String {
    doc.attention
        .iter()
        .filter(|a| !storage_only || STORAGE_CODES.contains(&a.code.as_str()))
        .map(|a| {
            format!(
                "attention {}\n",
                serde_json::to_string(a).unwrap_or_default()
            )
        })
        .collect()
}

pub fn catalog_text(doc: &ClusterViewDoc) -> String {
    storage_text(doc) + &attention_lines(doc, true)
}

pub fn cluster_text(doc: &ClusterViewDoc) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let m = &doc.cluster.membership;
    let _ = writeln!(s, "membership config_version={}", m.config_version);
    for x in &m.members {
        let _ = writeln!(s, "  id={} addr={} role={}", x.id, x.addr, word(&x.role));
    }
    // The same fields as `settings show`, through its renderer, but at the
    // settings record's own position (as the schedule line uses the table's);
    // `settings show` prints the view's position.
    let st = &doc.cluster.settings;
    let _ = writeln!(
        s,
        "settings {}",
        crate::settings::render_settings_line(st.position, &uc_node::view::convert::settings(st))
    );
    let _ = writeln!(
        s,
        "schedule entries={} position={}",
        doc.cluster.schedule.entries.len(),
        doc.cluster.schedule.position
    );
    for u in &doc.cluster.upgrades {
        let _ = writeln!(
            s,
            "upgrade row={} running={} pins={}",
            label(&u.name, u.row),
            u.running
                .as_ref()
                .map_or("-".into(), |r| r.version.text.clone()),
            u.pins.len()
        );
    }
    if let Some(sp) = &doc.self_part {
        for sv in &sp.services {
            let _ = writeln!(
                s,
                "service {} attached={} version={} lag_bytes={} pinned={}",
                label(&sv.name, sv.row),
                sv.attached,
                sv.version.as_ref().map_or("-".into(), |v| v.text.clone()),
                sv.lag_bytes,
                sv.pinned
            );
        }
    }
    storage_text(doc) + &s + &attention_lines(doc, false)
}

const NO_ARTIFACT: &str = "no cluster artifact yet (node not running; nothing to read offline)";

/// What `--json` prints for a fetched body: `None` when there is no
/// artifact (the caller says so on stderr, keeping stdout JSON-only).
fn json_output(body: Body, catalog_only: bool) -> anyhow::Result<Option<String>> {
    Ok(Some(match body {
        Body::NoArtifact => return Ok(None),
        // Verbatim (spec §6.3, ruling P16): the node's own bytes, with only
        // the schema checked.
        Body::Live(raw) if !catalog_only => {
            parse_value(&raw)?;
            raw
        }
        Body::Live(raw) => {
            serde_json::to_string_pretty(&catalog_value(&parse_value(&raw)?))? + "\n"
        }
        Body::Offline(doc) if !catalog_only => serde_json::to_string_pretty(&doc)? + "\n",
        Body::Offline(doc) => {
            serde_json::to_string_pretty(&catalog_value(&serde_json::to_value(&doc)?))? + "\n"
        }
    }))
}

pub fn run(common: &CommonArgs, json: bool, catalog_only: bool) -> anyhow::Result<()> {
    if json {
        match json_output(fetch_body(common, true)?, catalog_only)? {
            Some(text) => print!("{text}"),
            None => eprintln!("{NO_ARTIFACT}"),
        }
        return Ok(());
    }
    let doc = match fetch(common, true)? {
        Fetched::Live(d) | Fetched::Offline(d) => d,
        Fetched::NoArtifact => {
            println!("{NO_ARTIFACT}");
            return Ok(());
        }
    };
    if catalog_only {
        print!("{}", catalog_text(&doc));
    } else {
        print!("{}", cluster_text(&doc));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Review focus 4.
    #[test]
    fn a_foreign_schema_is_refused_by_name() {
        let doc = uc_node::view::offline::build_offline_doc(
            &uc_node::ClusterState::genesis_empty(),
            &Default::default(),
            0,
            0,
        );
        let mut v = serde_json::to_value(&doc).unwrap();
        v["schema"] = "uc2.cluster-view/2".into();
        let e = parse(&v.to_string()).unwrap_err().to_string();
        assert!(
            e.contains("uc2.cluster-view/2") && e.contains("uc2.cluster-view/1"),
            "{e}"
        );
    }

    /// P32: offline `empty` prints `unknown (offline)` when null, `false`
    /// when an earlier set completed; live prints the boolean.
    #[test]
    fn catalog_text_prints_offline_empty_as_unknown_or_false() {
        use uc_protocol::v2::catalog::{SetEntry, SetKind, SetState};
        let mut st = uc_node::ClusterState::genesis_empty();
        st.catalog = vec![SetEntry::commanded(1024, SetKind::Full, 0)];
        let doc = uc_node::view::offline::build_offline_doc(&st, &Default::default(), 1024, 0);
        let text = catalog_text(&doc);
        assert!(text.contains(" empty=unknown (offline) "), "{text}");
        let mut a = SetEntry::commanded(512, SetKind::Full, 0);
        a.state = SetState::Complete;
        st.catalog.insert(0, a);
        let doc = uc_node::view::offline::build_offline_doc(&st, &Default::default(), 1024, 0);
        assert!(
            catalog_text(&doc).contains(" empty=false "),
            "{}",
            catalog_text(&doc)
        );
        let live = parse(include_str!("../../uc_node/src/view/example.json")).unwrap();
        let want = format!(" empty={} ", live.cluster.catalog.empty.unwrap());
        assert!(
            catalog_text(&live).contains(&want),
            "{}",
            catalog_text(&live)
        );
    }

    /// Review focus 5: a declared row with no readable name prints `row<N>`.
    #[test]
    fn catalog_text_names_rows_and_falls_back_to_row_n() {
        use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
        let mut st = uc_node::ClusterState::genesis_empty();
        st.running[2] = Some(uc_protocol::v2::upgrade::RowRunning {
            row: 2,
            version: 0,
            record_pos: 64,
        });
        let mut a = SetEntry::commanded(1024, SetKind::Full, 0);
        a.state = SetState::Complete;
        a.rows[2] = RowEntry {
            version: 0,
            hash: 0xaa,
            verdict: RowVerdict::Diverged,
            size: 1,
        };
        st.catalog = vec![a];
        let doc = uc_node::view::offline::build_offline_doc(&st, &Default::default(), 1024, 0);
        let text = catalog_text(&doc);
        assert!(text.contains("offline: newest artifact at 1024"), "{text}");
        assert!(text.contains("row2"), "{text}");
        assert!(text.contains("set_diverged"), "{text}");
        // P18: JSON words, not Debug.
        assert!(text.contains("kind=full state=complete"), "{text}");
        assert!(text.contains("verdict=diverged"), "{text}");
        assert!(
            !text.contains("Full") && !text.contains("Diverged"),
            "{text}"
        );
    }

    /// Spec §6.3 "the whole view": the cluster text carries the settings,
    /// the same settings fields `settings show` prints, and the reference
    /// page's sample is this document's text.
    #[test]
    fn cluster_text_prints_the_settings_line() {
        let doc = parse(include_str!("../../uc_node/src/view/example.json")).unwrap();
        let text = cluster_text(&doc);
        eprintln!("{text}");
        let want = format!(
            "settings {}\n",
            crate::settings::render_settings_line(
                doc.cluster.settings.position,
                &uc_node::view::convert::settings(&doc.cluster.settings)
            )
        );
        assert!(text.contains(&want), "{text}");
        assert!(text.contains("free_bytes=unknown"), "{text}");
    }

    /// P16/P17: the catalog filter keeps unknown fields of the parts it
    /// keeps, and keeps exactly the storage attention codes.
    #[test]
    fn the_catalog_filter_works_on_the_json_value() {
        let doc = uc_node::view::offline::build_offline_doc(
            &uc_node::ClusterState::genesis_empty(),
            &Default::default(),
            0,
            0,
        );
        let mut v = serde_json::to_value(&doc).unwrap();
        v["cluster"]["catalog"]["a_newer_field"] = 7.into();
        v["attention"] = serde_json::json!([
            {"code": "catalog_empty"},
            {"code": "node_stale", "node": 3},
            {"code": "some_service_code"},
        ]);
        let c = catalog_value(&v);
        assert_eq!(c["schema"], uc_node::view::SCHEMA);
        assert_eq!(c["catalog"]["a_newer_field"], 7);
        let codes: Vec<&str> = c["attention"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["code"].as_str().unwrap())
            .collect();
        assert_eq!(codes, ["catalog_empty", "node_stale"]);
        assert!(c["holdings"].is_null());
    }

    /// P16: `cluster --json` on a live node prints the node's bytes exactly —
    /// odd whitespace, field order and an unknown field included.
    #[test]
    fn live_cluster_json_is_the_body_verbatim() {
        let raw = format!(
            "{{ \"zz_newer_field\" : [1, 2],\n\t\"schema\":\"{}\" ,\"source\":\"live\"}}\n\n",
            uc_node::view::SCHEMA
        );
        let out = json_output(Body::Live(raw.clone()), false)
            .unwrap()
            .unwrap();
        assert_eq!(out.as_bytes(), raw.as_bytes());
        // A foreign schema is still refused on the verbatim path.
        let foreign = raw.replace(uc_node::view::SCHEMA, "uc2.cluster-view/2");
        assert!(json_output(Body::Live(foreign), false).is_err());
        assert!(json_output(Body::NoArtifact, false).unwrap().is_none());
    }
}
