use uc_diffreplay::diff::{Surface, diff};
use uc_diffreplay::trace::{Entry, EntryKind, Sched, Trace};

fn trace(entries: Vec<(u64, &[u8], &[u8])>, proj_end: &str) -> Trace {
    Trace {
        row: 0,
        version: 1,
        origin: 32,
        end: 1000,
        projection_at_origin: Some("value=None\n".into()),
        projection_at_end: Some(proj_end.into()),
        entries: entries
            .into_iter()
            .map(|(pos, tag, resp)| Entry {
                pos,
                kind: EntryKind::Message,
                tag: tag.to_vec(),
                response: resp.to_vec(),
                sched: vec![],
            })
            .collect(),
    }
}

#[test]
fn identical_traces_produce_an_empty_profile() {
    let a = trace(
        vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")],
        "value=Some(1)\n",
    );
    let p = diff(&a, &a.clone()).unwrap();
    assert!(p.is_empty(), "{p:?}");
}

#[test]
fn one_changed_response_is_one_divergence_at_its_position() {
    let a = trace(
        vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")],
        "value=Some(1)\n",
    );
    let b = trace(
        vec![(32, b"\x01", b"ok"), (64, b"\x02", b"OK")],
        "value=Some(1)\n",
    );
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].pos, 64);
    assert_eq!(p.entries[0].tag, b"\x02");
    assert!(matches!(p.entries[0].surface, Surface::Response));
    assert!(p.projection_end.is_empty());
}

#[test]
fn projection_diff_is_a_line_set_difference() {
    let a = trace(vec![], "count=2\nkey=a version=1\nkey=b version=1\n");
    let b = trace(
        vec![],
        "count=2\nkey=a version=1 ttl=0\nkey=b version=1 ttl=0\n",
    );
    let p = diff(&a, &b).unwrap();
    assert_eq!(
        p.projection_end.removed,
        vec!["key=a version=1", "key=b version=1"]
    );
    assert_eq!(
        p.projection_end.added,
        vec!["key=a version=1 ttl=0", "key=b version=1 ttl=0"]
    );
}

#[test]
fn a_changed_sched_record_is_a_sched_divergence() {
    let mut a = trace(vec![(32, b"\x01", b"ok")], "");
    let mut b = a.clone();
    a.entries[0].sched = vec![Sched {
        op: "schedule".into(),
        id: 1,
        deadline_ns: 10,
    }];
    b.entries[0].sched = vec![Sched {
        op: "schedule".into(),
        id: 1,
        deadline_ns: 20,
    }];
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert!(matches!(p.entries[0].surface, Surface::Sched));
}

#[test]
fn mismatched_spans_are_refused() {
    let a = trace(vec![], "");
    let mut b = a.clone();
    b.origin = 0;
    assert!(diff(&a, &b).is_err());
}
