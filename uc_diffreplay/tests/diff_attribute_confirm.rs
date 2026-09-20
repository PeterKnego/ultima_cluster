use uc_diffreplay::attribute::{Attribution, Declaration, attribute};
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

#[test]
fn one_sided_positions_land_in_only_in_not_entries() {
    let a = trace(
        vec![(32, b"\x01", b"ok"), (64, b"\x01", b"ok")],
        "value=Some(1)\n",
    );
    let b = trace(
        vec![(32, b"\x01", b"ok"), (96, b"\x01", b"ok")],
        "value=Some(1)\n",
    );
    let p = diff(&a, &b).unwrap();
    assert!(p.entries.is_empty(), "{:?}", p.entries);
    assert_eq!(p.only_in_a, vec![64]);
    assert_eq!(p.only_in_b, vec![96]);
}

#[test]
fn duplicate_lines_diff_as_a_multiset() {
    let a = trace(vec![], "x=1\nx=1\ny=2\n");
    let b = trace(vec![], "x=1\ny=2\n");
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.projection_end.removed, vec!["x=1"]);
    assert!(p.projection_end.added.is_empty());
}

const DECL: &str = r#"
[tags]
"01" = "put"
"02" = "delete"
[touched]
arms = ["put"]
migration = true
[[expect]]
surface = "response"
arm = "put"
note = "put now acks with the new version"
[[expect]]
surface = "projection_origin"
note = "every entry gains ttl=0"
"#;

#[test]
fn declaration_parses_and_maps_tags() {
    let d = Declaration::from_toml(DECL).unwrap();
    assert_eq!(d.arm_of(b"\x01"), Some("put"));
    assert_eq!(d.arm_of(b"\x02"), Some("delete"));
    assert_eq!(d.arm_of(b"\x09"), None);
    assert!(d.touched.migration);
    assert_eq!(d.expect.len(), 2);
    // The default: the tag IS the application frame.
    assert_eq!(d.tag_offset, 0);
}

/// `tag_offset` drops a framework envelope before the `[tags]` prefixes are
/// matched — the real case being `Sessioned<S>`'s 16-byte `client_id ‖ seq`,
/// which would otherwise make every tag start with a client id and match
/// nothing. Two bytes here is the same mechanism, small enough to read.
#[test]
fn tag_offset_skips_the_envelope_before_matching_an_arm() {
    const DECL_OFFSET: &str = r#"
tag_offset = 2
[tags]
"01" = "put"
[touched]
arms = ["put"]
"#;
    let d = Declaration::from_toml(DECL_OFFSET).unwrap();
    assert_eq!(d.tag_offset, 2);
    assert_eq!(d.arm_of(&[0xAA, 0xBB, 0x01]), Some("put"));
    // Without the skip these would be the bytes matched — and are not.
    assert_eq!(d.arm_of(&[0x01, 0xBB, 0xAA]), None);
    // A tag shorter than the offset leaves nothing to match, not a panic.
    assert_eq!(d.arm_of(&[0xAA]), None);
    assert_eq!(d.arm_of(&[]), None);
}

#[test]
fn touched_arm_attributes_untouched_arm_is_unexplained() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")], "x=1\n");
    let b = trace(vec![(32, b"\x01", b"OK"), (64, b"\x02", b"OK")], "x=1\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.entries[0].1, Attribution::Arm(ref s) if s == "put"));
    assert!(matches!(att.entries[1].1, Attribution::Unexplained));
}

#[test]
fn origin_projection_diff_attributes_to_migration_when_declared() {
    let d = Declaration::from_toml(DECL).unwrap();
    let mut a = trace(vec![], "");
    let mut b = a.clone();
    a.projection_at_origin = Some("k=a\n".into());
    b.projection_at_origin = Some("k=a ttl=0\n".into());
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(
        att.projection_origin,
        Some(Attribution::Migration)
    ));
}

#[test]
fn origin_projection_diff_is_unexplained_when_migration_not_declared() {
    const DECL_NO_MIGRATION: &str = r#"
[touched]
arms = []
migration = false
"#;
    let d = Declaration::from_toml(DECL_NO_MIGRATION).unwrap();
    let mut a = trace(vec![], "");
    let mut b = a.clone();
    a.projection_at_origin = Some("k=a\n".into());
    b.projection_at_origin = Some("k=a ttl=0\n".into());
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(
        att.projection_origin,
        Some(Attribution::Unexplained)
    ));
}

/// A projection is ONE comparison over the whole state: no single arm owns
/// it. Naming the first touched arm (as the plan drafted) would blame an arm
/// the diff never implicated — `Touched` says what is actually known.
#[test]
fn end_projection_diff_attributes_to_the_touched_set_as_a_whole() {
    const DECL_ARM_AND_MIGRATION: &str = r#"
[touched]
arms = ["put", "delete"]
migration = true
"#;
    let d = Declaration::from_toml(DECL_ARM_AND_MIGRATION).unwrap();
    let a = trace(vec![], "value=Some(1)\n");
    let b = trace(vec![], "value=Some(2)\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.projection_end, Some(Attribution::Touched)));
}

#[test]
fn end_projection_diff_attributes_to_migration_when_no_arms_touched() {
    const DECL_MIGRATION_ONLY: &str = r#"
[touched]
arms = []
migration = true
"#;
    let d = Declaration::from_toml(DECL_MIGRATION_ONLY).unwrap();
    let a = trace(vec![], "value=Some(1)\n");
    let b = trace(vec![], "value=Some(2)\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.projection_end, Some(Attribution::Migration)));
}

#[test]
fn end_projection_diff_is_unexplained_when_nothing_touched() {
    const DECL_NOTHING_TOUCHED: &str = r#"
[touched]
arms = []
migration = false
"#;
    let d = Declaration::from_toml(DECL_NOTHING_TOUCHED).unwrap();
    let a = trace(vec![], "value=Some(1)\n");
    let b = trace(vec![], "value=Some(2)\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(matches!(att.projection_end, Some(Attribution::Unexplained)));
}

#[test]
fn end_projection_is_none_when_identical() {
    const DECL_ARM_AND_MIGRATION: &str = r#"
[touched]
arms = ["put", "delete"]
migration = true
"#;
    let d = Declaration::from_toml(DECL_ARM_AND_MIGRATION).unwrap();
    let a = trace(vec![], "value=Some(1)\n");
    let b = trace(vec![], "value=Some(1)\n");
    let att = attribute(&diff(&a, &b).unwrap(), &d);
    assert!(att.projection_end.is_none());
}

use uc_diffreplay::confirm::{Verdict, confirm};

#[test]
fn declared_and_observed_passes_undeclared_fails() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "x=1\n");
    let b = trace(vec![(32, b"\x01", b"OK")], "x=1\n");
    let mut a2 = a.clone();
    a2.projection_at_origin = Some("k=a\n".into());
    let mut b2 = b.clone();
    b2.projection_at_origin = Some("k=a ttl=0\n".into());
    let v = confirm(&attribute(&diff(&a2, &b2).unwrap(), &d), &d);
    assert!(!v.failed(), "{:?}", v.findings);
    assert!(
        v.findings
            .iter()
            .all(|f| matches!(f.verdict, Verdict::Pass))
    );
}

#[test]
fn an_observed_attributed_but_undeclared_diff_is_undeclared() {
    let mut d = Declaration::from_toml(DECL).unwrap();
    d.expect.clear(); // declare nothing
    let a = trace(vec![(32, b"\x01", b"ok")], "");
    let b = trace(vec![(32, b"\x01", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(v.failed());
    assert!(
        v.findings
            .iter()
            .any(|f| matches!(f.verdict, Verdict::Undeclared))
    );
}

#[test]
fn an_unexplained_diff_fails_as_unexplained() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x02", b"ok")], ""); // delete: not touched
    let b = trace(vec![(32, b"\x02", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(
        v.findings
            .iter()
            .any(|f| matches!(f.verdict, Verdict::Unexplained))
    );
}

#[test]
fn a_declared_but_absent_diff_fails_as_absent() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "");
    let v = confirm(&attribute(&diff(&a, &a.clone()).unwrap(), &d), &d);
    assert!(v.failed());
    assert_eq!(
        v.findings
            .iter()
            .filter(|f| matches!(f.verdict, Verdict::Absent))
            .count(),
        2
    );
}

const DECL_WILDCARD_FIRST: &str = r#"
[tags]
"01" = "put"
"02" = "delete"
[touched]
arms = ["put", "delete"]
migration = false
[[expect]]
surface = "response"
note = "any response change on a touched arm"
[[expect]]
surface = "response"
arm = "put"
note = "put now acks with the new version"
"#;

#[test]
fn most_specific_expect_wins_over_an_earlier_wildcard_leaving_the_wildcard_absent() {
    // The wildcard `[[expect]] surface = "response"` is declared BEFORE the
    // specific `arm = "put"` entry. One `put` divergence must satisfy the
    // specific entry, not the wildcard — declaration order must not decide
    // this. The wildcard then has nothing left to match and is `Absent`:
    // that is a correctly-reported over-specified declaration, not a bug.
    let d = Declaration::from_toml(DECL_WILDCARD_FIRST).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "");
    let b = trace(vec![(32, b"\x01", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    let put_finding = v
        .findings
        .iter()
        .find(|f| f.arm.as_deref() == Some("put"))
        .expect("a finding naming the put arm");
    assert!(matches!(put_finding.verdict, Verdict::Pass), "{v:?}");
    let wildcard_finding = v
        .findings
        .iter()
        .find(|f| f.arm.is_none())
        .expect("a finding for the unsatisfied wildcard");
    assert!(matches!(wildcard_finding.verdict, Verdict::Absent), "{v:?}");
}

#[test]
fn specific_and_wildcard_expect_both_pass_when_every_declared_arm_is_observed() {
    let d = Declaration::from_toml(DECL_WILDCARD_FIRST).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok"), (64, b"\x02", b"ok")], "");
    let b = trace(vec![(32, b"\x01", b"OK"), (64, b"\x02", b"OK")], "");
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(!v.failed(), "{:?}", v.findings);
    assert_eq!(v.findings.len(), 2);
    assert!(
        v.findings
            .iter()
            .all(|f| matches!(f.verdict, Verdict::Pass))
    );
}

#[test]
fn an_unknown_expect_surface_fails_to_parse_by_name() {
    const DECL_BAD_SURFACE: &str = r#"
[touched]
arms = []
migration = false
[[expect]]
surface = "respones"
note = "typo"
"#;
    let err = Declaration::from_toml(DECL_BAD_SURFACE).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("unknown surface \"respones\""), "{msg}");
    assert!(
        msg.contains("response | sched | projection_origin | projection_end"),
        "{msg}"
    );
}

/// I2: a position ONE build dispatched is not a value difference on any
/// surface, so `attribute` can never name an arm for it — but it is the
/// loudest thing a profile can hold (the two builds disagree about which
/// frames the FSM saw). It must reach `confirm` and the report, not stop at
/// the profile.
#[test]
fn a_one_sided_position_is_an_unexplained_finding_at_that_position() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(
        vec![(32, b"\x01", b"ok"), (64, b"\x01", b"ok")],
        "value=Some(1)\n",
    );
    let b = trace(vec![(32, b"\x01", b"ok")], "value=Some(1)\n");
    let p = diff(&a, &b).unwrap();
    assert_eq!(p.only_in_a, vec![64]);
    let att = attribute(&p, &d);
    assert_eq!(att.only_in_a, vec![64]);
    let v = confirm(&att, &d);
    assert!(v.failed(), "{:?}", v.findings);
    let one_sided: Vec<_> = v
        .findings
        .iter()
        .filter(|f| matches!(f.verdict, Verdict::Unexplained) && f.pos == Some(64))
        .collect();
    assert_eq!(one_sided.len(), 1, "{:?}", v.findings);
    assert!(
        one_sided[0].note.contains("only_in_a"),
        "{}",
        one_sided[0].note
    );
}

#[test]
fn a_one_sided_position_in_b_is_reported_as_only_in_b() {
    let d = Declaration::from_toml(DECL).unwrap();
    let a = trace(vec![(32, b"\x01", b"ok")], "value=Some(1)\n");
    let b = trace(
        vec![(32, b"\x01", b"ok"), (96, b"\x01", b"ok")],
        "value=Some(1)\n",
    );
    let v = confirm(&attribute(&diff(&a, &b).unwrap(), &d), &d);
    assert!(
        v.findings
            .iter()
            .any(|f| f.pos == Some(96) && f.note.contains("only_in_b")),
        "{:?}",
        v.findings
    );
}

/// A TIMER frame carries no application payload, so `[tags]` can never reach
/// it: its arm comes from `[timers]`, keyed on the timer id.
fn timer_trace(id: u64, resp: &[u8]) -> Trace {
    Trace {
        row: 0,
        version: 1,
        origin: 32,
        end: 1000,
        projection_at_origin: Some("value=None\n".into()),
        projection_at_end: Some(String::new()),
        entries: vec![Entry {
            pos: 32,
            kind: EntryKind::Timer {
                id,
                deadline_ns: 5,
                table: false,
            },
            tag: vec![],
            response: resp.to_vec(),
            sched: vec![],
        }],
    }
}

#[test]
fn a_timer_divergence_is_attributed_by_timer_id() {
    const DECL_TIMERS: &str = r#"
[timers]
"9" = "reaper"
[touched]
arms = ["reaper"]
"#;
    let d = Declaration::from_toml(DECL_TIMERS).unwrap();
    assert_eq!(d.arm_of_timer(9), Some("reaper"));
    let p = diff(&timer_trace(9, b"ok"), &timer_trace(9, b"OK")).unwrap();
    assert_eq!(p.entries.len(), 1);
    assert_eq!(p.entries[0].timer_id, Some(9));
    let att = attribute(&p, &d);
    assert!(matches!(att.entries[0].1, Attribution::Arm(ref s) if s == "reaper"));
}

/// Without the `[timers]` mapping the same divergence is permanently
/// unexplained — the defect I3 names. A `[tags]` entry does not rescue it: a
/// timer frame has no payload to tag.
#[test]
fn a_timer_divergence_without_a_timers_mapping_is_unexplained() {
    const DECL_NO_TIMERS: &str = r#"
[tags]
"09" = "reaper"
[touched]
arms = ["reaper"]
"#;
    let d = Declaration::from_toml(DECL_NO_TIMERS).unwrap();
    let att = attribute(
        &diff(&timer_trace(9, b"ok"), &timer_trace(9, b"OK")).unwrap(),
        &d,
    );
    assert!(matches!(att.entries[0].1, Attribution::Unexplained));
}

/// I4 at the parse boundary: a projection is attributed to the touched set
/// as a whole, so an `arm` on a projection `[[expect]]` is a promise the
/// harness cannot keep. Refuse it by name rather than let it sit there never
/// matching.
#[test]
fn a_projection_expect_carrying_an_arm_is_refused_by_name() {
    for surface in ["projection_origin", "projection_end"] {
        let src = format!(
            "[touched]\narms = [\"put\"]\n[[expect]]\nsurface = \"{surface}\"\n\
             arm = \"put\"\nnote = \"x\"\n"
        );
        let msg = Declaration::from_toml(&src).unwrap_err().to_string();
        assert!(
            msg.contains(&format!("surface = \"{surface}\" takes no arm")),
            "{msg}"
        );
        assert!(msg.contains("touched set as a whole"), "{msg}");
    }
}

/// I6: a declaration is the developer's statement of intent — a typo in a
/// key must not read as "not declared".
#[test]
fn an_unknown_declaration_key_fails_to_parse() {
    let err = Declaration::from_toml("tagoffset = 16\n[touched]\narms = []\n").unwrap_err();
    // `{:#}` walks the anyhow chain: the serde error naming the key sits
    // under the "declaration TOML" context.
    assert!(format!("{err:#}").contains("tagoffset"), "{err:#}");
    // …at every level, not just the top.
    assert!(Declaration::from_toml("[touched]\narms = []\nmigratoin = true\n").is_err());
    assert!(
        Declaration::from_toml(
            "[touched]\narms = []\n[[expect]]\nsurface = \"response\"\nnotes = \"typo\"\n"
        )
        .is_err()
    );
}

/// `arm_of` formats tag bytes as LOWERCASE hex, so an uppercase `[tags]` key
/// would silently never match. `from_toml` normalises them.
#[test]
fn uppercase_tag_keys_are_lowercased_at_parse() {
    let d = Declaration::from_toml("[tags]\n\"0A\" = \"put\"\n[touched]\narms = []\n").unwrap();
    assert_eq!(d.tags.get("0a").map(String::as_str), Some("put"));
    assert_eq!(d.arm_of(&[0x0a]), Some("put"));
}

/// One spelling of a surface, used by the declaration, the text report and
/// the JSON alike.
#[test]
fn surface_name_round_trips_through_parse() {
    for s in [
        Surface::Response,
        Surface::Sched,
        Surface::ProjectionOrigin,
        Surface::ProjectionEnd,
    ] {
        assert_eq!(Surface::parse(s.name()), Some(s), "{}", s.name());
    }
}
