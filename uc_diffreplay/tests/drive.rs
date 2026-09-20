mod common;
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::drive::{Origin, drive};
use uc_diffreplay::trace::EntryKind;
use uc_lincheck::register::RegisterSm;

#[test]
fn driver_replays_the_span_above_the_origin_and_projects_both_ends() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "drv", 5, 3);
    let corpus = Corpus::export(inst.path(), "drv", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Artifact).unwrap();

    // Origin projection: the artifact at P holds writes 0..5 → value=Some(4).
    // (`last_applied` is a position this test does not know, so only the first
    // line is asserted.)
    assert!(
        t.projection_at_origin
            .as_deref()
            .unwrap()
            .starts_with("value=Some(4)\n")
    );
    // Exactly the 3 writes above P were applied, in order, each acked.
    let msgs: Vec<_> = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .collect();
    assert_eq!(msgs.len(), 3, "{:?}", t.entries);
    assert!(msgs.windows(2).all(|w| w[0].pos < w[1].pos));
    assert!(
        t.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(7)\n")
    );
    assert_eq!(t.origin, p);
}

#[test]
fn genesis_origin_replays_everything_from_zero() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "gen", 5, 3);
    let corpus = Corpus::export(inst.path(), "gen", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Genesis).unwrap();
    assert_eq!(
        t.projection_at_origin, None,
        "genesis has nothing installed to project"
    );
    let msgs = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .count();
    assert_eq!(msgs, 8);
    assert!(
        t.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(7)\n")
    );
}
