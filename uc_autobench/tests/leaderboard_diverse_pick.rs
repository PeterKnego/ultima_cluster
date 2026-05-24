use uc_autobench::leaderboard::{Entry, Leaderboard, normalize_source};
use uc_autobench::task::Direction;

fn ent(id: &str, primary: f64, source: &str) -> Entry {
    Entry {
        variant_id: id.to_string(),
        primary_metric: primary,
        diversity_tag: uc_autobench::leaderboard::diversity_hash(source),
        hypothesis: format!("h-{id}"),
    }
}

#[test]
fn top_k_sorts_by_minimize() {
    let mut lb = Leaderboard::new(3, Direction::Minimize);
    lb.insert(ent("a", 100.0, "fn a() {}"));
    lb.insert(ent("b", 90.0, "fn b() {}"));
    lb.insert(ent("c", 110.0, "fn c() {}"));
    lb.insert(ent("d", 80.0, "fn d() {}")); // bumps "c"
    let top = lb.entries();
    assert_eq!(top.len(), 3);
    assert_eq!(top[0].variant_id, "d");
    assert_eq!(top[1].variant_id, "b");
    assert_eq!(top[2].variant_id, "a");
}

#[test]
fn diverse_pick_excludes_current_best_and_prefers_distinct_diversity_tags() {
    let mut lb = Leaderboard::new(10, Direction::Minimize);
    // Best
    lb.insert(ent("best", 100.0, "fn x() { let a = 1; }"));
    // Two near-clones (same diversity tag as best after normalization)
    lb.insert(ent("clone1", 101.0, "fn x() { let a = 1; } // comment"));
    lb.insert(ent("clone2", 102.0, "fn x() {\n    let a = 1;\n}"));
    // Two structurally different
    lb.insert(ent("alt1", 105.0, "fn y() { let b = 2; }"));
    lb.insert(ent("alt2", 110.0, "fn z() { let c = 3; }"));

    let picks = lb.diverse_pick("best", 2);
    assert_eq!(picks.len(), 2);
    // Neither pick is the best.
    assert!(picks.iter().all(|e| e.variant_id != "best"));
    // Neither pick has the best's diversity_tag (so clones are deprioritized).
    let best_tag = lb
        .entries()
        .iter()
        .find(|e| e.variant_id == "best")
        .unwrap()
        .diversity_tag;
    assert!(picks.iter().all(|e| e.diversity_tag != best_tag));
}

#[test]
fn normalize_source_strips_comments_and_whitespace() {
    let a = normalize_source("fn x() {\n    let a = 1; // comment\n}");
    let b = normalize_source("fn x(){let a=1;}");
    assert_eq!(a, b);
}

#[test]
fn diversity_hash_stable_across_cosmetic_edits() {
    let h1 = uc_autobench::leaderboard::diversity_hash("fn x() { let a = 1; }");
    let h2 = uc_autobench::leaderboard::diversity_hash("fn x(){\n    let a = 1; // c\n}");
    assert_eq!(h1, h2);
}
