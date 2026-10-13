# UC2 Backup Tier (3b: the tier as a holder) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the cluster rely on the backup tier: a node can be built from it and join live (lineage in the tier), every node holds its purge at what K tiers have confirmed (backup-gated purge), and a node fetches sets and catches up from the tier before loading a peer (the tier as a source).

**Architecture:** Three pieces built in order. **A** records the writer's term map (`base < S`) as a `lineage=` line in each `SET`, which `materialize` writes into `term_map.state`, so reconcile aligns a tier-seeded node instead of cutting it to 0; the consensus core is untouched. **B** adds Settings v6 `purge_requires_tiers` (K), a per-node clamp of the journal purge target to the K-th highest confirmed `through`, a writer-side read-back that filters the reported watermark to objects it has re-read, and a low-space release with its alert. **C** adds a node-local read-only `[tier_source]`, a per-fetch `uc2-tier-fetch` thread that copies and verifies a set against the **catalog's** hashes and then raises the same signals the receiver raises for a peer fetch, and a `TIER_INSTALL`/`TIER_INSTALL_STATUS` exchange (kinds 28/29) the leader's sender tries before opening a snapshot session.

**Tech Stack:** Rust (MSRV 1.89, pinned 1.96), existing `uc_node::tier`, `uc_protocol`, `uc_net`, `uc_consensus`, `uc_sim`; no new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-12-uc2-backup-tier-3b-design.md` (read whole, D1–D8, §12). Parents: `2026-10-10-uc2-backup-tier-design.md` — read its `#### Errata (as built, 2026-10-11)` block first (R8b, R9a, RF1, RF2, R11c, R12a, F3, F4 bind this plan) — then the catalog and lifecycle specs' errata.

## Global Constraints

- Wire stays `0.11.0` (unreleased; spec §7). Extend the `0.11.0` comment block in `uc_protocol/src/version.rs:86-98`; do not bump `CURRENT`.
- Pairwise datagram kinds `DGRAM_KIND_TIER_INSTALL = 28` (body 16 B: `floor u64 ‖ commit u64`) and `DGRAM_KIND_TIER_INSTALL_STATUS = 29` (body 24 B: `state u8 ‖ reason u8 ‖ reserved [u8; 6] ‖ position u64 ‖ copied u64`); both `Scope::Pairwise` in `uc_crypto/src/transport.rs:320-335`.
- `SETTINGS_VERSION` 5 → 6, `SETTINGS_LEN` 46 → 47, new `SETTINGS_LEN_V5 = 46`; `purge_requires_tiers: u8` @46; versions 1–5 decode it as `0`; `> MAX_BACKUP_TIERS` (4) refused `47 settings_bounds("purge_requires_tiers")`.
- `SET` gains exactly one line `lineage=<t>:<b>,…` (decimal, ascending by base, strictly increasing terms, last `base < position` — strictly; review amendment RA1); a `SET` without it, or with a malformed one, is refused `set_lineage_missing` / `set_lineage_malformed` and lists as a corrupt set (never a start, never fetched). No cap on its length.
- `node.toml` gains `[tier_source] target = "<dir>"` (the only key; `deny_unknown_fields`); carried as `StartupOptions::tier_source: Option<PathBuf>` → `StartOpts::tier_source`, never a `NodeConfig` field. A node with `[backup]` and no `[tier_source]` reads `backup.target`.
- Startup refusal `tier_source_foreign` (the tier's `TIER.app_id` ≠ the node's); an absent or unreadable source is NOT a refusal (log `tier_source_unavailable`, every fetch falls back to peers).
- New obs events: `backup_readback_failed{object, detail}`, `backup_gate_released{gate, free_bytes}`, `backup_gate_rearmed`, `tier_fetch_declined{position, reason}`, `tier_install_declined{reason}`, `tier_install_started{position}`, `tier_install_done{position}`, `tier_source_unavailable{detail}`, `tier_set_deferred{position}` (RA6).
- New metric families (contract 142 → 147, `docs/how-to/monitor-a-cluster.md:70` updated in the same commit): `uc2_purge_gate_position` (gauge; header-only, no sample, when K = 0), `uc2_backup_gate_released` (0/1), `uc2_backup_gate_released_total`, `uc2_snapshot_fetch_total{source="tier"|"peer"}`, `uc2_tier_install_total{outcome="installed"|"declined"|"timeout"}`. New alert `Uc2BackupGateReleased` with an `m10_alerts` scenario `backup_gate_released` and a `RULE_BUILDERS` entry. New `WriterError::Readback = 7` (label `readback`).
- Thread name for the tier copy: `uc2-tier-fetch` (one per fetch, joins on completion; never an agent in the `/healthz` list).
- Leader-side waits: `TIER_INSTALL_ANSWER_NS = 2 s`, `TIER_INSTALL_STALL_NS = 10 s`, follower status cadence `TIER_INSTALL_STATUS_NS = 1 s`, and a `TIER_INSTALL_DECLINE_HOLD_NS = 30 s` per-peer memory of a decline so a declining follower costs one round trip per below-floor episode, not one per NAK.
- Scratch and test artifacts never under `/tmp`: tests use `CARGO_TARGET_TMPDIR` (`tempdir()` helpers, prefix `uc2-tier-`); manual runs use `$HOME/scratch/`.
- Every commit passes `cargo fmt --all -- --check`. Before any push: `cargo clippy --workspace --all-targets -- -D warnings`; the MSRV gate `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`; every feature-gated clippy line in `.github/workflows/ci.yml` (`uc_service --features apply-profile`, `uc_gateway test-util`, `uc_lincheck replay-bin`, `uc_crashtest hard-crash-tests`).
- No `Co-Authored-By` or any attribution trailer in commits.
- Use a private `CARGO_TARGET_DIR` (`$HOME/.cache/cargo-target-tier3b`) for proof-stack runs.
- `docs/superpowers/` artifacts stay in place; never delete them.
- Run `python3 scripts/check_doc_links.py` before any docs commit.

## Pre-flight rulings (where the code differs from the spec's sketch)

- **PF1 — lineage agreement between writers is suffix agreement, not equality.** `NodeState::store_term_map` keeps only the newest 300 entries (`uc_log/src/state.rs:159`, `PERSISTED_TERM_MAP_MAX_ENTRIES`), so a restarted writer's in-memory map can be shorter than a never-restarted one's. `place_set` on `Put::Exists` compares every `SetLabel` field exactly EXCEPT `lineage`, which agrees iff the shorter is a suffix of the longer; any other difference is the existing `OverlapMismatch` stop. The line itself is uncapped (spec §4.1).
- **PF2 — read-back gates the REPORT, not the write (as amended by RA2).** The watermark is `watermark_of(listing)` over every writer's objects (`writer.rs:1080`). Every object this writer CREATES (`Put::Created`) is read back right after the put and enters the per-process verified-name cache only if it passes. An object found already in place (listed but not created by this process: another writer's, or this writer's from before a restart) counts without a re-read when the cluster's committed watermark entry for this tier covers it — a chunk with `to <= through`, a set with `position <= newest_set` — because its creator read it back; otherwise it is read back once before it counts. Residual (spec §5.3): a creator crashing between writing a set below `newest_set` and reading it back leaves that one set unchecked. `refresh` computes `wm`/`progress` over the listing filtered to confirmed ∪ verified objects. Placement decisions (`copy_one_set`, `cut_one_chunk`, retention) keep the RAW listing, so the writer never re-cuts over a persistently bad object — that object is the operator's, named by `tier verify` (spec §5.3). A tier with no committed entry yet (first registration) has nothing confirmed and is read back once.
- **PF3 — the gate's chunk size on a non-writer node.** `Consensus::backup_chunk_bytes` is 0 without a writer (`node.rs:2549`), and `fits(free, 0)` means "free ≥ 1 GiB". The gate's low-space test uses `backup_chunk_bytes` when a writer runs, else `tier::writer::DEFAULT_CHUNK_BYTES` (64 MiB).
- **PF4 — the tier copy is a thread, and it ends where the receiver's does.** Copying a set is I/O the consensus agent must not do. One `uc2-tier-fetch` thread per fetch serves both §6.2 (store-only) and §6.3 (install); on success it raises exactly the signals `Receiver::snap_complete` raises (`receiver.rs:3050-3104`: for store-only `stored_set_pos.fetch_max`; for install additionally `incoming_cluster_pos`, the `cluster_install` channel, `incoming_snapshot_pos`, `snap_adopt_pending`), factored into one `SnapIntakeSignals` struct both own. `maybe_adopt_incoming_snapshot` and `check_set_completeness` then serve a tier copy unchanged.
- **PF5 — the install adopts the `SET`'s lineage, not the leader's 64-entry tail.** `maybe_adopt_incoming_snapshot` (`node.rs:5791`) adopts `last_leader_map`; a tier install sets `pending_install_lineage = Some(set.lineage)` first and the handler prefers it. The leader's later `TermMap` datagrams then reconcile against a map that holds every committed entry ≤ S.
- **PF6 — the sim has no purge floor.** `uc_sim` models no snapshot install and no floor (`world.rs:2397-2400`). Spec §4.4's sim inv is built on a new seeding seam (Task 5). Spec §6.7's "a tier install never lands below the leader's floor or above commit" cannot be a sim inv without a floor model; it is pinned instead as an exhaustive test of the pure choice function `choose_install_set` (Task 13) and recorded as an erratum. Building a floor model is not 3b's.
- **PF7 — the leader-side wait lives in the sender.** The below-floor decision is `Sender::serve_nak` (`uc_net/src/sender.rs:1444-1494`), on the sender thread, timed with `self.base.elapsed()`; kind 29 reaches it as a `CtrlMsg` (the `SNAP_DONE` route). Kind 28 reaches the follower's node as a term-gated `NetEvent` (the `SNAP_REDIRECT` route). Floor = the journal's first archived base (what `serve_nak_from_journal` fails on); commit = the counter the sender already reads.

## Review amendments (maintainer-approved plan review, 2026-10-12)

Applied after Task 1 landed (`648d1105`); Tasks 2–18 below already carry them.

- **RA1 — lineage is `base < S`, strictly** (spec §4.1 amended). The bytes at S belong to the chain, and a term opened exactly at S may be uncommitted when the writer copies the set. Tasks 2 (parse refuses `last.base >= position`), 3 (filter `b < S`) and 4 (a term opening at S is derived, with `write_journal` still seeded from the lineage's last term).
- **RA2 — read-back: the creator checks what it writes; a found object is checked only if the watermark does not already confirm it** (spec §5.3 amended; PF2 above). Task 8.
- **RA3 — the tier copy's temp names are its own** (`incoming-<P>.tier.part`; spec §6.2 amended), so a fallback peer session for the same P never shares a file with it. Task 12.
- **RA4 — Task 12's e2e is deterministic**: genesis `auto_fetch = false`, the writer on the learner, the fetch triggered by `uc2ctl snapshot fetch` (admin op 9) only after the set is in the tier; and `issue_fetch` logs `tier_fetch_declined{position, reason}` once per position whenever a configured, registered source cannot serve P (so the e2e's `no_set` assertion matches the code).
- **RA5 — the leader's tier wait is per peer** (`HashMap<SocketAddr, TierInstallWait>`), and a timeout joins a decline in the 30 s hold. Task 14.
- **RA6 — an empty-lineage deferral is visible**: after 10 consecutive passes deferring the same set, `tier_set_deferred{position}` (Warn) once per position. Task 3.
- **RA7 — Task 7's gate always computes and stores `purge_gate`**, purge on or off; it clamps only when purge is on.

## Review Focus

1. **A leader change while a follower installs from the tier** (spec §12.1) — the new leader's `TIER_INSTALL` must get `installing` with the running copy's progress, never a second thread; the first leader's wait state dies with its leadership. Pinned by Task 14's `a_leader_change_mid_install_opens_no_second_path`.
2. **A `SET` whose lineage disagrees with its own position or the catalog** (§12.2) — `lineage.last().base > position`, non-ascending, or versions/hashes ≠ the catalog's: refused, never installed, no partial files left. Pinned by Task 2's `parse_refuses_a_lineage_at_or_above_the_position_or_out_of_order` and Task 13's `an_install_whose_set_disagrees_with_the_catalog_declines_and_leaves_nothing`.
3. **K raised above the registered tier count with purge on** (§12.3) — purge stops (`gate = 0`), no release, no alert storm, resumes when a tier registers. Pinned by Task 7's `k_above_the_registered_count_holds_everything_without_a_release` and Task 9's e2e step 5.
4. **A tier forgotten while a follower is mid-install from it** (§12.4) — the running copy reads only the `through` it captured at start; the next `TIER_INSTALL` declines `not_registered`. Pinned by Task 13's `choose_install_set_refuses_an_unregistered_tier` and `a_running_install_keeps_its_captured_through`.
5. **`auto_fetch = false` and no source on a voter** (§12.5) — `declined (no_source)` at once, the session opens inside one round trip, no stall. Pinned by Task 14's `a_voter_without_a_source_declines_at_once_and_the_session_serves_it`.

---

## File structure

| File | Responsibility |
|---|---|
| `uc_consensus/src/reconcile.rs` (tests) | the lineage ⇒ alignment property (Task 1) |
| `uc_node/src/tier/layout.rs` | `SetLabel::lineage`, render/parse, `lineage_agrees` |
| `uc_node/src/tier/writer.rs` | `WriterShared::term_map`, lineage at copy, read-back + verified set, `WriterError::Readback` |
| `uc_node/src/tier/chain.rs` | `pub(crate)` `walk_chunk` / `stream_object`; `watermark_of` over a filtered listing |
| `uc_node/src/tier/materialize.rs` | lineage ++ derived into `term_map.state`; `copy_artifact` `pub(crate)` |
| `uc_node/src/tier/source.rs` (new) | `TierSource` (open, foreign check, registered `through`), `choose_install_set`, the `uc2-tier-fetch` thread |
| `uc_node/src/tier/mod.rs` | `pub mod source` |
| `uc_protocol/src/v2/settings.rs`, `cluster_image.rs` | Settings v6 |
| `uc_protocol/src/v2/datagram.rs` | kinds 28/29 + bodies |
| `uc_crypto/src/transport.rs` | scope of 28/29 |
| `uc_net/src/receiver.rs` | demux 28 → `NetEvent::TierInstall`, 29 → `CtrlMsg::TierInstallStatus`; `SnapIntakeSignals` |
| `uc_net/src/sender.rs` | `TierInstallWait`, the 2 s / 10 s rules before `open_snap_session` |
| `uc_node/src/cluster_fsm.rs` | `purge_requires_tiers` door check + view atomic |
| `uc_node/src/node.rs` | `purge_gate`, clamp + archive safeguard, release latch; term-map publish; `[tier_source]` wiring; tier-first `issue_fetch`; `TierInstall` handler; `pending_install_lineage` |
| `uc_node/src/config_file.rs`, `preflight.rs`, `bin/uc2-node.rs` | `[tier_source]` |
| `uc_node/src/obs/metrics.rs`, `obs/mod.rs`, `examples/m10_alerts.rs`, `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh` | 5 families, 1 alert |
| `uc_ctl/src/tier.rs`, `settings.rs`, `main.rs` | `--new-node` enabled; K in show/apply; "counts toward K" marker |
| `uc_sim/src/world.rs`, `invariants.rs`, `tests/scenarios.rs` | seeding seam, inv14, fuzz arm |
| `uc_node/tests/backup_tier.rs` | A/B/C e2es (`mod writer_e2e`) |
| `uc_node/tests/lin_backup_tier.rs` | the join-from-tier phase |
| `fuzz/src/seeds.rs`, `fuzz/fuzz_targets/uc_protocol_datagram.rs`, corpora | v6 + kinds 28/29 |
| docs (Task 17) | how-tos, references, explainer, BACKLOG, runbook, monitor |

---

### Task 1: Pre-flight — the lineage ⇒ alignment property on the pure kernel

**Files:**
- Test: `uc_consensus/src/reconcile.rs` (new `#[cfg(test)]` tests at the bottom)

**Interfaces:**
- Consumes: `pub fn reconcile(own: &[(u32, u64)], own_durable: u64, leader: &[(u32, u64)]) -> Reconcile` (`reconcile.rs:93`), `Reconcile::{Ok(Outcome { valid_up_to, new_map }), NoCommonPrefix}`, `MAX_TERM_MAP_WIRE_ENTRIES = 64` (`reconcile.rs:59`).
- Produces: a ledger ruling that approach 1 (D2) fixes R12a with the kernel untouched.

This is the whole of A's argument in forty lines: a node whose map is the committed map truncated at Q aligns; a node whose map starts at `(t, S)` is cut.

- [ ] **Step 1: Write the two tests**

```rust
#[cfg(test)]
mod lineage_property {
    use super::*;

    /// A committed map with `n` terms whose bases climb by `step`.
    fn committed_map(n: u32, step: u64) -> Vec<(u32, u64)> {
        (1..=n).map(|t| (t, (t as u64 - 1) * step)).collect()
    }
    fn wire_tail(m: &[(u32, u64)]) -> Vec<(u32, u64)> {
        m[m.len().saturating_sub(MAX_TERM_MAP_WIRE_ENTRIES)..].to_vec()
    }
    /// What `materialize` writes after 3b: the entries at or below S, then every
    /// term whose base lies in (S, Q) — i.e. the committed map truncated at Q.
    fn seeded_from_set(m: &[(u32, u64)], s: u64, q: u64) -> Vec<(u32, u64)> {
        m.iter().copied().filter(|&(_, b)| b <= s || b < q).collect()
    }
    /// What 3a wrote (ruling R12a): the term covering S stamped AT S.
    fn seeded_from_frames_only(m: &[(u32, u64)], s: u64, q: u64) -> Vec<(u32, u64)> {
        let covering = m.iter().rev().find(|&&(_, b)| b <= s).map(|&(t, _)| t).unwrap();
        let mut v = vec![(covering, s)];
        v.extend(m.iter().copied().filter(|&(t, b)| t > covering && b < q));
        v
    }

    #[test]
    fn a_set_seeded_map_aligns_and_keeps_everything_up_to_q() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };
        for _ in 0..2000 {
            let n = (next() % 400 + 1) as u32;
            let step = next() % 5000 + 64;
            let m = committed_map(n, step);
            let end = (n as u64) * step;
            let s = next() % end;
            let q = s + 1 + next() % (end - s);
            let own = seeded_from_set(&m, s, q);
            let leader = wire_tail(&m);
            if leader[0].1 > q {
                // More than 64 terms opened after Q: today's wipe-and-rejoin, not A's case.
                assert!(matches!(reconcile(&own, q, &leader), Reconcile::NoCommonPrefix));
                continue;
            }
            match reconcile(&own, q, &leader) {
                Reconcile::Ok(o) => assert!(o.valid_up_to >= q, "n={n} s={s} q={q} cut to {}", o.valid_up_to),
                Reconcile::NoCommonPrefix => panic!("n={n} s={s} q={q}: no common prefix"),
            }
        }
    }

    #[test]
    fn a_frames_only_map_is_cut_which_is_ruling_r12a() {
        let m = committed_map(1, 1 << 20); // one term, base 0 — the e2e's shape
        let (s, q) = (38_528, 230_880);
        let own = seeded_from_frames_only(&m, s, q);
        assert_eq!(own, vec![(1, s)]);
        match reconcile(&own, q, &wire_tail(&m)) {
            Reconcile::Ok(o) => assert_eq!(o.valid_up_to, 0, "the cut R12a observed"),
            Reconcile::NoCommonPrefix => panic!("R12a's cut, not a wipe"),
        }
    }
}
```

- [ ] **Step 2: Run** `cargo test -p uc_consensus lineage_property -- --nocapture`. Expected: both PASS (the kernel already behaves this way; the test documents why A is sufficient). If the first FAILS, stop and record the failing `(n, s, q)` in the ledger — D2 would be wrong and the plan needs the maintainer.
- [ ] **Step 3: Commit** `git commit -m "consensus: pin the lineage-seeded alignment property (3b pre-flight)"`.

---

### Task 2: `SET` lineage — render, parse, agreement

**Files:**
- Modify: `uc_node/src/tier/layout.rs` (`SetLabel` :174-182, `render` :185-207, `parse` :209-276, `from_catalog` :280-301)
- Modify: `uc_node/src/tier/writer.rs` (`place_set` :695-744, the `Put::Exists` compare)
- Test: in-module tests in `layout.rs`; the `place_set` compare in `writer.rs`'s tests

**Interfaces:**
- Produces:
  ```rust
  pub struct SetLabel { /* existing */ pub lineage: Vec<(u32, u64)> }
  impl SetLabel {
      /// Every field equal except `lineage`, which agrees iff the shorter is a suffix of the longer (PF1).
      pub fn agrees_with(&self, other: &SetLabel) -> bool;
  }
  pub fn lineage_agrees(a: &[(u32, u64)], b: &[(u32, u64)]) -> bool;
  pub fn render_lineage(l: &[(u32, u64)]) -> String;            // "t:b,t:b"
  pub fn parse_lineage(v: &str, position: u64) -> Result<Vec<(u32, u64)>, TierError>;
  ```
- `from_catalog(e)` now takes `lineage: Vec<(u32, u64)>` as a second parameter: `pub fn from_catalog(e: &SetEntry, lineage: Vec<(u32, u64)>) -> SetLabel`.

Rules: `render` emits `lineage=` after `time_ns` and before the row lines. `parse`: missing → `bad("SET", "set_lineage_missing")`; empty value, a non-`t:b` item, a non-decimal number, terms not strictly increasing, bases not strictly increasing, or `last.base >= position` (RA1) → `bad("SET", format!("set_lineage_malformed: {detail}"))`. Such a set lists in `TierListing::corrupt_sets` (F4) — `tier verify` names the reason, `tier show` marks it CORRUPT, nothing starts or fetches from it.

- [ ] **Step 1: Failing tests** (in `layout.rs`'s test module):

```rust
#[test]
fn set_renders_and_parses_its_lineage() {
    let mut l = sample_label(); // the existing fixture, or build one with two rows
    l.lineage = vec![(1, 0), (3, 4096), (4, 90_112)];
    l.position = 100_000;
    let text = l.render();
    assert!(text.contains("\nlineage=1:0,3:4096,4:90112\n"));
    assert_eq!(SetLabel::parse(&text).unwrap(), l);
}

#[test]
fn a_set_without_lineage_is_refused_by_name() {
    let text = sample_label().render().lines().filter(|l| !l.starts_with("lineage=")).collect::<Vec<_>>().join("\n") + "\n";
    let e = SetLabel::parse(&text).unwrap_err().to_string();
    assert!(e.contains("set_lineage_missing"), "{e}");
}

#[test]
fn parse_refuses_a_lineage_at_or_above_the_position_or_out_of_order() {
    for (bad, why) in [
        ("lineage=1:0,2:200000", "above position"),   // position is 100_000
        ("lineage=1:0,2:100000", "at position — RA1: strictly below"),
        ("lineage=2:0,1:4096", "terms not increasing"),
        ("lineage=1:4096,2:0", "bases not increasing"),
        ("lineage=", "empty"),
        ("lineage=1-0", "shape"),
        ("lineage=1:0x10", "hex is not decimal"),
    ] {
        let mut l = sample_label(); l.position = 100_000; l.lineage = vec![(1, 0)];
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
```

In `writer.rs`'s tests, a `place_set` test: write a set with lineage `[(1,0),(2,10),(3,20)]`, then `place_set` the same label with lineage `[(2,10),(3,20)]` → `Ok` (no mismatch); with `[(2,11),(3,20)]` → `Err(TierError::OverlapMismatch(_))`.

- [ ] **Step 2: Run** `cargo test -p uc_node tier::layout tier::writer` → the new tests FAIL (no `lineage` field / method).
- [ ] **Step 3: Implement** the field, `render_lineage`/`parse_lineage`, `lineage_agrees`, `agrees_with`, the `from_catalog` parameter, and change `place_set`'s `Put::Exists` arm to `if !existing.agrees_with(label) { OverlapMismatch }`. Every `from_catalog(...)` caller in the tree passes `Vec::new()` for now (Task 3 fills it); every test fixture that builds a `SetLabel` literal gains `lineage: vec![(1, 0)]`.
- [ ] **Step 4: Run** `cargo test -p uc_node tier` → PASS. Between this task and Task 3 the writer has no lineage to record, so `copy_one_set` already applies Task 3's rule — `if label.lineage.is_empty() { return Ok(false); }` (defer the set) — rather than writing a `SET` its own parser refuses. The `backup_tier` e2es that await a set in the tier therefore time out until Task 3 lands; run only the unit tests here, and the whole of `backup_tier` at the end of Task 3.
- [ ] **Step 5: Commit** `git commit -m "tier: SET carries the writer's lineage; refused by name when missing"`.

---

### Task 3: The term map reaches the writer

**Files:**
- Modify: `uc_node/src/tier/writer.rs` (`WriterShared` :175-201, `copy_one_set` :1104, the `from_catalog` call ~:1140)
- Modify: `uc_node/src/node.rs` — every `self.state.store_term_map(` call site (boot :1446-1452; the snapshot adopt :5791-5796; the `Action::PersistTermMap` exec arm — `grep -n "store_term_map" uc_node/src/node.rs` lists them all)
- Test: `uc_node/tests/backup_tier.rs` (`mod writer_e2e`)

**Interfaces:**
- Produces: `WriterShared::term_map: Mutex<Vec<(u32, u64)>>` (the SM's full in-memory map, `ElectionSm::term_map()`, as last persisted) and `Consensus::publish_term_map(&self)` (locks and clones `self.sm.term_map()` into `backup_shared.term_map` when `backup_shared` is `Some`), called right after each `store_term_map`.
- Consumes: Task 2's `from_catalog(e, lineage)`.

Rule (spec §4.1 as amended, RA1): `lineage = term_map.iter().filter(|(_, b)| *b < S)` — strictly below S. An empty result defers the set: `copy_one_set` returns `Ok(false)`. Normally that is only the first pass after boot (the map not yet published) and logs nothing; the writer counts consecutive deferrals per set position and, at the 10th, logs `obs_event!(Warn, "tier_set_deferred", position)` once for that position (RA6 — e.g. a restarted writer whose persisted map, capped at 300 entries, no longer reaches below an old S).

- [ ] **Step 1: Failing test** — in `backup_tier.rs` `mod writer_e2e`, extend `a_writer_copies_the_agreed_set_and_committed_journal_and_reports_it` (:1059): after the set lands, read `sets/<P>/SET` through `FsTarget`, parse it, and assert `label.lineage == c.node(1).term_map_for_test().into_iter().filter(|(_, b)| *b < p).collect::<Vec<_>>()` and `!label.lineage.is_empty()`. Add the accessor `#[doc(hidden)] pub fn term_map_for_test(&self) -> Vec<(u32, u64)>` on `Node` (reads `backup_shared.term_map` or, without a writer, the persisted `NodeState::term_map()` via `to_pairs`). Add a second assertion in `two_writers_share_a_target_and_the_watermark_never_crosses_a_gap` (:1748): after both writers ran, every set's `SET` parses (no corrupt sets — the two writers' lineages agreed).
Add a writer unit `a_term_opened_exactly_at_s_is_not_in_the_lineage` (term map `[(1,0),(2,4096),(3,S)]` → lineage `[(1,0),(2,4096)]`) and `a_set_deferred_ten_passes_logs_tier_set_deferred_once` (an empty published map; 25 passes → one event, via the obs capture lock).
- [ ] **Step 2: Run** `cargo test -p uc_node --test backup_tier a_writer_copies` → FAIL (empty lineage: the set is deferred forever, the await times out).
- [ ] **Step 3: Implement** the shared cell, `publish_term_map` at every `store_term_map` site plus once at boot after the rederive branch (so a map that needed no rewrite is still published), and the filter in `copy_one_set`.
- [ ] **Step 4: Run** `cargo test -p uc_node --test backup_tier` (whole file) → PASS.
- [ ] **Step 5: Commit** `git commit -m "tier: the writer records the committed lineage at or below each set"`.

---

### Task 4: `materialize` writes the lineage; `--new-node` is enabled

**Files:**
- Modify: `uc_node/src/tier/materialize.rs` (`Mode::NewNode` doc :37-52; `BlockWriter` :761-771 and `take` :773; `write_journal` :838; the state writes :673-685; test :1115)
- Modify: `uc_ctl/src/tier.rs` (:162-169 `new_node_unsupported`, `restore` :173-197), `uc_ctl/src/main.rs` (:434-458 doc strings), `uc_ctl/tests/tier_bin.rs` (:202-247)
- Modify: `uc_node/tests/backup_tier.rs` (:2678 un-ignore)
- Docs touched here (the rest in Task 17): none.

**Interfaces:**
- `write_journal(t, l, s, q, dir, last_lineage_term: u32) -> Result<Vec<TermMapEntry>, TierError>` — `BlockWriter::last_term` starts at `last_lineage_term`, so the term covering S is never re-stamped at S.
- The state write becomes `state.store_term_map(&[lineage_entries, derived].concat())` where `lineage_entries = label.lineage` as `TermMapEntry`s.

- [ ] **Step 1: Failing tests**
  - New unit `a_term_opening_exactly_at_s_is_derived_not_recorded` (RA1): `SET` lineage `[(1,0),(2,4096)]`, S = 20_000, the first frame at S carries term 3 (a NewTerm at S): `st.term_map() == [(1,0),(2,4096),(3,20_000)]`; and with the frame at S still term 2 (no term change at S), no entry at S is written — the lineage's last term seeds `write_journal`, so the term covering S is never re-stamped at S.
  - Update `materialize_builds_an_instance_dir_that_verify_accepts` (:1115): the expected map is the fixture's full committed map truncated at Q — with the fixture's lineage `[(1, 0)]` and the frames' later terms, `vec![TermMapEntry{term:1, base:0}, TermMapEntry{term:2, base:1536}, …]` (no `(1, 1024)` entry).
  - New unit `materialize_term_map_equals_a_live_nodes_map_truncated_to_q`: build a tier fixture whose `SET` lineage is `[(1,0),(2,4096),(3,20_000)]` with S = 24_000 and chunks over `[S, Q)` whose frames carry terms 3 then 4 (term 4 opening at 30_000), Q = 40_000; assert `st.term_map() == [(1,0),(2,4096),(3,20_000),(4,30_000)]`.
  - `uc_ctl/src/tier.rs:340-344` and `uc_ctl/tests/tier_bin.rs:230-247`: flip — `RestoreMode::NewNode(9)` against a tier with no `TIER` now fails with the `no TIER` text, and the bin run with `--new-node --node-id 9` on a real tier exits 0 and prints `restored:`.
  - Remove `#[ignore]` from `a_new_node_seeded_from_the_tier_joins_and_catches_up` (:2680) and add, after node 9 starts, `assert_eq!(c.node(n9).archive_first_base(), m.start_set, "journal_first stays S")` (checked again after the final `await_until`).
- [ ] **Step 2: Run** `cargo test -p uc_node tier::materialize` and `cargo test -p uc_node --test backup_tier a_new_node` → FAIL (the e2e with R12a's `cut to=0`).
- [ ] **Step 3: Implement**: `build` passes `label.lineage.last().map(|(t,_)| *t).unwrap_or(0)` into `write_journal`; concatenate lineage ++ derived into the state write; delete the R12a paragraph from `Mode::NewNode`'s doc and from `materialize.rs`'s module docs; in `uc_ctl/src/tier.rs` delete `new_node_unsupported` and the early `bail!`, and let `RestoreMode::NewNode(id)` map to `Mode::NewNode { node_id: id }`; the success line prints the `add-learner` / `promote` hint (R11d).
- [ ] **Step 4: Run** `cargo test -p uc_node tier`, `cargo test -p uc_ctl`, `cargo test -p uc_node --test backup_tier a_new_node_seeded_from_the_tier_joins_and_catches_up -- --nocapture` → PASS, node 9 `truncations() == 0`, `wipes() == 0`.
- [ ] **Step 5: Commit** `git commit -m "tier: restore writes the set's lineage; --new-node joins a live cluster (closes R12a)"`.

---

### Task 5: Sim — the seeded-lineage invariant (inv14)

**Files:**
- Modify: `uc_sim/src/world.rs` (a seam near `restart` :3781; the Truncate exec :3196-3200), `uc_sim/src/invariants.rs` (`InvariantChecker` :148, `on_truncate` ~:562), `uc_sim/tests/scenarios.rs` (`fuzz_default_seeds` :634, `fuzz_heavy_seeds` :719)

**Interfaces:**
- Produces:
  ```rust
  impl World {
      /// Rebuild a CRASHED node as `materialize` would from a set at `s` with log to `q`
      /// (`s < q ≤` the cluster-wide committed frontier): its term map becomes the
      /// current leader's map truncated at `q`, its durable/append become `q`, and the
      /// checker records `q` as the node's seeded floor. Returns false (no-op) when no
      /// leader is known, the node is up, or `q` is above the committed frontier.
      pub fn seed_from_set(&mut self, node: usize, s: u64, q: u64) -> bool;
      pub fn seeded_checks(&self) -> u64;   // non-vacuity: reconciles a seeded node survived
  }
  impl InvariantChecker {
      pub seeded_floor: Vec<u64>,          // 0 = not seeded; checked in on_truncate
  }
  ```
- inv14 text: `"seeded lineage (inv14)"` — fires in `on_truncate` when `to < seeded_floor[node]` with detail `node {node} seeded from a set with log to {q} was cut to {to} by leader map {leader:?} against own {own_before:?}`.

Bytes below S are not modelled (the sim's content identity is `term_at(map, pos)`); the seam models exactly what `materialize` produces — a map that is the committed map truncated at Q — which is the property Task 1 pinned on the kernel and this inv pins under the sim's leader churn, drops and duplicates.

- [ ] **Step 1: Failing scenario** in `scenarios.rs`:

```rust
#[test]
fn inv14_a_set_seeded_node_is_never_cut_below_its_log_end() {
    let mut w = World::new(SimConfig { n_nodes: 3, seed: 14, max_steps: 20_000, drop_per_million: 20_000, dup_per_million: 5_000, ..SimConfig::default() });
    let mut seeded = 0u32;
    for round in 0..40 {
        run_storm_with_instants(&mut w, 400).expect("storm");
        if round % 4 == 3 {
            let victim = (round / 4) % 3;
            w.crash(victim);
            run_storm_with_instants(&mut w, 200).expect("storm while down");
            let commit = w.node_commit_high_water(w.current_leader().expect("leader"));
            let q = commit / 2 + commit / 4;
            let s = q / 2;
            if q > s + 1 && w.seed_from_set(victim, s, q) { seeded += 1; }
            w.restart(victim);
        }
    }
    assert!(seeded >= 5, "non-vacuity: {seeded} seedings");
    assert!(w.seeded_checks() > 0);
    assert_eq!(w.wipes(), 0, "a seeded node never wiped");
}
```

And a `fuzz_default_seeds` arm (fourth arm, same seeds) that seeds one crashed node from a set every 2 000 steps as above; the heavy fuzz gets the same arm.

- [ ] **Step 2: Run** `cargo test -p uc_sim inv14` → FAIL (no `seed_from_set`).
- [ ] **Step 3: Implement** the seam (set `nodes[i].term_map`, `durable`, `append`, `commit` to `q`, `committed_hw[i] = q`, `seeded_floor[i] = q`), the `on_truncate` check, the counter, and the fuzz arm. Then an injected-violation unit in `invariants.rs` (the `inv2_catches_*` pattern, :983): a seeded floor of 1000 and `on_truncate(.., to = 500, ..)` returns the inv14 violation.
- [ ] **Step 4: Run** `cargo test -p uc_sim` → PASS; `cargo test -p uc_sim --features sim-heavy fuzz_heavy_seeds --release` once → PASS.
- [ ] **Step 5: Commit** `git commit -m "sim: seed a node from a set and prove it is never cut below its log end (inv14)"`.

---

### Task 6: Settings v6 — `purge_requires_tiers`

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs` (:20-35 consts, :81-118 struct, :121-136 default, :139 encode, :159 decode, :218 assert, tests :228-430), `uc_protocol/src/v2/cluster_image.rs` (:255-262, :281-288)
- Modify: `uc_node/src/cluster_fsm.rs` (door :725-774; view atomic :1272/:1400/:1459/:1614; literals :1598-1616, :2135-2145), `uc_ctl/src/settings.rs` (:49-60, :116-138, :241-259, :300-310), `uc_node/src/config_file.rs` (:222-252, :968-1029, :1013-1027), `uc_node/src/node.rs:11188` comment
- Modify: `fuzz/src/seeds.rs:1942-1975`, regenerate `fuzz/corpus/{uc_protocol_settings,uc_protocol_cluster_frame/03-settings,uc_protocol_cluster_image,uc_node_cluster_artifact}` per `fuzz/README.md`

**Interfaces:**
- Produces: `Settings::purge_requires_tiers: u8` (@46); `ClusterView::purge_requires_tiers: AtomicU8` published/read like `backup_retain_sets`; TOML key `purge_requires_tiers` in `[settings]` (genesis) and in `settings apply`'s file; `settings show` prints `purge_requires_tiers={}` immediately before `datagram_mtu=`.
- Door: `ClusterRefusal::SettingsBounds("purge_requires_tiers")` when `> MAX_BACKUP_TIERS as u8`, in `validate_replicated`'s Settings arm (so apply refuses it on every node, like `retain_sets`).

- [ ] **Step 1: Failing tests** — mirror `settings_v5_round_trips_backup_retention_and_v1_to_v4_read_zero` (:379) as `settings_v6_round_trips_purge_requires_tiers_and_v1_to_v5_read_zero` (loop over `[(5, SETTINGS_LEN_V5), (4, _V4), (3, _V3), (2, _V2), (1, _V1)]`; end with `"a v6 header on a v5 length"` → `None`); update `settings_layout_is_frozen` to `(6, 47)` and the frozen-offset comment; move `decode_is_total_and_refuses_unknown_version_and_target`'s unknown version to 7; `cluster_fsm.rs`: `purge_requires_tiers_above_four_is_refused_with_47` (the `backup_retain_age_secs_max_is_refused_with_47` shape, :2894) and extend `the_view_publishes_backup_retention_and_to_state_returns_it` (:2914); `uc_ctl/src/settings.rs`: `purge_requires_tiers_parses_and_shows` asserting the substring `"backup_retain_age_secs=2592000 purge_requires_tiers=2 datagram_mtu="`; `config_file.rs`: `settings_purge_requires_tiers_seeds_genesis_and_defaults_to_zero` (the :2006 shape) plus `5` refused at the door through the node test `settings_apply_refuses_a_bad_digest_a_missing_file_and_out_of_bounds` (`node.rs:22329`, add a `purge_requires_tiers = 5` file).
- [ ] **Step 2: Run → fail.** **Step 3: Implement** (every literal the research lists; cluster_image's two match arms; the fuzz seeds; corpus regeneration). **Step 4: Run** `cargo test -p uc_protocol -p uc_node -p uc_ctl settings` → PASS; `(cd fuzz && cargo +nightly fuzz build)` compiles.
- [ ] **Step 5: Commit** `git commit -m "settings: v6 purge_requires_tiers (K), refused above MAX_BACKUP_TIERS"`.

---

### Task 7: The gate in the node

**Files:**
- Modify: `uc_node/src/node.rs` (`maybe_persist_snapshot_floor` :5917-6044, esp. the purge branch :6013-6036; `backup_purge_cap` :6057-6096 as the template; the archive `Purge` handler :2214-2225; `Consensus` fields near :3960; `Node::observability()` :3255-3276; unit-test harness defaults :15099)
- Modify: `uc_node/src/obs/mod.rs` (`ObsSources` :104-110 and every literal: `obs/mod.rs:252`, `metrics.rs:2026,2935,3050,3158`, `tests/obs_http.rs:99`, `examples/m10_alerts.rs:433`, `node.rs:3276`), `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` :112-133, `push_backup_tier_families` :703-790, counter block :1230-1236, count test :2301, zero-render :2490)
- Modify: `packaging/prometheus/uc2-alerts.yml` (after :465), `uc_node/examples/m10_alerts.rs` (`ALL_SCENARIOS` ~:80, dispatch :222, copy `scenario_backup_hold_released` :2069), `scripts/m10_alert_fire.sh` (`RULE_META` :282, builder :748, `RULE_BUILDERS` :873)
- Modify: `uc_ctl/src/tier.rs` (`show` :106-115), `uc_node/src/cluster_agent.rs` (`read_committed_backups` :189-202 → also return `purge_requires_tiers`)
- Modify: `docs/how-to/monitor-a-cluster.md:70` (family count — the rest of the doc in Task 17)

**Interfaces:**
- Produces on `Consensus`: `purge_gate: Arc<AtomicU64>` (`u64::MAX` = off/K = 0; else `gate(K)`, 0 = hold everything), `gate_released: bool` (the latch), `backup_gate_released: Arc<AtomicU64>` (counter), `backup_gate_released_level: Arc<AtomicBool>`; `fn purge_gate(&mut self) -> Option<u64>` (`#[inline(never)]`, the `backup_purge_cap` shape); the archive agent gets `arc_gate: Arc<AtomicU64>` and clamps `below.min(gate)` when `gate != u64::MAX`.
- `pub fn gate_position(k: u8, tiers: &[BackupTierEntry]) -> Option<u64>` (pure, in `uc_node/src/tier/mod.rs`): `None` for `k == 0`; else `Some(k-th highest through, or 0 when tiers.len() < k)`.
- `ObsSources` gains `purge_gate: Arc<AtomicU64>`, `backup_gate_released: Arc<AtomicU64>`, `backup_gate_released_level: Arc<AtomicBool>`.
- `cluster_agent::read_committed_backups(d) -> io::Result<Option<(u64, Vec<BackupTierEntry>, u8)>>` (the third = `purge_requires_tiers`); `tier show --instance-dir` prints `(counts toward K=<k>)` after the `k` highest-`through` entries and `purge_requires_tiers=<k>` on the watermark header line.

Rule (spec §5.2/§5.4): with K ≥ 1, in the purge branch `target = min(target, gate)` unless released; released iff `free != FREE_BYTES_UNKNOWN && !fits(free, chunk)` with `chunk = if backup_shared.is_some() { backup_chunk_bytes } else { DEFAULT_CHUNK_BYTES }` (PF3); on the `false → true` edge count once and `obs_event!(Warn, "backup_gate_released", node, gate, free_bytes)`, on `true → false` `obs_event!(Info, "backup_gate_rearmed", …)`. The gate never raises `target` and never touches `snapshot_persisted_floor` (R9a's rule: it clamps the journal purge target; local sets are governed by `retain_sets`). `purge_gate` runs on every `maybe_persist_snapshot_floor` pass, purge on or off, and always stores the atomic (so the metric reads on a node with purge off too); it CLAMPS only when purge is on (RA7).

- [ ] **Step 1: Failing tests**
  - Pure (`tier/mod.rs` tests): `gate_position_for_0_1_2_and_4_tiers_k_above_the_count_and_a_forgotten_tier` — K=0 → None; one tier through 500, K=1 → 500; two tiers 500/900, K=1 → 900, K=2 → 500; four tiers, K=4 → the lowest; K=3 with two tiers → 0; after removing one entry K=2 → 0.
  - `node.rs` unit (copy `the_backup_hold_caps_the_purge_at_the_writers_progress` :16135): `the_gate_caps_the_purge_at_the_kth_tier_and_releases_on_low_space` — no writer (`backup_shared = None`), view publishes `purge_requires_tiers = 1` and one backup entry `through = 4096`, floor at 65 536 with slack 0: the drained `ArchiveCmd::Purge { below }` has `below == 4096`; then `free_bytes = 1` → `below == 65 536`, counter 1, level true; `free_bytes = 1 << 40` → level false, counter still 1.
  - `k_above_the_registered_count_holds_everything_without_a_release`: K = 2, one tier: no `Purge` command at all over 20 passes, counter 0, level false, `purge_gate.load() == 0`.
  - `metrics.rs`: the contract count → 147; `uc2_purge_gate_position` renders header-only when `purge_gate == u64::MAX` and as a sample otherwise; rule test `the_backup_gate_rule_keys_on_its_counter` (the :2425 shape).
  - `uc_ctl`: a unit for the `show` marker over a fake `(pos, entries, k)`.
- [ ] **Step 2: Run → fail.** **Step 3: Implement** (node, obs, alert rule, scenario, builder, show). Run `scripts/m10_alert_fire.sh` locally per its header so its completeness cross-check (`:880-910`, every shipped `alert:` has a builder) passes and every rule fires under promtool. **Step 4: Run** `cargo test -p uc_node gate`, `cargo test -p uc_node obs`, `cargo test -p uc_ctl`, `cargo run -p uc_node --example m10_alerts -- backup_gate_released` → PASS.
- [ ] **Step 5: Commit** `git commit -m "node: backup-gated purge — hold the journal at the K-th confirmed tier, release on low space"`.

---

### Task 8: The writer's read-back

**Files:**
- Modify: `uc_node/src/tier/chain.rs` (`walk_chunk` :179 and `stream_object` :330 → `pub(crate)`), `uc_node/src/node.rs` (publish the committed entry for the writer's tier into `WriterShared::confirmed` beside `publish_term_map`), `uc_node/src/tier/writer.rs` (`WriterShared` gains `confirmed`; `Writer` :241-282 gains `verified: HashSet<String>`; `refresh` :1018-1044; `WriterError` :102-144 + `ALL`; `fail` :837), `uc_node/src/obs/metrics.rs:775` (HELP text), the zero-render list :2490, `packaging/prometheus/uc2-alerts.yml:466-474` (description names `readback`)
- Test: `writer.rs` in-module (a `BackupTarget` wrapper like `VanishOnce` :1755)

**Interfaces:**
- Produces:
  ```rust
  /// The listing filtered to objects that count (PF2 as amended, RA2): confirmed by
  /// `confirmed` (the committed watermark entry for this tier; None = nothing confirmed)
  /// or re-read and checked by this writer.
  pub(crate) fn verified_listing(&mut self, raw: &TierListing, confirmed: Option<&BackupTierEntry>) -> TierListing;
  /// Published by the consensus agent each pass from the committed view, like `term_map`.
  pub confirmed: Mutex<Option<BackupTierEntry>>, // on WriterShared
  /// Re-read one object through the target: header + every block CRC (the reader
  /// drained to EOF), the frame walk for a chunk, the raw hash against the SET for an
  /// artifact. Ok(()) on pass.
  pub(crate) fn readback(t: &dyn BackupTarget, obj: ReadbackObject<'_>) -> Result<(), TierError>;
  pub(crate) enum ReadbackObject<'a> { Chunk { from: u64, to: u64 }, Row { set: &'a SetLabel, row: &'a RowLabel }, Cluster { set: &'a SetLabel } }
  ```
- `refresh` becomes: `let raw = list_tier(..)?; fences…; let ver = self.verified_listing(&raw, shared.confirmed.lock().as_ref()); self.wm = watermark_of(&ver); note_gap…; progress from chain_end over ver` — and keeps `self.listing = raw` for placement (PF2).

Rules: an object this writer creates is read back right after its put; a found object with `to <= confirmed.through` (chunk) or `position <= confirmed.newest_set` (set) counts without a re-read; every other found object is re-read; each at most once per process (`verified` holds names); a failing object is NOT remembered as failed (a retry re-reads it next pass — spec §5.3), counted `Readback`, logged `backup_readback_failed{object, detail}`; a set counts only when its `SET`, every row object and the cluster object verified. Cost: only unconfirmed objects are re-read — normally the newest chunk or set — so a restart or a second writer never re-reads the tier's history; only a tier with no committed entry yet (first registration) is read back once. Note that in the how-to (Task 17).

- [ ] **Step 1: Failing tests** (in-module, with a wrapper target whose `open` returns a file with one flipped byte for a chosen name, and another whose `open` returns `Err(TierError::Io)` once):
  - `an_injected_readback_failure_leaves_the_report_unmoved_and_counts_readback`: two chunks `[0,a)`, `[a,b)`, the second corrupt → `watermark_of(verified) == through a`; `stats.errors(Readback) == 1`; fix the byte → the next `verified_listing` reports `b`.
  - `a_set_with_one_bad_artifact_does_not_count`: the set is absent from `verified_listing(..).sets`.
  - `an_object_this_writer_creates_is_read_back_before_it_counts` (RA2): a set written below `confirmed.newest_set` by this writer (the two-writer case) is opened once after its put, and a corrupt one does not count.
  - `objects_the_watermark_already_confirms_are_not_reread` (RA2): a counting wrapper target; `confirmed = {through: a, newest_set: P}` → zero `open`s of `[0,a)` and of `sets/<P>/…`, one of `[a,b)`; with `confirmed = None` every object is opened once, and a second `verified_listing` call opens nothing.
  - `writer_errors_have_distinct_labels` (:1910) covers 8.
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_node tier::writer` and `cargo test -p uc_node --test backup_tier` → PASS (e2e timings unchanged: the e2e tiers are small).
- [ ] **Step 5: Commit** `git commit -m "tier: the writer reads back every object before it counts (read-back gate)"`.

---

### Task 9: The gate e2e

**Files:**
- Modify: `uc_node/tests/backup_tier.rs` (`Opts` :458 gains `purge_requires_tiers: u8`, seeded into `settings_genesis` at :552-555; `mod writer_e2e`)

- [ ] **Step 1: Write** `every_node_holds_its_purge_at_the_tier_and_a_low_space_node_lets_go` (the `the_hold_lets_go_when_space_runs_low_and_records_a_gap` :2141 shape): `opts("tier-gate", true)` with `purge_requires_tiers: 1`, one writer on node 1; `submit_settled`, `instant_until_complete`, `await_agreed`, `churn(&c, &all, 3)`; assert for every node `archive_first_base() <= watermark(node).through` on every poll over 10 s (not just at the end); pause the writer (`paused_for_test`), `churn(.., 2)`; nodes 0 and 2 still hold (`archive_first_base <= through`); `c.node(2).set_free_bytes_for_test(1)`; `await_until(30, .., c.node(2).archive_first_base() > through)`; `observability().backup_gate_released.load() == 1` on node 2 and 0 on nodes 0/1; node 0 still holds; restore space → level false, counter still 1. **Spec §12.3** inside the same test: raise K to 2 through the replicated path — stage `<instance_dir>/settings.pending` and sign admin op 7 exactly as the existing in-tree settings-apply test does (`grep -rn "settings.pending" uc_node/tests uc_node/src/node.rs` finds the staging + digest helper; `uc_ctl::settings::parse_settings` builds the bytes) → no node's `archive_first_base` moves over 5 s and no `backup_gate_released` event; back to 1 → purge resumes.
- [ ] **Step 2: Run** `cargo test -p uc_node --test backup_tier every_node_holds -- --nocapture` → PASS. (If it fails, the defect is in Task 7's area; fix there.)
- [ ] **Step 3: Commit** `git commit -m "tests: backup-gated purge e2e — every node holds, one low-space node lets go"`.

---

### Task 10: `[tier_source]` — config, boot, the `TierSource` handle

**Files:**
- Create: `uc_node/src/tier/source.rs`
- Modify: `uc_node/src/tier/mod.rs` (`pub mod source;`), `uc_node/src/config_file.rs` (:276-292 pattern; `NodeConfigFile` :408-474; `parse_str_with_env` :789; `StartupOptions` literal :1064-1072; tests :2146-2194 pattern), `uc_node/src/preflight.rs:119-142`, `uc_node/src/node.rs:325-340` (`StartOpts`) and the boot path near :2685 (where the writer is spawned), `uc_node/src/bin/uc2-node.rs:128-136`, `uc_node/tests/backup_tier.rs:418-427` (`NodeH::start` passes `tier_source`)

**Interfaces:**
- Produces:
  ```rust
  // uc_node/src/tier/source.rs
  pub struct TierSource { target: Arc<FsTarget>, app_id: String }
  impl TierSource {
      /// Opens TIER; a present TIER with another app_id → Err(TierError::Foreign) (the
      /// startup refusal `tier_source_foreign`); absent or unreadable → Ok(Self) with the
      /// tier id learned lazily.
      pub fn open(path: &Path, app_id: &str) -> Result<TierSource, TierError>;
      /// The committed `through` for this tier, or None when TIER is unreadable or the
      /// tier id is not registered in `backups`.
      pub fn registered_through(&self, backups: &[BackupTierEntry]) -> Option<u64>;
      pub fn target(&self) -> &dyn BackupTarget;
  }
  ```
  `StartupOptions::tier_source: Option<PathBuf>`, `StartOpts::tier_source: Option<PathBuf>`; `Consensus::tier_source: Option<Arc<TierSource>>` (resolved at boot from `tier_source.or(backup.target)`).
- The startup refusal: the error `Node::start_with` returns gains a `TierSourceForeign { path, tier_app_id, app_id }` variant whose `Display` carries `tier_source_foreign`; `uc2-node`'s named-refusal table (`daemon_refusals.rs` tests) gains it.

- [ ] **Step 1: Failing tests** — `config_file.rs`: `tier_source_section_parses_its_one_key_and_refuses_others` (`[tier_source] target = "/mnt/x"` → `Some(PathBuf)`; `[tier_source]` with no target → `Invalid { field: "tier_source.target" }`; an extra key → `Parse`); `source.rs`: `open_refuses_a_foreign_tier_and_tolerates_an_absent_one`; `registered_through_is_none_for_an_unregistered_tier`; `uc_node/tests/daemon_refusals.rs`: a `node.toml` whose `[tier_source]` points at a tier created for another `app_id` refuses by name `tier_source_foreign`; a node with `[backup]` only resolves its source to `backup.target` (unit on the resolver function `resolve_source(tier_source: Option<PathBuf>, backup: Option<&BackupConfig>) -> Option<PathBuf>`).
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_node config_file tier::source daemon_refusals` → PASS.
- [ ] **Step 5: Commit** `git commit -m "node: [tier_source] — a read-only tier a node may fetch from, foreign refused by name"`.

---

### Task 11: Protocol — kinds 28 and 29

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (after :501; body codecs after :598; tests after :1665), `uc_crypto/src/transport.rs:320-335` (+ test near :1839), `uc_protocol/src/version.rs:86-98`, `uc_net/src/receiver.rs` (`NetEvent` :179, `kind_idx` :285-299 → max 14, demux in `on_datagram` after :2184; `CtrlMsg` :92), `fuzz/fuzz_targets/uc_protocol_datagram.rs`, `fuzz/src/seeds.rs:240-268`

**Interfaces:**
- Produces:
  ```rust
  pub const DGRAM_KIND_TIER_INSTALL: u8 = 28;
  pub const DGRAM_KIND_TIER_INSTALL_STATUS: u8 = 29;
  pub const TIER_INSTALL_BODY_LEN: usize = 16;
  pub const TIER_INSTALL_STATUS_BODY_LEN: usize = 24;
  pub struct TierInstallBody { pub floor: u64, pub commit: u64 }
  #[repr(u8)] pub enum TierInstallState { Declined = 0, Installing = 1, Installed = 2 }
  #[repr(u8)] pub enum TierInstallReason { None = 0, NoSource = 1, NoSet = 2, Verify = 3, NotRegistered = 4 }
  pub struct TierInstallStatusBody { pub state: TierInstallState, pub reason: TierInstallReason, pub position: u64, pub copied: u64 }
  pub fn write_tier_install_body(out: &mut [u8; 16], b: &TierInstallBody);
  pub fn read_tier_install_body(buf: &[u8]) -> Option<TierInstallBody>;              // exact 16, floor ≤ commit
  pub fn write_tier_install_status_body(out: &mut [u8; 24], b: &TierInstallStatusBody);
  pub fn read_tier_install_status_body(buf: &[u8]) -> Option<TierInstallStatusBody>; // exact 24, reserved zero, known state/reason, Declined ⇒ reason ≠ None, Installing/Installed ⇒ reason == None
  ```
  Receiver: `NetEvent::TierInstall { from: NodeId, floor: u64, commit: u64 }` (term-gated like `SnapRedirect`: leader only) and `CtrlMsg::TierInstallStatus { from: SocketAddr, body: TierInstallStatusBody }` (to the sender when `sender_route.is_some()`).
- [ ] **Step 1: Failing tests** — `tier_install_kinds_and_bodies_are_pinned` (the `snap_report_kind_and_body_are_pinned` :1620 shape: absolute LE bytes, too short/long, nonzero reserved, `floor > commit`, unknown state, `Declined` with reason `None`); `uc_crypto`: `tier_install_kinds_are_pairwise`; receiver demux unit (the existing kind-27 demux test's shape); a fuzz seed `"24-tier-install"`, `"25-tier-install-status"` and the two readers in the datagram target.
- [ ] **Step 2: Run → fail.** **Step 3: Implement** (+ the `version.rs` comment: "+ backup tier 3b: pairwise 28 `TIER_INSTALL` / 29 `TIER_INSTALL_STATUS`, Settings v6, `SET` lineage — a 0.10.0 peer drops both kinds; same flag day"). **Step 4: Run** `cargo test -p uc_protocol -p uc_crypto -p uc_net tier_install` → PASS; `(cd fuzz && cargo +nightly fuzz run uc_protocol_datagram -- -max_total_time=30)` clean.
- [ ] **Step 5: Commit** `git commit -m "protocol: TIER_INSTALL (28) and TIER_INSTALL_STATUS (29), pairwise, 0.11.0"`.

---

### Task 12: The `uc2-tier-fetch` thread and the store-only fetch from the tier

**Files:**
- Modify: `uc_node/src/tier/source.rs` (the thread), `uc_node/src/tier/materialize.rs:691` (`copy_artifact` → `pub(crate)`), `uc_net/src/receiver.rs` (`SnapIntakeSignals` factored out of `snap_complete` :3050-3104 and `snap_publish_complete_parts` :2876-2984; `write_foreign_marker` :1249 → `pub`), `uc_node/src/node.rs` (`issue_fetch` :11461-11505, `PendingFetch` :1091-1104, `poll_pending_fetch` :11687-11735, `auto_fetch_step` :11527), `uc_node/src/obs/metrics.rs` (`uc2_snapshot_fetch_total{source}`)
- Test: `uc_node/tests/backup_tier.rs`

**Interfaces:**
- Produces:
  ```rust
  // uc_net/src/receiver.rs
  #[derive(Clone)]
  pub struct SnapIntakeSignals { /* stored_set_pos, incoming_cluster_pos, cluster_install: SyncSender<(u64, PathBuf)>, cnc snapshots handle, snap_adopt_pending */ }
  impl SnapIntakeSignals {
      pub fn publish_stored(&self, pos: u64);     // store-only tail of snap_complete
      pub fn publish_installed(&self, pos: u64, cluster_artifact: PathBuf); // install tail
  }
  // uc_node/src/tier/source.rs
  pub enum FetchMode { StoreOnly, Install }
  pub struct TierFetch { pub position: u64, pub copied: Arc<AtomicU64>, pub done: Arc<Mutex<Option<Result<Vec<(u32, u64)>, String>>>> /* Ok(lineage) */, handle: JoinHandle<()> }
  /// Spawns `uc2-tier-fetch`: for each row in `expect` copy `sets/<P>/row-<id>.ultsnap` →
  /// `<root>/<row>/incoming-<P>.tier.part` (RA3: never the receiver's `incoming-<P>.part`), verify raw len + envelope position + hash against
  /// the CATALOG entry (D8) via `copy_artifact`, then marker + rename to `snap-<P>.ultsnap`;
  /// the cluster object likewise; SET versions must equal the catalog's; on success raise
  /// `signals` per `mode`; on any failure remove every `.part` and the renamed files it
  /// wrote (its own `.tier.part` names only) and report Err(reason).
  pub fn spawn_fetch(src: Arc<TierSource>, root: PathBuf, label: SetLabel, expect: SetEntry, mode: FetchMode, signals: SnapIntakeSignals) -> TierFetch;
  ```
  `PendingFetch` gains `source: FetchSource { Tier(TierFetch), Peer }`; `issue_fetch(from, position, auto)` becomes: if `tier_source` is `Some` and `registered_through() >= position` and `sets/<P>/SET` parses → spawn (store-only) and set `pending_fetch` with `Tier`; if `tier_source` is `Some` but it cannot serve P (not registered, `P > through`, no `SET`, a malformed `SET`) → `obs_event!(Info, "tier_fetch_declined", position, reason)` once per position (RA4; reasons `not_registered`, `no_set`, `lineage`) and the peer path; `poll_pending_fetch` on `Tier`: `done` Err → `obs_event!(Info, "tier_fetch_declined", position, reason)`, clear, and immediately `issue_fetch_peer(from, position, auto)` (the old body); Ok → the existing `stored_set_pos > stored_before` success path (`publish_stored` set it), count `uc2_snapshot_fetch_total{source="tier"}`; a peer success counts `source="peer"`.

- [ ] **Step 1: Failing tests**
  - `source.rs` unit: `the_tier_copy_never_uses_the_receivers_temp_names` (RA3): with a receiver-style `incoming-<P>.part` already present (a peer session mid-copy) the tier copy completes, leaves that file untouched, and the final `snap-<P>.ultsnap` has the catalog hash.
  - `source.rs` unit: `a_store_only_fetch_from_the_tier_lands_every_artifact_with_the_foreign_marker` (a tier fixture + a fake `SnapIntakeSignals` over plain atomics; afterwards `snap-<P>.ultsnap` exists per row + `cluster/snap-<P>.ultcluster`, markers present, no `.part`); `a_mismatching_artifact_removes_what_it_wrote_and_reports_verify` (flip a byte in one tier object: `done == Err("verify")`, no files remain).
  - E2E (`backup_tier.rs`), deterministic by construction (RA4): `a_store_only_fetch_takes_the_set_from_the_tier_and_from_a_peer_when_the_tier_lacks_it` — 3 voters + 1 learner, genesis `auto_fetch = false` and `snapshot_target = learners` (standby instants: only the learner builds, and no voter fetches on its own), the writer on the LEARNER (it holds every standby set), `tier_source` on voter 0 pointing at the tier. Take a standby instant P1; `await_watermark(newest_set == P1)`; THEN trigger a store-only fetch on node 0 with `uc2ctl snapshot fetch` in-process (admin op 9; copy the in-process fetch helper from `uc_node/tests/catalog.rs` or `learner.rs`): assert `metric_labeled(node0, "uc2_snapshot_fetch_total", "source=\"tier\"") == 1`, `source="peer" == 0`, and node 0 holds P1. Pause the writer; take P2; trigger the fetch of P2 on node 0 → `tier_fetch_declined{position=P2, reason="no_set"}` logged once, `source="peer" == 1`, node 0 holds P2.
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_node tier::source`, `cargo test -p uc_node --test backup_tier a_store_only_fetch_takes`, `cargo test -p uc_node --test catalog` (the peer auto-fetch e2e still passes), `cargo test -p uc_net` → PASS.
- [ ] **Step 5: Commit** `git commit -m "node: store-only fetch tries the tier first, verified against the catalog, peers as the fallback"`.

---

### Task 13: `TIER_INSTALL` — the follower

**Files:**
- Modify: `uc_node/src/tier/source.rs` (`choose_install_set`), `uc_node/src/node.rs` (the `NetEvent::TierInstall` arm beside `SnapRedirect` :10633; `maybe_adopt_incoming_snapshot` :5779-5796 for `pending_install_lineage`; a per-pass status send; `Consensus` fields `tier_install: Option<TierFetch>`, `pending_install_lineage: Option<Vec<(u32, u64)>>`, `tier_install_leader: SocketAddr`, `tier_install_last_status_ns`)
- Modify: `uc_node/src/obs/metrics.rs` (`uc2_tier_install_total{outcome}` — the follower counts `installed`/`declined`; the leader counts `timeout`, Task 14)

**Interfaces:**
- Produces:
  ```rust
  /// The newest tier set S with `floor ≤ S ≤ min(commit, through)` that is agreed in the
  /// catalog; `through` None → Err(NotRegistered); none in range → Err(NoSet).
  pub fn choose_install_set(floor: u64, commit: u64, through: Option<u64>, sets: &[SetLabel], catalog: &[SetEntry]) -> Result<(SetLabel, SetEntry), TierInstallReason>;
  ```
  Follower rules (spec §6.3): on `TierInstall { floor, commit }` from the leader: no source → send `Declined(NoSource)` at once; a fetch running → `Installing { position, copied }`; else `choose_install_set` (the `through` captured NOW and handed to the thread — a later `forget` does not reach a running copy); Err → `Declined(reason)` + `obs_event!(Info, "tier_install_declined", reason)`; Ok → `spawn_fetch(.., FetchMode::Install, ..)`, `tier_install_started{position}`, answer `Installing { position, copied: 0 }`. Each pass: if a fetch runs and 1 s passed since the last status, send `Installing { copied }`. On `done`: Err → `Declined(Verify)` (files already removed), count `declined`; Ok(lineage) → `pending_install_lineage = Some(lineage)` (the thread already called `publish_installed`, so `maybe_adopt_incoming_snapshot` runs on this or the next pass and adopts THAT lineage, then `adopt_snapshot_config` + `AdoptFloor` as today), then `Installed { position }` once, `tier_install_done`, count `installed`.
- `maybe_adopt_incoming_snapshot`: `let lineage = self.pending_install_lineage.take().unwrap_or_else(|| self.last_leader_map.clone()); if !lineage.is_empty() { adopt… }`.

- [ ] **Step 1: Failing tests**
  - Pure, exhaustive (PF6): `choose_install_set_never_lands_below_the_floor_or_above_commit` — for every `floor, commit ∈ {0..=12}` with `floor ≤ commit`, `through ∈ {None, Some(0..=12)}`, sets at `{2, 5, 8, 11}` with catalog agreement toggled per set (16 combinations): the result, when Ok, satisfies `floor ≤ S ≤ min(commit, through)`, is agreed, and is the newest such; when Err, no set satisfied it (`NoSet`) or `through` was None (`NotRegistered`).
  - `choose_install_set_refuses_an_unregistered_tier` (None → `NotRegistered` even when a set is in range).
  - Node unit (the fixture of Task 7's node tests): `a_running_install_keeps_its_captured_through` — spawn with through 8 000, then publish a view with the tier forgotten; the fetch completes and `Installed` is sent; a SECOND `TierInstall` afterwards answers `Declined(NotRegistered)`.
  - `an_install_whose_set_disagrees_with_the_catalog_declines_and_leaves_nothing` — the tier's `SET` row version ≠ the catalog's: `Declined(Verify)`, no files under `snapshots/`.
  - `a_second_tier_install_while_one_runs_answers_installing_not_a_second_thread` (review focus 1's follower half): two `TierInstall` events in one pass → one `uc2-tier-fetch` thread (`TierFetch` count 1), two `Installing` statuses.
  - `a_late_tier_install_below_durable_adopts_nothing` (spec §8's "the leader opens the session after 10 s"): the leader gave up and a session installed a NEWER set, so `durable > S` when the tier thread finishes; the handler's existing `durable < pos` guard (`node.rs:5779`) skips lineage/config/floor adoption, `pending_install_lineage` is cleared, and `Installed` is still answered (harmless to a leader no longer waiting).
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_node tier_install` → PASS.
- [ ] **Step 5: Commit** `git commit -m "node: a below-floor follower installs the newest agreed set from its tier on TIER_INSTALL"`.

---

### Task 14: `TIER_INSTALL` — the leader, and the C e2es

**Files:**
- Modify: `uc_net/src/sender.rs` (`serve_nak` :1444-1494; a `TierInstallWait` beside `SnapSession` :301-323; the `CtrlMsg` arm; `drive_snap_session` :1745 as the timing template; `SenderStats` :411 gains `tier_install_timeouts`, `tier_install_sent`), `uc_node/src/node.rs` (`set_snapshot_source` :1898 neighbourhood: hand the sender the journal-first-base and commit readers it needs, if it lacks them), `uc_node/src/obs/metrics.rs` (`uc2_tier_install_total{outcome="timeout"}` from the sender stat)
- Test: `uc_net/src/sender.rs` unit (the sender's existing session unit-test harness), `uc_node/tests/backup_tier.rs`, `docs/how-to/back-up-to-a-tier.md` (the `auto_fetch = false` paragraph — written in Task 17, named here)

**Interfaces:**
- Produces in the sender:
  ```rust
  struct TierInstallWait { peer: SocketAddr, sent_ns: u64, answered: bool, position: u64, last_copied: u64, last_progress_ns: u64 }
  const TIER_INSTALL_ANSWER_NS: u64 = 2_000_000_000;
  const TIER_INSTALL_STALL_NS: u64 = 10_000_000_000;
  const TIER_INSTALL_DECLINE_HOLD_NS: u64 = 30_000_000_000;
  // fields (RA5): tier_waits: HashMap<SocketAddr, TierInstallWait>, tier_declined: HashMap<SocketAddr, u64 /* until_ns */>
  fn try_tier_install(&mut self, to: SocketAddr) -> bool;   // sends 28 unless the peer declined within the hold; true = waiting
  fn drive_tier_waits(&mut self) -> Vec<SocketAddr>;       // peers whose wait expired/declined → caller opens their sessions (one at a time, as today)
  ```
  Leader rules (spec §6.3): in `serve_nak`'s Overrun branch, before `try_open_snap_session`: if THIS peer has no entry in `tier_waits` and is not in `tier_declined`, `try_tier_install(to)` and return — independent of other peers' waits and of a session serving another peer (RA5); a peer with a live wait returns without opening anything; else fall through as today. `drive_tier_waits` each pass, per peer: unanswered for 2 s, or `Declined` → remove the wait, remember it in `tier_declined` for 30 s, `try_open_snap_session(peer, None)` (a session busy with another peer leaves this one to its next NAK, which the hold sends straight to the session path); `Installing` → refresh `last_progress_ns` iff `copied > last_copied`; 10 s without progress → remove the wait, remember it in `tier_declined` for 30 s (RA5: a timeout is held like a decline), count `tier_install_timeouts`, open the session; `Installed` → clear, nothing more. The wait state is dropped with leadership (the role gate at :1205-1230).

- [ ] **Step 1: Failing tests**
  - Sender unit: `a_below_floor_nak_sends_tier_install_before_any_session` (the overrun path with a journal that lacks the position → one kind-28 datagram, `snap_sessions == 0`); `no_answer_in_two_seconds_opens_the_session`; `a_decline_opens_the_session_at_once_and_is_remembered_for_thirty_seconds` (a second NAK from the same peer within the hold opens the session with no kind 28); `installing_with_rising_copied_holds_the_session_ten_seconds_past_the_last_rise`; `installed_opens_nothing`; `two_below_floor_followers_each_get_tier_install` (RA5: two peers NAK below the floor in one pass → two kind-28 datagrams, two waits, no session); `a_timeout_is_held_like_a_decline` (after a 10 s stall the same peer's next NAK goes straight to the session path).
  - E2E (`backup_tier.rs`, `mod writer_e2e`):
    - `a_learner_with_a_tier_source_joins_a_purged_cluster_from_the_tier` — `opts("tier-src", true)` + `purge_requires_tiers: 0`, writer on node 1; churn until every voter's `archive_first_base > 0` and the tier's `through ≥` that; start a fresh learner `NodeH` (the Task 4 e2e's shape) with `tier_source = tier.path()`; await it reaching commit; assert `c.node(l).observability().sender.snap_sessions.load() == 0` on the leader, the learner's `metric_labeled("uc2_tier_install_total", "outcome=\"installed\"") == 1`, and `learner.archive_first_base() >= S`.
    - `a_corrupt_tier_artifact_declines_and_the_session_installs_it` — the same, with one byte flipped in `sets/<S>/row-0.ultsnap` before the learner starts: `outcome="declined"` 1 on the learner, `snap_sessions == 1` on the leader, the learner still reaches commit.
    - `a_voter_without_a_source_declines_at_once_and_the_session_serves_it` (review focus 5) — `auto_fetch: false` in genesis, a voter restarted after its peers purged past it, NO `tier_source`: `tier_install_declined{reason="no_source"}` logged once, `snap_sessions == 1`, the session opened within 1 s of the NAK (time the leader's `tier_install_sent` → `snap_sessions` edge).
    - `a_leader_change_mid_install_opens_no_second_path` (review focus 1) — make the tier copy slow enough to observe with a test seam `#[doc(hidden)] pub fn set_tier_fetch_delay_for_test(&self, per_artifact: Duration)` on `Node` (the thread sleeps that long before each artifact; default zero); stop the leader while the learner reports `Installing`; the new leader's `tier_install_sent` becomes 1, the learner's spawn count stays 1 (`#[doc(hidden)] pub fn tier_fetches_spawned(&self) -> u64` on `Node`), `snap_sessions == 0` on both leaders, the learner reaches commit.
- [ ] **Step 2: Run → fail.** **Step 3: Implement.** **Step 4: Run** `cargo test -p uc_net tier`, `cargo test -p uc_node --test backup_tier -- tier_source corrupt_tier voter_without leader_change` → PASS; `cargo test -p uc_node --test learner` (the existing below-floor joins still open a session: no source configured → one round trip then the session).
- [ ] **Step 5: Commit** `git commit -m "net: the leader asks a below-floor follower to install from its tier before opening a session"`.

---

### Task 15: `lin_backup_tier` — a learner joins from the tier mid-load

**Files:**
- Modify: `uc_node/tests/lin_backup_tier.rs` (:305-577; `ClusterCfg` at `tests/lincheck_v2/mod.rs:239` — `purge`, `journal_segment_bytes`; `random_config_op` :1414 shows `AddLearner` through `admin_request`), `uc_node/tests/lincheck_v2/mod.rs` (a `tier_source: Option<PathBuf>` on the cluster start options so the added learner gets one)

- [ ] **Step 1: Add the phase** between the existing phases 7 and 8: with purge ON for this run (`ClusterCfg { purge: Some(BelowSnapshot{slack_bytes:0}), journal_segment_bytes: 64 KiB, .. }` — a second test function `a_learner_joining_from_the_tier_mid_load_keeps_the_history_linearizable` rather than widening the first, so the whole-cluster restore test's envelope is untouched): after `through > Q`, wait until every voter's `archive_first_base() > S`, then `admin_request(.., 1 /* AddLearner */, 4, ..)` for a new learner started with `tier_source = backup.target`; keep the workers running; await the learner at commit; assert the leader's `snap_sessions == 0` and the learner's `uc2_tier_install_total{outcome="installed"} == 1`; the verdict phase runs the WGL checker over the whole history as the existing test does.
- [ ] **Step 2: Run** `cargo test -p uc_node --release --test lin_backup_tier -- --nocapture` → both tests PASS (Linearizable).
- [ ] **Step 3: Commit** `git commit -m "tests: lin capstone — a learner joins from the tier mid-load"`.

---

### Task 16: `auto_fetch = false` with a tier source, and the remaining visibility

**Files:**
- Modify: `uc_node/tests/backup_tier.rs`, `uc_node/src/obs/metrics.rs` (every family of this plan present, the zero-render list), `docs/how-to/monitor-a-cluster.md` (family rows; the count was fixed in Task 7)

- [ ] **Step 1: Failing e2e** `auto_fetch_off_with_a_tier_source_restarts_a_voter_through_the_tier`: `opts("tier-nofetch", true)`, genesis `auto_fetch: false`, `snapshot_target = learners`, writer on the learner (so standby sets reach the tier), `tier_source` on every voter: after two standby sets, assert no voter holds a copy (`metric_labeled(.., "uc2_snapshot_fetch_total", ..) == 0` for both sources); stop voter 2, churn past its position until the leader's `archive_first_base` passes it, restart voter 2 → it installs from the tier (`outcome="installed"` 1, `snap_sessions` unchanged) and reaches commit.
- [ ] **Step 2: Run → fail or pass** (it may already pass after Task 14; a pass still pins §6.5). **Step 3: Fix anything red in the owning task's area.** **Step 4: Run** `cargo test -p uc_node --test backup_tier`, `cargo test -p uc_node obs` → PASS.
- [ ] **Step 5: Commit** `git commit -m "tests: auto_fetch = false with a tier source on every voter is a supported configuration"`.

---

### Task 17: Documentation

**Files:**
- Modify: `docs/how-to/back-up-to-a-tier.md` (§6 Restore :200 — replacing a node from the tier; "Purge and the writer's hold" :313 → add "Backup-gated purge" with §5.5's warning and the read-back's cost (only unconfirmed objects are re-read, RA2); "What the tier does not do yet" :326 — delete the two 3b lines, keep the rest; a new "The tier as a source" section: `[tier_source]`, what is fetched from it, `auto_fetch = false` with a source on every voter, the fallback rules and their timings)
- Modify: `docs/how-to/bound-journal-growth.md` ("What happens to a node that falls below the floor" :161 — the tier install; a "Purge with a tier" subsection under :97)
- Modify: `docs/reference/configuration.md` (`[settings]` :116 gains `purge_requires_tiers`; after `[backup]` :153-190 a `[tier_source]` block; "Startup refusals" :192 gains `tier_source_foreign`)
- Modify: `docs/reference/wire-protocol.md` (kinds table :74-87: rows 28/29; body sections after :274; the kind-3 Settings row → v6)
- Modify: `docs/reference/uc2ctl.md` (`tier restore` :859 — `--new-node` no longer refused, the `add-learner`/`promote` follow-up; `tier show` — the K marker; `settings apply/show` — the key; "Refusal reasons" :1052)
- Modify: `docs/how-to/monitor-a-cluster.md` (the five families, the new events, `Uc2BackupGateReleased`), `docs/ops/uc2-runbook.md` (:355 "When something is wrong": `Uc2BackupGateReleased`, `backup_readback_failed`, `tier_install_declined`)
- Modify: `docs/notes/uc2-backup-tier-explained.md` (new section "The tier as a holder": lineage and why a receipt is needed — the term-map argument; the gate; the source; what we learned)
- Modify: `docs/BACKLOG.md:396-428` (close the 3b line: "3b implemented on `design/backup-tier-3b` (not yet released)"; D3 recorded as revisited and kept; the two sub-bullets resolved)
- Modify: `docs/superpowers/specs/2026-10-10-uc2-backup-tier-design.md` errata R12a (one line: "Closed by 3b, Task 4")

- [ ] **Step 1: Write** every item above. Each new config key, refusal, event, metric and alert named in this plan appears in the reference that lists its kind.
- [ ] **Step 2: Run** `python3 scripts/check_doc_links.py` → no errors. `grep -rn "new_node_unsupported" docs/ uc_ctl/ uc_node/` → only the 3a spec's errata and this plan.
- [ ] **Step 3: Commit** `git commit -m "docs: backup tier 3b — lineage, backup-gated purge, the tier as a source"`.

---

### Task 18: Full proof stack and errata

- [ ] **Step 1:** With `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-tier3b`: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; the four feature-gated clippy lines from `ci.yml`; the MSRV gate (`cargo +1.89.0 clippy ... --locked`); `cargo test` (workspace); `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay`; `cargo test -p uc_node --test lin_v2`; `cargo test -p uc_node --test lin_partition_v2`; `cargo test -p uc_crashtest --features hard-crash-tests`; `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`; `cargo test -p uc_node --release --test lin_backup_tier`; `cargo test -p uc_sim --features sim-heavy --release`; `scripts/fuzz_smoke.sh 60 --min-runs 10000 uc_protocol_datagram uc_protocol_settings uc_protocol_cluster_image uc_node_cluster_artifact`; `cargo deny check`; `python3 scripts/check_doc_links.py`.
- [ ] **Step 2:** Record each command and its result line in the ledger. Any red → fix in the owning task's area, re-run that line and everything after it.
- [ ] **Step 3:** Add the spec's `#### Errata (as built, <date>)` block: PF1–PF7 and every execution ruling that changed the spec (at minimum: suffix agreement; read-back gates the report; the non-writer chunk size; the thread; the SET lineage preferred over the leader's tail; §6.7's sim inv replaced by the exhaustive choice test; the decline hold; and RA1–RA7, recording that the spec text for RA1, RA2 and RA3 was amended before execution, not after).
- [ ] **Step 4: Commit** `git commit -m "spec(backup tier 3b): errata as built"`.
