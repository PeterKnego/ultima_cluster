# Jumbo frames — Implementation Plan (plan 2 of 2: the force gate, the join refusal, observability, proof and docs)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the discovered ceiling operable and provable: an operator can demand jumbo paths and have the cluster refuse to start without them; a node whose path is narrower than the committed rung refuses to join instead of stalling; an application developer is told at submit time that their command needs jumbo support; and the rung, the ceiling, the ladder's activity and the failure modes are visible in `/metrics`, two alerts, `uc2ctl status` and the audit log.

**Architecture:** Everything here is additive on top of plan 1 (merged, local `main` `1c0eeb1`). Three new gates live in the consensus agent's pass and fail-stop through the existing named-panic path (`panic!("consensus fatal (fail-stop): …")` → the agent's failed flag → `uc2-node`'s monitor loop emits `agent_failstopped` and exits 1): the `force_jumbo_frames` window, the committed-rung join check, and nothing else. The developer notification is one latched warn per client in `uc_client::Engine` and `uc_remote::RemoteClient`. Observability is six series off existing atomics plus two Prometheus rules with their `RULE_BUILDERS` scenarios. Proof adds one fuzz target and a pre-committed fleet gate doc with a `--selftest`-able driver. Docs finish the release writeup.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol`, `uc_net`, `uc_node` (+ its `obs` module), `uc_client`, `uc_remote`, `uc_ctl`; `packaging/prometheus`, `scripts/`, `bench-infra/scripts/`, `fuzz/` (nightly + cargo-fuzz); docs.

**Spec:** `docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md` — §5.4 (restart/join refusal), §6 (`force_jumbo_frames`), §8 (developer notification), §9 (observability), §10 (the fuzz target, the fleet gate rows a–f), §11 (docs), plus **the "Errata (plan 1, as built)" section, which is binding**: read all five items before writing code. Plan 1 shipped §2–§4, §5.1–§5.3, §5.5, §7 and §10's unit/fault-layer/in-process tiers.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test timers --test services --test reconfig --test jumbo --test obs_http --test obs_log`; after Task 4, `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`.
- **`2.12.0` is still unreleased and this plan adds NO wire or cnc change.** `uc_protocol::version::CURRENT` stays `0.8.0`, `CNC_V2_VERSION` stays 3.2, the workspace version stays `2.11.0` until the release procedure. Nothing in this plan may touch a frozen layout: not `RUNGS`, not kinds 24/25, not the probe/ack bodies, not Settings v2, not `CNC_OFF_PAYLOAD_CEILING`. A task that finds itself needing one has found a plan defect — stop and report.
- **Frozen once shipped, each pinned by a test whose comment says so**: `force_jumbo_frames` as the `node.toml` key name and `UC2_FORCE_JUMBO_FRAMES` as its env override; `JUMBO_GATE_WINDOW = 30s`; the refusal names `jumbo_path_too_narrow`, `jumbo_peer_silent`, `path_below_committed_mtu`; the six metric names in §9 exactly as written; the two alert names `Uc2MtuDiscoveryStalled` and `Uc2PathBelowMtu`.
- **The hot loops take nothing new that a constant-time check cannot cover.** The force gate and the join check are **one-shot**: each holds a `bool`/`Option` on the consensus struct and, once resolved, costs one `if` per pass (the same shape as `maybe_commit_datagram_mtu`'s throttle). The two new counters added to hot paths are: one `EMSGSIZE` branch on a `send_to` whose `io::Result` is already produced, and one compare-and-increment in the leader's append path. Anything heavier is a finding (CLAUDE.md: "code in a hot loop's body costs even on paths that never run").
- **`uc_client` and `uc_remote` stay small-dep.** `uc_obs` is a dependency-free leaf and may be added to both. `uc_remote` must NOT gain `uc_protocol` as a runtime dependency — it is the crate a third-party remote client copies. Its payload constants stay local, pinned to `uc_protocol`'s values by a **dev-dependency** test.
- **Every metric reads an atomic that already exists or a cnc word already written.** No new shared-memory word, no new cnc offset (that would be a flag day).
- **Fleet spend is user-gated.** This plan WRITES the gate doc with pre-committed bars and a driver; it does not run a fleet. A dev-box run is smoke, never a gate (CLAUDE.md "Benchmarking discipline"). Never write scratch to `/tmp`; test instance dirs go through `CARGO_TARGET_TMPDIR`.
- **Surfaces this plan builds on (as built, verified 2026-09-12)**: `uc_net::probe::ProbeTable::{own_min_rung() -> u32, table_min(&[SocketAddr]) -> Option<u32>, get(SocketAddr) -> Option<PeerProbe>, peers() -> Vec<SocketAddr>, unsent() -> u64, note_unsent(), on_peer_seen(SocketAddr)}` and `PeerProbe { verified, advertised, attempts, next_due_ns }` (`uc_net/src/probe.rs`); `uc_net::sender::SenderStats::probes_sent`, `uc_net::receiver::FollowerStats::{probes_answered, probe_acks, probes_wrong_length}`; `uc_net::sockopt::set_dont_fragment` (Linux/Android only, else `io::ErrorKind::Unsupported`); `uc_protocol::v2::datagram::{RUNGS, MTU_DEFAULT, MTU_BOUND, JUMBO_MIN_RUNG, is_rung, payload_ceiling(usize, bool), MAX_PAYLOAD_DEFAULT}`; `uc_node::Node::{datagram_mtu() -> u32, payload_ceiling() -> usize, probe_table() -> Arc<ProbeTable>, can_serve(), is_leader()}`; `Consensus` fields `cnc`, `buffer`, `pass_mono_ns`, `max_payload`, `last_cluster_append`, `probe_table`, `live_mtu`, `crypto_on`, `next_mtu_check_ns`, and its methods `refresh_from_view` (`uc_node/src/node.rs` ~5960), `maybe_commit_datagram_mtu` (~5627), `committed_rung` (~512), the `can_serve_flag` store in `do_work` step 5 (~3690), the named fail-stop `panic!` idiom (~8238) and `ring_error_fail_stop` (~6113) as its worked example; `uc_node::obs::metrics` (`METRIC_NAMES` list ~60–150, `push_gauge`/`push_counter` ~242–264, `ObsSources` in `obs/mod.rs:45`, which already carries `sender`/`receiver`/`cnc`); `uc_node/src/bin/uc2-node.rs`'s monitor loop (~185–240) and its `agent_failstopped` exit; `uc_node::audit::op_name` (`:137`, `7 => "settings_apply"`); `uc_ctl/src/main.rs`'s status printer (~880–915) and `uc_ctl/src/settings.rs::show` (~194); `uc_client::engine::{EngineConfig::max_payload, Shared, SubmitError::PayloadTooLarge { len, max }}` and `uc_client::error::ClientError::PayloadTooLarge`; `uc_remote::{RemoteConfig::out_ring_bytes_resolved (`engine.rs:210`, the `1344` literal at `:217`), RemoteError::PayloadTooLarge}`; `uc_gateway::edge`'s `RETRY_PAYLOAD_TOO_LARGE` write (`edge.rs:1346`); `packaging/prometheus/uc2-alerts.yml` (23 rules, `- alert:` blocks with a comment block then `expr`/`for`/`labels`/`annotations`); `scripts/m10_alert_fire.sh` (`RULE_BUILDERS` ~666, `load_scenario`, `new_rule`, `add_hold_last`, `select`, `total_for`, one builder at ~521); `fuzz/fuzz_targets/uc_protocol_settings.rs` as the target shape, `fuzz/src/seeds.rs` + `cargo +nightly run --bin seed-corpus` for the corpus, `fuzz/README.md:121/132` for the "committed corpus is exactly the generator's output" rule; `bench-infra/scripts/m13_hop_bench.py`'s `--selftest` as the driver idiom; `docs/VERIFICATION.md`, `docs/BACKLOG.md` item 1, `RELEASES.md`, `docs/releases.md`, `docs/security/threat-model.md`.
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first** (mutation is acceptable for a defensive branch); the commit body says how.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `uc_net/src/sender.rs`, `uc_net/src/probe.rs` | `emsgsize` counter; `note_unsent` no longer spends an attempt; one clock read per pass | 0 |
| `uc_node/src/node.rs` (append path), `uc_node/src/obs/metrics.rs` | `commands_over_standard` counter; the six §9 series | 1 |
| `uc_ctl/src/main.rs`, `uc_node/src/node.rs` (audit source) | `uc2ctl status` ceiling/rung/over-standard lines; `settings_apply` audit `source` | 1 |
| `uc_node/src/config_file.rs` | `force_jumbo_frames` key + `UC2_FORCE_JUMBO_FRAMES` override | 2 |
| `uc_node/src/node.rs` (gates) | the force window and the committed-rung join check, both one-shot, both named fail-stops | 2 |
| `uc_node/tests/jumbo.rs` | force-gate and join-refusal tests (spec §10 fault-layer items c and d) | 2 |
| `uc_client/src/engine.rs`, `uc_client/Cargo.toml` | the latched submit warning + remedy text on `PayloadTooLarge` | 3 |
| `uc_remote/src/{engine.rs,client.rs,error.rs}`, `uc_remote/Cargo.toml` | the same warning; the `1344` literal becomes the bound; a dev-dep pin test | 3 |
| `uc_gateway/src/edge.rs` | remedy text on the `RETRY_PAYLOAD_TOO_LARGE` path | 3 |
| `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh` | `Uc2MtuDiscoveryStalled`, `Uc2PathBelowMtu` + two scenarios | 4 |
| `fuzz/fuzz_targets/uc_protocol_probe.rs`, `fuzz/src/seeds.rs`, `fuzz/corpus/uc_protocol_probe/`, `fuzz/README.md` | the 24th target | 4 |
| `docs/benchmarks/uc2-jumbo-frame-discovery-gate-TEMPLATE.md`, `bench-infra/scripts/jumbo_gate.py` | the pre-committed bars and a `--selftest`-able driver | 5 |
| `docs/notes/uc2-jumbo-frame-discovery-explained.md`, `docs/how-to/jumbo-frames.md` | the explainer and the how-to | 6 |
| `docs/how-to/upgrade-a-cluster.md`, `docs/ops/uc2-runbook.md`, `docs/reference/configuration.md`, `docs/security/threat-model.md`, `docs/VERIFICATION.md`, `docs/BACKLOG.md`, `docs/reference/{wire-protocol,cnc-page}.md` | the remaining rewritten statements + the pre-existing "2.11 pending" sweep | 6 |
| `RELEASES.md`, `docs/releases.md`, `CLAUDE.md` | the `2.12.0` release writeup | 7 |
| `uc_net/src/{probe.rs,sender.rs,receiver.rs,sockopt.rs}`, `uc_node/src/node.rs`, `uc_client/tests/engine_synthetic.rs` | the ledger's deferred minors that ride here | 7 |

---

### Task 0: the two counters discovery needs, and two carried probe fixes

**Files:**
- Modify: `uc_net/src/sender.rs` (`SenderStats`, the `send_to` call sites, `send_due_probes`)
- Modify: `uc_net/src/probe.rs` (`note_unsent`, the `earliest_due_ns` doc already fixed in plan 1 — do not touch it)

**Interfaces:**
- Produces: `SenderStats::emsgsize: AtomicU64` (non-probe sends the kernel refused for size) and `SenderStats::probe_emsgsize: AtomicU64` (probe sends it refused — expected on a narrow path, kept separate so the alert's counter stays clean); `ProbeTable::note_unsent_for(&self, peer: SocketAddr)` replacing the argument-less `note_unsent()` (the old name is removed; one caller).

- [ ] **Step 1: Write the failing tests**

`uc_net/src/fault.rs` needs one new knob before the sender test can exist: `FaultConfig::emsgsize_over: usize` (default `usize::MAX`), which makes `send_to` return `io::Error::from_raw_os_error(libc::EMSGSIZE)` for a longer datagram — the in-process stand-in for a do-not-fragment send the kernel refuses, and the mirror of `max_datagram` (which models the silent drop at a downstream hop; loopback's 65 536 B MTU cannot provoke the real error). With it, in `uc_net/src/sender.rs`'s `mod tests`:

```rust
    #[test]
    fn an_emsgsize_send_is_counted_and_probe_emsgsize_is_separate() {
        let b = jumbo_buffer();
        let (mut s, _tx) = sender_to(&[&fake], &b);
        s.set_faults_for_test(FaultConfig { emsgsize_over: 1408, ..FaultConfig::default() });
        // A DATA datagram past the cap: counted in `emsgsize`.
        append_n(&b, 1, 4096);
        s.do_work();
        assert_eq!(s.stats().emsgsize.load(Ordering::Relaxed), 1);
        // A probe past the cap: counted in `probe_emsgsize`, NOT `emsgsize`.
        let t = ProbeTable::new(ProbeCadence::default());
        t.set_peers(&[fake.addr()]);
        s.set_probe_table(Arc::clone(&t));
        s.do_work();
        assert!(t.get(fake.addr()).unwrap().attempts >= 1);
        assert!(s.stats().probe_emsgsize.load(Ordering::Relaxed) >= 1);
        assert_eq!(s.stats().emsgsize.load(Ordering::Relaxed), 1, "unchanged by probes");
    }
```

(Use the file's own fixtures: `jumbo_buffer()` and `sender_to` exist since plan 1's Task 6; `Fake` is at `sender.rs:2041`; if `Sender` has no faults setter, construct the sender with a `FaultSocket` whose faults are already set, as `sender_to` does — read it and follow suit rather than adding a setter.)

In `uc_net/src/probe.rs`'s `mod tests`:

```rust
    /// Errata-adjacent (final review, plan 1): a probe that never left the host
    /// — no pairwise session yet — must not spend one of the five fast
    /// attempts, or a peer whose handshake takes longer than the fast window
    /// is on the 30 s cadence before its first probe ever goes out.
    #[test]
    fn an_unsent_probe_does_not_spend_a_fast_attempt() {
        let t = ProbeTable::new(fast()); // fast_attempts: 2, fast_ns: 10
        t.set_peers(&[a(1)]);
        for _ in 0..4 {
            let due = t.due(0);
            assert_eq!(due.len(), 1, "still due: nothing was ever sent");
            t.note_unsent_for(a(1));
        }
        assert_eq!(t.get(a(1)).unwrap().attempts, 0);
        assert_eq!(t.unsent(), 4);
        // Once a probe DOES go out, the cadence advances as before.
        t.due(0);
        assert_eq!(t.get(a(1)).unwrap().attempts, 1);
    }
```

- [ ] **Step 2: Run to verify red**

`cargo test -p uc_net an_unsent_probe_does_not_spend_a_fast_attempt an_emsgsize_send_is_counted` → compile errors (`note_unsent_for`, `emsgsize_over`, `emsgsize` missing).

- [ ] **Step 3: Implement**

`uc_net/src/fault.rs` — beside `max_datagram`:

```rust
    /// Jumbo spec §9's stand-in for a DF'd send the kernel refuses: a datagram
    /// longer than this fails with `EMSGSIZE` instead of being sent. The mirror
    /// of [`FaultConfig::max_datagram`], which models the silent loss at a
    /// downstream hop; this one models the LOCAL refusal when the route's MTU
    /// is already known. `usize::MAX` (the default) = no cap. Checked before the
    /// seeded rolls, so it consumes no RNG draw.
    pub emsgsize_over: usize,
```

In `send_to`, immediately after the `max_datagram` check:

```rust
        if buf.len() > self.cfg.emsgsize_over {
            return Err(io::Error::from_raw_os_error(libc::EMSGSIZE));
        }
```

`uc_net/src/sender.rs`:

```rust
    /// Jumbo spec §9: non-probe datagrams the kernel refused for size
    /// (`EMSGSIZE` under do-not-fragment). Must be 0 on a healthy cluster —
    /// `Uc2PathBelowMtu` fires on any increase, because it means a path
    /// degraded below the rung the cluster committed.
    pub emsgsize: AtomicU64,
    /// Jumbo spec §9: the same, for PROBE datagrams, where it is EXPECTED —
    /// probing a rung the local route cannot carry is how the ladder finds the
    /// ceiling. Kept separate so the alert's counter stays clean.
    pub probe_emsgsize: AtomicU64,
```

Every `let _ = self.sock.send_to(…)` in the sender becomes a match that counts the size refusal. There are five such sites (fan-out, heartbeat, NAK serve, replay, snapshot chunk — `grep -n "sock.send_to" uc_net/src/sender.rs`); factor one helper rather than repeating the match:

```rust
    /// One datagram out, counting the one error that means "this path cannot
    /// carry this size" (jumbo spec §9). Every other error stays ignored, as
    /// before: a reliable-UDP sender's job is to keep going and let NAK repair
    /// fill the hole.
    #[inline]
    fn send_counted(&self, to: SocketAddr, buf: &[u8]) -> bool {
        match self.sock.send_to(buf, to) {
            Ok(()) => true,
            Err(e) => {
                if e.raw_os_error() == Some(libc::EMSGSIZE) {
                    self.stats.emsgsize.fetch_add(1, Ordering::Relaxed);
                }
                false
            }
        }
    }
```

`send_to` takes `&mut self` on `FaultSocket`, so `send_counted` needs `&mut self` too — check and match the existing borrow shape; if the borrow checker fights the `self.stats` read, clone the `Arc<SenderStats>` into a local first. `uc_net` gains `libc = { workspace = true }`? — it already has it (plan 1's Task 2). In `send_due_probes`, the probe send uses the same match but increments `probe_emsgsize` and still calls `table.note_unsent_for(peer)` so the attempt is not spent (the kernel refusing the size is also "never left the host").

`uc_net/src/probe.rs` — rename and re-shape:

```rust
    /// A probe for `peer` that never left the host: no pairwise session yet, or
    /// the kernel refused its size. Counts the miss AND gives the peer its
    /// attempt back, so an attempt is only ever spent on a datagram that
    /// actually went out (final review, plan 1).
    pub fn note_unsent_for(&self, peer: SocketAddr) {
        self.unsent.fetch_add(1, Ordering::Relaxed);
        let mut g = self.peers.lock().unwrap();
        if let Some(p) = g.get_mut(&peer) {
            p.attempts = p.attempts.saturating_sub(1);
            p.next_due_ns = 0;
        }
        self.publish_earliest(&g);
    }
```

Delete `note_unsent()` and update its one caller. **Why a decrement and not "don't count until sent":** `due()` bumps `attempts` and schedules the next deadline before the caller knows whether the send succeeds, and moving that bookkeeping after the send would put the mutex back on the send path — the one thing plan 1's fast path removed.

- [ ] **Step 4: Run** `cargo test -p uc_net`, `cargo test -p uc_node --test jumbo` (the three-node proofs must be unchanged), `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`.

- [ ] **Step 5: Commit**

```bash
git add uc_net/src/fault.rs uc_net/src/sender.rs uc_net/src/probe.rs
git commit -m "feat(uc_net): jumbo — count EMSGSIZE sends, keep probe refusals separate, and stop spending a fast attempt on an unsent probe"
```

---

### Task 1: the six §9 series, the status lines, and the audit source

**Files:**
- Modify: `uc_node/src/node.rs` (the leader's append path: `commands_over_standard`; `apply_settings`/`maybe_commit_datagram_mtu`: the audit `source`)
- Modify: `uc_node/src/obs/metrics.rs` (`METRIC_NAMES` + the render fn), `uc_node/src/obs/mod.rs` (`ObsSources` gains the probe table if it does not already reach it)
- Modify: `uc_ctl/src/main.rs` (status), `uc_node/src/audit.rs` (the record's `source` field if it is not free-form)
- Test: `uc_node/tests/obs_http.rs` (the series appear with the right values), `uc_node/src/obs/metrics.rs`'s own tests (the name list), `uc_ctl`'s status test if one exists

**Interfaces:**
- Consumes: Task 0's `SenderStats::{emsgsize, probe_emsgsize}`; `ProbeTable::{own_min_rung, unsent}`; `FollowerStats::probe_acks`; `Node::{datagram_mtu, payload_ceiling}`.
- Produces: `Consensus`'s `commands_over_standard: Arc<AtomicU64>` (exported; also read by `uc2ctl status` off… see below), the six series `uc2_datagram_mtu_bytes`, `uc2_payload_ceiling_bytes`, `uc2_probe_min_mtu_bytes`, `uc2_probe_sent_total`, `uc2_probe_acked_total`, `uc2_send_emsgsize_total`, `uc2_commands_over_standard_total` (seven names; §9's table lists `probe_sent`/`probe_acked` on one row).

**Decision the implementer must not re-litigate:** `uc2_commands_over_standard_total` counts **appended** frames above `MAX_PAYLOAD_DEFAULT` (1312), per §9 ("frames appended above `MAX_PAYLOAD_DEFAULT`"), so it lives on the leader's append path and is leader-only by construction. `uc2ctl status` reads it from `/metrics`? No — `uc2ctl` does not scrape. Export it in `/metrics` from the counter, and in `uc2ctl status` print it only if it is reachable without a new cnc word; **if it is not, omit that one line from `status` and say so in the task report** (§9's status sentence is then satisfied by the ceiling/rung half, and the counter lives in `/metrics` alone). Do not add a cnc word for it — that would be a flag day this plan forbids.

- [ ] **Step 1: Write the failing tests**

In `uc_node/tests/obs_http.rs`, following the shape of its existing scrape assertions (read the file first — it starts a node with `[metrics]` configured and greps the body):

```rust
/// Jumbo spec §9: the discovery series are present on a fresh single-node
/// cluster, with the BASELINE values (errata 4: a solo cluster does not raise).
#[test]
fn the_jumbo_series_report_the_baseline_on_a_solo_node() {
    let (node, _svc, body) = scrape_single_node(); // whatever this file's helper is called
    assert!(body.contains("\nuc2_datagram_mtu_bytes 1408\n"), "{body}");
    assert!(body.contains("\nuc2_payload_ceiling_bytes 1344\n"), "{body}");
    // No peers: own_min_rung answers MTU_BOUND for an empty map (errata 4's
    // parenthetical), so this is 8960 and NOT 0.
    assert!(body.contains("\nuc2_probe_min_mtu_bytes 8960\n"), "{body}");
    assert!(body.contains("\nuc2_probe_sent_total 0\n"), "{body}");
    assert!(body.contains("\nuc2_probe_acked_total 0\n"), "{body}");
    assert!(body.contains("\nuc2_send_emsgsize_total 0\n"), "{body}");
    assert!(body.contains("\nuc2_commands_over_standard_total 0\n"), "{body}");
    let _ = node;
}
```

In `uc_node/src/obs/metrics.rs`'s test module, extend the existing "every name in `METRIC_NAMES` appears in the rendered body" test (it exists — find it by `grep -n "METRIC_NAMES" uc_node/src/obs/metrics.rs`) by simply adding the seven names to `METRIC_NAMES`; that test then fails until the render fn pushes them, which is the red run.

In `uc_node/src/node.rs`'s `Consensus` tests, beside plan 1's `the_commit_rule_*` tests:

```rust
    /// Spec §9: an appended frame above the STANDARD ceiling (1312) is counted,
    /// whatever this cluster's live ceiling is — that counter is the ops-side
    /// view of the §8 developer warning.
    #[test]
    fn a_frame_above_the_standard_ceiling_is_counted() {
        let mut h = harness(); // the in-file Consensus harness
        let c = h.commands_over_standard();
        h.append_client_payload(&[0u8; 1300]);
        assert_eq!(h.commands_over_standard(), c, "under the standard ceiling");
        h.append_client_payload(&[0u8; 2000]);
        assert_eq!(h.commands_over_standard(), c + 1);
    }
```

(Adapt to the harness's real helpers — plan 1 added `harness_with_settings`; read the module and reuse. If the harness cannot append a 2000 B payload because its buffer's bound is small, raise that harness's bound as plan 1's Task 6 did for `jumbo_buffer`.)

- [ ] **Step 2: Run to verify red** — `cargo test -p uc_node --test obs_http the_jumbo_series` and `cargo test -p uc_node --lib metrics` and `--lib a_frame_above_the_standard_ceiling`.

- [ ] **Step 3: Implement**

`node.rs`, in `try_append_client` (and `try_append`, the in-process queue twin) right after a successful append:

```rust
            Ok(_) => {
                // Spec §9: the ops-side view of the §8 developer warning. One
                // compare per append against a CONSTANT, not the live ceiling:
                // the question is "does this deployment depend on jumbo", and
                // the standard ceiling is the same on every cluster.
                if payload.len() > MAX_PAYLOAD_DEFAULT {
                    self.commands_over_standard.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
```

`obs/metrics.rs` — add the names to `METRIC_NAMES` in a commented block ("Jumbo frames (spec §9): the discovered ceiling and the ladder's activity."), then render them beside the existing `uc2_fsm_lag_bytes`/`uc2_log_time_ns` block:

```rust
    // Jumbo frames (spec §9). The rung and the ceiling come off the same two
    // sources every other reader uses — the committed view and the cnc word —
    // so a scrape can never disagree with what the appender enforces.
    push_gauge(out, "uc2_datagram_mtu_bytes",
        "The committed datagram rung this node applies (jumbo spec §5.3); 1408 = the baseline every cluster starts from.",
        s.datagram_mtu() as u64);
    push_gauge(out, "uc2_payload_ceiling_bytes",
        "The live command payload ceiling in bytes — payload_ceiling(rung, crypto), the same value clients read from the cnc page.",
        s.cnc.payload_ceiling());
    push_gauge(out, "uc2_probe_min_mtu_bytes",
        "This node's own verified minimum over its peers (jumbo spec §5.2); 0 while any peer is unresolved.",
        s.probe.own_min_rung() as u64);
    push_counter(out, "uc2_probe_sent_total",
        "PROBE datagrams this node put on the wire (jumbo spec §5.1).",
        s.sender.probes_sent.load(Ordering::Relaxed));
    push_counter(out, "uc2_probe_acked_total",
        "PROBE_ACK datagrams this node received and credited.",
        s.receiver.probe_acks.load(Ordering::Relaxed));
    push_counter(out, "uc2_send_emsgsize_total",
        "Non-probe datagrams the kernel refused for size under do-not-fragment: a path degraded below the committed rung. Must be 0 (Uc2PathBelowMtu).",
        s.sender.emsgsize.load(Ordering::Relaxed));
    push_counter(out, "uc2_commands_over_standard_total",
        "Frames appended above the standard 1312 B ceiling: this deployment depends on jumbo-frame support (jumbo spec §8).",
        s.commands_over_standard.load(Ordering::Relaxed));
```

`ObsSources` (`obs/mod.rs:45`) gains `probe: Arc<ProbeTable>`, `commands_over_standard: Arc<AtomicU64>` and a `datagram_mtu()` accessor (or a `cluster_view: Arc<ClusterView>` if that is already there — read the struct and take the narrowest addition that works). Wire them where the node builds `ObsSources`.

`uc2ctl status` (`uc_ctl/src/main.rs` ~901): add one line after the `services:` line, reading the cnc word and the artifact the way `settings show` does:

```rust
    // Jumbo spec §9. The rung comes from the committed artifact (the same
    // reader `settings show` uses) and the ceiling from the live cnc word, so
    // the two halves cannot disagree; "baseline" vs "discovered" is whether a
    // rung was ever committed.
    let ceiling = cnc.payload_ceiling();
    let (rung, origin) = match uc_node::cluster_agent::read_committed_settings(&args.instance_dir) {
        Ok(Some((_, s))) if s.datagram_mtu != 0 => (s.datagram_mtu, "discovered"),
        _ => (uc_protocol::v2::datagram::MTU_DEFAULT as u32, "baseline"),
    };
    println!("ceiling: {ceiling} B (rung {rung}, {origin})");
```

Audit `source`: `maybe_commit_datagram_mtu`'s append is a `CLUSTER kind=Settings` frame that no admin op produced, so nothing writes an audit line today. Per §9, discovery commits are audited as `settings_apply` with `source = "discovery"` and operator applies with `source = "operator"`. Read `uc_node/src/audit.rs`'s record writer: if the record has no free-form field, add `source: &'static str` to the one call site shape and default the operator path to `"operator"`; the discovery path writes its own line with `"discovery"`. Keep the audit file's existing JSON-lines shape and its fsync-per-record discipline — an extra field is additive for readers.

- [ ] **Step 4: Run** `cargo test -p uc_node --lib --test obs_http --test admin_auth --test jumbo`, `cargo test -p uc_ctl`, the four clippy invocations, `cargo fmt --all -- --check`. Also `python3 -c "import yaml,sys; yaml.safe_load(open('packaging/prometheus/uc2-alerts.yml'))"` is NOT needed here (Task 4 touches that file).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/node.rs uc_node/src/obs uc_node/src/audit.rs uc_ctl/src/main.rs uc_node/tests/obs_http.rs
git commit -m "feat(uc_node, uc_ctl): jumbo — the seven §9 series, the status ceiling line, and the discovery audit source"
```

---

### Task 2: `force_jumbo_frames` and the committed-rung join refusal

**Files:**
- Modify: `uc_node/src/config_file.rs` (the key, the env override, tests)
- Modify: `uc_node/src/node.rs` (`NodeConfig::force_jumbo_frames`; the two one-shot gates in the consensus pass; `can_serve` held false while the force gate is pending)
- Modify: `uc_node/tests/jumbo.rs` (spec §10 items c and d)
- Modify: `packaging/node.example.toml` (the commented key — plan 1 deliberately left it out)

**Interfaces:**
- Consumes: `ProbeTable::{own_min_rung, get, peers}`, `JUMBO_MIN_RUNG`, `Node::{can_serve, datagram_mtu}`, `committed_rung`, the named-`panic!` fail-stop idiom.
- Produces: `NodeConfig::force_jumbo_frames: bool`; `JUMBO_GATE_WINDOW: Duration = 30s` and `JOIN_CHECK_WINDOW: Duration = 5s` in `uc_node::node`; the three refusal strings `jumbo_path_too_narrow`, `jumbo_peer_silent`, `path_below_committed_mtu`.

**The shape, decided here so the implementer does not invent one:** both gates are `Option<JumboGate>` state on `Consensus`, evaluated in `do_work` **before** step 5's `can_serve_flag` store, each costing one `Option::is_some()` once resolved:

```rust
/// Jumbo spec §6 and §5.4: the two startup gates. Both are one-shot — once
/// `Passed`, the pass pays one `matches!` and nothing else.
enum JumboGate {
    /// `force_jumbo_frames`: hold `can_serve` false until every peer has
    /// proven `JUMBO_MIN_RUNG`, or fail-stop at the window.
    Forcing { deadline_ns: u64 },
    /// §5.4: a committed rung above the baseline exists; this node must prove
    /// it carries it before it serves, or fail-stop.
    Joining { committed: u32, deadline_ns: u64 },
    Passed,
}
```

`Forcing` is installed at construction when `cfg.force_jumbo_frames`; `Joining` is installed at construction when the recovered view's `committed_rung` is above `MTU_DEFAULT` (and replaces `Forcing` when both apply — §6: "§5.4's `PathBelowCommittedMtu` still applies under the flag and takes precedence"). Resolution:

- `Forcing`: pass when `own_min_rung() >= JUMBO_MIN_RUNG`. At the deadline, classify every peer from `ProbeTable::get`: a peer with `verified == 0` is **silent**; a peer with `0 < verified < JUMBO_MIN_RUNG` is **too narrow**. Fail-stop naming the first offender by member id, listing all of them in the log line.
- `Joining`: pass when `own_min_rung() >= committed`. At the deadline, fail-stop `path_below_committed_mtu` naming the first peer whose `verified` is below `committed` (or silent).

- [ ] **Step 1: Write the failing tests**

`uc_node/src/config_file.rs` tests:

```rust
    /// Jumbo spec §6: the key and its env override, default false.
    #[test]
    fn force_jumbo_frames_defaults_false_and_takes_an_env_override() {
        let (cfg, _) = load_str(MINIMAL).unwrap();
        assert!(!cfg.force_jumbo_frames);
        let toml = format!("force_jumbo_frames = true\n{MINIMAL}");
        let (cfg, _) = load_str(&toml).unwrap();
        assert!(cfg.force_jumbo_frames);
        let (cfg, _) = load_str_env(MINIMAL, &[("UC2_FORCE_JUMBO_FRAMES", "1")]).unwrap();
        assert!(cfg.force_jumbo_frames, "env override");
        let e = load_str_env(MINIMAL, &[("UC2_FORCE_JUMBO_FRAMES", "yes please")]).unwrap_err();
        assert!(e.to_string().contains("UC2_FORCE_JUMBO_FRAMES"), "{e}");
    }
```

(`load_str_env` is the module's env-injecting loader — `apply_env_overrides` takes an `env: &impl Fn(&str) -> Option<String>`, so the tests already have a shape for this; read one, e.g. the `UC2_BIND` test, and follow it. Accept `1`/`true`/`0`/`false` and refuse anything else by name.)

`uc_node/tests/jumbo.rs` — spec §10 items c and d, using the existing `Fleet`/`bind_fleet` harness:

```rust
/// Spec §10(c): under `force_jumbo_frames`, a path that cannot carry
/// JUMBO_MIN_RUNG fail-stops the node by name within the window.
#[test]
fn the_force_gate_refuses_a_path_too_narrow() {
    let c = spawn_cluster_forced(3, FaultConfig { max_datagram: 1408, ..FaultConfig::default() });
    // Every node's consensus agent panics with the named reason; the node's
    // agent-failed flag is what the daemon turns into exit 1.
    await_agent_failstop(&c.nodes, "jumbo_path_too_narrow", 45);
    for n in &c.nodes {
        assert!(!n.can_serve(), "a node that never proved its paths must not serve");
    }
}

/// Spec §10(c): a member that never answers is a LIVENESS fact, worded as one.
#[test]
fn the_force_gate_refuses_a_silent_peer() {
    let f = bind_fleet(3);
    let nodes = start_forced(&f, &[0, 1]); // the third is never started
    await_agent_failstop(&nodes, "jumbo_peer_silent", 45);
}

/// Spec §10(d)/§5.4: a node whose path is narrower than the rung the cluster
/// already committed refuses to join rather than stalling commit.
#[test]
fn a_restart_below_the_committed_rung_refuses_to_join() {
    // Three nodes on an uncapped loopback commit 8960 (plan 1's proof).
    let c = spawn_cluster(3, FaultConfig::default());
    await_single_leader(&c.nodes, 10);
    await_rung(&c.nodes, 8960, 20);
    // Stop one and restart it behind a 1408 cap against the SAME instance dir,
    // so it recovers the committed artifact and sees 8960.
    let restarted = restart_node_capped(&c, 2, 1408);
    await_agent_failstop(&[restarted], "path_below_committed_mtu", 30);
}
```

`await_agent_failstop` is new in this file: poll each node's agent-failed flags (the same source `uc2-node`'s loop reads — `Node::obs_sources()`/`agents` in `obs/mod.rs`, or add a small `Node::failed_agent() -> Option<&'static str>` accessor if none exists) plus the captured `obs` records. **If the reason string is not reachable from the process** (the panic message goes to the agent's stderr), assert on the flag and capture the message with `uc_obs`'s test capture the way `uc_node/tests/obs_log.rs` does — read that file and reuse its capture, so the test pins the NAME, not just the failure.

- [ ] **Step 2: Run to verify red** — the config test fails to compile (no field); the three `jumbo.rs` tests fail because no gate exists (nodes serve happily).

- [ ] **Step 3: Implement** the config key (`#[serde(default)] force_jumbo_frames: bool` on `NodeConfigFile`, into `NodeConfig`, `("UC2_FORCE_JUMBO_FRAMES", "force_jumbo_frames")` in `ENV_OVERRIDES` with a bool parse in `apply_env_overrides`), then the gate on `Consensus` as sketched above. Placement in `do_work`: immediately before step 5's `can_serve_flag` store, so a pending gate suppresses serving in the same pass it is evaluated:

```rust
        // Jumbo spec §6 / §5.4. One-shot: `Passed` costs one match arm.
        let jumbo_ok = self.check_jumbo_gate();
        self.can_serve_flag
            .store(jumbo_ok && !self.halt_removed && self.sm.can_serve(), Ordering::Release);
```

`check_jumbo_gate` returns `true` for `Passed`/none, `false` while pending, and panics with the named reason at the deadline (the `ring_error_fail_stop` idiom at `node.rs:6113` is the worked example of a named fail-stop: an `obs_event!(Error, …)` naming the reason and its fields, then the `panic!`). Use `self.pass_mono_ns` for the deadline arithmetic, never a fresh clock read.

`packaging/node.example.toml`: add the commented key with three lines of why.

- [ ] **Step 4: Run** `cargo test -p uc_node --lib --test jumbo --test daemon_refusals --test smoke`, `cargo test --workspace --exclude uc_node`, the four clippy invocations, `cargo fmt --all -- --check`. Report each `jumbo.rs` test's duration.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/config_file.rs uc_node/src/node.rs uc_node/tests/jumbo.rs packaging/node.example.toml
git commit -m "feat(uc_node): jumbo — force_jumbo_frames holds serving then fail-stops by name; a node below the committed rung refuses to join"
```

---

### Task 3: the developer notification, and `uc_remote`'s ceiling

**Files:**
- Modify: `uc_client/src/engine.rs`, `uc_client/Cargo.toml`
- Modify: `uc_remote/src/engine.rs` (the `1344` literal and the warning), `uc_remote/src/client.rs` or wherever `submit` enters, `uc_remote/Cargo.toml`
- Modify: `uc_gateway/src/edge.rs` (remedy text only)
- Test: `uc_client/tests/engine_synthetic.rs`, `uc_remote`'s own tests, `uc_gateway/tests/roundtrip.rs`

**Interfaces:**
- Produces: `uc_client` and `uc_remote` depend on `uc_obs`; a `warned_over_standard: AtomicBool` latch in each client's shared state; `uc_remote`'s `BASELINE_PAYLOAD_CEILING: usize = 1344` and `BOUND_PAYLOAD_CEILING: usize = 8896` with a dev-dependency test pinning both against `uc_protocol`.

- [ ] **Step 1: Write the failing tests**

`uc_client/tests/engine_synthetic.rs`:

```rust
/// Jumbo spec §8: a command above the STANDARD ceiling warns once per client,
/// even when this cluster carries it — a dev box's loopback carries everything.
#[test]
fn a_command_above_the_standard_ceiling_warns_once() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_wide(dir.path(), "eng-warn", 1 << 20, 1 << 20);
    let page = CncPage::open_file(&dir.path().join("cnc2.dat"), "eng-warn").unwrap();
    page.store_payload_ceiling(8864); // a cluster that DOES carry it
    let (s, _p) = Engine::attach(dir.path(), "eng-warn", cfg()).unwrap();
    let records = uc_obs::test_capture(); // follow uc_node/tests/obs_log.rs's capture idiom
    s.try_submit(1, &[0u8; 2000]).unwrap();
    s.try_submit(1, &[0u8; 2000]).unwrap();
    let lines: Vec<_> = records.drain();
    assert_eq!(
        lines.iter().filter(|l| l.contains("command_over_standard_ceiling")).count(),
        1,
        "once per client, not once per command: {lines:?}"
    );
    assert!(lines[0].contains("2000") && lines[0].contains("1312") && lines[0].contains("8864"));
    // At or below the standard ceiling: silent.
    let (s2, _p2) = Engine::attach(dir.path(), "eng-warn", cfg()).unwrap();
    s2.try_submit(1, &[0u8; 1312]).unwrap();
    assert!(!records.drain().iter().any(|l| l.contains("command_over_standard_ceiling")));
}

/// Jumbo spec §8: the refusal's text carries the remedy, not just the number.
#[test]
fn payload_too_large_names_the_remedy() {
    // …attach against a page whose ceiling is 1312, submit 2000 B…
    let e = s.try_submit(1, &[0u8; 2000]).unwrap_err();
    let text = e.to_string();
    assert!(text.contains("2000") && text.contains("1312"), "{text}");
    assert!(text.contains("force_jumbo_frames"), "the remedy: {text}");
}
```

(`uc_obs` may have no test capture helper. Check `uc_obs/src/log.rs`'s test module — it has a `TEST_LOCK` and writes through `format_line_at`. If there is no public capture, add one behind `#[cfg(feature = "test-capture")]` or assert on the counter + a single `emit` call via a public `LogLevel` filter instead, and say in the report which you chose. Do NOT make the warning untestable.)

`uc_remote`: a unit test that `out_ring_bytes_resolved` now sizes from the bound, and the dev-dep pin:

```rust
    /// Jumbo spec §7.4: the inflight ring must hold a window of MAX-size
    /// commands, which is the BOUND's crypto-off ceiling since 2.12.0.
    #[test]
    fn the_out_ring_is_sized_for_a_jumbo_window() {
        let c = RemoteConfig { max_inflight: 8, out_ring_bytes: None, ..RemoteConfig::default() };
        assert!(c.out_ring_bytes_resolved() >= 8 * (crate::frame::HEADER_LEN + BOUND_PAYLOAD_CEILING));
    }

    /// `uc_remote` carries its own copies of two numbers `uc_protocol` owns,
    /// because a third-party remote client copies this crate and must not need
    /// the node's protocol crate. A DEV-dependency pins them so they cannot
    /// drift: if this fails, the wire changed and these constants must follow.
    #[test]
    fn the_local_ceilings_match_uc_protocol() {
        use uc_protocol::v2::datagram::{MTU_BOUND, MTU_DEFAULT, payload_ceiling};
        assert_eq!(BASELINE_PAYLOAD_CEILING, payload_ceiling(MTU_DEFAULT, false));
        assert_eq!(BOUND_PAYLOAD_CEILING, payload_ceiling(MTU_BOUND, false));
    }
```

- [ ] **Step 2: Run to verify red.**

- [ ] **Step 3: Implement.** `uc_client`: `uc_obs = { path = "../uc_obs", version = "2.11.0" }`; a `warned_over_standard: AtomicBool` on `Shared`; in the submit door, after the ceiling check passes:

```rust
        // Jumbo spec §8: the dev trap — loopback carries 8960, a 1500 B
        // production path does not. Warn on SUCCESS, once per client, so the
        // developer learns the dependency here rather than at deployment.
        if wire_len > MAX_PAYLOAD_DEFAULT_LOCAL
            && !s.warned_over_standard.swap(true, Ordering::Relaxed)
        {
            uc_obs::obs_event!(Warn, "command_over_standard_ceiling",
                len = wire_len as u64, standard = MAX_PAYLOAD_DEFAULT_LOCAL as u64,
                ceiling = max as u64);
        }
```

`uc_client` already depends on `uc_protocol`, so it uses `MAX_PAYLOAD_DEFAULT` directly (no local copy). The warning's full remedy sentence goes in the event's message or a `remedy` field — keep the record machine-readable (`uc_obs` is JSON lines): fields `len`, `standard`, `ceiling`, and `remedy = "set force_jumbo_frames = true in node.toml"`. `SubmitError::PayloadTooLarge`'s `#[error]` text gains the remedy clause; so does `ClientError::PayloadTooLarge` and the edge's `RETRY_PAYLOAD_TOO_LARGE` log line (the wire reason byte does not change).

`uc_remote`: the two local consts with the "why local" comment, `out_ring_bytes_resolved` using `BOUND_PAYLOAD_CEILING`, the same latched warning in its submit path (omitting the `ceiling` field — protocol v1 advertises none, per §8), `uc_obs` as a dependency and `uc_protocol` as a **dev**-dependency.

- [ ] **Step 4: Run** `cargo test -p uc_client -p uc_remote`, `cargo test -p uc_gateway --features test-util`, `cargo test -p uc_node --test smoke --test services`, the four clippy invocations, `cargo fmt --all -- --check`, and `./scripts/check_publish_metadata.sh` if it exists (two crates gained dependencies — the publish DAG must still hold: `uc_obs` publishes before both).

- [ ] **Step 5: Commit**

```bash
git add uc_client uc_remote uc_gateway/src/edge.rs Cargo.lock
git commit -m "feat(uc_client, uc_remote): jumbo — warn once per client above the standard ceiling; remedy text on the refusals; size the remote ring for a jumbo window"
```

---

### Task 4: the two alerts and the 24th fuzz target

**Files:**
- Modify: `packaging/prometheus/uc2-alerts.yml` (23 → 25 rules)
- Modify: `scripts/m10_alert_fire.sh` (two builders + two scenarios + two `RULE_BUILDERS` entries + the `ALERT_META` severity table at ~274)
- Create: `fuzz/fuzz_targets/uc_protocol_probe.rs`; modify `fuzz/Cargo.toml`, `fuzz/src/seeds.rs`, `fuzz/README.md`; add `fuzz/corpus/uc_protocol_probe/*`

**Interfaces:**
- Consumes: the §9 series from Task 1 (the rules name them).
- Produces: alerts `Uc2MtuDiscoveryStalled` and `Uc2PathBelowMtu`; fuzz target `uc_protocol_probe`.

- [ ] **Step 1: Write the rules** (YAML, in the file's established shape — a comment block explaining the mechanism, then `expr`/`for`/`labels`/`annotations`):

```yaml
  - alert: Uc2MtuDiscoveryStalled
    # Jumbo spec §9. This node has PROVEN more than the cluster has COMMITTED:
    # `uc2_probe_min_mtu_bytes` is its own verified minimum over its peers and
    # `uc2_datagram_mtu_bytes` is the rung the leader committed, so a gap that
    # persists means some OTHER member is holding discovery back — silent, or
    # on a narrower path. Not an error: a cluster that legitimately cannot do
    # better than the baseline never raises this, because a narrow peer pins
    # every node's own minimum too (errata 1). The 60 s window clears the
    # ordinary case, where the gap exists for the 1-3 s discovery takes.
    expr: uc2_probe_min_mtu_bytes > uc2_datagram_mtu_bytes
    for: 60s
    labels:
      severity: warning
    annotations:
      summary: "MTU discovery stalled on {{ $labels.instance }}"
      description: "This node proved {{ $value }} B but the cluster still commits a smaller rung; a peer is silent or narrower. Check uc2_probe_min_mtu_bytes on every member."

  - alert: Uc2PathBelowMtu
    # Jumbo spec §9/§4.3: with do-not-fragment set, a datagram the route cannot
    # carry fails locally with EMSGSIZE instead of fragmenting. Any increase
    # means a path degraded below the rung the cluster committed (or below the
    # 1408 B baseline) — the runtime face of the outage §5.4 refuses at
    # startup. Probe refusals are counted separately and never fire this.
    expr: increase(uc2_send_emsgsize_total[5m]) > 0
    labels:
      severity: critical
    annotations:
      summary: "A path on {{ $labels.instance }} cannot carry the committed MTU"
      description: "{{ $value }} datagrams were refused for size in 5m. The committed rung is monotone: fix the path (MTU/jumbo support) — the cluster cannot lower it."
```

Then the two builders in `scripts/m10_alert_fire.sh`, modelled on `build_Uc2LogTimeFrozen` (a two-series `and`-free comparison needs both series held; read `add_hold_last` and `select`), their `ALERT_META` entries (`"Uc2MtuDiscoveryStalled": {"severity": "warning", "real": False, "scenario": "mtu_discovery_stalled"}`, `"Uc2PathBelowMtu": {"severity": "critical", "real": False, "scenario": "path_below_mtu"}`), their `RULE_BUILDERS` entries, and the two scenario files the script's `load_scenario` reads (find where scenarios live — `grep -n "def load_scenario" -A10 scripts/m10_alert_fire.sh`). The completeness cross-check at `:713` fails until both entries exist, which is this task's red run.

- [ ] **Step 2: Run red** — `python3 scripts/m10_alert_fire.sh --help` or whatever its no-cluster entry point is (read the top of the file; the completeness check runs before any builder, deliberately). Expect it to name the two rules as missing builders, then pass once added. If a local promtool is available, `promtool check rules packaging/prometheus/uc2-alerts.yml` and the script's own promtool pass; if not, say so in the report — the M10 gate row 4 is the fleet-side proof.

- [ ] **Step 3: The fuzz target**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::datagram::{
    PROBE_ACK_BODY_LEN, read_probe_ack_body, read_probe_rung, write_probe_ack_body,
};

fuzz_target!(|data: &[u8]| {
    // A probe body is a rung plus padding: the decoder must never panic and
    // must agree with the first four bytes.
    if let Some(rung) = read_probe_rung(data) {
        assert_eq!(rung, u32::from_le_bytes(data[0..4].try_into().unwrap()));
    }
    // The ack body is exact-length and must round-trip.
    if let Some(b) = read_probe_ack_body(data) {
        let mut re = [0u8; PROBE_ACK_BODY_LEN];
        write_probe_ack_body(&mut re, &b);
        assert_eq!(read_probe_ack_body(&re), Some(b));
    }
});
```

Register it in `fuzz/Cargo.toml`, add seeds to `fuzz/src/seeds.rs` (a baseline probe body, a top-rung probe body, a level ack, a zero-`own_min_rung` ack, a too-short body), regenerate the corpus with `cd fuzz && cargo +nightly run --bin seed-corpus` and **commit the corpus files** (`fuzz/README.md:132` — the committed corpus is exactly the generator's output), and add the target to `fuzz/README.md`'s list and its count (23 → 24).

- [ ] **Step 4: Run** `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`, `(cd fuzz && cargo +nightly fuzz run uc_protocol_probe -- -max_total_time=30)` if `cargo-fuzz` is installed (say so either way), `git status fuzz/corpus` to show the regenerated files, plus `cargo fmt --all -- --check`.

- [ ] **Step 5: Commit**

```bash
git add packaging/prometheus/uc2-alerts.yml scripts/m10_alert_fire.sh fuzz
git commit -m "feat(packaging, fuzz): jumbo — Uc2MtuDiscoveryStalled and Uc2PathBelowMtu with their scenarios; the uc_protocol_probe target"
```

---

### Task 5: the fleet gate doc and its driver

**Files:**
- Create: `docs/benchmarks/uc2-jumbo-frame-discovery-gate-TEMPLATE.md` (the pre-commitment; renamed to `-<run date>.md` when it runs)
- Create: `bench-infra/scripts/jumbo_gate.py`

**Interfaces:**
- Produces: a gate doc carrying spec §10's rows a–f verbatim as **bars pre-committed before any run**, and a driver with `--selftest` (row arithmetic, no fleet/ssh) and `--arms {a,b,c,d,e,f}`.

- [ ] **Step 1: Write the gate doc.** Copy the structure of `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (status header; the pre-committed bar table; a "Reading the rules" section; one results section per row, empty and marked UNRUN). Record, verbatim from spec §10: row a (every node reports `uc2_datagram_mtu_bytes = 8960` within 10 s of the last node's start, 3 of 3 reps), row b (1500 B arm: rung stays 1408, `uc2_send_emsgsize_total = 0`, 64 B throughput paired delta within −3 %, pair count fixed from the base tree's observed spread, minimum 5 pairs), row c (the envelope-map brief's §6 disposition: jumbo soak plateau ≥ 15 % over standard AND the jumbo 64 B rung within −3 %, throughput and p99 — decides whether the runbook *recommends* jumbo, not whether the feature ships), row d (the force gate on the 1500 B arm and one node held down on the 9001 arm: `jumbo_path_too_narrow` within 30 s naming a peer; `jumbo_peer_silent` naming the third), row e (`m5_gate` paired against the base tree, reported, **no bar**), row f (`scripts/hop1_ab.sh` with its same-source rebuild control, dev-box smoke, reported, **no bar**). Add the two facts plan 1 learned that the gate must respect: errata 1 (a narrow cluster probes forever, so row b must expect a non-zero `uc2_probe_sent_total` that keeps climbing — not a leak) and errata 4 (a solo cluster never raises, so row a needs ≥ 2 nodes before any raise can be observed).
- [ ] **Step 2: Write the driver** with `--selftest` asserting the row arithmetic locally (the ≥ 15 % / −3 % comparisons, the pair-count rule, the 10 s adoption window), modelled on `bench-infra/scripts/m13_hop_bench.py --selftest`. The fleet arms shell out through the same ansible/ssh helpers the other drivers use (`grep -n "def ssh\|def run_remote" bench-infra/scripts/m12_fleet_gate.py`). Row c's arm runs UC's own ladder as the blackhole probe and **aborts loudly** if a jumbo arm's nodes still report 1408 after 30 s.
- [ ] **Step 3: Run** `python3 bench-infra/scripts/jumbo_gate.py --selftest` (must pass, no fleet), `python3 -m py_compile bench-infra/scripts/jumbo_gate.py`.
- [ ] **Step 4: Commit**

```bash
git add docs/benchmarks/uc2-jumbo-frame-discovery-gate-TEMPLATE.md bench-infra/scripts/jumbo_gate.py
git commit -m "docs(bench): jumbo — the pre-committed gate doc and a --selftest-able driver"
```

---

### Task 6: the explainer, the how-to, and the remaining statements

**Files:**
- Create: `docs/notes/uc2-jumbo-frame-discovery-explained.md`, `docs/how-to/jumbo-frames.md`
- Modify: `docs/how-to/upgrade-a-cluster.md`, `docs/ops/uc2-runbook.md`, `docs/reference/configuration.md`, `docs/security/threat-model.md`, `docs/VERIFICATION.md`, `docs/BACKLOG.md`, `docs/reference/wire-protocol.md`, `docs/reference/cnc-page.md`

- [ ] **Step 1: The explainer** — why one value cluster-wide (a frame in the log must be shippable to every member, forever), why do-not-fragment (a fragmented probe proves nothing and one lost fragment loses the datagram), the ladder and why fixed rungs beat a binary search, the monotone rule and what it costs (errata 4: a solo cluster cannot raise, and why that is the safe direction), the rejoin reset (errata 2), the dev trap (§8), and what a narrow cluster looks like in the metrics (errata 1: probes forever, `uc2_probe_min_mtu_bytes` equal to the rung).
- [ ] **Step 2: The how-to** — enabling jumbo on AWS/GCP (VPC MTU, the interface setting, and that UC needs no config), verifying with `uc2ctl status` and the two gauges, `force_jumbo_frames` and its two refusals, the CI pin (`ClientConfig::max_payload = MAX_PAYLOAD_DEFAULT` as the dev-time check, §8), what `path_below_committed_mtu` means and that the remedy is the path (never a wipe), and the non-Linux refusal (plan 1's ruling: no DF outside Linux/Android, so the node refuses to start by name — a macOS dev box cannot run a node).
- [ ] **Step 3: The rewritten statements** — `upgrade-a-cluster.md` gains a `2.12.0` section (one combined flag day with the log clock: wire `0.8.0`, cnc `3.2`; **no wipe** because Settings v1 still decodes; delete `max_payload` from every `node.toml`; the DF behaviour change; and that a path below 1436 B IPv4 / 1456 B IPv6 now fails by name instead of fragmenting). `uc2-runbook.md` gains the status line, the two alerts and their remedies. `configuration.md` documents `force_jumbo_frames`. `threat-model.md` gains the fact the final review surfaced: under crypto-off a forged `PROBE_ACK` raises the committed rung, and because the rung is monotone and survives restart **the damage is persistent** — same threat class as forged DATA, new flavour, and the remedy is `[crypto].enabled = true`. `VERIFICATION.md` gains the fault-layer tier (the three-node jumbo proofs, `max_datagram`/`emsgsize_over`), the 24th fuzz target, and the honest sim gap (§10: `uc_sim` does not model datagram size). `BACKLOG.md` item 1: the payload-ceiling question is answered by discovery; the remote-protocol-v2 question stands. Finally sweep the pre-existing staleness the final review flagged: `wire-protocol.md` and `cnc-page.md` still label shipped `2.11.0` facts "2.11 pending" — correct them to shipped.
- [ ] **Step 4: Verify** `grep -rn "2.11 pending" docs/ | wc -l` is 0 and nothing else regressed; read each page back once.
- [ ] **Step 5: Commit**

```bash
git add docs
git commit -m "docs: jumbo — the explainer, the how-to, the upgrade/runbook/threat-model/VERIFICATION statements, and the 2.11-pending sweep"
```

---

### Task 7: the `2.12.0` release writeup, and the deferred minors that ride here

**Files:**
- Modify: `RELEASES.md` (a new top section), `docs/releases.md` (the engineering record), `CLAUDE.md` (the status paragraph and the "Next up" list)
- Modify: `uc_net/src/probe.rs` (docs on `ProbeTable`, `PeerProbe`, `new`, `peers`, `get`, `unsent`), `uc_net/src/sender.rs` (the `.expect("checked above")` restructure; the per-pass clock read), `uc_net/src/sockopt.rs` (fd validity in the SAFETY comments), `uc_node/src/node.rs` (the harness comment the final review parked), `uc_client/tests/engine_synthetic.rs` (the override-above-the-page test)

- [ ] **Step 1: The release writeup.** `RELEASES.md` gets a `2.12.0` section at the top, per the project's release rule: one bullet per feature (jumbo-frame discovery, linking the explainer and the how-to; the monotonic log clock, already on `main`, linking its spec/explainer), one bullet for fixed bugs (the `CncPage::meta()` torn-header panic and the boot-gap attach refusal, both already on `main`), one bullet for performance (link the gate doc, marked UNRUN). `docs/releases.md` gets the deep entry: the flag day's two version bumps, the `max_payload` retirement as an operator-visible break, the five spec errata, and the release-evidence table row. `CLAUDE.md`: update the "Current version" paragraph to name `2.12.0` as pending with wire `0.8.0` / cnc `3.2`, and replace the jumbo item in "Next up" with what actually remains (the fleet gate run).
- [ ] **Step 2: The deferred minors** (each one line, from the archived ledger at `~/uc2-sdd-archive/2026-09-10-uc2-jumbo-frame-discovery-plan1/progress.md`): docs on the `ProbeTable` surface; `send_due_probes`'s `.expect` replaced by a `match`/`let Some(table) = … else`; reuse one per-pass timestamp instead of `now_ns()` per probe check (CLAUDE.md's hot-loop lesson — and say in the report whether it measurably matters or is hygiene); `fd` validity named in `sockopt.rs`'s SAFETY comments; the `node.rs` harness comment corrected to "no peers are seeded into the probe table, so `table_min` misses on both member addresses and answers `None`"; and the client test for an override ABOVE the page's ceiling (page 128, `Some(512)`, accepted).
- [ ] **Step 3: Run** the full Global Constraints list.
- [ ] **Step 4: Commit**

```bash
git add RELEASES.md docs/releases.md CLAUDE.md uc_net/src uc_node/src/node.rs uc_client/tests/engine_synthetic.rs
git commit -m "docs: the 2.12.0 release writeup; polish the jumbo surfaces the plan-1 reviews deferred"
```

---

## Self-review

**Spec coverage.** §5.4 → Task 2 (`Joining` gate, `path_below_committed_mtu`, the restart test). §6 → Task 2 (key, env override, window, both refusals, `can_serve` held, the fail-stop path, §5.4 precedence). §8 → Task 3 (both clients, the latch, the remedy text on all three refusals, the CI pin documented in Task 6's how-to). §9 → Task 1 (all seven series, the status line, the audit `source`) + Task 4 (both alerts with `RULE_BUILDERS` coverage). §10 → Task 2 (fault-layer items c and d), Task 4 (the fuzz target), Task 5 (the gate doc and driver, rows a–f, bars pre-committed). §11 → Tasks 6 and 7 (every named page, the explainer, the how-to, the release writeup). The errata are carried explicitly: errata 1 into the explainer and row b's expectations, errata 2 into the explainer, errata 4 into row a and the solo-node metric test, errata 5 into nothing new (already documented). **Deliberately not here:** the counter-per-append and the clock read are measured by rows e and f rather than assumed; the `daemon_refusals` SIGTERM race the ledger flagged is pre-existing and belongs in `docs/BACKLOG.md` (Task 6, one line), not in this plan's code.

**Placeholder scan.** Three places hand the implementer a decision with the deciding rule stated rather than the code: `uc2ctl status`'s over-standard line (omit it if it needs a new cnc word — forbidden), `uc_obs`'s test capture (use it, add one, or assert the counter — "do not make the warning untestable"), and `await_agent_failstop`'s reason plumbing (flag plus log capture). Each names the file to read and the outcome that counts. No "TBD", no "handle errors appropriately".

**Type consistency.** `note_unsent_for(SocketAddr)` replaces `note_unsent()` in Tasks 0 and its one caller. `SenderStats::{emsgsize, probe_emsgsize}` in Tasks 0 and 1. `FaultConfig::emsgsize_over` in Tasks 0 and (optionally) 2's tests. `ProbeTable::own_min_rung() -> u32` feeds Task 1's gauge and Task 2's two gates. `JumboGate`/`JUMBO_GATE_WINDOW`/`JOIN_CHECK_WINDOW` are Task 2's alone. The seven metric names are spelled identically in Task 1's render, Task 1's test, Task 4's two rules and Task 5's row a/b bars.
