# M14d — Fleet Gate and the 2.8.0 Release: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adjudicate M14 (multi-service) on a real fleet against the bars pre-committed in spec §15, and ship `2.8.0` with the writeup, the doc sweep and the security-posture refresh the release requires.

**Architecture:** No new binary. `uc_gateway/examples/m12_gate.rs` — the gate harness the M12 and M13 fleet drivers already launch as `systemd-run` units — grows a multi-FSM node config, a `SpinCountSm` slow FSM, fan-in submission, a windowed rate, a per-second timeline and a `check-fsms` divergence role. A new driver `bench-infra/scripts/m14_fleet_gate.py` reuses `m12_fleet_gate.py`'s ssh/unit/host helpers and `m13_hop_bench.py`'s pure-verdict + `--selftest` pattern. Rows are adjudicated by pure functions over recorded numbers; the exit code is the verdict. The release half is documentation and version work in the order `docs/how-to/cut-a-release.md` §1 prescribes.

**Tech Stack:** Rust 1.96 (workspace), `clap`, `uc_client::Engine`/`Client`, `uc_service::{StateMachine, SnapshotStateMachine, Sessioned}`, Python 3 (stdlib only — the other drivers use nothing else), ssh + `systemd-run` on Ubuntu hosts, terraform + ansible (`bench-infra/`), `uc2ctl`.

**Spec:** `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` **§15** (binding; §12's fleet-gate paragraph and §14.1's cut are amended by it). Read §15 in full before Task 1.

## Global Constraints

- **Bars are committed before any run** (spec §15 preamble, honest-failure protocol M7–M13): the gate doc's bar table lands in its own commit with every result cell empty, before the commit that records any fleet result. A miss is a FAIL and keeps the bar. Never edit a bar to match a result.
- **Local runs are smoke, never a gate** (CLAUDE.md "Benchmarking discipline"). Every number produced on the dev box in this plan is labelled smoke; rate bars are fleet-only.
- **Row bars, verbatim from spec §15.4:** a `rate(n2eq)/rate(n1) ≥ 0.90`; b `rate(pair)/rate(slow1) ∈ [0.90, 1.10]`; c any divergence = FAIL (blocks the release); d `≤ 15 s` (M9's rule, both clauses); e reported, not barred; f converges `≤ 60 s` with `snapshot_session_refusals() == (0, 0)` on every node and both artifacts present on the learner; g CI + nightly green at the gated commit.
- **"Rate" = client-observed completed operations/s over the steady window** — the middle 8 s of a 12 s arm (2 s warm-up, 2 s tail discarded), envelope on, the direct client at `--inflight 4096`, 64-byte payload (spec §15.3).
- **The slow FSM is defined against the cluster rate**: `K` is chosen by the `calib` arm so `SpinCountSm(K)`'s solo rate is nearest `0.5 ×` the N=1 rate, and recorded (spec §15.3).
- **Topology deviation from spec §15.2, ruled here and recorded in Task 0:** the direct `Engine` client is shmem-attached and therefore runs **on the leader host** (exactly as `m12_fleet_gate.run_direct_arm` and M13's direct arm did — `m12_fleet_gate.py:421-437` runs `client-direct` on `node_hosts[leader]`). A separate client host is not needed. The gate runs on **4 × `c6id.2xlarge`** (32 vCPU): `hosts[0..3]` voters, `hosts[3]` the learner (idle until row f). Rows and bars are unchanged.
- **M14c2 is after this release** (spec §15.1): the two-FSM capstones are *not* in this plan; every place that states coverage says so in §15.1's words.
- **`cargo fmt` stays deferred** (CLAUDE.md): do not reformat files you touch.
- **Never write scratch to `/tmp`** (CLAUDE.md "Local scratch"): the in-process smoke's `--root` defaults under `$HOME/.cache/cargo-target/m12_gate`; keep it.
- **Fleet runs are user-approved** (spec §12/§15): Task 6 is executed only when the user says so; this plan hands them the exact commands.
- **The `v2.8.0` tag and the crates.io publish are the user's steps** (`cut-a-release.md` §4, §6). No task in this plan tags or publishes.
- Commit subjects follow the tree's convention: `type(scope): imperative summary` (examples in `git log --oneline -30`).

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` §15.2 | errata: the client runs on the leader host; 4 hosts | 0 |
| `uc_gateway/examples/m12_gate.rs` — `NodeArgs`, `node_config`, `services_from_flags` | node role: `--services`, `--fsm-lag`, `--purge-below-snapshot`, `--journal-segment-bytes`; stats line gains `snap_refusals` | 1 |
| same file — `CountSm` snapshot impl, `SpinCountSm`, `ServiceArgs`, `run_service_role` | slow FSM + per-id attach + snapshot policy | 2 |
| same file — `run_client_measurement`, `ClientDirectArgs`, `print_result_json`, `Role::CheckFsms`, `Arm::Fsms`, `boot_cluster2` | fan-in submit, windowed rate, timeline, divergence role, in-process two-FSM smoke | 3 |
| `bench-infra/scripts/m14_fleet_gate.py` | the driver: bars, arms, verdicts, selftest, main | 4a–4d |
| `docs/benchmarks/uc2-m14-gate-2026-08-29.md` | the gate doc: bars first (own commit), results after | 5, 6 |
| `docs/notes/uc2-m14-multi-service-explained.md` | the plain-language explainer | 7 |
| `Cargo.toml` + 6 crate manifests, `README.md`, `packaging/`, `docs/how-to/run-a-cluster.md`, `SECURITY.md` | version bump + literal sweep | 8 |
| `RELEASES.md`, `docs/releases.md` | the release writeup | 9 |
| `docs/reference/limits.md`, `docs/how-to/upgrade-a-cluster.md`, `CLAUDE.md`, `docs/VERIFICATION.md` | invalidated-statement sweep | 10 |
| `docs/security/{attack-surface,threat-model,self-assessment}.md` | security-posture refresh | 11 |
| (no files) | rc tag → verify → tag: the user's commands | 12 |

---

### Task 0: Spec errata — the client runs on the leader host

**Files:**
- Modify: `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` (§15.2, §15.3 "What rate means")

**Interfaces:** none.

- [ ] **Step 1: Amend §15.2**

Replace the §15.2 paragraph with:

```markdown
### 15.2 Topology

Four `c6id.2xlarge` (8 vCPU each, 32 vCPU), `us-east-1`, one placement
group, NVMe journals, fsync on — the M13 shape. Roles: `hosts[0..3]` three
voters, `hosts[3]` the learner (idle until row f). **The measuring client is
the direct `Engine`, which is shmem-attached and therefore runs on the
leader host** — exactly as the M12 and M13 direct arms did
(`m12_fleet_gate.py:421-437`). A separate client host was drafted on
2026-08-29 and withdrawn the same day (errata: this paragraph); the
account's 48-vCPU quota leaves room for a fifth host if a remote-path row
is ever added, but no row in §15.4 needs one. Rows a–e use exactly M13's
voter shape, so row a's N=1 number is directly comparable to M13's
full-stack direct arm.
```

- [ ] **Step 2: Amend §15.3's "What rate means" sentence**

Replace `one direct-client process on `hosts[4]` at `m12_gate`'s direct-client` with `one direct-client process on the leader host at `m12_gate`'s direct-client`.

- [ ] **Step 3: Verify and commit**

Run: `grep -n 'hosts\[4\]' docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md`
Expected: no output.

```bash
git add docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md
git commit -m "spec(m14): §15.2 errata — the direct Engine client is shmem-attached and runs on the leader host; four hosts, not five"
```

---

### Task 1: `m12_gate` node role — multi-FSM config, purge, refusal counters

**Files:**
- Modify: `uc_gateway/examples/m12_gate.rs:152-168` (`NodeArgs`), `:402-432` (`node_config`), `:459-507` (`boot_cluster` call site), `:1196-1234` (`run_node_role`)
- Test: `#[cfg(test)] mod tests` at the bottom of the same file, run with `cargo test -p uc_gateway --example m12_gate`

**Interfaces:**
- Consumes: `uc_node::{ServicesConfig, FsmLag, PurgePolicy, DEFAULT_JOURNAL_SEGMENT_BYTES}`, `uc_node::services::parse_fsm_lag(&str) -> Result<FsmLag, String>` (`uc_node/src/services.rs:172`, module is `pub` at `lib.rs:48`), `ServicesConfig::from_ids(&[u8], Option<FsmLag>) -> Result<ServicesConfig, String>` (`services.rs`), `ServicesConfig::declared() -> u64`, `ServicesConfig::resolve_lag(u64) -> FsmLag`, `Node::snapshot_session_refusals() -> (u64, u64)` (`uc_node/src/node.rs:1547`).
- Produces: `fn services_from_flags(services: Option<&str>, fsm_lag: Option<&str>) -> anyhow::Result<ServicesConfig>`; `node_config(..)` gains three trailing parameters `services: ServicesConfig, purge: PurgePolicy, journal_segment_bytes: u64`; the node role's stats line becomes `m12_gate node {id} stats: reports_unattested={n} snap_refusals=({a},{b})`. Node CLI flags: `--services 0,1`, `--fsm-lag lockstep|<bytes-string>`, `--purge-below-snapshot`, `--journal-segment-bytes N`.

- [ ] **Step 1: Write the failing tests**

Append to the bottom of `uc_gateway/examples/m12_gate.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_node::FsmLag;

    #[test]
    fn services_from_flags_absent_is_the_node_default() {
        let s = services_from_flags(None, None).unwrap();
        assert_eq!(s.declared(), 0b1);
        assert_eq!(s.resolve_lag(1 << 20), ServicesConfig::default().resolve_lag(1 << 20));
    }

    #[test]
    fn services_from_flags_two_ids_bounded_and_lockstep() {
        let s = services_from_flags(Some("0,1"), Some("65536")).unwrap();
        assert_eq!(s.declared(), 0b11);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Bounded(65536));
        let s = services_from_flags(Some("0, 1"), Some("lockstep")).unwrap();
        assert_eq!(s.declared(), 0b11);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Lockstep);
    }

    #[test]
    fn services_from_flags_refuses_by_name() {
        let e = services_from_flags(Some("1"), None).unwrap_err().to_string();
        assert!(e.contains("--services"), "{e}");
        let e = services_from_flags(Some("0,x"), None).unwrap_err().to_string();
        assert!(e.contains("--services"), "{e}");
        let e = services_from_flags(Some("0"), Some("bogus")).unwrap_err().to_string();
        assert!(e.contains("--fsm-lag"), "{e}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc_gateway --example m12_gate -- services_from_flags 2>&1 | tail -5`
Expected: compile error `cannot find function services_from_flags`.

- [ ] **Step 3: Add the flags, the helper, and thread them through**

In `NodeArgs` (after `admission_kib`):

```rust
    /// M14d: declared FSM ids, comma-separated (`0,1`). Absent = `{0}` with
    /// the node's default lag bound — every pre-M14 arm is byte-for-byte
    /// unchanged. Refused by name like `node.toml`'s `[services].ids`.
    #[arg(long)]
    services: Option<String>,
    /// M14d: `lockstep` or a byte bound (`65536`, `16MiB`) — the string form
    /// of `[services].fsm_lag`, parsed by the same function.
    #[arg(long)]
    fsm_lag: Option<String>,
    /// M14d row f: `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` (as
    /// `m6_gate`'s node role) so a late joiner is genuinely below the floor.
    #[arg(long, default_value_t = false)]
    purge_below_snapshot: bool,
    /// M14d row f: journal segment size; small (16 KiB, M7's value) so purge
    /// actually drops prefixes inside a 60 s arm.
    #[arg(long, default_value_t = uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES)]
    journal_segment_bytes: u64,
```

Add the helper next to `parse_id_addr_list`:

```rust
/// M14d: `--services` / `--fsm-lag` → the `ServicesConfig` a node boots
/// with. Absent flags are the node default; refusals name the flag, the way
/// `node.toml`'s loader names the field (`config_file.rs`'s `services.ids` /
/// `services.fsm_lag`).
fn services_from_flags(
    services: Option<&str>,
    fsm_lag: Option<&str>,
) -> anyhow::Result<ServicesConfig> {
    let lag = match fsm_lag {
        None => None,
        Some(raw) => Some(
            uc_node::services::parse_fsm_lag(raw.trim())
                .map_err(|detail| anyhow::anyhow!("--fsm-lag {raw:?}: {detail}"))?,
        ),
    };
    match services {
        None if lag.is_none() => Ok(ServicesConfig::default()),
        None => ServicesConfig::from_ids(&[0], lag)
            .map_err(|detail| anyhow::anyhow!("--services (default 0): {detail}")),
        Some(list) => {
            let ids = list
                .split(',')
                .map(|s| {
                    s.trim()
                        .parse::<u8>()
                        .map_err(|e| anyhow::anyhow!("--services {list:?}: {s:?} is not an id ({e})"))
                })
                .collect::<anyhow::Result<Vec<u8>>>()?;
            ServicesConfig::from_ids(&ids, lag)
                .map_err(|detail| anyhow::anyhow!("--services {list:?}: {detail}"))
        }
    }
}
```

Change `node_config`'s signature and body — three new trailing parameters replacing the three hard-coded fields:

```rust
fn node_config(
    id: u32,
    members: Vec<(u32, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: &str,
    buffer_bytes: usize,
    admission_bytes: u64,
    services: ServicesConfig,
    purge: uc_node::PurgePolicy,
    journal_segment_bytes: u64,
) -> NodeConfig {
    NodeConfig {
        // … every existing field unchanged except these three:
        purge,
        journal_segment_bytes,
        services,
        // …
    }
}
```

Update the in-process call site in `boot_cluster` to pass `ServicesConfig::default(), uc_node::PurgePolicy::Disabled, uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES`.

In `run_node_role`, build the config from the flags and extend the stats line:

```rust
    let services = services_from_flags(a.services.as_deref(), a.fsm_lag.as_deref())?;
    let purge = if a.purge_below_snapshot {
        uc_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 }
    } else {
        uc_node::PurgePolicy::Disabled
    };
    let cfg = node_config(
        id, members, a.bind, a.instance_dir, &a.app_id, FLEET_BUFFER_BYTES,
        a.admission_kib * 1024, services, purge, a.journal_segment_bytes,
    );
    let node = Node::start(cfg)?;
    println!(
        "m12_gate node {id} up (services={:#b}); parking (killed externally by the harness)",
        services.declared()
    );
    let mut last = (u64::MAX, (u64::MAX, u64::MAX));
    loop {
        let now = (node.reports_unattested(), node.snapshot_session_refusals());
        if now != last {
            println!(
                "m12_gate node {id} stats: reports_unattested={} snap_refusals=({},{})",
                now.0, now.1 .0, now.1 .1
            );
            last = now;
        }
        thread::sleep(Duration::from_millis(500));
    }
```

Add `use uc_node::ServicesConfig;` to the imports (it is referenced unqualified above and in the tests).

- [ ] **Step 4: Run the tests and clippy**

Run: `cargo test -p uc_gateway --example m12_gate -- services_from_flags 2>&1 | tail -6`
Expected: `test result: ok. 3 passed`.

Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings 2>&1 | tail -3`
Expected: no warnings.

- [ ] **Step 5: Smoke the in-process arm is unchanged**

Run: `cargo run -p uc_gateway --release --example m12_gate -- --arm direct --secs 3 2>&1 | grep -E 'RESULT|leader'`
Expected: a `leader elected` line and one `RESULT {"arm":"direct"...}` line (smoke, not a gate).

- [ ] **Step 6: Commit**

```bash
git add uc_gateway/examples/m12_gate.rs
git commit -m "bench(m12_gate): node role takes --services/--fsm-lag/--purge-below-snapshot/--journal-segment-bytes; stats line prints snapshot_session_refusals (M14d T1)"
```

---

### Task 2: `SpinCountSm`, snapshots for `CountSm`, per-id service attach

**Files:**
- Modify: `uc_gateway/examples/m12_gate.rs:353-380` (`CountSm`), `:170-187` (`ServiceArgs`), `:1236-1277` (`run_service_role`), imports `:71-76`
- Test: the `#[cfg(test)] mod tests` from Task 1

**Interfaces:**
- Consumes: `uc_service::{SnapshotStateMachine, SnapshotError, SnapshotPolicy}` (`uc_service/src/config.rs:64` — `SnapshotPolicy { interval_bytes: u64 }`), `ServiceConfig::service_id(u8) -> Self` (`config.rs:44`), `ServiceConfig::snapshot_policy(SnapshotPolicy) -> Self` (`config.rs:38`), `Sessioned<S: SnapshotStateMachine>: SnapshotStateMachine` (`session.rs:274`). The trait shape is `m6_gate.rs:202-240`'s `RegSm` impl: `type SnapshotHandle = Vec<u8>; fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError>; fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError>; fn install_snapshot(&mut self, position: u64, src: &mut dyn Read) -> Result<u64, SnapshotError>` returning `Ok(position)`.
- Produces: `struct SpinCountSm { inner: CountSm, spin: u64 }` implementing `StateMachine` (same associated types as `CountSm`) and `SnapshotStateMachine`; `CountSm: SnapshotStateMachine`. Service CLI flags: `--service-id <u8>` (default 0), `--work-spin <u64>` (default 0 = plain `CountSm`), `--snapshot-interval-bytes <u64>` (default 0 = no snapshots).

- [ ] **Step 1: Write the failing tests**

Add to `mod tests`:

```rust
    fn drive<S: StateMachine<Command = Vec<u8>, Response = u64, Query = (), QueryResponse = u64>>(
        sm: &mut S,
    ) -> Vec<u64> {
        (1..=200u64)
            .map(|i| sm.apply(i * 64, vec![(i & 0xff) as u8; 8]))
            .collect()
    }

    #[test]
    fn spin_count_sm_is_count_sm_with_a_price_not_a_different_answer() {
        let mut plain = CountSm::default();
        let mut spin = SpinCountSm::with_spin(5_000);
        assert_eq!(drive(&mut plain), drive(&mut spin));
        assert_eq!(plain.query(()), spin.query(()));
        assert_eq!(plain.last_applied(), spin.last_applied());
        // Two different K's, same answers: K prices the apply, it never
        // reaches the response (spec §15.3).
        let mut spin2 = SpinCountSm::with_spin(50);
        assert_eq!(drive(&mut spin2), drive(&mut SpinCountSm::with_spin(0)));
    }

    #[test]
    fn count_sm_snapshot_round_trips_and_pins_the_position() {
        let mut a = SpinCountSm::with_spin(10);
        drive(&mut a);
        let (blob, pos) = a.freeze().unwrap();
        assert_eq!(pos, 200 * 64);
        let mut b = SpinCountSm::with_spin(0);
        let got = b.install_snapshot(pos, &mut &blob[..]).unwrap();
        assert_eq!(got, pos);
        assert_eq!(b.query(()), 200);
        assert_eq!(b.last_applied(), Some(pos));
        let err = SpinCountSm::with_spin(0).install_snapshot(pos + 64, &mut &blob[..]);
        assert!(err.is_err(), "a mis-tagged artifact must be refused");
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_gateway --example m12_gate -- spin_count count_sm_snapshot 2>&1 | tail -5`
Expected: compile errors (`SpinCountSm` not found; `freeze` not found for `CountSm`).

- [ ] **Step 3: Implement**

Extend the `uc_service` import to `{RawStateMachine, SESSION_HEADER_LEN, Service, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned, SnapshotError, SnapshotPolicy, SnapshotStateMachine, StateMachine, TAG_FRESH}`.

Below `impl StateMachine for CountSm`:

```rust
/// M14d row f: the typed counter can be shipped as a snapshot — 16 bytes,
/// `count ++ last_applied`, position-pinned on install (the `RegSm` shape in
/// `m6_gate.rs`). `Sessioned<CountSm>` inherits it (session.rs:274).
impl SnapshotStateMachine for CountSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let pos = self.last_applied.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        Ok((buf, pos))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        if buf.len() < 16 {
            return Err(SnapshotError::Codec(format!("short snapshot: {} bytes", buf.len())));
        }
        let count = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let pos = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if pos != position {
            return Err(SnapshotError::Codec(format!(
                "snapshot payload position {pos} != requested {position}"
            )));
        }
        self.count = count;
        self.last_applied = Some(position);
        Ok(position)
    }
}

/// M14d: `CountSm` with a fixed price per apply. `spin` rounds of an integer
/// LCG, seeded from the position, consumed through `black_box` — so the loop
/// cannot be optimised away and its result never reaches the response.
/// `K` changes cost, not output (spec §15.3); the test above pins that.
#[derive(Default)]
struct SpinCountSm {
    inner: CountSm,
    spin: u64,
}

impl SpinCountSm {
    fn with_spin(spin: u64) -> Self {
        Self { inner: CountSm::default(), spin }
    }
}

impl StateMachine for SpinCountSm {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Vec<u8>) -> u64 {
        let mut x: u64 = position ^ 0x9E37_79B9_7F4A_7C15;
        for _ in 0..self.spin {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            x ^= x >> 29;
        }
        std::hint::black_box(x);
        self.inner.apply(position, cmd)
    }

    fn query(&self, q: ()) -> u64 {
        self.inner.query(q)
    }

    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied()
    }
}

impl SnapshotStateMachine for SpinCountSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        self.inner.freeze()
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        CountSm::stream_snapshot(handle, dst)
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        self.inner.install_snapshot(position, src)
    }
}
```

`ServiceArgs` gains:

```rust
    /// M14d: attach as this FSM id (`ServiceConfig::service_id`). The node
    /// must have declared it (`--services`), else the attach is refused by
    /// name (`ServiceNotDeclared`).
    #[arg(long, default_value_t = 0)]
    service_id: u8,
    /// M14d: `> 0` runs `SpinCountSm` with this many LCG rounds per apply —
    /// the deliberately slow FSM. `0` = plain `CountSm`. Incompatible with
    /// `--raw-sm`.
    #[arg(long, default_value_t = 0)]
    work_spin: u64,
    /// M14d row f: `SnapshotPolicy { interval_bytes }` on the service so the
    /// leader has artifacts to ship. `0` = no snapshots (every prior arm).
    #[arg(long, default_value_t = 0)]
    snapshot_interval_bytes: u64,
```

`run_service_role` becomes:

```rust
fn run_service_role(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat at {cnc:?}");
        thread::sleep(Duration::from_millis(20));
    }
    anyhow::ensure!(
        !(a.raw_sm && a.work_spin > 0),
        "--raw-sm and --work-spin are exclusive: the slow FSM is the typed tier (spec §15.3)"
    );
    let mut cfg = ServiceConfig::new(&a.instance_dir, &a.app_id).service_id(a.service_id);
    if a.snapshot_interval_bytes > 0 {
        cfg = cfg.snapshot_policy(SnapshotPolicy { interval_bytes: a.snapshot_interval_bytes });
    }
    let envelope = a.envelope == Envelope::On;
    let tag = format!("id={} spin={} snap={}", a.service_id, a.work_spin, a.snapshot_interval_bytes);
    // Each arm diverges (parks forever), so the `Service<_>` types never need
    // to unify — `m5_gate`'s service role does the same.
    match (envelope, a.raw_sm, a.work_spin > 0) {
        (true, false, false) => {
            let _svc = ServiceBuilder::new(cfg, Sessioned::new(CountSm::default(), SessionConfig::default())).start()?;
            park_service(&format!("Sessioned<CountSm> (typed tier, envelope on, {tag})"))
        }
        (true, false, true) => {
            let _svc = ServiceBuilder::new(cfg, Sessioned::new(SpinCountSm::with_spin(a.work_spin), SessionConfig::default())).start()?;
            park_service(&format!("Sessioned<SpinCountSm> (typed tier, envelope on, {tag})"))
        }
        (false, false, false) => {
            let _svc = ServiceBuilder::new(cfg, CountSm::default()).start()?;
            park_service(&format!("CountSm (typed tier, envelope off, {tag})"))
        }
        (false, false, true) => {
            let _svc = ServiceBuilder::new(cfg, SpinCountSm::with_spin(a.work_spin)).start()?;
            park_service(&format!("SpinCountSm (typed tier, envelope off, {tag})"))
        }
        (true, true, _) => {
            let _svc = ServiceBuilder::new(cfg, Sessioned::new(RawCountSm::default(), SessionConfig::default())).start()?;
            park_service(&format!("Sessioned<RawCountSm> (raw tier, envelope on, {tag})"))
        }
        (false, true, _) => {
            let _svc = ServiceBuilder::new(cfg, RawCountSm::default()).start()?;
            park_service(&format!("RawCountSm (raw tier, envelope off, {tag})"))
        }
    }
}
```

`park_service` already takes `&str`; the `format!` calls above pass `&String`, which coerces.

- [ ] **Step 4: Run the tests, clippy**

Run: `cargo test -p uc_gateway --example m12_gate 2>&1 | tail -6`
Expected: `test result: ok. 5 passed`.

Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings 2>&1 | tail -3`
Expected: clean. (If clippy flags `RawCountSm`'s snapshot-less path as unreachable, it is not — leave the match as written.)

- [ ] **Step 5: Commit**

```bash
git add uc_gateway/examples/m12_gate.rs
git commit -m "bench(m12_gate): SpinCountSm (the slow FSM, K prices apply and never reaches the response), CountSm snapshots, service --service-id/--work-spin/--snapshot-interval-bytes (M14d T2)"
```

---

### Task 3: Client — fan-in, windowed rate, timeline, `check-fsms`, in-process two-FSM smoke

**Files:**
- Modify: `uc_gateway/examples/m12_gate.rs:85-135` (`Cli`, `Arm`), `:136-150` (`Role`), `:210-228` (`ClientDirectArgs`), `:528-550` (`ClientStats`), `:588-609` (`run_direct_arm`), `:610-820` (`run_client_measurement`), `:1323-1397` (`print_result_json`, `run_client_direct_role`), `main`'s dispatch `:244-300`
- Test: `mod tests` + the in-process smoke arm

**Interfaces:**
- Consumes: `Engine` send half `try_submit_all(user_data, &[u8])` (`uc_client/src/engine.rs:508`), `declared() -> u64` (`:473`), `Outcome::Responses(&[(u8, Bytes)])` (`:310`), `uc_client::Client::connect(&Path, &str) -> Result<Client, ClientError>` (`client.rs:58`), `Client::declared() -> u64`, `Client::query_linearizable_on(id, &Q) -> Result<QR, _>` (`client.rs:157`), `Client::query_snapshot_on(id, &Q)` (`:148`).
- Produces: `run_client_measurement(instance_dir, app_id, secs, payload_len, inflight_cap, session_client_id, opts: &MeasureOpts) -> ClientStats` with `struct MeasureOpts { fan_in: bool, warmup_secs: u64, measure_secs: u64, timeline: bool }`; `ClientStats` gains `window_rps: f64, window_responses: u64, fan_in: bool`; the `RESULT` JSON gains `"window_rps"`, `"window_responses"`, `"fan_in"`, `"declared"`; per-second `TL {"sec":N,"unix_ms":M,"responses":R}` lines when `--timeline`; a new role `check-fsms --instance-dir D --app-id A --mode linearizable|snapshot [--expect N] [--expect-min N] [--settle-secs S]` printing `FSMS {"id":..,"count":..}` per id and one `FSMS-OK {"declared":mask,"count":N}` (exit 1 on divergence); a new in-process arm `--arm fsms`.

- [ ] **Step 1: Write the failing test for the pure window arithmetic**

The windowed rate is computed by a small pure function so it can be unit-tested without a cluster. Add to `mod tests`:

```rust
    #[test]
    fn window_rate_counts_only_completions_inside_the_window() {
        // completions at 0.5s, 1.5s, 2.5s, 3.5s, 9.5s, 10.5s, 11.5s with a
        // 2 s warm-up and an 8 s window → the 2.5, 3.5, 9.5 completions.
        let ns = |s: f64| (s * 1e9) as u64;
        let done = [ns(0.5), ns(1.5), ns(2.5), ns(3.5), ns(9.5), ns(10.5), ns(11.5)];
        let (n, rps) = window_rate(&done, 2, 8);
        assert_eq!(n, 3);
        assert!((rps - 3.0 / 8.0).abs() < 1e-9, "{rps}");
        assert_eq!(window_rate(&done, 0, 0), (0, 0.0));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_gateway --example m12_gate -- window_rate 2>&1 | tail -3`
Expected: `cannot find function window_rate`.

- [ ] **Step 3: Implement the measurement changes**

Add near `ClientStats`:

```rust
/// M14d: what the fleet driver varies per arm, beyond `secs/payload/inflight`.
#[derive(Clone, Copy, Debug, Default)]
struct MeasureOpts {
    /// `try_submit_all` (one response per declared FSM, counted as ONE
    /// completed op when every part arrived) instead of `try_submit`.
    fan_in: bool,
    /// Steady window: completions in `[warmup, warmup + measure)` seconds
    /// after t0. `measure == 0` disables the window (`window_rps` = 0).
    warmup_secs: u64,
    measure_secs: u64,
    /// Print one `TL {...}` line per elapsed second (row d's recovery clock).
    timeline: bool,
}

/// Completions inside `[warmup, warmup + measure)` and their rate. Pure, so
/// the arithmetic is testable; `done_ns` are completion times since t0.
fn window_rate(done_ns: &[u64], warmup_secs: u64, measure_secs: u64) -> (u64, f64) {
    if measure_secs == 0 {
        return (0, 0.0);
    }
    let lo = warmup_secs * 1_000_000_000;
    let hi = lo + measure_secs * 1_000_000_000;
    let n = done_ns.iter().filter(|&&t| t >= lo && t < hi).count() as u64;
    (n, n as f64 / measure_secs as f64)
}
```

In `ClientStats` add `window_responses: u64, window_rps: f64, fan_in: bool` and fill them.

In `run_client_measurement`: add the `opts: &MeasureOpts` parameter; record every completion's `now` into a `Mutex<Vec<u64>>` (`done_ns`) **only when** `opts.measure_secs > 0 || opts.timeline` (the Vec is the price of the window; unbounded arms skip it); count per-second buckets for the timeline in a `Vec<AtomicU64>` of length `secs + 40` indexed by `now / 1e9`; treat `Outcome::Responses(parts)` as one completed op when `opts.fan_in` and every part's body starts with `TAG_FRESH` (session on) — otherwise it is `not_fresh`; keep `Outcome::Responses` under `lost` when `!opts.fan_in` (a bench that did not ask for a fan-in must not receive one). Submit with:

```rust
        let r = if opts.fan_in {
            send.try_submit_all(sent_idx, submit_bytes)
        } else {
            send.try_submit(sent_idx, submit_bytes)
        };
        match r { /* unchanged arms */ }
```

After the drain, when `opts.timeline`, print one line per bucket: `println!("TL {{\"sec\":{i},\"unix_ms\":{ms},\"responses\":{n}}}")` where `ms` is `t0_unix_ms + i * 1000` and `t0_unix_ms` is captured with `SystemTime::now()` next to `t0`. Compute `(window_responses, window_rps) = window_rate(&done_ns, opts.warmup_secs, opts.measure_secs)`.

`print_result_json` adds `"window_rps":{:.1},"window_responses":{},"fan_in":{},"declared":{}` (declared from `send.declared()`, threaded out via `ClientStats.declared: u64`).

`ClientDirectArgs` gains:

```rust
    /// M14d: submit to every declared FSM and count a completion only when
    /// every FSM answered (spec §15.3).
    #[arg(long, default_value_t = false)]
    fan_in: bool,
    /// M14d: steady-window start (seconds after t0). 0 = whole run.
    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,
    /// M14d: steady-window length. 0 = no window (`window_rps` reads 0).
    #[arg(long, default_value_t = 0)]
    measure_secs: u64,
    /// M14d row d: print `TL` per-second completion buckets.
    #[arg(long, default_value_t = false)]
    timeline: bool,
```

and `run_client_direct_role` passes `&MeasureOpts { fan_in: a.fan_in, warmup_secs: a.warmup_secs, measure_secs: a.measure_secs, timeline: a.timeline }`. `run_direct_arm` (in-process) passes `&MeasureOpts::default()`. The `not_fresh == 0` ensure stays.

- [ ] **Step 4: Add the `check-fsms` role**

`Role` gains `CheckFsms(CheckFsmsArgs)`:

```rust
#[derive(clap::Args)]
struct CheckFsmsArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "m12-gate")]
    app_id: String,
    /// `linearizable` goes through the leader's quorum barrier (run on the
    /// leader host); `snapshot` reads each FSM's local state (any host).
    #[arg(long, value_enum, default_value_t = CheckMode::Linearizable)]
    mode: CheckMode,
    /// Every FSM's count must equal this exactly (rows a/b/e: the client's
    /// completed ops on this cluster generation).
    #[arg(long)]
    expect: Option<u64>,
    /// Every FSM's count must be at least this (rows d/f: ops the client saw
    /// complete; commands still in flight at a kill may add to it).
    #[arg(long)]
    expect_min: Option<u64>,
    /// Keep re-reading until the counts agree, up to this long — followers
    /// apply asynchronously and a check right after load may catch one
    /// mid-frame.
    #[arg(long, default_value_t = 10)]
    settle_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum CheckMode {
    Linearizable,
    Snapshot,
}

/// M14d row c: every declared FSM answers the same count, equal to (or at
/// least) what the client completed. Any mismatch is exit 1 — the row is a
/// consensus/apply defect, not a rate.
fn run_check_fsms_role(a: CheckFsmsArgs) -> anyhow::Result<()> {
    let client = uc_client::Client::connect(&a.instance_dir, &a.app_id)?;
    let declared = client.declared();
    let ids: Vec<u8> = (0..8u8).filter(|i| declared & (1u64 << i) != 0).collect();
    anyhow::ensure!(!ids.is_empty(), "no FSM declared on {:?}", a.instance_dir);
    let deadline = Instant::now() + Duration::from_secs(a.settle_secs);
    let mut last: Vec<(u8, u64)> = Vec::new();
    loop {
        last.clear();
        for &id in &ids {
            let c: u64 = match a.mode {
                CheckMode::Linearizable => client.query_linearizable_on(id, &())?,
                CheckMode::Snapshot => client.query_snapshot_on(id, &())?,
            };
            last.push((id, c));
        }
        let agree = last.windows(2).all(|w| w[0].1 == w[1].1);
        let n = last[0].1;
        let vs_expect = match (a.expect, a.expect_min) {
            (Some(e), _) => n == e,
            (None, Some(m)) => n >= m,
            (None, None) => true,
        };
        if agree && vs_expect {
            break;
        }
        if Instant::now() >= deadline {
            for (id, c) in &last {
                println!("FSMS {{\"id\":{id},\"count\":{c}}}");
            }
            anyhow::bail!(
                "divergence after {}s: counts {last:?}, expect {:?}, expect_min {:?}",
                a.settle_secs, a.expect, a.expect_min
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
    for (id, c) in &last {
        println!("FSMS {{\"id\":{id},\"count\":{c}}}");
    }
    println!("FSMS-OK {{\"declared\":{declared},\"count\":{},\"mode\":\"{:?}\"}}", last[0].1, a.mode);
    Ok(())
}
```

Wire `Role::CheckFsms(a) => run_check_fsms_role(a)` into `main`'s dispatch.

- [ ] **Step 5: Add the in-process two-FSM smoke arm**

`Arm` gains `Fsms` ("two FSMs — `CountSm` + `SpinCountSm(2000)` — fan-in load, then the divergence check; smoke only"). Add:

```rust
/// M14d: `boot_cluster` for two declared FSMs per node — id 0 `CountSm`,
/// id 1 `SpinCountSm(spin)`. Bounded lag at the node default.
fn boot_cluster2(
    root: &std::path::Path,
    app_id: &str,
    n: usize,
    spin: u64,
) -> (Vec<Node>, Vec<Service<CountSm>>, Vec<Service<SpinCountSm>>, Vec<PathBuf>) {
    let socks: Vec<UdpSocket> = (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(u32, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as u32, s.local_addr().unwrap())).collect();
    let services = ServicesConfig::from_ids(&[0, 1], None).expect("ids 0,1");
    let (mut nodes, mut s0, mut s1, mut dirs) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        std::fs::create_dir_all(&instance_dir).expect("instance dir");
        let cfg = node_config(
            i as u32, members.clone(), addr, instance_dir.clone(), app_id,
            NODE_BUFFER_BYTES, DEFAULT_ADMISSION_BYTES, services,
            uc_node::PurgePolicy::Disabled, uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let a = ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id).service_id(0), CountSm::default())
            .start().expect("service 0");
        let b = ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id).service_id(1), SpinCountSm::with_spin(spin))
            .start().expect("service 1");
        nodes.push(node); s0.push(a); s1.push(b); dirs.push(instance_dir);
    }
    (nodes, s0, s1, dirs)
}

fn run_fsms_arm(root: &std::path::Path, secs: u64, payload: usize, inflight: u64) -> anyhow::Result<()> {
    const APP_ID: &str = "uc2-m12-gate-fsms";
    let (nodes, s0, s1, dirs) = boot_cluster2(root, APP_ID, 3, 2_000);
    let leader = await_single_leader(&nodes, 30);
    println!("[fsms] leader elected: n{leader}");
    let opts = MeasureOpts { fan_in: true, warmup_secs: 1, measure_secs: secs.saturating_sub(2), timeline: false };
    let stats = run_client_measurement(&dirs[leader], APP_ID, secs, payload, inflight, None, &opts);
    print_report("fsms (fan-in, 2 FSMs)", &stats);
    print_result_json("fsms", &stats, secs, payload, inflight);
    anyhow::ensure!(stats.lost == 0, "{} lost", stats.lost);
    run_check_fsms_role(CheckFsmsArgs {
        instance_dir: dirs[leader].clone(),
        app_id: APP_ID.into(),
        mode: CheckMode::Linearizable,
        expect: Some(stats.responses),
        expect_min: None,
        settle_secs: 10,
    })?;
    for n in nodes { n.stop(); }
    for s in s0 { s.stop(); }
    for s in s1 { s.stop(); }
    Ok(())
}
```

In `main`, before the existing direct/gateway arms: `if cli.arm == Arm::Fsms { return run_fsms_arm(&root, cli.secs, cli.payload, cli.inflight); }` (after `root` is resolved the way the existing arms resolve it).

- [ ] **Step 6: Run the tests, the smoke, clippy**

Run: `cargo test -p uc_gateway --example m12_gate 2>&1 | tail -4`
Expected: `test result: ok. 6 passed`.

Run: `cargo run -p uc_gateway --release --example m12_gate -- --arm fsms --secs 4 2>&1 | grep -E 'leader|RESULT|FSMS'`
Expected: one `RESULT {"arm":"fsms",...,"fan_in":true,"declared":3...}` line, two `FSMS {"id":..}` lines with equal counts, and `FSMS-OK`. Smoke, not a gate.

Run: `cargo run -p uc_gateway --release --example m12_gate -- --arm direct --secs 3 2>&1 | grep RESULT`
Expected: the pre-existing arm still prints its RESULT (with `"fan_in":false,"window_rps":0.0`).

Run: `cargo clippy -p uc_gateway --all-targets -- -D warnings 2>&1 | tail -3`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add uc_gateway/examples/m12_gate.rs
git commit -m "bench(m12_gate): fan-in submit, steady-window rate, per-second timeline, check-fsms divergence role, in-process two-FSM smoke arm (M14d T3)"
```

---

### Task 4a: `m14_fleet_gate.py` — bars, verdicts, selftest

**Files:**
- Create: `bench-infra/scripts/m14_fleet_gate.py`

**Interfaces:**
- Consumes: from `m12_fleet_gate`: `ssh, start_unit, kill_unit, truncate_log, tail_log, run_foreground, parse_result, echo, Verdict, prepare_host, members_str, wipe_dirs, APP, PORT, REMOTE_ROOT, UNIT_PREFIX, BUILT_GATE, BUILT_PROBE, BOOT_SETTLE_SECS, CLIENT_SLACK_SECS, LEADER_WAIT_SECS`; from `m6_fleet_gate`: `build_fleet_hosts(gate_bin, ssh_user, ssh_key, hosts_arg, count, ctl_bin, unit_prefix, remote_root, probe_bin)`, `wait_leader(hosts, voter_idxs, secs)`, `SshHost.ctl(op, node_id, addr)`, `SshHost.dir`, `.private_ip`, `.public_ip`; from `m13_hop_bench`: `sync_tree(hosts, local_tree)`.
- Produces (pure, used by 4b–4d and the selftest): `verdict_row_a(rates)`, `verdict_row_b(rates)`, `verdict_row_c(checks)`, `verdict_row_d(kill)`, `verdict_row_e(rates)`, `verdict_row_f(join)` each returning `m12.Verdict(row, passed, detail)` and printing one `GATE-JSON {...}` line; `recovery_time(timeline, t0_ms, baseline_lo_ms, baseline_hi_ms) -> (baseline_rps, recovered_at_s | None, windows)` implementing M9's rule; `pick_k(calib) -> (K, rate)`.

- [ ] **Step 1: Write the file with the bars, the pure functions and the selftest**

```python
#!/usr/bin/env python3
"""UC v2 M14 fleet-gate driver — spec §15 rows a–g.

Topology (4 hosts): hosts[0..3] voters, hosts[3] the learner (idle until row
f). The direct Engine client is shmem-attached and runs ON THE LEADER HOST.

Arms (each a fresh cluster generation unless noted):
  calib   FSM 0 alone, SpinCountSm at a K ladder → pick K (spec §15.3)
  n1      {0} CountSm                                → rate(n1)
  n2eq    {0,1} CountSm + CountSm, bounded           → rate(n2eq)      row a
  slow1   {0} SpinCountSm(K)                         → rate(slow1)
  pair    {0,1} CountSm + SpinCountSm(K), bounded    → rate(pair)      row b
  n2eq-ls / pair-ls  the same two pairs in lockstep  → reported        row e
  kill    pair under load; SIGKILL FSM 1 on the leader host; restart   row d
  join    pair + purge + snapshots; add-learner on hosts[3] under load row f
  row c   check-fsms after EVERY arm above (leader: linearizable; every
          host: snapshot) — any mismatch FAILs the gate.

Every row verdict is a PURE function of recorded numbers, so `--selftest`
replays canned inputs through them with no fleet. Bars are the constants
below; they are printed beside each verdict as a GATE-JSON line. The exit
code is the verdict: a green terminal is not a PASS.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m6_fleet_gate as m6  # noqa: E402
import m12_fleet_gate as m12  # noqa: E402
from m12_fleet_gate import (  # noqa: E402
    ssh, start_unit, kill_unit, truncate_log, tail_log, run_foreground,
    parse_result, echo, Verdict, APP, PORT, REMOTE_ROOT, UNIT_PREFIX,
    BUILT_GATE, BUILT_PROBE, BOOT_SETTLE_SECS, CLIENT_SLACK_SECS,
    LEADER_WAIT_SECS,
)
from m13_hop_bench import sync_tree  # noqa: E402

BUILT_CTL = "/opt/bench/uc/target/release/uc2ctl"

# ------------------------------------------------------------------ bars
# Spec §15.4, verbatim. Committed before any run; never edited to fit one.
BAR_A_RATIO = 0.90          # rate(n2eq) / rate(n1)
BAR_B_LO, BAR_B_HI = 0.90, 1.10   # rate(pair) / rate(slow1)
BAR_D_SECS = 15.0           # M9's bar: recovered AND attached+lag≤bound by then
BAR_D_FRACTION = 0.80       # M9's rule: a 2 s window at ≥ 80 % of baseline …
BAR_D_WINDOW_SECS = 2       # … confirmed by the next such window
BAR_F_JOIN_SECS = 60.0      # M6's JOIN_BUDGET
CALIB_TARGET = 0.5          # slow-solo ≈ 0.5 × rate(n1)

# ------------------------------------------------------------- arm shape
ARM_SECS = 12               # 2 s warm-up + 8 s window + 2 s tail (spec §15.3)
WARMUP_SECS, MEASURE_SECS = 2, 8
KILL_ARM_SECS = 45          # row d: baseline [2,10) s, kill at ~12 s, 30 s to recover
JOIN_ARM_SECS = 90          # row f: load for the whole join
JOIN_AT_SECS = 10           # row f: add-learner this long after load starts
STATUS_RE = re.compile(
    r"id=(\d+) attached=(true|false) epoch=(\d+) incarnation=(\d+) "
    r"applied=(\d+) lag=(\d+) snapshot_pos=(\d+)")
TL_RE = re.compile(r'^TL\s+(\{.*\})\s*$', re.M)
FSMS_OK_RE = re.compile(r'^FSMS-OK\s+(\{.*\})\s*$', re.M)
STATS_RE = re.compile(r"reports_unattested=(\d+) snap_refusals=\((\d+),(\d+)\)")

M14_SEGMENT_BYTES = 16 * 1024          # M7's value: purge inside one arm
M14_SNAPSHOT_INTERVAL_BYTES = 32 * 1024


def gate_json(row, passed, **fields):
    print("GATE-JSON " + json.dumps({"row": row, "pass": passed, **fields}), flush=True)


# ------------------------------------------------------ pure verdicts
def pick_k(calib):
    """`calib` = [(K, rate)], the ladder. Return the (K, rate) whose rate is
    nearest CALIB_TARGET × n1_rate — the caller passes the ladder already
    scaled (rate / n1_rate) as `calib[i] = (K, ratio)`."""
    if not calib:
        raise ValueError("empty calibration ladder")
    return min(calib, key=lambda kr: abs(kr[1] - CALIB_TARGET))


def verdict_row_a(rates):
    n1, n2 = rates.get("n1"), rates.get("n2eq")
    ok = bool(n1 and n2) and (n2 / n1) >= BAR_A_RATIO
    ratio = (n2 / n1) if n1 and n2 else None
    gate_json("a", ok, n1=n1, n2eq=n2, ratio=ratio, bar=BAR_A_RATIO)
    return Verdict("a equal-speed pair vs N=1", ok,
                   f"n2eq/n1 = {ratio:.3f} (bar ≥ {BAR_A_RATIO})" if ratio else "missing rate")


def verdict_row_b(rates):
    s, p = rates.get("slow1"), rates.get("pair")
    ratio = (p / s) if s and p else None
    ok = ratio is not None and BAR_B_LO <= ratio <= BAR_B_HI
    gate_json("b", ok, slow1=s, pair=p, ratio=ratio, bar=[BAR_B_LO, BAR_B_HI])
    return Verdict("b bounded pair converges to the slow FSM", ok,
                   f"pair/slow1 = {ratio:.3f} (bar [{BAR_B_LO}, {BAR_B_HI}])" if ratio else "missing rate")


def verdict_row_c(checks):
    """`checks` = [(arm, host, mode, ok, count)] — one per check-fsms run.
    Every one must be ok AND, per arm, every host's count must agree."""
    bad = [c for c in checks if not c[3]]
    by_arm = {}
    for arm, host, mode, ok, count in checks:
        by_arm.setdefault(arm, set()).add(count)
    disagree = {arm: sorted(cs) for arm, cs in by_arm.items() if len(cs) > 1}
    ok = not bad and not disagree and bool(checks)
    gate_json("c", ok, checks=len(checks), failed=[c[:3] for c in bad], cross_host=disagree)
    detail = f"{len(checks)} checks; " + ("all agree" if ok else f"failed={bad} cross-host={disagree}")
    return Verdict("c zero divergence", ok, detail)


def recovery_time(timeline, t0_ms, base_lo_ms, base_hi_ms):
    """M9's rule over 1 s buckets `[(unix_ms, responses)]`: baseline = mean
    rate over [base_lo, base_hi); recovered = the first 2 s window at ≥ 80 %
    of baseline whose END is after t0, confirmed by the NEXT 2 s window.
    Returns (baseline_rps, recovered_at_secs_after_t0 | None, windows)."""
    base = [r for ms, r in timeline if base_lo_ms <= ms < base_hi_ms]
    baseline = (sum(base) / len(base)) if base else 0.0
    after = [(ms, r) for ms, r in timeline if ms + 1000 > t0_ms]
    windows = []
    for i in range(0, len(after) - BAR_D_WINDOW_SECS + 1):
        w = after[i:i + BAR_D_WINDOW_SECS]
        end_ms = w[-1][0] + 1000
        rate = sum(r for _, r in w) / BAR_D_WINDOW_SECS
        windows.append((end_ms, rate))
    recovered = None
    for i in range(len(windows) - BAR_D_WINDOW_SECS):
        end_ms, rate = windows[i]
        nxt = windows[i + BAR_D_WINDOW_SECS][1]
        if baseline > 0 and rate >= BAR_D_FRACTION * baseline and nxt >= BAR_D_FRACTION * baseline:
            recovered = (end_ms - t0_ms) / 1000.0
            break
    return baseline, recovered, windows


def verdict_row_d(kill):
    """`kill` = {"baseline": rps, "recovered_at": s|None, "attached_at": s|None}."""
    r, a = kill.get("recovered_at"), kill.get("attached_at")
    ok = r is not None and a is not None and r <= BAR_D_SECS and a <= BAR_D_SECS
    gate_json("d", ok, **kill, bar=BAR_D_SECS)
    return Verdict("d FSM kill on the leader host recovers", ok,
                   f"rate back at {r}s, attached+lag≤bound at {a}s (bar ≤ {BAR_D_SECS}s), "
                   f"baseline {kill.get('baseline', 0):.0f}/s")


def verdict_row_e(rates):
    pairs = [("n2eq-ls", "n2eq"), ("pair-ls", "pair")]
    out = {}
    for ls, base in pairs:
        if rates.get(ls) and rates.get(base):
            out[ls] = rates[ls] / rates[base]
    gate_json("e", True, ratios=out, bar=None)
    return Verdict("e lockstep cost (reported, no bar)", True,
                   ", ".join(f"{k} = {v:.3f}× bounded" for k, v in out.items()) or "no lockstep rates")


def verdict_row_f(join):
    """`join` = {"joined_at": s|None, "refusals": {host: (legacy, mismatch)},
    "artifacts": {0: n, 1: n}, "check_ok": bool}."""
    j = join.get("joined_at")
    refusals_zero = all(tuple(v) == (0, 0) for v in join.get("refusals", {}).values()) and bool(join.get("refusals"))
    both = all(join.get("artifacts", {}).get(i, 0) > 0 for i in (0, 1))
    ok = j is not None and j <= BAR_F_JOIN_SECS and refusals_zero and both and join.get("check_ok", False)
    gate_json("f", ok, **{k: (v if k != "refusals" else {h: list(t) for h, t in v.items()}) for k, v in join.items()},
              bar=BAR_F_JOIN_SECS)
    return Verdict("f two-FSM learner join over wire 0.6.0", ok,
                   f"joined at {j}s (bar ≤ {BAR_F_JOIN_SECS}s), refusals zero={refusals_zero}, "
                   f"both artifacts={both}, divergence check={join.get('check_ok')}")


# ---------------------------------------------------------------- selftest
def selftest():
    fails = 0

    def expect(name, cond):
        nonlocal fails
        print(f"  [{'ok' if cond else 'FAIL'}] {name}")
        fails += 0 if cond else 1

    expect("pick_k nearest 0.5", pick_k([(500, 0.9), (2000, 0.52), (8000, 0.2)])[0] == 2000)
    expect("row a pass at 0.95", verdict_row_a({"n1": 1000.0, "n2eq": 950.0}).passed)
    expect("row a fail at 0.85", not verdict_row_a({"n1": 1000.0, "n2eq": 850.0}).passed)
    expect("row a fail on missing", not verdict_row_a({"n1": 1000.0}).passed)
    expect("row b pass at 1.05", verdict_row_b({"slow1": 500.0, "pair": 525.0}).passed)
    expect("row b fail at 0.85", not verdict_row_b({"slow1": 500.0, "pair": 425.0}).passed)
    expect("row b fail at 1.2 (outran the bound)", not verdict_row_b({"slow1": 500.0, "pair": 600.0}).passed)
    expect("row c pass", verdict_row_c([("n1", "h0", "lin", True, 10), ("n1", "h1", "snap", True, 10)]).passed)
    expect("row c fail on one bad check", not verdict_row_c([("n1", "h0", "lin", False, 10)]).passed)
    expect("row c fail on cross-host disagreement",
           not verdict_row_c([("n1", "h0", "lin", True, 10), ("n1", "h1", "snap", True, 9)]).passed)
    expect("row c fail on no checks", not verdict_row_c([]).passed)
    # recovery: 1 s buckets, baseline 1000/s over [2000,10000) ms, kill at
    # 12000 ms, zero until 20000, back to 900 from 20000 on → recovered when the
    # window ending at 22000 (rates 900,900) is confirmed by [22000,24000).
    tl = [(ms, 1000) for ms in range(0, 12000, 1000)] + \
         [(ms, 0) for ms in range(12000, 20000, 1000)] + \
         [(ms, 900) for ms in range(20000, 30000, 1000)]
    base, rec, _ = recovery_time(tl, 12000, 2000, 10000)
    expect("recovery baseline 1000", abs(base - 1000) < 1e-9)
    expect("recovery at 10 s", rec == 10.0)
    _, rec2, _ = recovery_time([(ms, 1000) for ms in range(0, 12000, 1000)] +
                               [(ms, 0) for ms in range(12000, 40000, 1000)], 12000, 2000, 10000)
    expect("no recovery → None", rec2 is None)
    lucky = [(ms, 1000) for ms in range(0, 12000, 1000)] + [(ms, 0) for ms in range(12000, 20000, 1000)] + \
            [(20000, 900), (21000, 900), (22000, 0), (23000, 0)] + [(ms, 900) for ms in range(24000, 30000, 1000)]
    _, rec3, _ = recovery_time(lucky, 12000, 2000, 10000)
    expect("one lucky window is not recovery", rec3 == 14.0)
    expect("row d pass", verdict_row_d({"baseline": 1000, "recovered_at": 9.5, "attached_at": 3.0}).passed)
    expect("row d fail late attach", not verdict_row_d({"baseline": 1000, "recovered_at": 9.5, "attached_at": 16.0}).passed)
    expect("row d fail never", not verdict_row_d({"baseline": 1000, "recovered_at": None, "attached_at": 3.0}).passed)
    expect("row e always passes", verdict_row_e({"n2eq": 100.0, "n2eq-ls": 40.0}).passed)
    good = {"joined_at": 30.0, "refusals": {"h0": (0, 0), "h1": (0, 0), "h2": (0, 0), "h3": (0, 0)},
            "artifacts": {0: 1, 1: 1}, "check_ok": True}
    expect("row f pass", verdict_row_f(good).passed)
    expect("row f fail on a refusal", not verdict_row_f({**good, "refusals": {**good["refusals"], "h3": (1, 0)}}).passed)
    expect("row f fail on one artifact", not verdict_row_f({**good, "artifacts": {0: 1, 1: 0}}).passed)
    expect("row f fail late", not verdict_row_f({**good, "joined_at": 61.0}).passed)
    expect("row f fail divergence", not verdict_row_f({**good, "check_ok": False}).passed)
    print(f"selftest: {'PASS' if fails == 0 else f'FAIL ({fails})'}")
    return 0 if fails == 0 else 1


def main():
    ap = argparse.ArgumentParser(description="UC v2 M14 fleet-gate driver (spec §15 rows a–g)")
    ap.add_argument("--selftest", action="store_true", help="replay canned rows through the verdicts; no fleet")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    ap.error("--fleet is added in the next task; only --selftest exists yet")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the selftest**

Run: `python3 bench-infra/scripts/m14_fleet_gate.py --selftest`
Expected: every line `[ok]`, last line `selftest: PASS`, exit 0. (`echo $?` → `0`.)

- [ ] **Step 3: Break a bar to prove the selftest bites, then restore**

Temporarily set `BAR_A_RATIO = 0.99`, run the selftest → `row a pass at 0.95` reads `[FAIL]` and the exit is 1. Restore `0.90`. Re-run → PASS.

- [ ] **Step 4: Commit**

```bash
git add bench-infra/scripts/m14_fleet_gate.py
git commit -m "bench(m14): m14_fleet_gate.py — spec §15 bars as constants, pure row verdicts (M9 recovery rule, calibration pick), --selftest (M14d T4a)"
```

---

### Task 4b: `m14_fleet_gate.py` — fleet setup, cluster arms (rows a, b, e) and row c checks

**Files:**
- Modify: `bench-infra/scripts/m14_fleet_gate.py`

**Interfaces:**
- Produces: `setup_fleet(a) -> (hosts, voters, learner)`, `start_cluster_m14(voters, a, fsms, lag, purge=False, snap=0)`, `stop_cluster_m14(hosts)`, `run_rate_arm(voters, a, label, fan_in) -> (rate, responses)`, `check_all(voters, leader, arm, expect=None, expect_min=None, checks)`, `arm_rates(voters, a, rates, checks) -> K`.

- [ ] **Step 1: Add the fleet helpers**

```python
# ------------------------------------------------------------------ fleet
def prepare_host_m14(host):
    """m12's build plus the uc2ctl binary (rows d/f drive real admin ops)."""
    m12.prepare_host(host, apply_profile=False)
    env = "sudo env CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup"
    cmd = (f"{env} {m6.SshHost.CARGO} build --release --manifest-path {m6.SshHost.UC_SRC}/Cargo.toml "
           f"-p uc_ctl && test -x {BUILT_CTL} && echo CTL-OK")
    r = ssh(host, cmd, label="build-ctl")
    if "CTL-OK" not in (r.stdout or ""):
        raise RuntimeError(f"uc2ctl build on {host.public_ip}: {r.stderr or r.stdout}")


def setup_fleet(a):
    hosts = m6.build_fleet_hosts(BUILT_GATE, a.ssh_user, a.ssh_key, a.hosts, count=4,
                                 ctl_bin=BUILT_CTL, unit_prefix=UNIT_PREFIX,
                                 remote_root=REMOTE_ROOT, probe_bin=BUILT_PROBE)
    if not a.no_sync:
        sync_tree(hosts, a.local_tree)
    for h in hosts:
        prepare_host_m14(h)
        stop_cluster_m14([h])
    voters, learner = hosts[:3], hosts[3]
    print(f"INFO topology: voters {[h.public_ip for h in voters]}, learner {learner.public_ip}; "
          f"the direct client runs on the leader host", flush=True)
    return hosts, voters, learner


SERVICE_UNITS = ("service0", "service1")


def stop_cluster_m14(hosts):
    for h in hosts:
        for u in ("client",) + SERVICE_UNITS + ("node",):
            kill_unit(h, u)


def node_args(h, node_id, members, fsms, lag, purge, snap):
    args = ["node", "--id", str(node_id), "--bind", f"{h.private_ip}:{PORT}",
            "--instance-dir", h.dir, "--members", members, "--app-id", APP,
            "--admission-kib", str(ADMISSION_KIB),
            "--services", ",".join(str(i) for i, _ in fsms)]
    if lag is not None:
        args += ["--fsm-lag", lag]
    if purge:
        args += ["--purge-below-snapshot", "--journal-segment-bytes", str(M14_SEGMENT_BYTES)]
    return args


def service_args(h, sid, spin, snap):
    args = ["service", "--instance-dir", h.dir, "--app-id", APP, "--envelope", "on",
            "--service-id", str(sid), "--work-spin", str(spin)]
    if snap:
        args += ["--snapshot-interval-bytes", str(snap)]
    return args


ADMISSION_KIB = 256


def start_cluster_m14(voters, fsms, lag=None, purge=False, snap=0):
    """`fsms` = [(id, spin)], e.g. [(0, 0)] or [(0, 0), (1, K)]. A FRESH
    generation: dirs wiped, nodes then services, settle after each."""
    m12.wipe_dirs(voters)
    ms = m12.members_str(voters)
    for i, h in enumerate(voters):
        start_unit(h, "node", node_args(h, i, ms, fsms, lag, purge, snap), nofile=True)
    time.sleep(BOOT_SETTLE_SECS)
    for h in voters:
        for sid, spin in fsms:
            truncate_log(h, f"service{sid}")
            start_unit(h, f"service{sid}", service_args(h, sid, spin, snap))
    time.sleep(BOOT_SETTLE_SECS)
    leader = m6.wait_leader(voters, list(range(len(voters))), LEADER_WAIT_SECS)
    if leader is None:
        raise RuntimeError("no single serving leader")
    return leader


def run_rate_arm(voters, leader, a, label, fan_in, secs=ARM_SECS, timeline=False, unit=False):
    """The direct client on the leader host. Foreground (returns the RESULT
    dict) unless `unit`, in which case it is started as a transient unit and
    the caller reads the log later (row d/f keep it running across an action)."""
    h = voters[leader]
    args = ["client-direct", "--instance-dir", h.dir, "--app-id", APP,
            "--secs", str(secs), "--payload", str(a.payload), "--inflight", str(a.inflight),
            "--envelope", "on", "--warmup-secs", str(WARMUP_SECS), "--measure-secs", str(MEASURE_SECS)]
    if fan_in:
        args.append("--fan-in")
    if timeline:
        args.append("--timeline")
    if unit:
        truncate_log(h, "client")
        start_unit(h, "client", args)
        return None
    rc, out = run_foreground(h, args, timeout=secs + CLIENT_SLACK_SECS)
    echo(label, out)
    d = parse_result(out, "direct")
    if d is None:
        raise RuntimeError(f"{label}: no RESULT line (rc={rc})")
    return d


def check_fsms(h, mode, expect=None, expect_min=None):
    args = ["check-fsms", "--instance-dir", h.dir, "--app-id", APP, "--mode", mode]
    if expect is not None:
        args += ["--expect", str(expect)]
    if expect_min is not None:
        args += ["--expect-min", str(expect_min)]
    rc, out = run_foreground(h, args, timeout=60)
    echo(f"check-fsms {h.public_ip} {mode}", out, lines=6)
    m = FSMS_OK_RE.search(out)
    count = json.loads(m.group(1))["count"] if m else None
    return rc == 0 and m is not None, count


def check_all(hosts, leader, arm, checks, expect=None, expect_min=None):
    """Row c after an arm: linearizable on the leader, snapshot on every host.
    Appends (arm, host, mode, ok, count) tuples; never raises — the verdict
    function judges."""
    ok, c = check_fsms(hosts[leader], "linearizable", expect, expect_min)
    checks.append((arm, hosts[leader].public_ip, "linearizable", ok, c))
    for h in hosts:
        ok, c = check_fsms(h, "snapshot", expect, expect_min)
        checks.append((arm, h.public_ip, "snapshot", ok, c))
```

- [ ] **Step 2: Add the rate arms (calib, n1, n2eq, slow1, pair, the two lockstep twins)**

```python
def rate_of(d):
    return float(d["window_rps"])


def one_arm(voters, a, label, fsms, lag, rates, checks, fan_in):
    leader = start_cluster_m14(voters, fsms, lag=lag)
    print(f"INFO arm {label}: leader n{leader} on {voters[leader].public_ip}", flush=True)
    d = run_rate_arm(voters, leader, a, label, fan_in)
    rates[label] = rate_of(d)
    print(f"INFO arm {label}: window_rps={rates[label]:.0f} responses={d['responses']} lost={d['lost']}", flush=True)
    check_all(voters, leader, label, checks, expect=int(d["responses"]))
    stop_cluster_m14(voters)
    return d


def arm_calib(voters, a, rates, checks):
    """FSM 0 alone as SpinCountSm over a K ladder; pick the K nearest 0.5 × n1."""
    ladder = []
    for k in [int(x) for x in a.calib_ks.split(",")]:
        d = one_arm(voters, a, f"calib-{k}", [(0, k)], None, rates, checks, fan_in=False)
        ladder.append((k, rate_of(d) / rates["n1"]))
        print(f"INFO calib K={k}: {ladder[-1][1]:.3f} × n1", flush=True)
    k, ratio = pick_k(ladder)
    gate_json("calib", True, ladder=ladder, K=k, ratio=ratio)
    return k


def arm_rates(voters, a, rates, checks):
    one_arm(voters, a, "n1", [(0, 0)], None, rates, checks, fan_in=False)
    K = a.k if a.k else arm_calib(voters, a, rates, checks)
    print(f"INFO slow FSM K = {K}", flush=True)
    one_arm(voters, a, "n2eq", [(0, 0), (1, 0)], None, rates, checks, fan_in=True)
    one_arm(voters, a, "slow1", [(0, K)], None, rates, checks, fan_in=False)
    one_arm(voters, a, "pair", [(0, 0), (1, K)], None, rates, checks, fan_in=True)
    one_arm(voters, a, "n2eq-ls", [(0, 0), (1, 0)], "lockstep", rates, checks, fan_in=True)
    one_arm(voters, a, "pair-ls", [(0, 0), (1, K)], "lockstep", rates, checks, fan_in=True)
    return K
```

- [ ] **Step 3: Extend `main`** (replace the stub):

```python
def main():
    ap = argparse.ArgumentParser(description="UC v2 M14 fleet-gate driver (spec §15 rows a–g)")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--fleet", action="store_true")
    ap.add_argument("--hosts", default="", help="pub/priv,... (else terraform output); 4 needed")
    ap.add_argument("--ssh-user", default="ubuntu")
    ap.add_argument("--ssh-key", default="/home/claude/.ssh/id_ed25519")
    ap.add_argument("--local-tree", default=str(Path(__file__).resolve().parent.parent.parent))
    ap.add_argument("--no-sync", action="store_true")
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--inflight", type=int, default=4096)
    ap.add_argument("--calib-ks", default="250,500,1000,2000,4000,8000",
                    help="SpinCountSm K ladder for the calibration arm")
    ap.add_argument("--k", type=int, default=0, help="skip calibration and use this K")
    ap.add_argument("--rows", default="abcdef", help="subset of a b c d e f (c runs with every arm)")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    if not a.fleet:
        ap.error("one of --fleet or --selftest is required")
    hosts, voters, learner = setup_fleet(a)
    rates, checks, verdicts = {}, [], []
    kill = join = None
    try:
        if any(r in a.rows for r in "abe"):
            K = arm_rates(voters, a, rates, checks)
        else:
            K = a.k
        if "d" in a.rows:
            kill = arm_kill(voters, a, K, checks)
        if "f" in a.rows:
            join = arm_join(voters, learner, a, K, checks)
    finally:
        stop_cluster_m14(hosts)
    print("\nM14 gate — FLEET (rates in ops/s over the 8 s window)")
    for k, v in rates.items():
        print(f"  {k:10s} {v:12.0f}")
    if "a" in a.rows: verdicts.append(verdict_row_a(rates))
    if "b" in a.rows: verdicts.append(verdict_row_b(rates))
    verdicts.append(verdict_row_c(checks))
    if kill is not None: verdicts.append(verdict_row_d(kill))
    if "e" in a.rows: verdicts.append(verdict_row_e(rates))
    if join is not None: verdicts.append(verdict_row_f(join))
    for v in verdicts:
        print(f"  [{'PASS' if v.passed else 'FAIL'}] {v.row} — {v.detail}")
    failed = [v for v in verdicts if not v.passed]
    if failed:
        print(f"RESULT: FAIL (honest) — {len(failed)} of {len(verdicts)} rows missed: {[v.row for v in failed]}")
        sys.exit(1)
    print(f"RESULT: PASS — {len(verdicts)} rows")
    sys.exit(0)
```

`arm_kill` and `arm_join` are defined in Tasks 4c and 4d; until then add two stubs that raise `NotImplementedError("Task 4c/4d")` so the module imports.

- [ ] **Step 4: Import check + selftest still green**

Run: `python3 -c "import sys; sys.path.insert(0,'bench-infra/scripts'); import m14_fleet_gate as g; print(g.node_args.__name__, g.BAR_B_LO)"`
Expected: `node_args 0.9`.

Run: `python3 bench-infra/scripts/m14_fleet_gate.py --selftest | tail -1`
Expected: `selftest: PASS`.

- [ ] **Step 5: Commit**

```bash
git add bench-infra/scripts/m14_fleet_gate.py
git commit -m "bench(m14): fleet setup (4 hosts + uc2ctl), per-arm cluster generations, calibration ladder, rows a/b/e arms, row c checks after every arm (M14d T4b)"
```

---

### Task 4c: `m14_fleet_gate.py` — row d (FSM kill on the leader host)

**Files:**
- Modify: `bench-infra/scripts/m14_fleet_gate.py` (replace the `arm_kill` stub)

**Interfaces:**
- Produces: `arm_kill(voters, a, K, checks) -> dict` for `verdict_row_d`; `status_slots(h) -> {id: {attached, applied, lag, snapshot_pos}}`; `node_stats(h) -> (unattested, legacy, mismatch)`; `lag_bound(h) -> int`.

- [ ] **Step 1: Implement**

```python
def status_slots(h):
    """`uc2ctl status` per-FSM rows (M14c) → {id: {...}}; also returns the
    node's fsm_lag bound (bytes; 0 = lockstep) from the `services:` line."""
    r = ssh(h, f"sudo {BUILT_CTL} status --instance-dir {h.dir} --app-id {APP}", label="uc2ctl")
    out = (r.stdout or "") + (r.stderr or "")
    slots = {}
    for m in STATUS_RE.finditer(out):
        slots[int(m.group(1))] = {
            "attached": m.group(2) == "true", "applied": int(m.group(5)),
            "lag": int(m.group(6)), "snapshot_pos": int(m.group(7)),
        }
    lm = re.search(r"fsm_lag=(\d+) bytes|fsm_lag=(lockstep)", out)
    bound = 0 if (lm is None or lm.group(2)) else int(lm.group(1))
    return slots, bound


def node_stats(h):
    """Last `stats:` line of the node unit's log → (unattested, legacy, mismatch)."""
    out = tail_log(h, "node", lines=400)
    hits = STATS_RE.findall(out or "")
    if not hits:
        return None
    u, l, m = hits[-1]
    return int(u), int(l), int(m)


def parse_timeline(out):
    return [(int(json.loads(m)["unix_ms"]), int(json.loads(m)["responses"])) for m in TL_RE.findall(out)]


def arm_kill(voters, a, K, checks):
    """Row d: the bounded pair under fan-in load; SIGKILL FSM 1's unit on the
    leader host; start it again at once. Recovery is judged twice — the
    client's own per-second timeline (M9's window rule, same host as the
    kill so no clock skew) and `uc2ctl status` showing FSM 1 attached with
    lag ≤ bound."""
    leader = start_cluster_m14(voters, [(0, 0), (1, K)])
    h = voters[leader]
    run_rate_arm(voters, leader, a, "kill", fan_in=True, secs=KILL_ARM_SECS, timeline=True, unit=True)
    t_start = time.time()
    time.sleep(12.0)                       # 2 s ramp + [2,10) s baseline + slack
    t0 = time.time()
    ssh(h, f"sudo systemctl kill --signal=SIGKILL {UNIT_PREFIX}-service1", label="SIGKILL")
    start_unit(h, "service1", service_args(h, 1, K, 0))
    attached_at = None
    deadline = t0 + 30.0
    _, bound = status_slots(h)
    while time.time() < deadline:
        slots, _ = status_slots(h)
        s1 = slots.get(1)
        if s1 and s1["attached"] and (bound == 0 or s1["lag"] <= bound):
            attached_at = round(time.time() - t0, 2)
            break
        time.sleep(0.25)
    # let the client finish, then read its timeline
    time.sleep(max(0.0, (t_start + KILL_ARM_SECS + 8) - time.time()))
    out = tail_log(h, "client", lines=2000) or ""
    d = parse_result(out, "direct")
    tl = parse_timeline(out)
    t0_ms = int(t0 * 1000)
    base_lo, base_hi = int((t_start + 2) * 1000), int((t_start + 10) * 1000)
    baseline, recovered, windows = recovery_time(tl, t0_ms, base_lo, base_hi)
    print("INFO recovery timeline (ops/s per 2 s window, end-relative to t0): " +
          ", ".join(f"{(e - t0_ms) / 1000:.1f}s:{r:.0f}" for e, r in windows[:25]), flush=True)
    print(f"INFO row d: baseline {baseline:.0f}/s, rate recovered at {recovered}s, "
          f"FSM 1 attached+lag≤{bound} at {attached_at}s; client lost={d['lost'] if d else '?'}", flush=True)
    kill_unit(h, "client")
    check_all(voters, leader, "kill", checks, expect_min=int(d["responses"]) if d else None)
    stop_cluster_m14(voters)
    return {"baseline": baseline, "recovered_at": recovered, "attached_at": attached_at,
            "bound": bound, "client_lost": d["lost"] if d else None}
```

- [ ] **Step 2: Import check, selftest**

Run: `python3 bench-infra/scripts/m14_fleet_gate.py --selftest | tail -1` → `selftest: PASS`.

- [ ] **Step 3: Commit**

```bash
git add bench-infra/scripts/m14_fleet_gate.py
git commit -m "bench(m14): row d — SIGKILL FSM 1 on the leader host under fan-in load; recovery by the client's own timeline (M9 rule) and uc2ctl status (M14d T4c)"
```

---

### Task 4d: `m14_fleet_gate.py` — row f (two-FSM learner join over wire 0.6.0)

**Files:**
- Modify: `bench-infra/scripts/m14_fleet_gate.py` (replace the `arm_join` stub)

**Interfaces:**
- Produces: `arm_join(voters, learner, a, K, checks) -> dict` for `verdict_row_f`.

- [ ] **Step 1: Implement**

```python
def arm_join(voters, learner, a, K, checks):
    """Row f: voters run the bounded pair with purge ON and snapshots every
    32 KiB; fan-in load runs for the whole arm; 10 s in, a learner declared
    {0,1} is admitted (`uc2ctl add-learner` on the leader — M7's pattern:
    the learner boots as a plain node with the CURRENT voters as its seed
    members) and must reach both voters' `applied` within 60 s via a
    two-artifact snapshot session (wire 0.6.0), with zero refusals."""
    leader = start_cluster_m14(voters, [(0, 0), (1, K)], purge=True, snap=M14_SNAPSHOT_INTERVAL_BYTES)
    h = voters[leader]
    run_rate_arm(voters, leader, a, "join", fan_in=True, secs=JOIN_ARM_SECS, timeline=False, unit=True)
    time.sleep(JOIN_AT_SECS)
    new_id, addr = 3, f"{learner.private_ip}:{PORT}"
    m12.wipe_dirs([learner])
    rc, out = h.ctl("add-learner", new_id, addr)
    if rc != 0:
        raise RuntimeError(f"add-learner refused: {out.strip()}")
    t0 = time.time()
    start_unit(learner, "node", node_args(learner, new_id, m12.members_str(voters), [(0, 0), (1, K)], None,
                                          True, M14_SNAPSHOT_INTERVAL_BYTES), nofile=True)
    time.sleep(2.0)
    for sid, spin in [(0, 0), (1, K)]:
        truncate_log(learner, f"service{sid}")
        start_unit(learner, f"service{sid}", service_args(learner, sid, spin, M14_SNAPSHOT_INTERVAL_BYTES))
    target = {i: s["applied"] for i, s in status_slots(h)[0].items()}
    print(f"INFO row f: leader applied at join start {target}", flush=True)
    joined_at = None
    while time.time() < t0 + BAR_F_JOIN_SECS + 5:
        slots, _ = status_slots(learner)
        if all(i in slots and slots[i]["attached"] and slots[i]["applied"] >= target.get(i, 0) for i in (0, 1)) \
                and all(slots[i]["snapshot_pos"] > 0 for i in (0, 1)):
            joined_at = round(time.time() - t0, 2)
            break
        time.sleep(0.5)
    time.sleep(max(0.0, (t0 - JOIN_AT_SECS + JOIN_ARM_SECS + 8) - time.time()))
    out = tail_log(h, "client", lines=200) or ""
    d = parse_result(out, "direct")
    kill_unit(h, "client")
    artifacts = {}
    for i in (0, 1):
        r = ssh(learner, f"sudo find {learner.dir}/snapshots/{i} -type f ! -name '*.part' 2>/dev/null | wc -l", label="ls")
        artifacts[i] = int((r.stdout or "0").strip() or 0)
    refusals = {}
    for hh in voters + [learner]:
        st = node_stats(hh)
        refusals[hh.public_ip] = (st[1], st[2]) if st else (-1, -1)
    hosts_all = voters + [learner]
    before = len(checks)
    check_all(hosts_all, leader, "join", checks, expect_min=int(d["responses"]) if d else None)
    check_ok = all(c[3] for c in checks[before:]) and len({c[4] for c in checks[before:]}) == 1
    print(f"INFO row f: joined_at={joined_at}s artifacts={artifacts} refusals={refusals} check_ok={check_ok}", flush=True)
    stop_cluster_m14(hosts_all)
    return {"joined_at": joined_at, "refusals": refusals, "artifacts": artifacts, "check_ok": check_ok,
            "client_lost": d["lost"] if d else None}
```

- [ ] **Step 2: Import check + selftest**

Run: `python3 bench-infra/scripts/m14_fleet_gate.py --selftest | tail -1` → `selftest: PASS`.
Run: `python3 -m py_compile bench-infra/scripts/m14_fleet_gate.py && echo OK` → `OK`.

- [ ] **Step 3: Commit**

```bash
git add bench-infra/scripts/m14_fleet_gate.py
git commit -m "bench(m14): row f — two-FSM learner join under load with purge on; both artifacts, uc2ctl catch-up, refusal counters (M14d T4d)"
```

---

### Task 5: The gate doc — bars first, results empty

**Files:**
- Create: `docs/benchmarks/uc2-m14-gate-2026-08-29.md`

**Interfaces:** none. Mirror `docs/benchmarks/uc2-m13-gate-2026-08-24.md`'s sections (title → decide-rule blockquote → "What the gate measures" → "The bar" table → "Reading row X's rule" → "How it is run" → "Dev box is not a bench" → "Honest-failure protocol" → "Results" → "Links"), with an M14 body.

- [ ] **Step 1: Write the doc**

Content (write it in full; the table is copied verbatim from spec §15.4 with the `result` column empty):

```markdown
# uc2 M14 gate — multi-service on the fleet

**Date:** 2026-08-29 (bars); fleet run: *pending*

> **Decide rule committed before any run.** The bar table below is copied
> verbatim from the design spec's §15.4
> (`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md`), written
> and reviewed before the driver existed. This document's own commit — the
> bar, with every result cell empty — lands **before** the commit that
> produces any result, per the honest-failure protocol carried forward from
> M7/M9/M10/M11/M12/M13. Nothing in the bar may be edited to match a result:
> a run that misses the bar is recorded as a FAIL and keeps the bar.

## What the gate measures

Spec: §1–§9 (the design), §14 (as built), **§15 (this gate)**. M14 puts N
state-machine processes behind one log: a bounded lag barrier or lockstep
between them (M14a), per-FSM query routing and a client fan-in (M14b), a
snapshot session that ships one artifact per declared FSM on wire `0.6.0`
(M14c), and per-FSM observability (M14c). Local numbers exist
(`uc2-m14a-apply-hop-2026-08-27.md`, `uc2-m14c-client-hop-2026-08-28.md`)
and set no bar; this is where the rates are adjudicated.

**Coverage statement (spec §15.1).** `2.8.0` ships multi-service with unit
tests, in-process integration on one node and a 3-node cluster, the M14b sim
scenario and the fuzz seeds. The two-FSM linearizability, partition,
hard-crash and Elle capstones are **M14c2**, a proof-only `2.8.1` after this
release. This gate's rows are what only a fleet can measure; they are not a
substitute for those capstones and do not claim to be.

## The bar

Pre-committed, verbatim from spec §15.4. Rows a–f are **fleet only**
(4 × `c6id.2xlarge`, `m12_gate` roles + `bench-infra/scripts/m14_fleet_gate.py`);
row g is CI.

| row | measure | bar | result |
|---|---|---|---|
| a | `n2eq` (two `CountSm`, bounded) vs `n1` (one), same run | **≥ 0.90** | |
| b | `pair` (`CountSm` + `SpinCountSm(K)`, bounded) vs `slow1` (`SpinCountSm(K)` alone) | **within [0.90, 1.10]** | |
| c | after every arm: every FSM on every host answers the same count, equal to the client's completed ops (rows a/b/e) or at least them (d/f) | **any mismatch = FAIL; blocks the release** | |
| d | SIGKILL FSM 1 on the leader host under fan-in load, restart at once | **≤ 15 s** to a 2 s window ≥ 80 % of baseline confirmed by the next, and FSM 1 attached with lag ≤ bound | |
| e | lockstep pairs vs their bounded twins | reported, **no bar** | |
| f | learner declared `{0,1}` joins a purged two-FSM leader under load | **≤ 60 s**, `snapshot_session_refusals() == (0, 0)` on every node, both artifacts present on the learner, row c on the learner | |
| g | `ci.yml` and the newest `nightly.yml` at or after the gated commit | green; this doc states the M14c2 deferral | |

### Reading the rules

**Rate** is the direct `Engine` client's completed operations per second over
the middle 8 s of a 12 s arm (2 s warm-up, 2 s tail), `--inflight 4096`,
64-byte payload, session envelope on, fan-in (`try_submit_all`) whenever two
FSMs are declared — one completion = every declared FSM answered. The client
runs **on the leader host** (shmem-attached; spec §15.2 errata).

**`K`** is chosen by a calibration ladder run first (`SpinCountSm(K)` alone
at K ∈ {250 … 8000}, 12 s each): the K whose rate is nearest 0.5 × `n1`.
Row b compares `pair` against `slow1` **at that same K in the same run**, so
the bar is independent of where the calibration lands. Both numbers and the
ladder are recorded below.

**Row d** is M9's rule (`m9_fleet_gate.py:343-379`), read from the client's
own per-second timeline on the same host as the kill (no clock skew): baseline
= mean over seconds [2, 10) of the arm; t0 is taken before the SIGKILL ssh;
recovered = the end of the first 2 s window at ≥ 80 % of baseline, confirmed
by the next 2 s window also qualifying. The second clause polls
`uc2ctl status` on that host for `id=1 attached=true lag ≤ fsm_lag`.

**Row f**: the voters run `PurgePolicy::BelowSnapshot { slack_bytes: 0 }`
with 16 KiB journal segments and a 32 KiB snapshot interval, so the joiner
is below the floor and converges by a snapshot session — two artifacts
(`SNAP_BEGIN_LAYOUT_V2`, `services_declared = 0b11`). "Joined" = both FSMs
attached on the learner with `applied ≥` the leader's per-id `applied` at
`add-learner` time and `snapshot_pos > 0`; the refusal pair is the node
role's `snap_refusals=(a,b)` stats line (`Node::snapshot_session_refusals`).

## How it is run

```sh
# fleet: 4 × c6id.2xlarge, us-east-1 (bench-infra/, `make up-uc` with node_count=4)
python3 bench-infra/scripts/m14_fleet_gate.py --selftest        # verdict arithmetic, no fleet
python3 bench-infra/scripts/m14_fleet_gate.py --fleet             # rows a–f; exit code = verdict
python3 bench-infra/scripts/m14_fleet_gate.py --fleet --rows d --k <K>   # one row, with the recorded K
```

Local smoke (never a gate): `cargo run -p uc_gateway --release --example
m12_gate -- --arm fsms --secs 4`.

## Dev box is not a bench

Rate bars are fleet-only (CLAUDE.md "Benchmarking discipline";
`docs/notes/dev-box-not-a-bench.md`). Every local number in the M14a/M14c
hop docs is smoke; nothing here is adjudicated from one.

## Honest-failure protocol

Adopted verbatim from M7/M9/M10/M11/M12/M13: the driver prints the bar and
exits non-zero on any FAIL — a green terminal is not a PASS; the exit code
is. Bar and result land in separate commits, bar first. A FAIL is diagnosed
before any re-run; harness defects and genuine product properties are both
recorded. Rows c and f's refusal clause are consensus/wire defects and block
the release outright. Tagging `v2.8.0` is a separate user-approved step.

## Results

*Empty until the fleet run. Facts to record (spec §15.5): the calibration
ladder and K; every arm's window rate, responses, lost, leader; row c's
counts per host per id; row d's timeline and both recovery times; row f's
per-id artifact counts, join time, refusal pairs; CI and nightly run ids;
the commit gated; the M14c2 deferral.*

## Links

- Spec: `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` (§15)
- Plan: `docs/superpowers/plans/2026-08-29-uc2-m14d-fleet-gate-and-release.md`
- Local hop docs: `uc2-m14a-apply-hop-2026-08-27.md`, `uc2-m14c-client-hop-2026-08-28.md`
- Prior gates: `uc2-m13-gate-2026-08-24.md` (row a's comparison point), `uc2-m9-gate-2026-08-19` (row d's rule), `uc2-m6-gate-*` (row f's budget)
```

- [ ] **Step 2: Commit — the bar, alone**

```bash
git add docs/benchmarks/uc2-m14-gate-2026-08-29.md
git commit -m "docs(bench): M14 gate doc — spec §15.4 bars committed, every result cell empty (M14d T5)"
```

---

### Task 6: The fleet run (user-approved) and the results commit

**Files:**
- Modify: `docs/benchmarks/uc2-m14-gate-2026-08-29.md` (Results, the table's result column, the date line)

**Interfaces:** none. **Do not start this task until the user has approved the fleet run.** Fleet ops facts (memory, `bench-infra/`): rsync ships the local tree; ansible builds as root; the real terraform state is `bench-infra/terraform/`; ssh with `-i key` and `SSH_AUTH_SOCK` unset; daemons under `systemd-run` need `LimitNOFILE` (the node units already pass it).

- [ ] **Step 1: Bring up 4 hosts**

```sh
cd bench-infra && make up-uc          # with node_count=4 in the tfvars the Makefile reads (TFVARS)
```

Confirm: `terraform -chdir=terraform output -json nodes | jq length` → `4`.

- [ ] **Step 2: Run the gate**

```sh
cd /home/claude/ultima/ultima_cluster
python3 bench-infra/scripts/m14_fleet_gate.py --fleet 2>&1 | tee ~/.cache/uc2-m14-gate-$(date +%F).log
echo "exit=$?"
```

Expected on a clean run: `RESULT: PASS — 6 rows`, exit 0. On any FAIL: **stop, diagnose, record** — do not re-run first. The log is the evidence; keep it.

- [ ] **Step 3: Tear down**

```sh
cd bench-infra && make destroy && terraform -chdir=terraform state list | wc -l   # → 0
```

- [ ] **Step 4: Record the results**

Fill the `result` column and the Results section with spec §15.5's facts, from the log's `GATE-JSON` and `INFO` lines: the ladder and K; each arm's `window_rps`/`responses`/`lost`/leader; row c's `FSMS` counts per host; row d's timeline line and both times; row f's join time, artifacts, refusals; the gated commit (`git rev-parse --short HEAD` before the run); CI run id (`gh run list --workflow ci.yml --limit 1`) and the newest nightly at or after that commit (`gh run list --workflow nightly.yml --limit 3`). Row g's result cell also carries the sentence: "The 2026-08-28 scheduled nightly (33184711408) failed `crashtest` and `survival` on 4347bc2; `a4a7a9c`'s commit body names it — the enospc respawn raced M14a's `service.<id>.lock` — and the next nightly (33246873016, on 5242054) passed `crashtest`, `survival` and `crashtest-crypto`." Verify those run ids with `gh run view <id> --json conclusion,headSha` before writing them. Change the date line to `fleet run: <date>` and the title's status.

- [ ] **Step 5: Commit the results**

```bash
git add docs/benchmarks/uc2-m14-gate-2026-08-29.md
git commit -m "docs(bench): M14 gate — fleet run <date>: rows a–f <PASS/FAIL per row>, K=<K>, commit <sha> (M14d T6)"
```

If any row FAILed, the subject says so and the Results section carries the diagnosis; the bar stays.

---

### Task 7: The multi-service explainer

**Files:**
- Create: `docs/notes/uc2-m14-multi-service-explained.md`

**Interfaces:** none. Sources to read first: spec §1–§5, §7.3, §9, §14; `docs/reference/configuration.md` § `[services]`; `docs/how-to/upgrade-a-cluster.md` § "Wire change in 2.8.0"; `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md` (the lockstep lesson); `uc_node/src/services.rs` (the `FsmLag` doc comments); `uc_service/src/apply.rs` (the lag barrier). Style: `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` — a dated italic status line, then sections that explain the mechanism in plain language with the file:line that implements it.

- [ ] **Step 1: Write the explainer** with exactly these sections:

1. `# One log, N state machines — how M14 multi-service works` + status line (`*Written 2026-08-29 for the 2.8.0 release. Coverage: see the gate doc's coverage statement; the two-FSM capstones are M14c2.*`).
2. **Why N FSMs behind one log** — the problem (two applications wanting one consensus plane; the alternative of one log per app), what stays identical (one leader, one commit position, one journal), and what is new (per-service rings/dirs/slots, ids `0..8`, id 0 the default responder and the only one the remote path reaches — `docs/reference/limits.md`).
3. **The lag barrier** — why "unbounded" is a silent death spiral (a lagging FSM's replay cost grows without bound — quote `services.rs`'s doc comment), `Bounded(bytes)` (`applied_a − applied_b ≤ bytes` for any pair) as a `next_batch` target cap in the apply loop, the default `buffer_bytes / 4`, what an operator sees (`uc_service_lag_bytes{service}`, `lag_waits_total`, the `Uc2ServicePinnedAtLagBound` alert).
4. **Lockstep and what it costs** — no FSM starts frame k+1 until every FSM finished k; the N-way cross-core handshake (~1.6 µs/frame on the dev box, M14a); the 2026-08-27 lesson: a barrier wait must never sleep on a live sibling (18 k → 631 k frames/s) and why that generalises.
5. **The quorum-gated report ceiling** — a node's durable report is capped by its own FSMs' progress plus the bound, so commit stalls iff a quorum's FSMs are stuck (M14a; sim inv10) — the liveness coupling this buys and its cost (one stalled FSM on a quorum of hosts is a cluster-scope stall — link the threat-model §5 line Task 11 adds).
6. **Routing and fan-in** — `submit_to(id)`, `submit_all`, `query_*_on(id)`, `MSG_V2_BAD_SERVICE`, the client's fan-in buffer; what happens to a command when only one FSM answers.
7. **Snapshots on wire 0.6.0** — one artifact per declared FSM in a session, `SNAP_BEGIN`'s `layout`/`service_id`/`services_declared`, adopt-on-complete, the two named refusals (`peer wire 0.5.0`, `declared-set mismatch`) and why a mixed cluster stalls a joiner rather than installing half a set; the flag-day terms.
8. **Observing it** — the per-FSM metric twins, the two alerts, `uc2ctl status`'s table, `service_attached`/`service_detached` records.
9. **What is not there yet** — remote-path FSM selection, a datagram header version field, the M14c2 capstones (in §15.1's words), the M14b/M14c deferred minors by pointer.

Every section names the file(s) implementing it. No section may restate a number the gate doc will carry; link the gate doc instead.

- [ ] **Step 2: Link it**

`docs/notes/` has no index file (verified 2026-08-29). Add one line to `docs/reference/configuration.md`'s `[services]` section: "Background: [how multi-service works](../notes/uc2-m14-multi-service-explained.md)."

- [ ] **Step 3: Commit**

```bash
git add docs/notes/uc2-m14-multi-service-explained.md docs/reference/configuration.md
git commit -m "docs(notes): uc2-m14-multi-service-explained — one log, N FSMs: lag barrier, lockstep, report ceiling, routing, 0.6.0 snapshots (M14d T7)"
```

---

### Task 8: Version bump and the literal-string sweep

**Files:**
- Modify: `Cargo.toml:8` (`version = "2.7.0"` → `"2.8.0"`), `Cargo.toml:44`, `uc_net/Cargo.toml:15-17`, `uc_lincheck/Cargo.toml:25`, `uc_crypto/Cargo.toml:15`, `examples/uc_crashtest/Cargo.toml:47-76`, `uc_log/Cargo.toml:18`, `uc_client/Cargo.toml:17-18` (every `version = "2.7.0"` pin — the list above is `grep -rn '2\.7\.0' --include=Cargo.toml .` on 2026-08-29; re-run it), `README.md:33,37,38`, `packaging/Dockerfile:22-38`, `packaging/compose.yml:15,18,40`, `docs/how-to/run-a-cluster.md:31,65,71,73`, `SECURITY.md` (supported line `2.7.x` → `2.8.x`, table rows).

**Interfaces:** none.

- [ ] **Step 1: Bump every manifest pin**

```sh
grep -rln '2\.7\.0' --include=Cargo.toml . | grep -v target | xargs sed -i 's/version = "2\.7\.0"/version = "2.8.0"/g'
grep -rn '2\.7\.0' --include=Cargo.toml . | grep -v target      # → no output
cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="uc_node") | .version'   # → 2.8.0
cargo build --workspace 2>&1 | tail -2
```

- [ ] **Step 2: Sweep the literal strings**

```sh
sed -i 's/2\.7\.0/2.8.0/g' packaging/Dockerfile packaging/compose.yml docs/how-to/run-a-cluster.md
sed -i 's/v2\.7\.0` tarball/v2.8.0` tarball/; s/uc2-2\.7\.0-x86_64/uc2-2.8.0-x86_64/g' README.md
grep -rn '2\.7\.0' packaging/ docs/how-to/run-a-cluster.md README.md QUICKSTART.md docs/QUICKSTART.md 2>/dev/null
```

Expected: the only remaining `2.7.0` hits are historical ("since 2.7.0", the v2.7.0 README table row, `packaging/systemd/uc2-gateway.service`'s "Before 2.7.0" comment). Leave those.

- [ ] **Step 3: `SECURITY.md`**

Change `**`2.7.x`**` → `**`2.8.x`**` in the prose and the table rows to `| `2.8.x` | yes |` / `| `< 2.8` | no — upgrade |`.

- [ ] **Step 4: Verify the workspace still packages**

```sh
cargo package -p uc_protocol --allow-dirty --no-verify 2>&1 | tail -2
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -2
```

Expected: package OK; clippy clean.

- [ ] **Step 5: Commit**

```bash
git add -A Cargo.toml '*/Cargo.toml' examples/uc_crashtest/Cargo.toml Cargo.lock README.md packaging/ docs/how-to/run-a-cluster.md SECURITY.md
git commit -m "chore(release): 2.7.0 → 2.8.0 — workspace version, intra-workspace pins, packaging/docs literals, SECURITY.md supported line (M14d T8)"
```

---

### Task 9: `RELEASES.md` and `docs/releases.md`

**Files:**
- Modify: `RELEASES.md` (new `## v2.8.0` section at the top, after the intro paragraph), `docs/releases.md` (new top entry)

**Interfaces:** links to docs that exist after Tasks 5–7: the gate doc, the explainer, `docs/reference/configuration.md#services`, `docs/how-to/upgrade-a-cluster.md` § "Wire change in 2.8.0", `docs/reference/limits.md`, `docs/how-to/monitor-a-cluster.md`, `docs/reference/uc2ctl.md`, `docs/how-to/back-up-a-cluster.md`, the two hop docs. Section 6 of the explainer must carry the heading `## Routing and fan-in` (the release bullet links its anchor).

- [ ] **Step 1: `RELEASES.md`** — insert after the intro, before `## v2.7.0`:

```markdown
## v2.8.0 — <tag date> — several state machines behind one log (M14)

A cluster can now run up to eight state-machine processes per node, all fed
by the one replicated log: submit to any of them, fan a query across all, and
keep them within a bounded distance of each other or in lockstep. The
node-to-node wire moves to `0.6.0` (one datagram changed) and the control
page to cnc `3.0` (8 KiB) — both flag days, on the same terms as every prior
one. Proof record, row by row: [M14 gate](docs/benchmarks/uc2-m14-gate-2026-08-29.md).
Background: [how multi-service works](docs/notes/uc2-m14-multi-service-explained.md).

- **`[services]`: declare N state machines, bounded or lockstep**
  (`uc_node`, `uc_service`): ids `0..8` (id 0 is the default responder and
  the only one the remote path reaches), each attaching with
  `ServiceConfig::service_id`, holding `service.<id>.lock`, and publishing
  its progress on the cnc page's per-service band. A lag policy keeps them
  together: `fsm_lag = "<bytes>"` bounds how far any FSM may lead another;
  `"lockstep"` makes every FSM finish frame k before any starts k+1. A
  node's durable report is capped by its own FSMs' progress, so commit
  stalls only when a quorum's FSMs are stuck — never on one straggler. →
  [Configuration § `[services]`](docs/reference/configuration.md#services) ·
  [Limits](docs/reference/limits.md)
- **Per-FSM routing and a client fan-in** (`uc_client`, `uc_protocol`):
  `submit_to(id)`, `submit_all` (one ticket, every FSM's answer),
  `query_snapshot_on` / `query_linearizable_on`; a query names its FSM on the
  wire and an undeclared id answers `BAD_SERVICE` instead of parking. →
  [How it works § routing and fan-in](docs/notes/uc2-m14-multi-service-explained.md#routing-and-fan-in) · [Read path](docs/reference/read-path.md)
- **A snapshot session ships every FSM's artifact — wire `0.6.0`**
  (`uc_net`): `SNAP_BEGIN` now names the FSM, the sender's declared set and
  a layout byte; a joiner adopts the floor only once the whole set has
  landed, and refuses by name a `0.5.0` sender or a mismatched set rather
  than installing half a cluster. → [Upgrade: the 0.6.0 flag day](docs/how-to/upgrade-a-cluster.md#wire-change-in-280-snap_begin-carries-every-fsms-snapshot-060) ·
  [Wire protocol](docs/reference/wire-protocol.md)
- **Per-FSM observability** (`uc_node`, `uc2ctl`): `service="<id>"` twins of
  the service families, `uc_service_attached`, `uc_service_lag_bytes`,
  `uc_service_lag_waits_total`, `uc_services_declared`; two alerts
  (`Uc2ServiceAbsent`, `Uc2ServicePinnedAtLagBound`) proven to fire; a
  per-FSM table in `uc2ctl status`; `service_attached`/`service_detached`
  transition records. → [Monitor a cluster](docs/how-to/monitor-a-cluster.md) ·
  [uc2ctl](docs/reference/uc2ctl.md)
- **Per-FSM backup and restore**: `snapshots/<id>/` per FSM in the backup
  artifact and on restore. → [Back up a cluster](docs/how-to/back-up-a-cluster.md)
- **Fixed:** an unservable `SNAP_NAK` no longer pins a snapshot-session slot;
  intake I/O failures are retried and counted (`uc2_snapshot_intake_io_failures_total`).
  The lockstep barrier no longer sleeps on a live sibling (18 k → 631 k
  frames/s at N=2 on the dev box) — [apply-hop bench](docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md).
- **Performance:** [M14 gate](docs/benchmarks/uc2-m14-gate-2026-08-29.md)
  (rows a–f, fleet); [apply hop](docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md)
  and [client hop](docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md) (local, smoke).

**Upgrade consequence.** Wire `0.6.0` and cnc `3.0` are flag days: stop every
node, upgrade, start them together; a mixed cluster replicates and elects but
a snapshot session between versions is refused by name, so a joiner stalls
until the fleet matches. Existing single-service deployments need no config
change — no `[services]` section means `{0}` with the default bound. A
service must attach as id 0 (the default). Details, in the imperative:
[Upgrade a cluster](docs/how-to/upgrade-a-cluster.md).

**Coverage.** Multi-service ships with unit tests, in-process integration
(one node and a 3-node cluster), a sim scenario for the report ceiling and
fuzz seeds for the new wire bytes. The two-FSM linearizability, partition,
hard-crash and Elle capstones are the next, proof-only release (`2.8.1`,
"M14c2") — stated in [VERIFICATION §11](docs/VERIFICATION.md#11-what-is-not-verified).
```

Verify each linked file exists: `for f in $(grep -oE '\(docs/[^)#]+' RELEASES.md | head -40 | tr -d '('); do test -f "$f" || echo "MISSING $f"; done` → no `MISSING`. (There is no client-SDK reference page — verified 2026-08-29; the fan-in API is documented in `semver-policy.md`'s additive rows, `read-path.md` and the explainer, which is why the bullet links the explainer's section.)

- [ ] **Step 2: `docs/releases.md`** — new top entry mirroring the 2.7.0 entry's shape: `## v2.8.0 — <date> — M14 multi-service`, a bold opening claim, the spec/gate/explainer paths, a paragraph on what the release touches (wire `0.6.0`: `SNAP_BEGIN` 26 → 34 fixed bytes; cnc `3.0`: 8 KiB, page-2 `ServiceSlot[8]`, the 4032 pair; `uc_protocol::version::CURRENT` = `0.6.0`), then `###` subsections: **M14a foundation** (lag barrier, report ceiling, service slots, lock, per-id dirs, reservation formula), **M14b routing** (query codec, `BAD_SERVICE`, fan-in buffer, sim inv10), **M14c** (the client hot-path A/B that refuted its own premise — link the client-hop doc; the 0.6.0 session; observability), **the gate** (row summary with numbers from Task 6), **deferred** (M14c2 by name; the M14b/M14c deferred minors by pointer to the plans' execution records), and **the three rustdoc-only behaviours** the M14b plan's execution record (`docs/superpowers/plans/2026-08-27-uc2-m14b-query-routing-and-fan-in.md:1814-1817`) asked to carry into the writeup — read that list and state each in one sentence.

- [ ] **Step 3: Commit**

```bash
git add RELEASES.md docs/releases.md
git commit -m "docs(release): v2.8.0 writeup — RELEASES.md section + docs/releases.md engineering record (M14d T9)"
```

---

### Task 10: The invalidated-statement sweep

**Files:**
- Modify: `docs/reference/limits.md:10,21,42`, `docs/how-to/upgrade-a-cluster.md:238-270`, `README.md:89`, `CLAUDE.md:13,20-39,43,182,351`, `docs/VERIFICATION.md:13-16,653-656`

**Interfaces:** none.

- [ ] **Step 1: `limits.md`** — line 10: drop "and the unreleased M14 work on `main`" (the sentence becomes "Where a limit changed between releases, the row says so"); line 21: `(M14, unreleased)` → `(2.8.0)`; line 42: "`2.7.0` ships wire `0.5.0`; M14c on `main` moves it to `0.6.0`" → "`2.7.0` shipped wire `0.5.0`; `2.8.0` moves it to `0.6.0`".

- [ ] **Step 2: `upgrade-a-cluster.md`** — the "Wire change in 2.8.0" section: read it; replace any "on `main`"/"this branch" phrasing with the release; add the cnc `3.0` same-host restart sentence if absent (nodes, services and clients on a host restart together because the page grew to 8 KiB — `docs/reference/cnc-page.md`).

- [ ] **Step 3: `README.md:89`** — the `*on main* (M14)` row becomes `| v2.8.0 (M14) | Several state machines per cluster, fed by one log; submit to any, or fan a query across all | [How it works](/docs/notes/uc2-m14-multi-service-explained.md) · [M14 gate](/docs/benchmarks/uc2-m14-gate-2026-08-29.md) |`.

- [ ] **Step 4: `CLAUDE.md`** — line 13: `2.7.0` (M13) → `2.8.0` (M14), "M1–M13" → "M1–M14"; add the table row `| M14 | v2.8.0 | multi-service: one log → N FSMs (bounded/lockstep lag, per-FSM routing + fan-in, 0.6.0 snapshot stream, per-FSM observability) | `uc2-m14-gate-2026-08-29` |`; replace lines 38–39 ("Next up: M14 … worktree") with "Next up: **M14c2** — the two-FSM capstones (`lin_v2 two_fsm`, `lin_partition_v2`, hard-crash, Elle) as a proof-only `2.8.1`; spec §15.1."; line 43: "Wire protocol is 0.5.0" → "Wire protocol is 0.6.0" and add "(`0.6.0` changed `SNAP_BEGIN` only; a `0.5.0` sender's session is refused by name, so a mixed cluster stalls a joiner rather than installing half a set)"; lines 182 and 351: "4 KiB" → "8 KiB (cnc 3.0: page 2 is the per-service slot band)"; the M13-mechanics bullet gains one M14 sibling: "**M14 mechanics worth knowing**: ≤ 8 FSMs, id 0 mandatory and remote-reachable; lag policy per node must match cluster-wide (checked on the snapshot path); one stalled FSM on a quorum of hosts stalls commit by design (report ceiling); `service.<id>.lock` per FSM." Also line 99's worktree mention: it names `worktree-uc2-multi-service` as long-lived — that worktree is gone (`git worktree list`); change the fmt-deferral sentence to name only `fix/remaining-flakes`.

- [ ] **Step 5: `docs/VERIFICATION.md`** — header (lines 13–16): "current as of the M14d release pass (2026-08-29), which added the M14 coverage statement to §11; §7 as of M12d; the proof, simulation and capstone tiers as of the M8 gate (2026-07-29)". §11: replace the M14a bullet (653–656) with: "**Multi-service (M14) is covered by unit tests, in-process integration on one node and a 3-node cluster (`uc_node/tests/services.rs`, `learner.rs`'s two-FSM join, `uc_net/tests/snapshot_session.rs`'s two-artifact stream), the M14b sim scenario (inv10) and fuzz seeds for the 0.6.0 `SNAP_BEGIN` and the query split. It is NOT yet covered by any linearizability capstone, partition test, hard-crash scenario or Elle tier with two FSMs — those are M14c2, a proof-only `2.8.1` (spec §15.1). The M14 fleet gate (`docs/benchmarks/uc2-m14-gate-2026-08-29.md`) measures rates, a kill and a join; it is not a substitute.**"

- [ ] **Step 6: Verify nothing stale remains**

```sh
grep -nE 'unreleased|on `main`|worktree-uc2-multi-service|Wire protocol is 0\.5|4 KiB' CLAUDE.md docs/reference/limits.md README.md docs/how-to/upgrade-a-cluster.md docs/VERIFICATION.md
```

Expected: no hits except historical "4 KiB" mentions that describe cnc 2.x explicitly (read each; leave those).

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md README.md docs/reference/limits.md docs/how-to/upgrade-a-cluster.md docs/VERIFICATION.md
git commit -m "docs: 2.8.0 sweep — limits/upgrade/README/CLAUDE.md project status (M14 row, wire 0.6.0, cnc 8 KiB), VERIFICATION §11 M14 coverage statement (M14d T10)"
```

---

### Task 11: Security-posture refresh

**Files:**
- Modify: `docs/security/attack-surface.md` (the cnc row; a new row; the `SNAP_BEGIN` row), `docs/security/threat-model.md` §5, `docs/security/self-assessment.md` (§1 scope note, §2 F7, §4 item, §5 table, status line)

**Interfaces:** facts verified 2026-08-29: `uc_protocol/src/v2/cnc.rs:266-284` (`CNC_OFF_SERVICE_SLOTS = 4096`, `CNC_MAX_SERVICES = 8`, page 8 KiB); `uc_protocol/src/v2/ipc.rs:89-95` (`split_query_payload`, `write_query_payload`), fuzzed via `fuzz/fuzz_targets/ring_mpsc_record.rs` since `5feae7c`; `uc_gateway/src` has no `service_id` reference (remote path = FSM 0); `uc_net/src/receiver.rs:1711` (declared-set check), `:1837` (`id < 64`), `sender.rs:1039`; `uc_service/src/attach.rs:95` (`service.<id>.lock`); `a405e71` (the `SNAP_NAK` slot-pinning fix).

- [ ] **Step 1: `attack-surface.md`**

cnc row: "4 KiB fixed layout, magic + version gate" → "8 KiB fixed layout since cnc `3.0` (2.8.0): page 1 as before, page 2 the `ServiceSlot[8]` band (`CNC_OFF_SERVICE_SLOTS = 4096`); magic + version gate; offsets pinned in two crates with assertion tests". New row after it:

`| **Query payload split** (`service_id ++ query`, M14b) | `uc_protocol/src/v2/ipc.rs: split_query_payload` (reached from the node's query ring; `MSG_V2_BAD_SERVICE` answers an undeclared id) | a same-uid local client through the shmem query ring — **not** the gateway (`uc_gateway` never sets an id; the remote path reaches FSM 0 only) | no (boundary D) | one leading byte; an empty payload is `None`, never a panic; an id ≥ 8 or undeclared is refused by name | `ring_mpsc_record` (seeds `10-query-with-id`, `11-query-empty`) | Boundary D is not a security boundary; the check catches mistakes. |`

`SNAP_BEGIN` row, Notes column: append "Since 0.6.0 a session carries one artifact per declared id, so a forged `SNAP_BEGIN` (crypto off) can populate **N** `snapshots/<id>/` directories on the joiner, not one; the id is bounded (`< 64`, and must be in the receiver's own declared set — `receiver.rs:1711`, `:1837`) so it selects among ≤ 8 fixed directories and never a path."

- [ ] **Step 2: `threat-model.md` §5** — add a bullet after "Denial of service beyond the stated caps":

"- **A stalled FSM is a cluster-scope liveness lever (2.8.0).** M14's report ceiling caps a node's durable report by its own FSMs' progress, so one stalled or slow FSM process on a *quorum* of hosts stalls commit cluster-wide — by design, so a lagging FSM never falls unrecoverably behind. In lockstep mode one stalled FSM also parks every sibling on its host. Same-uid processes are inside the trust boundary, so this is not a new actor; it is a new blast radius for a same-uid mistake (a wedged service, a squatted `service.<id>.lock`). The `Uc2ServiceAbsent` / `Uc2ServicePinnedAtLagBound` alerts are the detection; restarting the FSM is the remedy."

- [ ] **Step 3: `self-assessment.md`**

§1, after the "**When:**" sentence: "**Revised 2026-08-29 for `2.8.0` (M14, multi-service):** the M12d dating below is kept as history; the additions are F7, item 8 in §4, and the M14 line in §5."

§2, after F6:

```markdown
### F7 — an unservable `SNAP_NAK` pinned the snapshot-session slot

**Severity:** low (liveness of a joiner; no integrity effect) · **Status:**
fixed, `a405e71` (M14c) · **Found by:** the M14c review of the per-FSM intake.

With one artifact per declared FSM in a session (wire 0.6.0), a `SNAP_NAK`
for a range the sender could not serve left the session slot occupied, so a
joiner could wedge a sender's slot until the 30 s cycle. The sender now
refuses a set that misses a declared id up front, an unservable NAK releases
the slot, and intake I/O failures are retried and counted
(`uc2_snapshot_intake_io_failures_total`). Reachability: any peer, or anyone
spoofing one with crypto off — the same reach as every SNAP kind.
```

§4, new item 8: "**The multi-artifact snapshot intake state machine** (`uc_net/src/receiver.rs`, M14c): adopt-on-complete across N artifacts, `.part` files, an abandoned intake's unlink, the declared-set and layout refusals, and the interaction with a concurrent second session from another peer. It is unit-tested and exercised by one two-FSM learner test; nobody outside the project has read it against interleaved or malformed sessions."

§5 table, new row: `| **M14 multi-service** | unit + in-process integration + sim inv10 + fuzz seeds; **no two-FSM lincheck/partition/crash/Elle yet** (M14c2, `2.8.1`) — [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified) |`.

Status line: "**Status: package prepared 2026-08-24; revised for 2.8.0 on 2026-08-29; external review pending (gate row 10).**"

- [ ] **Step 4: Commit**

```bash
git add docs/security/attack-surface.md docs/security/threat-model.md docs/security/self-assessment.md
git commit -m "docs(security): 2.8.0 refresh — cnc 3.0 row, query-split row, N-artifact SNAP_BEGIN note, stalled-FSM liveness lever, F7 (SNAP_NAK slot pinning), M14 coverage line (M14d T11)"
```

---

### Task 12: Release candidate, verification, tag — the user's steps

**Files:** none (commands only).

**Interfaces:** `docs/how-to/cut-a-release.md` §2–§5.

- [ ] **Step 1: Pre-tag checks (run these; paste the output into the final report)**

```sh
git status --short | wc -l                                   # → 0
cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="uc_node") | .version'   # → 2.8.0
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1
cargo test --workspace 2>&1 | grep -E '^test result' | grep -v ' 0 failed' ; echo "(no line above = all suites 0 failed)"
gh run list --workflow ci.yml --limit 1                       # green on HEAD
(cd fuzz && cargo +nightly run --bin seed-corpus) && git status --short fuzz/ | wc -l   # → 0 (corpus = seeds only)
```

- [ ] **Step 2: Hand the user the tag commands** (cut-a-release §3–§5), verbatim in the final report:

```sh
git tag -s v2.8.0-rc.1 -m "v2.8.0-rc.1"
git push origin main v2.8.0-rc.1
gh run watch                                                  # version → build → sbom → release-smoke → release → image
# then, as a stranger:
cosign verify-blob --bundle SHA256SUMS.bundle \
  --certificate-identity-regexp 'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
git tag -s v2.8.0 -m "v2.8.0 — M14 multi-service"
git push origin v2.8.0
```

The crates.io publish is §6 of the same how-to and remains manual.

- [ ] **Step 3: Update the memory index** (the assistant's own memory, not the repo): `m13-multi-service-design.md` → "2.8.0 tagged <date>, M14c2 next"; `verification-drift-audit-2026-08-29.md` → the items this plan closed.

---

## Self-review (run after writing; findings fixed inline)

**Spec coverage (§15):**
- §15.1 cut + M14c2 deferral stated → Tasks 5 (coverage statement), 9, 10 (VERIFICATION §11, CLAUDE.md "Next up") ✓
- §15.2 topology → Task 0 (errata), 4b (`setup_fleet`) ✓
- §15.3 harness: node flags (T1), `SpinCountSm` + `--service-id/--work-spin` (T2), `--purge` (T1 as `--purge-below-snapshot`), stats line (T1), fan-in + `query_linearizable_on` (T3), driver + selftest + `GATE-JSON` (T4a), K calibration (T4b), rate definition (T3 window + T4b constants) ✓
- §15.4 rows a–g → T4b (a, b, e, c), T4c (d), T4d (f), T5/T6 (g) ✓
- §15.5 facts → T6 step 4 ✓
- §15.6 release items 1–6 → T8, T9 + T7, T10, T11, T6 (nightly recorded in row g), T12 ✓
- §15.7 acceptance → tests in T1–T3, selftest T4a, gate doc T5, run T6, writeup T7–T11 ✓

**Placeholder scan:** none of the forbidden phrases; every code step carries code. T7's explainer lists section contents rather than prose — that is the deliverable's outline, and each section names its sources.

**Type consistency:** `services_from_flags(Option<&str>, Option<&str>)` (T1) used with `.as_deref()` (T1); `node_config`'s new trailing triple used identically in T1's `run_node_role` and T3's `boot_cluster2`; `SpinCountSm::with_spin(u64)` (T2) used in T3; `MeasureOpts` fields match `ClientDirectArgs` flags (T3) and the driver's flags `--warmup-secs/--measure-secs/--fan-in/--timeline` (T4b); `check-fsms` flags `--mode/--expect/--expect-min` (T3) match `check_fsms` (T4b); the node stats regex `snap_refusals=(a,b)` (T4c `STATS_RE`) matches T1's `println!`; `STATUS_RE` (T4a) matches `uc_ctl/src/main.rs:551-557`'s `id=… attached=… epoch=… incarnation=… applied=… lag=… snapshot_pos=…` order ✓; `Verdict(row, passed, detail)` positional as in `m12_fleet_gate.py:453`.
