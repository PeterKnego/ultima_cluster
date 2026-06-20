# Cross-Host Journal-Preallocation Commit A/B Run-Book

**Goal:** Measure the per-commit latency impact of journal **segment preallocation**
(`preallocate_segments` on vs. off) on a live 3-node UC cluster over a real cross-host
network, using the existing `bench-infra` run role and `commit-path-load` open-loop harness.

This is the authoritative validation gated by `ultima_db` **task36 §7** (the local microbench +
SMR gates are already green; flag ships default-off until this run justifies flipping it).

**Billable cloud run** — provisions real hosts (~$0.70/hr/node AWS `c7i.4xlarge`, ~$2.10/hr for 3).
**Do not start without an explicit go-ahead.**

---

## Read this first — what this A/B can and cannot show

Preallocation removes the ext4 **jbd2 metadata commit** a size-extending append forces on
`fdatasync`. The local microbench measured that barrier at ~600µs → ~210µs (−65% p50). But:

1. **It only acts under `Durability::Consistent`** (the per-commit `fdatasync`). In Eventual
   mode fsync is background/amortized and preallocation does ~nothing. **The A/B MUST run
   Consistent.**
2. **Group commit amortizes the barrier at high load.** The win is *per-commit latency*, visible
   at low/sub-saturation rates + low inflight, NOT throughput (task36 §1). Expect `achieved_rate`
   to be roughly flat.
3. **Full end-to-end p50/p99 will likely be ≈NULL — by design.** The end-to-end commit path is
   dominated by `api_batch_linger` (~5ms) + replication (~2.7ms); journal fsync is ~4% / a P99
   tail (this is exactly why the **fdatasync** A/B came back NULL — task13 §16). A ~390µs barrier
   cut is in the noise of an ~8ms path.
4. **Therefore the PRIMARY metric is the targeted `submitted→persisted` decomposition** (the
   journal write+fsync segment, leader-local), NOT end-to-end submit→response. This is the
   decomposition the fdatasync A/B explicitly deferred. It requires the `profile/raftcore-stats`
   instrument (RAFT_RUNTIME_STATS scraped from node0 stderr). End-to-end p50/p99 + throughput are
   captured as SECONDARY (expected NULL, recorded for completeness).
5. **Prod NVMe caveat:** on a power-loss-protected write cache the device flush is cheap and the
   jbd2 delta may shrink vs. the dev host. The mechanism is filesystem-level (ext4 data=ordered),
   so the direction holds, but the *magnitude* is exactly what this run measures.

**Falsifiable prediction:** `submitted→persisted` p50/p99 drops materially (toward the microbench
−50..65%) with preallocation on; end-to-end p50/p99 and `achieved_rate` move within noise.

---

## Step 0 — one-time prep (env toggle, mirrors `UC_PIPELINE_DEPTH`)

Preallocation is currently hardcoded `false` in `log_storage.rs`. To A/B it on the **same binary**
(no rebuild confound), make it env-readable and thread it through the `run` role — exactly like
`UC_PIPELINE_DEPTH` (uc_node `pipeline_depth()` + `uc_pipeline_depth` group var).

**(a) `uc_node/src/raft/log_storage.rs`** — replace the hardcoded field:

```rust
// helper near the top of the impl (mirrors network::parse_pipeline_depth):
fn journal_prealloc_from_env() -> bool {
    matches!(std::env::var("UC_JOURNAL_PREALLOC").ok().as_deref(), Some("1") | Some("true"))
}
```
```rust
        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: log_durability,
-           preallocate_segments: false,
+           // A/B toggle (task36): off unless UC_JOURNAL_PREALLOC=1. Default stays OFF.
+           preallocate_segments: journal_prealloc_from_env(),
        })?);
```
Add a unit test mirroring `parse_pipeline_depth_*` (None→false, "1"→true, "0"/"bad"→false).

**(b) `bench-infra/ansible/group_vars/all.yml`** — add the group var (default off):

```yaml
uc_journal_prealloc: 0          # journal segment preallocation (task36) via UC_JOURNAL_PREALLOC; override with -e uc_journal_prealloc=1
```

**(c) `bench-infra/ansible/roles/run/tasks/main.yml`** — export it next to the existing
`UC_PIPELINE_DEPTH` line (~main.yml:87), before the `setsid ... uc-node-launch`:

```yaml
        export UC_JOURNAL_PREALLOC={{ uc_journal_prealloc }}
```

Commit (a)+(b)+(c) together: `feat(bench)+feat(uc_node): UC_JOURNAL_PREALLOC env toggle for task36 A/B`.
Run `cargo test -p uc_node` (the new unit test + the lincheck/hard-crash gates already cover the
flag-on path). Working tree must be clean before provisioning.

---

## Prerequisites (same fleet as the pipeline A/B)

- `bench-infra/.env` with AWS creds; `bench-infra/terraform.tfvars` from `example.aws.tfvars`
  (`cloud=aws`, `instance_type=c7i.4xlarge`, 3-node placement-group). Fill `ssh_public_key`,
  `ssh_private_key_file`, `allow_ssh_cidr`. Override `netem_iface` to the real NIC (`enp39s0` on
  c7i — check `ip link`).
- Terraform state empty (`make -C bench-infra destroy` if needed).
- **CRITICAL config for this A/B:**
  - **Consistent durability** — pass `-e durability=consistent` (anything ≠ `none` → `UC_DURABILITY=consistent`). Eventual mode measures nothing here.
  - **Journal on real disk** — `uc-node-launch --data-dir /opt/bench/uc-data` is the instance NVMe (the journal lives here; `/dev/shm/uc-nodeN` is only the shmem cnc IPC). Confirm `/opt/bench` is ext4 on the NVMe, not tmpfs (`df -T /opt/bench` on a host).
  - **Low inflight** — use `-e inflight=8` (or sweep low) so group commit does not amortize the per-commit barrier away. The default 128 will hide the effect.

---

## Passes — INTERLEAVED A/B/A/B (control for host drift)

Do **not** run a single A then a single B. On this project the journal flake + microbench both
showed real host-load drift between time windows; interleave and average, as the fdatasync A/B did
(task13 §16, B,A,B,A). Each pass re-runs the `run` role (kills + restarts the cluster with a clean
election — no state leak between passes).

```bash
# From repo root — provision once:
make -C bench-infra up-uc

# Then 4 interleaved passes against the SAME fleet. tag = a1,b1,a2,b2.
cd bench-infra
for pass in a1:0 b1:1 a2:0 b2:1; do
  tag=${pass%:*}; prealloc=${pass#*:}
  ansible-playbook ansible/bench.yml \
    -e durability=consistent -e inflight=8 \
    -e uc_journal_prealloc=$prealloc
  # collect role writes bench-out/dist/<TIMESTAMP>/node0/uc_sweep.csv
  cp "../bench-out/dist/$(ls -t ../bench-out/dist | head -1)/node0/uc_sweep.csv" \
     "../bench-out/ab-prealloc-$tag.csv"
done
```

**Primary metric (`submitted→persisted`):** run with the `profile/raftcore-stats` build so node0
emits `RAFT_RUNTIME_STATS` (the leader's submit→journal-persist segment). Scrape node0 stderr
(`/opt/bench/uc-node.out`) per pass and record the `submitted_to_persisted` p50/p99. If that
instrument branch is not yet merged, build the fleet from it for this run (note it in the result).

---

## Comparing & decision rule

Per rung, average the two A passes and the two B passes, then compare:

```bash
# end-to-end secondary view (uc_sweep.csv cols: 7 achieved, 8 p50_ns, 9 p99_ns):
for t in a1 b1 a2 b2; do echo "== $t =="; awk -F, 'NR>1{print $6,$7,$8,$9}' bench-out/ab-prealloc-$t.csv; done
```

- **PRIMARY:** `submitted→persisted` p50/p99 — B (on) vs A (off). This is where the win, if real on
  prod NVMe, appears. Target: B materially below A (toward microbench −50..65%).
- **SECONDARY:** end-to-end `p50_ns`/`p99_ns` and `achieved_rate` — expected within noise (NULL),
  same reason as fdatasync. A regression here would be a blocker.

**Flip the default to ON** (set `preallocate_segments: true` in `log_storage.rs`, and/or
`uc_journal_prealloc: 1`) **only if:** `submitted→persisted` p99 improves materially AND end-to-end
p50/p99 + throughput show no regression. Record the numbers in `ultima_db/docs/tasks/task36` §7 and
in `task13` (new sub-section, alongside the fdatasync NULL result). If `submitted→persisted` is also
NULL on prod NVMe (PLP cache makes jbd2 cheap), keep the flag off and document that preallocation is
a dev-ext4-only win not worth the default.

---

## Tear down

```bash
make -C bench-infra destroy   # always, to stop charges
```

## Notes

- `dist_3node` `--config` exercises the full leader→2-followers→ack quorum path.
- The journal win needs Consistent + low inflight + real NVMe simultaneously; miss any one and the
  A/B reads NULL for an uninteresting reason. The "Read this first" section is not optional.
- Keep the `profile/raftcore-stats` instrument OUT of the committed default (it's a measurement
  branch); build the fleet from it only for this run.
