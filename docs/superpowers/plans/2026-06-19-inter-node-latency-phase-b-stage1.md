# Inter-node Latency Phase B — Stage 1 (SO_BUSY_POLL + V2 commit A/B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans for **Tasks 1–4 only** (local, buildable). Steps use checkbox (`- [ ]`) syntax. **Tasks 5–8 are a BILLABLE cloud run-book — do NOT auto-execute them; they require explicit user go-ahead and a live AWS fleet, and are run interactively, not by autonomous subagents.**

**Goal:** Measure, cross-host on a real NIC, whether `SO_BUSY_POLL` kernel-UDP closes a meaningful fraction of the UC-vs-Aeron latency gap, and what the V2 pipelined `stream_append` actually buys on real RTT — then decide (Gate-B1) whether the heavy AF_XDP work is warranted.

**Architecture:** Add a `busypoll-udp` echo rung to the existing `internode-rpc-bench` ladder (mirrors the Phase A `busyspin-udp` rung, but a blocking recv with `SO_BUSY_POLL` set via `libc::setsockopt`). Fix the task16 bench-infra rough edges (os_tune busy-poll sysctls; aeron deploy-dir chown; aeron result fetch). Thread `UC_PIPELINE_DEPTH` through the existing cross-host `run` role so the depth=1-vs-8 commit A/B can run against a real 3-node fleet. Build/validate everything locally; run the measurements on a 3× c7i.4xlarge fleet on go-ahead.

**Tech Stack:** Rust (libc setsockopt), tokio, `core_affinity`, Ansible (`bench-infra`), `internode-rpc-bench` + `commit-path-load` + `aeron-echo-baseline.sh`, AWS (Terraform/Ansible via bench-infra).

## Global Constraints

- openraft pinned `0.10.0-alpha.21` (do not bump).
- **Measure-and-decide only — NO production `ClusterTransport` is built in this plan.** The `busypoll-udp` rung is throwaway bench harness code in `bench_support.rs`.
- Bench rung code lives in `uc_node/src/network/bench_support.rs` (already an unconditional `pub mod`); `clippy -p uc_node -- -D warnings` must stay clean; must build default AND `--features fault-injection`.
- Commit only the files each task names; never `git add -A` (the pre-existing dirty `uc_autobench/tasks/netping/results.tsv` must stay untouched).
- Cloud stages bill (~$2.14/hr, 3× c7i.4xlarge); destroy the fleet immediately after the run. `ttl_hours` is advisory only.
- All datapaths measured on the SAME fleet with the SAME pinning (rungs core-pin via `core_affinity`; Aeron via its own core map) — no tuned-vs-untuned comparison.
- Results are recorded as **Part C of `docs/tasks/task17_inter_node_latency.md`**.
- QUIC stays the default transport.

---

## Task 1: `busypoll-udp` echo rung

**Files:**
- Modify: `uc_node/Cargo.toml` (add `libc = "0.2"` under `[dependencies]`)
- Modify: `uc_node/src/network/bench_support.rs` (helper + enum variants + constructor + dispatch arm)
- Modify: `uc_autobench/src/bin/internode-rpc-bench.rs` (transport dispatch — the same ~5 sites the Phase A rungs use)
- Test: inline `#[tokio::test]` in `bench_support.rs`

**Interfaces:**
- Consumes: the existing `EchoClient { async fn rpc(&self, body: Bytes) -> Result<Bytes, NetworkError> }` / `EchoServer { local_addr(); async fn shutdown(self) }` and the `EchoClientInner`/`EchoServerInner` enums + the `BareBusyspinUdp` rung pattern (dedicated `core_affinity`-pinned `std::thread` + mpsc-request/oneshot-reply).
- Produces: `pub async fn busypoll_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError>` and transport string `"busypoll-udp"` (system label `"busypoll-udp-rpc"`).

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn busypoll_udp_echo_roundtrips() {
    let (client, server) = busypoll_udp_echo_pair().await.unwrap();
    let resp = client.rpc(bytes::Bytes::from_static(b"busypoll-payload")).await.unwrap();
    assert_eq!(&resp[..], b"busypoll-payload");
    server.shutdown().await;
}
```

- [ ] **Step 2: Run it, expect FAIL** — `cargo test -p uc_node busypoll_udp_echo_roundtrips` → `busypoll_udp_echo_pair` not found.

- [ ] **Step 3: Add `libc` dep** to `uc_node/Cargo.toml` `[dependencies]`: `libc = "0.2"`.

- [ ] **Step 4: Add the setsockopt helper** in `bench_support.rs` (near the busyspin rung). `SO_BUSY_POLL` is constant `46`; the value is the busy-poll budget in microseconds:

```rust
/// Set SO_BUSY_POLL (usecs) on a std UDP socket so blocking recv busy-polls
/// the NAPI ring before sleeping. Bench-only; Linux. No-op-safe if unsupported.
fn set_so_busy_poll(sock: &std::net::UdpSocket, usecs: u32) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    const SO_BUSY_POLL: libc::c_int = 46;
    let v = usecs as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            SO_BUSY_POLL,
            &v as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == -1 { Err(std::io::Error::last_os_error()) } else { Ok(()) }
}
```

- [ ] **Step 5: Implement the rung** — mirror `bare_busyspin_udp_echo_pair` EXACTLY, with two differences: (a) sockets are **blocking** (do NOT call `set_nonblocking(true)`); (b) call `set_so_busy_poll(&sock, 50)` on both the server and client sockets right after bind/connect; the recv loops use a plain **blocking** `recv_from`/`recv` (no `spin_loop()`). Add `EchoClientInner::BusyPollUdp { tx, thread }` and `EchoServerInner::BusyPollUdp { handle, local }` variants, the `EchoClient::rpc` arm (mpsc→thread→oneshot, identical to busyspin), and `EchoServer::shutdown`/`local_addr` arms. Pin server thread to `core_affinity` id[0], client to id[1] (fall back unpinned if <2 cores), same as busyspin. If `set_so_busy_poll` returns `Err` (kernel/NIC without support), log once to stderr and continue (the rung still works, just without the busy-poll benefit) — do NOT fail the pair.

- [ ] **Step 6: Run the test, expect PASS** — `cargo test -p uc_node busypoll_udp_echo_roundtrips`. (On loopback there's no NAPI ring to poll, so no latency benefit — this only proves correctness/round-trip.)

- [ ] **Step 7: Wire bench dispatch** — add `"busypoll-udp"` at the same sites the Phase A rungs use in `internode-rpc-bench.rs` (the `value_parser` list, the `role=server`/`role=client` pair-path arms, and the `fanout_system`/`ping_system` label arms), system label `"busypoll-udp-rpc"`, mapping to `busypoll_udp_echo_pair()`.

- [ ] **Step 8: Build both configs + clippy + smoke** —
  `cargo build -p uc_node && cargo build -p uc_node --features fault-injection && cargo clippy -p uc_node -- -D warnings` (clean), then
  `cargo run -p uc_autobench --bin internode-rpc-bench --release -- --role both --transport busypoll-udp --mode ping --duration 5 --payload 64` → one CSV row `system=busypoll-udp-rpc`, count>0.

- [ ] **Step 9: Commit**

```bash
git add uc_node/Cargo.toml uc_node/src/network/bench_support.rs uc_autobench/src/bin/internode-rpc-bench.rs
git commit -m "feat(bench): SO_BUSY_POLL UDP echo rung (kernel busy-poll, real-NIC datapath)"
```

## Task 2: os_tune busy-poll sysctls + Aeron deploy-dir chown

**Files:**
- Modify: `bench-infra/ansible/roles/os_tune/tasks/main.yml` (add 2 sysctls to the existing sysctl loop ~lines 24–34)
- Modify: `bench-infra/ansible/roles/build_aeron/tasks/main.yml` (add a chown task after the extract, ~after line 37)
- Modify: `bench-infra/ansible/roles/run/tasks/main.yml` (set `owner` on the results dir, ~lines 3–10)

**Interfaces:** none (infra config). Run user is `{{ ansible_user_id }}` (AWS = `ubuntu`).

- [ ] **Step 1: Add busy-poll sysctls.** In `os_tune/tasks/main.yml`, add to the sysctl list:
```yaml
  - { k: "net.core.busy_poll", v: "50" }
  - { k: "net.core.busy_read", v: "50" }
```
(These are the system defaults; the per-socket `SO_BUSY_POLL` from Task 1 is what actually drives it, but set the sysctls so the NIC/NAPI busy-poll path is enabled fleet-wide.)

- [ ] **Step 2: Add the Aeron chown.** In `build_aeron/tasks/main.yml`, after the deploy-tar extract task, add:
```yaml
- name: Make aeron-deploy writable by the run user (non-root JVM writes results here)
  ansible.builtin.file:
    path: "{{ aeron_deploy_dir }}"
    owner: "{{ ansible_user_id }}"
    group: "{{ ansible_user_gid }}"
    recurse: true
  when: ansible_user_id != 'root'
```

- [ ] **Step 3: Fix results-dir ownership.** In `run/tasks/main.yml`, on the directory-creation `ansible.builtin.file` task for `{{ remote_home }}/results` (and the cluster dir), add `owner: "{{ ansible_user_id }}"` so the JVM/UC processes can write.

- [ ] **Step 4: Validate (no cluster needed).** Run:
  `cd bench-infra/ansible && ansible-playbook --syntax-check provision.yml netping.yml` (must pass), and `yamllint roles/os_tune/tasks/main.yml roles/build_aeron/tasks/main.yml roles/run/tasks/main.yml` if `yamllint` is present (else skip with a note).
  Expected: syntax OK; the two sysctls + the chown + owner appear in the rendered files.

- [ ] **Step 5: Commit**

```bash
git add bench-infra/ansible/roles/os_tune/tasks/main.yml bench-infra/ansible/roles/build_aeron/tasks/main.yml bench-infra/ansible/roles/run/tasks/main.yml
git commit -m "fix(bench-infra): busy-poll sysctls + aeron-deploy chown + results-dir owner (Phase B B0)"
```

## Task 3: Aeron result fetch-back fix

**Files:**
- Modify: `uc_autobench/scripts/aeron-echo-baseline.sh` (add an explicit fetch-back after the orchestrator run)

**Interfaces:** Consumes `NODE0_IP`, `SSH_USER`, `SSH_KEY`, `OUT_DIR` (already defined in the script).

- [ ] **Step 1: Add a fetch step.** The orchestrator runs ON node0 and its `--download-dir` is a control-box path that does not exist on node0 (the task16 AWS run failed with `mkdir: cannot create directory '/home/claude'`). After the `ssh ... "${REMOTE_CMD}"` invocation (the orchestrator run), add an explicit scp-back of the results that landed on node0, into the control box's `${OUT_DIR}`:
```bash
# The orchestrator writes HDR results under node0:<benchmarks_path>/scripts/results.
# Its own download step targets a control-box path that doesn't exist on node0,
# so fetch the tarball back ourselves.
echo "[fetch] pulling aeron results from node0 -> ${OUT_DIR}/" >&2
mkdir -p "${OUT_DIR}"
ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new -o BatchMode=yes "${SSH_USER}@${NODE0_IP}" \
  "cd ${AERON_DEPLOY_DIR}/scripts/results 2>/dev/null && tar -czf /tmp/aeron-results.tgz . 2>/dev/null && echo ok" \
  && scp -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new "${SSH_USER}@${NODE0_IP}:/tmp/aeron-results.tgz" "${OUT_DIR}/aeron-results.tgz" \
  && tar -xzf "${OUT_DIR}/aeron-results.tgz" -C "${OUT_DIR}" \
  && echo "[fetch] aeron results in ${OUT_DIR}/" >&2 \
  || echo "[fetch] WARN: no aeron results fetched (check node0 ${AERON_DEPLOY_DIR}/scripts/results)" >&2
```
(`AERON_DEPLOY_DIR` is already defined in the script; if the results path differs on first run, adjust the `cd` path — the HDR `.hgrm` files are the target.)

- [ ] **Step 2: Validate** — `bash -n uc_autobench/scripts/aeron-echo-baseline.sh` (parses) and `shellcheck uc_autobench/scripts/aeron-echo-baseline.sh` if available (clean or only pre-existing warnings).

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/scripts/aeron-echo-baseline.sh
git commit -m "fix(netping): aeron-echo-baseline fetches HDR results back from node0 (Phase B B0)"
```

## Task 4: Thread `UC_PIPELINE_DEPTH` into the cross-host run role + depth-A/B driver

**Files:**
- Modify: `bench-infra/ansible/roles/run/tasks/main.yml` (export `UC_PIPELINE_DEPTH` into the `uc-node-launch` environment)
- Modify: `bench-infra/ansible/group_vars/all.yml` (add `uc_pipeline_depth` default = 8)
- Create: `uc_autobench/scripts/pipeline-commit-ab.md` (a short run-book documenting the depth=1-vs-8 cross-host A/B procedure — the actual run is a cloud task)

**Interfaces:** Consumes the existing cross-host launch (`run` role starts `uc-node-launch` per host with cross-host peers from the `config` role; `commit-path-load` runs on node0 against `/dev/shm/uc-node0`). `UC_PIPELINE_DEPTH` is read at node startup by `pipeline_depth()` in `uc_node` (Phase A), so it must be in each node process's env.

- [ ] **Step 1: Add the var.** In `group_vars/all.yml`, add `uc_pipeline_depth: 8` (overridable on the command line with `-e uc_pipeline_depth=1`).

- [ ] **Step 2: Export it to the nodes.** In `run/tasks/main.yml`, in the `uc-node-launch` start command (the shell task that backgrounds the node), prefix the launch with the env var so each node process inherits it:
```yaml
      UC_PIPELINE_DEPTH={{ uc_pipeline_depth }} {{ uc_target_bin }}/uc-node-launch \
        --node-id $UC_NODE_ID --listen $UC_LISTEN $UC_PEERS \
        ...
```
(Place it on the launch line so it is in the node's environment, where `pipeline_depth()` reads it.)

- [ ] **Step 3: Write the A/B run-book** `uc_autobench/scripts/pipeline-commit-ab.md`: documents that the cross-host depth A/B is two provision-and-run passes — one with `-e uc_pipeline_depth=1`, one with `=8` — each launching the 3-node cluster + `commit-path-load` (open-loop, high inflight) on node0, plus a lagging-follower catch-up variant (start the cluster, kill+restart one follower under load, observe catch-up), and that the two CSVs are compared on commit p50/p99 + achieved throughput. Include the exact `make`/`ansible-playbook` invocations and the CSV columns. (This is documentation; the run is Task 7.)

- [ ] **Step 4: Validate** — `cd bench-infra/ansible && ansible-playbook --syntax-check provision.yml` (must pass); confirm `uc_pipeline_depth` is referenced in `run/tasks/main.yml` and defined in `group_vars/all.yml`.

- [ ] **Step 5: Commit**

```bash
git add bench-infra/ansible/roles/run/tasks/main.yml bench-infra/ansible/group_vars/all.yml uc_autobench/scripts/pipeline-commit-ab.md
git commit -m "feat(bench-infra): thread UC_PIPELINE_DEPTH to nodes + cross-host commit A/B run-book (Phase B B-V2)"
```

---

# CLOUD RUN-BOOK (Tasks 5–8) — BILLABLE, EXPLICIT GO-AHEAD ONLY

> These are operational steps, not TDD. They provision real AWS hosts (~$2.14/hr). Do NOT run them autonomously. Each is a run-and-record procedure; capture the exact numbers into the memo. Destroy the fleet at the end (Task 8).

## Task 5: Provision the fleet

- [ ] Confirm `bench-infra/.env` has the scoped `uc-bench-terraform` AWS key; set `terraform.tfvars` `cloud=aws`, `region`, `instance_type` unset (use the `up-fanout` knob).
- [ ] `make -C bench-infra up-fanout FANOUT_INSTANCE_TYPE=c7i.4xlarge` (3× c7i.4xlarge, ENA, placement group). This runs `provision.yml` (os_tune w/ busy-poll sysctls + build_uc + build_aeron-with-chown + cross-host config).
- [ ] Verify: 3 nodes up, `NETEM_IFACE` auto-detects (expect `enp39s0`), `os_tune` sysctls applied (`ssh nodeX 'sysctl net.core.busy_poll'` → 50).

## Task 6: B1 — SO_BUSY_POLL datapath RTT (cross-host, node0↔node1)

- [ ] Run the netping ping sweep, NO netem, transports `udp quic busypoll-udp` (and `aeron` via `aeron-echo-baseline.sh`), 64 B single-inflight, over the private link. Capture p50/p99/p99.9 per transport.
- [ ] Record the table: UC-UDP / UC-QUIC / busypoll-udp / Aeron.

## Task 7: B-V2 — pipelined commit A/B (cross-host, 3-node)

- [ ] Pass 1: `make -C bench-infra ... -e uc_pipeline_depth=1` style run → `commit-path-load` on node0, open-loop high inflight, several rates → CSV (sequential).
- [ ] Pass 2: same with `uc_pipeline_depth=8` → CSV (pipelined).
- [ ] Catch-up variant: under sustained load, kill+restart one follower; record commit-latency recovery for depth 1 vs 8.
- [ ] Record commit p50/p99 + achieved throughput, depth 1 vs 8, steady-state and catch-up.

## Task 8: Gate-B1 decision + Part C + destroy

- [ ] **Gate-B1 (judgment):** did `busypoll-udp` close a meaningful fraction of the ~44µs UC-vs-Aeron gap toward the 47µs floor? Record the call (and thus whether AF_XDP / Phase B Stage 2 is warranted).
- [ ] Append **Part C** to `docs/tasks/task17_inter_node_latency.md`: the B1 datapath RTT table, the B-V2 depth-1-vs-8 commit numbers (steady + catch-up), the Gate-B1 decision, and the adopt/no-adopt recommendation per datapath. Commit.
- [ ] `make -C bench-infra destroy`; verify `terraform state list` empty (billing stopped).

---

## Self-Review (done at authoring)

- **Spec coverage:** B0 → Tasks 2,3 (+ provision in 5); B1 SO_BUSY_POLL rung → Task 1, measured Task 6; B-V2 → Task 4 (wiring) + Task 7 (run); Gate-B1 → Task 8; Part C deliverable → Task 8. AF_XDP (B2) is deliberately a separate plan (gated on Task 8). ✓
- **Placeholder scan:** the cloud tasks (5–8) are run-books with exact commands but not TDD code — by design (they can't be unit-tested or auto-run); they carry concrete invocations + what-to-capture, not "TBD". The one soft spot is the exact node0 aeron results path in Task 3 (`scripts/results`) — flagged as "adjust on first run" because it depends on the orchestrator's runtime layout, with the `.hgrm` files as the unambiguous target.
- **Type consistency:** `busypoll_udp_echo_pair` / `"busypoll-udp"` / `"busypoll-udp-rpc"` / `set_so_busy_poll` used consistently; `UC_PIPELINE_DEPTH` matches Phase A's `pipeline_depth()` env reader; `uc_pipeline_depth` ansible var feeds it.

## Notes / honest limitations

- **Core isolation:** true `isolcpus`/`nohz_full` needs kernel cmdline (cloud-init), out of scope here. The rungs core-pin in-process via `core_affinity` and Aeron pins via its own map — that is the practical equalizer; os_tune adds the performance governor + busy-poll sysctls. The plan does NOT claim kernel-level isolation.
- **Tasks 1–4 are fully local/testable** (the rung round-trips on loopback for correctness even though busy-poll gives no loopback benefit; the ansible changes are syntax/dry-run validated). The *value* (real-NIC numbers) comes only from the cloud run.

## Execution Handoff

Build **Tasks 1–4** via subagent-driven-development (local, no cloud). Then **Tasks 5–8** run interactively on explicit go-ahead (billable). AF_XDP (Phase B Stage 2) gets its own plan only if Gate-B1 (Task 8) warrants it.
