# netfault nightly scenarios — Implementation Plan (plan 1 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put three real UC node processes on one host under one-kind, one-link, one-direction network faults from `testing/netfault`, under the WGL linearizability checker, as a feature-gated `uc_crashtest` suite that `nightly.yml` runs.

**Architecture:** First the tool gains a direction selector (`--out`/`--in`) in its BPF rule key and CLI, shipped as its own PR. Then a new test file `testing/uc_crashtest/tests/netfault.rs` reuses the multi-process helpers in `tests/common/mod.rs` (three nodes on `127.0.0.2/3/4`, the shmem worker workload, `assert_linearizable`), drives the CLI through a `Netfault` guard that detaches on drop, and asserts for every scenario that the rule fired, the history is linearizable and the stated liveness held. A new nightly job builds the BPF object and runs the suite with `--test-threads=1`, cleartext and crypto-on.

**Tech Stack:** C (BPF, clang, libbpf headers), bash (CLI + self-test), Rust (test suite over `uc_crashtest`, `uc_lincheck`, `uc_log::cnc`), GitHub Actions. Plan 2 (the fleet measurement) follows once this lands and is written against the helpers this plan produces.

**Spec:** `docs/superpowers/specs/2026-11-03-uc2-netfault-scenarios-design.md` (§3, §4a, §4, §5, §7, §8).

## Global Constraints

- Every nightly-suite rule is `--out` and names a `--peer` (spec §3): on `lo` a datagram passes both hooks.
- The suite never skips: a missing BPF object, a failed `sudo -n`, or a failed attach **fails by name** at the first scenario (spec §2 anti-goals, §5).
- Scenarios never run concurrently: `--test-threads=1` in the nightly command AND a process-wide `static NETFAULT_LOCK: Mutex<()>` taken by every scenario (spec §3).
- A scenario whose rule counted nothing fails (spec §2): assert `dropped > 0` / `delayed > 0` on the named rule **before** detaching.
- Workload shape is the existing one: 3 workers, 15 ms throttle, the 15 s per-op deadline inside `worker_multi` (spec §4 common shape); fault window 5 s, heal window 5 s.
- Liveness bounds (spec §4): a serving leader within 10 s of a kill; F1's `durable` reaches L's `commit` (as of heal) within 10 s of `detach`.
- `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and the MSRV gate `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv-netfault cargo +1.89.0 clippy -p uc_crashtest --all-targets --features hard-crash-tests,netfault-tests --locked -- -D warnings` pass before every push. No `Co-Authored-By` trailers.
- Execution environment for anything that attaches: this session has no sudo. The spike and the suite run inside a **privileged container on the rootful docker daemon** (`sg docker -c 'DOCKER_CONTEXT=default …'`), where the test process is root. The `Netfault` guard therefore prefixes `sudo -n` only when `geteuid() != 0`.

## Review Focus

1. A rule installed before the leader is known names the wrong node: the suite must install rules only after `await_single_leader_multi` returns, and name nodes by role (Task 3's `Roles` helper; pinned by Scenario A asserting the NAK rule's `dropped > 0`, which is impossible if F1 was mis-identified).
2. A packet on `lo` matched twice: pinned by Task 2's spike (a `--out --peer` block cuts exactly one direction, counted once per datagram) and by Task 1's self-test case for `--out` leaving the reverse direction untouched.
3. `detach` not reached on a panic leaves `lo` with `fq` and stale rules for the next scenario: pinned by the `Netfault` guard's `Drop` (Task 3) and by running two scenarios back to back with `--test-threads=1` (Task 6).
4. Scenario B's kill landing before F1 has cast a vote in the new term, so the vote rule counts nothing and the scenario fails for the wrong reason: pinned by asserting `dropped > 0` only after the election, with the kill issued at least 300 ms after the rule (longer than the 150–300 ms election timeout) — Task 4.
5. Crypto-on: Scenario D's `retransmit` frontier depends on subtracting the sealed overhead; a mistake there would read every DATA as a retransmit and the block would halt replication to F1 entirely. Pinned by the crypto leg of the nightly job (Task 5) running Scenario D and asserting commit still advanced (L+F2) **and** the retransmit rule's `dropped` is below the DATA rule's sent count proxy (`dropped(retransmit) < 10 × dropped(data loss)` is not computable; instead assert F1 catches up after heal, which cannot happen if every DATA was a "repair").

---

### Task 1: Direction selector in netfault (`--out` / `--in`)

**Files:**
- Modify: `testing/netfault/bpf/netfault.bpf.c` (`struct rule_key`, `lookup`, `find`, `match`)
- Modify: `testing/netfault/bin/netfault` (option parsing, `selector`, `rule_key`, `stats`, header comment)
- Modify: `testing/netfault/test/netfault-rules.sh` (new rules + sends + expects)
- Modify: `testing/netfault/README.md` (selector list, precedence sentence, lookup count 18 → 36)

**Interfaces:**
- Produces: CLI flags `--out` and `--in` on `rule`/`block`/`remove` (mutually exclusive; neither = both directions). `stats` line format becomes `<port> <kind> [peer=A.B.C.D] [out|in] loss=…` — the selector prefix (everything before ` loss=`) is what Task 3's parser keys on.
- BPF: `struct rule_key { __u16 port; __u16 kind; __u32 peer; __u8 dir; __u8 pad[3]; }` (12 bytes; `dir` 0 any, 1 out, 2 in). CLI key bytes: port BE u16, kind LE u16, peer 4 bytes, dir 1 byte, 3 zero bytes.

- [ ] **Step 1: Write the failing self-test cases** in `test/netfault-rules.sh`. Add to the rules block:
  ```bash
  # Direction: an --out rule leaves what comes back alone, an --in rule what goes out.
  fault "${SENDER}" block --out --kind data 9140
  fault "${SENDER}" block --in --kind data 9141
  fault "${RECEIVER}" rule --out --duplicate 100 9134     # the receiver's replies, not what it receives
  ```
  Add sends: 5 data datagrams to `10.99.0.1:9140` and 5 to `10.99.0.1:9141` (both only ever leave the sender, so only the `--out` rule can count), plus 10 lines to `$RX:9134` where the receiver's `nc` echoes nothing back (so the receiver's `--out` rule must count 0 while an un-directed duplicate rule on 9133 still counts 10). Add expects:
  ```bash
  echo "-- direction"
  expect "${S}" "9140 data out" dropped 5
  expect "${S}" "9141 data in" dropped 0
  expect "${LOG_DIR}/receiver.txt" "9134 any out" duplicated 0
  ```
  and a refusal `"block --out --in 1"` to the refusal list.
- [ ] **Step 2: Run the self-test to verify it fails**: `cd testing/netfault && sg docker -c 'DOCKER_CONTEXT=default test/netfault-rules.sh'`. Expected: the rules block fails at `block --out` with `unknown option: --out`.
- [ ] **Step 3: BPF.** Add `dir` to `rule_key`; `lookup(peer, port, kind, dir)`; in `find`, for each of retransmit/kind/any try `dir = egress ? 1 : 2` then `dir = 0`; thread `egress` through `find_any`/`find`. Keep `__always_inline`. Update the header comment's lookup count to 36.
- [ ] **Step 4: CLI.** Parse `--out`/`--in` as flags (no value) into `dir=1|2`, refuse both together with `die "--out and --in exclude each other"`. `selector` sets `DIR`; `rule_key` appends `$(printf '%02x 00 00 00' "${DIR}")`. `stats`: the key is now 12 bytes — value offsets shift by 4 (loss at 12, duplicate 16, delay 20, jitter 24, dropped 28, duplicated 36, delayed 44); print ` out`/` in` after the peer when `b[8]` is 1/2. Update the usage comment.
- [ ] **Step 5: `make -C testing/netfault`** compiles clean; run the self-test. Expected: `NETFAULT RULES TEST: PASS` with the three new expects and the new refusal `ok`.
- [ ] **Step 6: README** — add `--out`/`--in` to the selector list, insert "direction" into the precedence sentence after the port, change 18 to 36, add `block --out --kind vote 9001` as an example. `cargo test -p uc_crashtest --test netfault_pins` still passes (it pins kinds and the header only).
- [ ] **Step 7: Commit** `netfault: --out/--in direction selector in the rule key`, push branch `netfault/direction`, open the PR, watch every job (the `netfault` job runs the self-test on a runner), merge when green. Then rebase `design/netfault-scenarios` onto main.

### Task 2: Loopback spike (throwaway)

**Files:**
- Create (scratch only, not committed): `$HOME/scratch/netfault-spike/spike.sh`

**Interfaces:**
- Produces: observations recorded as a short paragraph in the plan's Task 2 (edit this file): (a) a `block --out --kind any --peer 127.0.0.3 <port A>` on `lo` drops A→B and leaves B→A, counted once per datagram; (b) `rule --out --delay 200 --peer …` on `lo` delays arrival by ≈200 ms; (c) a datagram's `retransmit` classification on `lo` at ingress is harmless to egress classification (send DATA with ascending positions through an `--out --kind retransmit` block: `dropped` stays 0).

- [ ] **Step 1: Write `spike.sh`**: inside `docker run --rm --privileged -v <checkout>/testing/netfault:/nf:ro netfault:local bash`, with `NETFAULT_DEV=lo`: start `nc -u -l 127.0.0.3 <B>` and `nc -u -l 127.0.0.2 <A>` writers to files, send 5 datagrams A→B and 5 B→A with `/dev/udp/`, install the block, send 5 more each way, read `stats`, detach; then the delay check with `/proc/uptime` stamps as the self-test does; then the retransmit check with the self-test's `dgram` helper (copy the function).
- [ ] **Step 2: Run it** via `sg docker -c 'DOCKER_CONTEXT=default bash $HOME/scratch/netfault-spike/spike.sh'`. Expected: block `dropped=5`, B→A file has 10 lines, A→B file has 5; delay ≈ 0.2 s; retransmit `dropped=0`.
- [ ] **Step 3: Record** the three observations in this task; if any fails, stop and amend the spec §3 before Task 3.

### Task 3: Suite scaffolding + Scenario A (NAK loss on one link)

**Files:**
- Modify: `testing/uc_crashtest/Cargo.toml` — add `netfault-tests = ["hard-crash-tests"]` under `[features]`.
- Modify: `testing/uc_crashtest/tests/common/mod.rs` — add `free_addr_on`, `Roles`, `cnc_positions`, and the `Netfault` guard (new section at the end, `// ---- netfault (spec 2026-11-03)`).
- Create: `testing/uc_crashtest/tests/netfault.rs` with `#![cfg(feature = "netfault-tests")]`.

**Interfaces:**
- Produces, in `common`:
  - `pub fn free_addr_on(ip: Ipv4Addr) -> SocketAddr` — binds `ip:0`, returns the address, drops the socket.
  - `pub struct Roles { pub leader: usize, pub f1: usize, pub f2: usize }` and `pub fn roles(dirs: &[PathBuf], secs: u64) -> Roles` — `await_single_leader_multi` then the other two in index order.
  - `pub fn cnc_positions(dir: &Path) -> (u64, u64)` — `(commit, durable)` from `open_cnc(dir).counters()` via `load_acquire()`; panics if the page cannot be opened.
  - `pub struct Netfault { dev: String, obj: PathBuf }` with `pub fn attach(dev: &str) -> Netfault` (fails by name: `"netfault: no BPF object at {path}; run make -C testing/netfault or set NETFAULT_OBJ"`, `"netfault: attach on {dev} failed: {stderr}"`), `pub fn run(&self, args: &[&str])` (runs `[sudo -n] env NETFAULT_DEV=<dev> NETFAULT_OBJ=<obj> <cli> <args>`, panics with the CLI's stderr on non-zero), `pub fn stats(&self) -> BTreeMap<String, BTreeMap<String, u64>>` (selector prefix → counters), `pub fn counter(&self, selector: &str, name: &str) -> u64` (panics `"netfault: no rule {selector} in stats"` if absent), and `impl Drop` running `detach` (ignoring errors).
  - `pub static NETFAULT_LOCK: Mutex<()>`.
  - The CLI path: `NETFAULT_OBJ` env or `concat!(env!("CARGO_MANIFEST_DIR"), "/../netfault/build/netfault.bpf.o")`; the script at `…/../netfault/bin/netfault`.
- Produces, in `netfault.rs`: `struct Cluster { tmp, dirs: Arc<Vec<PathBuf>>, addrs: Vec<SocketAddr>, members_str, node_procs, svc_procs, crypto, roles }` with `fn boot(seed) -> Cluster` (the exact spawn sequence of `leader_node_sigkill_recovery_multi_once` with `free_addr_on(127.0.0.{2,3,4})`), `fn port(&self, idx) -> u16`, `fn ip(&self, idx) -> String`, and `fn workload(&self, seed) -> Workload { history, stop, last_seen, handles }` wrapping `warmup_write_multi` + `spawn_workers_multi(…, Duration::from_millis(15), 3)`.

- [ ] **Step 1: Write the failing test** `nak_loss_on_one_link` in `netfault.rs`:
  ```rust
  #[test]
  fn nak_loss_on_one_link() {
      let _g = NETFAULT_LOCK.lock().unwrap();
      shorten_client_timeout();
      let c = Cluster::boot(0x4E41_4B00);
      let nf = Netfault::attach("lo");
      let w = c.workload(0x4E41_4B01);
      let (l, f1) = (c.roles.leader, c.roles.f1);
      let nak = format!("{} nak peer={} out", c.port(f1), c.ip(l));
      let data = format!("{} data peer={} out", c.port(l), c.ip(f1));
      nf.run(&["block", "--out", "--kind", "nak", "--peer", &c.ip(l), &c.port(f1).to_string()]);
      nf.run(&["rule", "--out", "--kind", "data", "--loss", "10", "--peer", &c.ip(f1), &c.port(l).to_string()]);
      let ok_at_fault = History::ok_count(&w.history.snapshot());
      std::thread::sleep(Duration::from_secs(5));
      let ok_in_fault = History::ok_count(&w.history.snapshot());
      let (commit_at_heal, _) = cnc_positions(&c.dirs[l]);
      assert!(nf.counter(&nak, "dropped") > 0, "the NAK rule counted nothing: {nak}");
      assert!(nf.counter(&data, "dropped") > 0, "the DATA loss rule counted nothing: {data}");
      drop(nf); // detach = heal
      assert!(ok_in_fault > ok_at_fault, "commit did not advance during the fault (L+F2 are a quorum)");
      wait_until(Duration::from_secs(10), || cnc_positions(&c.dirs[f1]).1 >= commit_at_heal,
                 "F1 durable did not reach L's commit-at-heal within 10 s");
      std::thread::sleep(Duration::from_secs(5));
      let entries = w.finish();
      assert_linearizable(&entries, "netfault_nak_loss", "run0");
  }
  ```
  with `fn wait_until(timeout, pred, msg)` as a small local helper (poll every 50 ms, panic with `msg` on timeout) and `Workload::finish(self) -> Vec<Entry>` (stop, `join_workers`, `into_entries`).
- [ ] **Step 2: Run to verify it fails**: `cargo test -p uc_crashtest --features netfault-tests --test netfault -- --test-threads=1`. Expected: compile error (`Netfault`, `Cluster` undefined).
- [ ] **Step 3: Implement the `common` additions** (signatures above). `stats` parsing: split each line at `" loss="`; the left is the selector key; the right is whitespace-separated `name=value` pairs, `%`/`ms` suffixes stripped, parsed as `u64`.
- [ ] **Step 4: Implement `Cluster` and `Workload`** in `netfault.rs` (copy the boot sequence; `Cluster::drop` is the existing `Reap` semantics via the `Option<Reap>` vectors).
- [ ] **Step 5: Run the test** inside the privileged container (Task 6's `run-in-container.sh`, written now in its minimal form: `docker run --rm --privileged -v <checkout>:/src -v <target dir>:/target -w /src rust:1.96-bookworm` with `apt-get install -y bpftool jq iproute2 clang libbpf-dev linux-libc-dev`, `make -C testing/netfault`, then the cargo command with `CARGO_TARGET_DIR=/target`). Expected: PASS, with the eprintln line showing non-zero `dropped` on both rules.
- [ ] **Step 6: `cargo fmt --all`, clippy with the feature, commit** `crashtest: netfault suite scaffolding + Scenario A (NAK loss on one link)`.

### Task 4: Scenarios B, C, D

**Files:**
- Modify: `testing/uc_crashtest/tests/netfault.rs`

**Interfaces:**
- Consumes: Task 3's `Cluster`, `Workload`, `Netfault`, `Roles`, `cnc_positions`, `wait_until`.
- Produces: `Cluster::kill_node_and_service(&mut self, idx)` and `Cluster::restart(&mut self, idx)` (the SIGKILL + `spawn_node_multi` + `wait_for_fresh_cnc_instance` + `spawn_service` sequence from `leader_node_sigkill_recovery_multi_once`).

- [ ] **Step 1: Write `votes_silenced`** (spec §4 B): rules `block --out --kind vote --peer <F2 ip> <F1 port>` and the same with `--peer <L ip>`; sleep 300 ms; `ok_before_kill`; `kill_node_and_service(l)`; `let leader1 = await_single_leader_multi(&dirs, 10)`; assert `leader1 != l`; `eprintln!` the winner and assert `leader1 == f1` with the message `"F2 won although F1's votes were blocked — a vote reached F2 by a path the rule does not cover"`; assert `nf.counter(&vote_f2, "dropped") > 0`; `restart(l)`; `await_single_leader_multi(&dirs, 20)`; sleep 2 s; `ok_after_recovery > ok_before_kill`; `drop(nf)`; finish; `assert_linearizable(…, "netfault_votes_silenced", "run0")`.
- [ ] **Step 2: Write `one_direction_slow`** (C): `rule --out --delay 20 --jitter 30 --peer <L ip> <F1 port>`; 5 s; assert `delayed > 0`; assert `ok_in_fault > ok_at_fault`; additionally count linearizable reads: `w.history.snapshot()` entries with `Op::Read` and `Outcome::Ok` during the window > 0; heal; 5 s; linearizable.
- [ ] **Step 3: Write `repairs_dropped`** (D): `rule --out --kind data --loss 10 --peer <F1 ip> <L port>` and `block --out --kind retransmit --peer <F1 ip> <L port>`; 5 s; assert `dropped(retransmit) > 0` and `dropped(data) > 0`; `commit_at_heal`; heal; `wait_until` F1 durable ≥ `commit_at_heal` (10 s); 5 s; linearizable.
- [ ] **Step 4: Run all four** in the container with `--test-threads=1`. Expected: 4 passed. If B's winner is F2, record it in the test's eprintln and in `docs/VERIFICATION.md` (Task 5) as a finding, and investigate which kind carried the vote before changing anything.
- [ ] **Step 5: fmt, clippy, commit** `crashtest: netfault Scenarios B (votes silenced), C (slow direction), D (repairs dropped)`.

### Task 5: Nightly job + docs

**Files:**
- Modify: `.github/workflows/nightly.yml` — new jobs `netfault-scenarios` and `netfault-scenarios-crypto` after `crashtest-crypto`.
- Modify: `docs/VERIFICATION.md` §2 "The kernel-level fault layer" — replace the "What is **not** done" paragraph with the four-scenario table and the nightly command.
- Modify: `docs/BACKLOG.md` §5 — the network-fault bullet now says the nightly suite exists and the fleet measurement is plan 2.
- Modify: `testing/README.md` — the netfault row mentions the suite; `testing/netfault/README.md` § Tests — add the suite.

- [ ] **Step 1: Write the jobs.** Steps: checkout, rust-cache, `sudo apt-get update && sudo apt-get install -y --no-install-recommends clang libbpf-dev linux-libc-dev jq iproute2`, a `bpftool` step that tries `sudo apt-get install -y linux-tools-common linux-tools-$(uname -r)` and falls back to downloading the pinned static `bpftool` release (`libbpf/bpftool` v7.5.0 `bpftool-v7.5.0-amd64.tar.gz`, sha256 checked, installed to `/usr/local/sbin`), `bpftool version`, `make -C testing/netfault`, then `cargo test -p uc_crashtest --features netfault-tests --test netfault -- --test-threads=1 --nocapture`. The crypto job sets `UC2_CRYPTO: '1'`. `timeout-minutes: 30`.
- [ ] **Step 2: Prove the job on a draft PR**: push, `gh workflow run nightly.yml --ref design/netfault-scenarios`, watch the two new jobs. Expected: both green. If `bpftool` is missing after both install paths, fix the step; the suite's own by-name refusal is the failure line to read.
- [ ] **Step 3: Docs** as listed; run `python3 scripts/check_doc_links.py` (0 errors).
- [ ] **Step 4: Commit** `nightly: netfault scenarios (cleartext + crypto); docs record the suite`.

### Task 6: Full local run, both legs, then PR

**Files:**
- Create: `testing/netfault/scripts/run-suite-in-container.sh` (committed: the documented way to run the suite without host root).

**Interfaces:**
- Produces: `run-suite-in-container.sh [--crypto]` — the Task 3 Step 5 container invocation, parameterised by checkout path and target dir, honouring `DOCKER_CONTEXT`.

- [ ] **Step 1: Write the script** and reference it from `testing/netfault/README.md` § Tests.
- [ ] **Step 2: Run both legs** back to back (`--test-threads=1`, cleartext then `--crypto`). Expected: 8 passes; `lo` clean after (`tc qdisc show dev lo` inside a fresh container shows no `fq`/`clsact` — trivially, since each container has its own `lo`; the guard's `Drop` is instead proved by running the suite twice in one container).
- [ ] **Step 3: fmt, workspace clippy, MSRV clippy, `check_doc_links.py`; commit** `netfault: run-suite-in-container.sh`; push; open the PR with the spec and plan linked; watch every job; merge when green; delete branches, remove the worktree and private target dirs; update memory.
