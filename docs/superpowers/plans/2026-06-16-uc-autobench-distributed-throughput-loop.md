# uc_autobench Distributed-Throughput Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the `uc_autobench` autoresearch loop optimize UC's 3-node distributed throughput ceiling, measured each iteration on a persistent `bench_infra` cloud fleet (UC-only, no Aeron), gated by the lincheck capstone for correctness.

**Architecture:** Add a UC-only mode to `bench_infra` (`aeron_enabled` flag), an `iterate` operation (rsync+rebuild+UC-sweep against the live fleet, emitting a fitness JSON), and a new `uc-throughput` autobench task whose per-iteration driver runs cheap local gates (compile → lincheck) before spending the ~5–10 min cloud sweep. Fitness = `max(achieved_rate)` from the UC sweep.

**Tech Stack:** Ansible + Terraform (`bench_infra`), bash, python3, Make; the existing `uc_autobench` loop conventions (task overlay + results.tsv).

**Build order:** Tasks 1–6 are offline-verifiable (ansible syntax-check, a python fitness parser TDD'd against the real parity CSV, a shell driver tested with stubbed gate commands, `make -n`). Task 7 is the live cloud end-to-end and needs a running fleet + `HCLOUD_TOKEN` + spend — handed to the operator.

**Spec:** `docs/superpowers/specs/2026-06-16-uc-autobench-distributed-throughput-loop-design.md`

**Conventions:** Infra/orchestration code — verify with `ansible-playbook --syntax-check`, `shellcheck`, `make -n`, and a python parser unit test. The control-machine tools (terraform, ansible, jq, shellcheck, python3) are installed and on `PATH`.

---

## Task 1: bench_infra UC-only mode (`aeron_enabled` flag)

**Files:**
- Modify: `bench-infra/ansible/group_vars/all.yml`
- Modify: `bench-infra/ansible/provision.yml`
- Modify: `bench-infra/ansible/roles/run/tasks/main.yml`

- [ ] **Step 1: Add the flag to group_vars**

Add this line to `bench-infra/ansible/group_vars/all.yml` under the matched-sweep section (after `aeron_ingress: udp`):
```yaml
aeron_enabled: true      # false = UC-only fleet (skip Aeron build + Aeron sweep), used by the throughput loop
```

- [ ] **Step 2: Gate the build_aeron role in provision.yml**

Replace the `roles:` list in `bench-infra/ansible/provision.yml` with:
```yaml
  roles:
    - os_tune
    - toolchains
    - role: build_aeron
      when: aeron_enabled | default(true)
    - build_uc
    - config
```

- [ ] **Step 3: Gate the Aeron tasks in the run role**

In `bench-infra/ansible/roles/run/tasks/main.yml`, add `when: aeron_enabled | default(true)` to each of the six Aeron tasks. The tasks and their new `when:` (keep all other keys unchanged):

- `Start Aeron media driver` → add `when: aeron_enabled | default(true)`
- `Start Aeron cluster node` → add `when: aeron_enabled | default(true)`
- `Wait for cluster election to settle` → this is `run_once: true`; change to:
  ```yaml
  - name: Wait for cluster election to settle
    ansible.builtin.pause:
      seconds: 20
    run_once: true
    when: aeron_enabled | default(true)
  ```
- `Copy Aeron sweep script to node0` → change its `when:` from `node_role == "node0"` to `when: aeron_enabled | default(true) and node_role == "node0"`
- `Drive Aeron rate ladder (node0)` → change its `when:` to `when: aeron_enabled | default(true) and node_role == "node0"`
- `Stop Aeron JVMs` → add `when: aeron_enabled | default(true)`

Leave the `Clean stale cluster state` task and all UC tasks unconditional.

- [ ] **Step 4: Verify syntax + that UC tasks remain unconditional**

Run:
```bash
cd bench-infra/ansible
ansible-playbook --syntax-check provision.yml bench.yml 2>&1 | sed -E 's/\x1b\[[0-9;]*m//g' | tail -3
grep -c "aeron_enabled | default(true)" roles/run/tasks/main.yml
```
Expected: syntax-check lists both playbooks with no error; the grep prints `6` (six Aeron tasks gated). The cleanup + UC tasks have no `aeron_enabled` guard.

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/group_vars/all.yml bench-infra/ansible/provision.yml bench-infra/ansible/roles/run/tasks/main.yml
git commit -m "feat(bench-infra): aeron_enabled flag — UC-only provision + run (no Aeron)"
```

---

## Task 2: UC fitness extractor (`uc_fitness.py`) — TDD against the real parity CSV

**Files:**
- Create: `bench-infra/scripts/uc_fitness.py`
- Create: `bench-infra/scripts/testdata/uc_sweep_sample.csv`
- Create: `bench-infra/scripts/test_uc_fitness.sh`

- [ ] **Step 1: Create the test fixture (the real parity-run UC sweep)**

`bench-infra/scripts/testdata/uc_sweep_sample.csv`:
```csv
system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count
uc,dist_3node,kv,64,128,100,100.0,2433023,3383295,5390335,5791743,5791743,1000
uc,dist_3node,kv,64,128,500,499.9,1927167,2865151,3989503,5611519,5611519,5000
uc,dist_3node,kv,64,128,1000,804.3,1263534079,2422210559,2434793471,2434793471,2436890623,10000
uc,dist_3node,kv,64,128,2000,781.2,7746879487,15443427327,15594422271,15602810879,15611199487,20000
uc,dist_3node,kv,64,128,5000,777.1,27195867135,53821308927,54324625407,54358179839,54358179839,50000
uc,dist_3node,kv,64,128,10000,774.6,59290681343,117977382911,118984015871,119118233599,119118233599,100000
uc,dist_3node,kv,64,128,20000,759.4,127506841599,250852933631,253134635007,253403070463,253403070463,200000
```

- [ ] **Step 2: Write the failing test**

`bench-infra/scripts/test_uc_fitness.sh`:
```bash
#!/usr/bin/env bash
# Verifies uc_fitness.py extracts the throughput ceiling + knee from a UC sweep CSV.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$(python3 "$HERE/uc_fitness.py" "$HERE/testdata/uc_sweep_sample.csv")"
echo "got: $out"
# uc_throughput_msgs = max achieved_rate = 804.3
echo "$out" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert abs(d["uc_throughput_msgs"]-804.3)<0.01, d
# knee = highest target where achieved >= 0.95*target → 500 (achieved 499.9); 1000 has achieved 804<950
assert d["knee_rate"]==500, d
# p99 at the knee rung (target=500) = 2865151 ns = 2.865 ms
assert abs(d["p99_at_knee_ms"]-2.865151)<0.001, d
print("UC_FITNESS OK")
'
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `chmod +x bench-infra/scripts/test_uc_fitness.sh && bench-infra/scripts/test_uc_fitness.sh`
Expected: FAIL — `python3: can't open file '.../uc_fitness.py'` (script not created yet).

- [ ] **Step 4: Implement `uc_fitness.py`**

`bench-infra/scripts/uc_fitness.py`:
```python
#!/usr/bin/env python3
"""Extract UC distributed-throughput fitness from a commit-path-load sweep CSV.

Prints ONE JSON line:
  {"uc_throughput_msgs": <max achieved_rate>,
   "knee_rate": <highest target_rate sustained (achieved >= 0.95*target)>,
   "p99_at_knee_ms": <p99_ns at the knee rung / 1e6>}

Fitness = uc_throughput_msgs (maximize) = UC's sustained 3-node throughput ceiling.
"""
import csv
import json
import sys


def main(path: str) -> int:
    rows = list(csv.DictReader(open(path)))
    if not rows:
        print(json.dumps({"error": "empty csv"}))
        return 1
    achieved = [(float(r["target_rate"]), float(r["achieved_rate"]),
                 float(r["p99_ns"])) for r in rows]
    ceiling = max(a for _, a, _ in achieved)
    # knee = highest target_rate the system sustained (achieved within 5% of target)
    sustained = [(t, p99) for t, a, p99 in achieved if a >= 0.95 * t]
    if sustained:
        knee, knee_p99_ns = max(sustained, key=lambda x: x[0])
    else:
        knee, knee_p99_ns = achieved[0][0], achieved[0][2]
    print(json.dumps({
        "uc_throughput_msgs": round(ceiling, 3),
        "knee_rate": round(knee, 1) if knee % 1 else int(knee),
        "p99_at_knee_ms": round(knee_p99_ns / 1e6, 6),
    }))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: uc_fitness.py <uc_sweep.csv>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `bench-infra/scripts/test_uc_fitness.sh`
Expected: prints `got: {"uc_throughput_msgs": 804.3, "knee_rate": 500, "p99_at_knee_ms": 2.865151}` then `UC_FITNESS OK`.

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
chmod +x bench-infra/scripts/uc_fitness.py
git add bench-infra/scripts/uc_fitness.py bench-infra/scripts/testdata/uc_sweep_sample.csv bench-infra/scripts/test_uc_fitness.sh
git commit -m "feat(bench-infra): uc_fitness.py — extract throughput ceiling/knee from UC sweep CSV (+test)"
```

---

## Task 3: `iterate.yml` playbook (rebuild + UC-only sweep + fetch)

**Files:**
- Create: `bench-infra/ansible/iterate.yml`
- Modify: `bench-infra/ansible/group_vars/all.yml`

- [ ] **Step 1: Add the iterate output path to group_vars**

Add to `bench-infra/ansible/group_vars/all.yml` (under the Results section, after `results_local_dir`):
```yaml
iterate_local_csv: "{{ playbook_dir }}/../../bench-out/iterate/uc_sweep.csv"  # fixed path the loop reads
```

- [ ] **Step 2: Create the iterate playbook**

`bench-infra/ansible/iterate.yml`:
```yaml
---
# One loop iteration against the LIVE UC-only fleet: re-sync + incremental rebuild,
# run the UC-only 3-node sweep, fetch node0's uc_sweep.csv to a fixed local path.
# No re-provision, no Aeron. Run with: ansible-playbook iterate.yml -e aeron_enabled=false
- name: Rebuild UC and run the UC-only sweep
  hosts: cluster
  become: true
  gather_facts: true
  vars:
    aeron_enabled: false
  pre_tasks:
    - name: Wait for SSH
      ansible.builtin.wait_for_connection:
        delay: 5
        timeout: 120
  roles:
    - build_uc
    - run
  post_tasks:
    - name: Fetch node0 UC sweep CSV to the fixed loop path
      ansible.builtin.fetch:
        src: "{{ remote_home }}/results/uc_sweep.csv"
        dest: "{{ iterate_local_csv }}"
        flat: true
      when: node_role == "node0"
```

- [ ] **Step 3: Verify syntax**

Run:
```bash
cd bench-infra/ansible
ansible-playbook --syntax-check iterate.yml 2>&1 | sed -E 's/\x1b\[[0-9;]*m//g' | tail -3
```
Expected: `playbook: iterate.yml`, no error. (It reuses the `build_uc`, `run`, `collect`-free path; `run` with `aeron_enabled=false` skips Aeron per Task 1.)

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/iterate.yml bench-infra/ansible/group_vars/all.yml
git commit -m "feat(bench-infra): iterate.yml — rebuild UC + UC-only sweep + fetch CSV (loop iteration)"
```

---

## Task 4: Makefile `up-uc` and `iterate` targets

**Files:**
- Modify: `bench-infra/Makefile`

- [ ] **Step 1: Add the two targets**

In `bench-infra/Makefile`, add `up-uc` and `iterate` to the `.PHONY` line, and add these targets after the existing `up:` target:
```makefile
up-uc: ## provision a UC-only fleet (no Aeron) for the throughput loop
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml -e aeron_enabled=false

iterate: ## one loop iteration on the live fleet: rebuild UC + UC sweep, print fitness JSON
	cd ansible && $(ANSIBLE) iterate.yml -e aeron_enabled=false
	@python3 scripts/uc_fitness.py ../bench-out/iterate/uc_sweep.csv
```

Path note: `iterate.yml` fetches to `iterate_local_csv` = repo-root `bench-out/iterate/uc_sweep.csv` (same `bench-out/` tree as `results_local_dir`, gitignored). The Makefile recipe runs with CWD `bench-infra/`, so it reads `../bench-out/iterate/uc_sweep.csv`. The fitness line is the LAST line of `make iterate` output (prefixed `@` so Make doesn't echo the command), so a caller takes the final `{...}` line as the fitness JSON.

- [ ] **Step 2: Verify the Makefile parses**

Run:
```bash
cd bench-infra
cp -n example.tfvars terraform.tfvars 2>/dev/null || true
make -n up-uc >/dev/null && echo "up-uc OK"
make -n iterate >/dev/null && echo "iterate OK"
```
Expected: `up-uc OK` and `iterate OK` (no "missing separator"/syntax error). (`terraform.tfvars` may already exist from a prior run; the `cp -n` is a no-op then.)

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/Makefile
git commit -m "feat(bench-infra): make up-uc (UC-only fleet) + make iterate (loop fitness)"
```

---

## Task 5: `uc-throughput` autobench task (overlay + results.tsv)

**Files:**
- Create: `uc_autobench/tasks/uc-throughput/program.md`
- Create: `uc_autobench/tasks/uc-throughput/results.tsv`

- [ ] **Step 1: Create the task overlay**

`uc_autobench/tasks/uc-throughput/program.md`:
```markdown
# Task overlay: uc-throughput

Optimize UC's **3-node distributed throughput ceiling**, measured on a persistent
`bench_infra` cloud fleet (UC-only). See
`../../../docs/superpowers/specs/2026-06-16-uc-autobench-distributed-throughput-loop-design.md`.

## Prerequisite (human, once per session)
A UC-only cloud fleet must be UP before the loop runs:
`cd ../../bench-infra && make up-uc`  (and `make destroy` when done).

## The loop
Per iteration: edit a mutable path, then run the driver:
`bash uc_autobench/scripts/uc-throughput-iter.sh > /tmp/uc-iter.json`
Parse `jq '.status, .metrics, .gate' /tmp/uc-iter.json` and:
- status=="pass" AND `uc_throughput_msgs` beats current best beyond noise → KEEP (commit, append TSV row).
- status=="pass" but no improvement → DISCARD (revert mutable paths, append row).
- status in {build_failed, lincheck_failed} → DISCARD (revert, append row). No cloud spend was incurred.

## Mutable paths (the throughput lever — task13 §6: submit/apply pipeline)
- uc_node/src/runtime/builder.rs        (openraft Config: api_batch_capacity, max_payload_entries, api_batch_linger_ms)
- uc_node/src/ipc/                       (client_dispatcher, apply-ring publish/enqueue, wakeup path)
- uc_node/src/raft/                      (apply pipeline, log_storage append batching)
Do NOT edit uc_protocol/src/ring/ (that is the `shmem` task's domain).

## Metrics
- Primary: `uc_throughput_msgs` (maximize) — max achieved_rate across the ladder.
- Secondary/observability: `knee_rate`, `p99_at_knee_ms` (not gated).
- Correctness gate: lincheck capstone `linearizable_under_failover` MUST pass — a
  throughput win that breaks linearizability is discarded before any cloud spend.

## Noise
Cloud `achieved_rate` has run-to-run variance; treat changes within ~5% as noise.

## TSV schema (results.tsv)
```
commit	uc_throughput_msgs	knee_rate	p99_at_knee_ms	lincheck_passed	status	description
```
Statuses: keep, discard, crash. Use 0 for metrics that didn't run.
```

- [ ] **Step 2: Create the results.tsv with its header**

`uc_autobench/tasks/uc-throughput/results.tsv` (a single tab-separated header line):
```
commit	uc_throughput_msgs	knee_rate	p99_at_knee_ms	lincheck_passed	status	description
```

- [ ] **Step 3: Verify the files**

Run:
```bash
cd /home/claude/ultima/ultima_cluster
test -f uc_autobench/tasks/uc-throughput/program.md && echo "overlay OK"
head -1 uc_autobench/tasks/uc-throughput/results.tsv | tr '\t' ',' | grep -q '^commit,uc_throughput_msgs,knee_rate,p99_at_knee_ms,lincheck_passed,status,description$' && echo "TSV header OK"
```
Expected: `overlay OK` and `TSV header OK`.

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add uc_autobench/tasks/uc-throughput/program.md uc_autobench/tasks/uc-throughput/results.tsv
git commit -m "feat(uc_autobench): uc-throughput task overlay + results.tsv"
```

---

## Task 6: Iteration driver `uc-throughput-iter.sh` (gate-then-fitness) + test

**Files:**
- Create: `uc_autobench/scripts/uc-throughput-iter.sh`
- Create: `uc_autobench/scripts/test_uc_throughput_iter.sh`

- [ ] **Step 1: Write the failing test (gates stubbed via env overrides)**

`uc_autobench/scripts/test_uc_throughput_iter.sh`:
```bash
#!/usr/bin/env bash
# Tests the driver's gate ordering + JSON output with the cargo/lincheck/cloud
# commands stubbed (no real build, no cloud spend).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D="$HERE/uc-throughput-iter.sh"

# Case 1: build fails → status=build_failed, no lincheck/cloud run.
out="$(UC_BUILD_CMD=false UC_LINCHECK_CMD='echo SHOULD_NOT_RUN; false' \
       UC_ITER_CMD='echo SHOULD_NOT_RUN' bash "$D")"
echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);assert d["status"]=="build_failed",d;print("c1 ok")'

# Case 2: build ok, lincheck fails → status=lincheck_failed, no cloud run.
out="$(UC_BUILD_CMD=true UC_LINCHECK_CMD=false \
       UC_ITER_CMD='echo SHOULD_NOT_RUN' bash "$D")"
echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);assert d["status"]=="lincheck_failed",d;assert d["gate"]["lincheck_passed"]==False,d;print("c2 ok")'

# Case 3: gates pass, cloud returns fitness → status=pass, metric threaded through.
out="$(UC_BUILD_CMD=true UC_LINCHECK_CMD=true \
       UC_ITER_CMD='echo {\"uc_throughput_msgs\":812.5,\"knee_rate\":600,\"p99_at_knee_ms\":3.1}' \
       bash "$D")"
echo "$out" | python3 -c '
import json,sys
d=json.load(sys.stdin)
assert d["status"]=="pass",d
assert d["gate"]["lincheck_passed"]==True,d
assert abs(d["metrics"]["uc_throughput_msgs"]-812.5)<0.01,d
assert d["metrics"]["knee_rate"]==600,d
print("c3 ok")'
echo "DRIVER OK"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `chmod +x uc_autobench/scripts/test_uc_throughput_iter.sh && uc_autobench/scripts/test_uc_throughput_iter.sh`
Expected: FAIL — `uc-throughput-iter.sh: No such file or directory`.

- [ ] **Step 3: Implement the driver**

`uc_autobench/scripts/uc-throughput-iter.sh`:
```bash
#!/usr/bin/env bash
# One uc-throughput iteration: cheap local gates first (compile, lincheck), then the
# expensive cloud fitness (make iterate) only if both pass. Emits ONE JSON line on
# stdout. Exit 0 even on gate failure (status carries the verdict — matches run-iter).
#
# Commands are overridable via env for testing:
#   UC_BUILD_CMD     (default: cargo build the bench bins)
#   UC_LINCHECK_CMD  (default: cargo test the lincheck capstone)
#   UC_ITER_CMD      (default: make -C bench-infra iterate)
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

BUILD_CMD="${UC_BUILD_CMD:-cargo build -p uc_autobench --bin uc-node-launch --bin commit-path-load --release}"
LINCHECK_CMD="${UC_LINCHECK_CMD:-cargo test -p uc_node --test lin_register linearizable_under_failover --release -- --test-threads=1}"
ITER_CMD="${UC_ITER_CMD:-make -C $ROOT/bench-infra iterate}"

emit() { # emit <status> <lincheck_passed> [fitness_json]
  local status="$1" lp="$2" fit="${3:-{\}}"
  python3 - "$status" "$lp" "$fit" <<'PY'
import json,sys
status, lp, fit = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    metrics = json.loads(fit)
except Exception:
    metrics = {}
print(json.dumps({
    "status": status,
    "gate": {"lincheck_passed": lp == "true"},
    "metrics": {
        "uc_throughput_msgs": metrics.get("uc_throughput_msgs", 0),
        "knee_rate": metrics.get("knee_rate", 0),
        "p99_at_knee_ms": metrics.get("p99_at_knee_ms", 0),
    },
}))
PY
}

# 1. compile check (cheap)
if ! eval "$BUILD_CMD" >/tmp/uc-iter-build.log 2>&1; then
  emit build_failed false; exit 0
fi

# 2. correctness gate (local lincheck capstone, ~40s) — before any cloud spend
if ! eval "$LINCHECK_CMD" >/tmp/uc-iter-lincheck.log 2>&1; then
  emit lincheck_failed false; exit 0
fi

# 3. cloud fitness (make iterate prints the fitness JSON as its last { ... } line)
iter_out="$(eval "$ITER_CMD" 2>/tmp/uc-iter-cloud.log)"
fit="$(printf '%s\n' "$iter_out" | grep -E '^\{.*uc_throughput_msgs' | tail -1)"
if [ -z "$fit" ]; then
  emit iterate_failed true; exit 0
fi
emit pass true "$fit"
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uc_autobench/scripts/test_uc_throughput_iter.sh`
Expected: `c1 ok`, `c2 ok`, `c3 ok`, `DRIVER OK`.

- [ ] **Step 5: shellcheck the driver**

Run: `shellcheck uc_autobench/scripts/uc-throughput-iter.sh`
Expected: exit 0. (If it flags the `eval` of the command vars, that is intentional — the commands are trusted/overridable; add `# shellcheck disable=SC2086` only if needed for the unquoted `$ROOT` in the default `ITER_CMD`.)

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
chmod +x uc_autobench/scripts/uc-throughput-iter.sh
git add uc_autobench/scripts/uc-throughput-iter.sh uc_autobench/scripts/test_uc_throughput_iter.sh
git commit -m "feat(uc_autobench): uc-throughput-iter.sh driver (compile→lincheck→cloud fitness) +test"
```

---

## Task 7: Live end-to-end (operator — needs a fleet + HCLOUD_TOKEN + spend)

**This task provisions a real UC-only Hetzner fleet and runs one real iteration. ~€0.24/hr; needs the dedicated-vCPU quota for 3× CCX33.**

- [ ] **Step 1: Bring up the UC-only fleet**

Run:
```bash
cd bench-infra
export HCLOUD_TOKEN=...   # or via .env
make up-uc
```
Expected: 3 servers created; Ansible provision green with **no `build_aeron` tasks** (skipped) and UC built. ~10–12 min.

- [ ] **Step 2: Run one `make iterate` directly**

Run: `make iterate`
Expected: rebuild + UC-only sweep (no Aeron tasks), ends with a single fitness line like `{"uc_throughput_msgs": 8XX.X, "knee_rate": 500, "p99_at_knee_ms": ...}`. Confirm `bench-out/iterate/uc_sweep.csv` exists.

- [ ] **Step 3: Run the driver once (full gate chain)**

Run: `cd /home/claude/ultima/ultima_cluster && bash uc_autobench/scripts/uc-throughput-iter.sh | tee /tmp/uc-iter.json`
Expected: a `{"status":"pass","gate":{"lincheck_passed":true},"metrics":{"uc_throughput_msgs":8XX.X,...}}` line. (Compile + lincheck run locally first; then the cloud sweep.)

- [ ] **Step 4: Seed the baseline TSV row and start a loop session**

Append the baseline to `uc_autobench/tasks/uc-throughput/results.tsv` (tab-separated), e.g.:
```
<current-sha>	<uc_throughput_msgs>	<knee_rate>	<p99_at_knee_ms>	true	keep	baseline (no change)
```
Then run the autoresearch loop per `uc_autobench/tasks/uc-throughput/program.md`. Destroy when done:
```bash
cd bench-infra && make destroy
```

- [ ] **Step 5: Commit the baseline row**

```bash
cd "$(git rev-parse --show-toplevel)"
git add uc_autobench/tasks/uc-throughput/results.tsv
git commit -m "chore(uc_autobench): seed uc-throughput baseline row"
```

---

## Self-review notes (for the executor)

- **Spec deviation (intentional):** the spec said "`run-iter --task uc-throughput` arm"; this plan instead uses a dedicated `uc-throughput-iter.sh` driver to keep cloud/ansible orchestration out of the Rust `run-iter` binary (which is the `shmem` task's fast-microbench tool). Same JSON contract; cleaner isolation.
- The `iterate.yml` includes the `run` role, which already has the `Clean stale cluster state` task — so repeated iterations are idempotent on the same fleet.
- The driver's three commands are env-overridable strictly for testing; in production all three default to the real cargo/make commands.
- The correctness gate runs **before** the cloud sweep every iteration, so a linearizability-breaking change costs ~40s locally and zero cloud time.
- Task 7 is the only task needing cloud credentials/spend; Tasks 1–6 are fully offline-verifiable.
