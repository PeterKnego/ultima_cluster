# aeron-go ClusteredServiceContainer Fleet A/B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure aeron-go's ClusteredServiceContainer (Go echo service) against the Java echo service on the AWS fleet — identical Java consensus module, media driver, and LoadTestRig client in both arms; only the service-container language swaps.

**Architecture:** aeron-go has no consensus module — its `cluster/` package is a service container + ingress client only, so the Go arm runs the stock aeron-io/benchmarks `ClusterNode` JVM with the embedded service container disabled (a small upstream-friendly patch adds an `external` service type), plus the aeron-go `echo_service` as a separate process on the same host attached via the shared aeron dir. Everything else (media driver, archive, LoadTestRig rate ladder, collect) reuses the existing bench-infra flow, so results are directly comparable with `bench-out/aeron-echo` and the scorecard.

**Tech Stack:** aeron-io/benchmarks (Java, gradle), aeron-go (`github.com/lirm/aeron-go`, Go ≥1.20), ansible roles in `bench-infra/`, 3×c6id.4xlarge fleet.

## Global Constraints

- **Version compatibility is PROVEN, not assumed:** local spike 2026-07-03 ran Go container + Go client against Java `ClusteredMediaDriver` from BOTH `aeron-all-1.43.0.jar` (vendored in aeron-go) and `aeron-all-1.51.0.jar` (= benchmarks master pin). Echo round-trips green both times. No version pinning gymnastics needed; keep benchmarks at master, but **pin `aeron_benchmarks_ref` to SHA `6afb215`** (current master) for reproducibility and so the patch applies.
- Pin aeron-go to `8b05ad1` (upstream HEAD, 2024-06-06).
- The ClusterNode patch must default to existing behavior (`ECHO`) — the Java arm's build and runtime are byte-identical unless `-Dio.aeron.benchmarks.aeron.cluster.service=external` is set.
- Go service runs with `NO_OP_IDLE=1` (busy-spin idle strategy) to match the Java container's `BusySpinIdleStrategy`.
- Both arms run in ONE `bench.yml` invocation on the SAME instances (Java arm first, then Go arm), gated by `aerongo_enabled` (default `false`).
- Aeron `.hdr` result values are NANOSECONDS (recorded gotcha).
- Working repo: `ultima_cluster` main branch; commit after each task.
- Fleet facts referenced throughout: aeron dir = `/dev/shm/{{ node_role }}-driver`, cluster dir = `{{ remote_home }}/cluster/{{ node_role }}/cluster` (from `node.properties.j2`); benchmarks scripts live in `{{ aeron_deploy_dir }}/scripts/aeron/`.

---

### Task 1: ClusterNode external-service patch + pinned ref

**Files:**
- Create: `bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch`
- Modify: `bench-infra/ansible/roles/build_aeron/tasks/main.yml` (apply patch after clone)
- Modify: `bench-infra/ansible/group_vars/all.yml` (`aeron_benchmarks_ref: "6afb215"` + new aerongo vars)

**Interfaces:**
- Produces: `cluster-node` launcher that skips the embedded `ClusteredServiceContainer` when `-Dio.aeron.benchmarks.aeron.cluster.service=external` is set. Tasks 2 and 4 rely on that property name and value exactly.

- [ ] **Step 1: Generate the patch from a local benchmarks checkout**

A blob-filtered clone already exists at the session scratchpad (`$SP/benchmarks`); otherwise `git clone https://github.com/aeron-io/benchmarks.git`. Check out `6afb215`, then edit `benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/ClusterNode.java` with exactly these three changes (match real-logic style — checkstyle runs in the build):

Change 1 — the `Type` enum (bottom of the file) gains `EXTERNAL`:

```java
    private enum Type
    {
        ECHO,
        FAILOVER,
        EXTERNAL;

        public static Type fromSystemProperty()
        {
            final String clusteredServiceName = System.getProperty(CLUSTER_SERVICE_PROP_NAME);
            if ("failover".equals(clusteredServiceName))
            {
                return FAILOVER;
            }
            return "external".equals(clusteredServiceName) ? EXTERNAL : ECHO;
        }
    }
```

Change 2 — in `main`, the try-with-resources line that starts the container becomes conditional (null resources are legal and skipped on close):

```java
                Component<ClusteredServiceContainer> csc =
                    Type.EXTERNAL == type ? null : clusteredServiceContainer.start();
```

Change 3 — guard the service-mark-file error dump after `signalBarrier.await()` (the Go service owns that mark file):

```java
                if (Type.EXTERNAL != type)
                {
                    AeronUtil.dumpClusterErrors(
                        logsDir.resolve(prefix + "clustered-service-errors.txt"),
                        clusterDir,
                        ClusterMarkFile.markFilenameForService(serviceId),
                        ClusterMarkFile.linkFilenameForService(serviceId));
                }
```

- [ ] **Step 2: Verify the patch compiles**

Run in the benchmarks checkout: `./gradlew :benchmarks-aeron:compileJava checkstyleMain --console=plain` (JAVA_HOME = local JDK 21).
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Export the patch into bench-infra**

```bash
git -C <benchmarks-checkout> diff > bench-infra/ansible/roles/build_aeron/files/clusternode-external-service.patch
```

- [ ] **Step 4: Apply it in the build role, idempotently**

In `bench-infra/ansible/roles/build_aeron/tasks/main.yml`, insert between "Clone aeron-benchmarks" and "Build deployTar":

```yaml
- name: Copy ClusterNode external-service patch
  ansible.builtin.copy:
    src: clusternode-external-service.patch
    dest: "{{ remote_home }}/clusternode-external-service.patch"
    mode: "0644"

- name: Apply ClusterNode external-service patch
  ansible.builtin.shell: |
    cd {{ remote_home }}/aeron-benchmarks
    if git apply --reverse --check {{ remote_home }}/clusternode-external-service.patch 2>/dev/null; then
      echo already-applied
    else
      git apply {{ remote_home }}/clusternode-external-service.patch
    fi
  register: patch_apply
  changed_when: "'already-applied' not in patch_apply.stdout"
```

- [ ] **Step 5: Pin the ref + add aerongo vars in `group_vars/all.yml`**

```yaml
aeron_benchmarks_ref: "6afb215"   # pinned 2026-07-03 for reproducibility + external-service patch
# ---- aeron-go arm (Go ClusteredServiceContainer A/B) ----
aerongo_enabled: false            # true = run the Go-service arm after the Java arm
aerongo_repo: "https://github.com/lirm/aeron-go.git"
aerongo_ref: "8b05ad1"            # upstream HEAD 2024-06-06; proven against Aeron 1.43 AND 1.51
aerongo_dir: "/opt/bench/aerongo"
```

Note: the git-module clone task uses `depth: 1`; pinning a SHA needs the full-ish fetch — change `depth: 1` to `depth: 0`/omit if the clone task fails on the SHA. Verify on first provision.

- [ ] **Step 6: Commit**

```bash
git add bench-infra && git commit -m "bench: ClusterNode external-service patch + pinned benchmarks ref (aeron-go arm groundwork)"
```

---

### Task 2: Local single-node end-to-end smoke (patched node + Go service + LoadTestRig)

Everything in this task is throwaway (scratchpad); it gates the fleet work. No repo changes except fixes it uncovers.

**Files:**
- Scratchpad only: `$SP/localbench/{cluster,node,client}.properties`, built `benchmarks.tar` dist, Go `echo_service` binary (already built at `$SP/bin/echo_service`).

**Interfaces:**
- Consumes: patched benchmarks checkout from Task 1; Go binary from the spike.
- Produces: confidence + any property fixes fed back into Task 4's YAML.

- [ ] **Step 1: Build the patched deployTar locally**

```bash
cd <benchmarks-checkout> && ./gradlew clean deployTar -x test --no-daemon --console=plain
mkdir -p $SP/localbench/deploy && tar -xf build/distributions/benchmarks.tar -C $SP/localbench/deploy
```

Expected: `$SP/localbench/deploy/scripts/aeron/cluster-node` exists.

- [ ] **Step 2: Write single-node property files** (generated via heredoc so `$SP` expands; they mirror `cluster.properties.j2` / `node.properties.j2` / `client.properties.j2` for one member, no fsync)

```bash
L=$SP/localbench; mkdir -p $L/results
cat > $L/cluster.properties <<EOF
aeron.cluster.members=0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004
aeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0
aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0
aeron.archive.recording.events.enabled=false
aeron.cluster.appointed.leader.id=0
aeron.cluster.ingress.channel=aeron:udp
aeron.cluster.ingress.endpoints=0=localhost:20000
aeron.archive.file.sync.level=0
aeron.archive.catalog.file.sync.level=0
aeron.term.buffer.sparse.file=true
EOF
cat > $L/node.properties <<EOF
aeron.dir=/dev/shm/node0-driver
aeron.cluster.dir=$L/cluster/node0/cluster
aeron.cluster.member.id=0
aeron.archive.dir=$L/cluster/node0/archive
aeron.archive.control.channel=aeron:udp?endpoint=localhost:20004
EOF
cat > $L/client.properties <<EOF
aeron.dir=/dev/shm/node0-driver
aeron.cluster.egress.channel=aeron:udp?endpoint=localhost:0
io.aeron.benchmarks.batch.size=1
io.aeron.benchmarks.message.length=32
io.aeron.benchmarks.output.directory=$L/results
EOF
```

- [ ] **Step 3: Launch the three server-side processes**

```bash
D=$SP/localbench/deploy/scripts/aeron; L=$SP/localbench
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export JVM_OPTS="-Xms64M -Dio.aeron.benchmarks.output.directory=$L/results"
setsid $D/media-driver $L/cluster.properties $L/node.properties > $L/md.out 2>&1 < /dev/null &
JVM_OPTS="$JVM_OPTS -Dio.aeron.benchmarks.aeron.cluster.service=external" \
  setsid $D/cluster-node $L/cluster.properties $L/node.properties > $L/node.out 2>&1 < /dev/null &
AERON_DIR=/dev/shm/node0-driver CLUSTER_DIR=$L/cluster/node0/cluster NO_OP_IDLE=1 \
  setsid $SP/bin/echo_service > $L/service.out 2>&1 < /dev/null &
sleep 15 && tail -5 $L/service.out
```

Expected in `service.out`: `OnStart`, `OnRoleChange - role=2`, `OnNewLeadershipTermEvent`.

- [ ] **Step 4: Drive LoadTestRig at a low rung**

```bash
export JVM_OPTS="-Xms64M -Dio.aeron.benchmarks.output.directory=$L/results \
 -Dio.aeron.benchmarks.message.rate=1000 -Dio.aeron.benchmarks.message.length=32 \
 -Dio.aeron.benchmarks.batch.size=1 -Dio.aeron.benchmarks.warmup.iterations=3 \
 -Dio.aeron.benchmarks.iterations=10 -Dio.aeron.benchmarks.output.file=smoke_go"
timeout 180 $D/cluster-client $L/cluster.properties $L/client.properties
```

Expected: rig completes all iterations, prints a histogram summary, writes `smoke_go*.hdr` under `$L/results`, and `service.out` shows no warnings. If the rig hangs at "await connection", debug BEFORE touching the fleet (most likely suspects: cluster dir mismatch between `node.properties` and `CLUSTER_DIR`, or ingress endpoints).

- [ ] **Step 5: Tear down**

```bash
pkill -9 -f '[i]o.aeron'; pkill -9 -f '[b]in/echo_service'; rm -rf /dev/shm/node0-driver
```

- [ ] **Step 6: Commit any fixes this smoke forced into Task 1's files**

```bash
git add -u bench-infra && git commit -m "bench: local smoke fixes for external-service cluster node" # only if needed
```

---

### Task 3: `build_aerongo` role (fleet Go toolchain + service build)

**Files:**
- Create: `bench-infra/ansible/roles/build_aerongo/tasks/main.yml`
- Modify: `bench-infra/ansible/build.yml` (add the role to the same play/hosts as `build_aeron`, gated on `aerongo_enabled`)

**Interfaces:**
- Consumes: `aerongo_repo` / `aerongo_ref` / `aerongo_dir` vars from Task 1.
- Produces: `{{ aerongo_dir }}/echo_service` executable on every cluster host. Task 4 launches exactly that path.

- [ ] **Step 1: Write the role**

```yaml
---
- name: Install Go toolchain
  ansible.builtin.apt:
    name: golang-go
    state: present
    update_cache: true
  become: true

- name: Clone aeron-go
  ansible.builtin.git:
    repo: "{{ aerongo_repo }}"
    dest: "{{ remote_home }}/aeron-go"
    version: "{{ aerongo_ref }}"

- name: Ensure aerongo dir exists
  ansible.builtin.file:
    path: "{{ aerongo_dir }}"
    state: directory
    mode: "0755"
  become: true

- name: Build echo_service
  ansible.builtin.shell: |
    cd {{ remote_home }}/aeron-go
    go build -o /tmp/echo_service ./examples/cluster
  changed_when: true

- name: Install echo_service
  ansible.builtin.copy:
    src: /tmp/echo_service
    dest: "{{ aerongo_dir }}/echo_service"
    remote_src: true
    mode: "0755"
  become: true
```

- [ ] **Step 2: Wire into `build.yml`**

Read `bench-infra/ansible/build.yml`, find where `build_aeron` is included, and add beside it:

```yaml
    - { role: build_aerongo, when: aerongo_enabled | default(false) | bool }
```

(Adjust to the file's actual include style — `roles:` list vs `import_role` tasks.)

- [ ] **Step 3: Lint**

Run: `ansible-playbook --syntax-check bench-infra/ansible/build.yml -i bench-infra/inventory 2>&1 | tail -3` (or the repo's usual check; inventory may need `-i localhost,`).
Expected: no syntax errors.

- [ ] **Step 4: Commit**

```bash
git add bench-infra && git commit -m "bench: build_aerongo role — Go toolchain + echo_service on fleet hosts"
```

---

### Task 4: run-role Go arm + sweep prefix

**Files:**
- Modify: `bench-infra/ansible/roles/run/files/run_aeron_sweep.sh` (7th arg: output-file prefix)
- Create: `bench-infra/ansible/roles/run/files/aerongo_service_loop.sh`
- Modify: `bench-infra/ansible/roles/run/tasks/main.yml` (Go-arm block after the Java-arm "Stop Aeron JVMs" task)

**Interfaces:**
- Consumes: patched `cluster-node` (Task 1), `{{ aerongo_dir }}/echo_service` (Task 3).
- Produces: result files `{{ remote_home }}/results/aerongo_rung_<rate>*` — same directory the collect role already gathers; Java arm keeps writing `aeron_rung_<rate>*`.

- [ ] **Step 1: Add the prefix arg to `run_aeron_sweep.sh`**

After the existing `BATCH="$1"; shift` line:

```bash
PREFIX="${1:-aeron}"
```

and change the output-file property to `-Dio.aeron.benchmarks.output.file=${PREFIX}_rung_${r}`. Update the Java-arm invocation in `roles/run/tasks/main.yml` to pass `aeron` explicitly as the 7th argument.

- [ ] **Step 2: Write the Go-service supervisor**

`bench-infra/ansible/roles/run/files/aerongo_service_loop.sh`:

```bash
#!/usr/bin/env bash
# Supervises the aeron-go echo service. The Go agent panics if the media
# driver / consensus module are not up yet, so retry until they are; once
# running it blocks for the whole sweep. Killed by the teardown pkill.
set -u
BIN="$1"; AERON="$2"; CLUSTER="$3"
export AERON_DIR="$AERON" CLUSTER_DIR="$CLUSTER" NO_OP_IDLE=1
for _ in $(seq 1 120); do
  "$BIN"
  sleep 1
done
```

- [ ] **Step 3: Add the Go-arm block to `roles/run/tasks/main.yml`**

Insert directly after the Java-arm "Stop Aeron JVMs" task, all tasks gated `when: aerongo_enabled | default(false) | bool` (AND `node_role == "node0"` where marked):

```yaml
# ---- Aeron-Go arm: same consensus core, service container in Go ----
- name: Clean cluster state between arms (go arm)
  ansible.builtin.shell: |
    pkill -9 -f '[i]o.aeron' || true
    sleep 1
    rm -rf /dev/shm/*-driver {{ remote_home }}/cluster
  changed_when: false

- name: Start Aeron media driver (go arm)
  ansible.builtin.shell: |
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
    export JVM_OPTS="-Xms16M"
    setsid {{ aeron_deploy_dir }}/scripts/aeron/media-driver \
      {{ remote_home }}/aeron-cfg/cluster.properties \
      {{ remote_home }}/aeron-cfg/node.properties > {{ remote_home }}/md-go.out 2>&1 < /dev/null &
  changed_when: false

- name: Start Aeron cluster node with external service (go arm)
  ansible.builtin.shell: |
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
    export JVM_OPTS="-Xms16M -Dio.aeron.benchmarks.output.directory={{ remote_home }}/results -Dio.aeron.benchmarks.aeron.cluster.service=external"
    setsid {{ aeron_deploy_dir }}/scripts/aeron/cluster-node \
      {{ remote_home }}/aeron-cfg/cluster.properties \
      {{ remote_home }}/aeron-cfg/node.properties > {{ remote_home }}/node-go.out 2>&1 < /dev/null &
  changed_when: false

- name: Copy aeron-go service supervisor
  ansible.builtin.copy:
    src: aerongo_service_loop.sh
    dest: "{{ remote_home }}/aerongo_service_loop.sh"
    mode: "0755"

- name: Start aeron-go echo service
  ansible.builtin.shell: |
    setsid {{ remote_home }}/aerongo_service_loop.sh \
      {{ aerongo_dir }}/echo_service \
      /dev/shm/{{ node_role }}-driver \
      {{ remote_home }}/cluster/{{ node_role }}/cluster \
      > {{ remote_home }}/aerongo-service.out 2>&1 < /dev/null &
  changed_when: false

- name: Wait for go-arm cluster election to settle
  ansible.builtin.pause:
    seconds: 20
  run_once: true

- name: Drive Aeron-Go rate ladder (node0)
  ansible.builtin.command: >
    {{ remote_home }}/run_aeron_sweep.sh
    {{ remote_home }}
    {{ rate_ladder | join(',') }}
    {{ payload_bytes }} {{ warmup_seconds }} {{ measure_seconds }}
    {{ batch_size }}
    aerongo
  when: node_role == "node0"
  changed_when: true

- name: Stop go-arm processes
  ansible.builtin.shell: |
    pkill -9 -f '[i]o.aeron' || true
    pkill -9 -f '[a]erongo_service_loop' || true
    pkill -9 -f '[e]cho_service' || true
  changed_when: false
  failed_when: false
```

Note the existing role-level `when: aeron_enabled` pattern on the Java tasks — apply `aerongo_enabled` the same way (per-task, or wrap the block in `block:/when:`). Also extend the role's initial "Clean stale cluster state" pkill list with `pkill -9 -f '[e]cho_service' || true`.

- [ ] **Step 4: Lint + review the whole run flow once end-to-end**

Run: `ansible-playbook --syntax-check bench-infra/ansible/bench.yml -i bench-infra/inventory 2>&1 | tail -3`. Then reread the role top-to-bottom checking: Java arm unchanged when `aerongo_enabled=false`; go arm cleans state first; sweep args line up with the new PREFIX param.

- [ ] **Step 5: Commit**

```bash
git add bench-infra && git commit -m "bench: aeron-go service arm in run role + sweep output prefix"
```

---

### Task 5: Fleet A/B run

**Files:** none (operational). Results land in `bench-out/`.

**Interfaces:**
- Consumes: everything above; `bench-infra` README/Makefile flow (terraform apply → provision → build → configure → bench → collect → destroy) with `terraform.tfvars.aws` (3×c6id.4xlarge).
- Produces: `results/aeron_rung_*` (Java arm) + `results/aerongo_rung_*` (Go arm) `.hdr` files collected locally under `bench-out/aerongo-ab/`.

- [ ] **Step 1: Bring up the fleet** — follow `bench-infra/README.md` / Makefile targets exactly as in previous runs (AWS tfvars). This costs money; verify `terraform plan` shows 3 instances before applying.
- [ ] **Step 2: Provision + build with the Go arm enabled** — pass `-e aerongo_enabled=true` to the build/configure/bench plays (or set it in group_vars for the run). Watch the build_aeron patch task and build_aerongo tasks succeed on all 3 hosts.
- [ ] **Step 3: Run `bench.yml`** with the standard Aeron rate ladder used for `bench-out/aeron-echo` (same payload/warmup/measure/batch). Both arms run in this single invocation.
- [ ] **Step 4: Sanity-check mid-run** — on node0: `tail aerongo-service.out` (no crash loops), `results/` filling with both `aeron_rung_*` and `aerongo_rung_*`.
- [ ] **Step 5: Collect + destroy** — run the collect play into `bench-out/aerongo-ab/<date>/`, then `terraform destroy`. Never leave the fleet up.
- [ ] **Step 6: Commit raw results**

```bash
git add bench-out/aerongo-ab && git commit -m "bench: aeron-go vs Java service A/B raw results (3x c6id.4xlarge)"
```

---

### Task 6: Analysis + docs

**Files:**
- Create: `docs/benchmarks/aerongo-cluster-echo-<run-date>.md`
- Create: `docs/tasks/task21_aerongo_cluster_bench.md` (check `docs/tasks/` for the next free number first)

**Interfaces:**
- Consumes: `.hdr` files from Task 5. **Values are NANOSECONDS.** Use the same histogram processing as the scorecard runs (see `docs/benchmarks/` for the prior aeron-echo analysis method; benchmarks repo also ships `scripts/aggregate-results`).

- [ ] **Step 1: Produce per-rung p50/p99/p99.9 + achieved-rate tables for both arms; identify each arm's knee.**
- [ ] **Step 2: Write the results doc** — method (external-service patch, versions: benchmarks 6afb215 / Aeron 1.51 / aeron-go 8b05ad1), fairness notes (Go arm = extra process; both arms `sync.level` per run config; busy-spin idle both), tables, knee comparison, and the delta attributed to the Go container.
- [ ] **Step 3: Write the task doc** (canonical record per CLAUDE.md — stands alone, folds in the design rationale; leave this plan file in place).
- [ ] **Step 4: Update memory** (`aeron-go-cluster-bench-feasibility.md` → outcome + numbers).
- [ ] **Step 5: Commit**

```bash
git add docs bench-out && git commit -m "docs: aeron-go vs Java clustered-service A/B results + task doc"
```
