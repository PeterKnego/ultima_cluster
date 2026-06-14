# Bench Provisioning Rig (Terraform + Ansible) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `bench-infra/` rig that provisions 3 dedicated-vCPU hosts (Hetzner default; AWS/GCP also supported), configures toolchains/builds/configs/OS-tuning, and runs + collects the Aeron-Cluster vs `ultima_cluster` parity sweep — reproducibly and cheaply.

**Architecture:** Two layers with one handoff. Terraform owns infrastructure and emits a `nodes[]` output via a fixed per-cloud module contract; a script turns that into an Ansible inventory. Ansible (provider-agnostic) tunes the OS, builds both systems, templates configs with private IPs, runs the sweep, and pulls results back. A Makefile wraps it into `up` / `bench` / `bench-oneshot` / `status` / `destroy`.

**Tech Stack:** Terraform (hcloud, aws, google providers), Ansible, Jinja2 templates, bash, Make. Targets Ubuntu 24.04 hosts. JDK 21 (Temurin), Rust (rustup), aeron-benchmarks (gradle), `uc_autobench` (cargo).

**Build order rationale:** Tasks 1–12 deliver a complete, working **Hetzner** vertical slice (provision → configure → bench → collect → destroy). Tasks 13–14 add AWS and GCP as drop-in modules against the same contract — no changes to root Terraform or any Ansible. Task 15 is the live end-to-end smoke + README. This way the rig is usable after Task 12.

**Spec:** `docs/superpowers/specs/2026-06-14-bench-infra-terraform-ansible-design.md`

**Conventions for verification:** This is infrastructure code, not unit-testable logic. Each task verifies with the appropriate static gate (`terraform validate`/`fmt`, `ansible-playbook --syntax-check`, `shellcheck`) and, where a task changes runtime behavior, an idempotence or smoke check. Live cloud steps that need credentials are explicitly marked and concentrated in Task 15.

**Node model (used throughout):** exactly 3 hosts. `node0` = appointed Aeron leader + co-located client host (UC client also runs here). `node1`/`node2` = followers. Each module outputs nodes ordered `[node0, node1, node2]` with `{name, role, public_ip, private_ip}`.

---

## Task 1: Scaffolding — directory, gitignore, Terraform contract skeleton

**Files:**
- Create: `bench-infra/.gitignore`
- Create: `bench-infra/terraform/versions.tf`
- Create: `bench-infra/terraform/variables.tf`
- Create: `bench-infra/terraform/main.tf`
- Create: `bench-infra/terraform/outputs.tf`

- [ ] **Step 1: Create `bench-infra/.gitignore`**

```gitignore
# Terraform
terraform/.terraform/
terraform/.terraform.lock.hcl
*.tfstate
*.tfstate.*
*.tfvars
!example.tfvars
crash.log

# Generated inventory & SSH
inventory/hosts.yml
*.pem

# Ansible
ansible/*.retry
```

- [ ] **Step 2: Create `bench-infra/terraform/versions.tf`**

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.48"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}
```

- [ ] **Step 3: Create `bench-infra/terraform/variables.tf`**

```hcl
variable "cloud" {
  description = "Which cloud module to use."
  type        = string
  default     = "hetzner"
  validation {
    condition     = contains(["hetzner", "aws", "gcp"], var.cloud)
    error_message = "cloud must be one of: hetzner, aws, gcp."
  }
}

variable "node_count" {
  description = "Number of cluster hosts (fixed at 3 for topology B)."
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "Per-cloud instance type. Empty string uses the module default."
  type        = string
  default     = ""
}

variable "region" {
  description = "Per-cloud region/location. Empty string uses the module default."
  type        = string
  default     = ""
}

variable "ssh_public_key" {
  description = "SSH public key contents to install on the hosts."
  type        = string
}

variable "ssh_private_key_file" {
  description = "Path to the matching private key, written into the inventory for Ansible."
  type        = string
}

variable "allow_ssh_cidr" {
  description = "CIDR allowed to SSH to the hosts (e.g. your IP/32)."
  type        = string
}

variable "ttl_hours" {
  description = "Advisory TTL tag for the cost guard."
  type        = number
  default     = 4
}

variable "owner" {
  description = "Owner tag/label for resources."
  type        = string
  default     = "uc-bench"
}
```

- [ ] **Step 4: Create `bench-infra/terraform/main.tf` (provider dispatch; only Hetzner wired so far)**

```hcl
locals {
  enable_hetzner = var.cloud == "hetzner"
  enable_aws     = var.cloud == "aws"
  enable_gcp     = var.cloud == "gcp"
}

module "hetzner" {
  source = "./modules/hetzner"
  count  = local.enable_hetzner ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}

# AWS and GCP modules are added in Tasks 13 and 14 with identical call shape.

locals {
  active_module = (
    local.enable_hetzner ? module.hetzner[0] :
    null
  )
}
```

- [ ] **Step 5: Create `bench-infra/terraform/outputs.tf`**

```hcl
output "nodes" {
  description = "Ordered [node0, node1, node2] with role + public/private IPs."
  value       = local.active_module.nodes
}

output "ssh_user" {
  description = "SSH username for Ansible (per-cloud default image user)."
  value       = local.active_module.ssh_user
}
```

- [ ] **Step 6: Verify formatting (validate needs the module, which lands in Task 2)**

Run: `cd bench-infra/terraform && terraform fmt -recursive`
Expected: prints the filenames it formatted (or nothing if already formatted); exit 0.

- [ ] **Step 7: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/.gitignore bench-infra/terraform/versions.tf bench-infra/terraform/variables.tf bench-infra/terraform/main.tf bench-infra/terraform/outputs.tf
git commit -m "feat(bench-infra): terraform scaffolding + cloud-dispatch contract"
```

---

## Task 2: Hetzner Terraform module

**Files:**
- Create: `bench-infra/terraform/modules/hetzner/variables.tf`
- Create: `bench-infra/terraform/modules/hetzner/main.tf`
- Create: `bench-infra/terraform/modules/hetzner/outputs.tf`

- [ ] **Step 1: Create `bench-infra/terraform/modules/hetzner/variables.tf`**

```hcl
variable "node_count"     { type = number }
variable "instance_type"  { type = string }
variable "region"         { type = string }
variable "ssh_public_key" { type = string }
variable "allow_ssh_cidr" { type = string }
variable "ttl_hours"      { type = number }
variable "owner"          { type = string }

locals {
  instance_type = var.instance_type != "" ? var.instance_type : "ccx33" # 8 dedicated vCPU / 32GB
  location      = var.region != "" ? var.region : "nbg1"
  roles         = [for i in range(var.node_count) : "node${i}"]
}
```

- [ ] **Step 2: Create `bench-infra/terraform/modules/hetzner/main.tf`**

```hcl
# Token comes from HCLOUD_TOKEN env var (provider reads it automatically).
provider "hcloud" {}

resource "hcloud_ssh_key" "bench" {
  name       = "${var.owner}-key"
  public_key = var.ssh_public_key
}

resource "hcloud_network" "bench" {
  name     = "${var.owner}-net"
  ip_range = "10.10.0.0/16"
}

resource "hcloud_network_subnet" "bench" {
  network_id   = hcloud_network.bench.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.10.1.0/24"
}

resource "hcloud_firewall" "bench" {
  name = "${var.owner}-fw"
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = [var.allow_ssh_cidr]
  }
}

resource "hcloud_server" "node" {
  count       = var.node_count
  name        = "${var.owner}-node${count.index}"
  server_type = local.instance_type
  image       = "ubuntu-24.04"
  location    = local.location
  ssh_keys    = [hcloud_ssh_key.bench.id]
  firewall_ids = [hcloud_firewall.bench.id]

  labels = {
    owner     = var.owner
    ttl_hours = tostring(var.ttl_hours)
    role      = "node${count.index}"
  }

  network {
    network_id = hcloud_network.bench.id
    ip         = "10.10.1.${count.index + 10}"
  }

  depends_on = [hcloud_network_subnet.bench]
}
```

- [ ] **Step 3: Create `bench-infra/terraform/modules/hetzner/outputs.tf`**

```hcl
output "nodes" {
  value = [
    for i, s in hcloud_server.node : {
      name       = s.name
      role       = "node${i}"
      public_ip  = s.ipv4_address
      private_ip = "10.10.1.${i + 10}"
    }
  ]
}

output "ssh_user" {
  value = "root" # Hetzner Ubuntu images log in as root
}
```

- [ ] **Step 4: Verify the module validates**

Run: `cd bench-infra/terraform && terraform init -backend=false && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Verify a plan renders (requires HCLOUD_TOKEN)**

Run:
```bash
cd bench-infra/terraform
export HCLOUD_TOKEN=<your-token>
terraform plan \
  -var 'ssh_public_key='"$(cat ~/.ssh/id_ed25519.pub)" \
  -var 'ssh_private_key_file=~/.ssh/id_ed25519' \
  -var 'allow_ssh_cidr='"$(curl -s ifconfig.me)/32"
```
Expected: plan shows `3 hcloud_server.node` + network + subnet + firewall + ssh_key to add; no errors. (If you have no token yet, skip — Task 15 runs the live plan.)

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/terraform/modules/hetzner
git commit -m "feat(bench-infra): hetzner terraform module (3 dedicated-vCPU nodes, private net, fw)"
```

---

## Task 3: Inventory generator (Terraform output → Ansible inventory)

**Files:**
- Create: `bench-infra/inventory/terraform_to_inventory.sh`
- Create: `bench-infra/inventory/.gitkeep`

- [ ] **Step 1: Create `bench-infra/inventory/terraform_to_inventory.sh`**

```bash
#!/usr/bin/env bash
# Turn `terraform output -json` into an Ansible inventory at inventory/hosts.yml.
# Groups: [cluster] = all nodes; [node0] = the leader/client host.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${HERE}/../terraform"
OUT="${HERE}/hosts.yml"
SSH_KEY="${SSH_PRIVATE_KEY_FILE:?set SSH_PRIVATE_KEY_FILE to the private key path}"

json="$(cd "$TF_DIR" && terraform output -json)"
ssh_user="$(echo "$json" | jq -r '.ssh_user.value')"

{
  echo "all:"
  echo "  vars:"
  echo "    ansible_user: ${ssh_user}"
  echo "    ansible_ssh_private_key_file: ${SSH_KEY}"
  echo "    ansible_ssh_common_args: '-o StrictHostKeyChecking=accept-new'"
  echo "  children:"
  echo "    cluster:"
  echo "      hosts:"
  echo "$json" | jq -r '.nodes.value[] |
    "        \(.name):\n          ansible_host: \(.public_ip)\n          private_ip: \(.private_ip)\n          node_role: \(.role)\n          node_id: \(.role | ltrimstr("node"))"'
  echo "    node0:"
  echo "      hosts:"
  echo "$json" | jq -r '.nodes.value[] | select(.role=="node0") | "        \(.name): {}"'
} > "$OUT"

echo "wrote $OUT"
```

- [ ] **Step 2: Make it executable and shellcheck it**

Run: `chmod +x bench-infra/inventory/terraform_to_inventory.sh && shellcheck bench-infra/inventory/terraform_to_inventory.sh`
Expected: shellcheck exits 0 (no warnings). If `shellcheck` is absent, `bash -n` the file instead — expected: no output, exit 0.

- [ ] **Step 3: Verify it parses against a fixture (no cloud needed)**

Create a throwaway fixture and confirm the generator + Ansible parse it:
```bash
cd bench-infra/inventory
cat > /tmp/tf.json <<'EOF'
{"nodes":{"value":[
 {"name":"uc-bench-node0","role":"node0","public_ip":"1.1.1.1","private_ip":"10.10.1.10"},
 {"name":"uc-bench-node1","role":"node1","public_ip":"1.1.1.2","private_ip":"10.10.1.11"},
 {"name":"uc-bench-node2","role":"node2","public_ip":"1.1.1.3","private_ip":"10.10.1.12"}]},
 "ssh_user":{"value":"root"}}
EOF
# temporarily stub terraform output for the fixture:
SSH_PRIVATE_KEY_FILE=/tmp/key.pem bash -c '
  json=$(cat /tmp/tf.json); ssh_user=$(echo "$json"|jq -r .ssh_user.value);
  { echo "all:"; echo "  vars:"; echo "    ansible_user: $ssh_user";
    echo "    ansible_ssh_private_key_file: /tmp/key.pem"; echo "  children:";
    echo "    cluster:"; echo "      hosts:";
    echo "$json"|jq -r ".nodes.value[] | \"        \(.name):\n          ansible_host: \(.public_ip)\n          private_ip: \(.private_ip)\n          node_role: \(.role)\n          node_id: \(.role|ltrimstr(\"node\"))\"";
    echo "    node0:"; echo "      hosts:";
    echo "$json"|jq -r ".nodes.value[]|select(.role==\"node0\")|\"        \(.name): {}\""; } > hosts.yml'
ansible-inventory -i hosts.yml --list >/dev/null && echo "INVENTORY OK"; rm -f hosts.yml
```
Expected: `INVENTORY OK` (Ansible parsed the generated YAML).

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/inventory/terraform_to_inventory.sh bench-infra/inventory/.gitkeep
git commit -m "feat(bench-infra): terraform-output → ansible-inventory generator"
```

---

## Task 4: Ansible scaffolding + matched run parameters

**Files:**
- Create: `bench-infra/ansible/ansible.cfg`
- Create: `bench-infra/ansible/group_vars/all.yml`
- Create: `bench-infra/ansible/provision.yml`
- Create: `bench-infra/ansible/bench.yml`

- [ ] **Step 1: Create `bench-infra/ansible/ansible.cfg`**

```ini
[defaults]
inventory = ../inventory/hosts.yml
host_key_checking = False
roles_path = roles
stdout_callback = yaml
forks = 3
timeout = 60

[ssh_connection]
pipelining = True
```

- [ ] **Step 2: Create `bench-infra/ansible/group_vars/all.yml` (single source of truth, mirrors RUN-PARAMS.md)**

```yaml
# --- Matched sweep (identical for UC and Aeron) ---
rate_ladder: [100, 500, 1000, 2000, 5000, 10000, 20000]  # msgs/s, one run per rung
payload_bytes: 64
warmup_seconds: 2
measure_seconds: 10
batch_size: 1            # per-message pacing (Aeron); UC is open-loop per-message
inflight: 128            # UC --inflight for compared points; Aeron is open-loop
durability: consistent   # 'consistent' = both fsync; 'none' = both non-durable. NEVER mixed.
aeron_ingress: udp       # 'udp' (proven) | 'ipc' (gated behind task13 §11 fix, do not use yet)
idle_strategy: busyspin

# --- Build sources ---
aeron_benchmarks_repo: "https://github.com/aeron-io/benchmarks.git"
aeron_benchmarks_ref: "master"          # pin to a SHA for canonical runs
jdk_version: "21"
uc_src_mode: "rsync"                     # 'rsync' local working tree | 'git'
uc_git_ref: ""                           # used when uc_src_mode=git
uc_repo_url: ""                          # used when uc_src_mode=git
uc_local_path: "{{ playbook_dir }}/../.."  # repo root, for rsync mode

# --- Layout on hosts ---
remote_home: "/opt/bench"
aeron_deploy_dir: "/opt/bench/aeron-deploy"
uc_src_dir: "/opt/bench/uc"
uc_target_bin: "/opt/bench/uc/target/release"

# --- Aeron port bases (matches canonical cluster_localhost ranges) ---
# member fields: ingress, consensus, log, catchup, archive-control
aeron_port_base: { node0: 20000, node1: 21000, node2: 22000 }

# --- UC ports (per node) ---
uc_raft_port: { node0: 7001, node1: 7002, node2: 7003 }
uc_app_id: "uc-bench-dist"

# --- Results ---
results_local_dir: "{{ playbook_dir }}/../../bench-out/dist"
```

- [ ] **Step 3: Create `bench-infra/ansible/provision.yml`**

```yaml
---
- name: Provision bench hosts (tune, build, configure)
  hosts: cluster
  become: true
  gather_facts: true
  roles:
    - os_tune
    - toolchains
    - build_aeron
    - build_uc
    - config
```

- [ ] **Step 4: Create `bench-infra/ansible/bench.yml`**

```yaml
---
- name: Run the parity sweep and collect results
  hosts: cluster
  become: true
  gather_facts: true
  roles:
    - run
    - collect
```

- [ ] **Step 5: Verify both playbooks parse (syntax-check needs a dummy inventory)**

Run:
```bash
cd bench-infra/ansible
printf 'all:\n  hosts:\n    node0: {ansible_host: 127.0.0.1}\n    node1: {ansible_host: 127.0.0.1}\n    node2: {ansible_host: 127.0.0.1}\n  children:\n    cluster:\n      hosts: {node0: {}, node1: {}, node2: {}}\n    node0:\n      hosts: {node0: {}}\n' > /tmp/dummy.yml
ansible-playbook -i /tmp/dummy.yml --syntax-check provision.yml bench.yml
```
Expected: lists both playbooks, no error. (Roles don't exist yet → if syntax-check complains about missing roles, that's expected until Tasks 5–11; re-run this gate at the end of Task 11.)

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/ansible.cfg bench-infra/ansible/group_vars bench-infra/ansible/provision.yml bench-infra/ansible/bench.yml
git commit -m "feat(bench-infra): ansible scaffolding + matched run-params group_vars"
```

---

## Task 5: `os_tune` role — low-latency host posture

**Files:**
- Create: `bench-infra/ansible/roles/os_tune/tasks/main.yml`
- Create: `bench-infra/ansible/roles/os_tune/handlers/main.yml`

- [ ] **Step 1: Create `bench-infra/ansible/roles/os_tune/tasks/main.yml`**

```yaml
---
- name: Install tuning tools
  ansible.builtin.apt:
    name: [linux-tools-common, util-linux, tuned]
    update_cache: true
    state: present
  failed_when: false   # linux-tools-common naming varies; non-fatal

- name: Set CPU governor to performance
  ansible.builtin.shell: |
    if command -v cpupower >/dev/null; then cpupower frequency-set -g performance || true; fi
    for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
      [ -w "$c" ] && echo performance > "$c" || true
    done
  changed_when: false

- name: Disable transparent hugepages
  ansible.builtin.shell: |
    echo never > /sys/kernel/mm/transparent_hugepage/enabled || true
    echo never > /sys/kernel/mm/transparent_hugepage/defrag || true
  changed_when: false

- name: Apply low-latency sysctls
  ansible.posix.sysctl:
    name: "{{ item.k }}"
    value: "{{ item.v }}"
    sysctl_set: true
    reload: true
  loop:
    - { k: "vm.swappiness",        v: "0" }
    - { k: "net.core.rmem_max",    v: "16777216" }
    - { k: "net.core.wmem_max",    v: "16777216" }
    - { k: "net.core.rmem_default", v: "1048576" }
    - { k: "net.core.wmem_default", v: "1048576" }

- name: Raise open-file limits
  ansible.builtin.copy:
    dest: /etc/security/limits.d/99-bench.conf
    content: |
      * soft nofile 1048576
      * hard nofile 1048576
    mode: "0644"

- name: Apply tuned latency profile (best effort)
  ansible.builtin.command: tuned-adm profile latency-performance
  changed_when: false
  failed_when: false
```

- [ ] **Step 2: Create `bench-infra/ansible/roles/os_tune/handlers/main.yml`**

```yaml
---
# No handlers required; sysctl reloads inline. File exists so role layout is complete.
```

- [ ] **Step 3: Verify role syntax (lint if available)**

Run: `cd bench-infra/ansible && ansible-lint roles/os_tune || python3 -c "import yaml,glob; [yaml.safe_load(open(f)) for f in glob.glob('roles/os_tune/**/*.yml', recursive=True)]; print('YAML OK')"`
Expected: `ansible-lint` clean, or `YAML OK` if ansible-lint absent.

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/os_tune
git commit -m "feat(bench-infra): os_tune role (governor, THP off, sysctls, ulimits)"
```

---

## Task 6: `toolchains` role — JDK 21, Rust, build deps

**Files:**
- Create: `bench-infra/ansible/roles/toolchains/tasks/main.yml`

- [ ] **Step 1: Create `bench-infra/ansible/roles/toolchains/tasks/main.yml`**

```yaml
---
- name: Install base build dependencies
  ansible.builtin.apt:
    name:
      - git
      - unzip
      - curl
      - build-essential
      - pkg-config
      - protobuf-compiler
      - "openjdk-{{ jdk_version }}-jdk-headless"
    update_cache: true
    state: present

- name: Detect JAVA_HOME
  ansible.builtin.shell: "dirname $(dirname $(readlink -f $(which javac)))"
  register: java_home_detect
  changed_when: false

- name: Export JAVA_HOME for all shells
  ansible.builtin.copy:
    dest: /etc/profile.d/java.sh
    content: "export JAVA_HOME={{ java_home_detect.stdout }}\nexport PATH=$JAVA_HOME/bin:$PATH\n"
    mode: "0644"

- name: Install rustup + stable toolchain (idempotent)
  ansible.builtin.shell: |
    if ! [ -x "{{ remote_home }}/.cargo/bin/cargo" ]; then
      curl -sSf https://sh.rustup.rs | CARGO_HOME={{ remote_home }}/.cargo RUSTUP_HOME={{ remote_home }}/.rustup sh -s -- -y --default-toolchain stable --profile minimal
    fi
  args:
    creates: "{{ remote_home }}/.cargo/bin/cargo"

- name: Export cargo on PATH
  ansible.builtin.copy:
    dest: /etc/profile.d/cargo.sh
    content: "export CARGO_HOME={{ remote_home }}/.cargo\nexport RUSTUP_HOME={{ remote_home }}/.rustup\nexport PATH={{ remote_home }}/.cargo/bin:$PATH\n"
    mode: "0644"

- name: Record toolchain versions (provenance)
  ansible.builtin.shell: |
    javac -version 2>&1
    {{ remote_home }}/.cargo/bin/rustc --version
  register: tool_versions
  changed_when: false

- name: Show toolchain versions
  ansible.builtin.debug:
    var: tool_versions.stdout_lines
```

- [ ] **Step 2: Verify YAML**

Run: `cd bench-infra/ansible && python3 -c "import yaml; yaml.safe_load(open('roles/toolchains/tasks/main.yml')); print('YAML OK')"`
Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/toolchains
git commit -m "feat(bench-infra): toolchains role (JDK 21, rustup, build deps)"
```

---

## Task 7: `build_aeron` role — clone + deployTar + extract

**Files:**
- Create: `bench-infra/ansible/roles/build_aeron/tasks/main.yml`

- [ ] **Step 1: Create `bench-infra/ansible/roles/build_aeron/tasks/main.yml`**

```yaml
---
- name: Ensure bench home exists
  ansible.builtin.file:
    path: "{{ remote_home }}"
    state: directory
    mode: "0755"

- name: Clone aeron-benchmarks
  ansible.builtin.git:
    repo: "{{ aeron_benchmarks_repo }}"
    dest: "{{ remote_home }}/aeron-benchmarks"
    version: "{{ aeron_benchmarks_ref }}"
    depth: 1
  register: aeron_clone

- name: Build deployTar
  ansible.builtin.shell: ./gradlew clean deployTar -x test --no-daemon --console=plain
  args:
    chdir: "{{ remote_home }}/aeron-benchmarks"
  environment:
    JAVA_HOME: "{{ java_home_detect.stdout | default('/usr/lib/jvm/java-' + jdk_version + '-openjdk-amd64') }}"
  register: deploytar
  changed_when: "'BUILD SUCCESSFUL' in deploytar.stdout"
  failed_when: "'BUILD SUCCESSFUL' not in deploytar.stdout"

- name: Ensure deploy dir exists (pre-extract)
  ansible.builtin.file:
    path: "{{ aeron_deploy_dir }}"
    state: directory
    mode: "0755"

- name: Extract deployTar
  ansible.builtin.unarchive:
    src: "{{ remote_home }}/aeron-benchmarks/build/distributions/benchmarks.tar"
    dest: "{{ aeron_deploy_dir }}"
    remote_src: true
    creates: "{{ aeron_deploy_dir }}/scripts/aeron/cluster-node"

- name: Verify launchers + LoadTestRig present
  ansible.builtin.shell: |
    test -x {{ aeron_deploy_dir }}/scripts/aeron/cluster-node
    test -x {{ aeron_deploy_dir }}/scripts/aeron/media-driver
    test -x {{ aeron_deploy_dir }}/scripts/aeron/cluster-client
    {{ java_home_detect.stdout }}/bin/jar tf {{ aeron_deploy_dir }}/benchmarks-all/build/libs/benchmarks.jar | grep -q 'io/aeron/benchmarks/LoadTestRig.class'
  changed_when: false
```

- [ ] **Step 2: Verify YAML**

Run: `cd bench-infra/ansible && python3 -c "import yaml; yaml.safe_load(open('roles/build_aeron/tasks/main.yml')); print('YAML OK')"`
Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/build_aeron
git commit -m "feat(bench-infra): build_aeron role (clone, deployTar, extract, verify)"
```

---

## Task 8: `build_uc` role — sync source + cargo build

**Files:**
- Create: `bench-infra/ansible/roles/build_uc/tasks/main.yml`

- [ ] **Step 1: Create `bench-infra/ansible/roles/build_uc/tasks/main.yml`**

```yaml
---
- name: (rsync mode) Sync local UC working tree to host
  ansible.posix.synchronize:
    src: "{{ uc_local_path }}/"
    dest: "{{ uc_src_dir }}/"
    rsync_opts:
      - "--exclude=target"
      - "--exclude=.git"
      - "--exclude=bench-out"
  when: uc_src_mode == "rsync"

- name: (git mode) Clone UC at pinned ref
  ansible.builtin.git:
    repo: "{{ uc_repo_url }}"
    dest: "{{ uc_src_dir }}"
    version: "{{ uc_git_ref }}"
  when: uc_src_mode == "git"

- name: Build uc_autobench release bins
  ansible.builtin.command: >
    {{ remote_home }}/.cargo/bin/cargo build --release
    -p uc_autobench --bin uc-node-launch --bin commit-path-load
  args:
    chdir: "{{ uc_src_dir }}"
  environment:
    CARGO_HOME: "{{ remote_home }}/.cargo"
    RUSTUP_HOME: "{{ remote_home }}/.rustup"
  register: uc_build
  changed_when: "'Compiling' in uc_build.stderr or 'Finished' in uc_build.stderr"

- name: Verify UC bins exist
  ansible.builtin.shell: |
    test -x {{ uc_target_bin }}/uc-node-launch
    test -x {{ uc_target_bin }}/commit-path-load
  changed_when: false

- name: Record UC source provenance
  ansible.builtin.shell: |
    cd {{ uc_src_dir }} && (git rev-parse HEAD 2>/dev/null || echo "rsync-no-git"); \
    git status --porcelain 2>/dev/null | head -1 | grep -q . && echo "dirty" || echo "clean"
  register: uc_provenance
  changed_when: false
  run_once: true
```

- [ ] **Step 2: Verify YAML**

Run: `cd bench-infra/ansible && python3 -c "import yaml; yaml.safe_load(open('roles/build_uc/tasks/main.yml')); print('YAML OK')"`
Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/build_uc
git commit -m "feat(bench-infra): build_uc role (rsync|git source, cargo release build)"
```

---

## Task 9: `config` role + templates — private-IP wiring for both systems

**Files:**
- Create: `bench-infra/ansible/roles/config/tasks/main.yml`
- Create: `bench-infra/ansible/roles/config/templates/cluster.properties.j2`
- Create: `bench-infra/ansible/roles/config/templates/node.properties.j2`
- Create: `bench-infra/ansible/roles/config/templates/client.properties.j2`

- [ ] **Step 1: Create `bench-infra/ansible/roles/config/templates/cluster.properties.j2`**

```jinja
# Generated by bench-infra config role. Members use PRIVATE IPs (topology B).
aeron.cluster.members={% for h in groups['cluster'] %}{{ hostvars[h].node_id }},{{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] }},{{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] + 1 }},{{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] + 2 }},{{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] + 3 }},{{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] + 4 }}{% if not loop.last %}|{% endif %}{% endfor %}

aeron.cluster.replication.channel=aeron:udp?endpoint={{ private_ip }}:0
aeron.archive.replication.channel=aeron:udp?endpoint={{ private_ip }}:0
aeron.archive.recording.events.enabled=false
aeron.cluster.appointed.leader.id=0

{% if aeron_ingress == 'udp' %}
aeron.cluster.ingress.channel=aeron:udp
aeron.cluster.ingress.endpoints={% for h in groups['cluster'] %}{{ hostvars[h].node_id }}={{ hostvars[h].private_ip }}:{{ aeron_port_base[hostvars[h].node_role] }}{% if not loop.last %},{% endif %}{% endfor %}

{% else %}
# IPC ingress gated behind task13 §11 fix — do not enable until followers stop crashing.
aeron.cluster.ingress.channel=aeron:ipc
{% endif %}

{% if durability == 'consistent' %}
aeron.archive.file.sync.level=2
aeron.archive.catalog.file.sync.level=2
{% else %}
aeron.archive.file.sync.level=0
aeron.archive.catalog.file.sync.level=0
{% endif %}

aeron.term.buffer.sparse.file=true
aeron.socket.so_sndbuf=128k
aeron.socket.so_rcvbuf=128k
aeron.rcv.initial.window.length=128k
aeron.term.buffer.length=128k
aeron.ipc.term.buffer.length=128k
```

- [ ] **Step 2: Create `bench-infra/ansible/roles/config/templates/node.properties.j2`**

```jinja
# Per-node config for {{ node_role }} ({{ inventory_hostname }})
aeron.dir=/dev/shm/{{ node_role }}-driver
aeron.cluster.dir={{ remote_home }}/cluster/{{ node_role }}/cluster
aeron.cluster.member.id={{ node_id }}
aeron.archive.dir={{ remote_home }}/cluster/{{ node_role }}/archive
aeron.archive.control.channel=aeron:udp?endpoint={{ private_ip }}:{{ aeron_port_base[node_role] + 4 }}
```

- [ ] **Step 3: Create `bench-infra/ansible/roles/config/templates/client.properties.j2`** (rendered on node0 only)

```jinja
# Client co-located with node0; shares node0's media driver.
aeron.dir=/dev/shm/node0-driver
aeron.cluster.egress.channel=aeron:udp?endpoint={{ private_ip }}:0
io.aeron.benchmarks.batch.size={{ batch_size }}
io.aeron.benchmarks.message.length={{ payload_bytes }}
io.aeron.benchmarks.output.directory={{ remote_home }}/results
```

- [ ] **Step 4: Create `bench-infra/ansible/roles/config/tasks/main.yml`**

```yaml
---
- name: Create cluster + results dirs
  ansible.builtin.file:
    path: "{{ item }}"
    state: directory
    mode: "0755"
  loop:
    - "{{ remote_home }}/aeron-cfg"
    - "{{ remote_home }}/results"
    - "{{ remote_home }}/cluster/{{ node_role }}"

- name: Render Aeron cluster.properties (all nodes)
  ansible.builtin.template:
    src: cluster.properties.j2
    dest: "{{ remote_home }}/aeron-cfg/cluster.properties"
    mode: "0644"

- name: Render Aeron node.properties (all nodes)
  ansible.builtin.template:
    src: node.properties.j2
    dest: "{{ remote_home }}/aeron-cfg/node.properties"
    mode: "0644"

- name: Render Aeron client.properties (node0 only)
  ansible.builtin.template:
    src: client.properties.j2
    dest: "{{ remote_home }}/aeron-cfg/client.properties"
    mode: "0644"
  when: node_role == "node0"

- name: Write UC peer args fragment (consumed by run role)
  ansible.builtin.copy:
    dest: "{{ remote_home }}/uc-peers.env"
    mode: "0644"
    content: |
      UC_APP_ID={{ uc_app_id }}
      UC_NODE_ID={{ node_id }}
      UC_LISTEN={{ private_ip }}:{{ uc_raft_port[node_role] }}
      UC_PEERS={% for h in groups['cluster'] %}--peer {{ hostvars[h].node_id }}@{{ hostvars[h].private_ip }}:{{ uc_raft_port[hostvars[h].node_role] }} {% endfor %}
```

- [ ] **Step 5: Verify templates render against the Task 3 fixture**

Run:
```bash
cd bench-infra/ansible
python3 - <<'PY'
import yaml
from jinja2 import Environment
# smoke: ensure templates parse as valid Jinja (no syntax errors)
import glob
env = Environment()
for f in glob.glob('roles/config/templates/*.j2'):
    env.parse(open(f).read())
    print("parsed", f)
print("TEMPLATES OK")
PY
```
Expected: prints each template + `TEMPLATES OK` (no Jinja syntax errors).

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/config
git commit -m "feat(bench-infra): config role + templates (private-IP wiring, Aeron + UC)"
```

---

## Task 10: `run` role — bring up clusters and drive the sweep

**Files:**
- Create: `bench-infra/ansible/roles/run/tasks/main.yml`
- Create: `bench-infra/ansible/roles/run/files/run_aeron_sweep.sh`
- Create: `bench-infra/ansible/roles/run/files/run_uc_sweep.sh`

- [ ] **Step 1: Create `bench-infra/ansible/roles/run/files/run_aeron_sweep.sh`**

```bash
#!/usr/bin/env bash
# Runs on node0. Assumes media drivers + cluster nodes already started cluster-wide
# (the run role starts them per-host). Drives the rate ladder via LoadTestRig.
set -uxo pipefail
HOME_DIR="$1"; shift
RATES="$1"; shift          # comma-separated
PAYLOAD="$1"; shift
WARMUP="$1"; shift
MEASURE="$1"; shift
export JAVA_HOME; JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
export AERON_SCRIPT_HOME="${HOME_DIR}/aeron-deploy/scripts/aeron"
CFG="${HOME_DIR}/aeron-cfg"
mkdir -p "${HOME_DIR}/results"
IFS=',' read -ra RUNGS <<< "$RATES"
for r in "${RUNGS[@]}"; do
  export JVM_OPTS="-Xms64M \
-Dio.aeron.benchmarks.output.directory=${HOME_DIR}/results \
-Dio.aeron.benchmarks.message.rate=${r} \
-Dio.aeron.benchmarks.message.length=${PAYLOAD} \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.warmup.iterations=${WARMUP} \
-Dio.aeron.benchmarks.warmup.message.rate=${r} \
-Dio.aeron.benchmarks.iterations=${MEASURE} \
-Dio.aeron.benchmarks.output.file=aeron_rung_${r}"
  echo "=== aeron rung ${r} ==="
  timeout 180 "${AERON_SCRIPT_HOME}/cluster-client" "${CFG}/cluster.properties" "${CFG}/client.properties" || true
done
```

- [ ] **Step 2: Create `bench-infra/ansible/roles/run/files/run_uc_sweep.sh`**

```bash
#!/usr/bin/env bash
# Runs on node0. Drives commit-path-load against the local UC node's shmem instance.
set -uxo pipefail
BIN="$1"; shift          # path to commit-path-load
INSTANCE_DIR="$1"; shift
APP_ID="$1"; shift
RATES="$1"; shift
PAYLOAD="$1"; shift
INFLIGHT="$1"; shift
MEASURE="$1"; shift
OUT="$1"; shift
"$BIN" --connect "$INSTANCE_DIR" --app-id "$APP_ID" \
  --config dist_3node --rates "$RATES" --inflight "$INFLIGHT" \
  --payload-bytes "$PAYLOAD" --window-secs "$MEASURE" \
  --out "$OUT"
```

- [ ] **Step 3: Create `bench-infra/ansible/roles/run/tasks/main.yml`**

```yaml
---
# ---- Aeron: start drivers + nodes on every host, sweep on node0, then tear down ----
- name: Start Aeron media driver
  ansible.builtin.shell: |
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
    export JVM_OPTS="-Xms16M"
    setsid {{ aeron_deploy_dir }}/scripts/aeron/media-driver \
      {{ remote_home }}/aeron-cfg/cluster.properties \
      {{ remote_home }}/aeron-cfg/node.properties > {{ remote_home }}/md.out 2>&1 < /dev/null &
  changed_when: false

- name: Start Aeron cluster node
  ansible.builtin.shell: |
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
    export JVM_OPTS="-Xms16M -Dio.aeron.benchmarks.output.directory={{ remote_home }}/results"
    setsid {{ aeron_deploy_dir }}/scripts/aeron/cluster-node \
      {{ remote_home }}/aeron-cfg/cluster.properties \
      {{ remote_home }}/aeron-cfg/node.properties > {{ remote_home }}/node.out 2>&1 < /dev/null &
  changed_when: false

- name: Wait for cluster election to settle
  ansible.builtin.pause:
    seconds: 20
  run_once: true

- name: Copy Aeron sweep script to node0
  ansible.builtin.copy:
    src: run_aeron_sweep.sh
    dest: "{{ remote_home }}/run_aeron_sweep.sh"
    mode: "0755"
  when: node_role == "node0"

- name: Drive Aeron rate ladder (node0)
  ansible.builtin.command: >
    {{ remote_home }}/run_aeron_sweep.sh
    {{ remote_home }}
    {{ rate_ladder | join(',') }}
    {{ payload_bytes }} {{ warmup_seconds }} {{ measure_seconds }}
  when: node_role == "node0"
  changed_when: true

- name: Stop Aeron JVMs
  ansible.builtin.shell: "pkill -9 -f 'io.aeron' || true"
  changed_when: false

# ---- UC: start one uc-node per host, sweep on node0, then tear down ----
- name: Start UC node
  ansible.builtin.shell: |
    source {{ remote_home }}/uc-peers.env
    setsid {{ uc_target_bin }}/uc-node-launch \
      --node-id $UC_NODE_ID --listen $UC_LISTEN $UC_PEERS \
      --app-id $UC_APP_ID --with-service \
      --instance-dir /dev/shm/uc-{{ node_role }} \
      --data-dir {{ remote_home }}/uc-data \
      > {{ remote_home }}/uc-node.out 2>&1 < /dev/null &
  changed_when: false

- name: Wait for UC leader election
  ansible.builtin.pause:
    seconds: 15
  run_once: true

- name: Copy UC sweep script to node0
  ansible.builtin.copy:
    src: run_uc_sweep.sh
    dest: "{{ remote_home }}/run_uc_sweep.sh"
    mode: "0755"
  when: node_role == "node0"

- name: Drive UC rate ladder (node0)
  ansible.builtin.command: >
    {{ remote_home }}/run_uc_sweep.sh
    {{ uc_target_bin }}/commit-path-load
    /dev/shm/uc-node0
    {{ uc_app_id }}
    {{ rate_ladder | join(',') }}
    {{ payload_bytes }} {{ inflight }} {{ measure_seconds }}
    {{ remote_home }}/results/uc_sweep.csv
  when: node_role == "node0"
  changed_when: true

- name: Stop UC JVMs/procs
  ansible.builtin.shell: "pkill -9 -f 'uc-node-launch' || true; pkill -9 -f 'kv_service' || true"
  changed_when: false
```

- [ ] **Step 4: Shellcheck the sweep scripts + verify YAML**

Run:
```bash
cd bench-infra/ansible
shellcheck roles/run/files/*.sh || bash -n roles/run/files/run_aeron_sweep.sh && bash -n roles/run/files/run_uc_sweep.sh
python3 -c "import yaml; yaml.safe_load(open('roles/run/tasks/main.yml')); print('YAML OK')"
```
Expected: shellcheck clean (or `bash -n` silent) + `YAML OK`.

- [ ] **Step 5: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/run
git commit -m "feat(bench-infra): run role (start clusters, drive UC+Aeron rate ladders)"
```

---

## Task 11: `collect` role — pull results + provenance manifest

**Files:**
- Create: `bench-infra/ansible/roles/collect/tasks/main.yml`

- [ ] **Step 1: Create `bench-infra/ansible/roles/collect/tasks/main.yml`**

```yaml
---
- name: Compute run timestamp (once)
  ansible.builtin.command: date +%Y%m%dT%H%M%SZ
  register: run_ts
  run_once: true
  changed_when: false

- name: Write provenance manifest on node0
  ansible.builtin.copy:
    dest: "{{ remote_home }}/results/manifest.txt"
    mode: "0644"
    content: |
      timestamp={{ run_ts.stdout }}
      cloud_role={{ node_role }}
      instance={{ ansible_processor_vcpus | default('?') }}vcpu {{ ansible_memtotal_mb | default('?') }}MB
      kernel={{ ansible_kernel }}
      durability={{ durability }}
      aeron_ingress={{ aeron_ingress }}
      rate_ladder={{ rate_ladder | join(',') }}
      payload_bytes={{ payload_bytes }}
      inflight={{ inflight }}
      uc_provenance={{ hostvars[groups['node0'][0]].uc_provenance.stdout | default('n/a') }}
      aeron_benchmarks_ref={{ aeron_benchmarks_ref }}
  when: node_role == "node0"

- name: Fetch results to local bench-out/dist/<ts>
  ansible.posix.synchronize:
    mode: pull
    src: "{{ remote_home }}/results/"
    dest: "{{ results_local_dir }}/{{ hostvars[groups['node0'][0]].run_ts.stdout }}/{{ node_role }}/"
  changed_when: true

- name: Report collected location
  ansible.builtin.debug:
    msg: "Results pulled to {{ results_local_dir }}/{{ hostvars[groups['node0'][0]].run_ts.stdout }}/"
  run_once: true
```

- [ ] **Step 2: Verify YAML + re-run the full playbook syntax-check from Task 4 Step 5**

Run:
```bash
cd bench-infra/ansible
python3 -c "import yaml; yaml.safe_load(open('roles/collect/tasks/main.yml')); print('YAML OK')"
ansible-playbook -i /tmp/dummy.yml --syntax-check provision.yml bench.yml
```
Expected: `YAML OK` and both playbooks pass syntax-check now that all roles exist.

- [ ] **Step 3: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/ansible/roles/collect
git commit -m "feat(bench-infra): collect role (pull results + provenance manifest)"
```

---

## Task 12: Makefile orchestration + TTL cost guard

**Files:**
- Create: `bench-infra/Makefile`
- Create: `bench-infra/example.tfvars`

- [ ] **Step 1: Create `bench-infra/example.tfvars`**

```hcl
cloud                = "hetzner"
instance_type        = ""               # "" = module default (ccx33)
region               = ""               # "" = module default
ssh_public_key       = "ssh-ed25519 AAAA... you@host"
ssh_private_key_file = "~/.ssh/id_ed25519"
allow_ssh_cidr       = "203.0.113.4/32" # your IP/32
ttl_hours            = 4
owner                = "uc-bench"
```

- [ ] **Step 2: Create `bench-infra/Makefile`**

```makefile
TF      := terraform -chdir=terraform
TFVARS  ?= terraform.tfvars
ANSIBLE := ansible-playbook
SSH_KEY := $(shell awk -F'"' '/ssh_private_key_file/{print $$2}' $(TFVARS))

.PHONY: init up provision bench bench-oneshot status destroy ssh-node0 inventory

init:
	$(TF) init

up: ## provision infra + configure hosts
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml

inventory:
	SSH_PRIVATE_KEY_FILE=$(SSH_KEY) ./inventory/terraform_to_inventory.sh

bench: ## run the sweep + collect (hosts must be up)
	cd ansible && $(ANSIBLE) bench.yml

bench-oneshot: up bench destroy ## clean-room one-shot run

status: ## list instances + warn if past TTL
	@$(TF) output -json nodes | jq -r '.[] | "\(.name)\t\(.public_ip)\t\(.private_ip)\t\(.role)"'
	@echo "TTL guard: check instance uptime against ttl_hours in $(TFVARS)."
	@ttl=$$(awk '/ttl_hours/{print $$3}' ../$(TFVARS) 2>/dev/null || echo 4); \
	 cd ansible && ansible cluster -m shell -a "echo $$(hostname): up $$(awk '{print int($$1/3600)}' /proc/uptime)h" 2>/dev/null || true

destroy: ## tear everything down
	$(TF) destroy -auto-approve -var-file=../$(TFVARS)
	rm -f inventory/hosts.yml

ssh-node0:
	@ip=$$($(TF) output -json nodes | jq -r '.[]|select(.role=="node0").public_ip'); \
	 user=$$($(TF) output -raw ssh_user); ssh -i $(SSH_KEY) $$user@$$ip
```

- [ ] **Step 3: Optional on-host self-destruct backstop (append to `os_tune` later if wanted)**

Add this note to the Makefile header comment so operators know the guard is advisory:
```bash
# Cost guard is advisory: `make status` reports uptime; `make destroy` tears down.
# For a hard backstop, set ttl_hours and add an at-job in provision (out of scope here).
```
Verify the Makefile parses:

Run: `cd bench-infra && make -n status` (with a placeholder `terraform.tfvars` copied from example)
Expected: prints the commands `status` would run, no make syntax error. (`make -n` dry-runs.)

- [ ] **Step 4: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/Makefile bench-infra/example.tfvars
git commit -m "feat(bench-infra): Makefile (up/bench/bench-oneshot/status/destroy) + TTL guard"
```

---

## Task 13: AWS Terraform module (same contract)

**Files:**
- Create: `bench-infra/terraform/modules/aws/variables.tf`
- Create: `bench-infra/terraform/modules/aws/main.tf`
- Create: `bench-infra/terraform/modules/aws/outputs.tf`
- Modify: `bench-infra/terraform/main.tf` (wire the aws module + extend `active_module`)

- [ ] **Step 1: Create `bench-infra/terraform/modules/aws/variables.tf`**

```hcl
variable "node_count"     { type = number }
variable "instance_type"  { type = string }
variable "region"         { type = string }
variable "ssh_public_key" { type = string }
variable "allow_ssh_cidr" { type = string }
variable "ttl_hours"      { type = number }
variable "owner"          { type = string }

locals {
  instance_type = var.instance_type != "" ? var.instance_type : "c7i.4xlarge"
  region        = var.region != "" ? var.region : "us-east-1"
}
```

- [ ] **Step 2: Create `bench-infra/terraform/modules/aws/main.tf`**

```hcl
provider "aws" {
  region = local.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${var.owner}-vpc", owner = var.owner }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.10.1.0/24"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  name   = "${var.owner}-sg"
  vpc_id = aws_vpc.bench.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allow_ssh_cidr]
  }
  ingress {
    description = "intra-cluster"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "bench" {
  key_name   = "${var.owner}-key"
  public_key = var.ssh_public_key
}

resource "aws_placement_group" "bench" {
  name     = "${var.owner}-pg"
  strategy = "cluster"
}

resource "aws_instance" "node" {
  count                  = var.node_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = local.instance_type
  subnet_id              = aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  placement_group        = aws_placement_group.bench.id
  private_ip             = "10.10.1.${count.index + 10}"
  tags = {
    Name      = "${var.owner}-node${count.index}"
    owner     = var.owner
    ttl_hours = tostring(var.ttl_hours)
    role      = "node${count.index}"
  }
}
```

- [ ] **Step 3: Create `bench-infra/terraform/modules/aws/outputs.tf`**

```hcl
output "nodes" {
  value = [
    for i, s in aws_instance.node : {
      name       = s.tags["Name"]
      role       = "node${i}"
      public_ip  = s.public_ip
      private_ip = s.private_ip
    }
  ]
}

output "ssh_user" {
  value = "ubuntu"
}
```

- [ ] **Step 4: Wire the aws module in `bench-infra/terraform/main.tf`**

Add this `module` block after the `module "hetzner"` block:
```hcl
module "aws" {
  source = "./modules/aws"
  count  = local.enable_aws ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}
```
And replace the `active_module` local with:
```hcl
locals {
  active_module = (
    local.enable_hetzner ? module.hetzner[0] :
    local.enable_aws ? module.aws[0] :
    null
  )
}
```

- [ ] **Step 5: Validate**

Run: `cd bench-infra/terraform && terraform init -backend=false && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/terraform/modules/aws bench-infra/terraform/main.tf
git commit -m "feat(bench-infra): aws terraform module (EC2 + VPC + placement group)"
```

---

## Task 14: GCP Terraform module (same contract)

**Files:**
- Create: `bench-infra/terraform/modules/gcp/variables.tf`
- Create: `bench-infra/terraform/modules/gcp/main.tf`
- Create: `bench-infra/terraform/modules/gcp/outputs.tf`
- Modify: `bench-infra/terraform/main.tf` (wire the gcp module + extend `active_module`)

- [ ] **Step 1: Create `bench-infra/terraform/modules/gcp/variables.tf`**

```hcl
variable "node_count"     { type = number }
variable "instance_type"  { type = string }
variable "region"         { type = string }
variable "ssh_public_key" { type = string }
variable "allow_ssh_cidr" { type = string }
variable "ttl_hours"      { type = number }
variable "owner"          { type = string }

locals {
  machine_type = var.instance_type != "" ? var.instance_type : "c3-highcpu-8"
  region       = var.region != "" ? var.region : "us-central1"
  zone         = "${local.region}-a"
}
```

- [ ] **Step 2: Create `bench-infra/terraform/modules/gcp/main.tf`**

```hcl
# GOOGLE_PROJECT (or provider project) + GOOGLE_APPLICATION_CREDENTIALS from env.
provider "google" {
  region = local.region
  zone   = local.zone
}

resource "google_compute_network" "bench" {
  name                    = "${var.owner}-net"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "bench" {
  name          = "${var.owner}-subnet"
  ip_cidr_range = "10.10.1.0/24"
  region        = local.region
  network       = google_compute_network.bench.id
}

resource "google_compute_firewall" "ssh" {
  name          = "${var.owner}-ssh"
  network       = google_compute_network.bench.name
  source_ranges = [var.allow_ssh_cidr]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "intra" {
  name          = "${var.owner}-intra"
  network       = google_compute_network.bench.name
  source_ranges = ["10.10.1.0/24"]
  allow { protocol = "all" }
}

resource "google_compute_resource_policy" "compact" {
  name   = "${var.owner}-compact"
  region = local.region
  group_placement_policy {
    collocation = "COLLOCATED"
  }
}

resource "google_compute_instance" "node" {
  count        = var.node_count
  name         = "${var.owner}-node${count.index}"
  machine_type = local.machine_type
  zone         = local.zone

  boot_disk {
    initialize_params { image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64" }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.bench.id
    network_ip = "10.10.1.${count.index + 10}"
    access_config {} # ephemeral public IP
  }

  metadata = {
    ssh-keys = "ubuntu:${var.ssh_public_key}"
  }

  labels = {
    owner     = var.owner
    ttl_hours = tostring(var.ttl_hours)
    role      = "node${count.index}"
  }

  resource_policies = [google_compute_resource_policy.compact.id]
}
```

- [ ] **Step 3: Create `bench-infra/terraform/modules/gcp/outputs.tf`**

```hcl
output "nodes" {
  value = [
    for i, s in google_compute_instance.node : {
      name       = s.name
      role       = "node${i}"
      public_ip  = s.network_interface[0].access_config[0].nat_ip
      private_ip = s.network_interface[0].network_ip
    }
  ]
}

output "ssh_user" {
  value = "ubuntu"
}
```

- [ ] **Step 4: Wire the gcp module in `bench-infra/terraform/main.tf`**

Add after the `module "aws"` block:
```hcl
module "gcp" {
  source = "./modules/gcp"
  count  = local.enable_gcp ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}
```
And replace `active_module` with:
```hcl
locals {
  active_module = (
    local.enable_hetzner ? module.hetzner[0] :
    local.enable_aws ? module.aws[0] :
    local.enable_gcp ? module.gcp[0] :
    null
  )
}
```

- [ ] **Step 5: Validate**

Run: `cd bench-infra/terraform && terraform init -backend=false && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/terraform/modules/gcp bench-infra/terraform/main.tf
git commit -m "feat(bench-infra): gcp terraform module (GCE + VPC + compact placement)"
```

---

## Task 15: Live end-to-end smoke (Hetzner) + README

**Files:**
- Create: `bench-infra/README.md`

**This task requires a real `HCLOUD_TOKEN` and an SSH key. It actually spends money (~$0.27, one Hetzner hour).**

- [ ] **Step 1: Create `bench-infra/terraform.tfvars` from the example (gitignored)**

```bash
cd bench-infra
cp example.tfvars terraform.tfvars
# edit: ssh_public_key, ssh_private_key_file, allow_ssh_cidr (your IP/32)
```

- [ ] **Step 2: `make up` — provision + configure**

Run:
```bash
cd bench-infra
export HCLOUD_TOKEN=<token>
make init
make up
```
Expected: Terraform creates 3 servers + net + fw; inventory written; Ansible provision completes with all roles green (toolchain versions printed, UC + Aeron build verifications pass). Wall-clock ~12–18 min (cold builds).

- [ ] **Step 3: `make bench` — run sweep + collect**

Run: `make bench`
Expected: UC and Aeron sweeps run; results pulled to `bench-out/dist/<ts>/{node0,node1,node2}/` with `manifest.txt`. Confirm:
```bash
ls bench-out/dist/*/node0/
```
Expected: `aeron_rung_*.hgrm` (or `.hdr`), `uc_sweep.csv`, `manifest.txt`.

- [ ] **Step 4: Sanity-check the numbers are NOT host-starved**

Run:
```bash
cat bench-out/dist/*/node0/manifest.txt
# UC: lower rungs should show sub-ms..few-ms p50, NOT a flat ~100ms floor
column -t -s, bench-out/dist/*/node0/uc_sweep.csv | head
```
Expected: UC p50 at 500/s in the low-ms range; Aeron rungs present without `.FAIL` at sustainable rates. (If Aeron shows the flat ~100 ms floor, the host is still under-provisioned — bump `instance_type`.)

- [ ] **Step 5: `make destroy` — tear down**

Run: `make destroy`
Expected: all resources destroyed; `terraform output` empty; no lingering instances (`make status` errors cleanly / hcloud console empty).

- [ ] **Step 6: Create `bench-infra/README.md`**

```markdown
# bench-infra — provisioning rig for Aeron-vs-UC parity benchmarking

Provisions 3 dedicated-vCPU hosts (Hetzner default; AWS/GCP via `cloud=`),
configures toolchains/builds/configs/OS-tuning, runs the matched parity sweep, and
pulls results to `bench-out/dist/<ts>/`. See the design at
`docs/superpowers/specs/2026-06-14-bench-infra-terraform-ansible-design.md`.

## Prereqs
- terraform >= 1.6, ansible, jq, an SSH keypair.
- Credentials for your chosen cloud:
  - Hetzner: `export HCLOUD_TOKEN=...`
  - AWS: standard provider chain (`AWS_PROFILE` / env).
  - GCP: `GOOGLE_APPLICATION_CREDENTIALS` + `GOOGLE_PROJECT`.

## Quickstart
    cp example.tfvars terraform.tfvars   # edit ssh + allow_ssh_cidr
    make init
    make up            # provision + configure (~15 min, cold builds)
    make bench         # run sweep + collect to bench-out/dist/<ts>/
    make destroy       # tear down

One-shot: `make bench-oneshot` (up → bench → destroy). Persistent: `make up` once,
`make bench` repeatedly, `make ssh-node0` to investigate, `make destroy` when done.
`make status` lists hosts + uptime (cost guard).

## Switching cloud
Set `cloud = "aws"` (or `"gcp"`) in `terraform.tfvars`. Everything else is identical;
Ansible is cloud-agnostic.

## Matched run parameters
Edit `ansible/group_vars/all.yml` — rate ladder, payload, durability posture
(`consistent` = both fsync; `none` = both non-durable; never mix), inflight. Mirrors
`uc_autobench/bench-parity/RUN-PARAMS.md`.

## Known limitation
`aeron_ingress: ipc` (shmem client edge) is gated behind the task13 §11 follower-crash
fix; default `udp` (client edge = UDP-loopback on node0). UC always gets its shmem edge.
```

- [ ] **Step 7: Commit**

```bash
cd "$(git rev-parse --show-toplevel)"
git add bench-infra/README.md
git commit -m "docs(bench-infra): README + verified Hetzner end-to-end smoke"
```

---

## Self-review notes (for the executor)

- **Aeron IPC ingress** stays disabled (`aeron_ingress: udp`) until task13 §11 is fixed; the template supports `ipc` but the run will crash followers if switched on prematurely.
- **commit-path-load `--connect`** path is the UC node0 shmem instance dir (`/dev/shm/uc-node0`); it must match the `--instance-dir` the run role passes to `uc-node-launch`.
- **Durability matching** is enforced by a single `durability` var feeding both the Aeron template (`file.sync.level`) and UC (`uc-node-launch` uses `Durability::Consistent`; for `none`, a follow-up adds an env/flag — note this if you set `durability: none`).
- **Cost guard** is advisory (`make status` + `make destroy`); a hard on-host self-poweroff is noted as out-of-scope.
