# Start a cluster on an AWS fleet — the full walk

One linear recipe: three empty AWS hosts to a serving, verified UC cluster,
then a clean teardown. Written to be followed step by step by a person or an
AI agent — **every step ends with a check; do not proceed past a failed
check.** Costs real money while the fleet is up (three 4xlarge-class hosts ≈
a few dollars/hour; nothing auto-reaps them — teardown is step 8).

In a hurry? [Fleet start, the quick way](fleet_start_quick.md) is steps 3–7
of this page as one script; this page is what the script does, and the path
to follow when it fails or your setup diverges.

This uses the repo's own provisioning rig (`bench-infra/`, terraform +
ansible) for the hosts and the **signed release tarball** for the software.
If you already have machines, skip to step 4 with
[Run a cluster on real hosts](run-a-cluster.md) instead. Deeper explanations
live there and are linked per step; this page is deliberately just the rails.

## 0. Prerequisites

On the control machine (your laptop / a box that will run terraform):
`terraform` ≥ 1.6, `ansible-core` ≥ 2.16 (+ `ansible.posix`,
`community.general` collections), `jq`, `rsync`, an SSH keypair, and AWS
credentials for an account allowed to run 3 × 16-vCPU on-demand instances.
Non-root install commands for all of these:
[`bench-infra/README.md`](/bench-infra/README.md).

**Check:** `terraform version && ansible --version && jq --version` all print.

## 1. Configure the fleet

```bash
cd bench-infra
cp example.aws.tfvars terraform.tfvars
```

Edit `terraform.tfvars`:

- `cloud = "aws"`, `region` as desired.
- `instance_type`: pick with [Size a host](size-a-host.md) —
  `c9gd.4xlarge` (Graviton) and `c8id.4xlarge` (Intel) are the measured
  ones; the AMI architecture derives from the type automatically. Prefer a
  `d` type: local NVMe.
- `node_count = 3`.
- `ssh_public_key` / `ssh_private_key_file`: your keypair.
- `allow_ssh_cidr = "<your-ip>/32"` — never `0.0.0.0/0`.

Credentials go in a gitignored `.env` beside the Makefile
(`AWS_ACCESS_KEY_ID=...`, `AWS_SECRET_ACCESS_KEY=...`).

**Check:** `make init` ends with "Terraform has been successfully initialized".

## 2. Provision

```bash
make up-uc
```

Terraform creates VPC + 3 hosts in one placement group (~2 min); ansible
tunes the OS, mounts the instance-store NVMe at `/opt/bench`, and installs
toolchains (~5–15 min).

**Check:** the ansible `PLAY RECAP` shows `failed=0` for every host, and
`make status` prints three rows with public + private IPs. Record them; the
steps below call the three private IPs `P0 P1 P2` and the public ones
`H0 H1 H2`. All ssh below is `ssh -i <your-key> ubuntu@H<i>`.

## 3. Install the release on every host

On **each** of the three hosts (pick the tarball matching the host: `aarch64`
for Graviton types, `x86_64` for Intel/AMD; substitute the current version):

```bash
VER=2.10.0; ARCH=$(uname -m)  # aarch64 or x86_64
curl -fsSLO "https://github.com/PeterKnego/ultima_cluster/releases/download/v${VER}/uc2-${VER}-${ARCH}-unknown-linux-gnu.tar.gz"
curl -fsSLO "https://github.com/PeterKnego/ultima_cluster/releases/download/v${VER}/uc2-${VER}-${ARCH}-unknown-linux-gnu.tar.gz.sha256"
sha256sum -c "uc2-${VER}-${ARCH}-unknown-linux-gnu.tar.gz.sha256"
tar xzf "uc2-${VER}-${ARCH}-unknown-linux-gnu.tar.gz"
sudo install -m 0755 uc2-${VER}-*/bin/* /usr/local/bin/
```

(Signature verification with cosign: `packaging/README-release.md` in the
tarball. Building from source instead: the provisioned hosts already carry
the toolchain and the synced tree at `/opt/bench/uc`.)

**Check (per host):** `sha256sum -c` prints `OK`, and
`uc2-node --version && uc2ctl --version` print the version.

## 4. One admin key, shared by all hosts

On host 0:

```bash
sudo mkdir -p /etc/uc2
sudo uc2ctl gen-admin-key /etc/uc2/admin.key
```

Copy `/etc/uc2/admin.key` to the same path on hosts 1 and 2 (e.g. `scp`
through the control machine). Membership changes must be signed with it;
every node verifies against it.

**Check (per host):** `sudo test -s /etc/uc2/admin.key && echo present`.

## 5. Configure and start the nodes

On **each** host `i` (0, 1, 2), put the instance dir on the NVMe and write
`/etc/uc2/node.toml` — identical `[[members]]` everywhere, only `id`,
`bind` and the dir differ ([why](run-a-cluster.md)); `bind` must equal this
node's own members entry exactly, and the members addresses are the
**private** IPs:

```bash
sudo mkdir -p /opt/bench/uc2/n$i && sudo ln -sfn /opt/bench/uc2 /srv/uc2
sudo tee /etc/uc2/node.toml >/dev/null <<EOF
id = $i
bind = "P$i:9100"
instance_dir = "/srv/uc2/n$i"
app_id = "myapp"

[[members]]
id = 0
addr = "P0:9100"

[[members]]
id = 1
addr = "P1:9100"

[[members]]
id = 2
addr = "P2:9100"

# Both sections are REQUIRED; an absent one is a startup refusal by name.
# Cleartext peer traffic trusts the network — fine inside this fleet's
# security group (it admits only the members). Before any wider network:
# docs/how-to/encrypt-node-traffic.md (a flag day, easiest done NOW).
[crypto]
enabled = false

[admin]
auth = "hmac"
keys = [{ name = "admin", key_path = "/etc/uc2/admin.key" }]
EOF
```

(replace `P$i`/`P0`/`P1`/`P2` with the actual private IPs). Then install and
start the daemon, from the extracted tarball dir:

```bash
sudo install -m 0644 uc2-*/packaging/systemd/uc2-node.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now uc2-node
```

**Check (per host):** `systemctl is-active uc2-node` prints `active`. A
refusal instead? `journalctl -u uc2-node -n 20` names the reason — the config, address and
disk rules it enforces are
[Run a cluster on real hosts](run-a-cluster.md)'s middle sections. Then, once on
any host: `sudo uc2ctl status --instance-dir /srv/uc2/n$i --app-id myapp`
shows **one leader and three members**.

## 6. Start the state machine (the demo counter service)

The node replicates; a service applies. On **each** host `i`:

```bash
sudo sed "s|/srv/uc2/n0|/srv/uc2/n$i|" uc2-*/packaging/systemd/uc2-service@.service | sudo tee /etc/systemd/system/uc2-service@.service >/dev/null
sudo systemctl daemon-reload && sudo systemctl enable --now uc2-service@counter-service
```

**Check (per host):** `systemctl is-active uc2-service@counter-service`
prints `active`. (Your own application replaces `counter-service` later:
[Write a service binary](write-a-service-binary.md).)

## 7. Prove it end to end

Front the cluster with a gateway on **every** host — a gateway on a follower
redirects the client to the leader's gateway, so one lone gateway only works
while its own node happens to lead (the validation run hit exactly that: the
leader moved, the write timed out). On **each** host `i`:

```bash
sudo tee /etc/uc2/gw.toml >/dev/null <<EOF
[local]
instance_dir = "/srv/uc2/n$i"
app_id = "myapp"
listen = "P$i:9200"

[[members]]
node_id = 0
gateway = "P0:9200"

[[members]]
node_id = 1
gateway = "P1:9200"

[[members]]
node_id = 2
gateway = "P2:9200"

[session]
envelope = false
EOF
sudo systemd-run --unit=uc2-gw --collect uc2-gateway --config /etc/uc2/gw.toml
```

(private IPs again; the fleet's security group already admits them to each
other). Then, from any host, drive it with the demo remote client, giving it
**all three** gateways:

```bash
counter-remote --gateways P0:9200,P1:9200,P2:9200 --app-id myapp add 5
counter-remote --gateways P0:9200,P1:9200,P2:9200 --app-id myapp get --linearizable
```

**Check:** `get` prints `value=` summing what `add` wrote (run `add` twice,
watch it climb); `--linearizable` routes the read through the cluster's
read barrier, so it reflects every acknowledged write — whichever host
leads, and whichever gateway answered. That write went client → gateway → leader → quorum-durable commit →
apply on every host.

A performance note before you benchmark what you just built: this proof
drives the **remote** path (TCP through the gateway), which is the adoption
path but not the one the headline numbers were measured on — those used the
local shared-memory client on the leader host. The remote path's measured
toll is 0.62× direct per connection / 0.84× aggregate
([M13 gate](../benchmarks/uc2-m13-gate-2026-08-24.md)); comparing a
`counter-remote` loop against the [architecture sweep](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)
numbers is comparing different paths. Kill the leader's node process and rerun `get`
if you want to watch failover do its job — the client follows the redirect
to wherever the new leader's gateway is.

## 8. Tear down (or commit to keeping it)

```bash
cd bench-infra
make destroy
terraform -chdir=terraform state list   # MUST print nothing
```

**Check:** the state list is empty. Nothing auto-reaps a forgotten fleet —
if a run above failed, still do this step. Keeping the cluster instead?
The must-reads, in order: [encrypt node traffic](encrypt-node-traffic.md)
(crypto is OFF above), [monitor](monitor-a-cluster.md) (add `[metrics]`),
[back up](back-up-a-cluster.md), and
[bound the journal](bound-journal-growth.md) (purge is OFF by default).

## When a check fails

| Symptom | Where to look |
|---|---|
| a named startup refusal in `journalctl -u uc2-node` | [run-a-cluster.md](run-a-cluster.md) — bind/members mismatch, missing `[crypto]`/`[admin]`, tmpfs instance dir, full disk |
| provision fails / hosts unreachable | `allow_ssh_cidr` is your current IP? vCPU quota covers 3 × the type? |
| `uc2ctl status` shows no leader | peers can't reach UDP 9100 on the **private** IPs — wrong IPs in `[[members]]`, or nodes in different networks |
| anything else | [Diagnose a node](diagnose-a-node.md) |
