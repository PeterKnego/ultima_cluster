# bench-infra — provisioning rig for Aeron-vs-UC parity benchmarking

Provisions 3 dedicated-vCPU hosts (Hetzner default; AWS/GCP via `cloud=`),
configures toolchains/builds/configs/OS-tuning, runs the matched parity sweep, and
pulls results to `bench-out/dist/<ts>/`. See the design at
`docs/superpowers/specs/2026-06-14-bench-infra-terraform-ansible-design.md`.

## Control-machine setup

These tools must be on the machine that *runs* the rig (your laptop / a control box) —
**not** the provisioned hosts (those get JDK + Rust installed by the `toolchains` role).

| Tool | Min version | Used by |
|------|-------------|---------|
| `terraform` | >= 1.6 (tested 1.9.8) | `make init/up/destroy/status` |
| `ansible` (ansible-core) | >= 2.16 (tested 2.21) | `make up/bench` |
| collections `ansible.posix`, `community.general` | latest | `sysctl`, `synchronize` (rsync), misc modules |
| `jq` | any | inventory generator + `make status/ssh-node0` |
| `rsync` | any | `synchronize` (UC source sync + result pull) |
| an SSH keypair | — | host access (path goes in `terraform.tfvars`) |
| `shellcheck` | any (optional) | linting the sweep scripts during development |

Install on a fresh Debian/Ubuntu control box **without root** (binaries into `~/.local/bin`,
which must be on `PATH`):

```bash
mkdir -p ~/.local/bin

# terraform (pinned binary)
curl -fsSL -o /tmp/tf.zip https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip
unzip -o /tmp/tf.zip terraform -d ~/.local/bin

# shellcheck (optional, dev-only)
curl -fsSL https://github.com/koalaman/shellcheck/releases/download/v0.10.0/shellcheck-v0.10.0.linux.x86_64.tar.xz \
  | tar -xJ -C /tmp && cp /tmp/shellcheck-v0.10.0/shellcheck ~/.local/bin/

# ansible-core + required collections (via pip).
# NOTE: on PEP-668 "externally-managed" Pythons (Ubuntu 24.04, Python 3.12+) pip
# needs --break-system-packages, and if pip itself is missing, bootstrap it first:
#   curl -fsSL https://bootstrap.pypa.io/get-pip.py | python3 - --user --break-system-packages
python3 -m pip install --user --break-system-packages ansible-core
ansible-galaxy collection install ansible.posix community.general

# jq + rsync (system packages; need root, or use your distro's installer)
sudo apt-get install -y jq rsync   # or: brew install jq (macOS; rsync is preinstalled)
```

With root available, the simpler path is `apt-get install -y terraform ansible jq rsync shellcheck`
(terraform via the HashiCorp apt repo) and `ansible-galaxy collection install ...`.

## Credentials (per chosen cloud)
Put them in a gitignored `bench-infra/.env` — the Makefile auto-loads it and
exports the vars into terraform/ansible, so no manual `export` is needed:

    cp .env.example .env   # then fill in
    # .env: bare KEY=value, e.g.  HCLOUD_TOKEN=abc123...   (no surrounding quotes)

- Hetzner: `HCLOUD_TOKEN=...`
- AWS: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or just use `AWS_PROFILE` / the standard provider chain).
- GCP: `GOOGLE_PROJECT=...` + `GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json`.

> `.env` uses `KEY=value` (Make include syntax, not shell-sourced). Prefer bare
> values: unlike shell dotenv loaders, Make keeps surrounding quotes literally, so
> `HCLOUD_TOKEN="abc"` would reach terraform *with* the quotes. The Makefile strips
> surrounding double-quotes from the known cred vars as a safety net, but bare is
> cleanest. You can still `export HCLOUD_TOKEN=...` in your shell instead of `.env`.

> Note: `terraform init` downloads all three provider plugins (hcloud, aws, google) regardless of `cloud`. For `cloud=gcp` you must also set `GOOGLE_PROJECT`. A Hetzner-only run needs only `HCLOUD_TOKEN`.

## Quickstart
    cp example.tfvars terraform.tfvars   # edit ssh + allow_ssh_cidr
    make init
    make up            # provision + configure (~15 min, cold builds)
    make bench         # run sweep + collect to bench-out/dist/<ts>/
    make destroy       # tear down

One-shot: `make bench-oneshot` (up → bench → destroy). Persistent: `make up` once,
`make bench` repeatedly, `make ssh-node0` to investigate, `make destroy` when done.
`make status` lists hosts + uptime (cost guard).

Cross-host ping harness: `make up-ping` provisions a 2-host `ccx13` fleet and runs
`netping.yml`, standing up PERSISTENT UC UDP/QUIC + Aeron echo responders on node0
(ports `netping_udp_port` / `netping_quic_port` in `group_vars/all.yml`). A separate
uc_autobench driver points its ping clients at node0's IP:port; no re-provision
between experiments. `make destroy` tears it down (responders die with the hosts).

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
