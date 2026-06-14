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
