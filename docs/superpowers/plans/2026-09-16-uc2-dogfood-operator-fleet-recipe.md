# Dogfood operator-track bare-host fleet recipe

Record of wayfinder map #16, ticket **Write the bare-host fleet recipe for the
operator track** (#22). Charter decision 13 fixes the fleet; the operator
cards and blind fault kit are
`docs/superpowers/plans/2026-09-16-uc2-dogfood-operator-cards-and-fault-kit.md`.
**This ticket does not apply anything** — it commits the var-set and the
commands; every `terraform apply` is the maintainer's explicit approval gate.

## What it provisions

The var-set is `bench-infra/operator-fleet.aws.tfvars`, driving the existing
`bench-infra/terraform` AWS module unchanged (the heterogeneous fleet rides
`voter_count` + `client_instance_type`, already in the module):

| index | role | type | private IP | market |
|---|---|---|---|---|
| 0,1,2 | voters | `c6i.2xlarge` | `10.10.1.10/.11/.12` | On-Demand |
| 3 | observer (client + Prometheus + Grafana) | `c6i.large` | `10.10.1.13` | On-Demand |

Ubuntu 24.04 (Noble), one AZ pinned by the cluster placement group, root EBS
64 GB gp3 (the instance dir lives on root — c6i has no instance-store, which
also makes the full-disk fault a `fallocate` on the root filesystem). The
security group admits ssh from `allow_ssh_cidr` (a single `/32`) and all
traffic intra-cluster via a **revocable** `self = true` rule — the partition
fault is injected by revoking that rule through the AWS API, never on-host.

**Bare hosts, no ansible.** The operator installs everything from the 2.12.0
tarball and `packaging/`, so the recipe stops at the hardware and network.
Do **not** `make up` (it chains the ansible provision play); bring the fleet
up with `terraform apply` directly.

## Session discipline (the commands a session runs)

```bash
cd bench-infra
set -a; . ./.env; set +a          # AWS creds; terraform fails without them (aws + hcloud)

# 1. PLAN — the maintainer's approval artifact. Read-only; safe to run.
terraform -chdir=terraform plan -var-file=../operator-fleet.aws.tfvars

# 2. APPLY — ONLY on the maintainer's explicit go (charter decision 13).
terraform -chdir=terraform apply -var-file=../operator-fleet.aws.tfvars

# 3. Hand the operator their hosts (see "SSH handover" below).
terraform -chdir=terraform output -json nodes

# ... operator session runs ...

# 4. DESTROY at session end, always.
terraform -chdir=terraform destroy -var-file=../operator-fleet.aws.tfvars

# 5. LEAK CHECK — account-wide, independent of the local state file.
python3 scripts/dogfood_fleet_leak_check.py            # exit 0 = clean, 1 = a leak to destroy
```

`terraform -chdir=terraform state list` must also print nothing after step 4;
the boto3 sweep (step 5) is the belt to that suspenders, because a run that
failed before writing state leaves instances the local state never recorded.

## SSH handover to the operator

The operator receives, on the first card (`HOSTS.md` in `~/ultima/kv-ops`):

- **user** `ubuntu` (passwordless sudo), **key** the private key matching
  `ssh_public_key` (`ssh_private_key_file`);
- the four hosts' **public IPs** (`terraform output -json nodes`) and their
  **private IPs** (fixed above: `10.10.1.10-.13`), with node0-2 the voters and
  node3 the observer;
- nothing else — no procedure, per the card design.

The maintainer injects faults over this same key from the same egress IP (the
`/32` in `allow_ssh_cidr`), via a one-shot non-interactive ssh (no
`who`/`w`/`last` trace); the partition fault revokes the `self = true` rule via
the AWS API.

## The three requirements handed over from #21

1. **Observer host — DONE.** Index 3 (`c6i.large`, `10.10.1.13`) is the 4th
   host Card 3 and charter decision 13 assume. Grafana + Prometheus are
   *installed by the operator* on it (Card 3), not by the recipe — bare-hosts.
2. **Persistent gateway unit — SATISFIED by design.** The operator installs
   the packaged `packaging/systemd/uc2-gateway.service` (persistent,
   `BindsTo=uc2-node.service`), not `bench-infra`'s transient `systemd-run`
   unit — the operator never touches `fleet_quickstart.sh`. So a stopped
   gateway is `systemctl start`-able and the stopped-gateway fault is
   remediable. Recorded here so the operator docs (`run-a-gateway.md`,
   `packaging/`) are what the operator follows.
3. **Teardown leak check — DONE.** `scripts/dogfood_fleet_leak_check.py`
   sweeps the account by `owner=uc-dogfood` tag in `us-east-1`, read-only,
   exit 1 on any survivor.

## Dry validation (no apply, no account touched)

`terraform -chdir=terraform validate` → **"Success! The configuration is
valid."** Every key in the var-set is a declared module variable. A real
`terraform plan` evaluates the AMI / instance-type-offering data sources and
so needs `.env` (AWS creds) — it is step 1 above, the maintainer's approval
artifact, not run here. The plan will create ~12 resources: 1 VPC, 1 subnet,
1 internet gateway, 1 route table (+ association), 1 security group, 1 key
pair, 1 placement group, and **4 instances**.
