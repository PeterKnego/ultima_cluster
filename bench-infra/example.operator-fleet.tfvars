# Dogfood operator-track fleet (wayfinder map #16, ticket #22).
#
# TEMPLATE. Copy before filling in the last three values (they carry your IP
# and key, so the copy is gitignored):
#     cp example.operator-fleet.tfvars operator-fleet.tfvars
# then apply with `-var-file=../operator-fleet.tfvars`.
#
# BARE HOSTS — terraform only, NO ansible. The operator installs everything
# from the 2.12.0 tarball and packaging/, so this var-set provisions the
# hardware and the network and stops there. Do NOT `make up` (that chains the
# ansible provision play); bring it up with `terraform apply` directly (see
# docs/superpowers/plans/2026-09-16-uc2-dogfood-operator-fleet-recipe.md).
#
# Credentials are NOT here — source bench-infra/.env (AWS_PROFILE or
# AWS_ACCESS_KEY_ID/SECRET) before any terraform command. Every apply is the
# maintainer's explicit approval gate (charter decision 13).

cloud = "aws"

# Four hosts in ONE cluster placement group / ONE AZ (the module pins both):
#   index 0,1,2 -> voters, c6i.2xlarge   (private 10.10.1.10/.11/.12)
#   index 3     -> observer, c6i.large   (private 10.10.1.13)
# The observer is the client + Prometheus + Grafana box (charter decision 13);
# the operator installs those on it (Card 3). All On-Demand.
node_count           = 4
voter_count          = 3
instance_type        = "c6i.2xlarge"
client_instance_type = "c6i.large"
client_spot          = false

region = "us-east-1"

# A DISTINCT owner tag isolates this fleet from any perf-bench fleet and scopes
# the leak sweep (scripts/dogfood_fleet_leak_check.py) to exactly it. It also
# names the VPC/subnet/SG/key-pair, so two owners never collide.
owner     = "uc-dogfood"
ttl_hours = 6

# ---- fill these in before applying ----
# The operator and the maintainer inject faults from the SAME egress IP (the
# security group is a single /32). Set this to that shared IP/32, never
# 0.0.0.0/0. The intra-cluster rule (self=true) is separate and revocable — the
# partition fault is injected by revoking it via the AWS API, not on-host.
allow_ssh_cidr       = "REPLACE_WITH_YOUR_IP/32"
ssh_public_key       = "ssh-ed25519 AAAA... REPLACE"
ssh_private_key_file = "~/.ssh/id_ed25519"
