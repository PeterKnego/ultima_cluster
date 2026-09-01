# Start a cluster on an AWS fleet — the quick way

Three commands from an empty AWS account to a serving, end-to-end-verified
3-node cluster, using the provisioning rig and one script. Costs real money
while the fleet is up (three 4xlarge-class hosts ≈ a few dollars/hour), and
**nothing auto-reaps it** — the third command is not optional.

Prerequisites and per-step explanations live in
[the full walk](fleet_start.md); the script runs exactly its steps 3–7 with
the same checks. Do its steps 0–1 once (control-machine tools, AWS
credentials in `bench-infra/.env`, your instance type and SSH key in
`bench-infra/terraform.tfvars` — pick the type with
[Size a host](size-a-host.md)).

```bash
cd bench-infra
make up-uc                      # provision + tune 3 hosts (terraform + ansible)
scripts/fleet_quickstart.sh     # install release, configure, start, prove a write
make destroy                    # tear down — ALWAYS, even after a failure
```

## What the script does, and what PASS means

`fleet_quickstart.sh` discovers the hosts from terraform state, then on each:
downloads the signed release tarball for the host's architecture and verifies
its SHA-256, installs the binaries, shares one admin key, writes
`/etc/uc2/node.toml` (private-IP members, `[crypto]` off inside the fleet's
security group, `[admin]` HMAC), and starts `uc2-node` plus the demo
`counter-service` under systemd. It then waits for a serving leader, fronts
node 0 with a gateway, and proves one write end to end — client → gateway →
quorum-durable commit → apply — by asserting the counter's value moved by
exactly what it added. **PASS means all of that held.** Flags: `--version`,
`--app-id`.

Every step checks its outcome and fails loudly with the failing host and
step. On a failure: fix per the matching step of
[the full walk](fleet_start.md) and re-run the script (it is re-runnable),
or debug by hand — and run `make destroy` regardless when you stop.

## After PASS

- Benchmarking what you built? The demo drives the **remote** path
  (0.62–0.84× of direct, [M13 gate](../benchmarks/uc2-m13-gate-2026-08-24.md));
  the headline numbers used the local shmem client — see
  [the sweep record](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md).
- Keeping the cluster? The must-reads, in order:
  [encrypt node traffic](encrypt-node-traffic.md) (crypto is OFF here),
  [monitor](monitor-a-cluster.md), [back up](back-up-a-cluster.md),
  [bound the journal](bound-journal-growth.md).
- Verify teardown: `terraform -chdir=terraform state list` must print
  nothing.
