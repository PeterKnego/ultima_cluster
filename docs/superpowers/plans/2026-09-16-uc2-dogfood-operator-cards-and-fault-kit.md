# Dogfood operator scenario cards and the blind fault kit

Record of wayfinder map #16, ticket **Design the operator scenario cards and
the blind fault kit** (#21), grilled with the maintainer 2026-09-16. The
charter is `docs/superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md`
(decisions 13–16); the gate is `docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md`
(rows B1'-1 … B1'-7). Vocabulary: root `CONTEXT.md` § Dogfooding.

## Card anatomy (decided)

A card is **one goal line plus one outcome-shaped success line**, naming no
UC command, flag, type or step. The finish line is stated; the path is not.
The hand-in format (`LEDGER.md` + `RUNLOG.md`) lives in the operator's
standing `CLAUDE.md`, never on a card. Judging is **post-hoc**, by the
maintainer, from `RUNLOG.md` + `LEDGER.md` + the transcript audit against the
pasted-in success criterion (gate convention 8); a mid-run maintainer
intervention is itself a B1' FAIL (convention 2).

## The seven cards

Cards 1–3 are the provisioning session (#26), 4–6 the faults/membership/backup
session (#27), 7 the upgrade session (#28). Sessions #27 and #28 are handed a
**maintainer-provisioned, already-running, already-monitored cluster** — the
"you inherited this cluster" framing; provisioning is scored once, in #26.

**Card 1 — Bring up the cluster** (#26)
Goal: From the release in `release/`, stand up a three-node cluster across the
hosts in `HOSTS.md`.
Success: the three nodes form one cluster and one of them reports as the
serving leader.

**Card 2 — Prove it serves** (#26)
Goal: Deploy the `kv` service from `app/` to every node and make the store
reachable to a remote client.
Success: a value written through one gateway reads back, unchanged, through a
*different* gateway.

**Card 3 — See the cluster** (#26)
Goal: Stand up Prometheus and Grafana on the observer host and point them at
the cluster.
Success: every shipped alert rule loads and evaluates healthy in Prometheus,
and the shipped dashboard renders the cluster's live metrics.

**Card 4 — Find what's wrong** (#27, issued once per fault, five times)
Goal: The cluster is misbehaving. Find what is wrong, explain it, and restore
service.
Success: the cluster serves writes again and all replicas agree on the store's
contents.

**Card 5 — Reshape membership** (#27)
Goal: Grow and reshape the cluster's voting membership while it keeps serving.
Success: a learner is added and catches up, is promoted to voter, and one
original voter is removed — writes served throughout, and the final membership
reflects the change.

**Card 6 — Survive a lost node** (#27)
Goal: Back up the cluster's state, verify the backup, and restore it onto a
fresh node that rejoins.
Success: a value acknowledged before the backup reads back after the restored
node has rejoined.

**Card 7 — Upgrade the application** (#28)
Goal: Upgrade the `kv` service from v1 to the v2 binary in `app/` without
losing any acknowledged write.
Success: every value acknowledged before the upgrade reads back with its value
afterward, and all replicas agree on the store's contents.
(The charter's survey expects the operator to hit a **documented absence** of
an upgrade procedure; the card stays outcome-shaped and that gap is a ledger
item, not a softened bar.)

## The blind fault kit (Card 4)

**Framing.** One identical generic card per fault (so the wording leaks
nothing), issued *after* the fault is already in place. The operator is told
only "something is wrong; find it, name it, fix it." Diagnosis is genuinely
theirs.

**Remediation is in the success criterion** for all five faults. "Found and
named it but the docs did not let me fix it" is the docs-sufficiency signal
this study exists to catch, recorded as a blocking ledger item — never a
lowered bar.

**Structure.** All five faults, one per exercise, run sequentially on the
single #27 fleet, with the cluster **restored to health between exercises** so
each fault is diagnosed in isolation. Order is fixed for reproducibility and
invisible to the operator (identical card text).

**Injection mechanism.** `bench-infra` provisions one ssh identity (`ubuntu`,
passwordless sudo, a single imported key) from a single `/32` ingress CIDR;
there is no separate maintainer path and none is needed. Inject **live,
pause-inject-resume**, over a **one-shot non-interactive `ssh user@host
"cmd"`** from the same egress IP — which leaves no `who`/`w`/`last`/
`bash_history` trace, only an auth-log line (which a diagnosing operator
*should* be able to find; that is legitimate diagnosis). **No pre-arming** (no
`at`/cron/timer infra exists, and an armed timer is visible via
`systemctl list-timers` until it fires).

| fault | injection | maintainer's private "fixed" observable |
|---|---|---|
| node SIGKILL | `sudo systemctl kill --signal=SIGKILL uc2-node` (one-shot ssh) | the node rejoins and a write succeeds |
| killed service | `sudo systemctl kill --signal=SIGKILL uc2-service@kv-service` | the service reattaches and applies again |
| stopped gateway | `sudo systemctl stop` the gateway unit | a client reaches the cluster through that gateway |
| full disk | `fallocate`/`dd` a filler on the instance-dir filesystem until ENOSPC | the node serves again (wipe-and-rejoin if the fail-stop needs it) |
| partition | **AWS security-group edit** (revoke the intra-cluster rule via the maintainer's AWS creds) — zero host-side trace | the link heals and the cluster re-converges |

The AWS-security-group partition is the stealthiest of the five (no ssh, no
sudo, no host trace — only CloudTrail).

## Hand-back

`LEDGER.md` (the friction ledger) plus `RUNLOG.md` (every command run on a
host, with its outcome), per the operator's standing `CLAUDE.md`. Cards add
nothing.

## Requirements this ticket imposes on the bare-host fleet recipe (#22)

The scout found `bench-infra` cannot satisfy Card 3 or fault (e) as it stands:

1. **Observer host + Grafana.** `bench-infra` provisions no separate observer
   instance and no Grafana at all (Prometheus runs on node0 as a transient
   unit). Card 3 and charter decision 13 assume a 4th observer host running
   Prometheus + Grafana. #22 must provision it.
2. **Persistent gateway unit.** The fleet runs the gateway as a *transient*
   `systemd-run` unit, which cannot be `systemctl start`-ed back after a stop —
   so the operator could not *remediate* the stopped-gateway fault. #22 must
   install the packaged persistent `uc2-gateway.service`
   (`BindsTo=uc2-node.service`) instead.
3. **Teardown leak check.** `bench-infra`'s leak check verifies only the local
   `terraform.tfstate`; a real orphan sweep after each operator session needs
   `boto3 describe_instances` over `us-east-1` (`owner` tag), per the fleet
   account notes.
