# Operating a UC cluster: a clean-room operator's report

*Written 2026-09-18 as the operator half of the dogfood KV experience
assessment (wayfinder map #16,
charter `docs/superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md`). An
"operator" is an agent handed a set of bare Linux hosts, the 2.12.0 release
tarball, the published docs, and the `examples/kv` application binaries — in a
sandbox that cannot read this repository — and asked to stand the cluster up
and keep it running through a set of outcome-shaped scenario cards, with the
faults injected blind. Every place the published material fell short is a
ledger item. The runs used a real 3-voter + 1-observer AWS fleet, destroyed
and leak-checked after each session.*

## The question

Can someone **run** a UC cluster — bring it up, serve an application, monitor
it, survive faults, reshape membership, back up and restore, and upgrade the
application — from the published documentation alone, on bare hosts, without
reading the source and without being told the procedure? The operator worked
seven cards across three fleet sessions, each stating only a goal and a success
criterion, never a procedure.

## What the answer turned out to be

**Yes — all seven cards passed, from the docs and the tarball, with zero
maintainer interventions** (`docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md`,
rows B1'-1 through B1'-7). The operator:

- stood up a three-node cluster from the tarball and made it serve a remote
  client through a gateway (cards 1–2);
- installed Prometheus and Grafana and loaded every shipped alert rule healthy
  (card 3);
- **found, named, and remedied all five blind faults** — a downed node, a
  boot-refusing full disk, a network partition, a killed service, a stopped
  gateway — each from the docs, with writes served again and all replicas
  agreeing afterward (card 4, five exercises);
- added and promoted a learner and removed a voter while the cluster kept
  serving, with 1108 writes and zero loss through the reconfiguration (card 5);
- backed up, verified, and restored a lost node so that a value acknowledged
  before the backup read back after the restore (card 6);
- **upgraded the application v1 → v2 as a flag day without losing an
  acknowledged write** — 200 maintainer-planted keys and the operator's own
  500-key set all read back with their acknowledged values, and a coordinated
  snapshot hashed identically on all three replicas at `version=2.0.0` (card 7;
  gate rows B4.i/B4.ii).

Throughput was reported, never barred: **13,557 ops/s** at p50 5.76 ms on the
v1 cluster (B3-v1) and **9,590 ops/s** at p50 5.80 ms on a differently-shaped
v2 fleet (B3-v2) — context, not a comparison.

So the operational surface — the daemon, the config, the admin CLI, the
metrics, the backup tooling, live reconfiguration, the flag-day upgrade —
is **operable from the published docs alone**. That is the headline.

## Where the docs fell short

The operator ledger ran to 49 items across the three sessions. They triaged
([#39](https://github.com/PeterKnego/ultima_cluster/issues/39)) into three
mechanical doc fixes landed immediately (`8d25de4`), three product tickets, and
the rest folded into this report and the how-to work. One theme dominates.

### Degraded-but-quorate states are invisible on a quiet cluster

This is the operator track's central finding. On a cluster with light traffic,
**a lost voter, a five-minute partition, a stopped gateway, and a boot-refusing
full disk each fired no shipped alert** — every dashboard panel stayed green
while the cluster ran at zero fault tolerance:

- a **killed voter** left the cluster serving on two, and the alerts that
  should catch it (`Uc2AgentDead`, `Uc2ServiceAbsent`) need a series that reads
  0, but a dead node stops exporting entirely, so the series goes stale rather
  than to zero (L16);
- a **partition** could not trip `Uc2PeerLagging` (the peer's lag stayed far
  under the load-scaled threshold on a quiet cluster) nor `Uc2PeerNeverHeard`
  (the peer *had* been heard), so nothing changed state but the raw `Term`
  number (L28);
- a **stopped gateway** — a silent per-host client outage — showed nothing:
  `/readyz` 200, targets up, panels green (L36);
- and `Uc2LogTimeFrozen` **false-fires on any idle cluster**, because the log
  clock only advances on client writes, so its noise masks the real gaps above
  (L11).

Every one of these rules keys on a signal that only moves under load. The fix
direction — alert on membership and peer *liveness*, not on lag crossing a
load-scaled threshold — is filed P2 as
[#40](https://github.com/PeterKnego/ultima_cluster/issues/40), and relates to
the builder's dead-node-status finding
[#35](https://github.com/PeterKnego/ultima_cluster/issues/35).

### Significant state transitions leave no record

An election storm of ~1400 elections and a deposed leader produced no journal
line on any node (L29); a rejoin logs no `became_follower` (L20); the `members`
band shows dead and deposed voters at the current frontier with no `STALE`
marker (L23, L31); a removed node still reports alive (L41). And the v2 upgrade
**silently rewrites the pre-upgrade snapshot artifact in place and then deletes
it**, so the documented rollback point cannot survive on the node (L47) — the
operator preserved an off-node copy to keep a rollback possible. These are
filed as [#41](https://github.com/PeterKnego/ultima_cluster/issues/41); the
practical consequence, "back up every node before starting any v2 service,"
leads the upgrade how-to.

### Packaging assumes exactly one node per host

Growing the cluster (card 5) needs a fourth member, but the shipped systemd
units hard-code `/srv/uc2/n0` and their `BindsTo` cannot be re-targeted by a
drop-in, so co-locating a second node took hand-written units and non-standard
ports; and `uc2ctl backup` copies the 64 MiB zero-filled preallocation file, so
98 % of every backup artifact is dead weight. Filed
[#42](https://github.com/PeterKnego/ultima_cluster/issues/42).

The remaining items are missing how-to pages — a "member is down" diagnosis, a
"voter is partitioned" page, a lost-disk recovery procedure (whose pieces sit
on four pages, two pointing the wrong way), a new member's `node.toml`,
installing Prometheus/Grafana, and the application-upgrade page itself — all of
which this ticket's how-to and troubleshooting work supplies.

## Accepted limits

Recorded, not fixed: a v1 client reading a v2 list key fails with an "outcome
unknowable" exit code, so old clients must be upgraded before any list is
created (L48); a fresh cluster starts mid-term (L7); two critical alerts fire
for one absent-service fact (L32); and remote reads always reach the leader
(the same remote-protocol-v2 boundary the builder recorded). A handful of
smaller warts are recorded in the ledger and left as they are: `status` prints
a row's id where its name would read better (L6); a rejoined node given a new
id needs its peers' gateway member maps updated by hand (L26); a running node
survives a full disk, because the M11 preallocation makes the fault a boot
refusal rather than a mid-run failure (L27); the `Uc2ServiceAbsent` remedy is
simply to restart the service, and a harmless `service_detached` line follows
each successful reattach (L34); the `kv-service` crash message can blame the
node when the real cause is a wrong path (L37); and a planned learner pages
`Uc2ServiceAbsent` in the window before its own service starts (L42). These are
the cost of the operating envelope at 2.12.0, documented so an operator meets
them in the docs rather than in production.

## The verdict

UC is **operable from the published docs alone** through its whole lifecycle,
including the two operations most likely to go wrong — live reconfiguration and
the application upgrade. The friction was concentrated almost entirely in
**observability**: the cluster does the right thing under a fault, but too often
does it silently. The correctness held everywhere (every card's replicas
agreed; the upgrade lost nothing); what an operator could not always do was
*see* the degraded state before a second fault turned it into an outage. On the
charter's bar — docs sufficiency under the honest-failure protocol, extended to
the operator track — the operator track **passes**, with its ledger fully
resolved (gate row B5-operator) and its observability gaps filed as product
work.
