# How to monitor a cluster

Wire a running cluster into Prometheus and Grafana, and read the structured
records nodes print to stderr. All of it is optional and off by default — a
node with no `[log]`/`[metrics]` sections behaves exactly as before M10.

## Turn on the endpoint

Add a `[metrics]` section to the node's config file:

```toml
[metrics]
bind = "127.0.0.1:9600"
```

`bind` defaults to `127.0.0.1:9600` if the section is present but empty
(`[metrics]` with no `bind` key). Absent section means no endpoint at all —
the node never opens the port. See
[`packaging/node.example.toml`](../../packaging/node.example.toml) for the
annotated copy.

A bind failure (port already held, say) is a **runtime failure, exit 1** —
the same retried-by-systemd class as any other post-preflight startup error,
not the config-refusal exit 2. See
[Run a cluster on real hosts](run-a-cluster.md#supervise-the-processes) for
what the two exit codes mean to the packaged unit.

### Security note

`/metrics`, `/healthz`, and `/readyz` are **unauthenticated, read-only,
GET-only**, and the server sends `Connection: close` on every response — no
keep-alive, no other verbs to probe. There is no plan to add authentication;
the endpoint is meant to sit behind the same trust boundary as the rest of
the node.

Bind it to loopback or a private address and firewall it at the network
layer, the same posture as an unencrypted cluster's replication port. The
surface it exposes is operational, not payload: byte positions, term
numbers, peer addresses and ids, and counters. No application data, no
command bytes, no client identities ever appear on it.

## Scrape it with Prometheus

One target per node, all on port 9600 (or whatever `bind` you chose):

```yaml
scrape_configs:
  - job_name: uc2
    metrics_path: /metrics
    static_configs:
      - targets:
          - 10.0.0.10:9600
          - 10.0.0.11:9600
          - 10.0.0.12:9600
```

`/metrics` serves `text/plain; version=0.0.4` — standard Prometheus text
exposition. The full series contract — 64 families — is the
`CONTRACT_SERIES` array in
[`uc2_node/src/obs/metrics.rs`](../../uc2_node/src/obs/metrics.rs); a test
pins every family in that array against what the renderer actually emits, so
it cannot drift silently. This page names only the load-bearing subset: the
lag/saturation/heartbeat-age/peer-lag gauges and the `agent_alive` gauge that
the alert rules below key on, plus the counters they watch for edges. For
everything else — snapshot/session counters, sender/receiver datagram and
byte totals, resync counters — read the source array; each family carries
its own one-line doc comment there.

One family worth calling out because its shape is easy to misread:
`uc2_ingress_holes_skipped_total` (since 2.7.0) is a **single counter that
sums skipped holes across both client-facing rings**, `ingress.ring` and
`query.ring` — it does not tell you which ring hit the hole, and there is
no per-ring alert on it. It counts only the *recoverable* case, a stalled
or dead producer skipped after `hole_timeout` (default 1 s). The
unrecoverable case — a producer that dies in the few instructions between
claiming a slot and stamping its claim word — is not a counter at all; it
is the `IngressRingWedged` fail-stop. See
[Diagnose a node: my node just fail-stopped with `IngressRingWedged`](diagnose-a-node.md#my-node-just-fail-stopped-with-ingressringwedged).

## Install the alert rules

[`packaging/prometheus/uc2-alerts.yml`](../../packaging/prometheus/uc2-alerts.yml)
is a ready-to-load rule file, group `uc2`, evaluated every 15s. Point your
Prometheus (or a remote-write-fed Mimir/Thanos ruler) at it:

```yaml
rule_files:
  - /etc/prometheus/uc2-alerts.yml
```

Verify it loads before shipping it — `promtool` ships in the Prometheus
release tarball (the same class of external dependency as elle's `java`; it
is not part of this workspace's own toolchain):

```bash
promtool check rules packaging/prometheus/uc2-alerts.yml
```

Every rule's `expr` uses only names from `CONTRACT_SERIES` above, and every
`for:` follows the interpretations in
[Diagnose a node](diagnose-a-node.md) — these interpretations ship as alert
rules, not independent judgment calls, so if you disagree with a threshold,
change the rule rather than re-deriving the reasoning from scratch. The
table:

| Alert | Fires when | Severity |
|---|---|---|
| `Uc2AgentDead` | any polling agent's `uc2_agent_alive` reads 0 | critical |
| `Uc2NoLeader` | no node reports `uc2_is_leader == 1`, 30s sustained | critical |
| `Uc2LeaderNotServing` | a node is leader but `can_serve == 0` — the `0x01` flags state | critical |
| `Uc2ServiceWedged` | service heartbeat stale while the node heartbeat is fresh — the apply loop, not the cluster, is stuck | critical |
| `Uc2ReplicationStalled` | append is advancing but commit is not, for 1m — no quorum acknowledging | critical |
| `Uc2PeerNeverHeard` | a peer's reported-durable position has sat at 0 for 2m — usually the bind-address mismatch, not a network fault | warning |
| `Uc2PeerLagging` | a peer's replication lag exceeds the admission window, for 5m | warning |
| `Uc2AdmissionSaturated` | the ingress admission window is ≥90% consumed for 1m — commit is not keeping up with append | warning |
| `Uc2PurgeStalled` | purge is enabled but the journal head lags the snapshot floor by more than 2 segments, for 10m | warning |
| `Uc2RepeatedWipes` | a node wiped-and-rejoined more than once in 10m | warning |
| `Uc2UnattestedReports` | a pre-0.5.0 peer's un-attested durable reports are being counted — a flag-day violation; commits will stall | critical |
| `Uc2CleartextPeer` | cleartext datagrams arrived from a peer while crypto is on — a node missed the wire-crypto flag day | critical |
| `Uc2FollowerSealFailures` | outgoing control frames a **follower** could not seal (check pairwise sessions/allowlist) — a leader's own climb is benign and excluded by the rule | warning |
| `Uc2DiskLow` | `uc2_free_disk_bytes` has sat below 4 journal segments' worth of free space for 2m — the archive fail-stops at `ENOSPC`; purge or grow the disk | warning |

The per-peer band (`uc2_peer_reported_durable_bytes`, `uc2_peer_replication_lag_bytes`) is leader-authoritative — only the leader receives `AppendPosition` reports, so a follower's own scrape always reads 0 for every peer regardless of health (see [Diagnose a node](diagnose-a-node.md)); `Uc2PeerNeverHeard` and `Uc2PeerLagging` are scoped to `uc2_is_leader == 1` for exactly this reason, and the dashboard's per-peer panel does the same.

### Watching the disk before `ENOSPC` hits it

`uc2_free_disk_bytes` is `statvfs`'d against the instance directory's
filesystem on the daemon's ~1s outer-loop cadence and gauged directly — **it
is daemon-published only**: a node run in-process without the `uc2-node`
daemon (a library user, a test harness) never writes it, and the field reads
`0` and is **omitted from the scrape entirely**, same convention as
`uc2_leader_hint`'s omission at `u64::MAX`. Do not read its absence from a
scrape as "the disk is full" — it means no daemon is publishing it here.

`Uc2DiskLow` (table above) fires 2 minutes after free space drops below four
journal segments' worth — chosen because that is the fail-stop the archive
actually hits: any write or fsync error on the journal, `ENOSPC` included,
halts the writer, the archive agent panics, and the daemon exits 1 for
systemd to restart. This is *asserted*, not merely documented — see
`examples/uc2-crashtest/tests/enospc.rs`. Purging (or growing the disk) before
this alert escalates is the whole point of watching it; see
[Keep the journal from growing without bound](bound-journal-growth.md).

**One documented asymmetry, worth knowing before you rely on either side going
quiet:** the journal's own write path is fail-stop, but the *service's*
snapshot-publish path is not. A snapshot build or publish that fails under
disk pressure is logged and dropped — the snapshot marker simply does not
advance, and the next policy interval tries again — rather than crashing
anything. A disk running low can therefore degrade purge's snapshot cadence
silently, well before the journal itself ever reaches `ENOSPC` and forces the
loud failure. `Uc2DiskLow` and `Uc2PurgeStalled` (table above) are the two
signals that catch this quiet half; do not assume a stalled purge will
announce itself the way a fail-stopped archive does.

## Import the dashboard

[`packaging/grafana/uc2-dashboard.json`](../../packaging/grafana/uc2-dashboard.json)
is a hand-written, minimal, importable dashboard — `uid` `uc2-cluster`,
schema version 39. It declares one templated datasource variable,
`${DS_PROMETHEUS}`; Grafana's import flow prompts you to map it to your
Prometheus datasource at import time, and every panel's query rides that
variable rather than a hardcoded datasource id.

Six panels: commit/apply lag, cluster throughput, per-peer replication lag,
a cluster stat row (term, leader elected, every agent alive, config
version), heartbeat ages, and repair/drop counters (NAKs sent, replay
datagrams, receiver drops). Each is a straight PromQL expression over
contract series — nothing pre-aggregated beyond what the query itself does.

## The probe endpoints

Two boolean-shaped HTTP probes, meant for a load balancer or an orchestrator
rather than a human — for the operator's own diagnosis, prefer
[Diagnose a node](diagnose-a-node.md), which reads the same underlying state
with more explanation.

| Probe | Answers | 200 when | 503 when |
|---|---|---|---|
| `/healthz` | should this process be restarted? | all four agents alive and the node heartbeat is fresh (<3s) | any agent fail-stopped, or the node heartbeat is stale |
| `/readyz` | should traffic be routed here? | role-aware: a leader needs `can_serve` too; a follower/learner needs only to be healthy — both need a fresh **service** heartbeat as well | any `/healthz` failure, OR a leader with `can_serve == 0` (elected but its NewTerm frame isn't yet quorum-committed — flags `0x01`), OR a stale service heartbeat |

`/healthz` is deliberately role- and `can_serve`-blind: an elected-but-not-
yet-serving leader (flags `0x01`) is alive and should not be restarted, only
routed around. `/readyz` is where that distinction lives — it is why the
flags table in [Diagnose a node](diagnose-a-node.md#is-anyone-leading) now
also drives an HTTP probe, not just `uc2ctl status` and the raw cnc page.
The `0x01` case's body names `"NewTerm"` explicitly, so a `curl` against a
stuck node is self-explanatory without cross-referencing this page.

`GET /metrics`, `/healthz`, `/readyz` are the only three routes; anything
else — a non-`GET` method, an unknown path — is `404`.

## Structured records

Nodes print one JSON object per line to stderr, filtered by `[log]`'s
`level` (`error` < `warn` < `info`, each level including the ones before it;
default `info`):

```toml
[log]
level = "info"
```

```json
{"ts_ns":1755600000000000000,"level":"info","event":"became_leader","node":0,"term":3,"base":1048576}
```

Keys always appear in the order `ts_ns`, `level`, `event`, then the event's
own fields in the order the call site names them — this is a machine log, so
key order is a contract, not a cosmetic choice.

Two families of sites emit records. **Consensus-driven** records fire
exactly on the state transition they name, at the point in the code where it
happens — no polling, no delay. **Derived** records come from the daemon's
own ~1s pass over the counters (`uc2-node.rs`'s poll loop): edge-triggered
(only fire on a change since the last pass) and rate-limited to at most once
per 10s per event, so a sustained condition prints periodically instead of
flooding.

| Event | Fields | Means |
|---|---|---|
| `became_leader` | `node`, `term`, `base` | this node won an election for `term`; `base` is the position its term begins at |
| `became_follower` | `node`, `term`, `leader`? | adopted `term`, following `leader` (the field is absent when the leader is not yet known) |
| `serving_changed` | `node`, `term`, `can_serve` | edge-triggered on the `CAN_SERVE` cnc flag flipping — fires once per transition, not once per cycle |
| `log_truncated` | `node`, `epoch`, `to` | the log was cut back to position `to` as part of reconciliation epoch `epoch` |
| `log_wiped` | `node` | a stronger case of the above: no common prefix with the leader, so the node truncated to 0 and will rejoin from the snapshot floor (`wipes_total` also increments) |
| `snapshot_installed` | `node`, `pos` | the incoming-snapshot floor advanced to `pos`. **This fires whenever the floor marker moves, including the sub-case where the node already held the bytes and only the marker advanced** — it means "this node adopted a snapshot floor," not necessarily "a snapshot transfer happened." Don't read it as proof of a wire transfer. |
| `config_adopted` | `node`, `position`, `version`, `prev_position` | a new `ClusterConfig` (version `version`) was adopted at `position`, superseding the one at `prev_position` |
| `halt_removed` | `node`, `term`, `msg` | this node is not a member of the just-adopted config and has fail-stopped (parked permanently; the process keeps running but never serves again) |
| `stepdown_removed` | `node`, `term`, `msg` | this node's own self-removal just committed while it was leader; it fail-stopped the same way as `halt_removed` |
| `nak_storm` (derived) | `node`, `naks_dropped`, `naks_served` | the NAK-drop counter advanced in the last ~1s window — repair traffic is being shed |
| `seal_failures` (derived) | `node`, `count`, `is_leader` | `count` sealed-datagram failures since the *last emitted* record (not cumulative); on a leader this is expected and benign — see [Encrypt traffic between nodes](encrypt-node-traffic.md#confirm-it-is-healthy) |
| `snapshot_published` (derived) | `node`, `pos` | this node's own service-side snapshot position advanced to `pos` |
| `admin_op` | `actor`, `origin`, `op`, `op_name`, `id`, `addr`, `seq`, `nonce`, `outcome`, `reason`, `config_version` | this node answered an admin request (membership change). A **mirror** of the line already written to `<instance_dir>/audit.jsonl`, which is the record of record: the file is fsynced *before* the answer is published, this stream is best-effort. `actor` is the admin key name that signed it, `filesystem` when the node authenticates nothing (`auth = "none"`), `unverified` on a request that failed authentication, or `peer:<id>` on a proposal a follower forwarded (`origin: forwarded`). `outcome` is `accepted` (proposed and appended — not necessarily committed) / `refused` / `retry`. |
| `admin_audit_failed` | `node`, `seq`, `nonce`, `op`, `status`, `err` | the audit record for an admin request could NOT be written, so the request was refused with reason 24 rather than answered unrecorded. On an otherwise-accepted change this means the change may still be in the log — check `uc2ctl status`. Alert on this: it means the node's disk is failing or full. |
| `agent_failstopped` | `agent` | one of the four polling agents panicked; the daemon logs this and then **exits 1 without draining**, so systemd restarts it and the replay path (not reconstruction) picks the node back up |

`agent_failstopped` is a behavior change worth calling out on its own: before
M10, a mid-run agent panic could leave the process running with a healthy-
looking exterior — a zombie node still holding its instance-directory lock,
still answering `uc2ctl status` with stale-but-plausible-looking numbers,
while the rest of the cluster quietly lost a member. Now the crash is loud
and the process dies, which is what makes `/healthz` and `Uc2AgentDead`
meaningful signals rather than a race against a silent hang.

## Where to go next

- [Diagnose a node that is not serving](diagnose-a-node.md) — the same
  underlying state, read by hand, with the reasoning behind each threshold.
- [Configuration](../reference/configuration.md#log-and-metrics) — the
  `[log]`/`[metrics]` schema.
- [Run a cluster on real hosts](run-a-cluster.md) — where the observability
  endpoint fits into process supervision and exit codes.
