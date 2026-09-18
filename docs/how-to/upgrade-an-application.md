# Upgrade an application

This is how you move a running cluster's **application** — the service binary,
your state machine — from one version to the next. It is distinct from
[upgrading the cluster itself](upgrade-a-cluster.md) (the `uc2-node` binary and
the wire protocol); here the nodes and gateways stay up and only the service
processes change.

At 2.12.0 an application upgrade is a **flag day**: you stop every service,
install the new binary, and start every service. Read [why it is a flag
day](#why-a-flag-day-and-not-a-rolling-swap) before you begin — a rolling swap
looks safe and can silently lose an acknowledged write.

## Before you start

- A new service binary whose state machine can read the old version's on-disk
  snapshot image. (The KV store's v2 reads a v1 image; a v1 binary *refuses* a
  v2 image by name — rollback is one-way past the first v2 snapshot.)
- A maintenance window in which clients tolerate a short service-layer outage
  (seconds — the length of your stop/start, not an election).
- Admin access to every node (`uc2ctl`, the admin key).

## 1. Back up every node first — the rollback point does not survive the upgrade

Take a coordinated snapshot and copy it **off the node**, on every host, before
you stop anything:

```bash
# on each node
uc2ctl snapshot --instance-dir /srv/uc2/nN --app-id APP --admin-key /etc/uc2/admin/ops-admin.key
uc2ctl backup   --instance-dir /srv/uc2/nN --out /srv/uc2-backups/nN-preupgrade
# then copy /srv/uc2-backups/nN-preupgrade to somewhere off this host
```

This matters more than it looks. When the new service attaches it **rewrites
the pre-upgrade snapshot artifact in place** in the new image format, and the
next coordinated instant deletes the old one — so the on-node rollback point is
gone within seconds of the upgrade, silently. The only rollback image is the
copy you made here, off the host. (This is operator-dogfood finding L47, filed
as [#41](https://github.com/PeterKnego/ultima_cluster/issues/41); the backup is
the workaround until the platform stops discarding it.)

## 2. Stop every service — all of them, before starting any

```bash
# on every node, together
sudo systemctl stop uc2-service@APP
```

Do not stop them one at a time and start the new one in between. While the
cluster is mixed — some services old, some new — a new leader can **commit a
command the old replicas cannot apply**, and a failover to an old leader then
has no record of that acknowledged write until every service is new and replays
the log. The platform makes the mix *visible* (`Uc2ServiceVersionDrift`) but
does not prevent it. Stopping every service first closes the window.

Confirm the cluster is quiet: `uc2ctl status` shows each row `attached=false`.
The nodes and gateways are still up; writes submitted now will stall (the
leader's state machine is absent), so hold client traffic during the window.

## 3. Install the new binary and start every service

```bash
# on every node
sudo install -m 0755 kv-service /usr/local/bin/kv-service
sudo systemctl start uc2-service@APP
```

Each service reattaches, installs or reads the existing snapshot, replays the
journal tail, and resumes. Acknowledged writes are never lost across this: they
are quorum-durable in the log, and a restarted service reconstructs from the
snapshot floor plus the tail.

## 4. Verify

```bash
# every row reports the new version, caught up
uc2ctl status --instance-dir /srv/uc2/nN --app-id APP | grep 'row='
#   ... version=2.0.0 attached=true applied=<commit> lag=0

# a value acknowledged before the upgrade still reads back
kv --gateways host0:9200,host1:9200,host2:9200 get --linearizable SOME_PREUPGRADE_KEY

# and all replicas agree: a coordinated snapshot hashes identically everywhere
uc2ctl snapshot --instance-dir /srv/uc2/n0 --app-id APP --admin-key .../ops-admin.key
sha256sum /srv/uc2/nN/snapshots/0/snap-<instant>.ultsnap   # identical on every node
```

Identical snapshot hashes at the new version, with every pre-upgrade value
intact, is the upgrade done.

## Rolling back

Only to a set taken **before** the upgrade, and only from the off-node copy you
made in step 1 — a downgraded binary fail-stops on a post-upgrade image. Restore
that copy on every node and start the old service binary.

## Why a flag day, and not a rolling swap

A rolling application upgrade — canary the new version on a learner, run a
mixed-version window, roll learners then followers then the leader — needs the
log to carry each command's application version so an old replica can refuse a
command it cannot apply *before* acknowledging it. UC does not stamp that yet;
at 2.12.0 the service `VERSION` is equality-checked only on the snapshot path,
so a mixed cluster commits freely and the hazard above is real. The rolling
upgrade arrives with the release that ships log-stamped application versions
(tracked in `docs/BACKLOG.md`); until then, the flag day is the safe procedure.

The mixed-version hazard is not hypothetical — the builder dogfood reproduced
it in `examples/kv/tests/cluster.rs` (`upgrade_v1_to_v2_flag_day` shows the
partial upgrade committing an `append` a v1 successor then cannot see), and it
is filed as [#33](https://github.com/PeterKnego/ultima_cluster/issues/33).
