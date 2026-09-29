# Upgrade an application

This is how you move a running cluster's **application** — the service binary,
your state machine — from one version to the next. It is distinct from
[upgrading the cluster itself](upgrade-a-cluster.md) (the `uc2-node` binary and
the wire protocol); here the nodes and gateways stay up and only the service
processes change.

Since `2.13.0` an application upgrade is a **pinned, per-row flag day**. You
name the **origin** — a coordinated instant `P` whose complete snapshot set
exists on every node — and pin the row's next version to it *before* you stop
anything. After the pin commits, every old service still attached stops at the
pin record by itself (when the pin changes the row's major.minor), the platform refuses the old binary at attach by name
on every node, and the new one installs the artifact at `P` unconditionally
before it replays anything. The stages and the reasoning behind
them are [The upgrade lifecycle, per
row](../reference/application-sdlc.md#the-upgrade-lifecycle-per-row) in the
application SDLC standard; this page is its S6 procedure, command by command.
Read [the version rules](#the-version-rules) first, and [why it is a flag
day](#why-a-flag-day-and-not-a-rolling-swap) before you begin.

Everything below is **scoped to one row**. Upgrading two rows is two of these
procedures; they may share an origin, and nothing requires them to.

## The version rules

Since wire `0.10.0` / cnc `3.4` (issue
[#33](https://github.com/PeterKnego/ultima_cluster/issues/33); unreleased as
this is written) every declared row has one **running version**, committed
in the cluster FSM, and the platform enforces it: a row is never applied by
two versions of its state machine at once. The reasoning is [The row running
version, explained](../notes/uc2-row-running-version-explained.md); the rules
you build and operate by are these.

- **"The same version" means the same major.minor — the same *line*.** Patch
  is free: `1.4.2` and `1.4.9` are the same line, `1.4.2` and `1.5.0` are
  not. The comparison is `uc_protocol::identity::same_line`, and it is the one
  used everywhere — attach, the stop in the apply loop, the pin rule and the
  snapshot envelope check.
- **A patch must not change replicated behaviour, and it must not change the
  snapshot format.** Two patch builds of one line may run side by side on
  different nodes, and either may install an artifact the other wrote, so
  "patch" is a promise that `apply`'s results, state, responses, timers and
  `ids()` calls are identical, *and* that the snapshot payload each writes is
  readable by every other patch build of the line. The platform trusts this
  promise; it cannot check it. Anything that changes behaviour needs at least
  a **minor** bump. Check the promise before you ship with
  [`uc2-diffreplay upgrade`](diff-replay.md) run old-build against new-build
  over a corpus from this cluster: a patch that is really a patch comes out
  with no replicated difference at all.
- **Pack the version with `pack_version(major, minor, patch)`**
  (`uc_protocol::identity::pack_version`). The packed
  word is `major:8 ‖ minor:8 ‖ patch:16`, so a bare integer lands in the
  patch field: `const VERSION: u32 = 2` is `0.0.2`, a *patch* of `0.0.1`, and
  the platform will let it run beside `1` without a pin. Write
  `pack_version(0, 2, 0)` (or `2, 0, 0`) for a behaviour change.
- **`0` is a version.** A state machine that never set `const VERSION` runs
  version `0` (`0.0.0`, printed `unversioned`), and `0` is equal only to `0`.
  Giving such an app its first real `VERSION` is a change of line, so it is a
  pin (`--from 0.0.0`). You cannot pin *to* `0.0.0`: `uc2ctl` refuses it
  locally, because a pin's cnc words read `0` as "no pin".
- **A row's first version is recorded for you; every later change of line is
  a pin.** The first time a row has no running version, the leader appends a
  `RowGenesis` record naming the version of *its own* attached service for
  that row. Until every declared row has one, the leader admits **no client
  writes** — they wait in the ingress ring, and the node logs
  `version_gate_waiting` every 5 s naming the row and whether the leader's own
  service for it is attached. A declared row whose leader-side service never
  attaches keeps the cluster closed to writes. After genesis only
  `uc2ctl upgrade pin` moves the version, forward or back.
- **A pin names a line, not a build.** While a row has a pin, attach
  requires `VERSION` to be on the pin's `to` line — same major.minor, any
  patch (`PinnedVersionMismatch` otherwise). A patch build of `to` attaches
  through the same pinned install as `to` itself, so patch builds roll node
  by node on a pinned row exactly as on a row whose running version came
  from genesis: no pin, no window.
- **Snapshot sessions compare lines too.** A joiner below the purge floor is
  refused a snapshot session only by a peer whose attached version is on a
  different line from its own (`version mismatch` on the session); two patch
  builds of one line exchange sessions, so snapshot catch-up works in the
  middle of a patch roll-out.

**What the two new refusals mean, and what to do.**

`VersionMismatch` is an attach refusal on an unpinned row: the row runs a
version on a different line from this binary.

```
row 0 ("kv") runs 2.0.0; this binary is 1.0.0 — install a 2.0.x build, or move the row to this version with `uc2ctl upgrade pin`
```

Either you started the wrong build (install one on the running line), or you
mean to change the row's version — which is this whole procedure. Restarting
the same binary will not help. Two cases produce it without any operator
mistake: a follower whose service is on a different line from the leader's
when genesis records the leader's version, and a stale binary on a row that
has never been pinned.

`version_superseded` is a **stop**, not a refusal: a service that was
attached and applying reached the record that moved its row to another line.
It publishes `applied` equal to that record's start position — every frame
before the record applied, nothing at or after it — logs a
`version_superseded` event, and fail-stops its apply thread with:

```
version_superseded: row "kv" moved to 2.0.0 at position 81856; this binary (1.0.0) stopped there — restart it as a 2.0.x build
```

`Service::is_alive()` turns `false`, and a supervised service process exits
(`examples/kv`'s does). This is the expected end of an old service's life at
a pin (step 3 below): install the new build and start it. Restarting the old
binary is refused at attach instead, by `PinnedVersionMismatch` on a pinned
row or `VersionMismatch` on a row whose version genesis set. Outside a
planned upgrade it means the row's version was changed under this service —
read `uc2ctl upgrade show` to see which record did it.

**A durable state machine that stopped keeps the old line's state on disk.**
Everything a stopped service applied before the record, it applied as the
*old* build. An in-memory state machine loses that on exit and rebuilds, but
a **durable** one attaches above the record with it intact — so do not simply
restart it as the new line on top of its old store when frames before the
record may have been applied differently by the two lines. The sharpest case
is a genesis stop: a follower that was on another line from the leader when
genesis recorded the leader's version, which is exactly what crossing from
`2.13.0` with an already-mixed row produces. Either discard that node's store
for the row, so the new build rebuilds it from the journal or a snapshot, or
move the row with a pin, whose origin install replaces the state
unconditionally (step 3 below).

**Take the origin on a learner.** Every pin names an origin, and the
recommended source is a **standby instant**: `uc2ctl snapshot --standby`
freezes only learners, so no voter pauses for the upgrade, and
`uc2ctl snapshot fetch --from <learner>` then copies the learner's complete
set to each voter, store-only. One learner's artifacts are valid for every
replica because the state machine is deterministic. Step 2 below shows both
routes.

## Before you start

- The pre-rollout stages of the standard are done:
  [S1 classify](../reference/application-sdlc.md#s1-classify-the-change) the
  change against [the change
  taxonomy](../reference/application-sdlc.md#the-change-taxonomy),
  [S2 declare](../reference/application-sdlc.md#s2-declare-the-version) the new
  `const VERSION`, and
  [S3 write the shims](../reference/application-sdlc.md#s3-write-the-compatibility-shims)
  — in particular the snapshot dual-read, because the new binary is about to be
  handed the **old** version's image.
- A new service binary whose state machine can read the old version's on-disk
  snapshot image. (The KV store's v2 reads a v1 image; a v1 binary *refuses* a
  v2 image by name — rollback is one-way past the first v2 snapshot.)
- A maintenance window in which clients tolerate a short service-layer outage
  (seconds per row, not an election). It starts when the pin commits — the
  old services stop there by themselves (step 3) — and ends when the new
  build is attached on the leader.
- Admin access to every node (`uc2ctl`, the admin key), and an off-node
  destination for the backup in step 1.

**Rehearse the swap off the production cluster first.** `uc2-diffreplay
pin-verify` takes a corpus captured from this cluster and your two real
service binaries, and on a throwaway single-voter node it places a real
`uc2ctl upgrade pin` and checks the two things a pinned swap has to do: the
old binary is **refused by name** after the pin, and the new one's live state
is the one the artifact path computes rather than the genesis counterfactual.
The sequence covers both state-machine shapes without needing to know which
yours is — it stops the old binary above the pin's origin, so an in-memory
state machine attaches empty while a durable one attaches above the origin
and has to be rewound to it. It does not take that rewind on trust: the new
binary has to print the SDK's own `pinned install of snap-<origin>` line on
its stderr, or the run is a FAIL whatever the projections say. If your
version change also alters a command whose result depends on prior state
(not just one that overwrites it), the projections separate a third
case as well — a durable service that quietly kept the state it had
persisted instead of installing the artifact. This is a pre-flag-day check of the **pin
mechanism on your binaries**, and it is not the same thing as [step
7](#7-verify)'s `/metrics` check, which reads the real cluster after the real
upgrade. See
[Diff replay an FSM change § "Verify the pin
live"](diff-replay.md#5-verify-the-pin-live-reconstruction-mode-part-2).

## 1. Back up every node first — the rollback point does not survive the upgrade

Take a coordinated snapshot and copy it **off the node**, on every host, before
you stop anything — and, decisively, **before you pin**:

```bash
# once, on the leader — the instant is one command, and a follower answers retry
uc2ctl snapshot --instance-dir /srv/uc2/n0 --app-id APP --admin-key /etc/uc2/admin/ops-admin.key

# then on each node
uc2ctl backup --instance-dir /srv/uc2/nN --out /srv/uc2-backups/nN-preupgrade
# and copy /srv/uc2-backups/nN-preupgrade to somewhere off this host
```

This matters more than it looks, for two independent reasons.

- **The pin is the one-way door.** Once it commits (step 3) the old binary is
  refused at attach on every node, there is no unpin verb, and a lower origin
  is refused. Rolling back then means restoring *this* copy on every node —
  see [Rolling back](#rolling-back) and
  [S8](../reference/application-sdlc.md#s8-decide-the-point-of-no-return).
- **The on-node artifacts are not a rollback point, even though they survive.**
  The pinned origin's whole set is held on every node until a newer pin
  supersedes it, so `snap-P` — built by the old version, its envelope stamped
  with the old version — is still sitting there afterwards. It is not a way
  back: the old binary is refused at attach by name, and there is no operator
  verb that installs a chosen artifact by hand. (The *later* artifacts are the
  new version's image format besides — operator-dogfood finding L47, filed as
  [#41](https://github.com/PeterKnego/ultima_cluster/issues/41).)

The backup has to predate the pin. One taken *after* it carries the pin, so
restoring it restores the door you were trying to walk back through.

## 2. Take the origin: one coordinated instant, P

```bash
uc2ctl snapshot --instance-dir /srv/uc2/n0 --app-id APP --admin-key /etc/uc2/admin/ops-admin.key
#   instant=73792
```

`instant=` is **P**, the frame-end position of the `SNAPSHOT` frame. Every
declared row and the cluster FSM freezes there, so P is a complete set
cluster-wide regardless of which row is moving — that is deliberate, and it is
why a per-row upgrade still costs one instant for everyone.

The command is **leader-only**: a follower answers `retry` with a leader hint
(`uc2ctl status`'s `leader_hint`). It is refused `48 snapshot_unsupported`,
naming the row, if a declared row lacks the snapshot-capability bit — such a
row would ignore the frame and the set could never complete. Since #67 every
Rust SDK service gets this bit automatically (`ServiceBuilder::start()`
requires `S: SnapshotStateMachine`), so in practice this refusal only fires
for a row attached without the current Rust SDK — a non-Rust attacher, or a
binary built before #67.

Now wait for the complete set at P **on every node**. The pin's `54 pin_no_set`
is a door check on the **leader** alone, so it does not speak for the rest of
the cluster; the node that has to have the artifact is every node, because in
step 6 each one installs its own copy or refuses with `PinnedArtifactMissing`:

```bash
# on each node
uc2ctl snapshot show --instance-dir /srv/uc2/nN --app-id APP
#   ... set=73792
# or the live gauge
curl -s http://hostN:9600/metrics | grep uc2_snapshot_set_position
```

`set=` is the newest position present in **every** declared row's directory and
in `snapshots/cluster/` — the intersection of what is on disk, not each side's
own newest. Pin *that* position.

**The recommended route: a standby instant on a learner.** An instant freezes
every row that takes it, and a freeze on a quorum of voters stalls commit
until the slowest one ends. If the cluster has a learner, freeze only the
learner and bring its set to the voters instead:

```bash
# once, on the leader — only learners freeze
uc2ctl snapshot --standby --instance-dir /srv/uc2/n0 --app-id APP --admin-key /etc/uc2/admin/ops-admin.key
#   instant=73792

# then on EVERY voter, the leader included — store-only, nothing is installed
uc2ctl snapshot fetch --from 3 --position 73792 --instance-dir /srv/uc2/nN --app-id APP --admin-key /etc/uc2/admin/ops-admin.key
```

A fetch's `accepted` means the pull is underway; wait for `set=73792` on each
voter as above before you pin. Every voter needs the set, not just the
leader: the leader's newest set is what `54 pin_no_set` checks, and each
node's own copy is what its new service installs in step 6. The learner's
artifacts are valid on every voter because the state machine is
deterministic ([`uc2ctl` § `snapshot fetch`](../reference/uc2ctl.md#snapshot-fetch)).

## 3. Pin the row to the new version at P

```bash
uc2ctl upgrade pin --row 0 --to 2.0.0 --origin 73792 \
  --instance-dir /srv/uc2/n0 --app-id APP --admin-key /etc/uc2/admin/ops-admin.key
#   pinned: row=0 from=1.0.0 to=2.0.0 origin=73792 position=81920
```

`--from` is optional: omitted, `uc2ctl` reads the row's **running version**
off **this** node's page (the word `status` prints as `running=`), and falls
back to the attached-version word (`version=`) only if the row has no running
version yet. With neither it refuses locally — pass `--from 1.0.0` explicitly
then.

Like every `CLUSTER` command this is **leader-only** (a follower answers
`retry`) and **single in flight** across membership, schedule, settings and pin
changes (and the leader's own genesis records) — a pin retries behind another one, and a refused or timed-out pin
leaves its staged file in place, so a retry needs nothing re-staged.

Refused by name, with the reason code the CLI prints:

| Code | Reason | What it means, and what to do |
|---|---|---|
| 52 | `row_undeclared` | `--row R` names a row this node does not declare in `[services] names`. Check the row number against `node.toml`. (Named `pin_row_undeclared` through `2.13.0`; same number, same check) |
| 53 | `pin_from_mismatch` | `--from` is not on the row's running line: its major.minor differs from the row's running version (`status`'s `running=`; patch is ignored). With no running version recorded yet, `--from` must be on the line of the version the service is attached at. A stale `--from` usually means another pin already landed since it was read; re-read `uc2ctl status` and re-run |
| 54 | `pin_no_set` | `--origin` is not this node's **newest** complete snapshot set (an older set still on disk is refused too). Go back to step 2: run `uc2ctl snapshot`, wait for `uc2_snapshot_set_position` (or `uc2ctl snapshot show`'s `set=`) to reach it, and pin THAT position |
| 55 | `pin_not_monotone` | `--origin` is not above the row's current pin. A pin only ever moves a row's origin forward — there is no way to point one backwards |
| 56 | `pin_digest` | the staged pin file's digest is not the one the request signed: a different file was staged than was signed, or it changed in between. Re-run `upgrade pin` |
| 57 | `pin_missing` | no staged pin file on this node. Either `upgrade pin` was run against a different instance directory, or a successful apply already consumed it |
| 58 | `pin_decode` | the staged file is not a decodable 20-byte `UpgradePin` record |

Every outcome — accepted or refused — is recorded in `audit.jsonl` as
`upgrade_pin`.

**The old services stop at the pin by themselves.** Every service of this
row that is still attached applies up to the pin record and stops there —
`applied` equals the record's start position on every node, and each one logs
[`version_superseded`](#the-version-rules) and fail-stops. So from the moment
the pin commits the row applies nothing, anywhere, until the new build
attaches: client writes to the cluster stall (the leader's own service has
stopped), and reads of this row stay at the pin. This is the outage the
maintenance window is for. (A pin that stays on the running line — a patch
pin, `2.0.0` → `2.0.1` — does not stop anything; the old build keeps applying
until you stop it in step 5.)

**Pin promptly after the instant.** Between the origin P and the pin record
the old version was still applying, and its leader's `on_committed` emitted
side effects for that span. After the rewind the new version recomputes the
span, but the durable, increase-only output-progress marker stops it
re-emitting — so the outside world saw the old version's effects for a span
whose state is now the new version's. That is a [documented
limit](../reference/application-sdlc.md#s4-pin-the-origin), not a defect. Since
the old services stop at the pin record, the window is `(P, pin]` — bounded by
how soon you pin after the instant, not by how soon you stop anything.

## 4. Confirm the pin on every node before you stop anything

```bash
# on EVERY node
uc2ctl status --instance-dir /srv/uc2/nN --app-id APP | grep 'row=0'
#   row=0 name=kv version=1.0.0 ... upgrade_origin=73792 pinned=2.0.0 pinned_from=1.0.0 artifact_hash=0x... running=2.0.0 running_pos=81920
```

This reads the row's pin words on the **cnc page**, live, as the `uc2-cluster`
agent published them when the record committed. It is the right reading for
this step, and `uc2ctl upgrade show` is not: that command reads this node's
newest cluster **artifact**, which by construction lands at least one instant
behind ([`uc2ctl` § `upgrade show`](../reference/uc2ctl.md#upgrade-show)).

`running=2.0.0 running_pos=81920` is the row's new running version and the
end position of the pin record that set it — the same `position=` step 3
printed. A node whose line still reads `upgrade_origin=0 pinned=unversioned
pinned_from=unversioned` (or an old `running=`) has not applied the pin yet —
or the words were read mid-publish, which renders as the same zeros (and
`running=?`). Either
way you do not know that node's answer, so re-run it there. This matters
because a node that has not applied the pin would let the **old** service
reattach to that row after step 5.

The same line shows the stop: `applied=` on each node sits at the pin
record's start (`running_pos` less the record's 64-byte frame), and the old
service's `heartbeat_age=` grows.

## 5. Confirm every old service stopped at the pin, then stop its unit

```bash
# on every node
uc2ctl status --instance-dir /srv/uc2/nN --app-id APP | grep 'row=0'
#   row=0 name=kv version=1.0.0 ... applied=81856 ... running=2.0.0 running_pos=81920
journalctl -u uc2-service@APP | grep version_superseded
sudo systemctl stop uc2-service@APP
```

Since the pin commits a change of line, every old service of the row has
already stopped itself at the pin record (step 3): `applied=` reads the
record's start on every node and the service logged `version_superseded`.
The version stop clears the slot's ATTACHED bit before it fail-stops, as a
graceful stop does, so `attached=` reads `false` and `heartbeat_age=` keeps
growing until the new build takes the slot over. The row reads absent: a swap
slower than 30 s raises `Uc2ServiceAbsent`, and one slower than a minute
raises `Uc2ServiceWedged` as well
([Monitor a cluster](monitor-a-cluster.md)). Stopping the unit is
still worth doing: a supervisor that restarts the old binary gets it refused
at attach (`PinnedVersionMismatch`) over and over, which is harmless but
noisy.

A node on which the old service has **not** stopped has not applied the pin —
go back to step 4 on that node. `Uc2RowVersionMismatch` (an attached service
off its row's committed line that is still stamping its heartbeat, for a
minute) is the alert for a stop that did not happen. A service that stopped
at the record reads detached, so this alert does not fire on a node whose
new build is merely slow to attach.

**Why a mixed row cannot happen any more.** Before `0.10.0` this step said
"stop them all before starting any", because while a row was mixed — some
services old, some new — a new leader could **commit a command the old
replicas cannot apply**, and a failover to an old leader then had no record
of that acknowledged write
([#33](https://github.com/PeterKnego/ultima_cluster/issues/33)). The platform
now closes that window itself: before the pin commits the new build is
refused at attach (`VersionMismatch` — the row still runs the old line), and
from the pin onwards every old service stops at exactly the record and the
old build is refused at attach. There is no instant at which both lines apply
the row.

The nodes and gateways are still up; writes submitted now will stall (the
leader's state machine is stopped), so hold client traffic during the window.
Under bounded or lockstep lag this also stalls commit for the *other* rows
while this row's service is down on a quorum — budget for it.

## 6. Install the new binary and start every service

```bash
# on every node
sudo install -m 0755 kv-service /usr/local/bin/kv-service
sudo systemctl start uc2-service@APP
```

At attach, before the service publishes anything to its slot, the SDK reads the
row's pin and running version — one consistent read of the row's words —
under `service.<row>.lock` and decides in this order. (On a row with no pin,
the running version decides instead: a binary off the running line is
refused `VersionMismatch`, see [the version rules](#the-version-rules).)

**1. The pin words did not read consistently** (the `uc2-cluster` agent is
mid-publish) — refused:

```
row 0's pin words could not be read consistently (the uc2-cluster agent is mid-publish); retry the attach
```

It fails **closed** on purpose: treating an unreadable pin as "no pin" would
skip an install the cluster requires. Transient — retry the attach.

**2. The binary's `VERSION` is not on the pin's `to` line** — refused (a
patch build of `to` is admitted):

```
FSM "kv" at row 0 is pinned to version 0x02000000 from origin 73792, but this binary is 0x01000000; a stale binary cannot rejoin after `uc2ctl upgrade pin`
```

This is the backstop that makes step 4 worth doing: a host that was missed, or
a unit that restarted the old binary, stops here instead of diverging.

**3. The row attached without the install capability** — refused:

```
FSM "kv" at row 0 is pinned to origin 73792 but was attached without snapshot support; a pinned row must install snap-73792
```

Without the install capability it would replay the origin's prefix under the
*new* version, which is exactly the counterfactual the pin exists to avoid.
Since #67 every `ServiceBuilder::start()` carries this capability, so a Rust
SDK service cannot reach this refusal any more — it is `attach`'s backstop
for a row attached outside the SDK.

**4. The artifact at the origin is not on this node** — refused:

```
row 0 is pinned to origin 73792 but /srv/uc2/nN/snapshots/0/snap-73792.ultsnap does not exist on this node — the set at the origin was pruned or never fetched; take `uc2ctl snapshot fetch` or re-pin at a retained instant
```

**5. Otherwise the artifact at the origin is installed unconditionally**, and
the tail above P is replayed under the new version. There is no "already caught
up, skip it" arm: a durable state machine sitting above P is **rewound to P**
and recomputes the span above it, exactly as its fresh peers do. The artifact's
envelope is cross-checked against the pin's `from` line, not against the running
binary's version — this is the one sanctioned crossing of a version boundary.
The check is by line (major.minor), so an origin artifact built by `1.0.1` on
one node and by `1.0.3` on another both install under a pin `from=1.0.0`.

The SDK prints the record of that install on stderr, and it is what to look for
in the service's journal:

```
uc_service: row 0 pinned install of snap-73792 (from 0x01000000 to 0x02000000, artifact built by 0x01000000)
```

(The versions are the packed `major:8 ‖ minor:8 ‖ patch:16` words, so `1.0.0`
is `0x01000000`.) No such line on a pinned row means no pinned install
happened — treat that as a failure and stop, exactly as `pin-verify` does.

## 7. Verify

```bash
# every row reports the new version, caught up
uc2ctl status --instance-dir /srv/uc2/nN --app-id APP | grep 'row='
#   ... version=2.0.0 attached=true applied=<commit> lag=0

# a value acknowledged before the upgrade still reads back
kv --gateways host0:9200,host1:9200,host2:9200 get --linearizable SOME_PREUPGRADE_KEY

# and all replicas agree: take a coordinated instant, then read the LIVE gauge
uc2ctl snapshot --instance-dir /srv/uc2/n0 --app-id APP --admin-key .../ops-admin.key
curl -s http://hostN:9600/metrics | grep uc2_snapshot_hash_mismatch
#   uc2_snapshot_hash_mismatch{service="kv",row="0"} 0      # on EVERY node
```

`0` on every node, at the new version, with every pre-upgrade value intact,
is the upgrade done.

**Since `2.13.0` you do not hash the files by hand.** Every node hashes its
row artifact as the builder streams it and reports `(row, position, hash)` to
the leader; the leader commits one `SnapshotReport` record naming everyone who
reported, and every replica computes the same verdict from it. So the check is
committed cluster state rather than a per-file `sha256sum` you collect and
compare yourself.

**There are two readings of that state, and they are not equally prompt.**

- **The live one — use this for the upgrade.**
  `uc2_snapshot_hash_mismatch{service,row}` on `/metrics` is recomputed **at
  scrape time** from the node's committed cluster view, so it reflects the
  instant you just commanded as soon as that instant's `SnapshotReport`
  commits — seconds, not another instant. `0` = agreed (or no majority to
  differ from); nonzero = that many replicas off the majority. The
  `snapshot_hash_diverged` obs record fires on the same event and names the
  row and the node. `Uc2SnapshotHashDiverged` is the alert behind the gauge.
- **The durable one — `uc2ctl upgrade show` — lands ONE INSTANT BEHIND.** It
  reads this node's newest cluster **artifact** on disk, and that artifact was
  frozen *as of* instant `P`, while the `SnapshotReport` for `P` is appended
  to the log at a position strictly **above** `P` (the leader waits for every
  voter's report, which arrives after the freeze). An artifact can therefore
  never carry its own instant's verdict: `P`'s verdict first appears in the
  artifact written at the *next* instant. Run right after one instant,
  `upgrade show` prints no verdict line for that row, or the previous
  instant's. This is the same artifact-backed lag `schedule show`,
  `settings show` and `status`'s `schedule_position=` have had since the
  cluster FSM — see [`uc2ctl` § `upgrade show`](../reference/uc2ctl.md#upgrade-show).

To get the durable record for instant `P`, **take a second instant** (or wait
for the `snapshot_interval_bytes` cadence if you run one) and then read it:

```bash
uc2ctl snapshot --instance-dir /srv/uc2/n0 --app-id APP --admin-key .../ops-admin.key
uc2ctl upgrade show --instance-dir /srv/uc2/nN --app-id APP
#   row=0 hash_verdict=agreed position=<P> nodes=3 hash=0x...
```

Two things to check in that line:

- **`hash_verdict=agreed`** — every reporting node's artifact hashed the same.
- **`nodes=`** should equal your voter count. Short of it means the leader's
  5 s collection timeout fired and some voter did not report that instant:
  the verdict is still sound about the nodes it names, but it is not evidence
  about the one missing. Check `uc2_snapshot_reports_timed_out_total` on the
  leader and `uc2_snapshot_reports_unsent_total` on the quiet node.

And do **not** expect `position=` to be the instant you just commanded — by
the lag above it is the EARLIER one, the instant whose verdict the newest
artifact was able to carry.

`hash_verdict=DIVERGED` names the minority node ids outright — that is a
nondeterminism in the new version's `freeze`/`stream_snapshot`, and the next
step is `uc2-diffreplay determinism` on that row's corpus.

If you still want the file-level check (a node the cluster has not heard
from, say), note that the two numbers are **not** comparable: `sha256sum`
over `snap-<instant>.ultsnap` covers the 24-byte `ULTSNAP2` envelope as well,
while the reported hash is SHA-256 over the **payload only**, truncated to
its first 8 bytes. The per-node value is on the cnc page and
`uc2ctl status` prints it as `artifact_hash=` beside that row's
`snapshot_pos=`.

That same `status` line also carries `upgrade_origin=`, `pinned=` and
`pinned_from=` — the committed record of the upgrade you just performed.

## Rolling back

**Before the pin commits (through step 2)** nothing is pinned: stop whatever
you started, start the old binary, and the cluster is where it was. The instant
you took is harmless.

**After the pin commits (step 3 onwards) it is a one-way door.** The pin is
committed and monotone; there is **no unpin verb**, and a lower origin is
refused (`55 pin_not_monotone`). It is the **origin** that is checked for
monotonicity, not the version — so a pin back to `--to 1.0.0` at a *newer*
origin is accepted (its `--from` must be on the row's new running line), and
it is sound for the same reason every pin is — every replica reinstalls the
same origin. But it is a rollback only if the old binary can read the
artifact at that newer origin, which the new version wrote in its own image
format; usually it cannot, and you would need an old build with a dual-read
shim for the new image. The old binary as it was is refused at attach by
name on every node, so "put the old binary back" is not a rollback — it is a
service that will not start. The only way back that needs no new build is:

1. restore step 1's off-node backup on **every** node
   (`uc2ctl restore` — see [Back up a cluster](back-up-a-cluster.md)), and
2. start the old service binary.

**Every write acknowledged since that backup is lost.** That is the whole
reason step 1 is not optional, and why
[S8](../reference/application-sdlc.md#s8-decide-the-point-of-no-return) puts
the point of no return at the moment the pin commits, not at the moment you
stop the services.

One more consequence of abandoning a pinned upgrade: a node **holds its
snapshot/purge floor at the pinned origin** until that row is consumed there
(attached on the pin's `to` line — a patch build of `to` counts — **and**
replayed past the cut), and logs
`snapshot_floor_held_for_pin` when it does. A pin placed and then left alone
holds the journal at that origin **indefinitely** — there is no bound and no
alert on the hold. Clear it by finishing the upgrade or by pinning the row
forward to a newer origin, not by waiting
([Limits](../reference/limits.md)).

## Afterwards: close axis H

The backward-apply shim — the new binary's arm for the *old* command shape — is
bounded, not permanent, and this upgrade is what starts the clock. It may be
deleted in the **next** version, not this one, once both of these hold:

- a **pinned origin sits above the last occurrence** of the old command shape
  in the log, so the artifact at that origin already carries its effect and no
  binary will ever decode those bytes again; and
- the **oldest artifact any node could still reconstruct that row from is at or
  above that origin** — check each node's `snapshots/<row>/` listing and its
  purge floor, remembering that a pinned origin's set is retained on purpose.

Neither quantity is exposed as one fleet-wide reading, so until you have
checked both on every node, **keep the arm**: an axis-H failure is silent, and
the node that trips it is one you are not watching. The full rule is
[S9](../reference/application-sdlc.md#s9-close-axis-h).

## Why a flag day, and not a rolling swap

A rolling application upgrade — canary the new version on a learner, run a
mixed-version window, roll learners then followers then the leader — needs the
log to carry each command's application version so an old replica can refuse a
command it cannot apply *before* acknowledging it. UC does not do that. What
it does since wire `0.10.0` is the opposite, stricter thing: the running
version is a committed per-row fact, and a row is **never** applied by two
lines at once — an off-line binary is refused at attach, and an attached one
stops at the record that moves its row. The pin (`2.13.0`) made the *origin*
safe (every node's new binary starts from the same artifact, and no stale
binary can rejoin afterwards); the running version (`0.10.0`) closed the
mixed window. Together they are a **correct, enforced flag day**, not a
rolling path. Rolling application upgrades — two versions of one row running
on purpose, gated by a committed feature level — are
[#66](https://github.com/PeterKnego/ultima_cluster/issues/66), which builds on
this floor by relaxing "must be the same line" to "at or above the level".

The mixed-version hazard was not hypothetical — the builder dogfood reproduced
it in `examples/kv/tests/cluster.rs` (`upgrade_v1_to_v2_flag_day` shows the
partial upgrade committing an `append` a v1 successor then cannot see), and it
was filed as [#33](https://github.com/PeterKnego/ultima_cluster/issues/33).
`uc_node/tests/row_version.rs`'s
`a_mixed_version_row_never_loses_an_acknowledged_write` reproduces it end to
end and shows the write kept: the v1 followers are refused by name and no acknowledged write is lost.
