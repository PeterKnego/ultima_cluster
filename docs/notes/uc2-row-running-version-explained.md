# The row running version, explained

*Written 2026-09-27 for issue #33, which ships as wire `0.10.0` / cnc
`3.4` — unreleased as this is written. Spec:
`docs/superpowers/specs/2026-09-27-uc2-row-running-version-design.md`; read
its "Errata (as built)" block before the body, because several of the
behaviours below are errata rather than spec text. This note carries the
argument in plain language.*

Issue: [#33](https://github.com/PeterKnego/ultima_cluster/issues/33). The
operator's rules are [Upgrade an application § The version
rules](../how-to/upgrade-an-application.md#the-version-rules); the procedure
they sit in is the rest of that page.

## The problem in one sentence

A row — one declared state machine — could be served by two different builds
of its code at the same time, nothing noticed, and in that state the cluster
could acknowledge a write and then lose it.

## How an acknowledged write was lost

The builder dogfood hit this with the KV example, and #33 is its report.
Take a three-node cluster whose `kv` row runs version 1.0. Version 2.0 adds a
command, `append`. The operator upgrades the leader's service to 2.0 first,
meaning to do the followers next.

1. A client sends `append`. The leader appends it to the log and replicates
   it; both followers store it, so it is durable on all three nodes and
   **committed**.
2. The leader's 2.0 service applies it and answers the client: done. In UC
   only the leader's service answers clients, so this is the acknowledgement.
3. Each follower's 1.0 service reaches the same bytes and does not know the
   command. It answers `BAD_REQUEST unknown op` into the void and moves on.
   Its state never includes the append.
4. The leader dies. A follower becomes leader. Its service is 1.0, and its
   state is the state without the append. A read returns the old value.

Nothing was lost from the *log*: the command is still there, committed, on
every node. It was lost from the *state*, because the only replica that
turned it into state was the one that died. The consensus layer did its job;
the application layer had two opinions about what the log meant, and the
cluster served whichever one happened to lead.

UC's upgrade documentation already said "stop every service before starting
any". That procedure prevents this, but only if every operator follows it
every time. Before #33 the platform did not enforce it — a stale binary could
attach to any row that had never been pinned, and a service that was already
attached when a pin committed simply kept applying past it. Both holes are
now closed. `uc_node/tests/row_version.rs` reproduces the four steps above
end to end (`a_mixed_version_row_never_loses_an_acknowledged_write`); before
the fix it failed on the lost value itself, and now the 1.0 followers are
refused by name and the write survives the leader change.

## Why a row may never run two versions

State machine replication rests on one promise: every replica applies the
same commands in the same order, **with the same code**, so every replica
reaches the same state. The log guarantees the first two. Nothing in the log
guaranteed the third.

It is tempting to think that some version mixes are safe — a new version that
only adds a command, say, and never sends it until everyone is upgraded. But
the platform cannot tell a safe mix from an unsafe one: whether version A and
version B produce the same state from the same commands is a property of the
application's code, and a wrong guess is silent. A mixed row keeps
committing, every health check stays green, and the divergence surfaces only
when leadership moves — as a lost write, like the one above.

So the rule is blunt: **a row is never applied by two versions at once.** One
version runs the row, the whole cluster agrees which, and a service that
disagrees does not apply anything.

"Version" here means **major.minor** — the *line*. Patch is free: `1.4.2` and
`1.4.9` may run side by side. That is a promise the application makes, not
one the platform can check: a patch build changes nothing replicated — not
`apply`'s results, responses, timers or `ids()` calls, and not the snapshot
format, since patch builds install each other's artifacts. An application
that ships a behaviour change as a patch bump defeats the check. The tool that
catches it before release is `uc2-diffreplay upgrade`, which runs the old and
new builds over the same captured log and reports every replicated
difference. And `0` — the version of a state machine that never set
`const VERSION` — is a version like any other, equal only to itself.
Otherwise an application that never thought about versions would be the one
application with no protection.

## The running version: one committed fact per row

Each row now has a **running version**, and it lives where every other piece
of cluster-wide truth has lived since `2.11.0`: in [the cluster
FSM](uc2-cluster-fsm-explained.md), applied at commit on every node, carried
in its artifact, and republished onto the row's cnc words. Because it is
committed state, every node reads the same answer, and a node that joins or
restarts learns it before its services can do anything.

Two kinds of record set it:

- a **genesis** record, once per row, which records a fact nobody chose; and
- a **pin** (`uc2ctl upgrade pin`), which an operator places to change it.

Nothing else can.

## Genesis: how a row gets its first version

A new cluster has no record for any row, and someone has to write the first
one. An operator could be made to — but that is one more step to forget, and
forgetting it leaves the row unprotected, which is the state #33 was about.
So the **leader** does it. The first time it sees a row with no running
version and its own service for that row attached and heartbeating, it
appends a `RowGenesis` record naming that service's version. It records a
fact — "the leader runs 2.0" — and never changes one: a genesis for a row
that already has a version is refused (`60 version_already_set`).

Why the leader's version, and not a vote? Because the leader's service is the
one that acknowledges writes. Whatever version the leader runs is the version
whose answers clients are about to be told. Recording it makes that fact
binding on everyone else.

Until every declared row has a running version, the leader **admits no client
writes**. Commands wait in the ingress ring — clients see back-pressure, not
an error — and the node logs `version_gate_waiting` every five seconds,
naming the row it is waiting for and whether its own service for that row is
attached. This is a hard wait on purpose. If writes were admitted before the
version was recorded, the first commands of a mixed-version bootstrap would
be exactly the #33 window. The gate is deliberately *not* part of the node's
`can_serve` flag: that flag also decides whether the leader may append the
genesis record itself, and whether clients are redirected elsewhere, and
neither should change here. The cost is plain: a declared row whose service
never attaches on the leader keeps the cluster closed to writes, and the log
says which row.

## Attach: the refusal

A service that starts on a row which already has a running version compares
its own `VERSION` with the row's. If the major.minor differs, it is refused
before it writes anything to its slot:

```
row 0 ("kv") runs 2.0.0; this binary is 1.0.0 — install a 2.0.x build, or move the row to this version with `uc2ctl upgrade pin`
```

That closes the first hole: a stale binary can no longer attach to a row
that was never pinned. It also settles a mixed bootstrap. If the leader's
service is 2.0 and a follower's is 1.0, genesis records 2.0, and the
follower is refused from then on.

The service reads the row's pin and its running version in **one**
consistent read of the row's cnc words (a seqlock, the same one the pin
words already used). Two separate reads could pair a pin from one moment
with a running version from another and decide on a combination that was
never true. When the words are mid-update and cannot be read consistently,
attach refuses and asks for a retry. It never guesses "no version".

## The exact stop

A refusal at the door is not enough. A service can be attached, applying,
and on the right line — and then a record commits that moves its row to
another line: a pin to 2.0 while it runs 1.0, or a genesis for 2.0 while a
1.0 follower attached a moment before it. That service must stop, and the
question is where.

It stops at **exactly the record**. Every frame before the record is applied;
nothing at or after it is. The service publishes `applied` equal to the
record's start position, logs `version_superseded`, and fail-stops:

```
version_superseded: row "kv" moved to 2.0.0 at position 81856; this binary (1.0.0) stopped there — restart it as a 2.0.x build
```

Exactness is the point. The record's position is the boundary between two
versions' jurisdictions. A frame below it belongs to the old version; the new
version never applies it, because the new version starts from the pin's
origin artifact or from genesis. A frame above it belongs to the new version.
If the old service applied even one frame past the record, it would compute
state the new version is responsible for — the #33 hazard in miniature.

The service does not decide this from the record's bytes. The cluster FSM
can **refuse** a record: a pin whose `from` is not on the running line, say,
or a second genesis. A refused record must change nothing, and a service
that stopped on a record the cluster refused would be an outage caused by a
typo. So at a version record for its own row, the service waits until the
cluster agent has applied that record, and then asks the cluster what
happened:

- the record was refused → carry on;
- it was accepted and the row is still on this service's line (a patch pin)
  → carry on;
- otherwise → stop at the record.

The wait is short — the frame is already committed, and the cluster agent
applies at commit — and it never sleeps: the service spins, then yields, and
if the cluster agent still has not got there it hands the cycle back and
tries again on the next one, rewound to the record, with `applied` at the
record's start. That keeps a slow cluster agent from hanging the service's
own stop request.

The stop is safe to restart from. The next attach reads a running-version
record at or past the one the service stopped at, and every record up to it
is already decided by that attach, so a restarted service never meets the
same record again. It either attaches (on the right line) or is refused by
name.

The same check runs when a service catches up by replaying the journal
rather than reading the live log buffer. That path runs whenever the service
falls behind the buffer, including at attach, and it walks past the position
the attach decided, so it needs the stop too.

This is what closes the second hole, and with it the window the FSM upgrade
lifecycle's §9.2 left open: after a pin commits, no old service applies a
single frame past it.

## Why only a pin can change a version

A service rebuilds its state at attach in one of three ways: it replays the
journal from the beginning, it installs its newest local snapshot and
replays the tail, or — a durable state machine — it carries on from where it
last stopped. Every one of those can hand a new binary **state or commands
produced under the old version**. Whether that is sound depends on the new
version applying every old command exactly as the old one did, *and* reading
the old version's saved state correctly. That is a semantic promise about
the application's code. The platform cannot check it.

A pin removes the question instead of trusting an answer. Every replica of
the row installs **the same origin artifact** — the snapshot the old version
took at a coordinated instant — and applies only frames after it, under the
new version. What is left to trust is one thing: that the new version reads
the artifact the old version wrote. That is checked, not trusted. The
artifact's envelope names the version that built it, and the install
compares it with the pin's `from`.

So a pin is the **only** way to change a row's version, forward or back. Two
alternatives were considered and rejected, and they are recorded so they are
not proposed again.

**`uc2ctl upgrade adopt`** would commit "the row now runs V" with no origin
artifact. The operator would promise that V treats every existing command
and the old saved state exactly as before. That promise is the hardest
property in the whole upgrade story to get right. Nothing inside the cluster
can enforce it — `uc2-diffreplay upgrade` can only sample it over a corpus.
And a false promise re-creates exactly the divergence this work exists to
prevent. An operator who is sure can pin; the pin costs one instant and
proves the claim unnecessary.

**A genesis pin** would pin at position 0: every replica rebuilds the row
from empty by replaying the *whole* log under V. Its only use would be a row
that cannot snapshot, and the numbers say that is not a deployment option.
The M14 gate measured one state machine ingesting **1 362 555 commands/s**
([`uc2-m14-gate-2026-08-29.md:42`](../benchmarks/uc2-m14-gate-2026-08-29.md),
row a's `n1`). At a tenth of that — about 136 000 commands/s — with ~128 B
of log per command (the same gate's 64 B payload + 16 B session envelope +
32 B header, aligned), a cluster writes about **17 MB/s, or ~1.5 TB of log a
day**. The same doc measured a restart replaying **11 858 320 commands in
21.6 s** ([`:415-422`](../benchmarks/uc2-m14-gate-2026-08-29.md)), about
**550 000 commands/s**. A month of history at that ingest is ~3.5 × 10¹¹
commands, which takes **about 7.5 days** to replay — for each upgrade, on
every node, with the row down the whole time. A row without snapshots cannot
purge its log or rebuild in reasonable time either, so it is not something
to build an upgrade path for. Snapshot support becomes mandatory for every
row instead ([#67](https://github.com/PeterKnego/ultima_cluster/issues/67)).
Until then, a row that cannot snapshot cannot change version once it has
one; the upgrade how-to says so.

Rollback is also just a pin — to the older version, at a newer origin. It is
sound for the same reason every pin is. Whether it is *possible* depends on
the older build reading the artifact the newer one wrote, which is the same
dual-read question an upgrade asks, in the other direction.

## What it costs, and what it does not do

- **A flag day.** The new record kind and the new cnc words mean every node
  stops before any node starts, as for every wire bump. The cluster image
  moves to version 3, and versions 1 and 2 are still read, so nothing on disk
  needs wiping ([Upgrade a cluster](../how-to/upgrade-a-cluster.md)).
- **Patch is trusted.** A behaviour change shipped as a patch bump — or a
  bare `const VERSION: u32 = 2`, which packs as `0.0.2`, a patch — passes
  every check. Use `pack_version(major, minor, patch)`, and use
  `uc2-diffreplay upgrade` to prove a patch is a patch.
- **A pinned row admits exactly one build.** A pin names one `to`, and attach
  requires it bit for bit, patch included. Patch builds roll node by node
  only on a row whose version came from genesis. On a pinned row a patch
  roll-out is a pin of its own — one on the same line, which stops nothing.
- **Snapshot sessions still compare versions exactly.** A joiner below the
  purge floor is refused a snapshot session by a peer on a different patch
  build. Finish a patch roll-out before relying on snapshot catch-up.
- **The gate is a hard wait.** A declared row with no service on the leader
  keeps writes out, by design, and says so in the log.
- **A span jumped by a snapshot install is not walked.** A service that
  installs a snapshot mid-life skips the frames inside it, and any version
  records among them, without looking at them one by one. There the artifact's
  envelope check does the job: an artifact built on another line is refused.
- **Not a rolling upgrade.** Two versions of one row running *on purpose*,
  gated by a committed feature level, is
  [#66](https://github.com/PeterKnego/ultima_cluster/issues/66). This work is
  its floor: #66 relaxes "must be the same line" to "at or above the level"
  and needs nothing here undone. Rolling upgrades of UC itself are
  [#31](https://github.com/PeterKnego/ultima_cluster/issues/31).

## Where to go next

- [Upgrade an application](../how-to/upgrade-an-application.md) — the
  version rules and the pinned procedure, with the old services' stop in it.
- [`uc2ctl` § `status`](../reference/uc2ctl.md#status) and
  [§ `upgrade show`](../reference/uc2ctl.md#upgrade-show) — `running=`,
  `running_pos=` and the running line, plus refusals 52, 53 and 60.
- [The cnc control page § Service slots](../reference/cnc-page.md#service-slots)
  — `running_version`, `running_record_pos` and `cluster_applied`.
- [Monitor a cluster](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)
  — `uc2_row_running_version`, `Uc2RowVersionMismatch`, and the line-based
  `Uc2ServiceVersionDrift`.
- [The cluster FSM, explained § Pins and reports](uc2-cluster-fsm-explained.md#pins-and-reports-2130)
  — the pin this builds on.
