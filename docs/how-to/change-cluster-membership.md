# How to change cluster membership without downtime

Grow a cluster, shrink it, replace a machine, or retire the current leader —
live, under load, without restarting anything.

Every change is one member at a time. Exactly one change may be in flight; a
second proposal is refused with `ChangePending` until the first commits. The
cluster holds at most 8 members in total, voters and learners together,
including transitional states.

`uc2ctl` reaches a node through its instance directory's shared memory, not the
network, so **it must run on the same host as the node you point it at**. That
node need not be the leader — a follower forwards the request over the existing
control plane and relays the reply back.

Install it once on an operator host rather than building from a checkout:

```bash
cargo install --path uc2ctl
```

For every sub-command, argument and refusal code, see
[`uc2ctl`](../reference/uc2ctl.md).

## If the cluster requires signed admin requests

Since `v2.6.0` a node's `[admin]` section (see
[Configuration](../reference/configuration.md#admin-authentication)) says
whether it accepts unsigned requests at all. Under `auth = "hmac"`, every
mutating verb (`add-learner`, `promote`, `demote`, `remove-learner`,
`remove-voter`) needs a signed request or the node refuses it with
`auth_missing`; `status` and the offline verbs (`backup`, `verify-backup`,
`restore`, `force-single-member`) never need a key.

Generate a key once per operator:

```bash
uc2ctl gen-admin-key /etc/uc2/admin/alice.key
```

This writes 32 random bytes at mode `0600` (refuses to overwrite an existing
file) and prints the exact `[admin]` snippet to paste into every node's
config that should accept it — the key name defaults to the file's stem
(`alice`), and it must match across every node that should honor this key.

Pass it on every mutating command:

```bash
uc2ctl add-learner --instance-dir D --app-id A --id 4 --addr 10.0.0.14:9100 \
  --admin-key /etc/uc2/admin/alice.key
```

Three admin flags. They live in the shared `CommonArgs`, so `status` also
accepts them on the command line — but `status` never reads or needs them
(it writes no admin request). The offline verbs (`backup`, `verify-backup`,
`restore`, `force-single-member`) don't reuse `CommonArgs` at all, so these
flags are not valid there:

| Flag | Default | Meaning |
|---|---|---|
| `--admin-key PATH` | none (unsigned request) | sign this request under the named key |
| `--admin-key-name NAME` | `--admin-key`'s file stem | the key's name as loaded into the node's `[admin].keys` |
| `--admin-ttl-secs N` | `30` | how long the signature stays valid, counted from the moment `uc2ctl` signs it |

A bad key file (missing, wrong length, group/world-readable) is caught
before anything is written to the node's admin band — nothing is sent, and
the error names the path and points back at `gen-admin-key`.

**Pair `[admin] auth = "hmac"` with `[crypto].enabled = true`.** A follower
that authenticates a request locally forwards it to the leader over the
node-to-node UDP socket (wire kind 16, `ConfigProposal`), not the admin
band — with `[crypto].enabled = false` that plane trusts whichever address
a datagram claims to be from (the leader drops datagrams from addresses
outside the current membership, but a network-path adversary can spoof a
member's address). Signed admin requests only authenticate cluster-wide once
wire crypto is also on; see [Encrypt traffic between nodes](encrypt-node-traffic.md).

If a request is refused for an auth reason, `uc2ctl` prints one of:

| Reason | What it means |
|---|---|
| `auth_missing` (20) | the node requires a signed request; pass `--admin-key` |
| `auth_bad_tag` (21) | wrong key, a stale/cleared auth line, or a tampered request |
| `auth_expired` (22) | the signature's window has passed, or was stretched implausibly far into the future — check clock skew between `uc2ctl` and the node |
| `auth_unknown_key` (23) | this key name is not in the node's `[admin].keys` |
| `audit_failed` (24) | the node could not record the request to `audit.jsonl` (a full or failing disk) and refused rather than act unaccountably — **this does not mean nothing happened**: on the leader's accepted path the change may already be appended; check `uc2ctl status` |

## Read the audit log

Every admin request a node answers — accepted, refused, or retried — is
recorded to `<instance_dir>/audit.jsonl` before the answer is published, so
the log is never behind what an operator actually saw:

```bash
uc2ctl audit --instance-dir D
```

```
1755600000000000000  ops-alice  local  add_learner  4  10.0.0.14:9100  accepted(0)  cfg=7
```

`--tail N` shows only the last `N` records; `--json` prints the raw line
instead of the summarized form. `audit` is offline — it reads the file
directly, works whether or not a node is running, and needs no `--app-id` or
key. A line this small parser cannot make sense of (a torn write from a
crash mid-record, say) prints as-is with a leading `?` rather than being
dropped. See [Instance directory](../reference/instance-directory.md) for
the file's durability class and [Monitor a cluster](monitor-a-cluster.md)
for the same event mirrored to the structured-log sink.

## Before you start: pair with snapshots if you write continuously

A fresh learner catching up purely by log replay can be outrun indefinitely by
a fast enough writer. It is not a hang — stopping the writer lets it finish —
but under sustained load it may never converge, and `promote` will keep
refusing with `NotCaughtUp`.

If your deployment has meaningful sustained write load, turn on snapshots and
purging first so each new learner converges by snapshot install plus tail
replay: [Keep the journal from growing without bound](bound-journal-growth.md).

**M14a limitation:** a snapshot session ships FSM 0's artifact only. On a node
declaring more than one service id (`[services]` with `ids` beyond `[0]`), a
learner joining a cluster whose journal prefix has already been purged cannot
rebuild FSM 1..N-1 below the floor — its `min_applied` stays pinned at 0, its
report ceiling stays pinned at `fsm_lag`, and it can neither converge nor pass
the `promote` catch-up check. This is fixed in M14c, which ships per-FSM
snapshot artifacts. With purge disabled (the default), a learner replays from
genesis instead and is unaffected.

## Add a voter

Two independent changes, in order.

```bash
uc2ctl add-learner --instance-dir D --app-id A --id 4 --addr 10.0.0.14:9100
```

Wait for the learner to catch up. Watch its `reported_durable` climb toward
`commit` in `uc2ctl status`. Then:

```bash
uc2ctl promote --instance-dir D --app-id A --id 4
```

A refusal of `NotCaughtUp` means the learner is still too far behind to promote
safely. Wait, or reduce write load, and try again.

## Remove a voter

You cannot remove a voter directly. Demote it first:

```bash
uc2ctl demote        --instance-dir D --app-id A --id 4
uc2ctl remove-learner --instance-dir D --app-id A --id 4
```

Removal tombstones the id permanently.

## Resize the cluster

Repeat the add-a-voter sequence once per new member; repeat the remove sequence
once per departing member. There is no bulk operation — a 3 → 5 resize is two
independent single-server changes, and 5 → 3 is two more.

## Retire the leader

Demoting a leader is refused with `SelfDemote`. There are two routes.

**To remove it outright**, run `remove-voter` with the leader's own id, against
any node:

```bash
uc2ctl remove-voter --instance-dir D --app-id A --id 1
```

The leader keeps serving until its own removal commits, then steps down, and
the remaining voters elect a new one. Reads accepted during the window are
served; reads still in flight at the halt are answered `MSG_V2_RETRY`, which
clients redirect on. No committed entries are lost across the handoff.

**To turn it into a learner instead**, remove it as above, then `add-learner`
with a *fresh* id on that host. The old id is tombstoned and can never return.

## Check for stale members before removing one

`uc2ctl status` marks any member whose last reported durable trails commit by
more than one admission window:

```
-- STALE: N bytes behind commit
```

**The CLI does not block on this, and the judgement is yours.** Removing a
live-but-stale voter can stall the cluster: it can no longer acknowledge the
commit of its own removal, and if it was needed for quorum you have just made
that quorum unreachable.

Read the warning before running `remove-voter` on anything you have not
independently confirmed is actually down.

## Decommission a removed node

A node that adopts a configuration excluding its own id halts fail-stop at
once: its heartbeat freezes and it never re-claims leader or serving status.
Stale datagrams from its address cannot disturb the survivors.

The halted process parks rather than exiting. Stop it as you would any other
dead process — there is no further protocol step.

## Replace hardware

Bring the replacement up with a **new id and a fresh, empty instance
directory**.

A tombstoned id can never rejoin, under any circumstances, including on new
hardware. And a new id must never inherit an old id's `journal/` or `state/` —
`state/config.state` still carries the old id's membership and tombstones.
Reusing a directory across ids is undefined behaviour, not merely discouraged.

## Upgrading a mixed-version cluster

Finish the rolling restart, then reconfigure. Reconfiguring during the upgrade
window is not supported.

## If a change is refused

`uc2ctl` prints the reason. The full table is in
[`uc2ctl`](../reference/uc2ctl.md#refusal-reasons); the ones that most often
surprise:

| Reason | What to do |
|---|---|
| `ChangePending` | a change is still committing — wait and retry |
| `NotCaughtUp` | the learner is behind; wait, or reduce write load |
| `Tombstoned` | this id was removed before; use a fresh id |
| `SelfDemote` | use `remove-voter` on the leader instead |
| `NotServing` | the previous change is still settling |

Serialize your admin clients. At most one may write a given node's admin band
at a time — that includes `uc2ctl` and any harness that writes it directly.
Two concurrent clients can interleave field writes and compose a request
neither sent. The node validates every field, so the worst case is a nonsense
refusal rather than corruption, but it will waste your time.
