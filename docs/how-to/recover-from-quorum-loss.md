# How to recover from quorum loss

You have lost a majority of voters — enough hosts destroyed or unrecoverable
that no live quorum can elect a leader or commit anything. This procedure
forces a single surviving node back into service, **with data loss**, and
states exactly what is lost before it writes anything.

If you have not lost a majority — one node down, one host to replace — you do
not need this page. See [Change cluster membership](change-cluster-membership.md)
for an ordinary live reconfiguration, or
[Back up a cluster](back-up-a-cluster.md) if what you actually have is a
single node to restore against a still-healthy majority (a different, safe
operation this page's data-loss statement does not apply to).

## What you are about to accept

Every acknowledged write not held in the surviving node's own journal is
gone. `uc2ctl force-single-member` prints this before it writes, using the
exact same wording the tests pin byte-for-byte:

```
forcing node <N> to a single-member cluster at durable position <D>: any
write acknowledged by the old quorum but not held in this node's journal is
LOST; peers <ids> are dropped from the config and must be wiped and rejoined
as fresh learners.
```

There is no way to know, from the survivor alone, whether anything actually
was lost — only the dead peers' journals could tell you, and by definition
you cannot reach a quorum of them. Treat every write acknowledged after the
survivor's own durable position as gone, because you cannot prove otherwise.

## Force the survivor to a single-member cluster

The tool is offline: it never talks to a running node's control page. It
takes the instance directory's exclusive `flock` directly — the same lock a
real node holds — so **stop the node first**, or the command refuses with "a
node is running."

```bash
uc2ctl force-single-member \
  --instance-dir /srv/uc2/n0 \
  --app-id myapp \
  --node-id 0 \
  --confirm-cluster myapp
```

`--confirm-cluster` must echo `--app-id` exactly, or the command refuses
before touching anything — typing the cluster's identity a second time is the
only guard between an operator and an irreversible, data-losing write.

If a stale `cnc2.dat` is present from the node's last run, the command
cross-checks its recorded `node_id`/`app_id` against your flags first (a
free correctness check against a wrong `--instance-dir`/`--node-id` pairing);
its absence — an entirely fresh directory — is not an error, just nothing to
cross-check.

What gets written: a new `ConfigRecord`, pinned at the recovered durable
position, naming `--node-id` the **sole voter**. Every other voter and
learner from the recovered config is **dropped, not tombstoned** — they are
not permanently barred, they wipe-and-rejoin later as fresh ids (see below).
Vote and term-map state are left untouched entirely; quorum-of-1 falls out of
the election logic simply reading a one-voter config on the survivor's next
boot, not from any force-specific code path.

## Why it can refuse

`force-single-member` refuses (writing nothing) rather than guess, in every
one of these cases:

| Refusal | Meaning |
|---|---|
| a node is running | stop it first — this is offline-only |
| `--confirm-cluster` doesn't match `--app-id` | typed confirmation failed |
| no durable config record | this instance dir has never booted a real node — nothing to force |
| doubly-ahead crash window | two config adoptions were durably persisted in the same crash before any archive catch-up — nothing genuine is left to revert to; wipe this dir and rejoin it as a fresh id instead of forcing it |
| tombstoned id | a permanently-removed id can never be forced back in |
| not a member | `--node-id` is not a voter or learner in the recovered config |

The "no durable config record" and "doubly-ahead" refusals both exist for the
same reason: the underlying recovery logic has genesis-seeding and revert
paths that are correct for a real node *booting*, but would be silent data
loss if this offline tool let them fire — so it inspects the state first and
refuses outright rather than falling back to a fabricated empty config.

## Bring the survivor up

Start the node against the same `--instance-dir` as normal. It boots reading
the forced one-voter config, elects itself immediately, and serves.

```bash
uc2-node --config /etc/uc2/node.toml
uc2ctl status --instance-dir /srv/uc2/n0 --app-id myapp
```

Confirm `leader=true can_serve=true` and exactly one member in the config.

## Wipe and rejoin the dropped peers

The dropped peers are not tombstoned, so once their hosts are usable again
(repaired, replaced, or simply the same host with the old instance directory
cleared), they rejoin the same way any new member would — **as a fresh id**,
the same fresh-forever idiom every other membership change in
[Change cluster membership](change-cluster-membership.md) uses for a
permanently-removed (tombstoned) member:

1. **Wipe the dropped peer's instance directory.** Its old `journal/` and
   `state/` reflect a config that no longer exists on the survivor's side; do
   not attempt to reuse them.
2. **Bring it up as a fresh learner, under a fresh id** (never one of the
   original cluster's ids):
   ```bash
   uc2ctl add-learner --instance-dir D --app-id myapp --id 3 --addr 10.0.0.11:9100
   ```
3. **Promote once caught up**, exactly as in
   [Change cluster membership](change-cluster-membership.md#add-a-voter).

A dropped (not tombstoned) peer's **old** id is not refused by anything in
`force-single-member` or boot — nothing marks it as forbidden the way a
tombstone does — but reusing it is **untested and not the recommended
practice**, and this page does not walk through it. Every recovery path this
project actually exercises (the `survival.rs` e2e behind
`--features survival-tests`, and the module doc for
`uc_node::recovery::force_single_member`) rejoins a dropped peer under a
fresh id, exactly like step 2 above, and explicitly never reuses the old one.
Follow that idiom rather than the old id: it is what is actually proven,
not merely assumed safe.

## What this is not

- **Not a substitute for backups.** This procedure recovers *service*, not
  *data* — everything above the survivor's durable position is gone, and
  nothing here reconstructs it. Regular verified backups
  ([Back up a cluster](back-up-a-cluster.md)) are the only thing that bounds
  how much a quorum loss costs you; this tool exists for after the fact, not
  instead of them.
- **Not something to script around.** There is deliberately no
  non-interactive fast path and no "undo" verb — `--confirm-cluster` exists
  to put a human in the loop before a one-way, data-losing write. If you find
  yourself wrapping this in automation that supplies `--confirm-cluster`
  unattended, you have built a way to lose data without anyone confirming it.
- **Not a live operation.** Every other `uc2ctl` verb talks to a running
  node's admin band; this one refuses to run against one. If the node holding
  the instance directory is still up, use the ordinary reconfiguration path
  in [Change cluster membership](change-cluster-membership.md) instead — you
  do not have quorum loss if the node you'd force is still serving.

## Where to go next

- [Back up a cluster](back-up-a-cluster.md) — the routine-maintenance
  procedure this one is the "after the fact" complement to, including the
  minority-restore rule this page's majority case sits opposite.
- [Change cluster membership](change-cluster-membership.md) — the live
  add/promote/demote/remove path used above to rejoin dropped peers.
- [`uc2ctl`](../reference/uc2ctl.md#offline-commands) — full flag reference
  for `force-single-member`.
