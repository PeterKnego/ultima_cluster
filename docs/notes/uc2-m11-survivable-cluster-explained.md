# M11, explained: what "survivable cluster" actually shipped and why it is shaped this way

A plain-language companion to the M11 gate record
(`docs/benchmarks/uc2-m11-gate-2026-08-20.md`). That document is the
adjudicated record — bars, runs, corrections. This one explains the
mechanisms and the reasoning, for a reader who wants to understand the
design rather than audit it.

## 1. Backup and restore: why an ordering rule instead of a quiesce

The backup artifact is a plain directory holding copies of the three
durable trees — `journal/`, `state/`, `snapshots/` — plus a `MANIFEST` of
the positions the tool observed. No format, no compression, no daemon
coordination. The design question was never the format; it was: how can a
copy of a *running* node's files be sound when purge, snapshot retention,
and the appender are all mutating them mid-copy?

The answer is one enforced ordering — `journal/` first, then `state/`,
then `snapshots/` — resting on two monotonicity facts:

- the journal's first retained position only advances (purge deletes
  whole old segments, never middles), and
- the newest snapshot position only advances (publishes are atomic
  tmp+rename, retention keeps the newest two, and purge only ever runs
  below a durably-persisted floor that some retained snapshot covers).

So a snapshot set copied *after* the journal always covers any purge that
happened *before* the journal copy. Reverse the order and you can capture
yesterday's snapshots with today's purged journal: an artifact whose
snapshot doesn't reach back to the journal's first byte — a hole that
looks fine until restore day. `verify-backup` asserts the invariant
(`newest snapshot ≥ journal first-base`) instead of trusting the
operator, and the test suite constructs a genuine wrong-order-across-a-
purge artifact and proves verify rejects it.

Three sharp edges the tool absorbs so the operator never sees them:

- **A purge deleting files mid-copy.** The copy retries the whole
  directory (bounded, then a named error). Skipping the vanished file
  would be quietly catastrophic: segment N copied, purge deletes
  N+1..N+5, N+6 copied — a mid-journal gap the first-base check cannot
  see. Restarting the directory copy is equivalent to having started the
  backup later, which the ordering rule already covers.
- **Copying the actively-appended segment.** A torn trailing record is
  byte-for-byte the state a power cut produces, and recovery heals it as
  end-of-log. `verify` reports the heal (`healed_torn_tail`) — note this
  reads `true` routinely for preallocated nodes, because the preallocated
  zero tail is itself "torn-shaped"; it is not an alarm.
- **StableValue files copied mid-write.** The two-slot rotation means an
  arbitrary-instant copy always decodes: at worst one generation stale,
  never corrupt.

`restore` verifies first, refuses a non-empty target, copies the three
trees in, and lets the node's ordinary first boot do everything else
(volatile files are rebuilt unconditionally; the durable config record in
the artifact owns membership, so the seed config in the TOML is ignored).

Two boundaries worth internalizing:

- **The minority rule.** Restoring at most a minority of voters against a
  live majority is safe: the healthy quorum repairs the restored node's
  log rollback by replication, and its stale vote record can certify
  nothing alone. Restoring a majority makes the artifact the cluster's
  new truth — that is the quorum-loss procedure's territory, with its
  data-loss statement, not a backup operation.
- **The offline verbs refuse a live instance directory** (they probe the
  instance flock). `verify`'s one permitted mutation — healing a torn
  tail — is a physical truncate, and pointed at a directory whose writer
  is alive it would race an in-flight append. An artifact never contains
  `instance.lock`, so the probe costs legitimate use nothing.

## 2. Quorum loss: deliberately awkward, provably non-destructive

`uc2ctl force-single-member` exists for the day a majority of the cluster
is permanently gone. It is offline-only (takes the exclusive instance
lock; a running node refuses), and it reuses the node's *real* boot-time
config recovery — exported, not reimplemented — so the config it reasons
about is exactly the config the next boot will reason about.

The forced record's construction is where the correctness lives:

- `position = durable` — the boot-time revert walks back any record
  claiming survival past the journal frontier; pinning at durable makes
  that revert unreachable.
- `version = recovered + 1` — boot re-folds archived CONFIG frames with
  higher versions over the stored record; every archived frame at or
  below durable is already folded into `recovered`, and nothing exists
  above durable, so +1 wins forever.
- **Tombstones untouched.** Dropped peers are not tombstoned — tombstones
  are permanent by design, and the intended future for a repaired host is
  wipe-and-rejoin as a *fresh id* (the M7 fresh-forever idiom), then
  promotion through the ordinary admin path.

The awkwardness is deliberate, per the spec: `--confirm-cluster` must
repeat the app id; the node id is cross-checked against the leftover
control page when one survives; and the data-loss statement is printed
before anything is written — every write acknowledged by the old quorum
but not held in this node's journal is lost.

One property earned the hard way in review: the planning read is
**provably non-persisting**. The recovery function it wraps can seed and
store a genesis record on two paths; the wrapper refuses on both before
that code can run, pinned by tests asserting a failed force leaves a
fresh directory byte-identical. A recovery tool that could destroy the
membership of the node it was rescuing is worse than no tool.

Vote and term state are untouched: once the single-voter config is
adopted, quorum-of-one falls out of the ordinary election logic.

## 3. The disk wall: see it, then survive it

Observability half: the daemon (not any hot-path agent — the four polling
agents gain no syscalls) measures free space on the instance directory
about once a second and publishes it at cnc offset 3840, exported as
`uc2_free_disk_bytes` and alerted by `Uc2DiskLow` when free space drops
below four journal segments. The metric is omitted while unpublished
(library users without the daemon), the same convention as
`uc2_leader_hint`.

Behavior half: nothing new was built, deliberately. A journal write or
fsync error already halts the writer; the archive agent fail-stops; M10's
daemon turns that into a structured `agent_failstopped` record and exit
1; systemd restarts it. M11's contribution is *asserting* the chain
end-to-end in a multi-process crashtest: the full node dies with the
named panic, the surviving two keep acknowledging the client throughout,
and freeing space plus a restart converges the node back. The chain is
errno-agnostic — the local smoke drives it with a permission error, CI
drives it with genuine ENOSPC on a loopback filesystem — and the test
asserts both the errno text and the structurally-guaranteed ErrorKind, so
a refactor of error formatting cannot silently blind it.

One documented asymmetry: the *service's* snapshot-publish path is not
fail-stop — a failed publish is dropped and retried next interval. With a
full disk that means snapshots quietly stop advancing while the node
keeps running until the journal hits the wall; `Uc2DiskLow` is the
operator's early signal for both.

## 4. Flag days: a procedure with a number

UC's protocol changes are flag days on purpose — a mixed cluster stalls
commits rather than doing something unsound. `scripts/uc2_flag_day.sh`
turns the flag day from a runbook paragraph into a measured procedure:
verify all healthy → operator confirms traffic stopped → stop everything
(M9's drain makes restarts replay a tail, not reconstruct) → verify every
stopped node froze at the *same* durable position, read from the leftover
control pages → run the upgrade command per host → start everything →
wait for one serving leader at a common config version → print
`DOWNTIME:`, measured on the orchestrator's single clock.

The script's real engineering is in its failure paths: a bad preflight
refuses before touching anything; a durable mismatch restarts everything
(the un-upgrade path) and exits 1; a partial restart after an abort
escalates with a distinct exit code (3) naming the still-down nodes; and
the error trap actually catches failures inside helpers (`set -Eeuo
pipefail` — bash does not propagate ERR traps into functions without
`-E`, and the status-field parses are additionally tolerant so a
transient garbled status feeds the retry loop, not the trap). A `--local`
mode drives plain processes through identical logic, which is how the
script is validated on a dev box; the fleet execution against the
pre-committed ≤60 s bar is the gate's fleet row.

## 5. The unplanned yield: four journal defects

Putting the journal's recovery and write-ordering under the backup tests'
adversarial load surfaced four pre-existing defects, all fixed on this
branch (commits `1ee907e`, `d502935`, `7f690bc`, `8f9268c`, `7ba74e8`):

1. **A healable crash state refused boot.** A record whose length prefix
   landed while its body was still preallocated zero-fill hard-errored
   the strict recovery scan — though the tail reader already classified
   exactly this state as a torn tail. A `kill -9` at that interleaving
   could refuse a real node's boot. Recovery now heals it, on the active
   segment only; mid-journal corruption still fails loudly.
2. **The heal left a wedge behind.** Healing logically without zeroing
   the residue meant a routine truncation later strict-scanned the
   leftover bytes *after* durably recording its intent — an
   intent-replay/error loop that made the node permanently unbootable.
   The heal now zeroes what it healed, through physical EOF (the
   boundary-straddling last record was a second geometry of the same
   wedge).
3. **A masked acked-durability hole.** A write batch straddling a
   segment roll fsynced only the *new* segment; the rolled-off segment's
   acked bytes were never fsynced by anyone. Invisible today solely
   because the archive appends strictly serially — and demonstrated real
   the moment appends pipeline (47 acked-unfsynced records in the
   reproduction). Fixed by fsyncing the rolled-off segment before its
   successor exists, which also closes the `Durability::Eventual`
   idle-timer variant and a `Consistent` re-drain variant the initial
   analysis missed. The mask is now a guarantee.
4. **A latent writer panic** on a legal truncate-to-empty with the new
   dirty flag set — caught before it ever shipped in a release, fixed
   with the flag surviving truncation.

The meta-lesson repeats this project's oldest one: these bugs lived in
distinctions the existing tests erased (serial-vs-pipelined appends,
zeroed-vs-real torn bytes, logical-vs-physical healing). The backup work
didn't create them; it created the first test surface on which they were
visible.

## 6. Where the proof lives

- Gate record (bars, runs, the row-4 bar-integrity correction, pending
  rows): `docs/benchmarks/uc2-m11-gate-2026-08-20.md`
- Operator procedures: `docs/how-to/back-up-a-cluster.md`,
  `docs/how-to/recover-from-quorum-loss.md`,
  `docs/how-to/upgrade-a-cluster.md`
- CI: the nightly `survival` job (survival + quorum-loss + ENOSPC
  crashtests; the genuine-ENOSPC fixture runs where sudo exists)
- The library surface: `uc2_node::backup`, `uc2_node::recovery`, and the
  four offline `uc2ctl` subcommands (offline = no cnc admin band; the
  reference page draws the distinction).
