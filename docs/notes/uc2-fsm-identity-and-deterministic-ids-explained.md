# FSM identity and deterministic IDs, explained

*Written 2026-09-02 for the FSM identity work (implemented on branch
`uc2/fsm-identity`, release on hold). Spec:
`docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` — this note
draws on §1–§2, §3.3–§3.4, §4–§7, and reproduces §2.1's comparison table
verbatim below.*

## The problem in one sentence

Before this work, a state machine's place in the cluster was a bare number
— `service_id: u8` — and nothing anywhere checked what logic lived behind
that number. Two nodes could agree on the *set of numbers* they both
declared and disagree completely on what each number *meant*, and nothing
would tell them. This note explains what changed, why it changed this way
and not some other way, and what an operator sees when it catches a real
mistake.

## Why identity is in code, not config

The maintainer's framing when this started: "FSM represents business logic
and should generate same deterministic IDs across nodes. FSM on node1 in
slot1 should create same ID as same FSM on node2 slotX." Read that sentence
carefully and it already answers where identity belongs: it is a property
of the *logic*, not of where an operator happened to put it. A name typed
into a `node.toml` on three different hosts can drift — a typo, a copy-paste
that missed one host, a config template edited in the wrong order — and
nothing catches it until something breaks. A name that ships as part of the
compiled binary cannot drift between hosts, because it is not a per-host
fact at all.

That is why `RawStateMachine`/`StateMachine` gained a **required**
`const NAME: &'static str`, and `uc_service::ServiceConfig` **lost** its
`.service_id()` setter rather than gaining a `.name()` beside it. A service
no longer tells the node which row it is; it tells the node what it *is*,
and the node works out which row that is by matching names. If your FSM's
name doesn't appear in the node's declared list, you get refused at the
door — `ServiceError::UnknownFsm { name, declared }` — instead of silently
attaching to whatever row happened to be free.

## Why the row stays, and what "named rows" refuses that M14 did not

The obvious next question, once identity lives in code: if the name is
what actually matters, why keep the row (the slot number) at all? Two
designs were on the table, compared side by side in the spec (reproduced
below): keep the row as a required, positionally-checked index — "named
rows" — or cut the row loose entirely and let each node order its FSMs
however it likes, routing snapshot artifacts by a hash of the name instead.

The cut design buys real things: names on disk, a client that resolves an
FSM by name without first learning which row it landed on anywhere, restore
by name. It also costs real things: hash-routed snapshot artifacts, renamed
rings and directories, a handle-based client API, a default-row field for
the remote path, and a much larger surface to test — for a freedom nobody
had actually asked for. UC already requires the lag policy, the crypto
mode and the declared FSM set to be identical across every node; asking for
the *order* to match too is not new discipline on top of that, it is the
same discipline extended one field further.

So the row keeps exactly the meaning it had under M14: a cluster-wide
index, required to match on every node, the per-FSM analog of `app_id`.
What changes is that the row's identity — its declared *name* — is now
checked everywhere the row itself was already checked, and refused **by
name** when it doesn't match. Concretely, named rows refuse two things M14
could not:

- **Same logic, different numbers.** M14 would stall a joiner with an
  opaque declared-set mismatch and no way to say *why*. Named rows refuse
  by name: "row 1: ours=orders, theirs=kv" tells you immediately which row
  disagrees and what each side thinks is there.
- **Different logic, same number** — the worse failure, because M14 didn't
  even detect it. Two replicas of "FSM 1" silently applying different code
  to the same log is exactly the failure the maintainer named at the start.
  A name check at attach, plus the positional hash check on every snapshot
  session, turns that from silent divergence into a refused session and an
  alerting gauge.

### §2.1, verbatim: three designs compared, and the cut

| aspect | UC today (M14) | Aeron Cluster | **named rows (this spec)** | placement-independent rows (first draft, cut) |
|---|---|---|---|---|
| identity declared | operator: `service_id` u8 in `ServiceConfig` | operator: `serviceId` int per container | **code: `const NAME`** | code: `const NAME` |
| node config | `ids = [0, 1]` | `serviceCount` | **`names = [...]`, index = row** | `names = [...]`, order free |
| service states its row | yes | yes | **no — found by name** | no |
| rows agree across nodes | by convention, unchecked | by convention, unchecked | **required, checked by name** | not required |
| cross-node check | bitmask on `SNAP_BEGIN` | none | **hashes in row order** | hash set, order ignored, + default |
| when the check fires | snapshot sessions | never | snapshot sessions (+ alerting on exported hashes) | snapshot sessions |
| wrong logic at same row | silent divergence | silent divergence | **refused by name** | refused by name |
| artifact routing | by row | by id | **by row, unchanged** | by hash to the local row |
| on-disk names | `<id>` | `<id>` | **`<id>`, unchanged** | `<name>` everywhere |
| client API | `submit_to(u8)` | session-based | **+ `fsm("name") -> u8`** | handles, `submit_to(&handle)` |
| remote default | row 0 | none | **row 0, its name checked** | named `default`, page-1 field |
| per-FSM version | none | one cluster-wide `appVersion` in the log | **`const VERSION`, equality-checked** (§7) | reserved bytes only |
| deterministic IDs | none | none (inputs only) | **`IdGen` from `NAME`** | `IdGen` from `NAME` |
| flag day | — | — | cnc + wire, one release | cnc + wire, one release |
| size | shipped | shipped | **small milestone** | M14c-sized |

What Aeron Cluster actually does (read from source, 2026-09-02) is worth
knowing precisely, because it is easy to over-credit: `serviceId` is a dense
index, range-checked against a configured count and nothing else — the
count never even travels between nodes, so Aeron checks *less* than M14
did. Its `serviceName` looks like an answer to the same question this work
solves, but it is cosmetic: it feeds a log role name and a mark-file
header, never validated, never on the wire. UC already exceeded Aeron
before this work (the M14 bitmask); named rows go one step further than
either.

The piece the cut design left on the table — names on disk, client
handles — touches no wire and can still be added later without a flag day,
if a real reason shows up. That door is deliberately left open (spec §11);
nothing here closes it.

## What the version promises, and what it deliberately does not

Every state machine now carries `const VERSION: u32 = 0`, a packed semantic
version (`major:8 ‖ minor:8 ‖ patch:16`, UC's existing `ProtocolVersion`
layout — the same bit order Aeron's `SemanticVersion` uses, so the packed
integer compares in semver order without re-encoding). `0` means
unversioned; a real FSM sets it to something meaningful to its own release
process.

This exists to answer one operational question: *which version of `orders`
is running on each node, right now?* The service writes it into its cnc
slot at attach; `uc2ctl status` and `/metrics`
(`uc2_service_version{service="<name>",row="<r>"}`) read it back. On the
snapshot path, `SNAP_BEGIN` carries each row's version, and the two sides
are compared for **equality** — but only when both sides report something
non-zero. `0` on either side means *unknown* (a joiner whose service
process hasn't attached yet, most commonly), not a mismatch, so an
unversioned FSM keeps working exactly as before and a joiner in the middle
of starting up doesn't spuriously refuse.

**What it does not do is define compatibility.** Equality is the only
relation this ships: there is no floor, no validator, no notion of "close
enough." It is not stamped into the log, so a version mismatch is visible
on the snapshot path — where two nodes exchange artifacts — and nowhere
else; two nodes running different versions of the same FSM on the live
commit path will keep applying commands and can silently diverge if their
logic actually differs. The version makes that mismatch *visible*; it does
not make it *safe*.

This is deliberately the smaller half of a two-part mechanism Aeron
ships whole. Aeron's `appVersion` has a static half — `ctx.appVersion()`,
configured on every module and container — and a leader-stamped half: the
current version rides in every `NewLeadershipTermEvent` and every snapshot
marker, and a pluggable `VersionValidator` (default: major-equality)
checks the static value against the stamped one on every new-term event and
every snapshot load, fail-stop on mismatch. This work ships only the static
half — the per-binary `const VERSION`, checked at attach and on the
snapshot path. The log-stamped half — a term-boundary log event carrying
the version, a validator, and the rolling-upgrade semantics that follow
from having both — is left for later (`docs/BACKLOG.md` item 3), which now
has a concrete field and comparator to build on rather than reserved bytes.

One more thing the version is explicitly *not*: an `IdGen` input. An
FSM's ID series depends only on `(position, name, ordinal)` — never on
`VERSION` — because an upgrade must not change what a replay of the same
log mints. Whether two versions of an FSM actually apply a given command
identically remains the user's own determinism obligation; the version
makes a mixed deployment visible and, on the snapshot path, refused. It
does not make one correct.

## `IdGen` — deterministic IDs, and why per-apply scoping is the whole story

The first thing this identity work actually buys a state machine, beyond
safety checks, is `uc_service::ids::IdGen` — a way to mint IDs inside
`apply` that are the same on every replica, with zero coordination and zero
state to snapshot.

```rust
fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> Resp {
    let mut ids = ctx.ids();     // one generator per apply call, by construction
    let order_id = ids.next();   // u128
    let line_id  = ids.next();   // u128, unrelated-looking to order_id
    ...
}
```

The input to each ID is 128 bits: the frame's absolute log position (64
bits), an ordinal counting up from zero within this one `apply` call (32
bits), and a 32-bit fold of the FSM's identity hash (`fold32(h) = (h >> 32)
as u32 ^ h as u32`). That goes through a three-round Feistel permutation
using murmur3's `fmix64` as the round function — frozen, with golden
vectors pinned from the first run, exactly like the wire constants. A
Feistel network is a bijection for *any* round function, so two distinct
inputs are guaranteed to produce two distinct IDs by construction, not by
birthday-bound odds — the same guarantee a random 128-bit UUID only gives
you probabilistically, given here for free by the permutation's structure.

**Why the ordinal resets to zero every apply call, and never accumulates
across the FSM's lifetime, is the entire correctness argument.** Picture
the alternative: a counter that lives in the state machine's own struct
and increments every time `next()` is called, for as long as the process
runs. That counter diverges from reality in at least three ways nobody
would notice until it was too late:

- **A snapshot-installed replica has called it 0 times; a journal-replayed
  one has called it N times** for whatever N commands the replay walked
  through. Same logical state, different counter value — the two replicas
  would now mint different IDs for the same future command.
- **A linearizable read that happens to call the "next ID" function**
  advances the counter only on the replica that served that particular
  read. Reads aren't supposed to be replicated, so this counter would
  silently fork across replicas the moment any read touched it.
- **A restarted service has to recount from wherever its last applied
  position left off**, which means either persisting the counter
  separately (more state to keep consistent, more ways to lose it) or
  guessing — and guessing wrong is exactly the kind of divergence SMR
  exists to prevent.

Scoping the ordinal to one apply call sidesteps all three at once: the ID
series is a pure function of committed input. Nothing about it needs to be
snapshotted, because it isn't state — it's arithmetic over the frame you
were just handed. A node that installed a snapshot and tail-replayed, and a
node that replayed the whole journal from genesis, compute the identical
series for the identical future command, because both of them are looking
at the same `(position, identity, ordinal)` triple when they compute it.

The type system backs this up, not just the documentation: `ApplyCtx::ids()`
is the *only* way to get an `IdGen`, `query` receives no `ApplyCtx` at all
(so there is no context to build one from on the read path), and `IdGen`
is `!Send` — so the obvious mistake of stashing a generator into `self` to
reuse across calls fails to compile rather than merely being discouraged in
a doc comment.

### The retry rule under `Sessioned`

`uc_service::session::Sessioned<S>` gives a service exactly-once semantics
over a retried remote call: a client that resends the same `(client_id,
seq)` pair gets back the *cached* response from the first attempt, tagged
`REPLAYED`, and — this is the part that matters here — the inner state
machine's `apply` is never called a second time for that resend. Since
`IdGen` only exists inside `apply`, a replayed command never calls
`ctx.ids()` again either: the IDs a client sees for one logical write are
stable across every retry, because the second (and third, and Nth) attempt
never reaches the code that mints them. Without `Sessioned`, a retry is
just a new log position carrying the same bytes, and a new position always
means a fresh `IdGen` — the "duplicate write, new IDs" outcome is the
ordinary at-least-once behavior, not something the ID generator is
responsible for fixing on its own.

### The cross-FSM fold rule

Two FSMs on the same log mint disjoint ID series automatically, because
their `fold32(identity.hash)` values differ — no coordination between them
is needed, and none is provided. The one thing this doesn't guarantee is a
32-bit fold collision between two *specific* names chosen for one
deployment; that's a one-line test the fold being public makes easy to
write, and it's the deploying user's responsibility, not the framework's,
because the framework has no way to know your two chosen names in advance.
Renaming an FSM changes the series it mints from that point on (the name is
part of the input) and never collides with what the old name minted,
because the inputs genuinely differ. IDs are unique within one cluster —
one `app_id` — not across clusters, which matches the scope everything
else in UC operates at.

### Why no Snowflake

It would be natural to want IDs that sort by wall-clock time — Snowflake,
ULID, that family. This deliberately does not provide that, for a reason
that is structural, not an oversight: the FSM has no clock. `apply` is
required to be deterministic, sync, no I/O — reading the system clock
inside `apply` would make two replicas diverge the instant their local
clocks disagreed by even a microsecond. A leader-stamped timestamp would
fix that, but it's a frame-header change, and it's being designed
separately (the parallel timestamps/scheduler work this spec's `ApplyCtx`
was shaped to make room for — see the "why a context, and why now" note in
spec §3.3). What `IdGen` gives you instead — strict, global,
time-correlated ordering via the log position itself, since positions only
ever increase — already answers most of what people actually want a
sortable ID for. True wall-clock sortability belongs at the client edge,
where the client is a real worker with a real clock; the FSM's job is to
store whatever arrives at it, not to mint a timestamp for something it
never directly observed.

## The refusals an operator can actually see, and what to do about them

Everything above is invisible until something is actually wrong. When it
is, here's what shows up and what it means:

**At service startup**, if your service's `S::NAME` isn't in the node's
declared list:

```
FSM "orders" is not declared on this node (declared, in row order:
["kv"]); add it to [services] names on the node, or attach the service
that is
```

Fix: either add the missing name to that node's `node.toml`, in the right
position, or you're running the wrong service binary against this node.

**At node startup**, if `node.toml` has no `[services]` section at all:

```
[services] section is required: names = ["<fsm>", ...] in row order,
identical on every node (FSM identity, 2.11)
```

There is no default any more — a node names every FSM it hosts or refuses
to start, the same posture `[crypto]` and `[admin]` have had since 2.6.0.

**At node startup**, if `node.toml` still has the old `ids` key:

```
services.ids was replaced by services.names (FSM identity): list the FSM
names in row order, e.g. names = ["kv", "orders"]
```

Rewrite the section using `names`, in the order you want the rows in.

**On the snapshot path**, when a joiner or below-floor node opens a session
against a cluster whose declared names disagree, positionally, at some row
(same set in a different order counts as disagreeing — this is a
positional check, not a set check):

```
row 1: ours=orders, theirs=kv
```

— this is the shorthand the spec uses for the meaning of the structured
`snapshot_session_refused` log record (`reason = "identity mismatch"`,
`row`, `ours`, `theirs` as separate JSON fields — `uc_node/src/node.rs`'s
`report_snapshot_refusals`; a name the receiver recognizes anywhere in its
own list is printed as that name, an unrecognized one as its raw hash),
read as this node's row 1 (`ours`) versus the peer's row 1 (`theirs`). Fix:
make `[services] names` identical, **in the same order**, on every node,
and restart the odd one out. The session stays refused — never
half-installed — until you do; the joiner keeps NAKing rather than
converging on the wrong FSM. Steady state, before any session even runs,
the same drift shows up as the `Uc2ServiceIdentityDrift` alert on the
exported `uc2_service_identity_hash` gauge.

**On the snapshot path**, when both sides report a non-zero version for the
same row and they disagree, the same log record fires with `reason =
"version mismatch"` and both packed versions (`ours_version`,
`theirs_version`) alongside `row`/`ours`/`theirs` — in shorthand, "row 0:
ours=orders (v1.0.0), theirs=orders (v2.0.0)". Fix: attach the same build
of that FSM's service everywhere, or accept that a rolling upgrade is in
progress and expect this until it finishes. `Uc2ServiceVersionDrift` is the
steady-state counterpart, watching `uc2_service_version`, and it
deliberately ignores the `0` case (a service that hasn't attached yet) so
it doesn't page on ordinary startup.

None of these refusals is fatal to the cluster: a mismatched or mixed-name
session stays stalled-but-safe, never half-installed, exactly the same
posture every prior UC wire flag day has had. What's new is that the
mismatch now says, by name, exactly which row and which names disagree,
instead of leaving you to guess from a bitmask.
