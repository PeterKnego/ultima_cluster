# How UC lost committed bytes: the term-map window bug, explained

*(2026-08-16. Companion to the flake-hunt brief
`docs/superpowers/plans/2026-08-16-nightly-flake-hunt-brief.md`, which holds
the raw evidence trail. This note is the narrative version.)*

## The symptom

Between Aug 9 and Aug 16, six of eight nightly CI runs failed. Two failures
were safety verdicts from the elle checker — `incompatible-order`, meaning
two reads observed contradictory histories — and the rest were an assorted
family of "liveness timeouts": elections that never settled, reconfigs that
never adopted, a recovery that missed its deadline. There was also a
standing open item: an acked-write-loss witness that had been firing on
~0.7% of unmutated soak runs since early August.

All of it was one bug.

## The mechanism, step by step

**1. The map and the window.** Every node keeps a *term map* — the list of
`(term, base position)` pairs recording where each leadership term's bytes
begin. It is the instrument a follower uses to answer: "are the bytes I
hold a valid prefix of the current leader's history, and if not, where
exactly do they diverge?" Leaders gossip their map so followers can run
that comparison ("reconcile"). But a UDP datagram is small, so a leader
ships only the **last 64 entries** — a sliding window over its history.

**2. The alignment bug.** The reconcile routine compared the two maps
*entry by entry from index zero*. That is correct exactly as long as the
shipped window IS the whole map. The moment a cluster's lifetime leadership
count passes 64, the window no longer starts at genesis: the leader's first
shipped entry is some mid-history entry, while the follower's entry 0 is
still `(term 1, position 0)`. Index-aligned comparison finds zero matching
entries and concludes the histories share **no common prefix** — the
verdict reserved for a node so far behind that the leader has purged the
data it would need. The prescribed remedy for that verdict is drastic:
**wipe the entire local log and rejoin empty**.

So: after the 65th term, every reconcile against every healthy follower
ordered a full wipe. A code comment claimed this branch was "unreachable
at ≤ 64 terms" — true, and precisely the problem: nothing had ever tested
beyond it. The Lean proof of this subsystem explicitly modeled gossip as
shipping the *full* map ("simplification, decision 7"). The bug lived in
exactly the distinction the model erased.

**3. The wipe loop.** A wiped follower refills from position 0 via paced
journal replay from the leader. Refilling is slower than gossip: before the
follower can rebuild far enough to bridge the leader's window, the next
gossip arrives, reconcile runs again, and wipes it again. Traced live:
**179 full wipes in 42 seconds**, each discarding up to ~570 KB of bytes
the node's own commit counter showed as committed.

**4. The loss.** Wiping your own copy of committed bytes is survivable —
others hold them. But elections rank candidates by how much durable history
they hold, and a mid-wipe node honestly reports almost nothing. Kill
leaders fast enough (the nightly elle pass does this every 1.2 s) and
eventually an election happens while a **quorum** is mid-wipe. The
amnesiacs elect one of themselves, the new leader's history starts below
the old committed frontier, and when the one node that still held
everything comes back, it adopts the new timeline and truncates its own
committed tail to match. At that point the committed bytes exist nowhere —
in the captured run, 1,191 acknowledged writes (~52 KB) vanished
cluster-wide. The forensics were unambiguous: all three log buffers still
contained the orphaned term-103 byte range as ghost frames, all three term
maps had a hole where terms 89–123 used to be, and the commit counter still
pointed 52 KB past anything any journal held.

**5. Why reads lied.** Two aftershocks turned loss into visible
inconsistency:

- The **commit counter is never rewound** and is propagated by gossip, so
  the phantom frontier survived every reboot — barrier reads certified
  against coordinates from a dead timeline.
- The **service never learns the log rewound**. Its applied cursor is
  monotonic; when the log was cut beneath it, it idled — serving answers
  from the dead timeline's state — and once the new timeline grew past its
  cursor, it resumed applying new bytes *on top of old state*, merging two
  histories in one state machine. That merged state is what elle flagged
  as `incompatible-order`.

**6. Why nodes also died** (the "liveness" family). With wipes gone, a
second bomb surfaced from behind the first: the *persisted* term map lives
in a ~4 KiB durable slot, which overflows at about 340 lifetime entries —
and the persist call fail-stops the consensus thread. Under churn that is
minutes away. Pre-fix, the wipe loop kept resetting maps and accidentally
hid this; the "no survivor leader within 20 s" nightly assert is its
downstream signature.

## The fixes

1. **Alignment** (`uc_consensus::reconcile`): locate the leader's first
   shipped entry *inside* the follower's full map (terms are strictly
   ascending, so the position is unique) and compare from there. Entries
   below the window are the follower's honest record of history the leader
   simply didn't ship — absence is not contradiction. `NoCommonPrefix` now
   fires only for the genuine purged-prefix case. A window that starts
   inside our bytes at a term we never observed is proven divergence and
   truncates *at that point* instead of wiping everything.
2. **Rewind tripwire** (`uc_service`): if local durability ever drops
   below the service's applied cursor, the incarnation is poisoned — it
   stops applying and answers every read with RETRY — instead of serving
   dead-timeline answers or merging timelines. (In a healthy cluster this
   is unreachable: truncation never cuts below commit and apply never
   passes commit.)
3. **Persist clamp** (`uc_log`): the durable copy of the term map keeps
   only its newest entries, clamped by asking the encoder rather than
   counting. Boot re-derives the full map from journal frame headers
   anyway; the durable copy's only job is recent coverage.
4. **Lean/conformance**: the Rust fix is expressible as a thin wrapper
   around the unchanged proof core (`reconcileAligned`), so the existing
   theorems stand and the 100k-vector conformance suite passes with zero
   divergence against the new semantics.

## The follow-on fix: commit validation

The tripwire immediately exposed a real, pre-existing race, and that race
is now fixed too. **Commit gossip is position-only.** A follower that
adopts a new term holds a tail whose *content* no leader has validated —
the term-map reconcile is a separate datagram, arriving later or not at
all. Accepting the new leader's commit position blessed the follower's own
bytes at positions the new timeline owns, so the service applied a deposed
leader's content there and the late reconcile cut then landed beneath the
applied cursor. Raft gates `commitIndex` on the AppendEntries
`prevLogIndex`/`prevLogTerm` match; UC's equivalent evidence is the term-map
reconcile, so the commit advance now waits for it — an `awaiting_reconcile`
latch in the safety core, armed on adopting a strictly higher term and
released when this term's leader map reconciles clean or its truncation
acks. Held positions replay as one advance, so the cost is one gossip
round: the leader ships its term map alongside every commit gossip.

Two things that fix taught us. First, the model omitted this plane on
purpose — `ProtocolCommit.lean` §10 lists *"commit gossip / follower
`commit_seen`"* as a documented YAGNI omission. Same shape as the windowed
map. Second, removing the wipe loop made the system roughly eight times
faster, and that alone broke the term-map slot clamp: entry width under
bincode varints is value-dependent, so bigger terms and byte positions
overflowed a clamp that counted entries instead of asking the encoder.

## What is still open

The rewind did not go away — it went from 13-27 to 6-14 occurrences per
300-second kill storm, and the pre-committed bar for that fix (zero) was
missed and recorded as missed. Commit provenance tracing says every
surviving case is `gossip`: a follower validated cleanly against leader A,
took A's gossiped commit C, and a later leader B truncated below C. Either
A committed C without a surviving quorum, or B is missing committed bytes.
That is the quorum plane — the Figure-8 family — not the apply path, and
it is proofs-arc work. Severity is bounded: no acknowledged write was lost
across 8 x 300 s campaigns, and followers never publish responses, so the
damage is confined to a node that then poisons itself and is respawned.

A service that detects the rewind now POISONS its incarnation rather than
panicking: it stops applying (never merging the dead timeline with the new
one) and answers every read with RETRY (never serving dead-timeline
state), keeping its heartbeat so a supervisor can respawn it. The panic
version was correct in production and wrong in-process, where no
supervisor exists: it killed the apply thread, left the node silently
applying nothing, and re-raised at teardown, reddening capstones that had
otherwise passed. Related deferred items:
commit-floor anchoring of the wire window, election credential floors for
wiped nodes, and a persisted commit watermark (today the
truncation-below-commit defense forgets everything across a reboot because
the commit counter lives only in the recreated shared page).

## The meta-lesson (again)

This is the third time a UC bug lived precisely in a distinction a model
erased (see `docs/notes/` for the archive-cursor and Lean Finding #12
precedents). The window-vs-full-map gap was even *documented* as a model
simplification. When a proof says "X is simplified away," that line is a
map of where the bugs are.
