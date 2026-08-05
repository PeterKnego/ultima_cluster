# The mute leader

*Why a cluster with wire crypto ON and a config change in flight stops making
progress — and what the honest options are for fixing it. Companion to
[`uc2-two-framings-one-position.md`](uc2-two-framings-one-position.md); that one
is about who may write log bytes, this one is about who may hold a key.*

Status: mechanism **proven** by
`uc2_crypto::group::tests::a_never_acking_peer_mutes_a_fresh_leader_for_the_activation_timeout`.
Whether it fully explains the ~25 %/run failure of `sigkill_mid_config_window`
is **not** established. No fix has been taken.

## The two planes

M8 seals node↔node UDP with two different kinds of key, because the traffic has
two different shapes:

- **Pairwise.** A Noise `IK` session per peer pair. Used for anything addressed
  to one node: `NAK`, `STATUS`, `APPEND_POSITION`, `REQUEST_VOTE`, `VOTE`,
  `TERM_MAP`, snapshot chunks — and the group-key deliveries themselves.
- **Group.** One rotating symmetric key the whole cluster shares, so a fan-out
  is *one* seal and N sends rather than N seals. Used for **`DATA`,
  `HEARTBEAT`, `COMMIT_POSITION`, `READ_PROBE`**.

That list is the whole story of this bug. **Replication and heartbeats are
group-scope.** A leader with no usable group key cannot replicate and cannot
heartbeat. It is mute in the only two ways that keep it leader.

## The activation rule, and why it exists

A leader must not start sealing under a fresh epoch before its peers can open
it — they would drop everything. But it must not wait forever on a dead peer
either. `GroupPlane` resolves that with one line:

```rust
fn is_activated(pending, now_ns) -> bool {
    let all_acked = pending.peers.iter().all(|p| pending.acked.contains(p));
    let timed_out = now_ns.saturating_sub(pending.minted_at) > ACTIVATION_TIMEOUT_NS;
    all_acked || timed_out          // ACTIVATION_TIMEOUT_NS = 2 s
}
```

Both halves are deliberate and the module documents them as "the liveness trap".
The trap is real; it just has one more corner than the rule anticipates.

## The corner

`mint` is called with `gossip_targets()` — **voters plus learners**. M7 can add
a learner at runtime. In `sigkill_mid_config_window` the added learner (`op: 1`,
`AddLearner`) is a node that is never started and whose key was never put in the
crypto allowlist. So:

1. The ghost is named in **every** mint's peer list.
2. Its `HS_KEY` delivery cannot even be sealed — there is no pairwise session
   with it — so it provably never receives the key.
3. Therefore it can never ack, and `all_acked` can never be true.
4. So **every** mint must serve the full 2 s timeout before it activates.

For a leader that already has an activated epoch this is survivable: it keeps
sealing under the old one while the new one waits. The damage is to a **fresh**
leader, which has no `active_epoch` at all — so for two seconds
`sealing_epoch()` answers `None` and every `DATA`, `HEARTBEAT`,
`COMMIT_POSITION` and `READ_PROBE` is dropped `"no usable group key"`.

## Why it sustains itself

Rotation fires on `BecameLeader` — deliberately, so that no leader inherits a
predecessor's epoch. Now put the numbers together:

| | |
| --- | --- |
| new leader is mute for | **2000 ms** |
| follower election timeout | **150–300 ms** |

A fresh leader cannot heartbeat for roughly **ten election timeouts**. Its
followers therefore time out and campaign, one of them wins, mints, and is mute
for 2 s in its turn. The cluster cannot hold a leader long enough to become
useful, and it is the *crypto* plane doing it: the same test with crypto off
completes 205–376 client ops where crypto-on completes 10.

That is also why the mint counter is so lurid — 34–80 mints in a 15 s run,
against 6 for a crypto test that restarts without reconfiguring. **The mints are
a symptom of the churn, and the churn is a symptom of the muteness.** An earlier
draft of this investigation had that arrow backwards and "fixed" the mints; it
changed nothing (10 failures in 40 runs, every signature identical).

## What is NOT true

It is **not** a permanent wedge. A first version of the proving test asserted no
epoch ever activates under churn, and it failed (`Some(3)`, not `None`): a later
mint folds the earlier pending epoch in once *its* 2 s has rolled past, so a
leader does eventually acquire a usable key. The cost is bounded per mint. That
matters for choosing a fix — this is a recurring 2 s tax, not a deadlock.

Also unproven: that the 2 s windows arithmetically account for the whole ~30×
deficit. They are consistent with it. Nobody has done the sum.

## Why neither milestone caught it

M8 measured steady state — every peer reachable, every ack prompt, activation
effectively instant. Its gate reports 94.1 % of cleartext throughput and never
exercised reconfiguration. M7 exercised reconfiguration thoroughly, with crypto
off, where there is no activation rule to trip over. **The defect lives in the
seam between two milestones that were each tested alone**, which is exactly
where the previous two bugs in this codebase came from as well.

## The options

**A. Exclude peers we could not deliver to.** If the `HS_KEY` delivery could not
be sealed — no established pairwise session — that peer provably does not have
the key, and waiting for its ack is waiting for something impossible. Excluding
it from the activation set costs nothing the rule was protecting: confidentiality
is enforced by the delivery being pairwise-sealed to an allowlisted peer, not by
the activation set, and a peer that later establishes a session is picked up by
redelivery (`peers_missing_key`, `d4f7ef5`) and acks then.

This is the minimal principled fix, and it covers an unreachable **voter** (a
crashed node) as well as an unreachable learner. It leaves the 2 s wait intact
for its real purpose: a peer that *has* the key but has not answered yet.

**B. Exclude learners.** Simpler predicate — learners are non-quorum by
definition and arguably should never gate the voting plane. But it is a strict
subset of the problem: a crashed *voter* still costs every new leader 2 s.

**C. Let a fresh leader seal under the epoch it already holds.** A node that was
a follower under the previous leader has that leader's activated epoch, and so
does everyone else. Continuing to seal under it until the new mint activates
removes the mute window entirely, for every cause. This is the most complete
fix and the most invasive: it partially relaxes "a fresh leader always rotates
before it speaks", which exists so that leadership changes do not extend a
compromised epoch's reach. The window is bounded by the activation timeout, and
the epoch in question is one the cluster was already using — but that is a
security argument to be made deliberately, not in passing.

**D. Shorten the timeout.** Reduces the tax, fixes nothing. A 200 ms timeout
still exceeds an election timeout's lower bound.

**Recommendation: A, and consider C separately.** A removes provably futile
waiting and is defensible in one sentence. C removes the mute window even when
the wait is legitimate, and deserves its own decision with the threat model in
front of you.

## Reproducing

```bash
UC2_CRYPTO=1 cargo test -p uc2-crashtest --features hard-crash-tests \
    sigkill_mid_config_window
```

~25 % per run, so **fix your sample size before you read a result as a verdict**.
This investigation produced three separate wrong "fixed" calls from streaks of
20–30 runs; each one was caught by a pre-committed n, and none of them would
have been caught by looking at the last few runs and feeling encouraged.
