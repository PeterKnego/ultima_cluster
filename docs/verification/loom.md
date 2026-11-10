# Memory model: loom

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

Loom is a tool that runs a small concurrent Rust program under every possible thread interleaving. It also explores the reorderings a weak memory model allows. This tier uses it as an exhaustive interleaving check of UC's lock-free handshakes. Each model is a small test file that drives one protocol, and a green run means no interleaving breaks it. The four models live in `uc_log/tests/` and `uc_protocol/tests/`.

## The four models

Each model runs with `RUSTFLAGS="--cfg loom"`, which swaps the real atomics for loom's instrumented ones.

```bash
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_broadcast --release
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_reserve --release
```

| model | test | crate |
|---|---|---|
| Frame visibility | `loom_frame` | `uc_log` |
| MPSC per-record commit | `loom_mpsc` | `uc_protocol` |
| Broadcast seqlock read barrier | `loom_broadcast` | `uc_protocol` |
| Follower write-reserve seqlock | `loom_reserve` | `uc_log` |

A note on ordering terms. A `Release` store makes every write the thread did before it visible to any thread that reads the stored value with an `Acquire` load. It says nothing about writes the thread does after the store.

## Frame visibility

A torn frame is one whose bytes come partly from an old write and partly from a new one. The first model checks the frame-visibility protocol. This is the atomic handshake by which a reader observes a fully-written frame and never a torn one.

## MPSC per-record commit

The second model is the MPSC ring's per-record commit protocol, from M13a. MPSC means many producers and one consumer. The model covers three things:

- disjoint concurrent claims, so two producers never claim the same bytes
- the commit word's Release/Acquire visibility pair
- head-of-line behaviour at a claimed slot

## Broadcast seqlock read barrier

The third model is the **Broadcast ring's seqlock read barrier**, written 2026-08-31. **Writing this model found a real defect, which is the reason it exists.**

### Why Broadcast needs a re-check

Broadcast is the only ring with no backpressure. Its single producer may lap a reader while the reader is mid-copy. So a read's validity rests entirely on one step. After the copy, the reader re-reads `publish_position`. If the producer has moved past the record, the reader throws its copy away. So a Broadcast reader rejects a record the producer overwrote mid-copy. A read that copies and then re-checks a position this way is called a seqlock.

### Why the Release store did not suffice

The reader's post-copy re-check of `publish_position` needs one guarantee. If lap N+1's bytes are visible, then `publish_position >= N+1` must be visible too. The producer's `Release` store did not provide it. Release orders the accesses before the store, not the ones after it. So the next record's body writes could be observed ahead of the publish that would have warned the reader.

### What loom showed

Without the fix, loom's counterexample is a consumer accepting a torn record. The record's first word is from lap 0 and its second word is from lap 2. That torn read reaches the crc. The code's own comment says a torn read cannot reach it.

### The fix and what it costs

The fix is one publish-before-body `Release` fence in `BroadcastProducer::write`. TSO is x86's strong memory model. The fence costs **no instruction on x86_64**, because TSO already forbids the reordering. On aarch64 it costs one `dmb ish`. So the fence costs something exactly where it is needed.

### Why no test could have caught it

The Broadcast defect shows only under a weak memory model. UC builds aarch64 binaries that CI never executes. So the defect was unreachable by any test the project runs. Only a model could reach it.

### The ARM hardware run weakens nothing

On 2026-08-31, after the fix shipped, the full test stack ran once on real aarch64 hardware. The record is [`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md). That run weakens nothing here, for two reasons. First, it executed the fixed code. Second, a weak-memory race is not reliably reachable by running tests on weak hardware anyway. **The model remains the coverage. The hardware run is a smoke check.**

### The model has teeth

A mutation is a bug injected on purpose. The Broadcast model carries two, and both are `#[should_panic]` tests:

- `m1_without_the_producer_publish_before_body_fence`
- `m2_without_the_post_copy_revalidation`

Each mutation must fail under loom. So a green run means the model has teeth. It does not mean the model explored nothing.

The plain-language writeup is [`docs/notes/uc2-broadcast-seqlock-explained.md`](/docs/notes/uc2-broadcast-seqlock-explained.md).

## Follower write-reserve seqlock

The fourth model is the **follower write-reserve seqlock**, from #78. It lives in `uc_log/tests/loom_reserve.rs`. The protocol has two sides:

- The receiver raises `reserve` and fences before it overwrites a lap.
- A validated reader re-checks `reserve` after its copy.

So a follower reader whose copy saw any byte of the next lap refuses it. The model's mutation, `without_the_writer_fence_the_model_fails`, is `#[should_panic]`.

## What loom does not cover

All four models are of protocols, not of memory mappings. Loom cannot see an mmap. So the ring layout stays frozen by offset-pin tests instead. Offset-pin tests also freeze the wire and `cnc` page layouts. So a layout change cannot pass silently.
