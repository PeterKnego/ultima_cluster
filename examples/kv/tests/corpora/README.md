# Regression corpora

A corpus is a `uc2ctl backup`-shaped directory plus `CORPUS` (row, origin,
end, version) and `intent.toml` (the diff-replay declaration). Each one is a
recorded input that once mattered — a bug's trigger, a migration's shape —
kept so every future build is replayed over it (spec §5.9, §6.5.1).

`regression_corpora.rs` runs every corpus here in `determinism` mode on each
`cargo test`. To pin a behaviour across versions, add `baseline.json` (a trace
from the version you trust) and the test will also run `upgrade` mode against
it with `intent.toml`.

Regenerate a corpus with `cargo test --test gen_corpus -- --ignored` after a
wire or image format change; a corpus without its `intent.toml` is a recording,
not a test. The snapshot ENVELOPE counts as such a change: the artifact in a
corpus carries it, and a build whose envelope has moved on refuses the old one
by name rather than replaying it (an `ULTSNAP1` artifact under a 2.13.0
binary — "pre-2.13.0 artifact (ULTSNAP1): carries no version stamp"). That is
the regression gate working, not a corpus to patch by hand.

## What is here

`put-then-delete/` — two puts (`a=1`, `b=2`) below the origin, one
`delete a` above it, generated from a real in-process node running the real
`Sessioned<KvSm>` service (`tests/gen_corpus.rs`). Origin 192; the artifact
at 192 holds both keys, and replaying the span removes `a`. 42 KiB.

Its `intent.toml` carries `tag_offset = 16`: the live service is
`Sessioned<KvSm>`, so a command payload is `client_id ‖ seq` (16 bytes) and
only then the KV frame, whose first two bytes — `FORMAT_VERSION ‖ op` — are
what `[tags]` names.

**That declaration is written for `determinism`, not for an upgrade.** Its
`[touched] arms` is empty and it carries no `[[expect]]`, which is exactly
right for the gate that runs on every `cargo test` — under one build nothing
may differ, so nothing is declared. It is **not** a usable declaration for a
real v1 → v2 `upgrade` run on this corpus: with no touched arm and no
expectation, every divergence the new build produces comes back
`Unexplained` and the run fails. Write the declaration with the change —
name the arms it touched, set `migration = true` if the image format moved,
and add one `[[expect]]` per `(surface, arm)` pair — rather than reaching for
this file and wondering why the report is a wall of red.

## Running them by hand

    cargo build -p uc_diffreplay -p kv_store
    T=target/debug
    $T/uc2-diffreplay determinism --corpus examples/kv/tests/corpora/put-then-delete \
        --bin $T/kv-service --report /tmp/r.json

`determinism` replays the corpus twice through the same binary and requires
an empty profile. `upgrade --old A --new B --declare intent.toml` is the same
machinery across two builds, judged against the declaration.
