<!-- SPDX-License-Identifier: Apache-2.0 -->
# `uc2-fuzz` — coverage-guided fuzzing for ultima_cluster

Structure-unaware [libFuzzer](https://llvm.org/docs/LibFuzzer.html) targets,
driven by [`cargo-fuzz`](https://rust-fuzz.github.io/book/cargo-fuzz.html),
over every place ultima_cluster parses bytes it did not write: the pre-auth
UDP datagram path, the remote/gateway framing, and the on-disk artifacts.

**Nothing here is shipped.** `publish = false`, `version = 0.0.0`.

## Why this crate is outside the workspace

The root `Cargo.toml` carries `exclude = ["fuzz"]` and this crate declares its
own empty `[workspace]` table. Two reasons, both hard requirements:

* `libfuzzer-sys` needs `-Z sanitizer=address`, i.e. **nightly**. The
  workspace pins stable (`rust-toolchain.toml`, currently 1.96.0) with an MSRV
  floor of 1.89 (CI's `msrv` job). A nightly-only member would break both.
* `cargo build --workspace`, `cargo test`, `cargo clippy --workspace` and the
  `--locked` CI jobs must never see these dependencies or this lockfile.

The workspace's own package count is unchanged by this directory — a useful
one-liner to confirm that:

```bash
cargo metadata --no-deps --format-version 1 | python3 -c "import json,sys; print(len(json.load(sys.stdin)['packages']))"
```

## Prerequisites

```bash
rustup toolchain install nightly
cargo install cargo-fuzz          # 0.13.2 or newer
```

## Running one target

```bash
cd fuzz
cargo +nightly fuzz list                       # the available targets
cargo +nightly fuzz run uc_protocol_datagram -- -max_total_time=60
```

`cargo fuzz run` seeds itself from `fuzz/corpus/<target>/` (committed) and
writes new inputs back into it; it drops any crashing input into
`fuzz/artifacts/<target>/`. Both `target/` and `artifacts/` are gitignored —
a crash artifact is evidence for a bug report, not repo content, so copy it
somewhere and attach it to the issue.

## The smoke run

```bash
scripts/fuzz_smoke.sh              # 30 s per target, every target
scripts/fuzz_smoke.sh 60 uc_protocol_datagram
```

Runs each target for `SECS` seconds against the committed corpus and exits 1
on the first crash, naming the artifact directory. This is the CI-shaped
invocation: short, deterministic-ish, and green means "no new crash from this
corpus in that budget" — it is a regression gate, not a bug hunt. Real hunting
is a long `cargo fuzz run` with a large `-max_total_time`.

## Regenerating the seed corpus

Seeds are built from fixed literals with the real `uc_protocol` encoders — no
clock, no randomness — so the generator is deterministic and idempotent:

```bash
cd fuzz && cargo +nightly run --bin seed-corpus
```

Add or change a seed in `src/seeds.rs`, re-run, and commit the corpus files.

`cargo fuzz run` also writes its own coverage-expanding discoveries into the
same directory (hash-named files). Those are a local working corpus, not repo
content: **the committed corpus is exactly the generator's output**, so a
`git status` after a fuzz run shows them as untracked and you either `cmin`
and commit them deliberately, or delete them. Keeping the committed set
generator-shaped is what makes a corpus change reviewable in a diff.

## Minimising

```bash
cd fuzz
cargo +nightly fuzz tmin uc_protocol_datagram artifacts/uc_protocol_datagram/crash-<hash>
cargo +nightly fuzz cmin uc_protocol_datagram    # shrink the corpus, same coverage
```

`tmin` shrinks a single crashing input to the smallest one that still
reproduces (put THAT in a regression test); `cmin` prunes the corpus down to a
minimal set preserving coverage — worth running before committing corpus
growth.

## Targets

| Target | What it parses |
| --- | --- |
| `uc_protocol_datagram` | `uc_protocol::v2::datagram` — the 16-byte header plus every body reader, i.e. the first code an unauthenticated UDP packet reaches. |

## Adding a target

1. Write `fuzz_targets/<name>.rs` (`#![no_main]` + `fuzz_target!`).
2. Append a `[[bin]]` block for it to `fuzz/Cargo.toml` (`test`/`doc`/`bench = false`).
3. Add a `seeds::<name>()` function and a `write_target` call in
   `src/bin/seed_corpus.rs`; run the generator and commit the corpus.
4. Shared helpers (`uc2_fuzz::split`, `uc2_fuzz::NoopSm`) live in `src/lib.rs`.
