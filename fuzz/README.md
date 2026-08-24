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
scripts/fuzz_smoke.sh                                  # 30 s per target, every target
scripts/fuzz_smoke.sh 60 uc_protocol_datagram
scripts/fuzz_smoke.sh --min-runs 10000 600 uc2_node_toml
```

Runs each target for `SECS` seconds against the committed corpus and exits 1
on the first crash, naming the artifact directory. With no target arguments it
enumerates targets with `cargo fuzz list` and **skips the `seed-corpus`
generator bin**, which is a `[[bin]]` in this manifest but not a fuzz target.

`--min-runs N` makes the script assert, after each target, that libFuzzer's
`Done <N> runs` line reports at least `N` executions. This is not decoration:
a target can build, run for its full budget, print a clean line and exit 0
having executed about sixteen inputs. That is exactly what an `llvm-symbolizer`
stall did to four of these targets before `-print_funcs=0` was added, and the
smoke run reported "all targets clean" throughout. Without a floor, green does
not mean *fuzzed*. CI passes `--min-runs 10000` against a 600 s budget — three
to five orders of magnitude below what a healthy target does, so it catches a
stall without flaking on a slow runner.

Note that if your environment sets a global `build.target-dir` (this dev box
does, via `~/.cargo/config.toml`), sanitizer builds land there and share the
cache; otherwise they land in `fuzz/target/`. When it is set, the
sanitizer-instrumented fuzz builds land in that shared cache next to the
ordinary workspace artifacts. They are a separate target triple subdirectory,
so they do not collide with or invalidate the normal `cargo build` cache —
but they are not cleaned by removing `fuzz/target/` either. This is the CI-shaped
invocation: short, deterministic-ish, and green means "no new crash from this
corpus in that budget" — it is a regression gate, not a bug hunt. Real hunting
is a long `cargo fuzz run` with a large `-max_total_time`.

## How CI runs this

`nightly.yml` has two jobs (`.github/workflows/nightly.yml`):

* **`fuzz-groups`** — the four matrix legs are declared once, in that job's
  `FUZZ_GROUPS` env, and a ~10-second Python step asserts their union is
  **exactly** the set of `[[bin]]` targets in `fuzz/Cargo.toml` (minus the
  `seed-corpus` generator), with no target in two legs and no leg naming a
  target that no longer exists. Adding a fifteenth target without assigning it
  to a group fails the workflow here, before any sanitizer build. This is the
  mechanism that keeps "every target is fuzzed nightly" a fact rather than an
  intention — so when you add a target (see below), add it to a group.
* **`fuzz`** — four parallel legs, `600` seconds per target on the committed
  corpus, `--min-runs 10000`, `fail-fast: false`. A crash fails the leg and
  uploads `fuzz/artifacts` as a workflow artifact. A second cheap step
  re-checks the declared list against what `cargo fuzz list` actually
  enumerates, so the manifest parse and cargo-fuzz can never quietly diverge.

Both run on the `nightly` toolchain via `dtolnay/rust-toolchain@master`; the
workspace's own jobs stay on the pinned stable. The build cache is
`Swatinem/rust-cache@v2` with `workspaces: fuzz` — a cold sanitizer build is
two to three minutes. (The global `CARGO_TARGET_DIR` caveat below is a
property of this dev box, not of CI: runners have no such setting, so the
fuzz builds land under `fuzz/target/` there and the cache action finds them.)

Budget arithmetic, for whoever adds target fifteen: 600 s per target against a
60-minute job timeout means **four targets per leg is the ceiling**, minus the
build. Add a fifth leg rather than a fifth target.

## Regenerating the seed corpus

Seeds are built from fixed literals with the real encoders — no clock, no
randomness — so the generator is deterministic and idempotent:

```bash
cd fuzz && cargo +nightly run --bin seed-corpus
```

Add or change a seed in `src/seeds.rs`, re-run, and commit the corpus files.

Two seeds are the exception, marked `Regen::IfAbsent` in `src/seeds.rs`: a
genuine Noise `IK` message 1 and a genuinely minted group key. Both come from
real code paths that draw from the OS RNG, so their bytes cannot be
reproduced — having them is worth more than regenerating them, so the
generator writes those files only when they are absent and otherwise leaves
the committed bytes alone. Delete the file and re-run to capture a fresh one.

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
| `uc2_remote_frame` | `uc2_remote::frame` — the gateway edge's 24-byte TCP frame header and every typed body decoder. |
| `uc2_crypto_open` | `uc2_crypto::seal::{open_in_place, open_detached}` — the AEAD envelope's framing arithmetic on attacker-chosen bytes. |
| `uc2_crypto_handshake` | `uc2_crypto::handshake::Peers::on_message` — the pre-auth Noise `IK` surface, the first thing in the process to see bytes from anyone who can reach the UDP port. |
| `uc2_crypto_group_key` | `uc2_crypto::group::GroupPlane::on_key_message` — the two message shapes that share datagram kind 20. |
| `uc2_crypto_admin` | `uc2_crypto::admin` — a PROPERTY target over the M12b signed-tag layout (canonical length, sign/verify, tag bit-flip, foreign key). |
| `ultima_journal_record` | `ultima_journal`'s segment header and record decoder — what crash recovery meets in a torn or corrupt segment. |
| `ultima_journal_stable_value` | `ultima_journal::stable_value` — the durable vote / term map / snapshot floor slots. |
| `uc_protocol_cnc` | `uc_protocol::v2::cnc` — the 4 KiB control page every attaching process maps and parses. |
| `uc_protocol_log_frame` | `uc_protocol::v2::frame::read_header` behind the real caller's `len >= HEADER_LEN` guard (that reader is deliberately caller-guarded — see its doc). |
| `uc2_service_session` | `Sessioned<S>` — the exactly-once envelope (under a fuzz-derived, deliberately tiny `SessionConfig`, so client/byte eviction and the window trim are reachable) and its snapshot install path. |
| `uc2_node_toml` | `uc2_node::config_file::parse_str` — the `node.toml` parser behind every M9/M11/M12b named startup refusal. |
| `uc2_gateway_toml` | `uc2_gateway::config_file::parse_str` — the gateway's whole named-refusal path (it runs `EdgeConfig::validate` itself). |
| `uc2_node_http` | `uc2_node::obs::http::route_raw` — the unauthenticated `/metrics` + `/healthz` + `/readyz` request parser. |

## `cfg(fuzzing)` seams

`uc2_node_http` drives `obs::http::route_raw` and `ObsSources::for_tests`,
which exist only under `#[cfg(any(test, fuzzing))]` — they are a test seam,
not API, and are absent from a shipped build. `cargo fuzz` sets `--cfg
fuzzing` across the whole dependency graph, so the target builds under `cargo
+nightly fuzz build/run` but **not** under a plain `cargo +nightly build` in
this directory, which will fail to resolve `route_raw`. That is expected;
`cargo fuzz` is the entry point. `cargo +nightly run --bin seed-corpus` is
unaffected (it builds only the generator).

`uc2_node/Cargo.toml` declares `unexpected_cfgs = { check-cfg =
['cfg(fuzzing)'] }` so the workspace's `clippy -D warnings` stays clean
without promoting the seam to a Cargo feature (which would make it API).

## Adding a target

1. Write `fuzz_targets/<name>.rs` (`#![no_main]` + `fuzz_target!`).
2. Append a `[[bin]]` block for it to `fuzz/Cargo.toml` (`test`/`doc`/`bench = false`).
3. Add a `seeds::<name>()` function and a `write_target` call in
   `src/bin/seed_corpus.rs`; run the generator and commit the corpus.
4. **Assign it to a matrix group** in `nightly.yml`'s `fuzz-groups` job. The
   workflow fails until you do — deliberately.
5. Add a row to the target table above and a line to `docs/VERIFICATION.md`'s
   fuzzing section.
6. Shared helpers (`uc2_fuzz::split`, `uc2_fuzz::NoopSm`) live in `src/lib.rs`.
