# `uc_starter` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `PeterKnego/uc_starter`, a `cargo generate` template that produces a complete, working ultima_cluster application (FSM, service, remote client, tests, scripts, docs), plus an AI tutor (`WHAT-NEXT.md` + `scripts/next.sh` + an agent kit) that walks a developer from the skeleton to their own app in production.

**Architecture:** The template repo root is the template; liquid substitution touches only four files (`Cargo.toml`, `uc-app.env`, `src/identity.rs`, `README.md`) so Rust, shell and YAML stay literal. One Linux loop (Makefile + bash, adapted from `examples/kv/scripts/kvcluster.sh`) runs natively or inside a devcontainer. The tutor's position is computed from observable repo state (TODO markers, stamps with source hashes, `.uc-progress`), never remembered.

**Tech Stack:** Rust (edition 2024, MSRV 1.89, toolchain pinned 1.96.0), crates.io `uc_service`/`uc_remote`/`uc_protocol`/`uc_diffreplay` `=2.13.0`, serde + bincode 2 (standard config — the typed tier's codec), clap 4, proptest; bash; GNU make; cargo-generate ≥ 0.21 (rhai hooks); GitHub Actions; devcontainers.

**Spec:** `docs/superpowers/specs/2026-09-24-uc-starter-design.md` (in the `ultima_cluster` repo). Read it before any task.

## Global Constraints

- UC version `2.13.0` everywhere: crate pins are exact (`=2.13.0`), `UC_VERSION` file holds `2.13.0`, doc links pin `blob/v2.13.0/`.
- Nodes are Linux/Android only; any script that needs a node refuses a non-Linux host **by name**, pointing at the devcontainer.
- Cluster state never under `/tmp` or `/dev/shm` (RAM-backed; nodes refuse it). Default root `$HOME/.uc-starter/$APP_NAME`.
- Start order is **all nodes → wait for a serving leader → all services → all gateways**; stop order is services and gateways, then nodes.
- FSM `NAME`: 1–32 bytes of `[a-z0-9_-]`, first byte a lowercase letter (`uc_protocol::identity::FsmName::parse`), and never the reserved `uc_` prefix.
- `apply` is deterministic: no `SystemTime::now`/`Instant::now`/RNG/`HashMap`/`HashSet`/floats/I/O in FSM files (`src/commands.rs`, `src/state.rs`, `src/snapshot.rs`).
- One command must fit one datagram: payload ceiling 1344 B (crypto off) / 1312 B (crypto on) at the baseline rung; the skeleton caps key ≤ 128 B, value ≤ 1024 B.
- Every `TODO(app)` marker is a **whole-line comment** (`// TODO(app): …` or `# TODO(app): …` or `<!-- TODO(app): … -->`), so deleting marker lines never breaks code.
- `uc2ctl upgrade pin` is never run without explicit human confirmation (`UC_CONFIRM_PIN=yes` or an interactive `PIN` prompt).
- Scratch and generated test projects go under `$HOME/scratch/`, never `/tmp`.
- `uc_starter` stays **private** until the §9 clean-room walkthrough passes.
- Before every push of either repo that touched Rust: `cargo +1.89.0 clippy --all-targets --locked -- -D warnings` with a private target dir.

## Review Focus

1. **Oversize command** — a `put` whose key+value exceed the payload ceiling: expect the client to refuse it before submitting, naming the limit, exit 2 (test in Task 2: `validate_refuses_oversize`; client path in Task 3).
2. **Snapshot artifact from a different image version or a mis-tagged cursor** — expect `install_snapshot` to refuse by name and leave state untouched (Task 2: `install_refuses_unknown_image_version`, `install_refuses_cursor_at_or_above_instant`).
3. **Code edited after a stamp** — expect `make next` to report the step **stale**, not done; and after Part 1 completes, Part-2 code edits must not send the user back into Part 1 (Task 6: fixture steps 9 and 11).
4. **Port collision with a second generated app or a dev cluster** — expect `make up` to refuse by name, and the cluster test to use an offset so it never collides with the developer's running cluster (Task 5: `UC_PORT_OFFSET` in `tests/cluster.rs`; `cluster.sh` port check).
5. **Running on macOS/Windows or with a missing/mismatched binary** — expect a named refusal pointing at the devcontainer or `make bins`, never a raw exec error (Task 5: `fetch-uc.sh` host check; Task 6: `check_env` fixture).

---

## File Structure (the generated project; the template repo is the same tree plus `cargo-generate.toml`, `hooks/`, `template-tests/`, `.github/workflows/template-ci.yml`)

| path | responsibility |
|---|---|
| `cargo-generate.toml`, `hooks/validate.rhai` | placeholders + validation (template only) |
| `UC_VERSION`, `uc-app.env` | the single UC pin; app identity for scripts |
| `Cargo.toml`, `rust-toolchain.toml`, `clippy.toml`, `.gitignore` | build + determinism bans |
| `src/identity.rs` | `FSM_NAME`, `FSM_VERSION`, `APP_ID`, `BASE_PORT` — the only liquid-processed Rust file |
| `src/commands.rs` | wire types + `Command::validate` |
| `src/state.rs` | `State`, `Fsm`, `impl StateMachine` |
| `src/snapshot.rs` | `impl SnapshotStateMachine` (image v1, projection) |
| `src/lib.rs` | module map + re-exports |
| `src/bin/service.rs` | `<name>-service`: run / replay / project |
| `src/bin/client.rs` | `<name>`: remote CLI |
| `tests/{state,snapshot,determinism,cluster}.rs` | unit, image, proptest, cluster smoke |
| `scripts/lib.sh` | shared helpers: paths, hashes, stamps, progress |
| `scripts/{fetch-uc,cluster,demo,kill-leader,lint-determinism,next,stamp,progress}.sh` | Part 1 tooling |
| `scripts/{snapshot-drill,observe,corpus,upgrade-check,upgrade-drill,package,uc-upgrade}.sh` | Part 2 tooling |
| `Makefile` | the one entry point |
| `upgrade/intent.toml.example` | diff-replay declaration starter |
| `WHAT-NEXT.md`, `README.md`, `docs/**` | tutor path + docs |
| `AGENTS.md`, `CLAUDE.md`, `.claude/**` | AI kit |
| `.devcontainer/**`, `compose.yml`, `Dockerfile` | container paths |
| `.github/workflows/ci.yml` | generated project CI |

---

### Task 1: Repository bootstrap and the generator

**Files:**
- Create (in a new repo `~/ultima/uc_starter`): `cargo-generate.toml`, `hooks/validate.rhai`, `UC_VERSION`, `uc-app.env`, `Cargo.toml`, `rust-toolchain.toml`, `.gitignore`, `LICENSE`, `README.md` (stub), `src/identity.rs`, `src/lib.rs` (stub), `template-tests/gen.sh`, `template-tests/generator.sh`

**Interfaces:**
- Produces: `template-tests/gen.sh <dest-dir> [name] [fsm_name] [app_id] [base_port]` — generates a project; every later task tests through it. Defaults: `demo-app demo demo 7000`. Library crate is always named `app`.

- [ ] **Step 1: Confirm with the user, then create the repo.** This is the first outward-facing action; ask "create `PeterKnego/uc_starter` (private) now?" and wait for yes. Then:

```bash
gh repo create PeterKnego/uc_starter --private --description "Quick-start template for ultima_cluster applications, with an AI tutor"
git clone git@github.com:PeterKnego/uc_starter.git ~/ultima/uc_starter
gh repo edit PeterKnego/uc_starter --template   # spec §3: also a GitHub template
cargo install cargo-generate --locked   # if `cargo generate --version` fails
```

- [ ] **Step 2: Write the failing generator test** `template-tests/generator.sh`:

```bash
#!/usr/bin/env bash
# Generator contract: placeholders substituted in the four liquid files only,
# everything else verbatim, bad names refused by the pre-hook.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HOME/scratch/uc_starter-gen/generator"
rm -rf "$OUT"; mkdir -p "$OUT"
fail() { echo "FAIL: $*" >&2; exit 1; }

"$HERE/gen.sh" "$OUT/ok" my-app myfsm myid 7100 >/dev/null
P="$OUT/ok/my-app"
grep -q 'pub const FSM_NAME: &str = "myfsm";' "$P/src/identity.rs" || fail "fsm_name not substituted"
grep -q 'pub const APP_ID: &str = "myid";' "$P/src/identity.rs" || fail "app_id not substituted"
grep -q 'pub const BASE_PORT: u16 = 7100;' "$P/src/identity.rs" || fail "base_port not substituted"
grep -q '^APP_NAME=my-app$' "$P/uc-app.env" || fail "uc-app.env"
grep -q '^name = "my-app-service"$' "$P/Cargo.toml" || fail "service bin name"
grep -q 'LITERAL-CHECK {{not_a_placeholder}}' "$P/src/lib.rs" || fail "src/lib.rs was liquid-processed"
[ ! -e "$P/template-tests" ] || fail "template-tests leaked into the project"
[ ! -e "$P/hooks" ] || fail "hooks leaked into the project"
[ ! -e "$P/cargo-generate.toml" ] || fail "cargo-generate.toml leaked"
(cd "$P" && cargo metadata --format-version 1 --no-deps >/dev/null) || fail "generated Cargo.toml does not parse"

for bad in uc_mine 9lives Upper; do
  if "$HERE/gen.sh" "$OUT/bad-$bad" bad-app "$bad" x 7000 >/dev/null 2>&1; then fail "fsm_name $bad accepted"; fi
done
if "$HERE/gen.sh" "$OUT/bad-app" app ok x 7000 >/dev/null 2>&1; then fail "project name app accepted"; fi
if "$HERE/gen.sh" "$OUT/bad-port" p2 ok x 65400 >/dev/null 2>&1; then fail "base_port 65400 accepted"; fi
echo "generator: PASS"
```

`template-tests/gen.sh`:

```bash
#!/usr/bin/env bash
# gen.sh DEST [name fsm_name app_id base_port] — generate a project from this checkout.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$1"; NAME="${2:-demo-app}"; FSM="${3:-demo}"; APPID="${4:-demo}"; PORT="${5:-7000}"
mkdir -p "$DEST"
cargo generate --path "$ROOT" --name "$NAME" --destination "$DEST" --silent \
  --define fsm_name="$FSM" --define app_id="$APPID" --define base_port="$PORT"
```

- [ ] **Step 3: Run it to verify it fails** — `bash template-tests/generator.sh`. Expected: FAIL (no `cargo-generate.toml` yet).

- [ ] **Step 4: Write the generator files.**

`cargo-generate.toml`:

```toml
[template]
cargo_generate_version = ">=0.21.0"
# Only these files are liquid-processed; everything else is copied verbatim,
# so Rust format strings, shell ${…} and GitHub ${{ … }} stay literal.
include = ["Cargo.toml", "uc-app.env", "src/identity.rs", "README.md"]
ignore = [".github/workflows/template-ci.yml", "template-tests", "hooks", "target", ".uc"]

[hooks]
pre = ["hooks/validate.rhai"]

[placeholders.fsm_name]
type = "string"
prompt = "State machine NAME (1-32 of a-z 0-9 _ -, starts with a letter, not uc_…)"
regex = "^[a-z][a-z0-9_-]{0,31}$"

[placeholders.app_id]
type = "string"
prompt = "app_id — the cluster identity every process checks (a-z 0-9 _ -)"
regex = "^[a-z0-9_-]+$"

[placeholders.base_port]
type = "string"
prompt = "Base port: nodes UDP base..+2, gateways TCP base+100..+102, metrics base+200..+202"
regex = "^[1-9][0-9]{3,4}$"
default = "7000"
```

`hooks/validate.rhai`:

```rhai
let fsm = variable::get("fsm_name");
if fsm.starts_with("uc_") {
    abort("fsm_name must not start with `uc_`: that prefix is reserved for ultima_cluster's own state machines");
}
let project = variable::get("project-name");
if project == "app" {
    abort("project name `app` collides with the library crate `app`; pick another name");
}
let port = parse_int(variable::get("base_port"));
if port + 202 > 65535 {
    abort("base_port too high: the metrics band ends at base_port + 202, which must be <= 65535");
}
```

`UC_VERSION`: `2.13.0` (one line).

`uc-app.env`:

```
APP_NAME={{project-name}}
APP_ID={{app_id}}
FSM_NAME={{fsm_name}}
BASE_PORT={{base_port}}
```

`Cargo.toml`:

```toml
[package]
name = "{{project-name}}"
version = "0.1.0"
edition = "2024"
rust-version = "1.89"
publish = false

# The library is always `app`, so no Rust source needs template syntax.
[lib]
name = "app"
path = "src/lib.rs"

[[bin]]
name = "{{project-name}}-service"
path = "src/bin/service.rs"

[[bin]]
name = "{{project-name}}"
path = "src/bin/client.rs"

[features]
# The three-node cluster smoke spawns real processes; opt in.
cluster-tests = []

# Exact pins: every ultima_cluster minor so far has been a wire flag day.
# Move them only with `make uc-upgrade VERSION=x`.
[dependencies]
uc_service = "=2.13.0"
uc_remote = "=2.13.0"
uc_protocol = "=2.13.0"
# default-features = false: the service embeds the replay DRIVER only and must
# not link the consensus crate.
uc_diffreplay = { version = "=2.13.0", default-features = false }
serde = { version = "1", features = ["derive"] }
bincode = { version = "2", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
anyhow = "1"
signal-hook = "0.3"

[dev-dependencies]
proptest = "1"
```

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.96.0"
components = ["rustfmt", "clippy"]
```

`.gitignore`:

```
/target
/.uc/
/dist/
/upgrade/corpus/
/upgrade/old/
/upgrade/report.json
```

`src/identity.rs`:

```rust
//! Who this application is. Generated once by `cargo generate`; the only Rust
//! file with template substitution.

/// The state machine's identity (`StateMachine::NAME`). Changing it after a
/// cluster has run makes every node refuse this service by name.
pub const FSM_NAME: &str = "{{fsm_name}}";

/// The semantic version of what `apply` does. Bump it on ANY behaviour
/// change and run `make upgrade-check` (WHAT-NEXT.md, Step 12).
pub const FSM_VERSION: u32 = uc_protocol::identity::pack_version(1, 0, 0);

/// The cluster identity every process checks at attach.
pub const APP_ID: &str = "{{app_id}}";

/// Nodes UDP `BASE_PORT..+2`, gateways TCP `+100..+102`, metrics `+200..+202`.
pub const BASE_PORT: u16 = {{base_port}};

/// The local cluster's gateways, for the client's default `--gateways`.
pub fn local_gateways(offset: u16) -> Vec<String> {
    (0..3)
        .map(|i| format!("127.0.0.1:{}", BASE_PORT + offset + 100 + i))
        .collect()
}
```

`src/lib.rs` (stub; Task 2 replaces it but keeps the literal-check line):

```rust
//! LITERAL-CHECK {{not_a_placeholder}} — this line proves the generator copies
//! Rust sources verbatim (template-tests/generator.sh). Leave it.
pub mod identity;
```

`LICENSE`: Apache-2.0 text (copy from `~/ultima/ultima_cluster/LICENSE`). `README.md`: `# {{project-name}}` plus one line "Generated from uc_starter — full README in Task 9."

- [ ] **Step 5: Run the test to verify it passes** — `bash template-tests/generator.sh`. Expected: `generator: PASS`. If `project-name` is not visible in pre-hooks, or `include` does not behave as "only these are processed", stop and adjust (check `cargo generate --help` and the cargo-generate book via context7) — do not weaken the test.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: cargo-generate template skeleton with validated placeholders"
```

---

### Task 2: The state machine (commands, state, snapshot) and its tests

**Files:**
- Create: `src/commands.rs`, `src/state.rs`, `src/snapshot.rs`, `tests/state.rs`, `tests/snapshot.rs`, `tests/determinism.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `app::identity::{FSM_NAME, FSM_VERSION}`.
- Produces: `app::{Command, Response, Query, QueryResponse, Fsm, State, MAX_KEY_LEN, MAX_VALUE_LEN, CommandError}`; `Command::validate(&self) -> Result<(), CommandError>`; `Fsm::state(&self) -> &State`; `impl StateMachine for Fsm`; `impl SnapshotStateMachine for Fsm` with `SnapshotHandle = Frozen`; `pub fn encode<T: Serialize>(&T) -> Vec<u8>` and `pub fn decode<T: DeserializeOwned>(&[u8]) -> Result<T, String>` in `commands.rs` (bincode standard — the typed tier's codec).

All work in this task happens in the template repo; run tests in a generated project: `template-tests/gen.sh ~/scratch/uc_starter-gen/t2 && cd ~/scratch/uc_starter-gen/t2/demo-app`. Re-generate after every template edit (or edit the generated copy, then copy back — never leave the two diverged at commit time).

- [ ] **Step 1: Write the failing tests.**

`tests/state.rs`:

```rust
use app::{Command, Fsm, Query, QueryResponse, Response};
use uc_service::{ApplyCtx, StateMachine};

fn ctx(p: u64) -> ApplyCtx {
    ApplyCtx::for_sm::<Fsm>(p)
}

fn put(k: &str, v: &str) -> Command {
    Command::Put { key: k.into(), value: v.into() }
}

#[test]
fn put_then_get() {
    let mut sm = Fsm::default();
    let r = sm.apply(&mut ctx(32), put("a", "1"));
    assert_eq!(r, Response::Put { previous: None });
    assert_eq!(sm.query(Query::Get { key: "a".into() }), QueryResponse::Value(Some("1".into())));
}

#[test]
fn put_overwrites_and_returns_previous() {
    let mut sm = Fsm::default();
    sm.apply(&mut ctx(32), put("a", "1"));
    let r = sm.apply(&mut ctx(64), put("a", "2"));
    assert_eq!(r, Response::Put { previous: Some("1".into()) });
}

#[test]
fn delete_returns_removed() {
    let mut sm = Fsm::default();
    sm.apply(&mut ctx(32), put("a", "1"));
    assert_eq!(sm.apply(&mut ctx(64), Command::Delete { key: "a".into() }), Response::Delete { removed: Some("1".into()) });
    assert_eq!(sm.apply(&mut ctx(96), Command::Delete { key: "a".into() }), Response::Delete { removed: None });
    assert_eq!(sm.query(Query::Get { key: "a".into() }), QueryResponse::Value(None));
}

#[test]
fn last_applied_tracks_position() {
    let mut sm = Fsm::default();
    assert_eq!(sm.last_applied(), None);
    sm.apply(&mut ctx(4096), put("a", "1"));
    assert_eq!(sm.last_applied(), Some(4096));
}

#[test]
fn validate_refuses_oversize() {
    let long_key = "k".repeat(app::MAX_KEY_LEN + 1);
    let long_val = "v".repeat(app::MAX_VALUE_LEN + 1);
    assert!(put(&long_key, "x").validate().is_err());
    assert!(put("k", &long_val).validate().is_err());
    assert!(Command::Delete { key: long_key }.validate().is_err());
    assert!(put("k", "v").validate().is_ok());
    // The largest valid command still fits the 1312 B crypto-on ceiling with
    // the 16 B session envelope in front of it.
    let max = put(&"k".repeat(app::MAX_KEY_LEN), &"v".repeat(app::MAX_VALUE_LEN));
    assert!(app::encode(&max).len() + 16 <= 1312);
}

#[test]
fn wire_round_trip() {
    let c = put("a", "1");
    let back: Command = app::decode(&app::encode(&c)).unwrap();
    assert_eq!(back, c);
}
```

`tests/snapshot.rs`:

```rust
use app::{Command, Fsm};
use uc_service::{ApplyCtx, SnapshotStateMachine, StateMachine};

fn filled() -> Fsm {
    let mut sm = Fsm::default();
    for (i, k) in ["b", "a", "c"].iter().enumerate() {
        sm.apply(&mut ApplyCtx::for_sm::<Fsm>(32 * (i as u64 + 1)), Command::Put { key: (*k).into(), value: format!("v{i}") });
    }
    sm
}

fn image(sm: &Fsm) -> (Vec<u8>, u64) {
    let (h, pos) = sm.freeze().unwrap();
    let mut buf = Vec::new();
    Fsm::stream_snapshot(h, &mut buf).unwrap();
    (buf, pos)
}

#[test]
fn round_trip_restores_state_and_cursor() {
    let sm = filled();
    let (buf, pos) = image(&sm);
    assert_eq!(pos, 96);
    let mut back = Fsm::default();
    let got = back.install_snapshot(128, &mut buf.as_slice()).unwrap();
    assert_eq!(got, 128, "install returns the instant, not the cursor");
    assert_eq!(back.last_applied(), Some(96), "cursor restored from the image, strictly below P");
    assert_eq!(back.state(), sm.state());
}

#[test]
fn install_refuses_unknown_image_version() {
    let (mut buf, _) = image(&filled());
    buf[0..4].copy_from_slice(&99u32.to_le_bytes());
    let mut sm = Fsm::default();
    let err = sm.install_snapshot(128, &mut buf.as_slice()).unwrap_err().to_string();
    assert!(err.contains("image version 99"), "{err}");
    assert_eq!(sm.last_applied(), None, "a refused install leaves state untouched");
}

#[test]
fn install_refuses_cursor_at_or_above_instant() {
    let (buf, _) = image(&filled()); // cursor 96
    let mut sm = Fsm::default();
    let err = sm.install_snapshot(96, &mut buf.as_slice()).unwrap_err().to_string();
    assert!(err.contains("not below"), "{err}");
    assert!(sm.state().entries.is_empty());
}

#[test]
fn projection_is_sorted_and_quoted() {
    let mut out = Vec::new();
    filled().project(&mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "entry \"a\"=\"v1\"\nentry \"b\"=\"v0\"\nentry \"c\"=\"v2\"\n");
}
```

`tests/determinism.rs`:

```rust
//! Same commands in → same state and same responses out, and a snapshot taken
//! mid-stream then installed elsewhere converges to the same place.
//! TODO(app): extend `arb_command` when you add a command, so every arm is exercised.
use app::{Command, Fsm};
use proptest::prelude::*;
use uc_service::{ApplyCtx, SnapshotStateMachine, StateMachine};

fn arb_command() -> impl Strategy<Value = Command> {
    let key = prop::sample::select(vec!["a", "b", "c", "d"]).prop_map(String::from);
    prop_oneof![
        (key.clone(), "[a-z]{0,8}").prop_map(|(key, value)| Command::Put { key, value }),
        key.prop_map(|key| Command::Delete { key }),
    ]
}

fn pos(i: usize) -> u64 {
    32 * (i as u64 + 1)
}

fn projection(sm: &Fsm) -> String {
    let mut out = Vec::new();
    sm.project(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

proptest! {
    #[test]
    fn replicas_agree(cmds in prop::collection::vec(arb_command(), 0..64)) {
        let (mut a, mut b) = (Fsm::default(), Fsm::default());
        for (i, c) in cmds.iter().enumerate() {
            let ra = a.apply(&mut ApplyCtx::for_sm::<Fsm>(pos(i)), c.clone());
            let rb = b.apply(&mut ApplyCtx::for_sm::<Fsm>(pos(i)), c.clone());
            prop_assert_eq!(ra, rb);
        }
        prop_assert_eq!(projection(&a), projection(&b));
    }

    #[test]
    fn snapshot_then_continue_equals_uninterrupted(
        cmds in prop::collection::vec(arb_command(), 1..64),
        cut in 0usize..64,
    ) {
        let cut = cut % cmds.len();
        let mut whole = Fsm::default();
        for (i, c) in cmds.iter().enumerate() {
            whole.apply(&mut ApplyCtx::for_sm::<Fsm>(pos(i)), c.clone());
        }
        let mut first = Fsm::default();
        for (i, c) in cmds[..cut].iter().enumerate() {
            first.apply(&mut ApplyCtx::for_sm::<Fsm>(pos(i)), c.clone());
        }
        let (h, _) = first.freeze().unwrap();
        let mut img = Vec::new();
        Fsm::stream_snapshot(h, &mut img).unwrap();
        let mut restored = Fsm::default();
        restored.install_snapshot(pos(cut), &mut img.as_slice()).unwrap();
        for (i, c) in cmds.iter().enumerate().skip(cut) {
            restored.apply(&mut ApplyCtx::for_sm::<Fsm>(pos(i)), c.clone());
        }
        prop_assert_eq!(projection(&restored), projection(&whole));
    }
}
```

Note `install_snapshot(pos(cut), …)`: the instant P is the exclusive frontier — the next frame starts at `pos(cut)`, and the image's cursor is `pos(cut-1)` (or none).

- [ ] **Step 2: Run to verify failure** — in the generated project: `cargo test`. Expected: compile errors (`Command`, `Fsm` not found).

- [ ] **Step 3: Implement.**

`src/commands.rs`:

```rust
//! The wire contract between your client and your state machine.
//!
//! Commands go through consensus and are applied on every replica in log
//! order; queries are answered from one replica's local state.
//! TODO(app): replace the registry's commands and queries with your own (keep them in docs/app-design.md in sync).

use serde::{Deserialize, Serialize};

/// Largest key and value this app accepts. One command must fit one datagram:
/// the payload ceiling is 1344 B (1312 B with wire crypto) at the baseline
/// rung, and the session envelope takes 16 B of it.
pub const MAX_KEY_LEN: usize = 128;
pub const MAX_VALUE_LEN: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    // Append new variants at the END: the variant index is the wire tag, and
    // reordering changes what old log entries mean.
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Put { previous: Option<String> },
    Delete { removed: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    Get { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryResponse {
    Value(Option<String>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommandError(pub String);

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Command {
    /// Checked by the client BEFORE submitting: a command that cannot fit one
    /// datagram is refused at the door, not half-way through the cluster.
    pub fn validate(&self) -> Result<(), CommandError> {
        let (key, value) = match self {
            Command::Put { key, value } => (key, Some(value)),
            Command::Delete { key } => (key, None),
        };
        if key.len() > MAX_KEY_LEN {
            return Err(CommandError(format!("key is {} bytes; the limit is {MAX_KEY_LEN}", key.len())));
        }
        if let Some(v) = value {
            if v.len() > MAX_VALUE_LEN {
                return Err(CommandError(format!("value is {} bytes; the limit is {MAX_VALUE_LEN}", v.len())));
            }
        }
        Ok(())
    }
}

/// The typed tier's codec (bincode 2, standard config). The client must
/// encode exactly as the service decodes.
pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(v, bincode::config::standard()).expect("encoding cannot fail")
}

pub fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, String> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| format!("cannot decode ({e}) — client and service built from different code?"))
}
```

`src/state.rs`:

```rust
//! The replicated state and the deterministic transition function.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uc_service::{ApplyCtx, StateMachine};

use crate::commands::{Command, Query, QueryResponse, Response};
use crate::identity::{FSM_NAME, FSM_VERSION};

/// TODO(app): replace the registry with your state.
/// BTreeMap, not HashMap: a HashMap's iteration order differs between
/// processes, so anything that iterates it (a snapshot, a projection, a
/// "list" response) would differ between replicas.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub entries: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct Fsm {
    pub(crate) state: State,
    pub(crate) last_applied: Option<u64>,
}

impl Fsm {
    pub fn state(&self) -> &State {
        &self.state
    }
}

impl StateMachine for Fsm {
    const NAME: &'static str = FSM_NAME;
    const VERSION: u32 = FSM_VERSION;

    type Command = Command;
    type Response = Response;
    type Query = Query;
    type QueryResponse = QueryResponse;

    /// Runs on EVERY replica for every committed command, in log order. Same
    /// state + same command must give the same result everywhere, forever:
    /// no clock (use `ctx.time_ns`), no randomness (use `uc_service::IdGen`),
    /// no I/O, no HashMap iteration, no floats you compare.
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Command) -> Response {
        // TODO(app): one match arm per command in src/commands.rs.
        let out = match cmd {
            Command::Put { key, value } => Response::Put { previous: self.state.entries.insert(key, value) },
            Command::Delete { key } => Response::Delete { removed: self.state.entries.remove(&key) },
        };
        self.last_applied = Some(ctx.position);
        out
    }

    /// Answers a read from local state. Linearizable vs. snapshot is the
    /// client's choice, enforced by the framework; this method is the same.
    fn query(&self, q: Query) -> QueryResponse {
        // TODO(app): one match arm per query in src/commands.rs.
        match q {
            Query::Get { key } => QueryResponse::Value(self.state.entries.get(&key).cloned()),
        }
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}
```

`src/snapshot.rs`:

```rust
//! Snapshots: lets a node that fell behind install state instead of replaying
//! the whole log, and lets the journal be purged. The payload format is yours;
//! UC wraps it in its own envelope. This whole-state serde image is correct
//! for any `State`; replace it when state gets large (docs/how-to/change-the-state-shape.md).

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, SnapshotStateMachine};

use crate::state::{Fsm, State};

/// Bump when `State`'s serialized shape changes, and keep reading the old one.
const IMAGE_VERSION: u32 = 1;
/// A refusal, not an allocation: an image this large is corrupt.
const MAX_IMAGE_BYTES: u64 = 1 << 30;

#[derive(Serialize, Deserialize)]
struct Image {
    cursor: Option<u64>,
    state: State,
}

pub struct Frozen {
    cursor: Option<u64>,
    state: State,
}

fn codec(e: impl std::fmt::Display) -> SnapshotError {
    SnapshotError::Codec(e.to_string())
}

impl SnapshotStateMachine for Fsm {
    type SnapshotHandle = Frozen;

    /// Clones the state on the apply thread: O(state). Fine for a starter;
    /// see the how-to for an O(1) persistent-map freeze.
    fn freeze(&self) -> Result<(Frozen, u64), SnapshotError> {
        let h = Frozen { cursor: self.last_applied, state: self.state.clone() };
        Ok((h, self.last_applied.unwrap_or(0)))
    }

    /// Layout: `image_version u32 LE ‖ len u64 LE ‖ bincode(Image)`.
    fn stream_snapshot(h: Frozen, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        let body = bincode::serde::encode_to_vec(&Image { cursor: h.cursor, state: h.state }, bincode::config::standard())
            .map_err(codec)?;
        dst.write_all(&IMAGE_VERSION.to_le_bytes())?;
        dst.write_all(&(body.len() as u64).to_le_bytes())?;
        dst.write_all(&body)?;
        Ok(())
    }

    /// `position` is the instant P, an EXCLUSIVE frontier: the image covers
    /// frames strictly below P. Restore the image's own cursor, return P.
    /// Nothing changes unless the whole image decodes.
    fn install_snapshot(&mut self, position: u64, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut word = [0u8; 4];
        src.read_exact(&mut word)?;
        let v = u32::from_le_bytes(word);
        if v != IMAGE_VERSION {
            return Err(codec(format!("unknown image version {v} (this build reads {IMAGE_VERSION})")));
        }
        let mut len = [0u8; 8];
        src.read_exact(&mut len)?;
        let len = u64::from_le_bytes(len);
        if len > MAX_IMAGE_BYTES {
            return Err(codec(format!("image body of {len} bytes exceeds {MAX_IMAGE_BYTES}")));
        }
        let mut body = vec![0u8; len as usize];
        src.read_exact(&mut body)?;
        let (img, used): (Image, usize) =
            bincode::serde::decode_from_slice(&body, bincode::config::standard()).map_err(codec)?;
        if used != body.len() {
            return Err(codec(format!("image has {} trailing bytes", body.len() - used)));
        }
        if let Some(c) = img.cursor {
            if c >= position {
                return Err(codec(format!("image cursor {c} is not below the instant {position}")));
            }
        }
        self.state = img.state;
        self.last_applied = img.cursor;
        Ok(position)
    }

    /// Canonical text for diff replay: one line per entry, sorted (BTreeMap
    /// order), Debug-quoted so a newline in a value cannot forge a line.
    fn project(&self, out: &mut dyn Write) -> Result<(), SnapshotError> {
        // TODO(app): one line per record of your state, in a stable sorted order.
        for (k, v) in &self.state.entries {
            writeln!(out, "entry {k:?}={v:?}")?;
        }
        Ok(())
    }
}
```

`src/lib.rs`:

```rust
//! LITERAL-CHECK {{not_a_placeholder}} — this line proves the generator copies
//! Rust sources verbatim (template-tests/generator.sh). Leave it.
//!
//! Module map:
//! - `identity`  — FSM name/version, app id, ports (generated)
//! - `commands`  — the wire contract: Command / Response / Query / QueryResponse
//! - `state`     — the replicated state and `apply` / `query`
//! - `snapshot`  — snapshot image + projection
//! The two binaries are `src/bin/service.rs` and `src/bin/client.rs`.

pub mod commands;
pub mod identity;
pub mod snapshot;
pub mod state;

pub use commands::{decode, encode, Command, CommandError, Query, QueryResponse, Response, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use state::{Fsm, State};
```

Note the `{{not_a_placeholder}}` line lives in a `//!` doc comment and is harmless to rustdoc.

- [ ] **Step 4: Run tests** — `cargo test` in the generated project. Expected: all tests in `state`, `snapshot`, `determinism` PASS. Also run `cargo clippy --all-targets -- -D warnings` (clean).

- [ ] **Step 5: Watch one test fail for the right reason.** Temporarily change `if c >= position` to `if c > position` and re-run `cargo test --test snapshot`; expect `install_refuses_cursor_at_or_above_instant` to FAIL; revert. Record in the commit message that you watched it fail.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: registry state machine skeleton — typed tier, snapshot image v1, projection, determinism proptest"`

---

### Task 3: Service and client binaries

**Files:**
- Create: `src/bin/service.rs`, `src/bin/client.rs`

**Interfaces:**
- Consumes: Task 2's `app::*`, `app::identity::{APP_ID, FSM_NAME, local_gateways}`.
- Produces: `<name>-service [--instance-dir D] [--app-id A] [--wait-secs N] [replay --corpus C --out O [--from-genesis] | project --artifact F --position P]`; `<name> [--gateways h:p,…] [--app-id A] [--timeout-secs N] (put K V | delete K | get K [--linearizable])` printing exactly:
  - put: `ok previous=<none|"v"> position=<N> replayed=<bool>`
  - delete: `ok removed=<none|"v"> position=<N> replayed=<bool>`
  - get: `value=<none|"v">`
  Exit 0 ok, 1 request failed, 2 bad arguments (including `validate()` refusals). Default `--gateways` is `local_gateways($UC_PORT_OFFSET or 0)`.

- [ ] **Step 1: Write the failing CLI test** `tests/cli.rs`. Cargo's `CARGO_BIN_EXE_<name>` would need the templated bin name in Rust source, so the test locates binaries by package name at runtime instead:

```rust
//! Argument handling that must fail fast without a cluster.
use std::path::PathBuf;
use std::process::Command;

fn bin(suffix: &str) -> PathBuf {
    let exe = std::env::current_exe().unwrap(); // target/<profile>/deps/cli-<hash>
    let dir = exe.parent().unwrap().parent().unwrap();
    dir.join(format!("{}{suffix}", env!("CARGO_PKG_NAME")))
}

#[test]
fn oversize_value_is_refused_before_connecting() {
    let out = Command::new(bin(""))
        .args(["--gateways", "127.0.0.1:1", "put", "k", &"v".repeat(app::MAX_VALUE_LEN + 1)])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stderr).contains("limit is"));
}

#[test]
fn bad_gateway_address_is_exit_2() {
    let out = Command::new(bin("")).args(["--gateways", "nocolon", "get", "k"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn service_without_instance_dir_is_refused() {
    let out = Command::new(bin("-service")).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--instance-dir"));
}
```

Integration tests build the package's binaries first, so `bin()` resolves.

- [ ] **Step 2: Run** `cargo test --test cli` — expected FAIL (binaries missing).

- [ ] **Step 3: Implement** `src/bin/service.rs` — copy `examples/kv/src/bin/kv-service.rs` from the `ultima_cluster` repo at tag `v2.13.0` verbatim in structure, with these exact substitutions:
  - module doc: "The service half: runs `Sessioned<Fsm>` against a local `uc2-node`." plus the kv doc's two bullet points (sessions, snapshots) and exit-code line;
  - add `#![allow(clippy::disallowed_methods)]` as the first line with the comment `// The service's supervision loop legitimately reads the clock; apply does not.`;
  - `use app::{Fsm, identity::{APP_ID, FSM_NAME}};` and `use uc_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned, StateMachine};`
  - clap `name = env!("CARGO_BIN_NAME")`, `--app-id` `default_value = APP_ID`;
  - `let sm = || Sessioned::new(Fsm::default(), SessionConfig::default());`
  - the attach log line: `eprintln!("{}: attached fsm={FSM_NAME:?} version={} row={} epoch={} instance_dir={}", env!("CARGO_BIN_NAME"), uc_service_version_string(), service.service_id(), service.epoch(), instance_dir.display());` where `fn uc_service_version_string() -> String { let (a, b, c) = uc_protocol::identity::unpack_version(<Fsm as StateMachine>::VERSION); format!("{a}.{b}.{c}") }`;
  - every `"kv-service: …"` message prefix becomes `env!("CARGO_BIN_NAME")`.

`src/bin/client.rs` — structure from `examples/counter/src/bin/counter-remote.rs` at `v2.13.0` (keep its doc comment's points 1–3, its `Fail` enum, `connect` retry loop, `remaining`, `wait_timeout` use), changed as follows:
  - `#![allow(clippy::disallowed_methods)]` first line (deadlines need `Instant::now`);
  - `--gateways` is `Option<Vec<String>>` (`value_delimiter = ','`); when absent use `app::identity::local_gateways(offset)` with `offset = std::env::var("UC_PORT_OFFSET").ok().and_then(|s| s.parse().ok()).unwrap_or(0)`;
  - `--app-id` default `app::identity::APP_ID`;
  - `RemoteConfig { app_id, members, request_timeout, ..Default::default() }` — leave `resend_on_unknown` at its default `true`, with a comment: "the service runs `Sessioned`, so a re-send is answered `replayed`, never applied twice";
  - subcommands `Put { key, value }`, `Delete { key }`, `Get { key, #[arg(long)] linearizable: bool }` with a `// TODO(app): one subcommand per Command/Query in src/commands.rs.` line above the enum;
  - before connecting, for Put/Delete build the `Command` and call `validate()`; on `Err(e)` return `Fail::Args(e.to_string())`;
  - encode/decode with `app::encode` / `app::decode::<Response>` / `app::decode::<QueryResponse>`;
  - print formats exactly as in **Interfaces** above, rendering `Option<String>` as `none` or `{v:?}`.

- [ ] **Step 4: Run** `cargo test --test cli` and `cargo clippy --all-targets -- -D warnings`. Expected: PASS, clean.

- [ ] **Step 5: Commit** — `git commit -am "feat: service (run/replay/project) and remote client binaries"` (add new files first).

---

### Task 4: Determinism guardrails

**Files:**
- Create: `clippy.toml`, `scripts/lint-determinism.sh`, `template-tests/lint-determinism.sh`

**Interfaces:**
- Produces: `scripts/lint-determinism.sh [--all | FILE…]` — checks only FSM files (`src/commands.rs`, `src/state.rs`, `src/snapshot.rs`, anything under `src/fsm/`); prints `path:line: <hazard> — <substitute>`; exit 1 on any hit, 0 otherwise (including for non-FSM files). A line containing `determinism: ok` is exempt.

- [ ] **Step 1: Failing test** `template-tests/lint-determinism.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
P="$HOME/scratch/uc_starter-gen/lint/demo-app"
rm -rf "$(dirname "$P")"; "$HERE/gen.sh" "$(dirname "$P")" >/dev/null
cd "$P"
fail() { echo "FAIL: $*" >&2; exit 1; }
scripts/lint-determinism.sh --all || fail "skeleton must be clean"
for bad in 'let t = std::time::SystemTime::now();' 'let m: std::collections::HashMap<u8,u8> = Default::default();' \
           'let x: f64 = 0.1;' 'let r = rand::random::<u64>();' 'let _ = std::fs::read("x");' 'let i = std::time::Instant::now();'; do
  printf '%s\n' "fn _hazard() { $bad }" >> src/state.rs
  if scripts/lint-determinism.sh src/state.rs >/dev/null; then fail "not caught: $bad"; fi
  git checkout -q -- src/state.rs 2>/dev/null || sed -i '$d' src/state.rs
done
printf '%s\n' 'fn _ok() { let _x: f64 = 0.0; } // determinism: ok — test fixture' >> src/state.rs
scripts/lint-determinism.sh src/state.rs || fail "exemption ignored"
sed -i '$d' src/state.rs
scripts/lint-determinism.sh src/bin/client.rs || fail "non-FSM file must be skipped"
echo "lint-determinism: PASS"
```

(The generated project is a git repo — cargo-generate initialises one — so `git checkout` restores the file; the `sed` fallback covers `--vcs none`.)

- [ ] **Step 2: Run** — expected FAIL (script missing).

- [ ] **Step 3: Implement.**

`scripts/lint-determinism.sh`:

```bash
#!/usr/bin/env bash
# lint-determinism.sh [--all | FILE…] — grep-level determinism hazards in FSM
# files. Fast enough for an editor hook; `make lint` also runs clippy with
# clippy.toml's bans. Exempt one line with a `determinism: ok <why>` comment.
set -uo pipefail
cd "$(dirname "$0")/.."
is_fsm() { case "$1" in src/commands.rs|src/state.rs|src/snapshot.rs|src/fsm/*) return 0 ;; *) return 1 ;; esac; }
HAZARDS=(
  'SystemTime::now|wall clock — use ctx.time_ns'
  'Instant::now|clock — use ctx.time_ns'
  'rand::|thread_rng|randomness — use uc_service::IdGen'
  'HashMap|HashSet|hash iteration order differs per process — use BTreeMap/BTreeSet'
  '\bf32\b|\bf64\b|floating point — use integers (fixed-point)'
  'std::fs::|std::net::|tokio::|std::process::|I/O in apply — side effects belong in an OutputHandler'
  'std::env::|environment differs per host — pass it as a command'
)
files=()
if [ "${1:-}" = --all ]; then
  while IFS= read -r f; do files+=("$f"); done < <(ls src/commands.rs src/state.rs src/snapshot.rs 2>/dev/null; find src/fsm -name '*.rs' 2>/dev/null)
else
  for f in "$@"; do f="${f#./}"; f="${f#"$PWD"/}"; is_fsm "$f" && files+=("$f"); done
fi
[ "${#files[@]}" -eq 0 ] && exit 0
rc=0
for f in "${files[@]}"; do
  for h in "${HAZARDS[@]}"; do
    pat="${h%|*}"; why="${h##*|}"
    # `pat` may itself contain `|` alternations: everything before the LAST `|` is the regex.
    while IFS= read -r hit; do
      [ -z "$hit" ] && continue
      case "$hit" in *"determinism: ok"*) continue ;; esac
      echo "$f:${hit%%:*}: ${why}"; rc=1
    done < <(grep -nE "$pat" "$f" | grep -vE '^[0-9]+:\s*//' || true)
  done
done
exit $rc
```

(Comment-only lines are skipped by the second `grep -v`, so the doc comments that *mention* `HashMap` do not trip it.)

`clippy.toml`:

```toml
# Determinism bans for the whole crate. The binaries opt out with
# #![allow(clippy::disallowed_methods)] because supervision needs a clock;
# apply never does. RNG crates are not listed (clippy warns on paths that do
# not resolve); scripts/lint-determinism.sh covers them.
disallowed-methods = [
  { path = "std::time::SystemTime::now", reason = "apply must not read a clock: use ctx.time_ns" },
  { path = "std::time::Instant::now", reason = "apply must not read a clock: use ctx.time_ns" },
]
disallowed-types = [
  { path = "std::collections::HashMap", reason = "iteration order differs per process: use BTreeMap" },
  { path = "std::collections::HashSet", reason = "iteration order differs per process: use BTreeSet" },
]
```

- [ ] **Step 4: Run** `bash template-tests/lint-determinism.sh` → `PASS`; in the generated project `cargo clippy --all-targets -- -D warnings` → clean (if a test file trips `disallowed-types`, add a scoped `#![allow]` there with a one-line reason, never a crate-wide one).

- [ ] **Step 5: Commit** — `git commit -m "feat: determinism guardrails — clippy bans + fast grep lint"`

---

### Task 5: Fetch, cluster, demo, failover, Makefile and the cluster smoke

**Files:**
- Create: `scripts/lib.sh`, `scripts/fetch-uc.sh`, `scripts/cluster.sh`, `scripts/demo.sh`, `scripts/kill-leader.sh`, `scripts/stamp.sh`, `Makefile`, `tests/cluster.rs`

**Interfaces:**
- Produces (`scripts/lib.sh`, sourced by every script): `UC_BIN` (`.uc/bin`), `APP_BIN_DIR` (cargo release dir), `ROOT` (`${UC_ROOT:-$HOME/.uc-starter/$APP_NAME}`), `OFF` (`${UC_PORT_OFFSET:-0}`), `NODE_PORT(i)`, `GW_PORT(i)`, `METRICS_PORT(i)`, `gateways_csv`, `die MSG` (exit 3), `require_linux`, `tree_hash PATH…`, `code_hash` (= `tree_hash src Cargo.toml Cargo.lock`), `code_hash_with_tests` (+ `tests`), `write_stamp ID HASH`, `stamp_state ID HASH` → prints `missing|stale|fresh`, `progress_has ID`.
- `scripts/stamp.sh ID` writes the right hash for ID (`check` → `code_hash_with_tests`; `skeleton`, `snapshots`, `observe`, `upgrade-check`, `upgrade-drill` → `any`; else `code_hash`) unless `UC_NO_STAMP=1`.
- `scripts/cluster.sh` commands: `up [--fresh] | down | status | leader | wait-leader [secs] | stop|kill|start <node|service|gateway> N | restart-services | snapshot | snapshot-show N | ctl N ARGS… | metrics N | gateways | root`. `ctl` refuses any args containing `upgrade pin` unless `UC_CONFIRM_PIN=yes`.
- `scripts/demo.sh` — exit 0 and stamps `demo` (and `skeleton` if absent) on success.
- `scripts/kill-leader.sh` — kills the leader node, waits for a new one, runs the demo, stamps `failover`, restarts the killed node's processes.

- [ ] **Step 1: Failing smoke test** `tests/cluster.rs`:

```rust
//! Three nodes, three services, three gateways; the demo; a leader kill; the
//! demo again. `cargo test --release --features cluster-tests --test cluster`.
#![cfg(feature = "cluster-tests")]
use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    // CARGO_TARGET_TMPDIR is under target/, i.e. real disk, never /tmp.
    Path::new(env!("CARGO_TARGET_TMPDIR")).join("cluster-test")
}

fn sh(args: &[&str]) {
    let dir = env!("CARGO_MANIFEST_DIR");
    let status = Command::new(format!("{dir}/{}", args[0]))
        .args(&args[1..])
        .current_dir(dir)
        .env("UC_ROOT", root())
        .env("UC_PORT_OFFSET", "10") // never collide with the developer's own cluster
        .env("UC_NO_STAMP", "1")      // tests must not move the tutor
        .status()
        .unwrap();
    assert!(status.success(), "{args:?} failed");
}

struct Down;
impl Drop for Down {
    fn drop(&mut self) {
        let _ = Command::new(format!("{}/scripts/cluster.sh", env!("CARGO_MANIFEST_DIR")))
            .arg("down").env("UC_ROOT", root()).env("UC_PORT_OFFSET", "10").status();
    }
}

#[test]
fn demo_survives_a_leader_kill() {
    let _down = Down;
    sh(&["scripts/cluster.sh", "up", "--fresh"]);
    sh(&["scripts/demo.sh"]);
    sh(&["scripts/kill-leader.sh"]);
    sh(&["scripts/demo.sh"]);
}
```

- [ ] **Step 2: Run** `cargo test --release --features cluster-tests --test cluster` → FAIL (scripts missing).

- [ ] **Step 3: Implement the scripts.**

`scripts/lib.sh`:

```bash
# Sourced by every script. Paths, ports, stamps, progress.
set -uo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
# shellcheck disable=SC1091
. ./uc-app.env
UC_VERSION="$(cat UC_VERSION)"
UC_BIN="$PROJECT_DIR/.uc/bin"
ROOT="${UC_ROOT:-$HOME/.uc-starter/$APP_NAME}"
OFF="${UC_PORT_OFFSET:-0}"
NODE_PORT()    { echo $((BASE_PORT + OFF + $1)); }
GW_PORT()      { echo $((BASE_PORT + OFF + 100 + $1)); }
METRICS_PORT() { echo $((BASE_PORT + OFF + 200 + $1)); }
gateways_csv() { echo "127.0.0.1:$(GW_PORT 0),127.0.0.1:$(GW_PORT 1),127.0.0.1:$(GW_PORT 2)"; }
app_bin_dir() {
  local t; t="$(cargo metadata --format-version=1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
  echo "${t:-$PROJECT_DIR/target}/release"
}
die() { printf '%s: %s\n' "$(basename "$0")" "$*" >&2; exit 3; }
require_linux() {
  [ "$(uname -s)" = Linux ] || die "ultima_cluster nodes run on Linux only (this is $(uname -s)). Open this project in its devcontainer — see README.md § Devcontainer."
}
tree_hash() { # paths… → 16 hex chars over file names + contents
  local p; for p in "$@"; do [ -e "$p" ] && find "$p" -type f -not -path '*/target/*' -print0; done \
    | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | cut -c1-16
}
code_hash()            { tree_hash src Cargo.toml Cargo.lock; }
code_hash_with_tests() { tree_hash src tests Cargo.toml Cargo.lock; }
STATE_DIR="$PROJECT_DIR/.uc/state"
write_stamp() { [ "${UC_NO_STAMP:-0}" = 1 ] && return 0; mkdir -p "$STATE_DIR"; echo "$2" >"$STATE_DIR/$1.ok"; }
stamp_state() { # id hash → missing|stale|fresh ; hash "any" accepts any content
  local f="$STATE_DIR/$1.ok"
  [ -f "$f" ] || { echo missing; return; }
  [ "$2" = any ] && { echo fresh; return; }
  [ "$(cat "$f")" = "$2" ] && echo fresh || echo stale
}
PROGRESS="$PROJECT_DIR/.uc-progress"
progress_has() { [ -f "$PROGRESS" ] && grep -qE "^(done|skip) $1( |$)" "$PROGRESS"; }
progress_skipped() { [ -f "$PROGRESS" ] && grep -qE "^skip $1( |$)" "$PROGRESS"; }
```

`scripts/stamp.sh`:

```bash
#!/usr/bin/env bash
# stamp.sh ID — record that ID's proof ran, against the current code.
. "$(dirname "$0")/lib.sh"
case "$1" in
  check) write_stamp check "$(code_hash_with_tests)" ;;
  skeleton|snapshots|observe|upgrade-check|upgrade-drill) write_stamp "$1" any ;;
  *) write_stamp "$1" "$(code_hash)" ;;
esac
```

`scripts/fetch-uc.sh`:

```bash
#!/usr/bin/env bash
# fetch-uc.sh — download the ultima_cluster release named in UC_VERSION into
# .uc/bin (binaries) and .uc/packaging (systemd units, alert rules), verified.
. "$(dirname "$0")/lib.sh"
set -e
require_linux
case "$(uname -m)" in x86_64) ARCH=x86_64 ;; aarch64|arm64) ARCH=aarch64 ;; *) die "no ultima_cluster release for $(uname -m)" ;; esac
NAME="uc2-${UC_VERSION}-${ARCH}-unknown-linux-gnu"
URL="https://github.com/PeterKnego/ultima_cluster/releases/download/v${UC_VERSION}"
if [ -x "$UC_BIN/uc2-node" ] && "$UC_BIN/uc2-node" --version 2>/dev/null | grep -q "$UC_VERSION"; then
  echo "ultima_cluster $UC_VERSION already in .uc/bin"; exit 0
fi
DL="$PROJECT_DIR/.uc/download"; rm -rf "$DL"; mkdir -p "$DL"
echo "downloading $NAME.tar.gz"
curl -fsSL -o "$DL/$NAME.tar.gz" "$URL/$NAME.tar.gz"
curl -fsSL -o "$DL/SHA256SUMS" "$URL/SHA256SUMS"
(cd "$DL" && sha256sum -c SHA256SUMS --ignore-missing) || die "checksum mismatch for $NAME.tar.gz — refusing to install"
if command -v cosign >/dev/null; then
  curl -fsSL -o "$DL/$NAME.tar.gz.sigstore.json" "$URL/$NAME.tar.gz.sigstore.json"
  cosign verify-blob --bundle "$DL/$NAME.tar.gz.sigstore.json" \
    --certificate-identity-regexp '^https://github.com/PeterKnego/ultima_cluster/\.github/workflows/release\.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com "$DL/$NAME.tar.gz" >/dev/null \
    || die "cosign signature verification failed for $NAME.tar.gz"
  echo "signature verified (cosign)"
else
  echo "cosign not installed: checksum verified, signature not checked"
fi
tar xzf "$DL/$NAME.tar.gz" -C "$DL"
rm -rf "$UC_BIN" "$PROJECT_DIR/.uc/packaging"; mkdir -p "$UC_BIN"
cp "$DL/$NAME/bin/"* "$UC_BIN/"
cp -r "$DL/$NAME/packaging" "$PROJECT_DIR/.uc/packaging"
"$UC_BIN/uc2-node" --version
```

Verify the cosign identity regexp against `ultima_cluster/.github/workflows/release.yml` lines ~385-396 (its own `cosign verify-blob` call) and copy its exact `--certificate-identity*` flags.

`scripts/cluster.sh` — port `examples/kv/scripts/kvcluster.sh` (at `v2.13.0`) with these changes, keeping its functions (`pidfile`, `alive`, `spawn`, `stop_one`, `ctl`, `leader`, `wait_leader`, `write_config`, `cmd_up`, `cmd_down`, `cmd_status`) and its comments about BindsTo emulation and the SIGKILL'd control page:
  - source `lib.sh` instead of computing paths; `APP="$APP_ID"`; binaries `$UC_BIN/uc2-*` and `$(app_bin_dir)/$APP_NAME-service`;
  - ports from `NODE_PORT`/`GW_PORT`/`METRICS_PORT`; the marker file is `$ROOT/.uc-starter`;
  - in `cmd_up`: `require_linux` first; refuse RAM-backed roots (`/tmp*`, `/dev/shm*`) with `die "UC_ROOT=$ROOT is RAM-backed; nodes refuse it"`; check **all three bands** (UDP node ports via `ss -uln | grep -q ":$port "`, TCP gateway and metrics ports via `tcp_open`) and `die "port $p is in use — another cluster? set UC_PORT_OFFSET or run make down"`;
  - `node.toml`: `[services] names = ["$FSM_NAME"]`, `app_id = "$APP_ID"`, everything else as kvcluster's (small geometry, `[purge]`, `[settings]`, `[metrics]`, `[crypto] enabled = false`, `[admin] auth = "hmac"`); gateway toml as kvcluster's with `[session] envelope = true`;
  - new command `restart-services`: `for i in 0 1 2; do stop_one service $i; start_service $i; done`;
  - `ctl`: `case " $* " in *" upgrade pin "*|*" upgrade "*" pin "*) [ "${UC_CONFIRM_PIN:-}" = yes ] || die "refusing 'upgrade pin' without UC_CONFIRM_PIN=yes — a pin is a one-way door (WHAT-NEXT.md, Step 12)";; esac`;
  - `snapshot` prints uc2ctl's `instant=<P>` line unchanged (Task 8 parses it);
  - the usage text lists every command above.

`scripts/demo.sh`:

```bash
#!/usr/bin/env bash
# demo.sh — drive the running cluster through the gateways and check answers.
# TODO(app): rewrite these calls for your commands (keep the expect checks).
. "$(dirname "$0")/lib.sh"
CLI="$(app_bin_dir)/$APP_NAME"
[ -x "$CLI" ] || die "$CLI missing — run make build"
GW="$(gateways_csv)"
fails=0
expect() { # description, expected-substring, command…
  local d="$1" want="$2"; shift 2
  local got; got="$("$CLI" --gateways "$GW" "$@" 2>&1)"; local rc=$?
  if [ $rc -eq 0 ] && [[ "$got" == *"$want"* ]]; then printf '   %-28s -> %s\n' "$d" "$got"
  else printf '   %-28s -> FAILED (exit %s): %s\n' "$d" "$rc" "$got"; fails=$((fails+1)); fi
}
echo "demo against $GW"
expect "put greeting hello"      'ok previous='      put greeting hello
expect "get greeting"            'value="hello"'     get greeting --linearizable
expect "delete greeting"         'ok removed="hello"' delete greeting
expect "get greeting (deleted)"  'value=none'        get greeting --linearizable
[ $fails -eq 0 ] || { echo "FAIL ($fails)"; exit 1; }
"$(dirname "$0")/stamp.sh" demo
[ "$(stamp_state skeleton any)" = fresh ] || "$(dirname "$0")/stamp.sh" skeleton
echo PASS
```

`scripts/kill-leader.sh`:

```bash
#!/usr/bin/env bash
# kill-leader.sh — SIGKILL the leader node, watch the cluster elect another,
# prove writes still work, then bring the old leader back.
. "$(dirname "$0")/lib.sh"
C="$(dirname "$0")/cluster.sh"
old="$("$C" leader)" || die "no serving leader — run make up"
echo "leader is node $old; killing it (SIGKILL)"
"$C" kill service "$old"; "$C" kill node "$old"
new=""; for _ in $(seq 1 150); do n="$("$C" leader 2>/dev/null)" && [ "$n" != "$old" ] && { new="$n"; break; }; sleep 0.2; done
[ -n "$new" ] || die "no new leader within 30s"
echo "node $new is the new leader"
UC_NO_STAMP=1 "$(dirname "$0")/demo.sh" || exit 1
"$(dirname "$0")/stamp.sh" failover
echo "restarting node $old"
"$C" start node "$old"; sleep 1; "$C" start service "$old"; "$C" start gateway "$old"
echo PASS
```

`Makefile`:

```make
# The one entry point — for you, your agent and CI. `make help` lists targets.
include uc-app.env
UC_VERSION := $(shell cat UC_VERSION)
MSRV := 1.89.0
export APP_NAME APP_ID FSM_NAME BASE_PORT

.DEFAULT_GOAL := help
.PHONY: help next bins build up down status restart-services demo kill-leader test test-cluster lint check todo done skip \
        diffreplay corpus upgrade-check snapshot-drill observe upgrade-drill package uc-upgrade

help: ## this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t22

next: ## where am I on WHAT-NEXT.md? (agents: scripts/next.sh --json)
	@scripts/next.sh
bins: ## download + verify the ultima_cluster binaries for UC_VERSION
	@scripts/fetch-uc.sh
build: ## build the service and client (release)
	cargo build --release
up: build ## start 3 nodes + 3 services + 3 gateways (FRESH=1 wipes state)
	scripts/cluster.sh up $(if $(FRESH),--fresh)
down: ## stop the local cluster
	scripts/cluster.sh down
status: ## uc2ctl status on every node
	scripts/cluster.sh status
restart-services: build ## restart only your service after a rebuild
	scripts/cluster.sh restart-services
demo: ## run scripts/demo.sh against the gateways
	scripts/demo.sh
kill-leader: ## the failover exercise
	scripts/kill-leader.sh
test: ## unit + determinism + snapshot tests
	cargo test
test-cluster: build ## the 3-node smoke (spawns processes)
	cargo test --release --features cluster-tests --test cluster -- --nocapture
lint: ## fmt + clippy + MSRV clippy + determinism grep
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings
	CARGO_TARGET_DIR=target/msrv cargo +$(MSRV) clippy --all-targets --locked -- -D warnings
	scripts/lint-determinism.sh --all
check: test lint ## test + lint, and record it for the tutor
	@scripts/stamp.sh check
todo: ## the TODO(app) markers left
	@grep -rn 'TODO(app)' src tests scripts docs WHAT-NEXT.md 2>/dev/null || echo "no TODO(app) markers left"
done: ## record a step the repo cannot show: make done STEP=concepts
	@scripts/progress.sh done $(STEP)
skip: ## deliberately skip a step: make skip STEP=snapshots
	@scripts/progress.sh skip $(STEP)
```

(Part 2 targets are appended in Task 8; `progress.sh` is created in Task 6 — until then `done`/`skip` fail, which is fine.)

- [ ] **Step 4: Run** in a generated project: `make bins && make test-cluster`. Expected: the test PASSES (look for two demo `PASS` lines and "node N is the new leader"). Then `make up && make demo && make kill-leader && make down` by hand. Then run the whole thing inside `UC_PORT_OFFSET=0` while a second generated app with `base_port=7300` is up, to prove the port bands do not collide.

- [ ] **Step 5: Watch the port guard fire** — with the cluster up, run `UC_ROOT=$HOME/scratch/other scripts/cluster.sh up`; expect exit 3 with `port … is in use`.

- [ ] **Step 6: Commit** — `git commit -m "feat: local 3-node cluster tooling, demo, failover drill, Makefile, cluster smoke"`

---

### Task 6: The tutor checker (`next.sh`, `progress.sh`)

**Files:**
- Create: `scripts/next.sh`, `scripts/progress.sh`, `template-tests/tutor.sh`, `.uc-progress` (empty, committed)

**Interfaces:**
- Consumes: `lib.sh` (`stamp_state`, `code_hash`, `code_hash_with_tests`, `progress_has`, `progress_skipped`).
- Produces: `scripts/next.sh [--json | --list]`. `--list` prints the 13 step ids in order, one per line. Human output first line: `Step N/13 · Part P · <title> → WHAT-NEXT.md "Step N"`; then `  status: todo|stale`; then `  - <detail>` lines. When the first undone step is in Part 2 and Part 1 was just completed, a line `Part 1 complete — your app runs on a three-node cluster.` precedes it. All done: `All steps complete.` JSON: `{"step":N,"of":13,"part":P,"id":"…","title":"…","status":"todo|stale|complete","part1_complete":bool,"detail":["…"]}`. `scripts/progress.sh (done|skip) ID` appends `done|skip ID YYYY-MM-DD` to `.uc-progress`, refusing unknown ids (validated against `next.sh --list`).

- [ ] **Step 1: Failing fixture test** `template-tests/tutor.sh`:

```bash
#!/usr/bin/env bash
# Drives a generated project through every Part-1 transition by editing files
# and writing stamps directly — no cluster needed. Requires `make bins` to work
# (network) so step 1 can pass.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BASE="$HOME/scratch/uc_starter-gen/tutor"; rm -rf "$BASE"; "$HERE/gen.sh" "$BASE" >/dev/null
cd "$BASE/demo-app"
fail() { echo "FAIL: $*" >&2; scripts/next.sh >&2; exit 1; }
at() { scripts/next.sh --json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['id'], d['status'])"; }
expect() { local got; got="$(at)"; [ "$got" = "$1" ] || fail "expected '$1', got '$got'"; }
unmark() { sed -i '/TODO(app)/d' "$@"; }

[ "$(scripts/next.sh --list | tr '\n' ' ')" = "env skeleton concepts design commands state tests client failover snapshots observe upgrade deploy " ] || fail "--list"
# the doc and the checker agree on ids and order
[ "$(grep -oE '<!-- step: [a-z-]+ -->' WHAT-NEXT.md | sed 's/<!-- step: \(.*\) -->/\1/' | tr '\n' ' ')" = "$(scripts/next.sh --list | tr '\n' ' ')" ] || fail "WHAT-NEXT.md step ids differ from next.sh --list"

mv .uc .uc.away 2>/dev/null || true
expect "env todo"
mv .uc.away .uc 2>/dev/null || make -s bins >/dev/null
expect "skeleton todo"
scripts/stamp.sh skeleton;                      expect "concepts todo"
scripts/progress.sh done concepts;              expect "design todo"
unmark docs/app-design.md;                      expect "commands todo"
unmark src/commands.rs;                         expect "state todo"
unmark src/state.rs src/snapshot.rs;            expect "tests todo"
unmark tests/*.rs;                              expect "tests todo"   # no check stamp yet
scripts/stamp.sh check;                         expect "client todo"
unmark src/bin/client.rs scripts/demo.sh
scripts/stamp.sh demo;                          expect "failover todo"
# 9. an edit after the stamp is STALE, not done
echo "// touched" >> src/state.rs;              expect "tests stale"
sed -i '$d' src/state.rs;                       expect "failover todo"
scripts/stamp.sh failover;                      expect "snapshots todo"
scripts/next.sh | grep -q "Part 1 complete" || fail "no Part 1 banner"
grep -q "^done part1 " .uc-progress || fail "part1 completion not recorded"
# 11. after Part 1, code edits must not send the user back into Part 1
echo "// part 2 edit" >> src/state.rs;          expect "snapshots todo"
scripts/next.sh | grep -q "stale" || fail "stale Part-1 stamps should still be mentioned as a note"
# skips are recorded, and unknown ids refused
scripts/progress.sh skip snapshots;             expect "observe todo"
if scripts/progress.sh done nonsense 2>/dev/null; then fail "unknown id accepted"; fi
echo "tutor: PASS"
```

- [ ] **Step 2: Run** — FAIL (no `next.sh`).

- [ ] **Step 3: Implement.**

`scripts/progress.sh`:

```bash
#!/usr/bin/env bash
# progress.sh (done|skip) ID — record a step the repo cannot show, or a skip.
. "$(dirname "$0")/lib.sh"
verb="${1:-}"; id="${2:-}"
case "$verb" in done|skip) ;; *) die "usage: progress.sh (done|skip) STEP" ;; esac
[ -n "$id" ] || die "which step? e.g. make $verb STEP=concepts ('scripts/next.sh --list' lists ids)"
scripts/next.sh --list | grep -qx "$id" || [ "$id" = part1 ] || die "unknown step '$id' — ids: $(scripts/next.sh --list | tr '\n' ' ')"
echo "$verb $id $(date +%F)" >> "$PROGRESS"
echo "recorded: $verb $id"
```

`scripts/next.sh`:

```bash
#!/usr/bin/env bash
# next.sh — where am I on WHAT-NEXT.md? Computed from the repo, never remembered.
#   scripts/next.sh          human
#   scripts/next.sh --json   for agents
#   scripts/next.sh --list   step ids in order
. "$(dirname "$0")/lib.sh"
STEPS=(
  "env|1|Environment"
  "skeleton|1|Run the skeleton"
  "concepts|1|SMR in five minutes"
  "design|1|Design your app"
  "commands|1|Commands"
  "state|1|State, apply and query"
  "tests|1|Tests that cover your commands"
  "client|1|Client and demo for your commands"
  "failover|1|Kill the leader"
  "snapshots|2|Snapshots and purge"
  "observe|2|Observe the cluster"
  "upgrade|2|Your first FSM upgrade"
  "deploy|2|Deploy to three machines"
)
if [ "${1:-}" = --list ]; then for s in "${STEPS[@]}"; do echo "${s%%|*}"; done; exit 0; fi
JSON=0; [ "${1:-}" = --json ] && JSON=1

DETAIL=()
todo_in() { # 0 when no marker remains in the given paths
  local hits; hits="$(grep -rn 'TODO(app)' "$@" 2>/dev/null | cut -d: -f1,2)" || true
  [ -z "$hits" ] && return 0
  while IFS= read -r h; do DETAIL+=("TODO(app) at $h"); done <<<"$hits"
  return 1
}
stamp_check() { # id hash hint → 0 fresh, 1 missing, 2 stale
  case "$(stamp_state "$1" "$2")" in
    fresh) return 0 ;;
    missing) DETAIL+=("$3"); return 1 ;;
    stale) DETAIL+=("code changed since this was proven — $3"); return 2 ;;
  esac
}
check_env() {
  [ "$(uname -s)" = Linux ] || { DETAIL+=("not Linux ($(uname -s)): open the devcontainer — README.md § Devcontainer"); return 1; }
  command -v cargo >/dev/null || { DETAIL+=("cargo not found: install rustup (README.md § Prerequisites)"); return 1; }
  local v; v="$("$UC_BIN/uc2-node" --version 2>/dev/null)" || { DETAIL+=("ultima_cluster binaries missing: make bins"); return 1; }
  [[ "$v" == *"$UC_VERSION"* ]] || { DETAIL+=(".uc/bin has '$v' but UC_VERSION is $UC_VERSION: make bins"); return 1; }
}
check_skeleton() { stamp_check skeleton any "run: make up && make demo"; }
check_concepts() { progress_has concepts || { DETAIL+=("read WHAT-NEXT.md Step 3, then: make done STEP=concepts"); return 1; }; }
check_design()   { todo_in docs/app-design.md; }
check_commands() { todo_in src/commands.rs || return 1; cargo check -q 2>/dev/null || { DETAIL+=("cargo check fails — run it to see why"); return 1; }; }
check_state()    { todo_in src/state.rs src/snapshot.rs || return 1; cargo test -q --lib >/dev/null 2>&1 || { DETAIL+=("cargo test --lib fails"); return 1; }; }
check_tests()    { todo_in tests || return 1; stamp_check check "$(code_hash_with_tests)" "run: make check (tests + lint)"; }
check_client()   { todo_in src/bin/client.rs scripts/demo.sh || return 1; stamp_check demo "$(code_hash)" "run: make restart-services && make demo"; }
check_failover() { stamp_check failover "$(code_hash)" "run: make kill-leader"; }
check_snapshots(){ stamp_check snapshots any "run: make snapshot-drill"; }
check_observe()  { stamp_check observe any "run: make observe"; }
check_upgrade()  {
  grep -qE 'pack_version\(1, 0, 0\)' src/identity.rs && { DETAIL+=("FSM_VERSION is still 1.0.0: bump it in src/identity.rs for your v2 behaviour"); return 1; }
  stamp_check upgrade-check any "run: make corpus (before the change), then make upgrade-check" || return 1
  stamp_check upgrade-drill any "run: make upgrade-drill (asks before the one-way pin)"
}
check_deploy()   {
  ls dist/*.tar.gz >/dev/null 2>&1 || { DETAIL+=("run: make package HOSTS=ip0,ip1,ip2"); return 1; }
  progress_has deploy || { DETAIL+=("deploy it (docs/how-to/deploy.md), then: make done STEP=deploy"); return 1; }
}

json_str() { local s="${1//\\/\\\\}"; s="${s//\"/\\\"}"; printf '"%s"' "$s"; }
emit() { # n part id title status part1
  if [ $JSON = 1 ]; then
    printf '{"step":%s,"of":13,"part":%s,"id":"%s","title":%s,"status":"%s","part1_complete":%s,"detail":[' "$1" "$2" "$3" "$(json_str "$4")" "$5" "$6"
    local first=1 d; for d in "${DETAIL[@]}"; do [ $first = 1 ] || printf ','; json_str "$d"; first=0; done; printf ']}\n'
  else
    [ "$5" = complete ] && { echo "All steps complete."; return; }
    echo "Step $1/13 · Part $2 · $4 → WHAT-NEXT.md \"Step $1\""
    echo "  status: $5"
    local d; for d in "${DETAIL[@]}"; do echo "  - $d"; done
  fi
}

part1_done=0; progress_has part1 && part1_done=1
notes=()
n=0
for s in "${STEPS[@]}"; do
  n=$((n+1)); IFS='|' read -r id part title <<<"$s"
  progress_skipped "$id" && continue
  if [ "$part" = 1 ] && [ $part1_done = 1 ]; then
    # Part 1 is history once completed: report staleness as a note only.
    DETAIL=(); "check_$id" >/dev/null; [ $? = 2 ] && notes+=("note: Step $n ($title) is stale — ${DETAIL[*]}")
    continue
  fi
  DETAIL=(); "check_$id"; rc=$?
  if [ $rc != 0 ]; then
    status=todo; [ $rc = 2 ] && status=stale
    if [ "$part" = 2 ] && [ $part1_done = 0 ]; then
      echo "done part1 $(date +%F)" >> "$PROGRESS"; part1_done=1
      [ $JSON = 0 ] && echo "Part 1 complete — your app runs on a three-node cluster."
    fi
    DETAIL+=("${notes[@]}")
    emit "$n" "$part" "$id" "$title" "$status" "$([ $part1_done = 1 ] && echo true || echo false)"
    exit 0
  fi
done
DETAIL=("${notes[@]}")
emit 13 2 done "All steps complete" complete true
```

Note the Part-1-complete banner logic also covers a user who finishes step 9 and immediately runs `make next`: the first undone step is then `snapshots` (Part 2) and `part1` is recorded exactly once. In the fixture's step 11 the stale note is printed because `check_tests`/`check_client`/`check_failover` return 2 after the edit.

- [ ] **Step 4: Run** `bash template-tests/tutor.sh` → `tutor: PASS`. (It needs `WHAT-NEXT.md` with the 13 `<!-- step: id -->` markers and `docs/app-design.md` with at least one marker — create both now as stubs: `WHAT-NEXT.md` with 13 headings `### Step N — Title` each followed by `<!-- step: id -->`, and `docs/app-design.md` with one `<!-- TODO(app): fill in -->` line. Task 7 writes their content.)

- [ ] **Step 5: Watch it fail usefully** — comment out the `notes+=` line and re-run; expect FAIL at "stale Part-1 stamps should still be mentioned"; restore.

- [ ] **Step 6: Commit** — `git commit -m "feat: tutor checker — make next computes the step from the repo"`

---

### Task 7: `WHAT-NEXT.md` and `docs/app-design.md`

**Files:**
- Modify: `WHAT-NEXT.md` (full content), `docs/app-design.md` (full content)

**Interfaces:**
- Consumes: step ids/titles from `scripts/next.sh --list` (must stay identical — `tutor.sh` enforces it); Make targets from Tasks 5 and 8.

- [ ] **Step 1: Write `WHAT-NEXT.md`.** Top: a 6-line "How to use this file" (run `make next`; or ask your agent "what next?"; every step has the same six parts; `make skip STEP=id` to skip on purpose). Then `## Part 1 — your first working app`, `## Part 2 — to production`, and for each of the 13 steps:

```markdown
### Step N — <Title>
<!-- step: <id> -->

**Goal.** One sentence.

**Why.** 3–6 sentences on the UC concept, ending with a link pinned to
`https://github.com/PeterKnego/ultima_cluster/blob/v2.13.0/<path>`.

**Do it yourself.**
1. exact edit or command
2. …

**Ask the agent.** > "<the prompt a developer would type>"

**Done when.** The check `make next` runs, in words.

**Common mistakes.** 2–4 bullets.
```

Required content per step (the facts each *Why* and *Common mistakes* must carry; link targets are paths in the UC repo):

| step | Why must explain | link | common mistakes must include |
|---|---|---|---|
| env | nodes are Linux-only; binaries are verified against the release's SHA256SUMS (and cosign) | `docs/QUICKSTART.md` | running on macOS outside the devcontainer; a root under /tmp |
| skeleton | ten processes in four roles; start order nodes → services → gateways and why (`NodeBooting`) | `docs/QUICKSTART.md` §3 | port in use; starting a service before a leader exists |
| concepts | log of commands, same order everywhere, apply deterministic, positions as idempotency keys, sessions, what a snapshot is; the agent closes with 3 check questions (list them: "why can't apply read the clock?", "what does `replayed=true` mean?", "what is a position?") | `docs/notes/state-machine-replication-explained.md` | treating a query as if it went through consensus |
| design | commands vs queries; the response-size bound vs the 1344/1312 B ceiling; determinism hazards list | `docs/reference/state-machine-contract.md`, `docs/reference/limits.md` | unbounded responses (list/scan); putting wall-clock times in commands without saying whose clock |
| commands | append-only enum variants (variant index = wire tag); `validate()` | `docs/reference/state-machine-contract.md` | reordering variants; forgetting `validate` for new fields |
| state | `apply` rules; `ctx.time_ns`, `IdGen`; `last_applied` | `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md` | HashMap; `+` overflow (use `checked_`/`wrapping_`); panics in apply fail-stop the service |
| tests | what the determinism proptest proves and what it cannot (cross-version) | `docs/VERIFICATION.md` | not extending `arb_command` |
| client | gateways, redirects, `resend_on_unknown`, `replayed`; exit codes | `docs/how-to/run-a-gateway.md` | forgetting `make restart-services` after a rebuild |
| failover | election, commit quorum, why the write still lands | `docs/ARCHITECTURE.md` | reading a follower's state with a snapshot read and calling it stale data |
| snapshots | coordinated instants, exclusive frontier, purge OFF by default, install + tail replay | `docs/notes/uc2-cluster-fsm-explained.md` § Instants | expecting purge to show on a tiny write volume |
| observe | `/metrics` `/healthz` `/readyz`, `can_serve`, the alert rules in `.uc/packaging/prometheus` | `docs/how-to/monitor-a-cluster.md` | alerting on the leader flag |
| upgrade | `VERSION` bump, corpus + `intent.toml` + `upgrade-check`, the pin procedure; **the pin is a one-way door — no unpin; rollback is the pre-pin backup restored on every node** (bold, first line of Why) | `docs/how-to/upgrade-an-application.md`, `docs/how-to/diff-replay.md` | pinning before backing up; starting any new service before stopping all old ones; running `make restart-services` after bumping `FSM_VERSION` but before the pin (the new build is refused by name — use `make upgrade-drill`) |
| deploy | one node per machine, `[crypto]`/`[admin]` choices, systemd units | `docs/how-to/run-a-cluster.md` | all three nodes on one host and calling it HA |

- [ ] **Step 2: Write `docs/app-design.md`** — the fill-in design note, with these sections, each starting with a `<!-- TODO(app): … -->` line naming what to write, and each prefilled with the registry's answer so the skeleton is a worked example:
  1. What the app does (one paragraph).
  2. Commands — table: name, fields, what it changes, response, max encoded size.
  3. Queries — table: name, fields, answer, max size, linearizable or snapshot by default.
  4. State — the data structures and why they are deterministic (ordered maps, integer math).
  5. Determinism hazards considered — clock (`ctx.time_ns`), ids (`IdGen`), iteration order, floats, overflow.
  6. Size bounds — largest command and response vs the payload ceiling (1344 B crypto-off / 1312 B crypto-on at the baseline rung; 16 B session envelope).
  7. Snapshot — what the image contains, its version, when a change needs a new image version.
  8. Open questions.

- [ ] **Step 3: Run** `bash template-tests/tutor.sh` → PASS (ids still match). Proof-read `WHAT-NEXT.md` renders on GitHub (push to a scratch branch or preview with `gh markdown-preview` if installed).

- [ ] **Step 4: Commit** — `git commit -m "docs: WHAT-NEXT.md tutor path and the app design note"`

---

### Task 8: Part 2 drills

**Files:**
- Create: `scripts/snapshot-drill.sh`, `scripts/observe.sh`, `scripts/corpus.sh`, `scripts/upgrade-check.sh`, `scripts/upgrade-drill.sh`, `scripts/package.sh`, `upgrade/intent.toml.example`
- Modify: `Makefile` (append Part 2 targets), `tests/cluster.rs` (add drills test)

**Interfaces:**
- Consumes: `cluster.sh` commands, `stamp.sh`, `lib.sh`.
- Produces: Make targets `diffreplay`, `corpus`, `upgrade-check`, `snapshot-drill`, `observe`, `upgrade-drill`, `package HOSTS=a,b,c`. Stamps `snapshots`, `observe`, `upgrade-check`, `upgrade-drill`. `dist/<APP_NAME>-<ver>-<arch>.tar.gz`.

- [ ] **Step 1: Failing test** — append to `tests/cluster.rs`:

```rust
#[test]
fn snapshot_drill_and_observe() {
    let _down = Down;
    sh(&["scripts/cluster.sh", "up", "--fresh"]);
    sh(&["scripts/snapshot-drill.sh"]);
    sh(&["scripts/observe.sh"]);
}
```

(Both tests share the root and ports, so the Makefile's `test-cluster` runs with `--test-threads=1`: change it to `cargo test --release --features cluster-tests --test cluster -- --nocapture --test-threads=1`.)

- [ ] **Step 2: Run** `make test-cluster` → FAIL (scripts missing).

- [ ] **Step 3: Implement.**

`scripts/snapshot-drill.sh`:

```bash
#!/usr/bin/env bash
# snapshot-drill.sh — take a coordinated snapshot, SIGKILL one service, and
# watch it come back to the same state.
. "$(dirname "$0")/lib.sh"
C="$(dirname "$0")/cluster.sh"; CLI="$(app_bin_dir)/$APP_NAME"; GW="$(gateways_csv)"
l="$("$C" leader)" || die "no serving leader — run make up"
val="drill-$(date +%s)"
"$CLI" --gateways "$GW" put drill-key "$val" >/dev/null || die "write failed"
P="$("$C" snapshot | sed -n 's/^instant=//p')"; [ -n "$P" ] || die "uc2ctl snapshot gave no instant"
echo "1. coordinated snapshot instant P=$P (every node freezes its state at log position $P)"
for n in 0 1 2; do
  for _ in $(seq 1 150); do "$C" snapshot-show "$n" | grep -q "set=$P" && break; sleep 0.2; done
  "$C" snapshot-show "$n" | grep -q "set=$P" || die "node $n has no complete set at $P"
done
echo "2. all three nodes hold the complete set at P"
victim=$(( (l + 1) % 3 ))
"$C" kill service "$victim"; "$C" start service "$victim"
echo "3. SIGKILLed service $victim and restarted it — its in-memory state is gone"
ok=0; for _ in $(seq 1 150); do "$C" ctl "$victim" status 2>/dev/null | grep -qE 'attached=true.* lag=0' && { ok=1; break; }; sleep 0.2; done
[ $ok = 1 ] || die "service $victim did not catch up within 30s"
got="$("$CLI" --gateways "$GW" get drill-key --linearizable)"
[ "$got" = "value=\"$val\"" ] || die "read back '$got', want value=\"$val\""
echo "4. service $victim rebuilt its state (snapshot install + tail replay) and the value reads back"
grep -iE 'snapshot|install' "$ROOT/logs/service$victim.log" | tail -3 | sed 's/^/   log: /' || true
"$(dirname "$0")/stamp.sh" snapshots
echo PASS
```

Before trusting the `attached=true.* lag=0` regex, run `scripts/cluster.sh ctl 0 status` once and match the row line's real field order (`uc_ctl/src/main.rs:1034` prints `… applied={applied} lag={} …`); adjust the regex to the observed line and note it in a comment.

`scripts/observe.sh`:

```bash
#!/usr/bin/env bash
# observe.sh — what an operator watches: health, readiness, the key series.
. "$(dirname "$0")/lib.sh"
SERIES=(uc2_agent_alive uc2_is_leader uc2_commit_bytes uc2_fsm_lag_bytes uc2_service_version)
leaders=0
for n in 0 1 2; do
  m="http://127.0.0.1:$(METRICS_PORT "$n")"
  curl -fs -o /dev/null "$m/healthz" || die "node $n /healthz is not 200"
  curl -fs -o /dev/null "$m/readyz"  || die "node $n /readyz is not 200 (can_serve false?)"
  body="$(curl -fs "$m/metrics")" || die "node $n /metrics unreachable"
  for s in "${SERIES[@]}"; do grep -q "^$s" <<<"$body" || die "node $n exports no $s"; done
  l="$(grep -E '^uc2_is_leader(\{[^}]*\})? ' <<<"$body" | awk '{print $2}' | head -1)"
  [ "${l%%.*}" = 1 ] && leaders=$((leaders+1))
  echo "node $n: healthz ok, readyz ok, ${#SERIES[@]} key series present, is_leader=${l:-?}"
done
[ $leaders = 1 ] || die "expected exactly one uc2_is_leader=1, saw $leaders"
echo "alert rules to load into Prometheus: .uc/packaging/prometheus/uc2-alerts.yml"
"$(dirname "$0")/stamp.sh" observe
echo PASS
```

Verify each name in `SERIES` against one real `/metrics` scrape before committing; replace any that differ with the exported name (the list comes from `docs/how-to/monitor-a-cluster.md` at `v2.13.0`).

`scripts/corpus.sh`:

```bash
#!/usr/bin/env bash
# corpus.sh — capture a diff-replay corpus from the running cluster with the
# CURRENT (old) code, and keep that binary as the "old" side.
. "$(dirname "$0")/lib.sh"
set -e
DR="$PROJECT_DIR/.uc/cargo/bin/uc2-diffreplay"; [ -x "$DR" ] || die "run: make diffreplay"
C="$(dirname "$0")/cluster.sh"; CLI="$(app_bin_dir)/$APP_NAME"; GW="$(gateways_csv)"
P="$("$C" snapshot | sed -n 's/^instant=//p')"; [ -n "$P" ] || die "no instant"
for i in 1 2 3; do "$CLI" --gateways "$GW" put "corpus-$i" "v$i" >/dev/null; done
"$CLI" --gateways "$GW" delete corpus-2 >/dev/null
rm -rf upgrade/corpus upgrade/old; mkdir -p upgrade/old
"$DR" corpus export --instance-dir "$ROOT/n0" --app-id "$APP_ID" --row 0 --from "$P" --out upgrade/corpus
cp "$(app_bin_dir)/$APP_NAME-service" upgrade/old/
[ -f upgrade/intent.toml ] || cp upgrade/intent.toml.example upgrade/intent.toml
echo "corpus at upgrade/corpus (from P=$P); old binary at upgrade/old/; now change the code, bump FSM_VERSION, edit upgrade/intent.toml, run make upgrade-check"
```

`scripts/upgrade-check.sh`:

```bash
#!/usr/bin/env bash
. "$(dirname "$0")/lib.sh"
DR="$PROJECT_DIR/.uc/cargo/bin/uc2-diffreplay"; [ -x "$DR" ] || die "run: make diffreplay"
[ -d upgrade/corpus ] || die "no corpus — run make corpus BEFORE changing the code"
cargo build --release -q || exit 1
"$DR" upgrade --corpus upgrade/corpus --old "upgrade/old/$APP_NAME-service" --new "$(app_bin_dir)/$APP_NAME-service" \
  --declare upgrade/intent.toml --report upgrade/report.json
rc=$?
[ $rc = 0 ] && { "$(dirname "$0")/stamp.sh" upgrade-check; echo "PASS — every difference is declared and attributed"; } \
            || echo "FAIL — read upgrade/report.json (Undeclared / Unexplained / Absent); the upgrade-fsm skill explains each"
exit $rc
```

`upgrade/intent.toml.example`:

```toml
# What your change is SUPPOSED to change. uc2-diffreplay fails the run on any
# difference not declared here, and on any declaration it did not observe.
# Reference: https://github.com/PeterKnego/ultima_cluster/blob/v2.13.0/docs/how-to/diff-replay.md#3-declare-what-you-intend
tag_offset = 16   # skip the Sessioned envelope (client_id ‖ seq)

[tags]            # first byte of the bincode encoding = the enum variant index
"00" = "put"
"01" = "delete"

[touched]
arms = []         # e.g. ["put"] when you change what Put does
migration = false # true when the snapshot image format changes

# [[expect]]
# surface = "responses"
# arm = "put"
# note = "put now returns the new length"
```

Confirm the tag bytes: in a scratch test print `app::encode(&Command::Put{..})[0]` and `…Delete…[0]`; they must be `0x00` and `0x01`.

`scripts/upgrade-drill.sh`:

```bash
#!/usr/bin/env bash
# upgrade-drill.sh — the per-row upgrade procedure on the local cluster
# (upstream docs/how-to/upgrade-an-application.md §1–§7). THE PIN IS A ONE-WAY DOOR.
. "$(dirname "$0")/lib.sh"
C="$(dirname "$0")/cluster.sh"; CLI="$(app_bin_dir)/$APP_NAME"; GW="$(gateways_csv)"
new="$(sed -nE 's/.*pack_version\(([0-9]+), ([0-9]+), ([0-9]+)\).*/\1.\2.\3/p' src/identity.rs)"
old="$("$C" ctl 0 status | sed -nE 's/.*row=0 .*version=([0-9.]+).*/\1/p' | head -1)"
[ -n "$old" ] && [ -n "$new" ] || die "cannot read versions (running '$old', source '$new')"
[ "$old" != "$new" ] || die "running version is already $new — bump FSM_VERSION in src/identity.rs first"
[ "$(stamp_state upgrade-check any)" = fresh ] || die "run make upgrade-check first (diff-replay must PASS before a pin)"
cat <<EOF
About to upgrade row 0 from $old to $new on the local cluster at $ROOT.
After the pin commits there is NO unpin: the old binary is refused by name,
and the only way back is restoring the backups this script takes first.
EOF
if [ "${UC_CONFIRM_PIN:-}" != yes ]; then
  [ -t 0 ] || die "not a terminal and UC_CONFIRM_PIN != yes — refusing to pin"
  read -r -p "Type PIN to continue: " ans; [ "$ans" = PIN ] || die "not confirmed"
fi
export UC_CONFIRM_PIN=yes
set -e
"$CLI" --gateways "$GW" put upgrade-canary before >/dev/null
P="$("$C" snapshot | sed -n 's/^instant=//p')"; echo "1. origin instant P=$P"
for n in 0 1 2; do
  "$UC_BIN/uc2ctl" backup --instance-dir "$ROOT/n$n" --out "$ROOT/backups/n$n-pre-$new"
done; echo "2. backups in $ROOT/backups (your rollback point)"
for n in 0 1 2; do until "$C" snapshot-show "$n" | grep -q "set=$P"; do sleep 0.2; done; done
l="$("$C" leader)"
"$C" ctl "$l" upgrade pin --row 0 --to "$new" --origin "$P" --admin-key "$ROOT/admin.key"; echo "3. pinned row 0 to $new at $P"
for n in 0 1 2; do "$C" ctl "$n" status | grep -q "pinned=$new" || die "node $n does not show pinned=$new"; done
echo "4. every node shows the pin"
for n in 0 1 2; do "$C" stop service "$n"; done; echo "5. stopped every service"
cargo build --release -q
for n in 0 1 2; do "$C" start service "$n"; done; echo "6. started the new build everywhere"
for n in 0 1 2; do
  for _ in $(seq 1 150); do "$C" ctl "$n" status | grep -qE "row=0 .*version=$new" && break; sleep 0.2; done
  "$C" ctl "$n" status | grep -qE "row=0 .*version=$new" || die "node $n row 0 is not at $new"
done
[ "$("$CLI" --gateways "$GW" get upgrade-canary --linearizable)" = 'value="before"' ] || die "pre-upgrade value lost"
echo "7. every node runs $new and the pre-upgrade write reads back"
"$(dirname "$0")/stamp.sh" upgrade-drill
echo PASS
```

Before committing, check every `grep` pattern above against real `uc2ctl status` / `upgrade pin` output on the local cluster (the how-to shows `row=0 name=… version=1.0.0 … pinned=2.0.0`), and check `uc2ctl backup` accepts a running instance dir as the how-to's §1 implies; if it requires a stopped node, stop node+service+gateway around each backup and note why.

`scripts/package.sh`:

```bash
#!/usr/bin/env bash
# package.sh HOSTS=ip0,ip1,ip2 — a deploy bundle: binaries, systemd units,
# per-host node.toml/gateway.toml. See docs/how-to/deploy.md.
. "$(dirname "$0")/lib.sh"
set -e
IFS=, read -r -a H <<<"${HOSTS:?set HOSTS=ip0,ip1,ip2}"
[ "${#H[@]}" = 3 ] || die "HOSTS needs exactly three addresses"
cargo build --release -q
ver="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"; arch="$(uname -m)"
D="dist/$APP_NAME-$ver-$arch"; rm -rf "$D"; mkdir -p "$D/bin" "$D/systemd" "$D/hosts"
cp "$UC_BIN"/uc2-node "$UC_BIN"/uc2ctl "$UC_BIN"/uc2-gateway "$(app_bin_dir)/$APP_NAME-service" "$(app_bin_dir)/$APP_NAME" "$D/bin/"
cp .uc/packaging/systemd/uc2-node.service .uc/packaging/systemd/uc2-gateway.service "$D/systemd/"
sed "s|^ExecStart=.*|ExecStart=/usr/local/bin/%i --instance-dir /srv/uc2/$APP_NAME --app-id $APP_ID|" \
  .uc/packaging/systemd/uc2-service@.service > "$D/systemd/uc2-service@.service"
for i in 0 1 2; do
  mkdir -p "$D/hosts/${H[$i]}"
  # node.toml / gateway.toml: same keys as scripts/cluster.sh write_config, with
  # bind/addr on ${H[i]}:$(NODE_PORT i), instance_dir /srv/uc2/$APP_NAME,
  # [crypto] enabled = true, [admin] key_path /etc/uc2/admin/admin.key,
  # [metrics] bind 0.0.0.0:$(METRICS_PORT i). Generate with the same heredoc.
done
cp docs/how-to/deploy.md "$D/DEPLOY.md"
tar czf "$D.tar.gz" -C dist "$(basename "$D")"
echo "$D.tar.gz"
```

Implement the heredoc inside the loop by factoring `cluster.sh`'s `write_config` node/gateway heredocs into `lib.sh` functions `render_node_toml ID BIND_HOST INSTANCE_DIR CRYPTO ADMIN_KEY METRICS_HOST MEMBERS_HOSTS…` and `render_gateway_toml …`, used by both `cluster.sh` and `package.sh` — one source of truth for config shape. Crypto on for deploy requires the `[crypto]` key material keys documented in upstream `docs/how-to/encrypt-node-traffic.md`; render the keys section with placeholder paths the deploy how-to explains, and have `package.sh` print "edit hosts/*/node.toml [crypto] key paths before installing".

Makefile additions:

```make
diffreplay: ## install uc2-diffreplay for UC_VERSION into .uc/cargo
	cargo install uc_diffreplay --version $(UC_VERSION) --locked --root .uc/cargo
corpus: build ## capture a diff-replay corpus + keep the old binary (before changing code)
	scripts/corpus.sh
upgrade-check: ## diff-replay the corpus through old vs new builds
	scripts/upgrade-check.sh
snapshot-drill: ## snapshot, kill a service, watch it rebuild
	scripts/snapshot-drill.sh
observe: ## health, readiness and key metrics on every node
	scripts/observe.sh
upgrade-drill: ## the pinned upgrade on the local cluster (asks first: one-way door)
	scripts/upgrade-drill.sh
package: ## deploy bundle: make package HOSTS=ip0,ip1,ip2
	HOSTS=$(HOSTS) scripts/package.sh
```

- [ ] **Step 4: Run** `make test-cluster` → both tests PASS. Then by hand, once, the full Part-2 path in a generated project: `make up`, `make diffreplay corpus`, change `Put` to also upper-case nothing (a real behaviour change: e.g. trim whitespace in values), bump to `pack_version(1, 1, 0)`, declare `arms = ["put"]` + an `[[expect]] surface = "responses" arm = "put"`, `make upgrade-check` → PASS; then `UC_CONFIRM_PIN=yes make upgrade-drill` → PASS; `make package HOSTS=10.0.0.1,10.0.0.2,10.0.0.3` → a tarball. Record the transcript in `template-tests/part2-transcript.md` (evidence, ignored by the generator).

- [ ] **Step 5: Watch the pin guard fire** — `make upgrade-drill </dev/null` without `UC_CONFIRM_PIN` → exit 3 "refusing to pin"; `scripts/cluster.sh ctl 0 upgrade pin --row 0 --to 9.9.9 --origin 1` → exit 3.

- [ ] **Step 6: Commit** — `git commit -m "feat: Part 2 drills — snapshot, observe, diff-replay corpus/check, pinned upgrade, deploy bundle"`

---

### Task 9: Project docs

**Files:**
- Modify: `README.md`
- Create: `docs/concepts.md`, `docs/how-to/{add-a-command,add-a-query,change-the-state-shape,schedule-work,remove-sessions-or-snapshots,upgrade-uc,deploy,use-the-shmem-client}.md`, `docs/ai-engineering.md`, `docs/troubleshooting.md`

- [ ] **Step 1: Find the shmem-vs-remote evidence first.** In `ultima_cluster` at `v2.13.0`, search `docs/benchmarks/` for a doc measuring local `uc_client` vs remote `uc_remote` on the same rig (start with `uc2-m13-hop-bench-2026-08-24.md` and `uc2-service-time-2026-09-16.md`). Copy the exact figure, rig and doc path into `use-the-shmem-client.md`. If none compares them on one rig, write "not measured head-to-head on one rig" — never estimate.

- [ ] **Step 2: Write each doc** with these required contents (every upstream link pinned to `blob/v2.13.0/`):
  - `README.md`: UC in 3 sentences; Prerequisites (Linux + rustup, or Docker + VS Code/devcontainer CLI); the 15-minute path (`cargo generate --git https://github.com/PeterKnego/uc_starter` → `make bins up demo` → `make next`); § Devcontainer (open in container; the whole loop runs inside; macOS/Windows use this); § With an agent / without (ask "what next?" vs run `make next`); the Make target table (`make help` output); project map (the File Structure table's generated-project rows); ports and where state lives; link list to `WHAT-NEXT.md`, `docs/`.
  - `docs/concepts.md`: ≤ 2 pages: log + positions; deterministic apply (the hazard list); commands vs queries (linearizable vs snapshot reads); sessions (`replayed`); snapshots (instants, exclusive frontier, purge off by default); the payload ceiling; identity (`NAME`, `VERSION`) and why a change needs an upgrade. Each subsection ends with its upstream link.
  - `how-to/add-a-command.md`: the 6 edits in order (commands.rs variant at the END → `validate` → `apply` arm → client subcommand → `arb_command` + a unit test → `demo.sh` line) and then `make check restart-services demo`; if a cluster already ran the old version, this is a behaviour change → Step 12.
  - `how-to/add-a-query.md`: Query variant, `query` arm, client `get`-style subcommand with `--linearizable`, test.
  - `how-to/change-the-state-shape.md`: bump `IMAGE_VERSION`, keep reading the old image (show the match on version), bump `FSM_VERSION`, declare `migration = true` in intent.toml; O(1) freeze with a persistent map (`im::OrdMap`) and its cost trade-off, citing the kv example.
  - `how-to/schedule-work.md`: `ctx.schedule`/`ctx.cancel`, `on_timer`, `Timed<S>` for exactly-once; link `docs/how-to/schedule-work-in-a-service.md` upstream.
  - `how-to/remove-sessions-or-snapshots.md`: what breaks (re-send double-apply; unbounded journal, no below-floor catch-up), how to remove each (the service line, the gateway `[session] envelope = false`, `resend_on_unknown: false`).
  - `how-to/upgrade-uc.md`: `make uc-upgrade VERSION=x` (Task 12) then read the release's upgrade section; flag day = stop every node before starting any; one `snapshots/<row>/` wipe when the envelope changes.
  - `how-to/deploy.md`: `make package HOSTS=…`; per-host install steps (copy bins to `/usr/local/bin`, `/etc/uc2/node.toml`, systemd units, `systemctl enable --now uc2-node`, then services, then gateways); crypto + admin key generation; one node per machine; link upstream `run-a-cluster.md`, `encrypt-node-traffic.md`, `monitor-a-cluster.md`.
  - `how-to/use-the-shmem-client.md`: **What it provides** (same-host submission into the node's MPSC ingress ring; responses from the egress broadcast; no gateway hop); **What it requires** (a `uc_client::Engine`/`Client` binary on the node's host or in its container sharing the instance dir; `uc_client` dependency; handling `NotLeader` itself or running on every node; its own restart behaviour around node restarts; no session envelope unless you add one); **Pros/Cons** table; the evidence from Step 1; link upstream `examples/counter/src/bin/counter-client.rs` and `docs/how-to/write-a-service-binary.md`.
  - `docs/ai-engineering.md`: the loop (tutor → design note as spec → plan → implement → `determinism-reviewer` → `make check` → diff-replay before any `VERSION` bump); Claude Code kit (skills, subagent, hooks, permissions — what each does, and that `upgrade pin` always asks); other agents (AGENTS.md is the contract); how to add your own skill.
  - `docs/troubleshooting.md`: one section per named failure with symptom → cause → fix: `NodeBooting`; RAM-backed root; port in use; non-Linux host; checksum/cosign failure; `ULTSNAP1` refusal; mixed versions after `make uc-upgrade`; service exits "apply agent fail-stopped" (a panic in apply — read the log, fix, restart); decode error "client and service built from different code".

- [ ] **Step 3: Check links** — `grep -rhoE 'https://github.com/PeterKnego/ultima_cluster/blob/v2.13.0/[^) ]+' README.md WHAT-NEXT.md docs | sort -u | while read u; do p="${u#*v2.13.0/}"; p="${p%%#*}"; git -C ~/ultima/ultima_cluster cat-file -e "v2.13.0:$p" || echo "BROKEN $u"; done` → no `BROKEN` lines.

- [ ] **Step 4: Commit** — `git commit -m "docs: README, concepts, how-tos (incl. shmem client), AI engineering, troubleshooting"`

---

### Task 10: The AI kit

**Files:**
- Create: `AGENTS.md`, `CLAUDE.md`, `.claude/settings.json`, `.claude/hooks/post-edit.sh`, `.claude/skills/{next,add-command,determinism-review,upgrade-fsm,troubleshoot-cluster}/SKILL.md`, `.claude/agents/determinism-reviewer.md`, `template-tests/hook.sh`

**Interfaces:**
- Consumes: `scripts/next.sh --json`, `scripts/lint-determinism.sh FILE`, all Make targets.

- [ ] **Step 1: Failing hook test** `template-tests/hook.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
B="$HOME/scratch/uc_starter-gen/hook"; rm -rf "$B"; "$HERE/gen.sh" "$B" >/dev/null; cd "$B/demo-app"
fail() { echo "FAIL: $*" >&2; exit 1; }
payload() { printf '{"tool_name":"Edit","tool_input":{"file_path":"%s"}}' "$PWD/$1"; }
payload src/state.rs | CLAUDE_PROJECT_DIR="$PWD" .claude/hooks/post-edit.sh || fail "clean file flagged"
echo 'fn _h() { let _t = std::time::SystemTime::now(); }' >> src/state.rs
set +e; out="$(payload src/state.rs | CLAUDE_PROJECT_DIR="$PWD" .claude/hooks/post-edit.sh 2>&1)"; rc=$?; set -e
[ $rc = 2 ] || fail "hazard should exit 2, got $rc"
[[ "$out" == *"ctx.time_ns"* ]] || fail "message should name the substitute: $out"
sed -i '$d' src/state.rs
payload README.md | CLAUDE_PROJECT_DIR="$PWD" .claude/hooks/post-edit.sh || fail "non-rust file must pass"
python3 -m json.tool .claude/settings.json >/dev/null || fail "settings.json is not JSON"
echo "hook: PASS"
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement.**

`.claude/hooks/post-edit.sh`:

```bash
#!/usr/bin/env bash
# PostToolUse: format the edited Rust file and flag determinism hazards at the
# edit. Exit 2 feeds stderr back to the agent.
set -uo pipefail
input="$(cat)"
f="$(printf '%s' "$input" | sed -n 's/.*"file_path"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
case "$f" in *.rs) ;; *) exit 0 ;; esac
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
rel="${f#"$PWD"/}"
rustfmt --edition 2024 "$rel" 2>/dev/null || true
if ! out="$(scripts/lint-determinism.sh "$rel")"; then
  printf 'Determinism hazard in %s (apply must be identical on every replica):\n%s\n' "$rel" "$out" >&2
  exit 2
fi
exit 0
```

`.claude/settings.json`:

```json
{
  "permissions": {
    "allow": [
      "Bash(make:*)",
      "Bash(cargo build:*)",
      "Bash(cargo check:*)",
      "Bash(cargo test:*)",
      "Bash(cargo clippy:*)",
      "Bash(cargo fmt:*)",
      "Bash(scripts/next.sh:*)",
      "Bash(scripts/lint-determinism.sh:*)",
      "Bash(scripts/cluster.sh status:*)",
      "Bash(scripts/cluster.sh leader:*)"
    ],
    "ask": [
      "Bash(make upgrade-drill:*)",
      "Bash(scripts/upgrade-drill.sh:*)",
      "Bash(.uc/bin/uc2ctl:*)",
      "Bash(UC_CONFIRM_PIN=yes:*)"
    ]
  },
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Edit|Write|MultiEdit",
        "hooks": [{ "type": "command", "command": "\"$CLAUDE_PROJECT_DIR\"/.claude/hooks/post-edit.sh" }]
      }
    ]
  }
}
```

Confirm via the `claude-code-guide` agent (or Claude Code docs through context7) that `ask` rules take precedence over a matching `allow` rule (`Bash(make:*)` vs `Bash(make upgrade-drill:*)`); if not, drop `Bash(make:*)` from `allow` and list the safe targets individually. The script-level `UC_CONFIRM_PIN` guard (Task 5/8) is the backstop either way.

`AGENTS.md` — sections, in order, all mandatory:
  1. **What this is** — a UC application generated from uc_starter; UC version from `UC_VERSION`.
  2. **Map** — the File Structure rows for the generated project.
  3. **Commands** — `make help` table; "never start processes by hand — use `make up/down/restart-services`".
  4. **Hard rules** — verbatim list: determinism (the hazard list + substitutes); append-only enum variants; `validate()` for every size-bearing field; any change to what `apply`/`query` returns or stores → bump `FSM_VERSION` and run the Step-12 flow; **never run `uc2ctl upgrade pin`, `make upgrade-drill` or anything with `UC_CONFIRM_PIN=yes` without the developer's explicit go-ahead in this conversation — a pin is a one-way door**; never write cluster state under `/tmp`; never edit `.uc/`.
  5. **Evidence** — a step or task is done only when its check passes; show the command and its output; say "not verified" otherwise.
  6. **The tutor protocol** — when the developer asks "what next?", "where am I?", "help me continue" or similar: (1) run `scripts/next.sh --json`; never infer the step from memory or chat; (2) read that step in `WHAT-NEXT.md`; teach its **Why** in ≤ 6 sentences with its link; (3) ask exactly: "Do you want to do this yourself (I'll guide and review), or shall I do it?"; (4a) *guide*: give the next single action, wait, review their diff against the step's Common mistakes, repeat; (4b) *do it*: make the change, then walk the diff hunk by hunk naming the UC concept each hunk serves; (5) run the step's check (`scripts/next.sh`) and show its output; only then say the step is done and offer the next one. For Step 3 ask the three check questions from WHAT-NEXT.md and run `make done STEP=concepts` only after the developer answered. Never run `make skip` unless the developer asked to skip.
  7. **Upstream docs** — always the pinned `blob/v<UC_VERSION>/` links in `docs/concepts.md`; never `main`.

`CLAUDE.md`:

```markdown
@AGENTS.md

## Claude Code specifics

- `/next` (skill `next`) runs the tutor protocol above.
- Skills: `add-command`, `determinism-review`, `upgrade-fsm`, `troubleshoot-cluster` — use them for those tasks.
- Subagent `determinism-reviewer`: run it on every diff that touches `src/commands.rs`, `src/state.rs` or `src/snapshot.rs` before calling the work done.
- A PostToolUse hook formats edited Rust files and blocks determinism hazards at the edit; fix what it reports rather than suppressing it (`// determinism: ok <why>` only with the developer's agreement).
```

Skills (each `SKILL.md` has YAML frontmatter `name` + a `description` that says when to trigger; body ≤ 120 lines):
  - `next`: description "Use when the developer asks what to do next, where they are, or to continue the tutorial". Body = the tutor protocol (copy AGENTS.md §6 verbatim — a skill must stand alone), plus the JSON field meanings from `next.sh`.
  - `add-command`: trigger "adding or changing a command or query". Body: the 6 edits from `docs/how-to/add-a-command.md` with the file:function for each; then dispatch `determinism-reviewer`; then `make check restart-services demo`; then ask whether a cluster has run the old version (→ Step 12).
  - `determinism-review`: the checklist — clock, RNG, hash iteration, floats, overflow (`+` vs `checked_`/`wrapping_`), panics in apply, `IdGen` call-count changes, enum variant insertion/reorder, serde field reorder/rename, `#[serde(default)]` on new fields for image compatibility; output format `file:line — hazard — fix`.
  - `upgrade-fsm`: adapted from `ultima_cluster/.claude/skills/diff-replay-judge/SKILL.md` at `v2.13.0` — read that file and keep its five judgement steps (draft intent, classify, attribute residue, judge the origin delta, spot the hazards a lint cannot), rewritten for this project's paths (`upgrade/intent.toml`, `make corpus`, `make upgrade-check`, `upgrade/report.json`, tag bytes `00`/`01`, `tag_offset = 16`); ends with the one-way-door warning and "ask before `make upgrade-drill`".
  - `troubleshoot-cluster`: run `make status`; read `$ROOT/logs/<proc>N.log` tails (`scripts/cluster.sh root` prints ROOT); match the named refusal against `docs/troubleshooting.md` and quote the fix; never delete instance dirs without asking.

`.claude/agents/determinism-reviewer.md`:

```markdown
---
name: determinism-reviewer
description: Read-only reviewer for ultima_cluster state-machine diffs. Use after any change to src/commands.rs, src/state.rs or src/snapshot.rs, before declaring the work done.
tools: Read, Grep, Glob, Bash
model: sonnet
---
You review a diff of a replicated state machine. Every replica applies the same
commands in the same order and must reach bit-identical state; any divergence
is silent data corruption no consensus layer can detect.

Run `git diff HEAD -- src/` (or the diff you were given) and check each hunk
against the determinism-review checklist in .claude/skills/determinism-review/SKILL.md.
Also check: does the change alter what apply returns or stores for an existing
command? If yes, FSM_VERSION in src/identity.rs must be bumped and the upgrade
flow (WHAT-NEXT.md Step 12) is required.

Report `file:line — hazard — fix` lines, or exactly "No determinism hazards found."
Do not edit files. You may run `scripts/lint-determinism.sh --all` and `cargo test`.
```

- [ ] **Step 4: Run** `bash template-tests/hook.sh` → PASS. Then open the generated project in Claude Code and type "what next?": confirm the agent runs `scripts/next.sh --json` first and asks the guide-or-do question. Record the first reply in `template-tests/tutor-smoke.md`.

- [ ] **Step 5: Commit** — `git commit -m "feat: AI kit — AGENTS.md tutor protocol, Claude skills, reviewer subagent, edit hook, permissions"`

---

### Task 11: Devcontainer and containers

**Files:**
- Create: `.devcontainer/devcontainer.json`, `.devcontainer/Dockerfile`, `compose.yml`, `Dockerfile`, and a README § "Containers (optional)"

- [ ] **Step 1: Write `.devcontainer/Dockerfile`:**

```dockerfile
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm
ARG COSIGN_VERSION=2.4.1
RUN apt-get update && apt-get install -y --no-install-recommends jq iproute2 python3 \
 && rm -rf /var/lib/apt/lists/* \
 && arch="$(dpkg --print-architecture)" \
 && curl -fsSL -o /usr/local/bin/cosign "https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-linux-${arch}" \
 && chmod +x /usr/local/bin/cosign
USER vscode
RUN rustup toolchain install 1.96.0 --profile minimal --component rustfmt --component clippy \
 && rustup toolchain install 1.89.0 --profile minimal --component clippy \
 && cargo install cargo-generate --locked
```

Pin `COSIGN_VERSION` to the current release and verify its checksum from the sigstore release's `cosign_checksums.txt` in the same `RUN` (add the `sha256sum -c` line).

`.devcontainer/devcontainer.json`:

```json
{
  "name": "uc app",
  "build": { "dockerfile": "Dockerfile" },
  "features": { "ghcr.io/anthropics/devcontainer-features/claude-code:1": {} },
  "postCreateCommand": "make bins",
  "remoteUser": "vscode",
  "customizations": { "vscode": { "extensions": ["rust-lang.rust-analyzer", "anthropic.claude-code"] } }
}
```

Verify the Claude Code devcontainer feature id and VS Code extension id against current docs (claude-code-guide agent) before committing.

`Dockerfile` (app image for compose): multi-stage `rust:1.96-bookworm` builder → `cargo build --release` → `debian:bookworm-slim` with `/usr/local/bin/<service>` (use `ARG APP_NAME`, set by compose from `uc-app.env`).

`compose.yml`: adapt `ultima_cluster/packaging/compose.yml` at `v2.13.0` (keep its header comment on "DEMO TOPOLOGY, NOT A PRODUCTION ONE" and the `/etc/hosts` redirect caveat): three `uc2` node containers + three gateway containers from `ghcr.io/peterknego/uc2:${UC_VERSION}`, plus three `svcN` containers from this `Dockerfile`, each `svcN` mounting node N's instance-dir volume and depending on node N; `[services] names` set to the FSM name.

- [ ] **Step 2: Test the devcontainer** — `npm i -g @devcontainers/cli` (or use the CI action in Task 12), then in a generated project: `devcontainer up --workspace-folder . && devcontainer exec --workspace-folder . make bins test test-cluster` → PASS. On an aarch64 machine if available, the same; otherwise record "aarch64 devcontainer not exercised locally".

- [ ] **Step 3: Test compose** — `UC_VERSION=$(cat UC_VERSION) docker compose up -d --build`, then `docker compose run` the client against `gw0:…` per the header comment → a put/get round-trip; `docker compose down -v`.

- [ ] **Step 4: Commit** — `git commit -m "feat: devcontainer (the macOS/Windows path) and optional compose topology"`

---

### Task 12: CI and `make uc-upgrade`

**Files:**
- Create: `.github/workflows/ci.yml` (generated project's), `.github/workflows/template-ci.yml` (template's own), `scripts/uc-upgrade.sh`, `template-tests/uc-upgrade.sh`
- Modify: `Makefile` (`uc-upgrade` target)

**Interfaces:**
- Produces: `scripts/uc-upgrade.sh VERSION` — rewrites `UC_VERSION`, every `=X.Y.Z` pin of `uc_service|uc_remote|uc_protocol|uc_diffreplay` in `Cargo.toml`, and every `blob/vX.Y.Z/` and `releases/download/vX.Y.Z` in `*.md` and `.claude/**`; runs `cargo update -p uc_service …`; prints the release-notes and upgrade how-to links; exits non-zero if the version does not exist on crates.io (`cargo search`-free check: `curl -fs https://index.crates.io/uc/_s/uc_service | grep -q "\"vers\":\"$V\""`, with a `User-Agent`).

- [ ] **Step 1: Failing test** `template-tests/uc-upgrade.sh`: generate a project; run `scripts/uc-upgrade.sh 2.13.0` (idempotent → no diff); run it against a fake `9.9.9` with `UC_UPGRADE_SKIP_INDEX=1` and assert `UC_VERSION`, all four Cargo pins, and a sample of doc links now read `9.9.9`, and no `2.13.0` remains in `grep -rn '2\.13\.0' --include='*.md' --include='Cargo.toml' --include=UC_VERSION .`; run it with `9.9.9` and no skip flag → non-zero exit naming the missing version.

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** `scripts/uc-upgrade.sh` (sed over the listed files; `OLD="$(cat UC_VERSION)"`; escape dots) and the Makefile target `uc-upgrade: ## move to another UC release: make uc-upgrade VERSION=x` → `scripts/uc-upgrade.sh $(VERSION)`. **Step 4: Run** → PASS.

- [ ] **Step 5: Write `.github/workflows/ci.yml`** (generated project):

```yaml
name: ci
on: [push, pull_request]
jobs:
  lint-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: rustup toolchain install 1.89.0 --profile minimal --component clippy
      - run: make lint test
  cluster:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: make bins test-cluster
      - if: failure()
        run: tail -n 50 ~/.uc-starter/*/logs/*.log target/tmp/cluster-test/logs/*.log || true
  devcontainer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: devcontainers/ci@v0.3
        with:
          runCmd: make bins test-cluster
```

`.github/workflows/template-ci.yml`:

```yaml
name: template-ci
on:
  push:
  pull_request:
  schedule: [{ cron: "0 6 * * 1" }]   # weekly: against the latest crates.io UC
  workflow_dispatch:
permissions: { contents: read, issues: write }
jobs:
  template:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-generate }
      - run: rustup toolchain install 1.89.0 --profile minimal --component clippy
      - run: bash template-tests/generator.sh
      - run: bash template-tests/lint-determinism.sh
      - run: bash template-tests/hook.sh
      - run: bash template-tests/uc-upgrade.sh
      - name: generate + full project CI
        run: |
          D="$HOME/scratch/uc_starter-gen/ci"; rm -rf "$D"; template-tests/gen.sh "$D"
          cd "$D/demo-app"
          if [ "${{ github.event_name }}" = schedule ]; then
            latest="$(curl -fsA 'uc_starter-ci' https://index.crates.io/uc/_s/uc_service | tail -1 | sed -n 's/.*"vers":"\([^"]*\)".*/\1/p')"
            scripts/uc-upgrade.sh "$latest"
          fi
          make bins lint test test-cluster
      - run: bash template-tests/tutor.sh
      - if: failure() && github.event_name == 'schedule'
        env: { GH_TOKEN: "${{ github.token }}" }
        run: gh issue create --title "template-ci weekly: starter broken against latest ultima_cluster" --body "Run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
  devcontainer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-generate }
      - run: D="$HOME/scratch/uc_starter-gen/dc"; rm -rf "$D"; template-tests/gen.sh "$D"; echo "P=$D/demo-app" >> "$GITHUB_ENV"
      - uses: devcontainers/ci@v0.3
        with:
          subFolder: ${{ env.P }}
          runCmd: make bins test-cluster
```

If `devcontainers/ci` cannot take a folder outside the checkout, move the generated project under `$GITHUB_WORKSPACE/gen/` instead (and add `gen/` to the template's own `.gitignore`).

- [ ] **Step 6: Push and watch** — `git push origin main`; `gh run watch` on `template-ci`; all jobs green. Paste the run URL into the commit message of the next task.

- [ ] **Step 7: Commit** (before the push in Step 6) — `git commit -m "ci: generated-project CI, template CI with weekly latest-UC cron, make uc-upgrade"`

---

### Task 13: `ultima_cluster` side — release step and pointers

**Files (in `~/ultima/ultima_cluster`, branch `docs/uc-starter-spec`):**
- Modify: `docs/how-to/cut-a-release.md` (§7 "After"), `docs/BACKLOG.md` (deferred in-tree starter build job)

- [ ] **Step 1:** In `cut-a-release.md` §7 "After", add a numbered step: "**Bump `uc_starter`.** In a checkout of `PeterKnego/uc_starter`: `make uc-upgrade VERSION=<new>`, commit, push, wait for `template-ci` green, then `git tag v<new> && git push origin v<new>`. A release is not done until the starter generates, builds and passes its cluster smoke against it (spec `docs/superpowers/specs/2026-09-24-uc-starter-design.md` §8)."
- [ ] **Step 2:** In `docs/BACKLOG.md`, add under the open register: "**Build `uc_starter` against the in-tree crates in `ci.yml`** (`[patch.crates-io]`), so an SDK break is caught on the PR that makes it — deferred until `uc_starter` is public (a private checkout needs a secret). Spec §8."
- [ ] **Step 3:** Do NOT link `uc_starter` from README/QUICKSTART yet (it is private); add that as the last line of the BACKLOG entry: "on publication: link it from README.md, docs/QUICKSTART.md §7 and docs/tutorials/build-an-application.md".
- [ ] **Step 4:** Commit: `git commit -m "docs(release): uc_starter bump is part of every release; backlog the in-tree starter build"`.

---

### Task 14: Clean-room walkthrough and review

**Files:** `template-tests/clean-room-2026-MM-DD.md` (template repo; ignored by the generator)

- [ ] **Step 1:** Generate a project into a sandbox dir `$HOME/scratch/uc_starter-cleanroom/` with its own `.claude/settings.json` that adds `"deny": ["Read(~/ultima/**)"]` so the agent cannot read the UC repo (the method from the dogfood assessment — launch as `claude -p` from the sandbox dir so the sandbox settings apply).
- [ ] **Step 2:** Drive it with only "what next?" and, at each step, "do it for me" for half the steps and "I'll do it — guide me" (with you making the edits it suggests) for the other half, until `make next` prints "Part 1 complete". Record each turn's step id, what the agent did, whether it ran the check, and every friction point.
- [ ] **Step 3:** Pass bar (pre-committed): Part 1 completes; the agent ran `scripts/next.sh` at the start of every step and the step's check at the end of every step; it never ran a pin or `make skip` unasked; zero reads outside the sandbox (audit the transcript). A miss is recorded as FAIL with the friction list — fix, then re-run.
- [ ] **Step 4:** Run `review-branch`-style review of the whole template repo (dispatch a fresh reviewer on the most capable model with the spec + this plan), fix findings, re-run template CI.
- [ ] **Step 5:** Report to the user: CI run URL, clean-room verdict + transcript path, open friction; ask whether to make the repo public and tag `v2.13.0`. Do not change visibility or tag without an explicit yes.
