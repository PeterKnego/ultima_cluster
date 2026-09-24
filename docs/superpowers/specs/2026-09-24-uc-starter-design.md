# `uc_starter` — the developer quick-start template, with an AI tutor

**Status:** design approved in conversation 2026-09-24; this document awaits
written review. **Target UC version:** `2.13.0` (wire `0.9.0`, cnc `3.3`).
**Deliverable:** a new repository, `PeterKnego/uc_starter` — **private** until
a first review, then made public — plus one step added to this repo's release
procedure (§8).

## 1. Intent

A developer who wants to build an application on ultima_cluster (UC) should
go from nothing to **their own state machine running on a local three-node
cluster** in one sitting, and learn UC while doing it. Today the path is:
read `docs/tutorials/build-an-application.md`, then copy and gut
`examples/kv` — a worked example, not a starting point, and one wired to the
in-tree path dependencies.

`uc_starter` is a `cargo generate` template that produces a complete,
building, testable UC application — FSM, service binary, remote client,
tests, scripts, docs — where the developer only fills in marked places. It
is **AI-engineering-enabled**: a developer can ask their coding agent "what
next?" and get the next step on a defined path to a working app, explained,
with the choice of doing it themselves under guidance or having the agent do
it and explain the change. Without an agent, the same path is a document
(`WHAT-NEXT.md`) and a command (`make next`).

**Success criteria**

1. `cargo generate` → `make bins up demo` → PASS on a fresh Linux host or in
   the devcontainer (macOS / Windows), with no edits.
2. From there, a developer following `WHAT-NEXT.md` (with or without an
   agent) reaches "Part 1 complete" — *their* commands on a local
   three-node cluster, surviving a leader kill — in roughly one sitting.
3. `make next` answers the same thing an agent answers, deterministically,
   from observable repo state — never from chat memory.
4. The generated project's lint, tests and cluster smoke are green in CI,
   natively and inside the devcontainer image.
5. A UC release is not done until the starter builds against it (§8).

**Non-goals (YAGNI, recorded so they are decisions, not gaps):** a non-Rust
client; an HTTP/JSON facade in front of the client; generator toggles for
optional features; first-class Cursor/Copilot/Codex config beyond
`AGENTS.md`; a `cargo xtask` rewrite of the shell tooling (the upgrade path
if `cluster.sh` outgrows bash); a Windows-native or macOS-native node (nodes
are Linux/Android-only since `2.12.0` and refuse other hosts by name).

## 2. Decisions taken in brainstorming

| # | question | decision |
|---|---|---|
| D1 | platforms | Linux native **and** a devcontainer as a first-class peer; both green in CI |
| D2 | client | remote client (`uc_remote` → `uc2-gateway`) only; docs explain the shmem client as an option |
| D3 | scaffolding | `cargo generate` template, repo also marked a GitHub template |
| D4 | FSM skeleton | production-shaped, always on: typed `StateMachine` + `Sessioned` + `SnapshotStateMachine` + diff-replay hooks |
| D5 | AI depth | Claude Code first-class (skills, subagent, hooks, settings); every other agent through `AGENTS.md` |
| D6 | repo | `PeterKnego/uc_starter`, private until reviewed |
| D7 | run modes | one Linux loop (Makefile + bash, adapted from `examples/kv/scripts/kvcluster.sh`) run natively or in the devcontainer; compose is an optional extra |
| D8 | tutor path | Part 1 ends at a working local cluster; Part 2 goes to production (snapshots, observability, first FSM upgrade, deploy) |

## 3. The template repository

The repo root **is** the template. `cargo-generate.toml` declares four
placeholders:

| placeholder | used for | validation |
|---|---|---|
| `project-name` | package name, binary names (`<name>-service`, `<name>`) | cargo-generate's own crate-name rules |
| `fsm_name` | the state machine's `const NAME` | a rhai pre-hook refuses the reserved `uc_` prefix and anything that is not `[a-z][a-z0-9_]*` (the `regex` crate has no look-ahead, so this cannot be a placeholder regex) |
| `app_id` | the `app_id` every IPC entry checks | non-empty, `[a-z0-9_-]+` |
| `base_port` | offset for node UDP, gateway TCP and metrics ports, so two generated apps coexist | integer; default chosen so the three bands do not collide with UC's quickstart (9200-9202) or `kvcluster.sh` (9300-9502) |

The template repo's own workflow, `.github/workflows/template-ci.yml`, is in
`[template] ignore`, so it never lands in a generated project; the generated
project gets its own `ci.yml` (§5.4). Because template syntax makes the raw
repo non-buildable, the template's CI always generates first (§5.4).

The repo is also marked a GitHub template for discoverability; its README
says to use `cargo generate`, since "Use this template" copies placeholders
unsubstituted.

## 4. The generated project

One package with a library and two binaries — the `examples/kv` shape. A
workspace would only spare the client from linking `uc_service`, which is not
worth the structure in a starter.

```
<name>/
├── UC_VERSION               2.13.0 — the ONE pin: crates and release binaries move together
├── Cargo.toml               uc_service, uc_remote, uc_diffreplay (default-features = false)
├── rust-toolchain.toml      clippy.toml   Makefile   .uc-progress
├── src/
│   ├── lib.rs               re-exports; a module map in its doc comment
│   ├── commands.rs          Command / Response / Query / QueryResponse      ← TODO(app)
│   ├── state.rs             the state + apply / query                       ← TODO(app)
│   ├── snapshot.rs          whole-state serde build / install (works unchanged)
│   └── bin/
│       ├── <name>-service.rs  subcommands run | replay | project
│       └── <name>.rs          remote CLI, one subcommand per Command         ← TODO(app)
├── tests/                   state.rs (unit)  determinism.rs (proptest)  snapshot.rs
│                            cluster.rs (smoke; --features cluster-tests)
├── scripts/                 fetch-uc.sh  cluster.sh  demo.sh (TODO(app))  next.sh
│                            lint-determinism.sh
├── config/                  node.toml.tmpl   gateway.toml.tmpl
├── .devcontainer/           devcontainer.json  Dockerfile
├── compose.yml  Dockerfile  (optional containers path)
├── README.md  WHAT-NEXT.md  AGENTS.md  CLAUDE.md
├── docs/                    §6
├── .claude/                 §7
└── .github/workflows/ci.yml
```

### 4.1 The state machine skeleton (D4)

The skeleton is a working application, so every path — write, read,
snapshot, replay — runs on the first `make up`: a replicated
`String → String` **registry** with commands `Put { key, value }` and
`Delete { key }` and query `Get { key }`, over a `BTreeMap`. The `BTreeMap`
is deliberate and commented: `HashMap` iteration order is not deterministic,
and that hazard is the first thing the determinism docs point at in live
code.

- **Typed tier.** `impl StateMachine for <Fsm>` with `const NAME =
  "{{fsm_name}}"` and `const VERSION = 1`, serde commands, `apply(&mut self,
  ctx: &mut ApplyCtx, cmd)` and `query`. The typed tier's decode is
  fail-stop on a payload it does not consume whole (#49, `decode_exact`) —
  if that lands in a release after `2.13.0`, the starter picks it up at the
  next `make uc-upgrade`, not before.
- **Sessions.** The service runs `Sessioned::new(sm, SessionConfig::default())`,
  so a remote client's retried write applies exactly once. A remote client
  that retries *without* sessions is exactly the bug sessions exist to
  prevent, which is why this is not optional.
- **Snapshots.** `SnapshotStateMachine` with freeze/stream/install that
  serialize the whole state with serde, and the service starts with
  `start_with_snapshots()`. Correct for any serde state; the how-to explains
  when to replace it (large state, O(1) freeze).
- **Diff-replay hooks.** The service binary embeds `uc_diffreplay::drive`
  behind `replay` and `project` subcommands, exactly as
  `examples/kv/src/bin/kv-service.rs` does, so the first FSM upgrade (Part 2,
  step 12) needs no rewiring.

### 4.2 Fill-in markers

Every place the developer is expected to change carries
`// TODO(app): <one-line instruction>`. `make todo` lists them with file and
line. The tutor's checks (§5.2) count them per file; an agent greps them.
"What do I change?" therefore has a mechanical answer.

### 4.3 Determinism guardrails

- `clippy.toml` `disallowed-methods` / `disallowed-types`:
  `std::time::SystemTime::now`, `std::time::Instant::now`, `rand::*`
  entry points, `std::collections::HashMap` / `HashSet` in the state module
  (scoped by an `#![allow]` in the binaries, which legitimately need a clock),
  each with a message naming the replicated substitute (`ctx.time_ns`,
  `IdGen`, `BTreeMap`).
- `scripts/lint-determinism.sh <file>` — a fast grep-level check of the same
  hazards, used by the agent hook (§7) where full clippy is too slow.
- `tests/determinism.rs` — a proptest: two fresh state machines fed the same
  generated command sequence reach equal state and emit equal responses;
  plus snapshot → install → continue equals uninterrupted apply.

## 5. Tooling

### 5.1 Scripts and Make targets

**`scripts/fetch-uc.sh`** (`make bins`): reads `UC_VERSION` and the host
arch, downloads `uc2-<ver>-<arch>-unknown-linux-gnu.tar.gz` from the UC
GitHub release into `.uc/bin/`, verifies it against the release's
`SHA256SUMS` always and with `cosign verify-blob` when cosign is on `PATH`
(the devcontainer installs it). The `v2.13.0` release carries x86_64 and
aarch64 tarballs, each with `.sha256` and `.sigstore.json`, plus a signed
`SHA256SUMS` — aarch64 is what an Apple-silicon devcontainer needs. A
non-Linux host gets a named refusal pointing at the devcontainer.

**`scripts/cluster.sh`**, adapted from `examples/kv/scripts/kvcluster.sh`:
`up [--fresh] | down | status | leader | kill|stop|start <role> N |
snapshot | ctl N …`. Cluster state lives under `$HOME/.uc-starter/<name>/`;
a root under `/tmp` or `/dev/shm` is refused, as UC's own quickstart refuses
it. Start order is **every node, then every service, then every gateway**
(`2.13.0`'s attach gates on the node having joined; a service attached early
waits `boot_wait` and then refuses `NodeBooting`).

| target | does |
|---|---|
| `make next` (`--json`) | the tutor's progress check (§5.2) |
| `make bins` / `build` | fetch UC binaries / `cargo build --release` |
| `make up` / `down` / `status` / `restart-services` | 3 nodes + 3 services + 3 gateways; restart only the app after a rebuild |
| `make demo` | `scripts/demo.sh` — the client against the gateways; writes the demo stamp on success |
| `make kill-leader` | the failover exercise |
| `make test` / `test-cluster` | unit + proptest + snapshot / cluster smoke |
| `make lint` | fmt check + clippy `-D warnings` + **MSRV 1.89 clippy** in a private target dir |
| `make todo` | remaining `TODO(app)` markers |
| `make snapshot-drill` / `observe` / `upgrade-drill` / `package` | Part 2 exercises (§5.2) |
| `make corpus` / `upgrade-check` | export a diff-replay corpus / run `uc2-diffreplay upgrade` on it |
| `make done STEP=x` / `skip STEP=x` | record a step the repo cannot show, or a deliberate skip |
| `make uc-upgrade VERSION=x` | move `UC_VERSION`, the crate pins and the pinned doc links together, then print UC's upgrade how-to link — every UC minor so far has been a flag day |

The MSRV clippy is in `make lint` because 1.89's clippy fires lints the
pinned newer toolchain does not; UC's own PR #56 went red that way.

### 5.2 The tutor path and `make next` (D8)

`scripts/next.sh` walks the steps in order, runs each step's **done-when**
check, and prints the first step that is not done — or is **stale**:

```
Step 6/9 · Part 1 · State, apply and query → WHAT-NEXT.md#6-state-apply-and-query
  3 TODO(app) left: src/state.rs:12,30,44
```

`--json` emits the same as `{part, step, id, title, anchor, status,
detail[]}` for agents.

**Stamps.** A step the repo cannot show by its files alone (a demo ran, a
failover survived) writes `.uc/state/<id>.ok` containing a hash of `src/`
and `Cargo.lock`. If the code changes afterwards the step reads **stale —
re-run `make demo`**, not falsely green. `.uc/` is git-ignored (it holds
binaries and machine-local stamps).

**`.uc-progress`** is committed: one line per `make done` / `make skip`, so
a teammate's clone sees the same concept-step and skip decisions. A skip is
only ever recorded by `make skip`; nothing skips silently.

**Part 1 — your first working app**

| # | id | step | done when |
|---|---|---|---|
| 1 | `env` | Environment | Linux (or devcontainer), pinned toolchain present, `.uc/bin` binaries verified for `UC_VERSION` |
| 2 | `skeleton` | Run the skeleton | demo stamp exists (any `src/` hash — this step proves the environment, not your code) |
| 3 | `concepts` | SMR in five minutes | `make done STEP=concepts` (the agent closes the step with three short check questions) |
| 4 | `design` | Design your app in `docs/app-design.md` — commands, queries, state, determinism hazards, response-size bound against the payload ceiling | no `TODO(app)` in it |
| 5 | `commands` | Commands (`src/commands.rs`) | no `TODO(app)` there; `cargo build` succeeds |
| 6 | `state` | State, apply and query (`src/state.rs`) | no `TODO(app)` there; `cargo test --lib` passes |
| 7 | `tests` | Unit + determinism tests cover your commands | no `TODO(app)` in `tests/`; `make test` and `make lint` pass |
| 8 | `client` | Client CLI + `scripts/demo.sh` for your commands | no `TODO(app)` in the client or demo; a **fresh** demo stamp |
| 9 | `failover` | Kill the leader, watch it survive | failover stamp (`make kill-leader` then a successful demo) → **Part 1 complete** |

Step 4 is where AI engineering starts to pay: the filled design note is the
spec the agent implements steps 5–8 from, so "you do it" builds what the
developer decided, not what the agent guessed.

**Part 2 — to production**

| # | id | step | done when |
|---|---|---|---|
| 10 | `snapshots` | Turn purge on, take an instant, wipe one service's state, watch it rejoin by install | `make snapshot-drill` stamp |
| 11 | `observe` | `/metrics`, `/readyz`, the alert rules | `make observe` stamp (scrapes each node and checks the key series exist) |
| 12 | `upgrade` | First FSM upgrade: v2 behaviour, bump `VERSION`, write `intent.toml`, `make corpus` + `make upgrade-check`, then the pin on the local cluster | diff-replay PASS report + `make upgrade-drill` stamp. The step opens with a warning: **a pin is a one-way door** — no unpin, rollback is a pre-upgrade backup restored on every node |
| 13 | `deploy` | Deploy to three machines | `make package` succeeded (tarball + systemd units + per-host `node.toml`) and `make done STEP=deploy` |

Template CI asserts that the step ids in `WHAT-NEXT.md`'s headings equal
`next.sh`'s list, so the document and the checker cannot drift.

### 5.3 Devcontainer and containers (D1)

`.devcontainer/`: Debian base; rustup with the pinned toolchain and 1.89 for
the MSRV gate; cosign; `cargo-generate`; Claude Code;
`postCreateCommand: make bins`. The whole loop — edit, build, cluster,
client, agent — runs inside it, so the gateway-redirect hostname caveat
never arises there.

`compose.yml` + `Dockerfile` (optional): nodes and gateways from
`ghcr.io/peterknego/uc2:<UC_VERSION>`, the app's service from an image this
Dockerfile builds, sharing each node's instance-dir volume (a service
attaches through shared memory, so it must share the node's volume). Its
README section carries UC's own caveat: a gateway's REDIRECT names come from
`[[members]]`, so a host-side client needs `/etc/hosts` entries or must dial
the published ports and accept an unreachable redirect.

### 5.4 CI

- **Generated project, `ci.yml`:** `make lint test test-cluster` on
  ubuntu-latest natively; `make test-cluster` again inside the devcontainer
  image (`devcontainers/ci`), so both run paths are exercised.
- **Template repo, `template-ci.yml`:** `cargo generate` with fixed answers,
  then the generated project's full CI; plus the tutor's regression tests —
  `make next` on a fresh project reports step 1 (or 2 once `bins` ran), and
  on a checked-in pre-completed fixture reports "Part 1 complete"; plus the
  heading/step-id equality check. Weekly cron: the same against the latest
  crates.io UC version (§8).

## 6. Documentation (in the generated project)

Short, and pointing upstream for depth. Upstream links are **pinned to the
tag** (`https://github.com/PeterKnego/ultima_cluster/blob/v<UC_VERSION>/…`;
the UC repo is public) and `make uc-upgrade` rewrites them, so an agent never
reads `main`'s docs for a different wire version.

- `README.md` — UC in three sentences; the fifteen-minute path; "with an
  agent / without one".
- `WHAT-NEXT.md` — the path of §5.2. Every step: **Goal · Why** (the UC
  concept, linked upstream) **· Do it yourself** (exact edits, commands)
  **· Ask the agent** (the prompt) **· Done when · Common mistakes**.
- `docs/app-design.md` — the fill-in design note (step 4).
- `docs/concepts.md` — five minutes of SMR, determinism, positions, sessions,
  snapshots and the payload ceiling; each links
  `docs/notes/state-machine-replication-explained.md` and the relevant
  upstream reference.
- `docs/how-to/`: `add-a-command`, `add-a-query`, `change-the-state-shape`
  (snapshot compatibility, when to bump `VERSION`), `schedule-work`
  (`ctx.schedule`, `Timed`), `remove-sessions-or-snapshots` (and why not to),
  `upgrade-uc`, `deploy`, and **`use-the-shmem-client`**: what it provides
  (same-host submission straight into the node's ingress ring, no gateway
  hop), what it requires (a `uc_client::Engine` binary on the node's host or
  in its container, sharing the instance dir; an `Engine` window instead of
  the remote credit protocol; its own lifecycle around node restarts), and
  its pros and cons — with any throughput/latency difference **cited from a
  UC gate or benchmark doc**, not asserted.
- `docs/ai-engineering.md` — the workflow as a whole: tutor mode;
  spec-first through `app-design.md`; plan → implement → determinism review
  → diff-replay before any `VERSION` bump; how Claude Code and other agents
  each plug in.
- `docs/troubleshooting.md` — the named refusals a newcomer meets, each with
  its fix: `NodeBooting`, a RAM-backed root, a port in use, a non-Linux host,
  `ULTSNAP1`, a mixed-version cluster after `make uc-upgrade`.

## 7. The AI kit (D5)

**`AGENTS.md`** — read by every agent:

- the project map and the Make targets;
- **hard rules**: no ambient clock, RNG or `HashMap` iteration in `apply`
  (use `ctx.time_ns`, `IdGen`, `BTreeMap`); a response must fit the payload
  ceiling; any change to what `apply` does bumps `VERSION` and runs `make
  upgrade-check`; **never run `uc2ctl upgrade pin` (or `make upgrade-drill`)
  without the developer's explicit go-ahead** — a pin is a one-way door;
- evidence before claims: a step is done only when its check passes;
- **the tutor protocol**: on "what next?" (or similar) — (1) run `make next`,
  never infer the step from memory; (2) teach the step's *Why* in a few
  sentences with the upstream link; (3) ask **"do you want to do it (I'll
  guide and review), or shall I?"**; (4) *guide*: small hints, review their
  diff — *do it*: make the change, then walk the diff explaining the UC
  concept behind each part; (5) finish only by running the step's check, then
  offer the next step. A skip is recorded with `make skip`, never implied.

**`CLAUDE.md`** — `@AGENTS.md` plus the Claude-specific kit below.

**`.claude/skills/`**

| skill | does |
|---|---|
| `next` | the tutor protocol, invoked by `/next` or "what next?" |
| `add-command` | enum → apply → client subcommand → tests → demo, then the determinism reviewer |
| `determinism-review` | the hazard checklist (clock, RNG, hash iteration, floats, `ids()` call count, enum/field order) against a diff |
| `upgrade-fsm` | UC's `diff-replay-judge` steps adapted to an app: draft `intent.toml`, classify, attribute unexplained residue to a hunk, judge the state delta at the origin |
| `troubleshoot-cluster` | read `make status`, the per-process logs and the named refusal; link the fix in `docs/troubleshooting.md` |

**`.claude/agents/determinism-reviewer.md`** — a read-only reviewer
subagent, called by `add-command` and `upgrade-fsm` before either declares
done.

**`.claude/settings.json`**

- `permissions.allow`: `make *`, `cargo build|test|clippy|fmt *`,
  `scripts/*`;
- `permissions.ask`: any command containing `upgrade pin`, and
  `make upgrade-drill` — the one-way door needs a human click even in an
  auto mode;
- a PostToolUse hook on `Edit|Write` of `*.rs`: `cargo fmt` on the file and
  `scripts/lint-determinism.sh` on it, surfacing a hazard at the edit rather
  than at `make lint`.

The devcontainer ships Claude Code; other agents work from `AGENTS.md`
alone.

## 8. Drift guard and versioning

- **Tags follow UC.** `uc_starter` tag `v2.13.0` generates a project for UC
  `2.13.0`; `cargo generate … --tag v<X>` pins it; `main` tracks the newest
  UC release. An older-UC user generates from the older tag — necessary,
  since every UC minor so far has been a wire flag day.
- **UC's release procedure gains a step.** `docs/how-to/cut-a-release.md`
  §7 "After" gets: in `uc_starter`, run `make uc-upgrade VERSION=<new>`, get
  its template CI green, tag it `v<new>`. A UC release is not done until the
  starter builds against it.
- **Weekly cron** in `template-ci.yml` generates against the latest
  crates.io UC version and opens an issue on failure — the backstop for
  anything the release step missed.
- **Deferred:** a job in UC's own `ci.yml` that builds the starter against
  the in-tree crates via `[patch.crates-io]`, so an API break is caught on
  the UC PR that makes it. It needs a checkout secret while `uc_starter` is
  private, so it waits for publication.

## 9. Testing the starter itself

- Template CI (§5.4) is the integration test: generate → lint → test →
  cluster smoke, natively and in the devcontainer.
- `next.sh` has fixture tests: fresh project, each Part-1 step completed in
  turn (fixture diffs), a stale stamp after a `src/` edit, a `make skip`.
- A **clean-room walkthrough** before publication: a fresh agent session,
  denied read access to the UC repo, starting from the generated project,
  told only "what next?" repeatedly until Part 1 completes — the same
  clean-room method the dogfood assessment used (#16). Its transcript and
  friction list are the review evidence for making the repo public.

## 10. Open items for the implementation plan

- The exact `base_port` default and band layout (§3).
- Whether `make observe` needs a bundled Prometheus, or only `curl` against
  `/metrics` (lean: `curl` only; Grafana/Prometheus stay in UC's `packaging/`).
- The `use-the-shmem-client` how-to's cited numbers: find the gate or
  benchmark doc that measured local-vs-remote on the same rig; if none
  exists, the page says so rather than estimating.
