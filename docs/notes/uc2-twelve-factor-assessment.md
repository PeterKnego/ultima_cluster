# UC against the Twelve-Factor App — where it fits, where it deliberately does not

*Written 2026-08-30 against `v2.8.0`, grading the shipped binaries
(`uc2-node`, `uc2-gateway`, `uc2ctl`, a service binary) against the twelve
factors at <https://12factor.net/>. Each factor's own page was read, not just
the index; quotes below are verbatim from the pages. Repo evidence is cited by
path so the verdicts can be re-checked when something moves. Status: an
assessment, not a plan — the three follow-ups at the end are suggestions.*

## Why grade a consensus system against a web-app methodology

The twelve factors describe a stateless web tier that fronts someone else's
database. UC *is* the database tier. So the interesting result is not the
score; it is which factors UC fails **because it must** (a replicated log
whose durability lived in a "backing service" would just relocate the
consensus problem), which it fails **by choice** (config files instead of env
vars), and which it clears **more sharply than the page asks** (typed exit
codes that tell the supervisor whether a retry is worth it; an image built
from the signed release artifact rather than a second compile). Reading the
per-factor pages changed four verdicts from a first pass made against the
index alone — the pages carry concrete prohibitions ("never daemonize or
write PID files", "if the app needs to shell out to a system tool, that tool
should be vendored") that are checkable, and a definition of "release" that
UC does not have.

## The scorecard

| # | Factor | Verdict | Evidence |
|---|---|---|---|
| 1 | Codebase | **Pass** | One repo; 12 crates versioned in lockstep with the tag and image (`docs/how-to/cut-a-release.md`). The page's violation, "multiple apps sharing the same code", is avoided the way the page prescribes: the service SDK is a library dependency (`uc_service` on crates.io), not shared source. |
| 2 | Dependencies | **Pass** | `Cargo.lock` + `--locked` builds; `cargo-deny` in CI; the image is distroless with no shell. The page's extra rule — "if the app needs to shell out to a system tool, that tool should be vendored into the app" — is moot: `grep Command::new` over all nine shipped crates is empty. Java/`jq` are test-tier only (`scripts/elle_check.sh`). |
| 3 | Config | **Miss on mechanism, pass on the litmus test** | The page's rule is env vars; UC loads one TOML file per binary (`uc2-node --config`, `deny_unknown_fields`, validated before any agent starts — `docs/reference/configuration.md`). The only production-facing env var is `UC2_CLIENT_TIMEOUT_MS`; `UC2_ALLOW_VOLATILE_FS`, `UC2_MUTATION`, `UC2_TRUNC_TRACE` are test/dev switches (`configuration.md` "Environment switches"). But the page's *litmus test* — "whether the codebase could be made open source at any moment, without compromising any credentials" — passes: the repo is Apache-2.0 public, and `packaging/node.example.toml` holds key *paths* (`key_path`, `allowlist_path`, admin `keys[].key_path`), never key bytes; the loaders refuse group/world-readable key files. The page also says config that "does not vary between deploys … is best done in the code", which endorses the non-configurable `MTU_DEFAULT` and payload ceiling. |
| 4 | Backing services | **Vacuous** | The page defines one as "any service the app consumes over the network as part of its normal operation". UC consumes none: peers are the same app, the service and clients attach over shared memory in the instance directory (`docs/reference/instance-directory.md`), and Prometheus consumes *UC*. Where UC is on the other side of the relationship it fits the port-binding page's note that "one app can become the backing service for another app, by providing the URL … as a resource handle in the config" — a remote client's `--gateways gw0:9200,…` is exactly that. |
| 5 | Build / release / run | **Build: strong pass. Release stage: absent. Rollback: constrained.** | *Build* is immutable, signed and traceable: `packaging/Dockerfile` unpacks the checksummed, cosign-signed release tarball so `/usr/local/bin` is byte-identical to what `sha256sum -c` verified, with no toolchain and no network in the image build. But the page defines a *release* as build **plus** the deploy's config, with "a unique release ID" in "an append-only ledger" that "cannot be mutated once it is created". UC has no such artifact — config is a host file combined at run time — and rollback is "no rollback step beyond restarting the old binaries together" (`docs/how-to/upgrade-a-cluster.md`), because the node↔node wire and the `cnc.dat` layout are flag days. |
| 6 | Processes | **Opposed by design** | The page permits local memory/disk only as "a brief, single-transaction cache" and says "any data that needs to persist must be stored in a stateful backing service, typically a database." A node *is* its state: `journal/`, `state/` (vote, term map, floor, config record), `snapshots/`, all under one flock-guarded instance directory, with the service and clients pinned to the same host by shmem. This is the one factor UC should never try to satisfy. The stateless pieces — `uc2-gateway`, remote clients — do satisfy it. |
| 7 | Port binding | **Pass** | Node binds its UDP port, gateway its TCP port, and `[metrics]` opens its own HTTP listener (`uc_node::obs::http`) — no external web server is injected. Same-host IPC is files under the instance dir, not a `/dev/shm` discovery directory. |
| 8 | Concurrency | **Partial** | The page's hard rule — "processes should never daemonize or write PID files. Instead, rely on the operating system's process manager" — is met: the binaries neither daemonize nor write PID files (grep over `src/bin` and `node.rs` is empty), the systemd units are `Type=simple`, and `instance.lock` is a liveness flock, not a PID file. Process *types* are real (node / `uc2-service@<id>` / gateway; ≤ 8 FSMs per node since M14). What falls short is "adding more concurrency is a simple and reliable operation": horizontal scale is M7 membership change, one node at a time under quorum rules via signed `uc2ctl` requests — reliable, not simple. Only the gateway scales like a web tier. |
| 9 | Disposability | **Pass** | SIGTERM drains the archive to a bounded deadline (`--drain-timeout-secs`, default 5; `TimeoutStopSec=10` in `packaging/systemd/uc2-node.service`); a drain that cannot finish stops anyway and the restart re-fetches the tail. Startup refusals exit **2** and the unit sets `RestartPreventExitStatus=2` — a refused config is refused identically on every retry, so retrying only delays the operator seeing why; exit 1 (a port still held) is retried. The page's closing rule, "architected to handle unexpected, non-graceful terminations … crash-only design", is what the `uc_crashtest` tier tests literally: SIGKILL node + service mid-load, assert the history linearizable. The worker rule ("jobs … reentrant") maps to position-keyed idempotent apply and `Sessioned<S>` exactly-once. Cost: boot fallocates ~78 MiB and a restart converges by snapshot + tail-replay, so "fast" is bounded by the log gap, not constant. |
| 10 | Dev/prod parity | **Pass on tools and personnel; partial on time** | Same binaries and image locally (`packaging/compose.yml`, labelled a demo topology), on the fleet, and in CI's compose smoke. The page says "resist the urge to use different backing services between development and production"; the one divergence is `allow_volatile_fs` (tmpfs instance dirs for tests), warned on every boot. The page's time gap wants code "deployed hours or even just minutes later"; UC's August tags are days apart (`v2.3.0` 08-19, `v2.4.0` 08-20, `v2.5.0` 08-21, `v2.7.0` 08-26, `v2.8.0` 08-30) — a milestone cadence, and the flag-day upgrade rule means a deploy is a cluster-wide event rather than a rolling one. |
| 11 | Logs | **Pass with two deviations** | The page: "never concerns itself with routing or storage of its output stream … writes its event stream, unbuffered, to stdout". UC's structured stream is JSON lines, unbuffered (`uc_node/src/obs/log.rs` does `stderr().lock().write_all` per record, no `BufWriter`), level from `[log]`, no log files. Deviations: it goes to **stderr**, and the lifecycle lines in `uc_node/src/bin/uc2-node.rs` (`listening on`, `signalled, draining`, `drained, stopped cleanly`) go to **stdout** via `println!`, so a consumer must merge two streams; the gateway's 10 s stats line is a third format on stderr. `audit.jsonl` is not a log in the page's sense — it is fsync-per-record durable state, and the module docs say so. |
| 12 | Admin processes | **Pass** | `uc2ctl` ships in the same image at the same version (`--entrypoint /usr/local/bin/uc2ctl`), satisfying "same release … codebase and config"; `backup` / `verify-backup` / `restore` run offline against the instance dir; membership ops are HMAC-signed, audited one-shot requests. The page also wants a REPL "to run arbitrary code or inspect the app's models"; there is none — normal for a Rust binary, but a stated rule. |

**Tally: 7 pass (1, 2, 7, 9, 10, 11, 12 — with the deviations noted), 2
partial (5, 8), 1 miss by choice (3), 1 opposed by design (6), 1 vacuous (4).**

## What would move the score, and what should not

Three changes are cheap and would each close a real deviation:

1. **Env-var overrides for deploy-varying keys (#3).** `bind`, `[metrics]
   bind`, `[log] level`, the instance directory, `[[members]]`. That would
   let a container run from one immutable config without the busybox `init`
   render step `packaging/compose.yml` needs today. Key material should
   *stay* file-based: 0600-checked key files are a better secret posture
   than env vars, and `configuration.md` already documents why.
2. **One log stream (#11).** Route the `println!`/`eprintln!` lifecycle
   lines in `uc2-node.rs` (and the gateway's stats line) through
   `obs::log::emit`, and pick one of stdout/stderr for the whole stream.
3. **A release ledger (#5), if wanted.** A "release" in the page's sense is
   an image digest plus a config-file hash, recorded append-only. It is
   bookkeeping, not code; the rollback constraint underneath is structural.

Two things should not move:

- **#6.** A stateless node is a contradiction; the state *is* the product.
- **The flag-day upgrade rule** behind #5 and #10. Mixed-version clusters
  were rejected on safety grounds (a `0.4.0` peer's durable report is
  unattested and uncounted; a `0.5.0` `SNAP_BEGIN` sender is refused by
  name), and a rolling deploy that traded that for parity would be the
  wrong trade.

## Method notes

The per-factor pages were fetched raw and the quotes above checked against
the page text. Repo evidence: `docs/reference/{configuration,instance-directory,
semver-policy}.md`, `docs/how-to/upgrade-a-cluster.md`, `packaging/`,
`uc_node/src/bin/uc2-node.rs`, `uc_node/src/obs/log.rs`,
`uc_gateway/src/bin/uc2-gateway.rs`, `uc_ctl/src/main.rs`, and greps for
`Command::new`, daemonizing, and `std::env::var` across the shipped crates.
Not read for this note: the gateway's config loader end-to-end (its posture
is taken from `docs/reference/gateway-config.md`).
