# M12c — Packaging, publishing, hygiene: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A stranger with no Rust toolchain downloads a signed release, verifies it, and runs a three-node cluster with a gateway and a remote client from the artifacts alone; the crates carry a real version identity and a stated semver promise; the supply chain is gated.

**Architecture:** Lockstep `workspace.package.version = "2.6.0"` with every internal path dep versioned and publish metadata added; `rust-version` + pinned toolchain + an `msrv` CI job; `deny.toml` + a `deny` CI job (advisories, licenses, bans incl. no second AES-GCM implementation) and the dead `quinn`/`rustls`/`rcgen`/`rustls-pemfile`/workspace-`tokio`/`futures` deps removed; a new `counter-remote` example binary (the remote twin of `counter-client`) so the release has a real remote client; one `packaging/quickstart-local.sh` that stands up 3 nodes + 3 counter services + 1 gateway on one host from a bin dir and submits through the gateway — used by users and, unchanged, by the `release-smoke` job inside a bare `ubuntu:24.04` container; `release.yml` on `v*` tags (native `x86_64` + `aarch64` runners, `--release --locked`, strip, tarballs + `SHA256SUMS` + CycloneDX SBOM, cosign keyless, GitHub Release, `ghcr.io/peterknego/uc2:<ver>` on distroless with a compose smoke); a `publish-check` CI job (package all 12, dry-run the DAG leaves); docs: artifacts-first QUICKSTART, `semver-policy.md`, `cut-a-release.md`, gate rows; `cargo fmt` **deferred** (two long-lived worktrees are open — spec §6's own condition).

**Tech Stack:** cargo workspace (edition 2024), GitHub Actions (`ubuntu-latest`, `ubuntu-24.04-arm`), `sigstore/cosign-installer`, `cargo-cyclonedx`, `EmbarkStudios/cargo-deny-action`, `softprops/action-gh-release`, `docker/build-push-action`, `gcr.io/distroless/cc-debian12`, bash + `shellcheck`.

**Spec:** `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §3.2 and §6 (read both first); §8 gate rows; production-readiness §7.

## Global Constraints

- No consensus/wire/cnc change; `version::CURRENT` (wire 0.5.0) and the cnc page version are untouched by the crate version bump. No code behaviour change except the new `counter-remote` bin and `uc2-node --version`/build-info strings if any embed `CARGO_PKG_VERSION` (grep; keep them truthful).
- `cargo clippy --workspace --all-targets -- -D warnings` (+ `-p uc_service --features ultima_db`, `--features apply-profile`, `-p uc_gateway --features test-util`) clean and `cargo test --workspace --exclude uc_node` + the `uc_node` fast set green after every task; `Cargo.lock` committed.
- `--locked` everywhere a release artifact is built; the lockfile must resolve with the pinned toolchain and with `rust-version`.
- Tests/scratch under `CARGO_TARGET_TMPDIR` or `~/.cache`, never `/tmp`; the quickstart script refuses an instance-dir root under `/tmp` (it is where a stranger would put it — say why).
- Secrets: none in the repo; `release.yml` uses only `GITHUB_TOKEN` + OIDC (`id-token: write`); the crates.io publish is MANUAL (maintainer token) and documented, never in CI.
- Dev box is not a bench; nothing here is a perf number. `docker`, `cosign`, `cargo-deny`, `cargo-cyclonedx` are NOT installed locally — tasks that need them either install them (`cargo install cargo-deny cargo-cyclonedx` is fine; `cosign` via its GitHub release binary; `docker` is NOT installable here) or state "validated in CI only" explicitly in the report and the gate doc.
- The first real run of `release.yml` requires a pushed tag — that is a USER step (`v2.6.0-rc.1`); the plan validates everything that can be validated locally and says so.

## File structure

| Path | Responsibility |
|---|---|
| `Cargo.toml` (root) + every member `Cargo.toml` | version/metadata/publish flags/path-dep versions; dead deps gone |
| `rust-toolchain.toml`, `deny.toml`, `.github/workflows/ci.yml` | pin, supply-chain policy, `msrv`/`deny`/`publish-check` jobs |
| `examples/counter/src/bin/counter-remote.rs` | remote client example over `uc_remote::RemoteClient` |
| `packaging/quickstart-local.sh` | the one local 3-node+gateway+remote-submit script (users + release-smoke) |
| `packaging/Dockerfile`, `packaging/compose.yml` | image + single-host compose demo |
| `.github/workflows/release.yml`, `scripts/release_smoke.sh` | the release pipeline and its bare-container oracle |
| `docs/QUICKSTART.md`, `docs/how-to/run-a-cluster.md`, `docs/how-to/cut-a-release.md`, `docs/reference/semver-policy.md`, gate doc, README, CLAUDE.md, spec §6 "As built (M12c)" | docs |

---

### Task 1: Version identity, publish metadata, dead deps, `deny.toml` + CI `deny`/`publish-check` jobs

**Files:**
- Modify: root `Cargo.toml` (`[workspace.package]`, `[workspace.dependencies]`), every member `Cargo.toml` listed in the fact table below, `uc_journal/Cargo.toml`
- Create: `deny.toml`
- Modify: `.github/workflows/ci.yml` (+ `deny`, `publish-check` jobs)

**Interfaces / exact values:**
- `[workspace.package]`: `version = "2.6.0"`, `edition = "2024"`, `license = "Apache-2.0"`, `authors = ["Peter Knego"]`, **add** `repository = "https://github.com/PeterKnego/ultima_cluster"`, `homepage = "https://github.com/PeterKnego/ultima_cluster"`, `rust-version = "1.88"` (Task 2 may raise it), `keywords = ["consensus", "raft", "state-machine-replication", "aeron", "low-latency"]`, `categories = ["network-programming", "database-implementations"]`. Every member inherits `version/edition/license/authors/repository/homepage/rust-version/keywords/categories` via `.workspace = true` (keywords/categories may be overridden per crate if a crate wants different ones — not required).
- `uc_journal/Cargo.toml`: switch its literal `version`/`edition`/`license`/`authors` to `.workspace = true` (crate name stays `uc_journal`).
- `description` added where missing: `uc_node` ("The ultima_cluster node: four single-writer polling agents, the cnc page, elections, and the uc2-node daemon"), `uc_service` ("Service-side SDK: RawStateMachine / StateMachine, Sessioned, snapshots, the apply agent"), `uc_lincheck` (test lib — stays unpublished but give it one), `examples/*` (unpublished; optional).
- Every internal path dep across the workspace gains `version = "2.6.0"` (`{ path = "../x", version = "2.6.0" }`), including `[workspace.dependencies].uc_journal` and the `[dev-dependencies]` path entries (cargo requires versions there too for publish).
- `publish = false` stays on `uc_lincheck`, `examples/counter`, `examples/uc_crashtest`; **added** to `uc_sim` (simulation harness, not in the published DAG); **removed** from `uc2ctl`.
- Remove from `[workspace.dependencies]`: `quinn`, `rustls`, `rcgen`, `rustls-pemfile`, `tokio`, `futures` (none referenced by any member; `uc_service` keeps its own narrow `tokio`); delete the "QUIC + TLS (M2)" comment block. After `cargo update -w`/`cargo build`, assert `cargo tree -i ring` and `cargo tree -i quinn` print nothing.
- `deny.toml`:
  ```toml
  [graph]
  all-features = false
  [advisories]
  version = 2
  yanked = "deny"
  ignore = []          # add RUSTSEC ids here ONLY with a dated rationale comment
  [licenses]
  version = 2
  allow = ["Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unicode-3.0", "Zlib", "MPL-2.0", "CC0-1.0", "Unlicense", "OpenSSL", "BSL-1.0"]   # trim to what `cargo deny check licenses` actually reports; every entry must be justified by a crate in the graph — list them in a comment
  confidence-threshold = 0.8
  [bans]
  multiple-versions = "warn"
  wildcards = "deny"
  deny = [
    { crate = "ring",        reason = "one AES-GCM implementation in the binary: RustCrypto aes-gcm (see Cargo.toml crypto comment)" },
    { crate = "openssl",     reason = "no C TLS stack; UC seals its own transport" },
    { crate = "openssl-sys", reason = "same" },
    { crate = "quinn",       reason = "QUIC retired with v1 (CLAUDE.md); do not reintroduce" },
  ]
  [sources]
  unknown-registry = "deny"
  unknown-git = "deny"
  ```
- `ci.yml` gains jobs `deny` (`EmbarkStudios/cargo-deny-action@v2` with `command: check advisories licenses bans sources`) and `publish-check` (steps: `cargo package --no-verify --allow-dirty -p <crate>` for each of the 12 publishable crates in DAG order, then `cargo publish --dry-run -p uc_journal`, `-p uc_protocol`, `-p uc_remote`, `-p uc_consensus` — the leaves, the only ones resolvable before anything is on crates.io; a comment explains why the rest are validated by `cargo package` only and by the real publish order in `docs/how-to/cut-a-release.md`).

- [ ] **Step 1: Failing checks first.** `cargo tree -i ring | head -1` (expect output today — `ring` comes in via the dead `rustls`/`quinn`), `cargo package --no-verify --allow-dirty -p uc_log 2>&1 | tail -3` (expect "all dependencies must have a version specified when publishing" → this is the RED), and `cargo install cargo-deny` (if not present) + `cargo deny check 2>&1 | tail -5` (expect failures: no config / licenses).
- [ ] **Step 2: Implement** the manifest edits (use a small script or careful sed for the `version = "2.6.0"` insertion — then `cargo build --workspace` and read every `Cargo.toml` diff), `deny.toml` (run `cargo deny check licenses` and `cargo deny list` to derive the exact license allowlist from the real graph; every allow entry annotated with the crate(s) needing it), remove the dead deps, `cargo update -w` only if needed (prefer not to move unrelated versions; if the lockfile changes beyond dropping the dead trees, say so in the report).
- [ ] **Step 3: Verify** — `cargo tree -i ring`/`-i quinn`/`-i rustls`/`-i futures` all empty; `for c in uc_journal uc_protocol uc_log uc_crypto uc_net uc_consensus uc_node uc_client uc_service uc_remote uc_gateway uc2ctl; do cargo package --no-verify --allow-dirty -p $c >/dev/null && echo "ok $c"; done` all ok; `cargo publish --dry-run -p uc_journal` (+ the other three leaves) succeeds (network required); `cargo deny check` green; `cargo metadata --format-version 1 | python3 -c 'import json,sys; m=json.load(sys.stdin); print(sorted({(p["name"],p["version"]) for p in m["packages"] if p["source"] is None}))'` shows every workspace crate at 2.6.0; clippy + tests green; grep `CARGO_PKG_VERSION` usage (e.g. `uc2-node --version`, build_info metric) prints `2.6.0`.
- [ ] **Step 4: Commit** — `git commit -am "chore(release): lockstep 2.6.0, publish metadata + path-dep versions, dead quinn/rustls/tokio/futures deps removed, deny.toml + deny/publish-check CI jobs"`.

---

### Task 2: MSRV — `rust-version`, pinned toolchain, `msrv` CI job

**Files:**
- Modify: root `Cargo.toml` (`rust-version`), `rust-toolchain.toml`, `.github/workflows/ci.yml` (+ `msrv` job), `CLAUDE.md` (the build section: state the MSRV + the pin), `docs/how-to/run-a-cluster.md`/`QUICKSTART.md` "from source" note (Task 5 polishes)

**Interfaces:**
- `rust-toolchain.toml`: `channel = "1.96.0"` (the exact stable the box and CI build with today — `rustc 1.96.0`), `components = ["rustfmt", "clippy"]`, `profile = "minimal"`.
- `rust-version`: the LOWEST stable ≥ 1.88 for which `cargo +<v> check --workspace --all-targets --locked` passes. Procedure: `rustup toolchain install 1.88.0 --profile minimal` then `cargo +1.88.0 check --workspace --all-targets --locked`; if it fails on a feature/lint/let-chain/edition issue, try 1.89.0, 1.90.0 … until it passes; record the attempts in the report; set `rust-version` to the first passing one (note: edition-2024 let-chains are stable since 1.88; `is_multiple_of` since 1.87; check `Cargo.lock` deps' own `rust-version`s — `ultima-db 0.1.1` says 1.88).
- `ci.yml` `msrv` job: `dtolnay/rust-toolchain@<rust-version>` (or `rustup toolchain install`), `cargo check --workspace --all-targets --locked`. Keep the default jobs on the pinned stable via `rust-toolchain.toml` (rustup auto-installs it on the runner; `Swatinem/rust-cache` keys on it).

- [ ] **Steps:** install + check as above (RED = the first toolchain that fails, if any; GREEN = the chosen one passes) → set the values → `cargo +<rust-version> check --workspace --all-targets --locked` and `cargo check --workspace --all-targets` (pinned stable) both pass → docs lines → commit `git commit -am "chore(toolchain): rust-version <v> + rust-toolchain.toml pinned to 1.96.0 + msrv CI job"`.

---

### Task 3: `counter-remote` + `packaging/quickstart-local.sh` (the one-host, from-binaries quickstart) + its test

**Files:**
- Create: `examples/counter/src/bin/counter-remote.rs`; `packaging/quickstart-local.sh`; `examples/counter/tests/quickstart_local.rs` (feature `quickstart-tests`, nightly)
- Modify: `examples/counter/Cargo.toml` (+ `[[bin]] counter-remote`, dep `uc_remote`, feature `quickstart-tests = []`), `.github/workflows/nightly.yml` (+ `quickstart` job running that test), `examples/counter/src/lib.rs` docs if the Command/Query types need `pub` re-exports for the remote bin

**Interfaces:**
- `counter-remote --gateways HOST:PORT[,HOST:PORT…] --app-id A add <n> | reset | get [--linearizable]` — encodes `counter::Command` with bincode-standard into opaque bytes (exactly as `counter-client` does), submits via `uc_remote::RemoteClient` (`RemoteConfig { app_id, members, ..Default::default() }`), decodes `counter::Applied`/`QueryResponse`, prints `value=<v> position=<p> replayed=<bool>`; exit 0 on success, 1 on error, 2 on bad args; `--timeout-secs` (default 10). SPDX header; doc comment explains it is the remote twin of `counter-client` and the reference for "how a remote client talks to a gateway".
- `packaging/quickstart-local.sh`:
  ```
  Usage: quickstart-local.sh [--bin-dir DIR] [--root DIR] [--secs N] [--keep]
    --bin-dir   directory holding uc2-node, uc2ctl, uc2-gateway, counter-service, counter-remote
                (default: the directory containing this script's ../bin, i.e. an extracted release tarball; or $UC2_BIN_DIR)
    --root      instance-dir root (default: $HOME/uc2-quickstart; refuses /tmp — RAM-backed, fsync is a no-op there)
    --secs      how long to keep the cluster up after the demo submit (default 0 = stop right after)
    --keep      leave the cluster running (prints the PIDs and the stop command)
  ```
  Steps: `set -euo pipefail`; verify the five binaries exist+executable; `uc2ctl gen-admin-key $ROOT/admin.key` (once); write `$ROOT/n{0,1,2}/node.toml` (ids 0..2, `bind = 127.0.0.1:9100+i`, three `[[members]]`, `instance_dir = $ROOT/n{i}`, `app_id = "quickstart"`, `[crypto] enabled = false`, `[admin] auth = "hmac"` + the key, `[metrics]` optional off) and `$ROOT/gateway.toml` (`[local] instance_dir = $ROOT/n0, app_id, listen = 127.0.0.1:9200`, `[[members]]` = three gateway addrs — only n0's gateway runs in the demo; the other two entries point at 127.0.0.1:9201/9202 which are not started — document that REDIRECT to them would fail in this one-gateway demo, and start gateways on all three nodes if `--full`); start 3 `uc2-node --config`, wait for `uc2ctl status` to report a serving leader (poll ≤ 30 s), start 3 `counter-service --instance-dir --app-id`, start `uc2-gateway --config` (on the LEADER's node dir — re-render gateway.toml after discovering the leader id from `uc2ctl status`, or start one gateway per node with `--full`: prefer `--full` = 3 gateways by default so REDIRECT works; then `counter-remote --gateways 127.0.0.1:9200,9201,9202 add 5` twice and `get --linearizable` → expect `value=10`; print PASS; `trap` kills every PID on exit (unless `--keep`). Every command's stdout/stderr goes to `$ROOT/logs/*.log`. Exit 0 = PASS, 1 = FAIL with the failing step named, 3 = a precondition (missing binary, /tmp root).
- `examples/counter/tests/quickstart_local.rs` (feature-gated, nightly `quickstart` job): builds a bin dir from `CARGO_BIN_EXE_counter-service`/`counter-remote` + `uc2-node`/`uc2ctl`/`uc2-gateway` resolved with the `cargo build -p <crate> --bin <bin> --message-format=json` trick (copy `uc_node_daemon_bin()` from `examples/uc_crashtest/tests/enospc.rs`), runs the script with `--bin-dir`, `--root $CARGO_TARGET_TMPDIR/quickstart`, asserts exit 0 and that stdout contains `value=10` and `PASS`.

- [ ] **Steps:** TDD: write the test first (fails: no script/bin) → implement `counter-remote` → implement the script (`shellcheck packaging/quickstart-local.sh` clean if shellcheck is installed; else state) → run the test (`cargo test -p counter --features quickstart-tests --test quickstart_local -- --nocapture`) → also run the script by hand against `cargo build --release` binaries (`--bin-dir ~/.cache/cargo-target/release`) and paste the PASS output in the report → nightly job → commit `git commit -am "feat(quickstart): counter-remote example + packaging/quickstart-local.sh (3 nodes + 3 gateways + remote submit from a bin dir) + nightly test"`.

---

### Task 4: `release.yml`, `packaging/Dockerfile`, `packaging/compose.yml`, `scripts/release_smoke.sh`, `cut-a-release.md`

**Files:**
- Create: `.github/workflows/release.yml`, `packaging/Dockerfile`, `packaging/compose.yml`, `scripts/release_smoke.sh`, `docs/how-to/cut-a-release.md`, `packaging/README-release.md` (the short README that goes into the tarball: what's inside, how to verify, pointer to the quickstart)

**Interfaces (release.yml):**
- Triggers: `push: tags: ['v*']` and `workflow_dispatch` with input `dry_run` (boolean, default true — builds, smokes, uploads artifacts, but skips the GitHub Release, the image push and signing).
- `permissions: contents: write, id-token: write, packages: write` (job-scoped: build/smoke need only `contents: read`).
- Job `build` (matrix: `{target: x86_64-unknown-linux-gnu, os: ubuntu-latest}`, `{target: aarch64-unknown-linux-gnu, os: ubuntu-24.04-arm}`): checkout; toolchain from `rust-toolchain.toml`; `cargo build --release --locked -p uc_node --bin uc2-node -p uc_ctl -p uc_gateway --bin uc2-gateway -p counter --bin counter-service --bin counter-remote`; `strip`; assemble `uc2-${VERSION}-${target}/` = `bin/{uc2-node,uc2ctl,uc2-gateway,counter-service,counter-remote}`, `packaging/{node.example.toml,gateway.example.toml,quickstart-local.sh,systemd/,prometheus/,grafana/,compose.yml}`, `LICENSE`, `README-release.md`; `tar czf uc2-${VERSION}-${target}.tar.gz`; `sha256sum > uc2-${VERSION}-${target}.tar.gz.sha256`; `actions/upload-artifact`. `VERSION` = the tag without `v` (or `0.0.0-dry` for dispatch), asserted equal to `cargo metadata`'s workspace version for tags (fail fast on mismatch).
- Job `sbom` (ubuntu-latest): `cargo install cargo-cyclonedx --locked`; `cargo cyclonedx --workspace --format json --override-filename uc2-${VERSION}.cdx` (one workspace SBOM) → artifact.
- Job `smoke` (needs build; ubuntu-latest): download the x86_64 tarball; `scripts/release_smoke.sh <tarball>` = `docker run --rm -v $PWD:/work -w /work ubuntu:24.04 bash -c 'tar xzf …; ./uc2-*/packaging/quickstart-local.sh --bin-dir ./uc2-*/bin --root /work/qs'` — a bare image, no Rust, no extra packages (the script uses only bash/coreutils; if it needs `ss`/`curl`, don't). Asserts PASS. Then the **compose smoke** (same job): `docker build -f packaging/Dockerfile --build-arg TARBALL=<x86 tarball> -t uc2:smoke .`, `UC2_IMAGE=uc2:smoke docker compose -f packaging/compose.yml up -d`, wait, `docker run --network <compose net> uc2:smoke counter-remote --gateways gw:9200 add 5` … `get` → `value=…`, `compose down -v`.
- Job `release` (needs build, sbom, smoke; `if: startsWith(github.ref, 'refs/tags/v') && !inputs.dry_run`): download all artifacts; `SHA256SUMS` over every tarball; `sigstore/cosign-installer@v3`; `cosign sign-blob --yes --bundle <f>.sigstore.json <f>` for each tarball + `SHA256SUMS` + the SBOM; `softprops/action-gh-release@v2` with all files, `generate_release_notes: false`, body pointing at `RELEASES.md`.
- Job `image` (needs build, smoke; same `if`): `docker/setup-qemu-action` + `setup-buildx`; login ghcr with `GITHUB_TOKEN`; `docker/build-push-action` `platforms: linux/amd64,linux/arm64`, context with both tarballs, `tags: ghcr.io/peterknego/uc2:${VERSION}`; `cosign sign --yes ghcr.io/peterknego/uc2@${digest}`.
- `packaging/Dockerfile`: `FROM gcr.io/distroless/cc-debian12`; `ARG TARGETARCH`; `COPY uc2-${VERSION}-${arch}/bin/ /usr/local/bin/` (map amd64→x86_64, arm64→aarch64 in a tiny pre-stage or pass `--build-arg`), `COPY packaging/ /opt/uc2/packaging/`; `USER 65532`; `ENTRYPOINT ["/usr/local/bin/uc2-node"]`; a comment on how to run uc_ctl/uc2-gateway/counter-service from the same image (`--entrypoint`).
- `packaging/compose.yml`: services `n0 n1 n2` (`uc2-node --config /etc/uc2/node.toml`, configs rendered into named volumes by an `init` service from `node.example.toml`-shaped templates; static IPs 10.77.0.10–12 on network `uc2`; `bind` = that IP:9100; volumes `n0:/srv/uc2/n0` etc.), `svc0 svc1 svc2` (`counter-service --instance-dir /srv/uc2/nX --app-id quickstart`, sharing the node's volume; `depends_on`), `gw0 gw1 gw2` (`uc2-gateway --config`, ports 9200-9202 published), all from `${UC2_IMAGE:-ghcr.io/peterknego/uc2:2.6.0}`; `[crypto] enabled = false`, `[admin] auth = "hmac"` with a key generated by the init service. Header comment: single-host demo, NOT a production topology (one host); the quickstart docs explain.
- `docs/how-to/cut-a-release.md`: the exact sequence — bump check (`cargo metadata` version == tag), `RELEASES.md`/`docs/releases.md` written (CLAUDE.md rule), `git tag -s vX.Y.Z`, push tag → `release.yml` → verify with `cosign verify-blob --bundle … --certificate-identity-regexp 'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' --certificate-oidc-issuer https://token.actions.githubusercontent.com`, `cosign verify ghcr.io/peterknego/uc2:X.Y.Z …`; then the MANUAL crates.io publish order (`cargo publish -p uc_journal`, `uc_protocol`, `uc_crypto`, `uc_log`, `uc_consensus`, `uc_net`, `uc_client`, `uc_node`, `uc_service`, `uc_remote`, `uc_gateway`, `uc2ctl` — wait for each to index); the rc pattern (`v2.6.0-rc.1` first to exercise the workflow).

- [ ] **Steps:** write all files; validate locally what can be: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/release.yml'))"`; `shellcheck scripts/release_smoke.sh packaging/quickstart-local.sh` (install shellcheck via `apt`? not root — skip if unavailable, say so); `actionlint` if installable (`cargo install`? no — skip, say so); build the tarball layout locally with a `--release` build and run `packaging/quickstart-local.sh --bin-dir <that>/bin` once more as the tarball-layout proof; `cargo install cargo-cyclonedx` and generate the SBOM locally, attach its size to the report; download `cosign` (`curl -fsSL https://github.com/sigstore/cosign/releases/latest/download/cosign-linux-amd64 -o ~/.local/bin/cosign`) and `cosign sign-blob` a scratch file in **offline/no-OIDC** mode is not possible — state that signing is CI-only; docker/compose: NOT available locally — state "validated in CI only; first real run = the rc tag" in the report AND in the gate doc. Commit `git commit -am "ci(release): release.yml (native x86_64+aarch64, tarballs+SHA256SUMS+SBOM, cosign keyless, ghcr image, release-smoke in a bare container, compose smoke), Dockerfile, compose.yml, cut-a-release how-to"`.

---

### Task 5: Docs — artifacts-first QUICKSTART, install-from-release, `semver-policy.md`, gate rows, spec amendment, fmt deferral

**Files:**
- Rewrite: `docs/QUICKSTART.md` (sections: 1 Download + verify (cosign verify-blob command with the identity regexp; SHA256SUMS fallback), 2 Run the local quickstart (`packaging/quickstart-local.sh`, what it prints, where the logs/dirs are), 3 What just happened (nodes/services/gateways/remote client; the `node.toml` it wrote, annotated — both sections), 4 The state machine (existing content), 5 A real three-node cluster → points at `run-a-cluster.md` + `run-a-gateway.md` + compose, 6 From source (the old cargo path, condensed), 7 Where next)
- Modify: `docs/how-to/run-a-cluster.md` "Install the binaries" (download tarball → verify → `install -m 0755 bin/* /usr/local/bin/`; or the image; `cargo build … --locked` as the alternative), `docs/how-to/run-a-gateway.md` (one line: the release ships `uc2-gateway` + `counter-remote`), `README.md` (Try it → the 2-command artifact path; Build & test → MSRV + pin; crates.io badge-ish line), `CLAUDE.md` (M12c status; MSRV/pin; release process pointer; publish order), `docs/ops/uc2-runbook.md` (install/upgrade from release tarball), `docs/benchmarks/uc2-m12-gate-2026-08-22.md` (rows: no-toolchain quickstart = `release-smoke` (CI, pending the first rc tag); cosign verify (CI, pending); crates publish dry-run (CI `publish-check` PASS — leaves dry-run + all packaged); MSRV job PASS; deny job PASS; **fmt: DEFERRED** — two long-lived worktrees open (`fix/remaining-flakes`, `worktree-uc2-multi-service`), 2 715 hunks of drift; facts: what is validated locally vs CI-only), spec §6 "As built (M12c)" (native arm runner; `counter-remote`; one quickstart script; publish-check leaves-only; `uc_sim` unpublished; fmt deferred; `RELEASES.md` entry lands at the tag)
- Create: `docs/reference/semver-policy.md` — the promised surface verbatim from spec §3.2 (`RawStateMachine`, `StateMachine`, `SnapshotStateMachine`, `OutputHandler`, `Sessioned`, `NodeConfig` + `node.toml`, `gateway.toml`, the three client tiers, `uc_remote` protocol v1, `uc2ctl` verbs/exit codes), what is NOT promised (everything else: `#[doc(hidden)]` items, `pub(crate)`-by-intent modules listed per crate, the `apply-profile`/`test-util` features, `uc_sim`/`uc_lincheck`/examples), the flag-day rule for wire/cnc versions, the one-way door (a type implements exactly one of `RawStateMachine`/`StateMachine`; no second blanket impl can ever be added), "breaking = 3.0.0", and the lockstep rule (one version, one tag). Per-crate `lib.rs` top doc gets one line "Semver: see docs/reference/semver-policy.md; promised surface = …" (a sweep of the 12 crates).

- [ ] **Steps:** write; every command in QUICKSTART copy-pasteable and checked against the script's real flags and release.yml's real file names; link grep; `cargo doc --workspace --no-deps --lib` + the docs.yml landing-page guard; commit `git commit -am "docs(m12c): artifacts-first QUICKSTART, install from release, semver-policy, cut-a-release, gate rows (fmt deferred), spec amendment"`.

---

## Self-review against spec §3.2 / §6

- §3.2 lockstep 2.6.0, path-dep versions, `uc_journal` joins, `uc2ctl` publishable, examples/`uc_lincheck` not, `rust-version` + pin + `msrv` job, `semver-policy.md` → Tasks 1, 2, 5 (+ `uc_sim` unpublished — plan addition, ruled). ✔
- §6 supply chain (deny incl. one AES-GCM, SBOM, dead deps) → Tasks 1, 4. ✔ Publishing (DAG dry-run as CI job; manual publish) → Task 1 `publish-check` (leaves dry-run + package-all, with the reason) + Task 4 `cut-a-release.md`. ✔ `release.yml` (matrix, `--release --locked`, strip, tarballs + SHA256SUMS + SBOM, cosign keyless, GitHub Release, ghcr distroless image signed, `release-smoke` bare ubuntu running the binary quickstart incl. a remote submit) → Tasks 3, 4 (`counter-remote` is the plan's addition so the remote submit exists). ✔ Quickstart + packaging (artifacts-first QUICKSTART, compose.yml, gateway unit/example already shipped in M12a) → Tasks 3, 4, 5. ✔ `cargo fmt` conditional → deferred with the condition stated (Task 5). ✔
- Names consistent: `counter-remote`, `packaging/quickstart-local.sh`, `scripts/release_smoke.sh`, `packaging/Dockerfile`, `packaging/compose.yml`, `deny.toml`, CI jobs `deny`/`publish-check`/`msrv`, `docs/how-to/cut-a-release.md`, `docs/reference/semver-policy.md`, `packaging/README-release.md`. ✔
