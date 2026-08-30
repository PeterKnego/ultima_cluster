# Cut a release

You are shipping a version of `ultima_cluster` to people who did not build it.
That means three separate publications, in a fixed order: the **tagged
artifacts** (tarballs, checksums, SBOM, signatures) and the **container
image**, both produced by `.github/workflows/release.yml` when you push a tag;
and the **crates.io publish**, which is manual, sequential, and permanent.

The first two are reversible — delete a release, delete a tag. The third is
not: a version published to crates.io can be yanked but never replaced. So the
order below is deliberate. Publish to crates.io last, after the tag has built
and the artifacts have been verified.

Everything here assumes you are on `main`, with the work merged and CI green.

## 1. Before the tag: the writeup is part of the release

CLAUDE.md's rule, and it is not ceremony — the tag must *contain* the
documentation for what it changes, because the tag is what people read.

- [ ] A new section at the top of `RELEASES.md` (latest first): one bullet per
      feature, each linking to a how-to/reference/explainer that **exists**;
      an optional bullet for fixed bugs; an optional bullet for performance,
      linking the gate doc.
- [ ] The matching per-release entry in `docs/releases.md`.
- [ ] A sweep of `QUICKSTART.md`, `docs/how-to/`, `docs/reference/` for
      statements this release invalidated. Upgrade consequences that refuse to
      start an old config (v2.6.0's `[crypto]`/`[admin]` sections, for
      instance) belong in `docs/how-to/upgrade-a-cluster.md`, in the imperative.
- [ ] The version bumped in the root `Cargo.toml`'s `[workspace.package]`, and
      every intra-workspace `version = "…"` dependency pin with it. They move
      in lockstep; `cargo package` is what catches a straggler, and CI's
      `publish-check` job runs it on every push.
- [ ] The literal version string also appears outside the manifest, where
      `cargo package` cannot see it: `packaging/compose.yml`
      (`${UC2_IMAGE:-ghcr.io/peterknego/uc2:2.6.0}`), comments in
      `packaging/Dockerfile`, and worked examples in `docs/QUICKSTART.md` and
      `docs/how-to/run-a-cluster.md`. Find every straggler with:

      ```sh
      grep -rn "$OLD" packaging/ docs/
      ```

      where `$OLD` is the version being replaced, and update each hit.
- [ ] **Retire the pre-tag scaffolding the writeup left behind**, because the
      tag freezes whatever is there. For `v2.6.0` that is: delete the
      not-published-yet notes (`README.md`'s "Try it" blockquote and
      `docs/QUICKSTART.md` §1's, plus §1's caveat clause in the
      "every output is real" paragraph); date the `v2.6.0` headings in
      `RELEASES.md` and `docs/releases.md` and drop their "prepared, not yet
      tagged" qualifiers (`v2.5.0`'s headings are the model); replace
      `docs/releases.md`'s "M12d — security posture (this branch)" with the
      merge sha; confirm `SECURITY.md`'s supported-versions table names the
      new line; and prune the fuzz corpus back to its committed seeds, so a
      `git add -A` cannot smuggle libFuzzer discoveries into the tag —

      ```sh
      cd fuzz && cargo +nightly run --bin seed-corpus
      ```

      which regenerates the deterministic `NN-name` seeds; those are the only
      corpus files that belong in git. For `v2.8.0` the scaffolding to
      retire is: the two `<tag date>` headings (`RELEASES.md`,
      `docs/releases.md`) and the two `<!-- PENDING FLEET RUN … -->` HTML
      comments in the same two files, filled from the fleet gate's Results
      section once it has run (`grep -rn "PENDING FLEET RUN"` finds both;
      the comments are invisible when rendered, so they are easy to miss).

## 2. Check the version the way the workflow will

`release.yml` refuses to build a tag whose version disagrees with the
manifest, so find out before you tag:

```sh
cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.name=="uc2_node") | .version'
```

The tag is that string with a `v` in front: `2.6.0` → `v2.6.0`. A release
candidate may add a prerelease suffix the manifest does not carry — `v2.6.0-rc.1`
against a `2.6.0` workspace is accepted, and is the pattern in step 3.

## 3. Exercise the workflow before you spend the real tag

Two ways, cheapest first.

**A dispatch dry run.** Actions → *release* → *Run workflow*, leaving
`dry_run` ticked. It builds both tarballs, generates the SBOM, and runs the
full `release-smoke` job — the tarball unpacked in a bare `ubuntu:24.04` with
no toolchain in it, plus the compose stack driven through a gateway — then
stops. Nothing is published, nothing is signed, and the version is
`0.0.0-dry`. Use this after any edit to `release.yml`, the `Dockerfile`,
`compose.yml` or `quickstart-local.sh`.

**A release candidate.** A dry run stops short of signing, the GitHub Release
and the ghcr push — not because a dispatch run could not mint an OIDC identity
(it could), but because the `release` and `image` jobs are guarded on
`startsWith(github.ref, 'refs/tags/v') && !inputs.dry_run` and simply do not
run. Those three are only ever exercised by a real tag. An `-rc` tag is a real
tag:

```sh
git tag -a v2.6.0-rc.1 -m "ultima_cluster 2.6.0-rc.1"
git push origin v2.6.0-rc.1
```

It produces a real, verifiable, fully-signed release — of a version nobody
will mistake for the final one, and one the workflow marks `prerelease` so it
cannot take over the repository's "Latest release" pointer. Do this **once per
release**, verify it with step 5, then delete the tag and the release if you
like. The rc is also the only way to test that `cosign verify-blob` works for
someone who is not you.

## 4. Tag it

```sh
git tag -a v2.6.0 -m "ultima_cluster 2.6.0"
git push origin v2.6.0
```

`-a` makes an annotated tag (a tag object with a tagger and a message — the
workflow's `version` job reads the tag name, and the history reads the
message). **The tag is not GPG-signed, and never has been:** `git tag -v` on
`v2.6.0-rc.1`, `v2.7.0` and `v2.8.0` reports "no signature found" (this file
said `-s` until 2026-08-30, but no release box has ever held a signing key,
and the GPG signature you may see on the tagged *commit* is GitHub's web-flow
key on the PR merge, not the tag). Artifact authenticity rests on cosign's
keyless signature in the `release` and `image` jobs — which proves that *this
workflow, on a `v*` tag of this repository* made those files — and that is
what §5 verifies. If you want the tag itself to also prove *who* cut the
release, set up a key (`git config user.signingkey …`) and use `-s`; nothing
in the workflow checks it either way.

Then watch the run. Jobs, in order:

| job | what it proves |
| --- | --- |
| `version` | the tag and the manifest agree |
| `build` (×2) | the five binaries compile `--locked` on native x86_64 and native aarch64 hardware, and tar up with the packaging assets |
| `sbom` | a CycloneDX document per workspace crate |
| `release-smoke` | **the gate.** The x86_64 tarball, unpacked in a bare container with no Rust in it, runs its own quickstart to `PASS`; then the same binaries, as containers, form a three-node cluster and answer a linearizable read through a gateway |
| `release` | tarballs + `SHA256SUMS` + SBOM signed keylessly, GitHub Release published |
| `image` | `ghcr.io/peterknego/uc2:<version>` pushed for amd64 + arm64, signed by digest |

Nothing is published unless `release-smoke` passes. If it fails, fix the
cause, delete the tag (`git push --delete origin v2.6.0 && git tag -d v2.6.0`)
and tag again — do not push a `v2.6.0.1`.

**One gap to know about:** `release-smoke` runs the **x86_64** tarball only.
The aarch64 binaries are compiled and packaged on native arm hardware and
copied into the arm64 leg of the image, but nothing in CI ever *executes*
them. The first aarch64 run happens on somebody's machine. If you have an arm
host, unpack `uc2-<version>-aarch64-unknown-linux-gnu.tar.gz` on it and run
`packaging/quickstart-local.sh` by hand as part of step 5 — the rc tag is the
right place to spend that ten minutes.

## 5. Verify what was published, as a stranger would

Do this from a clean directory, downloading from the release page. You are
checking the thing users will download, not the thing your runner produced.

```sh
sha256sum -c SHA256SUMS --ignore-missing

cosign verify-blob \
  --bundle uc2-2.6.0-x86_64-unknown-linux-gnu.tar.gz.sigstore.json \
  --certificate-identity-regexp \
    'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  uc2-2.6.0-x86_64-unknown-linux-gnu.tar.gz

cosign verify \
  --certificate-identity-regexp \
    'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/peterknego/uc2:2.6.0
```

The `--certificate-identity-regexp` is the whole point. Signing is keyless:
there is no private key, and a signature on its own proves nothing except that
*somebody* had a sigstore certificate. What makes it meaningful is pinning
**which workflow, in which repository, on what kind of ref** was allowed to
produce it. Verifying without those two flags is not verification.

Then run the artifact:

```sh
tar xzf uc2-2.6.0-x86_64-unknown-linux-gnu.tar.gz
uc2-2.6.0-x86_64-unknown-linux-gnu/packaging/quickstart-local.sh
```

It should print `PASS` on a machine that has never had a Rust toolchain on it.

## 6. Publish to crates.io — manually, in this order

This is not in the workflow, on purpose. Twelve crates, each of which must be
*indexed* by the registry before the next one can resolve it, and every
version is permanent. An automated retry loop against an irreversible
operation is a bad trade; a person watching each one is not.

Order is dependency order — it is the only order that resolves:

```sh
cargo publish -p ultima-journal
cargo publish -p uc_protocol
cargo publish -p uc2_crypto
cargo publish -p uc2_log
cargo publish -p uc2_consensus
cargo publish -p uc2_net
cargo publish -p uc2_client
cargo publish -p uc2_node
cargo publish -p uc2_service
cargo publish -p uc2_remote
cargo publish -p uc2_gateway
cargo publish -p uc2ctl
```

Wait for each to appear on crates.io before starting the next — `cargo
publish` returns before the index has caught up, and the next crate's
dependency resolution reads the index. Modern cargo blocks on this for you;
if a publish fails with "no matching package named …", the previous one has
not indexed yet, so wait and re-run just that one.

`uc2_sim`, `uc-lincheck`, `counter` and `uc2-crashtest` are `publish = false`:
they are the proof and teaching apparatus, not the product.

If a crate fails partway through the list, the ones before it are already
public. Fix the failure, bump nothing, and re-run from the crate that failed —
a half-published version is recoverable; a wrong published version is not.

## 7. After

- [ ] The release page renders, and its links into `RELEASES.md` resolve at
      the tag (not at `main` — that is why the body pins `v<version>`).
- [ ] `docker run --rm ghcr.io/peterknego/uc2:2.6.0 --help` works.
- [ ] The gate doc for the milestone records the release, per the honest-failure
      protocol every milestone has used.

## Related

- [Upgrade a cluster](upgrade-a-cluster.md) — what an operator has to do to
  *take* the release you just cut. Read it before writing the `RELEASES.md`
  section; if it needs a new step, this release is not done.
- [Versioning and the semver promise](../reference/semver-policy.md) — what
  the number you are about to bump promises, which items are covered, and
  what makes a release major rather than minor.
- `packaging/README-release.md` — the short README that ships inside the
  tarball, and the first thing a downloader reads.
