#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Release smoke test: unpack a release tarball inside a BARE container and run
# the quickstart out of it.
#
#     scripts/release_smoke.sh dist/uc2-2.6.0-x86_64-unknown-linux-gnu.tar.gz
#
# What it proves is narrow and deliberate: that the artifact a stranger
# downloads is self-sufficient. The container is stock `ubuntu:24.04` with
# nothing installed into it — no Rust toolchain, no source tree, no curl, no
# jq, no ss — so anything the tarball forgot to ship, or any dependency the
# quickstart script quietly acquired from a developer's machine, fails here
# rather than in somebody's first ten minutes with the project.
#
# Inside the container it does exactly what the tarball's own instructions
# say to do:
#
#     tar xzf uc2-<ver>-<target>.tar.gz
#     uc2-<ver>-<target>/packaging/quickstart-local.sh --bin-dir .../bin --root /work/qs
#
# and that script starts three nodes, three services and three gateways,
# waits for a real election, drives two writes and a linearizable read
# through a gateway from the outside, and asserts the answer. PASS means the
# whole path worked from the artifact alone.
#
#   Usage: release_smoke.sh TARBALL [--image REF] [--work DIR]
#     --image  base image to run in. Default ubuntu:24.04. Whatever you pass
#              must have bash and coreutils and nothing else is assumed.
#     --work   host directory to unpack into (it is DELETED and recreated).
#              Default: $RUNNER_TEMP/.release-smoke under Actions, else
#              ./.release-smoke. IT MUST BE OUTSIDE ANY DOCKER BUILD CONTEXT:
#              the run below leaves hundreds of megabytes of cluster state
#              here, including a mode-0600 admin key owned by root, and a
#              later `docker build` whose context contains that key fails
#              while packing it, three steps away from the cause. That is
#              also why the default is NOT the tarball's own directory —
#              `dist/` is exactly where release.yml builds the image from.
#              Note that run from the repository root with no $RUNNER_TEMP,
#              the default `./.release-smoke` IS inside the image build
#              context; it is safe only because .dockerignore excludes it
#              (`*` then `!dist/*.tar.gz`, i.e. admit the artifacts and
#              nothing else). If your build context differs, pass --work
#              explicitly.
#
#   Exit codes: 0 = PASS, 1 = the smoke run failed,
#               3 = a precondition (no docker, no tarball, bad argument).
#
# The container runs as root and writes into --work, so the files it leaves
# behind are root-owned on the host; the next run deletes them from inside a
# container for exactly that reason.

set -euo pipefail

IMAGE="ubuntu:24.04"
TARBALL=""
WORK=""

precond() { printf 'release_smoke.sh: %s\n' "$*" >&2; exit 3; }
fail()    { printf 'release_smoke.sh: FAIL: %s\n' "$*" >&2; exit 1; }

usage() {
    sed -n '/^#   Usage:/,/^#               3 = a precondition/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --image) IMAGE="${2:?--image needs a reference}"; shift 2 ;;
        --work)  WORK="${2:?--work needs a directory}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        -*) printf 'release_smoke.sh: unknown option %s\n\n' "$1" >&2; usage >&2; exit 3 ;;
        *)
            [ -z "$TARBALL" ] || precond "expected exactly one tarball, also got '$1'"
            TARBALL="$1"; shift ;;
    esac
done

[ -n "$TARBALL" ] || { usage >&2; exit 3; }
[ -f "$TARBALL" ] || precond "no such tarball: $TARBALL"
command -v docker >/dev/null 2>&1 || precond "docker is not on PATH — this test needs a container runtime"

TARBALL_NAME="$(basename -- "$TARBALL")"
[ -n "$WORK" ] || WORK="${RUNNER_TEMP:-$PWD}/.release-smoke"
case "$WORK" in /*) ;; *) WORK="$PWD/$WORK" ;; esac

# The previous run's files are root-owned (the container writes them), so the
# delete happens in a container too rather than needing sudo on the host.
if [ -d "$WORK" ]; then
    docker run --rm -v "$(dirname -- "$WORK"):/parent" "$IMAGE" \
        rm -rf "/parent/$(basename -- "$WORK")" >/dev/null 2>&1 ||
        precond "cannot clear the work directory $WORK"
fi
mkdir -p "$WORK"
cp -- "$TARBALL" "$WORK/$TARBALL_NAME"

printf 'release_smoke.sh: %s in a bare %s\n\n' "$TARBALL_NAME" "$IMAGE"

# Everything below runs inside the container. `set -eu` there, and the
# quickstart's own exit codes (0 PASS / 1 named failure / 3 precondition)
# come straight back out through docker run.
#
# `set +e` around it on purpose: with `pipefail` on, a failing quickstart
# would abort this script through `set -e` before the exit code could be read
# off PIPESTATUS and turned into the named failure below.
set +e
docker run --rm \
    -v "$WORK:/work" -w /work \
    -e "TARBALL_NAME=$TARBALL_NAME" \
    "$IMAGE" \
    bash -c '
        set -eu
        tar xzf "/work/$TARBALL_NAME"
        # Exactly one uc2-<version>-<target>/ directory comes out of it; the
        # trailing slash keeps the glob off the tarball file itself.
        set -- /work/uc2-*/
        [ "$#" -eq 1 ] || { echo "expected one unpacked directory, got: $*" >&2; exit 1; }
        root="${1%/}"
        echo "unpacked $root"
        ls -l "$root"
        exec "$root/packaging/quickstart-local.sh" --bin-dir "$root/bin" --root /work/qs
    ' | tee "$WORK/smoke.log"
rc="${PIPESTATUS[0]}"
set -e

[ "$rc" -eq 0 ] || fail "the quickstart exited $rc inside the container (log: $WORK/smoke.log)"
grep -qx 'PASS' "$WORK/smoke.log" || fail "the quickstart exited 0 but never printed PASS (log: $WORK/smoke.log)"

printf '\nrelease_smoke.sh: PASS — %s is self-sufficient\n' "$TARBALL_NAME"
