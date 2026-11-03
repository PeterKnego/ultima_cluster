#!/usr/bin/env bash
# Runs the netfault scenario suite (testing/uc_crashtest/tests/netfault.rs) inside a privileged container on a
# ROOTFUL docker daemon, for a box where the developer has no root: the test process is root in the container's
# own network namespace, so the suite attaches to the container's `lo` and needs no sudo. Builds inside the
# container (the host's binaries may not run on the image's libc) into a target directory on real disk.
#
#   testing/netfault/scripts/run-suite-in-container.sh [--crypto] [-- <extra cargo test args>]
#
# NETFAULT_SUITE_TARGET names the target directory (default $HOME/scratch/netfault-target); DOCKER_CONTEXT is
# honoured (a box whose default context is rootless needs DOCKER_CONTEXT=default).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TARGET="${NETFAULT_SUITE_TARGET:-${HOME}/scratch/netfault-target}"
IMAGE=rust:1.96-bookworm
CRYPTO=""
if [[ "${1:-}" == "--crypto" ]]; then
    CRYPTO="-e UC2_CRYPTO=1"
    shift
fi
[[ "${1:-}" == "--" ]] && shift
mkdir -p "${TARGET}"
# shellcheck disable=SC2086
exec docker run --rm --privileged \
    -v "${ROOT}:/src" -v "${TARGET}:/target" -v "${HOME}/.cargo/registry:/usr/local/cargo/registry" \
    -w /src -e CARGO_TARGET_DIR=/target ${CRYPTO} "${IMAGE}" bash -c '
        set -euo pipefail
        apt-get update -qq > /dev/null
        apt-get install -y -qq --no-install-recommends bpftool jq iproute2 clang libbpf-dev linux-libc-dev make > /dev/null
        make -C testing/netfault
        cargo test -p uc_crashtest --features netfault-tests --test netfault -- --test-threads=1 --nocapture "$@"
    ' _ "$@"
