# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# The ultima_cluster container image: the five shipped binaries on a
# distroless base, built FROM A RELEASE TARBALL rather than from source.
#
# That is the point of the two stages below. The tarball is the release
# artifact — the thing that is checksummed, signed and downloaded — so
# unpacking it here makes the image's `/usr/local/bin` byte-identical to what
# `sha256sum -c` verified, instead of a second, separately-compiled build that
# merely claims the same version. It also means this Dockerfile needs no Rust
# toolchain, no source tree and no network: the whole build context is a
# `dist/` directory holding one tarball per architecture.
#
#   Build context layout:
#       dist/uc2-<version>-x86_64-unknown-linux-gnu.tar.gz
#       dist/uc2-<version>-aarch64-unknown-linux-gnu.tar.gz   (optional)
#
#   Single architecture, tarball named explicitly (what release_smoke.sh and
#   the CI compose smoke do):
#       docker build -f packaging/Dockerfile \
#           --build-arg TARBALL=uc2-2.7.0-x86_64-unknown-linux-gnu.tar.gz \
#           -t uc2:smoke .
#
#   Multi-architecture, tarball derived from VERSION + the platform being
#   built (what release.yml's `image` job does):
#       docker buildx build -f packaging/Dockerfile \
#           --platform linux/amd64,linux/arm64 \
#           --build-arg VERSION=2.7.0 -t ghcr.io/peterknego/uc2:2.7.0 .
#
# The image's ENTRYPOINT is `uc2-node`. The other four binaries live in the
# same image and are reached with --entrypoint:
#
#   docker run --rm -v uc2-n0:/srv/uc2/n0 uc2:2.7.0 --config /etc/uc2/node.toml
#   docker run --rm --entrypoint /usr/local/bin/uc2ctl          uc2:2.7.0 status --instance-dir /srv/uc2/n0 --app-id myapp
#   docker run --rm --entrypoint /usr/local/bin/uc2-gateway     uc2:2.7.0 --config /etc/uc2/gateway.toml
#   docker run --rm --entrypoint /usr/local/bin/counter-service uc2:2.7.0 --instance-dir /srv/uc2/n0 --app-id myapp
#   docker run --rm --entrypoint /usr/local/bin/counter-remote  uc2:2.7.0 --gateways gw0:9200 --app-id myapp get
#
# There is no shell in the image (distroless), so `docker exec ... sh` does
# not work and every `command:` must be an exec-form argv. See
# packaging/compose.yml for a worked three-node example.

# Declared before the first FROM so both stages can consume it; re-declared
# inside each stage that uses it, as Docker requires.
ARG VERSION=0.0.0-dry

# ---------------------------------------------------------------- unpack
# A throwaway stage with `tar` in it. Nothing from here reaches the final
# image except the extracted files themselves.
FROM debian:12-slim AS unpack
ARG VERSION
ARG TARGETARCH
# Set to a file name inside dist/ to pin one tarball explicitly. Left empty,
# the tarball is derived from VERSION and the platform being built, which is
# what makes one `buildx --platform linux/amd64,linux/arm64` invocation pick
# the right native binaries for each leg.
ARG TARBALL=""

COPY dist/ /dist/

RUN set -eu; \
    if [ -n "${TARBALL}" ]; then \
        tarball="/dist/${TARBALL}"; \
    else \
        case "${TARGETARCH}" in \
            amd64) rust_target="x86_64-unknown-linux-gnu" ;; \
            arm64) rust_target="aarch64-unknown-linux-gnu" ;; \
            *) echo "Dockerfile: unsupported TARGETARCH='${TARGETARCH}'" >&2; exit 1 ;; \
        esac; \
        tarball="/dist/uc2-${VERSION}-${rust_target}.tar.gz"; \
    fi; \
    if [ ! -f "${tarball}" ]; then \
        echo "Dockerfile: no release tarball at ${tarball}; build context holds:" >&2; \
        ls -l /dist >&2; \
        exit 1; \
    fi; \
    mkdir -p /unpacked; \
    tar xzf "${tarball}" -C /unpacked; \
    root="$(find /unpacked -mindepth 1 -maxdepth 1 -type d)"; \
    [ -x "${root}/bin/uc2-node" ] || { echo "Dockerfile: ${tarball} has no bin/uc2-node" >&2; exit 1; }; \
    mkdir -p /stage; \
    mv "${root}/bin" /stage/bin; \
    mv "${root}/packaging" /stage/packaging; \
    chmod 0755 /stage/bin/*

# ---------------------------------------------------------------- image
# distroless/cc: glibc + libgcc + libstdc++ and nothing else — no shell, no
# package manager, no coreutils. The binaries are dynamically linked against
# glibc (the release tarballs are built for *-unknown-linux-gnu), so `cc` is
# the right base; `static` would not run them.
FROM gcr.io/distroless/cc-debian12
ARG VERSION

LABEL org.opencontainers.image.title="ultima_cluster" \
      org.opencontainers.image.description="Aeron-shaped, Rust-native state-machine-replication server" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/PeterKnego/ultima_cluster" \
      org.opencontainers.image.documentation="https://github.com/PeterKnego/ultima_cluster/blob/main/QUICKSTART.md"

COPY --from=unpack /stage/bin/ /usr/local/bin/
COPY --from=unpack /stage/packaging/ /opt/uc2/packaging/

# distroless's `nonroot` account. A node's instance directory, its config
# file and its admin key must all be readable/writable by this uid — see the
# `init` service in packaging/compose.yml, which chowns the volumes before
# any node starts.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/uc2-node"]
