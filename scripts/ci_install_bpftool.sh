#!/usr/bin/env bash
# Installs bpftool on a CI host for testing/netfault's CLI. Ubuntu ships it per kernel (linux-tools-<release>, and
# linux-tools-azure on GitHub's runners); this tries the running kernel's package, then the cloud meta-package, and
# otherwise FAILS BY NAME. There is deliberately no "download a static binary" fallback here: that needs a checksum
# verified out of band, and a script that fetches and runs an unverified binary on a CI host is worse than a red job.
set -euo pipefail
if command -v bpftool > /dev/null 2>&1 && bpftool version > /dev/null 2>&1; then
    echo "bpftool already present: $(bpftool version | head -1)"
    exit 0
fi
sudo apt-get update -qq > /dev/null
for pkg in "linux-tools-$(uname -r)" linux-tools-azure linux-tools-generic; do
    if sudo apt-get install -y --no-install-recommends linux-tools-common "${pkg}" > /dev/null 2>&1 \
        && bpftool version > /dev/null 2>&1; then
        echo "bpftool from ${pkg}: $(bpftool version | head -1)"
        exit 0
    fi
done
echo "ci_install_bpftool: no bpftool for kernel $(uname -r) in apt (tried linux-tools-$(uname -r), linux-tools-azure," >&2
echo "linux-tools-generic); add a checksummed static release here once its checksum is verified out of band" >&2
exit 1
