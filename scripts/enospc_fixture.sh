#!/usr/bin/env bash
# M11 Task 6: a tiny loopback ext4 filesystem for the ENOSPC crashtest
# (examples/uc2-crashtest/tests/enospc.rs, feature `enospc-tests`, env-gated
# on UC2_ENOSPC_DIR). Needs sudo for mkfs/mount/chown (loop-device mounts are
# root-only); probed up front so a caller without passwordless sudo gets a
# clear exit-2 refusal instead of an interactive password prompt hanging the
# test/CI job forever.
#
# Usage:
#   scripts/enospc_fixture.sh create  <dir> <size-mb>
#   scripts/enospc_fixture.sh destroy <dir>
#
# `create <dir> <size-mb>`:
#   1. fallocate a <size-mb> MiB image under $HOME/.cache/uc2-enospc/ (never
#      /tmp — see CLAUDE.md; this box's /tmp is RAM-backed tmpfs with no
#      swap, and a loopback image there fights the same OOM-kill risk every
#      other heavy scratch artifact does).
#   2. mkfs.ext4 -q -m 0 the image (0% reserved-for-root: this is disposable
#      scratch, not a real volume, and the default 5% reserve would eat a
#      chunk of an already-tiny margin on a filesystem this small).
#   3. sudo mount -o loop the image at <dir>, sudo chown the caller.
#   4. Write a single deletable ballast file, $dir/uc2-enospc-ballast, that
#      pre-fills the filesystem down to a small headroom margin
#      (BALLAST_HEADROOM_MB, default 8 MiB) — enough for the node to boot
#      (cnc page + sparse ring files + a couple of small journal segments)
#      but little enough that continued journal growth under load exhausts
#      it in seconds. "Space returned" (the test's recovery step) is exactly
#      `rm $dir/uc2-enospc-ballast` — no unmount/remount, so the SAME
#      filesystem, SAME instance dir, is what the restarted node rejoins on.
#
# `destroy <dir>`: unmount <dir> if mounted (ignored if it wasn't) and remove
# its backing image. Idempotent — safe even if `create` never ran or failed
# partway. Does not remove <dir> itself (the operator/CI-provided mountpoint
# may be expected to persist as a plain, empty directory afterward).
set -euo pipefail

IMAGE_DIR="${UC2_ENOSPC_IMAGE_DIR:-$HOME/.cache/uc2-enospc}"
# 24 MiB, not 8: since 2026-08-21 the instance dir's mmapped files RESERVE
# their blocks at creation (uc_protocol's create_shared_backing_file switched
# from PUNCH_HOLE to ZERO_RANGE so a full disk can no longer SIGBUS a running
# process -- see the M11 gate doc, row 3b amendment 2). A node therefore needs
# ~15 MiB of free space just to START (1 MiB log buffer + three 4 MiB rings +
# two 1 MiB query rings), and with the old 8 MiB margin it refused to boot at
# all -- "failed to start node 0: io: No space left on device" -- which tests
# the startup refusal, not the under-load wall. The margin must leave room for
# the node to boot AND warm up; the test squeezes the remainder itself when it
# wants the wall (see `squeeze_free_space` in tests/enospc.rs).
BALLAST_HEADROOM_MB="${UC2_ENOSPC_HEADROOM_MB:-24}"
BALLAST_NAME="uc2-enospc-ballast"

usage() {
  echo "usage: $0 create <dir> <size-mb>" >&2
  echo "       $0 destroy <dir>" >&2
  exit 2
}

require_sudo() {
  # -n: non-interactive — fail immediately rather than prompting, which
  # would just hang a test run / CI job with no TTY. This is the ONLY
  # sudo-availability check in this script; every mkfs/mount/umount/chown
  # call below assumes it already passed.
  if ! sudo -n true 2>/dev/null; then
    echo "enospc_fixture.sh: passwordless sudo is not available on this host" >&2
    echo "  (probed via \`sudo -n true\`, which failed) — the loopback ext4" >&2
    echo "  fixture needs root for mkfs/mount/chown. The ENOSPC test that" >&2
    echo "  depends on this fixture skips cleanly when UC2_ENOSPC_DIR is" >&2
    echo "  unset; do not attempt to run it on a host without sudo." >&2
    exit 2
  fi
}

# Deterministic, collision-free image path for a given mountpoint dir: the
# absolute path with '/' -> '_', so `create`/`destroy` agree on the same
# image file without needing any separate state file.
image_path_for() {
  local dir_abs
  dir_abs="$(cd "$(dirname "$1")" 2>/dev/null && pwd)/$(basename "$1")" || dir_abs="$1"
  local sanitized="${dir_abs#/}"
  sanitized="${sanitized//\//_}"
  echo "$IMAGE_DIR/${sanitized}.img"
}

is_mounted() {
  mountpoint -q "$1" 2>/dev/null
}

cmd_create() {
  local dir="$1" size_mb="$2"
  [[ "$size_mb" =~ ^[0-9]+$ ]] || { echo "enospc_fixture.sh: size-mb must be a positive integer, got '$size_mb'" >&2; exit 2; }
  (( size_mb > BALLAST_HEADROOM_MB )) || {
    echo "enospc_fixture.sh: size-mb ($size_mb) must exceed the headroom margin ($BALLAST_HEADROOM_MB MiB)" >&2
    exit 2
  }

  require_sudo

  mkdir -p "$IMAGE_DIR"
  mkdir -p "$dir"
  if is_mounted "$dir"; then
    echo "enospc_fixture.sh: $dir is already mounted; destroy it first" >&2
    exit 2
  fi

  local image
  image="$(image_path_for "$dir")"
  rm -f "$image"

  echo "enospc_fixture.sh: creating ${size_mb}MiB image at $image"
  fallocate -l "${size_mb}M" "$image" || dd if=/dev/zero of="$image" bs=1M count="$size_mb" status=none

  echo "enospc_fixture.sh: formatting ext4 (0% reserved blocks)"
  mkfs.ext4 -q -m 0 "$image"

  echo "enospc_fixture.sh: mounting $image at $dir"
  sudo mount -o loop "$image" "$dir"
  # `${SUDO_UID:-...}`, not `id -u`: this script is normally invoked AS
  # `sudo scripts/enospc_fixture.sh create ...` on a box without passwordless
  # sudo, and inside that `id -u` is 0 -- which chowned the mount back to root
  # and left the test's daemon unable to create its instance dir ("instance dir
  # io error: Permission denied (os error 13)", observed 2026-08-21). SUDO_UID
  # / SUDO_GID name the INVOKING user in exactly that case, and fall back to
  # `id` when the script is run by a real root shell with no sudo in play.
  sudo chown "${SUDO_UID:-$(id -u)}:${SUDO_GID:-$(id -g)}" "$dir"

  # Ballast: fill down to BALLAST_HEADROOM_MB MiB free, measured via `df`
  # AFTER the mount+chown above (ext4 metadata/journal overhead already
  # accounted for by `df`'s available-block count, unlike computing the
  # ballast size from the raw image size).
  local avail_kb ballast_kb
  avail_kb="$(df -Pk "$dir" | awk 'NR==2 {print $4}')"
  ballast_kb=$(( avail_kb - BALLAST_HEADROOM_MB * 1024 ))
  if (( ballast_kb <= 0 )); then
    echo "enospc_fixture.sh: WARNING: only ${avail_kb}KiB available after mount," \
         "less than the ${BALLAST_HEADROOM_MB}MiB headroom margin -- skipping ballast" >&2
  else
    echo "enospc_fixture.sh: writing ballast ($((ballast_kb / 1024))MiB) to leave ~${BALLAST_HEADROOM_MB}MiB free"
    dd if=/dev/zero of="$dir/$BALLAST_NAME" bs=1K count="$ballast_kb" status=none || true
  fi

  echo "enospc_fixture.sh: ready. df -h $dir:"
  df -h "$dir"
}

cmd_destroy() {
  local dir="$1"
  require_sudo

  if is_mounted "$dir"; then
    echo "enospc_fixture.sh: unmounting $dir"
    sudo umount "$dir"
  else
    echo "enospc_fixture.sh: $dir not mounted, nothing to unmount"
  fi

  local image
  image="$(image_path_for "$dir")"
  if [[ -f "$image" ]]; then
    echo "enospc_fixture.sh: removing image $image"
    rm -f "$image"
  fi
}

main() {
  [[ $# -ge 1 ]] || usage
  local sub="$1"; shift
  case "$sub" in
    create)
      [[ $# -eq 2 ]] || usage
      cmd_create "$1" "$2"
      ;;
    destroy)
      [[ $# -eq 1 ]] || usage
      cmd_destroy "$1"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"
