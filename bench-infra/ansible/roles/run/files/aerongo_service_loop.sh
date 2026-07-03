#!/usr/bin/env bash
# Supervises the aeron-go echo service. The Go agent panics if the media
# driver / consensus module are not up yet, so retry until they are; once
# running it blocks for the whole sweep. Killed by the teardown pkill.
set -u
BIN="$1"; AERON="$2"; CLUSTER="$3"
export AERON_DIR="$AERON" CLUSTER_DIR="$CLUSTER" NO_OP_IDLE=1
for _ in $(seq 1 120); do
  "$BIN"
  sleep 1
done
