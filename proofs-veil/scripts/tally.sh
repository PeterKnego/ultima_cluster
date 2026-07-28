#!/bin/bash
for f in "$@"; do
  printf "%-34s %3s ✅ / %2s ❌ / %2s ⏱️   %s\n" "$(basename $f)" \
    "$(grep -c '✅' $f)" "$(grep -c '❌' $f)" "$(grep -c '⏱️' $f)" "$(grep -o 'EXIT=[0-9]* WALL=[0-9]*s' $f)"
done
