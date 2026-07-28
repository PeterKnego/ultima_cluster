#!/bin/bash
# usage: runmod.sh <ModuleBasename> <logpath>
mod="$1"; log="$2"
cd /home/claude/veil-spike/veil-preview || exit 2
start=$(date +%s)
lake build "Examples.UC.$mod" > "$log" 2>&1
rc=$?
echo "EXIT=$rc WALL=$(( $(date +%s) - start ))s" >> "$log"
