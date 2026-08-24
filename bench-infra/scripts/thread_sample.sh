#!/usr/bin/env bash
# Per-thread CPU + current syscall/wchan of one process over a window.
# usage: thread_sample.sh <pid> <secs>
# Prints: tid name cpu% (of one core) syscall_nr wchan — sorted by cpu%.
set -u
PID=$1; SECS=${2:-3}
HZ=$(getconf CLK_TCK)
declare -A T0 NAME
for t in /proc/$PID/task/*; do
  tid=${t##*/}
  read -r -a f < "$t/stat" 2>/dev/null || continue
  # fields after the ")" : utime is 14th, stime 15th (1-based) of the stat line
  s=$(cat "$t/stat" 2>/dev/null | sed 's/.*) //')
  set -- $s
  T0[$tid]=$(( ${12} + ${13} ))
  NAME[$tid]=$(cat "$t/comm" 2>/dev/null)
done
sleep "$SECS"
printf '%-8s %-18s %7s  %-10s %s\n' tid name cpu% syscall wchan
for t in /proc/$PID/task/*; do
  tid=${t##*/}
  [ -n "${T0[$tid]:-}" ] || continue
  s=$(cat "$t/stat" 2>/dev/null | sed 's/.*) //') || continue
  set -- $s
  t1=$(( ${12} + ${13} ))
  d=$(( t1 - ${T0[$tid]} ))
  pct=$(awk -v d=$d -v hz=$HZ -v s=$SECS 'BEGIN{printf "%.1f", 100*d/hz/s}')
  sc=$(cut -d' ' -f1 "$t/syscall" 2>/dev/null)
  wc=$(cat "$t/wchan" 2>/dev/null)
  printf '%-8s %-18s %7s  %-10s %s\n' "$tid" "${NAME[$tid]}" "$pct" "$sc" "$wc"
done | sort -k3 -n -r | head -24
