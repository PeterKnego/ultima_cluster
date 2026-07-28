#!/usr/bin/env python3
"""Per-action file: ReconfigCommitSMT.lean VERBATIM, module renamed, file-scope budget set,
#check_invariants -> #check_action <action>.  usage: make_act.py <action> <timeout> [suffix]"""
import sys
act, tmo = sys.argv[1], sys.argv[2]
suffix = sys.argv[3] if len(sys.argv) > 3 else ""
mod = f"ReconfigCommitSMTAct{act}{suffix}"
src = "/home/claude/veil-spike/veil-preview/Examples/UC/ReconfigCommitSMT.lean"
dst = f"/home/claude/veil-spike/veil-preview/Examples/UC/{mod}.lean"
out = []
for ln in open(src, encoding="utf-8").read().splitlines():
    if ln.startswith("veil module "):
        out.append(f"veil module Uc{mod}")
    elif ln.startswith("set_option veil.smt.timeout"):
        out.append(f"set_option veil.smt.timeout {tmo}")
    elif ln.startswith("#check_invariants"):
        out.append(f"#check_action {act}")
    else:
        out.append(ln)
open(dst, "w", encoding="utf-8").write("\n".join(out) + "\n")
print(f"wrote {dst}")
