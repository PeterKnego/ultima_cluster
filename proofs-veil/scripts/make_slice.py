#!/usr/bin/env python3
"""Build a SLICE of ReconfigCommitSMT.lean: the model VERBATIM (same requires,
assumptions, actions, ghosts) with the invariant conjunction cut to a recorded subset.
usage: make_slice.py <ModuleBasename> <timeout> <checkcmd> <clause> [clause...]
  checkcmd = 'inv' (#check_invariants) or an action name (#check_action <action>)"""
import sys, re, os

mod, tmo, check = sys.argv[1], sys.argv[2], sys.argv[3]
keep = set(sys.argv[4:])
src = "/home/claude/veil-spike/veil-preview/Examples/UC/ReconfigCommitSMT.lean"
dst = f"/home/claude/veil-spike/veil-preview/Examples/UC/{mod}.lean"
lines = open(src, encoding="utf-8").read().splitlines()

out, i, seen = [], 0, set()
BLOCK = re.compile(r'^(invariant|safety) \[(\w+)\]')
while i < len(lines):
    ln = lines[i]
    m = BLOCK.match(ln)
    if m:
        name = m.group(2)
        j = i + 1
        while j < len(lines) and not (BLOCK.match(lines[j]) or lines[j].startswith('#gen_spec')
                                      or lines[j].startswith('#check')):
            j += 1
        # trailing comment lines belong to the NEXT block; keep them with neither
        body = lines[i:j]
        while body and (body[-1].startswith('--') or body[-1].strip() == ''):
            body.pop()
        if name in keep:
            out.extend(body)
            seen.add(name)
        i = j
        continue
    if ln.startswith('veil module '):
        out.append(f'veil module {mod}')
    elif ln.startswith('set_option veil.smt.timeout'):
        out.append(f'set_option veil.smt.timeout {tmo}')
    elif ln.startswith('#check_invariants') or ln.startswith('#check_action'):
        out.append('#check_invariants' if check == 'inv' else f'#check_action {check}')
    elif ln.startswith('--') and i > 900:
        pass
    else:
        out.append(ln)
    i += 1

missing = keep - seen
if missing:
    sys.exit(f"ERROR: clauses not found: {sorted(missing)}")
open(dst, "w", encoding="utf-8").write("\n".join(out) + "\n")
print(f"wrote {dst}: {len(seen)} clauses kept: {sorted(seen)}")
