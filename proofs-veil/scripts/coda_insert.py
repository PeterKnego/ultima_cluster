#!/usr/bin/env python3
"""Insert the stub-derived theorem + a proof body into a slice module, between
#gen_spec and #check_action.  usage: coda_insert.py <Module> <proofbody-file>"""
import sys
mod, body = sys.argv[1], sys.argv[2]
path = f"/home/claude/veil-spike/veil-preview/Examples/UC/{mod}.lean"
stub = open("/home/claude/veil-spike/runs/coda-stub-CE.txt", encoding="utf-8").read().rstrip("\n")
# stub ends with "  by\n  unveil"; keep the statement + `by`, drop the placeholder `unveil`
lines = stub.splitlines()
assert lines[-1].strip() == "unveil" and lines[-2].strip() == "by", lines[-3:]
head = "\n".join(lines[:-1])          # statement + "  by"
proof = open(body, encoding="utf-8").read().rstrip("\n")
src = open(path, encoding="utf-8").read().splitlines()
out = []
for ln in src:
    if ln.startswith("#check_action"):
        out.append(head)
        out.append(proof)
        out.append("")
    out.append(ln)
open(path, "w", encoding="utf-8").write("\n".join(out) + "\n")
print(f"inserted into {path}")
