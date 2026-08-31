#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""Check the documentation's internal links against BOTH renderers we use.

Why this exists, and why it is Python in a directory of shell scripts: the
checks below are not expressible in shell without becoming unreadable, and
`bench-infra/scripts/` is already Python, so the toolchain is not new.

The motivating bug (2026-08-31): two links were valid on GitHub and dead in
the terminal viewer, for two DIFFERENT reasons, and neither a human nor a
file-existence check caught either. Markdown link rot is silent — the page
still renders, the link just goes nowhere — so it needs a machine.

Four classes are checked. Each has actually happened in this repo:

  1. DEAD FILE    a relative link to a path that does not exist.
  2. DEAD ANCHOR  `#foo` with no matching heading, under GitHub's slug rules
                  (lowercase, punctuation dropped, `_` and `-` KEPT, spaces
                  -> `-`, duplicates suffixed `-1`, `-2`, ...).
  3. MDTUI ANCHOR the same link under md-tui's rules, which differ in TWO
                  ways (`src/search.rs`'s `compare_heading`):
                    - it filters heading chars to `is_alphanumeric() || '-'`,
                      so it DROPS underscores where GitHub keeps them; and
                    - it `dedup_by`s consecutive '-', so an em-dash in a
                      heading yields ONE hyphen where GitHub yields two.
                  No anchor can satisfy both renderers in either case.
  4. INERT LINK   a link nested inside single-`*` emphasis. md-tui's grammar
                  (`src/md.pest`) defines `p_char` to exclude `link` — which
                  is what lets a link break out of ordinary text — but the
                  italic rules are built from `i_char_var_*` with no `link`
                  alternative, so a link inside `*...*` is consumed as
                  literal characters and never becomes a link node. It
                  renders as plain italic text with nothing to follow.

Classes 1, 2 and 4 are ERRORS: they are defects on the canonical published
surface (GitHub) or links that cannot be followed anywhere. Class 3 is a
WARNING, deliberately. GitHub is what the docs are published on; md-tui is a
local viewer whose slugger diverges. Making its quirks fatal would put 239
em-dashed headings — including ones inside gate docs, which are permanent
records this project does not rewrite — hostage to a third-party TUI. The
warning still names each divergence so it can be fixed where the heading is
cheap to change (a new doc), and ignored where it is not (a 2026 gate doc).

Exit 0 if no ERRORS, 1 otherwise; `--strict` makes warnings fatal too.
`--list` prints every checked link.
"""

import collections
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Historical records and vendored trees: true when written, not maintained.
SKIP = (
    "docs/superpowers/",
    "docs/tasks/",
    "proofs/.lake/",
    "target/",
    ".terraform",
    ".claude/worktrees/",
    "fuzz/target/",
)

LINK = re.compile(r"\[([^\]^]*)\]\(([^)\s]+)\)")


def _plain(heading: str) -> str:
    """Strip inline markup a renderer resolves before slugging."""
    h = re.sub(r"`([^`]*)`", r"\1", heading)
    h = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", h)
    h = re.sub(r"[*_]{1,3}(.+?)[*_]{1,3}", r"\1", h)
    return h.strip()


def slug_github(heading: str) -> str:
    h = _plain(heading).lower()
    h = re.sub(r"[^\w\s-]", "", h)  # \w keeps letters, digits and '_'
    return h.replace(" ", "-")


def slug_mdtui(heading: str) -> str:
    """md-tui `compare_heading`: words joined by '-', filtered to
    alnum-or-'-', then consecutive '-' de-duplicated."""
    joined = "-".join(w.lower() for w in _plain(heading).split())
    out = "".join(c for c in joined if c.isalnum() or c == "-")
    return re.sub(r"-+", "-", out).strip("-")


def mdtui_cause(anchor: str) -> str:
    """Which md-tui divergence makes `anchor` unreachable there."""
    if "_" in anchor:
        return "md-tui drops '_' from heading slugs"
    if "--" in anchor:
        return "md-tui collapses repeated '-' (an em-dash in the heading)"
    return "md-tui slugs this heading differently"


def headings(path: pathlib.Path):
    """(github_anchors, mdtui_anchors) for one file, ignoring fenced code."""
    gh, mt, seen = set(), set(), collections.Counter()
    fenced = False
    for line in path.read_text(errors="replace").splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        m = re.match(r"^#{1,6}\s+(.*?)\s*$", line)
        if not m:
            continue
        g = slug_github(m.group(1))
        seen[g] += 1
        gh.add(g if seen[g] == 1 else f"{g}-{seen[g] - 1}")
        mt.add(slug_mdtui(m.group(1)))
    return gh, mt


def italic_blocks(path: pathlib.Path):
    """Line ranges (1-based, inclusive) of whole-paragraph single-* emphasis.

    Only whole blocks: a paragraph that OPENS with a lone `*` and runs to the
    line whose text ends with one. Inline `*word*` inside a sentence is not a
    container for a link and is not flagged.
    """
    lines = path.read_text(errors="replace").splitlines()
    out, i, fenced = [], 0, False
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith("```"):
            fenced = not fenced
            i += 1
            continue
        if fenced or not stripped.startswith("*") or stripped.startswith(("**", "* ")):
            i += 1
            continue
        j = i
        while j < len(lines) and lines[j].strip():
            j += 1
            end = lines[j - 1].rstrip()
            if end.endswith("*") and not end.endswith("**"):
                out.append((i + 1, j))
                break
        i = max(j, i + 1)
    return out


def md_files():
    for p in sorted(ROOT.rglob("*.md")):
        rel = p.relative_to(ROOT).as_posix()
        if not any(s in rel for s in SKIP):
            yield p


def main() -> int:
    listing = "--list" in sys.argv
    strict = "--strict" in sys.argv
    errors = 0
    warnings = 0
    checked = 0

    for path in md_files():
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(errors="replace")
        lines = text.splitlines()

        # 4. links inside whole-paragraph emphasis
        for start, end in italic_blocks(path):
            block = "\n".join(lines[start - 1 : end])
            for m in LINK.finditer(block):
                print(
                    f"ERROR  INERT LINK   {rel}:{start}-{end}: "
                    f"[{m.group(1)}]({m.group(2)})"
                    "  -- inside *...*, md-tui renders it as plain text"
                )
                errors += 1

        # 1-3. targets and anchors
        for m in LINK.finditer(text):
            label, link = m.group(1), m.group(2)
            if link.startswith(("http://", "https://", "mailto:", "#!")):
                continue
            filepart, _, anchor = link.partition("#")
            line_no = text[: m.start()].count("\n") + 1

            if filepart:
                target = (
                    ROOT / filepart.lstrip("/")
                    if filepart.startswith("/")
                    else path.parent / filepart
                )
                if not target.exists():
                    print(f"ERROR  DEAD FILE    {rel}:{line_no}: [{label}]({link})")
                    errors += 1
                    continue
            else:
                target = path

            checked += 1
            if listing:
                print(f"  ok {rel}:{line_no} -> {link}")
            if not anchor or target.suffix != ".md" or not target.is_file():
                continue

            gh, mt = headings(target)
            if anchor not in gh:
                print(
                    f"ERROR  DEAD ANCHOR  {rel}:{line_no}: [{label}]({link})"
                    f"  -- no heading slugs to #{anchor} on GitHub"
                )
                errors += 1
            elif anchor not in mt:
                print(
                    f"warn   MDTUI ANCHOR {rel}:{line_no}: [{label}]({link})"
                    f"  -- fine on GitHub; {mdtui_cause(anchor)}"
                )
                warnings += 1

    print(
        f"--- {checked} internal link(s) checked: "
        f"{errors} error(s), {warnings} md-tui warning(s) ---"
    )
    return 1 if errors or (strict and warnings) else 0


if __name__ == "__main__":
    sys.exit(main())
