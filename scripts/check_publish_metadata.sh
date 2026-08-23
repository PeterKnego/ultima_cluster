#!/usr/bin/env bash
# Guard against publish-time metadata crates.io rejects that nothing else in
# the local proof stack catches: `cargo package`/`cargo publish --dry-run`
# both stop before the server-side validation crates.io actually runs at
# upload time, so a bad keyword or category slug sails through green locally
# and only fails once you're really trying to publish. Caught for real:
# M12c Task 1 review found `"state-machine-replication"` (25 chars) in
# `[workspace.package] keywords` — crates.io's limit is 20.
#
# Checks every PUBLISHABLE workspace crate (cargo metadata's `publish: null`;
# `publish: []` == `publish = false`, skipped) against crates.io's real
# constraints:
#   * at most 5 keywords
#   * each keyword at most 20 characters
#   * each category is a real crates.io category slug
#
# Run from anywhere; cds to the repo root itself.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0

metadata="$(cargo metadata --format-version 1 --no-deps)"

# --- keywords: count <= 5, each entry <= 20 chars -----------------------
while IFS=$'\t' read -r name kw; do
  len=${#kw}
  if (( len > 20 )); then
    echo "FAIL: $name keyword \"$kw\" is $len chars (crates.io max 20)" >&2
    fail=1
  fi
done < <(echo "$metadata" | jq -r '
  .packages[] | select(.publish == null) | .name as $n
  | (.keywords // [])[] | "\($n)\t\(.)"
')

while IFS=$'\t' read -r name count; do
  if (( count > 5 )); then
    echo "FAIL: $name has $count keywords (crates.io max 5)" >&2
    fail=1
  fi
done < <(echo "$metadata" | jq -r '
  .packages[] | select(.publish == null)
  | "\(.name)\t\((.keywords // []) | length)"
')

# --- categories: every slug must be a real crates.io category -----------
used_categories="$(echo "$metadata" | jq -r '
  .packages[] | select(.publish == null) | (.categories // [])[]
' | sort -u)"

known_categories=""
if live="$(curl -fsS --max-time 10 'https://crates.io/api/v1/categories?per_page=100' \
             -H 'User-Agent: ultima_cluster-ci-check (peter@knego.net)' 2>/dev/null)"; then
  # crates.io paginates at 100; page again if there's more than one page.
  all_pages="$live"
  page=2
  total="$(echo "$live" | jq -r '.meta.total_pages // 1')"
  while (( page <= total )); do
    next="$(curl -fsS --max-time 10 "https://crates.io/api/v1/categories?per_page=100&page=$page" \
              -H 'User-Agent: ultima_cluster-ci-check (peter@knego.net)' 2>/dev/null || true)"
    [[ -z "$next" ]] && break
    all_pages="$all_pages"$'\n'"$next"
    page=$((page + 1))
  done
  known_categories="$(echo "$all_pages" | jq -r 'select(.categories) | .categories[].slug' | sort -u)"
fi

if [[ -z "$known_categories" ]]; then
  echo "WARN: could not reach crates.io categories API; falling back to the" >&2
  echo "WARN: small embedded allowlist of categories this workspace actually uses." >&2
  # Verified against crates.io on 2026-08-23 (both are real, long-established
  # category slugs — e.g. tokio uses network-programming, sled/sanakirja use
  # database-implementations). Extend this list if a crate adds a new one
  # AND you've confirmed it against https://crates.io/category_slugs.
  known_categories=$'network-programming\ndatabase-implementations'
fi

while IFS= read -r cat; do
  [[ -z "$cat" ]] && continue
  if ! grep -qxF "$cat" <<< "$known_categories"; then
    echo "FAIL: category \"$cat\" is not a known crates.io category slug" >&2
    fail=1
  fi
done <<< "$used_categories"

if (( fail == 0 )); then
  echo "ok: publish metadata (keywords, categories) within crates.io limits"
fi
exit $fail
