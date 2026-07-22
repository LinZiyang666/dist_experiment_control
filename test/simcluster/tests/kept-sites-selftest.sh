#!/bin/sh
# kept-sites-selftest.sh (N-7) — pins the two properties the kept-sites ratchet + quote-mask depend on,
# so they cannot silently rot: (1) the quote-mask holds — a primitive keyword buried in a QUOTED
# description string is NOT counted; (2) dropping a real assertion site REDs `--check`. Pure awk/sh, no
# docker. Wired into run-all.sh. Uses the env-override of DRILLS (test-only) to point at a scratch fixture.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
KS="$HERE/kept-sites.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT INT TERM
RC=0

DRILLDIR="$TMP/drills"
mkdir -p "$DRILLDIR"

# A scratch drill with exactly FOUR real call sites (3 assert_ok + 1 assert_refuses). Two description
# strings — one DOUBLE-quoted, one SINGLE-quoted — deliberately name `assert_ok`, `product_red` and a `;`
# separator; none may count, because they sit inside a quoted string, not at a command position. Both
# quote kinds are covered so BOTH arms of the mask (the "..." arm and the \047...\047 arm) are pinned —
# deleting either arm from kept-sites.sh makes its buried keywords leak into the count and REDs this test.
cat > "$DRILLDIR/zz-selftest.sh" <<'DRILL'
drill_begin zz
assert_ok "real one" true
assert_ok "double-quoted prose naming assert_ok; product_red must NOT count as sites" true
assert_ok 'single-quoted prose naming assert_ok; product_red must NOT count either' true
assert_refuses "real two" nope
drill_end
DRILL

got=$(DRILLS="$DRILLDIR" sh "$KS" | awk -F'\t' '$1=="zz-selftest"{print $2}')
if [ "$got" = 4 ]; then
    echo "ok   quote-mask: keywords inside double- AND single-quoted descriptions are not counted (4 real sites)"
else
    echo "FAIL quote-mask: zz-selftest counted '$got', want 4 — a keyword inside a quoted string leaked into the count" >&2
    RC=1
fi

# Property 2: dropping a real assertion must RED --check. Baseline the scratch drill at its floor (4), then
# delete a real assert_ok (count -> 3 < 4) and confirm --check fails (exit non-zero).
printf 'zz-selftest\t4\n' > "$TMP/base.tsv"
grep -v 'real one' "$DRILLDIR/zz-selftest.sh" > "$TMP/zz.tmp" && mv "$TMP/zz.tmp" "$DRILLDIR/zz-selftest.sh"
if DRILLS="$DRILLDIR" sh "$KS" --check "$TMP/base.tsv" >/dev/null 2>&1; then
    echo "FAIL ratchet: deleting a real assert_ok did NOT RED --check (the floor is not guarding deletion)" >&2
    RC=1
else
    echo "ok   ratchet: deleting a real assertion site REDs --check"
fi

[ "$RC" = 0 ] && echo "kept-sites-selftest: PASS" || echo "kept-sites-selftest: FAIL" >&2
exit "$RC"
