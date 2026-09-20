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

# Property 3 (plan §6.3 D-6; round-2 review R1-F2 / R3-F3): an ARM-SPLIT drill is keyed per arm. Moving a
# site from one arm to its sibling keeps the drill total (the old, blind reading) but lowers the source
# arm's row — and only the arm rows make that visible. The fixture: 1 shared site, 2 in A, 1 in B.
cat > "$DRILLDIR/za-arms.sh" <<'DRILL'
# arms: A B
# fixture: A=N1 B=N1
# grows: A=0 B=0
# worst: A=60 B=60
# forgoes: A=- B=-
drill_begin za
assert_setup "shared fixture" true
case "${ARM:?}" in
    A)
        assert_ok "a-one" true
        assert_ok "a-two" true
        ;;
    B)
        assert_ok "b-one" true
        ;;
    *) setup_fail "unknown arm" ;;
esac
drill_end
DRILL
rows=$(DRILLS="$DRILLDIR" sh "$KS" | awk -F'\t' '$1 ~ /^za-arms/ { printf "%s=%s ", $1, $2 }')
if [ "$rows" = "za-arms=4 za-arms.A=2 za-arms.B=1 za-arms._shared=1 " ]; then
    echo "ok   arm rows: an arm-split drill reports its total plus per-arm and _shared rows"
else
    echo "FAIL arm rows: got '$rows', want 'za-arms=4 za-arms.A=2 za-arms.B=1 za-arms._shared=1 '" >&2
    RC=1
fi
DRILLS="$DRILLDIR" sh "$KS" | grep '^za-arms' > "$TMP/arms.base"
# Move a-two from A into B: total 4 → 4, A 2 → 1, B 1 → 2.
awk '/a-two/ { held = $0; next } /"b-one"/ { print; print held; next } { print }' "$DRILLDIR/za-arms.sh" > "$TMP/za.tmp" && mv "$TMP/za.tmp" "$DRILLDIR/za-arms.sh"
if DRILLS="$DRILLDIR" sh "$KS" --check "$TMP/arms.base" >/dev/null 2>&1; then
    echo "FAIL arm rows: moving a site from arm A to arm B did NOT RED --check (the total hides the move)" >&2
    RC=1
else
    echo "ok   arm rows: a site moved between arms REDs --check while the drill total is unchanged"
fi

[ "$RC" = 0 ] && echo "kept-sites-selftest: PASS" || echo "kept-sites-selftest: FAIL" >&2
exit "$RC"
