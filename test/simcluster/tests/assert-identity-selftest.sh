#!/bin/sh
# assert-identity-selftest.sh — the gate-control for tests/assert-identity.sh: a synthetic drill tree on
# which the gate must be GREEN when nothing moved and RED for each of the shapes it exists to catch.
# POSIX sh. No docker. Sub-second.
#
# The load-bearing row is the SWAP (D-5): delete one primitive+description and add a different one so the
# per-drill COUNT is unchanged — kept-sites.sh stays green on that edit by construction; only an identity
# multiset can see it. The others pin the multiset semantics (a duplicated row is two sites) and the
# fail-closed reading of a missing baseline / a missing drill.
# origin: simcluster-speed plan §6.3 D-5.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
GATE="$HERE/assert-identity.sh"
T="${TMPDIR:-/tmp}/assert-identity-selftest.$$"
mkdir -p "$T/drills"
trap 'rm -rf "$T"' EXIT INT TERM
FAIL=0
ok()   { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; FAIL=1; }

cat > "$T/drills/aa-one.sh" <<'EOF'
#!/bin/sh
drill_begin "aa"
assert_ok "up 1 broker" "$SIM" up --brokers 1
assert_refuses "force-single refuses while a peer is alive" 'peer_alive' "$SIM" force-single brk1
# assert_ok "this is prose, not a site"
if ! _probe; then not_covered "#99 face B" "the fixture did not land" gap; fi
assert_ok "dup row" true; assert_ok "dup row" true
drill_end
EOF
cat > "$T/drills/bb-two.sh" <<'EOF'
#!/bin/sh
assert_setup 'single-quoted desc with "inner" quotes' true
product_red "#42 known defect" "sig" false
EOF

DRILLS="$T/drills" sh "$GATE" > "$T/base.tsv"
if [ "$(wc -l < "$T/base.tsv")" -eq 7 ]; then ok "tokenizer: 7 sites (prose comment ignored, dup row counted twice, single-quoted desc kept)"; else fail "tokenizer saw $(wc -l < "$T/base.tsv") sites, want 7: $(cat "$T/base.tsv")"; fi
grep -q "aa-one	assert_ok	dup row" "$T/base.tsv" || fail "dup row missing"
[ "$(grep -c "aa-one	assert_ok	dup row" "$T/base.tsv")" -eq 2 ] || fail "dup row must appear twice (multiset)"
grep -q 'bb-two	assert_setup	single-quoted desc with "inner" quotes' "$T/base.tsv" || fail "single-quoted description not captured verbatim"

if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" >/dev/null 2>&1; then ok "unchanged tree: --check green"; else fail "unchanged tree reported as moved"; fi

# D-5 THE SWAP: remove the refusal, add an unrelated assert_ok — count unchanged, identity changed.
cp "$T/drills/aa-one.sh" "$T/aa.orig"
sed 's/^assert_refuses "force-single refuses while a peer is alive" .*$/assert_ok "cluster status prints" "$SIM" status/' "$T/aa.orig" > "$T/drills/aa-one.sh"
if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" > "$T/out" 2>&1; then fail "D-5 swap (count-neutral) was NOT detected"; else
    if grep -q -- '- aa-one	assert_refuses	force-single refuses' "$T/out" && grep -q -- '+ aa-one	assert_ok	cluster status prints' "$T/out"; then ok "D-5 swap: refusal dropped + print added, both named"; else fail "D-5 swap detected but not named: $(cat "$T/out")"; fi
fi
cp "$T/aa.orig" "$T/drills/aa-one.sh"

# A reworded description alone is a move (the claim's identity is its text).
sed 's/up 1 broker/up one broker/' "$T/aa.orig" > "$T/drills/aa-one.sh"
if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" >/dev/null 2>&1; then fail "reworded description not detected"; else ok "reworded description detected"; fi
cp "$T/aa.orig" "$T/drills/aa-one.sh"

# Deleting one of the two duplicate rows must be seen (multiset, not set).
grep -v '^assert_ok "dup row" true; assert_ok "dup row" true$' "$T/aa.orig" > "$T/drills/aa-one.sh"
printf 'assert_ok "dup row" true\n' >> "$T/drills/aa-one.sh"
if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" >/dev/null 2>&1; then fail "losing one of two identical rows not detected"; else ok "multiset: one of two identical rows lost is detected"; fi
cp "$T/aa.orig" "$T/drills/aa-one.sh"

# A drill vanishing entirely is the extreme case and must be red.
mv "$T/drills/bb-two.sh" "$T/bb.orig"
if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" >/dev/null 2>&1; then fail "a vanished drill not detected"; else ok "vanished drill detected"; fi
mv "$T/bb.orig" "$T/drills/bb-two.sh"

# Fail-closed on a missing baseline path.
if DRILLS="$T/drills" sh "$GATE" --check "$T/nope.tsv" >/dev/null 2>&1; then fail "missing baseline accepted"; else ok "missing baseline refused"; fi

# BLIND SHAPES (internal review round 1 R3-8). Control words in front of a primitive still run it and must be
# SEEN as ordinary rows; a primitive reached through a wrapper (assignment prefix, eval, sh -c, command) is
# invisible to this gate and to kept-sites at run time, so the gate must refuse to run (exit 3) and name the
# site rather than silently produce a listing without it. Prose naming a primitive inside a string is neither.
printf '%s\n' '#!/bin/bash' 'if ! assert_ok "seen through if-not" true; then :; fi' 'elif assert_ok "seen through elif" true; then :; fi' \
    'log "prose mentioning assert_ok inside a string is not a site"' > "$T/drills/cc-ctl.sh"
if out=$(DRILLS="$T/drills" sh "$GATE" 2>&1); then
    if printf '%s' "$out" | grep -q 'cc-ctl	assert_ok	seen through if-not' && printf '%s' "$out" | grep -q 'cc-ctl	assert_ok	seen through elif' \
       && ! printf '%s' "$out" | grep -q 'prose mentioning'; then ok "control-word prefixes are seen; quoted prose is not a site"; else fail "control-word rows wrong: $out"; fi
else fail "control-word prefixes made the gate refuse (rc=$?): $out"; fi
# The fifth shape (round-2 review R3-F9) is a drill-DEFINED wrapper: `_claim() { assert_ok "$1" "$2"; }`
# records one row (`assert_ok $1`) for N call sites. A variable where the description should be is the
# tell, and it is refused like the other wrappers.
for shape in 'FOO=1 assert_ok "wrapped by an assignment" true' "eval 'assert_ok \"inside eval\" true'" 'sh -c "assert_ok \"inside sh -c\" true"' 'command assert_ok "via command" true' \
    '_claim() { assert_ok "$1" "$2"; }'; do
    printf '%s\n' '#!/bin/bash' "$shape" > "$T/drills/cc-ctl.sh"
    if DRILLS="$T/drills" sh "$GATE" > "$T/out" 2>&1; then fail "blind shape accepted silently: $shape"; else
        if [ "$?" = 3 ] && grep -q 'INVISIBLE' "$T/out" && grep -q 'cc-ctl:' "$T/out"; then ok "blind shape refused and named: $shape"; else fail "blind shape: wrong rc/text for $shape: $(cat "$T/out")"; fi
    fi
    # --check must refuse for the same reason, not report "every baseline row missing".
    if DRILLS="$T/drills" sh "$GATE" --check "$T/base.tsv" > "$T/out" 2>&1; then fail "--check accepted a blind shape: $shape"; else
        if grep -q 'INVISIBLE' "$T/out" && ! grep -q 'NOT in the live tree' "$T/out"; then :; else fail "--check on a blind shape reported the wrong reason: $(head -3 "$T/out")"; fi
    fi
done
rm -f "$T/drills/cc-ctl.sh"

[ "$FAIL" = 0 ] && echo "assert-identity-selftest: PASS" || { echo "assert-identity-selftest: FAIL" >&2; exit 1; }
