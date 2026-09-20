#!/bin/sh
# arm-manifest-selftest.sh — gate-control for tests/arm-manifest-lint.sh: a well-formed synthetic arm
# drill must be GREEN, and each rule R1–R9 must go RED on its own injection, named. POSIX sh, sub-second.
# origin: simcluster-speed plan §5.5 / §6.3 D-1…D-4′, D-10.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
LINT="$HERE/arm-manifest-lint.sh"
T="${TMPDIR:-/tmp}/arm-manifest-selftest.$$"
mkdir -p "$T/d"
trap 'rm -rf "$T"' EXIT INT TERM
FAIL=0
ok()   { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; FAIL=1; }

good() {
cat <<'EOF'
#!/bin/sh
# 99-synthetic.sh — a well-formed arm-split drill for the lint self-test.
# arms: A B
# fixture: A=N3-live B=N3-live
# grows: A=2 B=2
# worst: A=600 B=120
# forgoes: A=- B="post-A residue"
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
_helper() { true; }
_gap_drill_level() { not_covered "drill-level gap" "counted by every arm" gap; }
drill_begin "99 synthetic ($ARM)"
grow_to_3 2 1
"$SIM" grow brk3
case "${ARM:?}" in
    A)
        _gap_drill_level
        assert_ok "A claim" true
        case "$x" in y) true ;; *) false ;; esac
        ;;
    B)
        _gap_drill_level
        assert_ok "B claim" true
        ;;
    *) setup_fail "unknown arm ${ARM}" ;;
esac
drill_end
EOF
}
lint() { DRILLS="$T/d" sh "$LINT" 2>&1; }
expect_red() { # expect_red <label> <rule-substring>
    out=$(lint); if printf '%s\n' "$out" | grep -q "$2"; then ok "$1"; else fail "$1 — expected '$2', got: $(printf '%s' "$out" | tr '\n' '|')"; fi
}
reset() { good > "$T/d/99-synthetic.sh"; }

reset
if lint >/dev/null; then ok "well-formed arm drill: green"; else fail "well-formed arm drill red: $(lint | tr '\n' '|')"; fi
# A manifest-free drill beside it stays fine.
printf '#!/bin/sh\nassert_ok "plain" true\n' > "$T/d/00-plain.sh"
if lint >/dev/null; then ok "manifest-free neighbour: green"; else fail "manifest-free neighbour red"; fi

reset; sed -i 's/^# fixture:/# fixtures:/' "$T/d/99-synthetic.sh"; expect_red "R1 misspelled key" "R1 misspelled"
reset; sed -i 's/^# arms: A B$/# arms: A A/' "$T/d/99-synthetic.sh"; expect_red "R1 duplicate arm" "R1 duplicate"
reset; sed -i 's/^# fixture: A=N3-live B=N3-live$/# fixture: A=N3-tpl B=N3-live/' "$T/d/99-synthetic.sh"; expect_red "R2 fixture outside vocabulary" "R2 arm A fixture"
reset; sed -i 's/^# worst: A=600 B=120$/# worst: A=600 B=30/' "$T/d/99-synthetic.sh"; expect_red "R2 worst < 60" "R2 arm B worst 30"
# capacity here is 3 (grow_to_3 = 2 + one "$SIM" grow); 3 is allowed, 4 is over.
reset; sed -i 's/^# grows: A=2 B=0$/# grows: A=3 B=0/' "$T/d/99-synthetic.sh"; if lint >/dev/null; then ok "R2 grows == capacity (grow_to_3 counts two) is accepted"; else fail "R2 grows=3 against capacity 3 was refused: $(lint | tr '\n' '|')"; fi
reset; sed -i 's/^# grows: A=2 B=2$/# grows: A=4 B=2/' "$T/d/99-synthetic.sh"; expect_red "R2 grows > capacity (D-10)" "R2 arm A declares grows=4"
reset; sed -i 's/^# forgoes: A=- B="post-A residue"$/# forgoes: A=-/' "$T/d/99-synthetic.sh"; expect_red "R2 missing forgoes token" "R2 arm B has no forgoes"
reset; sed -i 's/^case "${ARM:?}" in$/case "$ARM" in/' "$T/d/99-synthetic.sh"; expect_red "R3 no top-level ARM case" "R3 expected exactly one"
reset; sed -i 's/^    B)$/    C)/' "$T/d/99-synthetic.sh"; expect_red "R4 label not in manifest / arm without label (D-1)" "R4"
reset; sed -i 's/^    \*) setup_fail "unknown arm ${ARM}" ;;$/    *) true ;;/' "$T/d/99-synthetic.sh"; expect_red "R4 star branch without setup_fail" "does not call setup_fail"
reset; sed -i 's/^        assert_ok "B claim" true$/        _inner() { true; }\n        assert_ok "B claim" true/' "$T/d/99-synthetic.sh"; expect_red "R5 function inside the case (D-4′)" "R5"
reset; sed -i 's/^        assert_ok "B claim" true$/        assert_ok "B claim" true || exit 1/' "$T/d/99-synthetic.sh"; expect_red "R6 bare exit inside the case (D-3)" "R6"
# The word inside a claim text is not a command (drill 74: "SS via exit agt2 flows"; `sh -c "! \"$SIM\" …"` has escaped quotes).
reset; sed -i 's/^        assert_ok "B claim" true$/        assert_ok "B claim: SS via exit agt2 flows (exit=agt2)" sh -c "! \\"$SIM\\" ctl -- true; exit 0"/' "$T/d/99-synthetic.sh"
if lint >/dev/null; then ok "R6 the word 'exit' inside quoted claim text / sh -c strings is not a bare exit"; else fail "R6 false positive on quoted 'exit': $(lint | tr '\n' '|')"; fi
reset; awk 'BEGIN{skip=0} /^    B\)$/{print; getline; if ($0 ~ /_gap_drill_level/) next} {print}' "$T/d/99-synthetic.sh" > "$T/x" && mv "$T/x" "$T/d/99-synthetic.sh"; expect_red "R7 arm skips the drill-level gap (D-4)" "R7 arm B"
reset; sed -i 's/^drill_end$//' "$T/d/99-synthetic.sh"; expect_red "R8 no drill_end after esac" "R8"
reset; printf '#!/bin/sh\n# worst: A=100\nassert_ok "plain" true\n' > "$T/d/00-plain.sh"; expect_red "R9 manifest key without arms" "R9 manifest keys"
# The inner `case "$x" in` in arm A must NOT have been read as arm labels (nesting tracked).
reset; printf '#!/bin/sh\nassert_ok "plain" true\n' > "$T/d/00-plain.sh"
if lint >/dev/null; then ok "nested case inside an arm is not mistaken for arm labels"; else fail "nested case confused the label scan: $(lint | tr '\n' '|')"; fi

# round-2 review R3-F8: the shapes the first version let through.
reset; sed -i 's/^        assert_ok "B claim" true$/        assert_ok "B claim" true || exit;;/' "$T/d/99-synthetic.sh"; expect_red "R6 'exit;;' (a terminator right after exit) is still a bare exit" "R6"
reset; sed -i 's/^    \*) setup_fail "unknown arm ${ARM}" ;;$/    *) log "setup_fail would go here" ;;/' "$T/d/99-synthetic.sh"; expect_red "R4 setup_fail only inside a string in the '*)' branch" "R4 the '\*)' branch"
reset; sed -i 's/^# grows: A=2 B=2$/# grows: A=2 B=0/' "$T/d/99-synthetic.sh"; expect_red "R2 an arm under-declares the grows its shared fixture performs (grow_to_3 = 2)" "R2 arm B declares grows=0"
reset; sed -i 's/^# worst: A=600 B=120$/# worst: A=600 B=120\n# worst: A=999 B=999/' "$T/d/99-synthetic.sh"; expect_red "R1 a manifest key declared twice (only the first is read)" "R1 '# worst:' is declared 2 times"

[ "$FAIL" = 0 ] && echo "arm-manifest-selftest: PASS" || { echo "arm-manifest-selftest: FAIL" >&2; exit 1; }
