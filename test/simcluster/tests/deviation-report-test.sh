#!/bin/sh
# deviation-report-test.sh — HERMETIC gate for the M3/M4/V1 machinery in run-drills.sh: the expectation
# diff, the deviation report, longest-first launch ordering, the progress stream, and the additive
# attribution pass. No docker, no tether, no network — synthetic drills that just print a verdict line.
#
# WHY THIS EXISTS. On 2026-07-23 the runner printed "14 BLOCKER(S)"; nine were recorded, owned,
# deliberate INCOMPLETEs and two were a genuine product regression (c6b9c9e's unswept --reset-js gate).
# Nothing in the runner could tell them apart, so a human spent an evening doing it by hand. The
# machinery below is what does it now — and this file is what stops that machinery from quietly
# regressing into a wall of green that checks nothing, which is exactly the failure it was built for.
#
# THE INVARIANT THAT MATTERS MOST: the match axis is REPORTING, never authority. No expectation, no
# band, and no attribution label may change a verdict, a blocker count, or the exit code. Several cases
# below exist solely to pin that.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

command -v bash >/dev/null 2>&1 || { echo "  (skipped — no bash; run-drills.sh needs arrays)"; exit 0; }

RT=$(mktemp -d); mkdir -p "$RT/drills"
trap 'rm -rf "$RT"' EXIT
ln -sf "$SIM_ROOT/run-drills.sh" "$RT/run-drills.sh"
ln -sf "$SIM_ROOT/lib" "$RT/lib"    # so a drill can source the REAL lib/assert.sh (B1/B2 cases)
# The stub sets INSTANCE=drill-<name> exactly as the real simcluster does, so drill_begin's throwaway-instance
# guard passes and, for the B1/B2 cases, the flight recorder actually runs.
printf '#!/bin/sh\n[ "$1" = check-image ] && exit 0\n[ "$1" = drill ] || exit 9\nexport INSTANCE="drill-$2"\nexec sh "$(dirname "$0")/drills/$2.sh"\n' > "$RT/simcluster"
chmod +x "$RT/simcluster"

# mkd <name> <verdict> <rc> [nc_gap] [sleep] [first-failure-text]
# The 6th arg, when set, is emitted as a `[simcluster]` CAUSE diagnostic line followed by an `[err ] FAIL`
# title line, both carrying <text>, BEFORE the verdict. This mirrors a real drill: the band matcher reads
# only CAUSE diagnostics (`[simcluster]`/`[warn]` lines before the first `[err]`, per the runner's
# `_fail_context`) so a title-only red cannot launder a band; the deviation report's first-failure signature
# still reads the `[err]` title. A band case controls matching via the CAUSE line's text.
mkd() {
    af=0; sr=0; pr=0; nc=0; ncg="${4:-0}"; slp="${5:-0}"; ffl="${6:-}"
    case "$2" in
        ASSERT-FAIL) af=1 ;; SETUP-RED) sr=1 ;; PRODUCT-RED) pr=1 ;;
        INCOMPLETE)  nc="${4:-1}"; ncg="${4:-1}" ;;
    esac
    {
        printf '#!/bin/sh\n'
        [ "$slp" != 0 ] && printf 'sleep %s\n' "$slp"
        [ -n "$ffl" ] && printf 'printf "[simcluster] cause: %s\\n" >&2\nprintf "[err ] FAIL  %s\\n" >&2\n' "$ffl" "$ffl"
        printf 'printf "DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s nc_gap=%s nc_guard=0 pass=1 -- %s\\n" >&2\n' \
            "$2" "$3" "$af" "$sr" "$pr" "$nc" "$ncg" "$1"
        printf 'exit %s\n' "$3"
    } > "$RT/drills/$1.sh"
}

# The band signature dictionary the runner resolves sig:<slug> against (B3). Bands in this test point at it.
cat > "$RT/expected-verdicts-log.md" <<'EOF'
## band signatures
  sig:known-band := known-band-signature-here
EOF

# The expectation table under test. Deliberately mirrors the 2026-07-23 shapes: a drill expected GREEN
# that reds (the c6b9c9e class), a drill expected INCOMPLETE that reds differently, a drill with a band,
# and a drill whose gap COUNT changed while its verdict enum did not.
mk_tsv() {
    cat > "$RT/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
d-green	GREEN	0	-	-	d-green
d-was-green	GREEN	0	-	-	d-was-green
d-inc	INCOMPLETE	2	-	#77	d-inc
d-inc-gapdrift	INCOMPLETE	2	-	#77	d-inc-gapdrift
d-band-match	INCOMPLETE	1	ASSERT-FAIL@#34@sig:known-band	#34	d-band-match
d-band-nomatch	INCOMPLETE	1	ASSERT-FAIL@#34@sig:known-band	#34	d-band-nomatch
EOF
}
mk_tsv

echo "── expectation diff (M3) ───────────────────────────────────────────────────────"
mkd d-green        GREEN       0
mkd d-was-green    ASSERT-FAIL 1 0 0 "boom: --reset-js required"
mkd d-inc          INCOMPLETE  4 2
mkd d-inc-gapdrift INCOMPLETE  4 3
mkd d-band-match   ASSERT-FAIL 1 0 0 "known-band-signature-here and more"
mkd d-band-nomatch ASSERT-FAIL 1 0 0 "a BRAND-NEW unrelated failure"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l1" \
        d-green d-was-green d-inc d-inc-gapdrift d-band-match d-band-nomatch 2>&1); rc=$?
tsv="$RT/l1/rollup.tsv"
m() { awk -F'\t' -v d="$1" 'NF>=15 && $1==d {print $15}' "$tsv"; }

[ "$(m d-green)" = MATCH ] && pass "a drill matching its expectation → MATCH" || fail "d-green: $(m d-green)"
[ "$(m d-was-green)" = DEVIATION ] && pass "expected GREEN, got ASSERT-FAIL → DEVIATION" || fail "d-was-green: $(m d-was-green)"
[ "$(m d-inc)" = MATCH ] && pass "expected INCOMPLETE with the recorded gap count → MATCH" || fail "d-inc: $(m d-inc)"
# The gap-count check is the one that catches a drill silently losing or gaining coverage while its
# verdict enum stays put — invisible to any enum-only comparison.
[ "$(m d-inc-gapdrift)" = DEVIATION ] && pass "same verdict, DIFFERENT nc_gap → DEVIATION" || fail "d-inc-gapdrift: $(m d-inc-gapdrift)"
# B3: the band's SIGNATURE decides, not the verdict enum alone. Same enum (ASSERT-FAIL), matching
# first-failure text → MATCH-BAND; same enum, DIFFERENT text → DEVIATION. Matching on the enum alone let a
# band absorb a brand-new unrelated red — the "20/91 blindness inside every banded drill" the plan forbids.
case "$(m d-band-match)" in MATCH-BAND*) pass "band + matching signature → MATCH-BAND(#34)" ;; *) fail "d-band-match: $(m d-band-match)" ;; esac
[ "$(m d-band-nomatch)" = DEVIATION ] && pass "band + NON-matching signature (same enum) → DEVIATION" || fail "d-band-nomatch: $(m d-band-nomatch) — a band absorbed an unrelated red (B3)"

# THE central safety property: six drills, five non-GREEN → five blockers, regardless of how the match
# axis classified them. A band is not a waiver.
[ "$rc" = 5 ] && pass "exit code counts BLOCKERS, not deviations (5 non-GREEN → 5)" || fail "exit=$rc want 5 (a band or MATCH must not waive)"
printf '%s' "$out" | grep -q 'DEVIATION(S) FROM expected-verdicts.tsv' \
    && pass "deviation report section printed" || fail "no deviation report section"
# ...and the banded red must still be visible as a blocker in the human summary.
printf '%s' "$out" | grep -q 'BLOCKER' && pass "banded/expected reds still print as BLOCKERs" || fail "blockers vanished"

echo "── unknown drills and a missing table (M3 fail-soft) ───────────────────────────"
mkd d-unlisted ASSERT-FAIL 1
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l2" d-unlisted 2>&1); rc=$?
[ "$(awk -F'\t' 'NF>=15 && $1=="d-unlisted"{print $15}' "$RT/l2/rollup.tsv")" = NO-EXPECTATION ] \
    && pass "a drill absent from the table → NO-EXPECTATION (never a silent MATCH)" || fail "unlisted drill misclassified"
[ "$rc" = 1 ] && pass "an unlisted drill still blocks" || fail "unlisted exit=$rc want 1"

echo "── longest-first launch order (V1) ─────────────────────────────────────────────"
cat > "$RT/drill-costs.tsv" <<'EOF'
# drill	p50_secs	class	note
d-green	10	short	-
d-inc	900	long	-
d-band-match	400	medium	-
EOF
mk_tsv
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute -j 1 --logdir "$RT/l3" \
        d-green d-inc d-band-match 2>&1)
order=$(printf '%s\n' "$out" | sed -n 's/^\[[0-9:]*\] launch \([^ ]*\).*/\1/p' | tr '\n' ' ')
[ "$order" = "d-inc d-band-match d-green " ] && pass "launch order is cost-descending (d-inc d-band-match d-green)" \
    || fail "launch order was [$order], want [d-inc d-band-match d-green ]"
# The rollup is for humans and stays in the order the caller gave, NOT the launch order — otherwise
# every rollup silently reorders the day a cost changes.
rorder=$(cut -f1 "$RT/l3/rollup.tsv" | grep -v '^WAIVER\|^ATTRIBUTION' | tr '\n' ' ')
[ "$rorder" = "d-green d-inc d-band-match " ] && pass "rollup row order is UNCHANGED by launch ordering" \
    || fail "rollup order was [$rorder]"
# An unknown drill must sort FIRST at max cost: a new drill is dangerous as a straggler, harmless early.
mkd d-brandnew GREEN 0
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute -j 1 --logdir "$RT/l3b" \
        d-green d-brandnew 2>&1)
first=$(printf '%s\n' "$out" | sed -n 's/^\[[0-9:]*\] launch \([^ ]*\).*/\1/p' | head -1)
[ "$first" = d-brandnew ] && pass "a drill with no recorded cost launches FIRST" || fail "unknown drill launched [$first]"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute -j 1 --no-lpt --logdir "$RT/l3c" \
        d-inc d-green 2>&1)
first=$(printf '%s\n' "$out" | sed -n 's/^\[[0-9:]*\] launch \([^ ]*\).*/\1/p' | head -1)
[ "$first" = d-inc ] && pass "--no-lpt restores caller order" || fail "--no-lpt gave [$first]"

echo "── progress stream + sentinel (M3) ─────────────────────────────────────────────"
p="$RT/l3/progress.tsv"
[ -s "$p" ] && pass "progress.tsv written" || fail "no progress.tsv"
grep -q '^launch	d-inc' "$p" && pass "progress.tsv carries launch rows" || fail "no launch rows"
grep -q '^done	d-inc' "$p" && pass "progress.tsv carries completion rows" || fail "no done rows"
# Without the sentinel a partial file is indistinguishable from a finished sweep — that is the whole
# reason a killed run must not look like a conclusion.
grep -q '^RUN-COMPLETE' "$p" && pass "RUN-COMPLETE sentinel present on a finished sweep" || fail "no sentinel"
[ "$(grep -c '^RUN-COMPLETE' "$p")" = 1 ] && pass "exactly one sentinel" || fail "sentinel count wrong"

echo "── per-drill duration recorded (M3/V1 feedstock) ───────────────────────────────"
dur=$(awk -F'\t' 'NF>=15 && $1=="d-green"{print $11}' "$RT/l3/rollup.tsv")
case "$dur" in ''|*[!0-9]*) fail "duration_s not recorded (got [$dur])" ;; *) pass "duration_s recorded ($dur s)" ;; esac

echo "── attribution is ADDITIVE (M4) ────────────────────────────────────────────────"
rm -f "$RT/drill-costs.tsv"
mkd d-was-green ASSERT-FAIL 1 0 0 "deterministic boom"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l4" d-was-green 2>&1); rc=$?
printf '%s' "$out" | grep -q 'attribution pass' && pass "attribution pass runs on a deviation" || fail "attribution did not run"
lbl=$(awk -F'\t' '$1=="ATTRIBUTION" && $2=="d-was-green"{print $3}' "$RT/l4/rollup.tsv")
[ "$lbl" = REGRESSION ] && pass "reproduces solo with the same signature → REGRESSION" || fail "label=[$lbl] want REGRESSION"
# The property the whole ritual-replacement rests on.
[ "$rc" = 1 ] && pass "attribution did NOT change the exit code" || fail "exit=$rc want 1 — attribution altered authority"
[ -s "$RT/l4/d-was-green.attempt2.log" ] && pass "re-run evidence kept separately (attempt2)" || fail "no attempt2 log"
grep -q 'DRILL-VERDICT verdict=ASSERT-FAIL' "$RT/l4/d-was-green.log" \
    && pass "the FIRST run remains the verdict of record" || fail "first-run log was overwritten by the re-run"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l5" d-was-green 2>&1)
printf '%s' "$out" | grep -q '=== attribution pass ===' && fail "--no-attribute did not suppress the pass" \
    || pass "--no-attribute suppresses the pass"

# LOAD-SENSITIVE is the label the old "re-run it, it went green, call it a flake" ritual used to swallow
# silently. A stateful drill reds once and then behaves — exactly the shape of a timing-induced red.
# The point of the case is not the label: it is that the drill STILL BLOCKS afterwards.
cat > "$RT/drills/d-flaky.sh" <<EOF
#!/bin/sh
if [ -e "$RT/flaky.seen" ]; then
    printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- d-flaky\n' >&2
    exit 0
fi
: > "$RT/flaky.seen"
printf 'DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=0 -- d-flaky\n' >&2
exit 1
EOF
printf 'd-flaky\tGREEN\t0\t-\t-\td-flaky\n' >> "$RT/expected-verdicts.tsv"
rm -f "$RT/flaky.seen"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l6" d-flaky 2>&1); rc=$?
lbl=$(awk -F'\t' '$1=="ATTRIBUTION" && $2=="d-flaky"{print $3}' "$RT/l6/rollup.tsv")
[ "$lbl" = LOAD-SENSITIVE ] && pass "red once, then matches the expectation solo → LOAD-SENSITIVE" || fail "label=[$lbl] want LOAD-SENSITIVE"
[ "$rc" = 1 ] && pass "LOAD-SENSITIVE STILL BLOCKS (the anti-laundering property)" || fail "exit=$rc — a re-run laundered a red to green"
printf '%s' "$out" | grep -q 'STILL A BLOCKER' && pass "the report says so in words, not just in the exit code" || fail "no explicit still-blocks wording"
# And the verdict of record is untouched: the rollup row still says ASSERT-FAIL, not GREEN.
[ "$(awk -F'\t' 'NF>=15 && $1=="d-flaky"{print $2}' "$RT/l6/rollup.tsv")" = ASSERT-FAIL ] \
    && pass "rollup verdict remains the FIRST run's ASSERT-FAIL" || fail "rollup verdict was rewritten by the re-run"

echo "── M4 label discrimination (UNSTABLE ≠ REGRESSION ≠ LOAD-SENSITIVE) ────────────"
# Internal review MA11: the only REGRESSION fixture re-ran byte-identically and the only LOAD-SENSITIVE
# fixture was expected-GREEN, so inverting `[ "$v2" = "$exp" ]` or dropping the signature check both
# survived. These two cases pin the discriminators. UNSTABLE had NO fixture at all.
# UNSTABLE: reds on first run, reds AGAIN but with a DIFFERENT first-failure signature.
cat > "$RT/drills/d-unstable.sh" <<EOF
#!/bin/sh
if [ -e "$RT/unstable.seen" ]; then
    printf '[err ] FAIL  a DIFFERENT failure the second time\n' >&2
    printf 'DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=0 -- d-unstable\n' >&2
    exit 1
fi
: > "$RT/unstable.seen"
printf '[err ] FAIL  the ORIGINAL failure signature\n' >&2
printf 'DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=0 -- d-unstable\n' >&2
exit 1
EOF
printf 'd-unstable\tGREEN\t0\t-\t-\td-unstable\n' >> "$RT/expected-verdicts.tsv"
rm -f "$RT/unstable.seen"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l9" d-unstable >/dev/null 2>&1
lbl=$(awk -F'\t' '$1=="ATTRIBUTION" && $2=="d-unstable"{print $3}' "$RT/l9/rollup.tsv")
[ "$lbl" = UNSTABLE ] && pass "reds twice with DIFFERENT signatures → UNSTABLE (not REGRESSION)" || fail "label=[$lbl] want UNSTABLE"
# LOAD-SENSITIVE with a NON-GREEN expectation: reds ASSERT-FAIL, re-runs to its expected INCOMPLETE.
# This is the case that a "compare against GREEN" mutation (instead of against the expectation) breaks.
cat > "$RT/drills/d-ls-inc.sh" <<EOF
#!/bin/sh
if [ -e "$RT/lsinc.seen" ]; then
    printf 'DRILL-VERDICT verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0 not_covered=1 nc_gap=1 nc_guard=0 pass=1 -- d-ls-inc\n' >&2
    exit 4
fi
: > "$RT/lsinc.seen"
printf '[err ] FAIL  a load-induced failure\n' >&2
printf 'DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=0 -- d-ls-inc\n' >&2
exit 1
EOF
printf 'd-ls-inc\tINCOMPLETE\t1\t-\t#88\td-ls-inc\n' >> "$RT/expected-verdicts.tsv"
rm -f "$RT/lsinc.seen"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l10" d-ls-inc >/dev/null 2>&1
lbl=$(awk -F'\t' '$1=="ATTRIBUTION" && $2=="d-ls-inc"{print $3}' "$RT/l10/rollup.tsv")
[ "$lbl" = LOAD-SENSITIVE ] && pass "reds, then re-runs to its non-GREEN EXPECTATION → LOAD-SENSITIVE" || fail "label=[$lbl] want LOAD-SENSITIVE (compare vs expectation, not GREEN)"

echo "── B1/B2: evidence reachable across a name mismatch, and SETUP-RED records it ──"
# The two blockers internal review found: (B1) the evidence file was named from the free-text drill_begin
# TITLE while the report opened it by FILENAME, so it was unreachable on all 38 drills; (B2) no SETUP-RED
# path recorded evidence at all — and drills 30 and 91, the two motivating SETUP-RED deviations, wrote
# nothing. This drill reproduces both: a title unlike its filename, a SETUP-RED failure, a refusal on
# line 1 that a tail -3 of the log would drop.
cat > "$RT/drills/b1-setupred.sh" <<'EOF'
. "$(dirname "$0")/../lib/log.sh"; . "$(dirname "$0")/../lib/assert.sh"
drill_begin "A TITLE nothing like the filename — spaces & symbols!"
assert_ok "a green step first" true
assert_setup "the prerequisite that fails" sh -c 'echo "REFUSAL-LINE-ONE: --reset-js required"; echo L2; echo L3; echo L4; echo L5-LAST; exit 70'
EOF
printf 'b1-setupred\tGREEN\t0\t-\t-\tb1-setupred\n' >> "$RT/expected-verdicts.tsv"
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l7" b1-setupred 2>&1)
evf="$RT/l7/evidence/b1-setupred.evidence"
[ -s "$evf" ] && pass "B2: a SETUP-RED drill wrote a non-empty evidence record" || fail "B2: no evidence file for a SETUP-RED"
# The DRILL-EVIDENCE filename must match SIM_DRILL_ID (the drill FILENAME), not the sanitized title.
grep -q "DRILL-EVIDENCE file=$RT/l7/evidence/b1-setupred.evidence" "$RT/l7/b1-setupred.log" \
    && pass "B1: evidence filename keys on the drill id, not the free-text title" || fail "B1: evidence filename mismatch"
printf '%s' "$out" | grep -q "evidence: $evf" && pass "B1: the deviation report opens the announced evidence path" || fail "B1: report did not reach the evidence file"
# The whole point of M1: the report body carries the FIRST line of the refusal, which tail -3 would drop.
printf '%s' "$out" | grep -q 'REFUSAL-LINE-ONE' && pass "B1: report body carries the FULL refusal (line 1 survives)" || fail "B1: refusal line 1 not in the report"
printf '%s' "$out" | grep -q 'CLI-CONTRACT-SHAPED' && pass "the rc=70 + refusal text is tagged CLI-CONTRACT-SHAPED" || fail "no CLI-CONTRACT-SHAPED tag"
# The evidence record for a SETUP-RED must carry the captured command's rc, not a stale prior one (MA1).
grep -q '^rc:   70' "$evf" && pass "MA1: SETUP-RED evidence records the failing command's rc (70), not the prior pass" || fail "MA1: wrong/stale rc in evidence"

echo "── MA1: a non-captured red does NOT inherit the previous command's output ──────"
cat > "$RT/drills/ma1-stale.sh" <<'EOF'
. "$(dirname "$0")/../lib/log.sh"; . "$(dirname "$0")/../lib/assert.sh"
drill_begin "ma1"
assert_ok "a passing command with distinctive output" sh -c 'echo UNIQUE-PASSING-OUTPUT; exit 0'
product_red "a bare product_red that ran no command of its own"
EOF
printf 'ma1-stale\tGREEN\t0\t-\t-\tma1-stale\n' >> "$RT/expected-verdicts.tsv"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l8" ma1-stale >/dev/null 2>&1
evf="$RT/l8/evidence/ma1-stale.evidence"
if [ -s "$evf" ]; then
    grep -q 'UNIQUE-PASSING-OUTPUT' "$evf" && fail "MA1: the PRODUCT-RED record inherited the prior passing command's output" \
        || pass "MA1: the non-captured PRODUCT-RED did not inherit the prior command's output"
    grep -q 'NOT CAPTURED for this assertion' "$evf" && pass "MA1: it prints the honest NOT-CAPTURED note instead" || fail "MA1: no NOT-CAPTURED note"
else
    fail "MA1: no evidence file for the product_red"
fi

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then echo "ALL PASS"; exit 0; else echo "$FAILS FAILED"; exit 1; fi
