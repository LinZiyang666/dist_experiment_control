#!/bin/sh
# verdict-contract-test.sh — HERMETIC test of the lib/assert.sh verdict contract + the run-drills.sh
# DRILL-VERDICT parser. No docker, no tether, no network — pure shell. Run it on ANY box:
#
#     cd test/simcluster && sh tests/verdict-contract-test.sh          # (also passes under dash & bash)
#
# It exercises: the 5 landing verdicts, precedence between them, the fail-CLOSED harness-misuse paths
# (missing arg, empty description, empty signature, empty command), the structured verdict-line grammar,
# and the runner's classify-by-verdict + suite counting/exit code. This is the round-3 D4 regression that
# pins the contract so a future reviewer never has to hand-probe it again.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

# run_drill_body <sh-snippet> : source log.sh + assert.sh in a throwaway subshell (INSTANCE=drill-test so
# drill_begin does not refuse), run the snippet, and emit the drill's stderr (which carries the verdict
# line). Prints "RC=<n>" on the last line so the caller can read the drill's exit code too.
run_drill_body() {
    _body=$1
    ( set -u
      INSTANCE=drill-test
      . "$SIM_ROOT/lib/log.sh"
      . "$SIM_ROOT/lib/assert.sh"
      eval "$_body"
      # If the body did not abort (assert_setup/setup_fail call exit), close the frame here.
      drill_end
    ) 2>&1
    printf 'RC=%s\n' "$?"
}

# assert_verdict <label> <body> <want-verdict> <want-rc> : run the body, parse the DRILL-VERDICT line,
# check both the verdict enum and the drill rc.
assert_verdict() {
    _lbl=$1; _body=$2; _wv=$3; _wrc=$4
    _out=$(run_drill_body "$_body")
    _line=$(printf '%s\n' "$_out" | grep '^DRILL-VERDICT ' | tail -1)
    _gotv=$(printf '%s\n' "$_line" | sed -n 's/^DRILL-VERDICT verdict=\([^ ]*\) .*/\1/p')
    _gotrc=$(printf '%s\n' "$_out" | grep '^RC=' | tail -1 | sed 's/^RC=//')
    if [ -z "$_line" ]; then fail "$_lbl: NO DRILL-VERDICT line emitted"; return; fi
    if [ "$_gotv" != "$_wv" ]; then fail "$_lbl: verdict=$_gotv want $_wv  (line: $_line)"; return; fi
    if [ "$_gotrc" != "$_wrc" ]; then fail "$_lbl: rc=$_gotrc want $_wrc  (line: $_line)"; return; fi
    pass "$_lbl → $_gotv rc=$_gotrc"
}

# field <line> <key> : extract a key=value field from a verdict line.
field() { printf '%s\n' "$1" | sed -n "s/.* $2=\\([^ ]*\\).*/\\1/p"; }

# Self-guard (external review M2 negative control): prove the recorder/helper names this file calls
# actually EXIST. The die() frame gate was silently dead because it called ok/bad — names that don't
# exist here — so `grep && ok || bad` ran a command-not-found on every line and FAILS never moved. A
# missing helper must abort LOUDLY (exit 2, distinct from a normal 1-FAILED), never no-op into ALL PASS.
for _fn in pass fail field run_drill_body; do
    command -v "$_fn" >/dev/null 2>&1 || {
        printf 'FATAL: verdict-contract helper %s is undefined — the gate would silently no-op\n' "$_fn" >&2
        exit 2
    }
done

echo "── lib/assert.sh verdict contract ──────────────────────────────────────────────"

# 1. GREEN — every assertion a kept invariant.
assert_verdict "GREEN all-pass" \
    'drill_begin "t-green"; assert_ok "true is 0" true; assert_refuses "false refuses" "." sh -c "echo nope >&2; exit 1"' \
    GREEN 0

# 2. ASSERT-FAIL — a kept invariant broke.
assert_verdict "ASSERT-FAIL broken invariant" \
    'drill_begin "t-af"; assert_ok "false is not 0" false' \
    ASSERT-FAIL 1

# 3. ASSERT-FAIL — assert_bug APPEARS FIXED (want-fail, got 0).
assert_verdict "ASSERT-FAIL appears-fixed" \
    'drill_begin "t-fixed"; assert_bug "should still be broken" "#99" "boom" true' \
    ASSERT-FAIL 1

# 4. ASSERT-FAIL — assert_bug wrong-reason (fails but NOT for the documented sig).
assert_verdict "ASSERT-FAIL wrong-reason" \
    'drill_begin "t-wr"; assert_bug "wrong sig" "#99" "EXPECTED_TOKEN" sh -c "echo UNRELATED >&2; exit 1"' \
    ASSERT-FAIL 1

# 5. PRODUCT-RED — assert_bug reproduced for the documented reason.
assert_verdict "PRODUCT-RED reproduced" \
    'drill_begin "t-pr"; assert_bug "known defect" "#28" "still broken" sh -c "echo still broken here >&2; exit 1"' \
    PRODUCT-RED 3

# 6. SETUP-RED — assert_setup prerequisite fails (and ABORTS).
assert_verdict "SETUP-RED assert_setup abort" \
    'drill_begin "t-sr"; assert_setup "prereq" false; assert_ok "should never run" true' \
    SETUP-RED 2

# 7. SETUP-RED — setup_fail explicit abort.
assert_verdict "SETUP-RED setup_fail abort" \
    'drill_begin "t-sf"; X=$(false) || setup_fail "no value"; assert_ok "should never run" true' \
    SETUP-RED 2

# 8. INCOMPLETE — a recorded coverage gap.
assert_verdict "INCOMPLETE not_covered" \
    'drill_begin "t-inc"; assert_ok "one real pass" true; not_covered "some cell" "not exercisable in-sim this run" gap' \
    INCOMPLETE 4

# ── PRECEDENCE ──────────────────────────────────────────────────────────────────
# 9. ASSERT-FAIL beats everything.
assert_verdict "precedence ASSERT-FAIL>all" \
    'drill_begin "t-p1"; assert_ok "break" false; assert_bug "d" "#1" "x" sh -c "echo x>&2;exit 1"; not_covered "g" "r" gap' \
    ASSERT-FAIL 1
# 10. SETUP-RED beats PRODUCT-RED + INCOMPLETE (setup_fail aborts, so seed PRODUCT-RED first).
assert_verdict "precedence SETUP-RED>PRODUCT-RED" \
    'drill_begin "t-p2"; assert_bug "d" "#1" "x" sh -c "echo x>&2;exit 1"; not_covered "g" "r" gap; setup_fail "late prereq"' \
    SETUP-RED 2
# 11. PRODUCT-RED beats INCOMPLETE.
assert_verdict "precedence PRODUCT-RED>INCOMPLETE" \
    'drill_begin "t-p3"; not_covered "g" "r" gap; assert_bug "d" "#1" "x" sh -c "echo x>&2;exit 1"' \
    PRODUCT-RED 3

# ── FAIL-CLOSED harness misuse (round-3 M1/m1): NEVER a false GREEN, ALWAYS a verdict ──
# 12. assert_refuses with a MISSING signature (2 args, no cmd) → SETUP-RED, not a crash.
assert_verdict "misuse: assert_refuses missing sig+cmd" \
    'drill_begin "t-m1"; assert_refuses "desc only"' \
    SETUP-RED 2
# 13. assert_refuses with an EMPTY signature but a real failing cmd → must NOT false-GREEN.
assert_verdict "misuse: assert_refuses empty sig (would match any failure)" \
    'drill_begin "t-m2"; assert_refuses "d" "" sh -c "echo arbitrary >&2; exit 5"' \
    SETUP-RED 2
# 14. assert_bug with an EMPTY signature + a real failing cmd → must NOT forge PRODUCT-RED.
assert_verdict "misuse: assert_bug empty sig (would forge PRODUCT-RED)" \
    'drill_begin "t-m3"; assert_bug "d" "#1" "" sh -c "echo arbitrary >&2; exit 5"' \
    SETUP-RED 2
# 15. assert_bug with a MISSING gotcha+sig+cmd (2 args) → SETUP-RED, no set-u crash.
assert_verdict "misuse: assert_bug too few args" \
    'drill_begin "t-m4"; assert_bug "d" "#1"' \
    SETUP-RED 2
# 16. assert_ok with an EMPTY command word → SETUP-RED (would capture rc 0 = fail-open).
assert_verdict "misuse: assert_ok empty command" \
    'drill_begin "t-m5"; assert_ok "d" ""' \
    SETUP-RED 2
# 17. not_covered with a MISSING reason → SETUP-RED (an unexplained gap is misuse).
assert_verdict "misuse: not_covered missing reason" \
    'drill_begin "t-m6"; not_covered "gap with no reason"' \
    SETUP-RED 2
# 17b. not_covered with desc+reason but NO class → SETUP-RED (an untriageable gap is misuse; batch E).
assert_verdict "misuse: not_covered missing class" \
    'drill_begin "t-m8"; not_covered "gap" "a real reason"' \
    SETUP-RED 2
# 17c. not_covered with an EMPTY class → SETUP-RED (must not fall through to a nameless gap).
assert_verdict "misuse: not_covered empty class" \
    'drill_begin "t-m9"; not_covered "gap" "a real reason" ""' \
    SETUP-RED 2
# 17d. not_covered with an OUT-OF-ENUM class → SETUP-RED, and it must NOT have bumped not_covered.
assert_verdict "misuse: not_covered bogus class" \
    'drill_begin "t-m10"; not_covered "gap" "a real reason" flaky' \
    SETUP-RED 2
_out=$(run_drill_body 'drill_begin "t-m10b"; not_covered "gap" "a real reason" flaky')
_line=$(printf '%s\n' "$_out" | grep '^DRILL-VERDICT ' | tail -1)
{ [ "$(field "$_line" not_covered)" = 0 ] && [ "$(field "$_line" nc_gap)" = 0 ] && [ "$(field "$_line" nc_guard)" = 0 ]; } \
    && pass "misuse: a bogus class is rejected, never counted as a gap" \
    || fail "misuse: bogus class leaked into the not_covered counters ($_line)"
# 18. A misuse that would otherwise be GREEN must land SETUP-RED (proves misuse is not swallowed).
assert_verdict "misuse poisons an otherwise-GREEN drill" \
    'drill_begin "t-m7"; assert_ok "real pass" true; assert_refuses "d" "" sh -c "exit 1"' \
    SETUP-RED 2

# ── grammar: the verdict line fields are all present + numeric ──
echo "── verdict-line grammar ────────────────────────────────────────────────────────"
_out=$(run_drill_body 'drill_begin "t-grammar"; assert_ok "a" true; assert_ok "b" true; not_covered "g" "r" gap')
_line=$(printf '%s\n' "$_out" | grep '^DRILL-VERDICT ' | tail -1)
# R1: die() must be FRAME-AWARE. lib/log.sh's die is `err; exit 1`; 38 call sites across six drills used it
# as a mid-drill refusal guard, exiting WITHOUT a verdict line so the runner reported INFRA-ABORT — measured
# on drill 73, which had already recorded a real assert_fail and still got relabelled as "the harness broke".
_t=$(mktemp -d)
printf '. "$1/lib/log.sh"\n. "$1/lib/assert.sh"\ndrill_begin "die-frame"\ndie "mid-drill refusal"\n' > "$_t/d.sh"
# External review H-3/M2: these two checks called `ok`/`bad` — helpers that DO NOT EXIST in this
# file (it defines pass/fail; the names were copied from poll-reentrancy-test.sh). `grep && ok || bad`
# then ran `ok`/`bad`, both exiting 127 (command-not-found) on EVERY run, so FAILS never moved and the
# die() frame-aware regression gate — R1's headline deliverable — was silently dead (a reviewer proved a
# reverted die() still `ALL PASS`ed). Fixed to pass/fail; the checks are now a live regression gate.
_out=$(INSTANCE=drill-x sh "$_t/d.sh" "$HERE/.." 2>&1)
if printf '%s\n' "$_out" | grep -q 'DRILL-VERDICT verdict=SETUP-RED'; then
    pass "die() inside a frame lands SETUP-RED WITH a verdict line (no INFRA-ABORT)"
else
    fail "die() inside a frame emitted no SETUP-RED verdict line: $_out"
fi
# and it must NOT hijack die() outside a frame (lib helpers sourced by simcluster itself rely on plain abort)
printf '. "$1/lib/log.sh"\n. "$1/lib/assert.sh"\ndie "outside frame"\n' > "$_t/e.sh"
_out=$(INSTANCE=drill-x sh "$_t/e.sh" "$HERE/.." 2>&1)
if printf '%s\n' "$_out" | grep -q 'DRILL-VERDICT'; then
    fail "die() outside a frame wrongly emitted a verdict line"
else
    pass "die() outside a frame keeps plain abort semantics (non-vacuity control)"
fi
rm -rf "$_t"

if printf '%s\n' "$_line" | grep -qE '^DRILL-VERDICT verdict=[A-Z-]+ rc=[0-9]+ assert_fail=[0-9]+ setup_red=[0-9]+ product_red=[0-9]+ not_covered=[0-9]+ nc_gap=[0-9]+ nc_guard=[0-9]+ pass=[0-9]+ -- '; then
    pass "grammar: all machine fields present + numeric"
else
    fail "grammar: line does not match the contract: $_line"
fi
[ "$(field "$_line" pass)" = 2 ] && pass "grammar: pass=2 counted" || fail "grammar: pass count wrong ($_line)"
[ "$(field "$_line" not_covered)" = 1 ] && pass "grammar: not_covered=1 counted" || fail "grammar: not_covered count wrong ($_line)"
# The two class fields were APPENDED after not_covered: older field names/order are untouched, and the
# classes must always sum back to the parent counter (a drifting split would silently under-report debt).
[ "$(field "$_line" nc_gap)" = 1 ] && pass "grammar: nc_gap=1 counted" || fail "grammar: nc_gap wrong ($_line)"
[ "$(field "$_line" nc_guard)" = 0 ] && pass "grammar: nc_guard=0 counted" || fail "grammar: nc_guard wrong ($_line)"
# a drill name WITH SPACES must survive after the ` -- ` sentinel:
printf '%s\n' "$_line" | grep -q ' -- t-grammar$' && pass "grammar: free-text name after -- sentinel" || fail "grammar: name sentinel broken ($_line)"

# ── not_covered CLASSES (batch E) ───────────────────────────────────────────────
echo "── not_covered classes (gap vs runtime-guard) ──────────────────────────────────"
# A runtime-guard gets NO green back door: it still lands INCOMPLETE, exactly like a gap.
assert_verdict "INCOMPLETE not_covered runtime-guard (no green back door)" \
    'drill_begin "t-rg"; assert_ok "one real pass" true; not_covered "arm" "the fixture did not land this run" runtime-guard' \
    INCOMPLETE 4
# Mixed classes: nc_gap + nc_guard must sum to not_covered and be attributed to the right buckets.
_out=$(run_drill_body 'drill_begin "t-mix"; not_covered "a" "r" gap; not_covered "b" "r" runtime-guard; not_covered "c" "r" gap')
_line=$(printf '%s\n' "$_out" | grep '^DRILL-VERDICT ' | tail -1)
{ [ "$(field "$_line" not_covered)" = 3 ] && [ "$(field "$_line" nc_gap)" = 2 ] && [ "$(field "$_line" nc_guard)" = 1 ]; } \
    && pass "classes: nc_gap=2 + nc_guard=1 sum to not_covered=3" \
    || fail "classes: split does not reconcile with not_covered ($_line)"
# The human NOT-COVERED line must carry the class so a log reader can triage without re-parsing.
printf '%s\n' "$_out" | grep -q 'NOT-COVERED\[gap\]' && printf '%s\n' "$_out" | grep -q 'NOT-COVERED\[runtime-guard\]' \
    && pass "classes: the NOT-COVERED log line names its class" \
    || fail "classes: NOT-COVERED log line lost its class tag"
# drill_begin must RESET the class counters, or a second frame inherits the first frame's debt.
_out=$(run_drill_body 'drill_begin "t-r1"; not_covered "a" "r" gap; not_covered "b" "r" runtime-guard; drill_begin "t-r2"; assert_ok "clean" true')
_line=$(printf '%s\n' "$_out" | grep '^DRILL-VERDICT ' | tail -1)
{ [ "$(field "$_line" nc_gap)" = 0 ] && [ "$(field "$_line" nc_guard)" = 0 ]; } \
    && pass "classes: drill_begin resets nc_gap/nc_guard" \
    || fail "classes: class counters leaked across drill_begin ($_line)"

# ── run-drills.sh END-TO-END: build a synthetic drill tree + a fake simcluster, run the REAL runner, and
# assert its classify-by-verdict + blocker exit code (round-3 B1 + 疑惑-4). Needs bash (the runner uses
# arrays); skipped with a note if bash is absent.
echo "── run-drills.sh end-to-end (real runner, synthetic drills) ────────────────────"
if command -v bash >/dev/null 2>&1; then
    RT=$(mktemp -d); mkdir -p "$RT/drills"
    ln -sf "$SIM_ROOT/run-drills.sh" "$RT/run-drills.sh"
    printf '#!/bin/sh\n[ "$1" = drill ] || exit 9\nexec sh "$(dirname "$0")/drills/$2.sh"\n' > "$RT/simcluster"; chmod +x "$RT/simcluster"
    mkd() {
        af=0; sr=0; pr=0; nc=0
        case "$2" in ASSERT-FAIL) af=1 ;; SETUP-RED) sr=1 ;; PRODUCT-RED) pr=1 ;; INCOMPLETE) nc=1 ;; esac
        printf '#!/bin/sh\nprintf "DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s nc_gap=0 nc_guard=0 pass=1 -- %s\\n" >&2\nexit %s\n' \
            "$2" "$3" "$af" "$sr" "$pr" "$nc" "$1" "$3" > "$RT/drills/$1.sh"
    }
    mkd g GREEN 0; mkd p PRODUCT-RED 3; mkd i INCOMPLETE 4; mkd s SETUP-RED 2; mkd a ASSERT-FAIL 1
    printf '#!/bin/sh\necho "boom crash" >&2\nexit 1\n' > "$RT/drills/z.sh"   # INFRA-ABORT: no verdict line
    # a LEGACY drill: emits a GREEN verdict line (rc=0) but process exits 1 → CONTRACT-ERROR.
    printf '#!/bin/sh\nprintf "DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- legacy\\n" >&2\nexit 1\n' > "$RT/drills/m.sh"
    # Adversarial contracts: missing field, duplicate/conflicting line, and counter/verdict contradiction.
    printf '#!/bin/sh\nprintf "DRILL-VERDICT verdict=GREEN assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- missing-rc\\n" >&2\nexit 0\n' > "$RT/drills/bad-missing.sh"
    printf '#!/bin/sh\nprintf "DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=0 -- dup\\n" >&2\nprintf "DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- dup\\n" >&2\nexit 0\n' > "$RT/drills/bad-dup.sh"
    printf '#!/bin/sh\nprintf "DRILL-VERDICT verdict=GREEN rc=0 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- bad-counter\\n" >&2\nexit 0\n' > "$RT/drills/bad-counter.sh"
    # Contract contradiction plus an infra-looking string must never be retried/laundered.
    printf '#!/bin/sh\necho "container not running" >&2\nprintf "DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- mismatch-flake\\n" >&2\nexit 1\n' > "$RT/drills/bad-retry.sh"
    # (1) all six matched drills → 5 blockers by default (PRODUCT/INCOMPLETE included).
    bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l1" g p i s a z >/dev/null 2>&1
    [ "$?" = 5 ] && pass "runner e2e: 6 mixed drills → exit 5 (all non-GREEN block by default)" || fail "runner e2e: mixed exit != 5"
    # (1b) the legacy GREEN-line-but-rc1 drill → CONTRACT-ERROR blocker, NOT a trusted GREEN.
    out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/lm" m 2>&1); rc=$?
    { [ "$rc" = 1 ] && printf '%s' "$out" | grep -qi 'CONTRACT-ERROR'; } \
        && pass "runner e2e: legacy GREEN-line + rc1 → CONTRACT-ERROR blocker" || fail "runner e2e: rc cross-check missed legacy override (rc=$rc)"
    # (2) PRODUCT-RED and INCOMPLETE block by default.
    bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l2" g p i >/dev/null 2>&1
    [ "$?" = 2 ] && pass "runner e2e: PRODUCT-RED+INCOMPLETE fail closed by default" || fail "runner e2e: non-green default did not block"
    # (3) explicit, separate owner waivers yield exit 0 but never ALL GREEN/NO BLOCKERS.
    out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --allow-product-red --allow-incomplete --logdir "$RT/l3" g p i 2>&1); rc=$?
    { [ "$rc" = 0 ] && printf '%s' "$out" | grep -q 'WAIVED NON-GREEN' && ! printf '%s' "$out" | grep -q 'ALL GREEN\|NO BLOCKERS'; } \
        && pass "runner e2e: explicit waivers → WAIVED NON-GREEN exit 0" || fail "runner e2e: waiver reporting/exit wrong"
    # (4) pure GREEN → 'ALL GREEN' + exit 0.
    out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l4" g 2>&1); rc=$?
    { [ "$rc" = 0 ] && printf '%s' "$out" | grep -qi 'ALL GREEN'; } && pass "runner e2e: pure GREEN → ALL GREEN exit 0" || fail "runner e2e: pure-green wrong"
    # (5) malformed/duplicate/semantically inconsistent lines all fail closed under the REAL parser.
    for d in bad-missing bad-dup bad-counter; do
        out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --logdir "$RT/l-$d" "$d" 2>&1); rc=$?
        { [ "$rc" = 1 ] && printf '%s' "$out" | grep -q CONTRACT-ERROR; } \
            && pass "runner e2e: $d → CONTRACT-ERROR" || fail "runner e2e: $d was trusted (rc=$rc)"
    done
    # (6) a contract error carrying an infra signature is not retried and cannot become green.
    out=$(bash "$RT/run-drills.sh" --skip-preflight --logdir "$RT/l-retry" bad-retry 2>&1); rc=$?
    { [ "$rc" = 1 ] && printf '%s' "$out" | grep -q CONTRACT-ERROR && [ ! -e "$RT/l-retry/bad-retry.attempt1.log" ]; } \
        && pass "runner e2e: CONTRACT-ERROR is never retryable" || fail "runner e2e: contract error was retried/laundered"
    rm -rf "$RT"
else
    echo "  (skipped — no bash; runner needs arrays)"
fi

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then echo "ALL PASS"; exit 0; else echo "$FAILS FAILED"; exit 1; fi
