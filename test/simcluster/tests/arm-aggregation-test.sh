#!/bin/sh
# arm-aggregation-test.sh — HERMETIC gate for the unit model in run-drills.sh (simcluster-speed D/E/F/G):
# arm expansion, the DRILL-ARM cross-check, the per-drill verdict join, arms_missing, the per-drill exit
# code, the per-unit ceiling (2×worst) and the grow lane. Same harness shape as deviation-report-test.sh:
# a stub `simcluster` that runs synthetic drills, no docker, no server. ~40 s of real timers (the lane
# and ceiling cases have to actually wait).
# origin: simcluster-speed plan §6.3 F-1…F-9, G-1, E-1.
set -u
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SIM_ROOT=$(CDPATH= cd -- "$HERE/.." && pwd)
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1" >&2; FAILS=$((FAILS + 1)); }
command -v bash >/dev/null 2>&1 || { echo "  (skipped — no bash; run-drills.sh needs arrays)"; exit 0; }

RT=$(mktemp -d); mkdir -p "$RT/drills"
trap 'rm -rf "$RT"' EXIT
ln -sf "$SIM_ROOT/run-drills.sh" "$RT/run-drills.sh"
ln -sf "$SIM_ROOT/lib" "$RT/lib"
# The stub honours `drill <name> [--arm <A>]` exactly as cmd_drill will: exports ARM (empty when absent)
# and runs the drill script. Everything else the runner asks of simcluster is a no-op success.
cat > "$RT/simcluster" <<'EOF'
#!/bin/sh
[ "$1" = check-image ] && exit 0
[ "$1" = drill ] || exit 9
name=$2; arm=""
[ "${3:-}" = --arm ] && arm=$4
export INSTANCE="drill-$name${arm:+-$arm}" ARM="$arm" SIM_DRILL_NAME="$name"
exec sh "$(dirname "$0")/drills/$name.sh"
EOF
chmod +x "$RT/simcluster"
printf '# drill\texpected\texpected_nc_gap\tbands\towner\tnote-ref\n' > "$RT/expected-verdicts.tsv"
printf '## none\n' > "$RT/expected-verdicts-log.md"

# verdict <name> <verdict> <rc> <af> <sr> <pr> <nc> <ncg> : the exact contract line a real drill_end prints
verdict() { printf 'printf "DRILL-VERDICT verdict=%s rc=%s assert_fail=%s setup_red=%s product_red=%s not_covered=%s nc_gap=%s nc_guard=0 pass=1 -- %s\\n" >&2\n' "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$1"; }
armline() { printf 'printf "DRILL-ARM arm=%%s of=%%s\\n" "$ARM" "$SIM_DRILL_NAME" >&2\n'; }

# ── F: an arm-split drill whose arms land differently ───────────────────────────────────────────────
{
    printf '#!/bin/sh\n# arms: A D F\n# fixture: A=N1 D=N1 F=N1\n# grows: A=0 D=0 F=0\n# worst: A=600 D=600 F=600\n# forgoes: A=- D=- F=-\n'
    printf 'case "${ARM:?}" in\n'
    printf '  A) '; verdict f-split GREEN 0 0 0 0 0 0; armline; printf ' exit 0 ;;\n'
    printf '  D) '; verdict f-split ASSERT-FAIL 1 1 0 0 2 2; armline; printf ' exit 1 ;;\n'
    printf '  F) '; verdict f-split INCOMPLETE 4 0 0 0 3 3; armline; printf ' exit 4 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/f-split.sh"
# a plain drill beside it, green
printf '#!/bin/sh\n' > "$RT/drills/p-plain.sh"; verdict p-plain GREEN 0 0 0 0 0 0 >> "$RT/drills/p-plain.sh"; printf 'exit 0\n' >> "$RT/drills/p-plain.sh"

out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l1" f-split p-plain 2>&1); rc=$?
tsv="$RT/l1/rollup.tsv"
echo "── F: arm expansion, DRILL-ARM cross-check, verdict join ───────────────────────"
for u in f-split.A f-split.D f-split.F p-plain; do
    [ -f "$RT/l1/$u.log" ] && pass "unit $u ran and has its own log" || fail "unit $u has no log (expansion failed)"
done
[ "$(awk -F'\t' '$1=="f-split.A"{print $2}' "$tsv")" = GREEN ] && pass "F-8: the arm's own DRILL-VERDICT line classifies as-is (GREEN)" || fail "f-split.A not GREEN: $(grep '^f-split.A' "$tsv")"
[ "$(awk -F'\t' '$1=="f-split.D"{print $2}' "$tsv")" = ASSERT-FAIL ] && pass "f-split.D classifies ASSERT-FAIL" || fail "f-split.D wrong"
arms=$(awk -F'\t' '$1=="ARMS" && $2=="f-split"' "$tsv")
[ -n "$arms" ] && pass "an ARMS row exists for the split drill" || fail "no ARMS row: $(grep ARMS "$tsv")"
[ "$(printf '%s' "$arms" | cut -f3)" = ASSERT-FAIL ] && pass "F-1: GREEN + ASSERT-FAIL + INCOMPLETE join to ASSERT-FAIL" || fail "joined verdict: $arms"
[ "$(printf '%s' "$arms" | cut -f4)" = 0 ] && pass "arms_missing = 0 when every arm reached drill_end" || fail "arms_missing: $arms"
[ "$(printf '%s' "$arms" | cut -f8)" = 5 ] && [ "$(printf '%s' "$arms" | cut -f9)" = 5 ] && pass "F-6: not_covered / nc_gap summed across arms (2+3=5)" || fail "counter sum: $arms"
[ "$rc" = 1 ] && pass "F-7: exit code counts DRILLS — two red arms of one drill = 1, plus p-plain green" || fail "exit code $rc, want 1"
printf '%s\n' "$out" | grep -q 'ASSERT-FAIL[^a-z]*f-split ' && pass "summary prints the joined drill row" || fail "no joined summary row: $(printf '%s' "$out" | grep f-split | head -3)"
grep -q '^f-split.A	' "$tsv" && [ "$(grep -c '^f-split	' "$tsv")" = 0 ] && pass "rollup unit rows are keyed <drill>.<arm>, no bare drill row" || fail "rollup keys wrong"

# F-2: SETUP-RED + PRODUCT-RED → SETUP-RED
{
    printf '#!/bin/sh\n# arms: A B\n# fixture: A=N1 B=N1\n# grows: A=0 B=0\n# worst: A=600 B=600\n# forgoes: A=- B=-\ncase "${ARM:?}" in\n'
    printf '  A) '; verdict f-sp SETUP-RED 2 0 1 0 0 0; armline; printf ' exit 2 ;;\n'
    printf '  B) '; verdict f-sp PRODUCT-RED 3 0 0 1 0 0; armline; printf ' exit 3 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/f-sp.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l2" f-sp >/dev/null 2>&1
[ "$(awk -F'\t' '$1=="ARMS" && $2=="f-sp"{print $3}' "$RT/l2/rollup.tsv")" = SETUP-RED ] && pass "F-2: SETUP-RED + PRODUCT-RED join to SETUP-RED (precedence)" || fail "F-2 join: $(grep ARMS "$RT/l2/rollup.tsv")"
# F-2b: precedence is ASSERT-FAIL > SETUP-RED even when the setup_red count is the larger one — an arm
# whose own line is ASSERT-FAIL with a setup_red beside it, joined with a SETUP-RED arm, is ASSERT-FAIL.
{
    printf '#!/bin/sh\n# arms: A B\n# fixture: A=N1 B=N1\n# grows: A=0 B=0\n# worst: A=600 B=600\n# forgoes: A=- B=-\ncase "${ARM:?}" in\n'
    printf '  A) '; verdict f-pre ASSERT-FAIL 1 1 1 0 0 0; armline; printf ' exit 1 ;;\n'
    printf '  B) '; verdict f-pre SETUP-RED 2 0 2 0 0 0; armline; printf ' exit 2 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/f-pre.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l2b" f-pre >/dev/null 2>&1
[ "$(awk -F'\t' '$1=="ARMS" && $2=="f-pre"{print $3}' "$RT/l2b/rollup.tsv")" = ASSERT-FAIL ] && pass "F-2b: ASSERT-FAIL(af=1,sr=1) + SETUP-RED(sr=2) join to ASSERT-FAIL, not the larger count" || fail "F-2b join: $(grep ARMS "$RT/l2b/rollup.tsv")"

# F-3: an arm that aborts before drill_end + an arm that fails → joined ASSERT-FAIL with arms_missing=1
{
    printf '#!/bin/sh\n# arms: A D\n# fixture: A=N1 D=N1\n# grows: A=0 D=0\n# worst: A=600 D=600\n# forgoes: A=- D=-\ncase "${ARM:?}" in\n'
    printf '  A) echo "boom before drill_end" >&2; exit 70 ;;\n'
    printf '  D) '; verdict f-miss ASSERT-FAIL 1 1 0 0 0 0; armline; printf ' exit 1 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/f-miss.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l3" f-miss >/dev/null 2>&1; rc=$?
arms=$(awk -F'\t' '$1=="ARMS" && $2=="f-miss"' "$RT/l3/rollup.tsv")
[ "$(printf '%s' "$arms" | cut -f3)" = ASSERT-FAIL ] && [ "$(printf '%s' "$arms" | cut -f4)" = 1 ] && pass "F-3: INFRA-ABORT arm does not hide the sibling's ASSERT-FAIL; arms_missing=1" || fail "F-3: $arms"
[ "$rc" = 1 ] && pass "F-3: one drill, one blocker" || fail "F-3 exit $rc"
[ "$(awk -F'\t' '$1=="f-miss.A"{print $2}' "$RT/l3/rollup.tsv")" = INFRA-ABORT ] && pass "the aborted arm is INFRA-ABORT as a unit" || fail "f-miss.A verdict wrong"

# F-4 / F-5: DRILL-ARM cross-check — wrong arm named, and no DRILL-ARM line at all → CONTRACT-ERROR
{
    printf '#!/bin/sh\n# arms: A B C\n# fixture: A=N1 B=N1 C=N1\n# grows: A=0 B=0 C=0\n# worst: A=600 B=600 C=600\n# forgoes: A=- B=- C=-\ncase "${ARM:?}" in\n'
    printf '  A) '; verdict f-ca GREEN 0 0 0 0 0 0; printf 'printf "DRILL-ARM arm=Z of=f-ca\\n" >&2; exit 0 ;;\n'
    printf '  B) '; verdict f-ca GREEN 0 0 0 0 0 0; printf ' exit 0 ;;\n'
    printf '  C) '; verdict f-ca GREEN 0 0 0 0 0 0; armline; armline; printf ' exit 0 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/f-ca.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l4" f-ca >/dev/null 2>&1
[ "$(awk -F'\t' '$1=="f-ca.A"{print $2}' "$RT/l4/rollup.tsv")" = CONTRACT-ERROR ] && pass "F-4: DRILL-ARM naming a different arm → CONTRACT-ERROR" || fail "F-4: $(grep '^f-ca.A' "$RT/l4/rollup.tsv")"
[ "$(awk -F'\t' '$1=="f-ca.B"{print $2}' "$RT/l4/rollup.tsv")" = CONTRACT-ERROR ] && pass "F-5: an arm unit with no DRILL-ARM line → CONTRACT-ERROR (a GREEN verdict does not rescue it)" || fail "F-5: $(grep '^f-ca.B' "$RT/l4/rollup.tsv")"
[ "$(awk -F'\t' '$1=="f-ca.C"{print $2}' "$RT/l4/rollup.tsv")" = CONTRACT-ERROR ] && pass "F-5b: two DRILL-ARM lines, even both correct → CONTRACT-ERROR (exactly one)" || fail "F-5b: $(grep '^f-ca.C' "$RT/l4/rollup.tsv")"
[ "$(awk -F'\t' '$1=="ARMS" && $2=="f-ca"{print $4}' "$RT/l4/rollup.tsv")" = 3 ] && pass "all three contract errors count as arms_missing" || fail "arms_missing for f-ca: $(grep ARMS "$RT/l4/rollup.tsv")"

# a single-arm selection by name expands to that unit only
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l5" f-split.A >/dev/null 2>&1; rc=$?
[ "$rc" = 0 ] && [ -f "$RT/l5/f-split.A.log" ] && [ ! -f "$RT/l5/f-split.D.log" ] && pass "naming <drill>.<arm> runs exactly that unit" || fail "single-arm selection wrong (rc=$rc)"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l5b" f-split.Q >/dev/null 2>&1 && fail "an unknown arm name was accepted" || pass "an unknown arm name is refused"
# The parser is load-bearing: a runner beside no lib/ once expanded every drill to zero units and ended
# "ALL GREEN" over nothing (rc=0). It must refuse instead.
mkdir -p "$RT/nolib/drills"; ln -sf "$SIM_ROOT/run-drills.sh" "$RT/nolib/run-drills.sh"; cp "$RT/simcluster" "$RT/nolib/simcluster"; cp "$RT/drills/p-plain.sh" "$RT/nolib/drills/"
out=$(bash "$RT/nolib/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/l5c" p-plain 2>&1); rc=$?
[ "$rc" = 3 ] && ! printf '%s' "$out" | grep -q 'ALL GREEN' && pass "a runner without lib/manifest.sh refuses (rc=3) instead of reporting ALL GREEN over zero units" || fail "no-lib runner: rc=$rc $(printf '%s' "$out" | grep -c 'ALL GREEN') ALL GREEN line(s)"

echo "── simcluster drill --arm: the refusals happen before any docker call ───────────"
# The real `simcluster` against the real drills/ tree: an arm-split drill without --arm, an unknown arm,
# and --arm on a manifest-free drill are all refused BEFORE check_image_or_die, so no docker is needed.
sp=$(ls "$SIM_ROOT"/drills/*.sh | while read -r f; do if grep -q '^# arms:' "$f"; then basename "$f" .sh; break; fi; done)
if [ -n "$sp" ]; then
    out=$("$SIM_ROOT/simcluster" drill "$sp" 2>&1); rc=$?
    [ "$rc" != 0 ] && printf '%s' "$out" | grep -q 'run one arm at a time' && pass "an arm-split drill ($sp) without --arm is refused with the hint" || fail "no --arm refusal: rc=$rc $out"
    out=$("$SIM_ROOT/simcluster" drill "$sp" --arm ZZ 2>&1); rc=$?
    [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "is not an arm of" && pass "an unknown arm is refused" || fail "unknown arm: rc=$rc $out"
else
    fail "no arm-split drill under drills/ to exercise the refusal against"
fi
out=$("$SIM_ROOT/simcluster" drill 00-skeleton --arm A 2>&1); rc=$?
[ "$rc" != 0 ] && printf '%s' "$out" | grep -q "no '# arms:' manifest" && pass "--arm on a manifest-free drill is refused" || fail "manifest-free --arm: rc=$rc $out"

echo "── G: the per-unit ceiling (2×worst, capped by --drill-timeout) ─────────────────"
{
    printf '#!/bin/sh\n# arms: A\n# fixture: A=N1\n# grows: A=0\n# worst: A=4\n# forgoes: A=-\ncase "${ARM:?}" in\n'
    printf '  A) sleep 30; '; verdict g-slow GREEN 0 0 0 0 0 0; armline; printf ' exit 0 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/g-slow.sh"
t0=$(date +%s)
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --drill-timeout 600 --logdir "$RT/l6" g-slow >/dev/null 2>&1
el=$(( $(date +%s) - t0 ))
[ "$(cat "$RT/l6/g-slow.A.tmo")" = 8 ] && pass "G-1: ceiling applied = 2×worst = 8 s (not the 600 s global)" || fail "tmo: $(cat "$RT/l6/g-slow.A.tmo" 2>/dev/null)"
[ -e "$RT/l6/g-slow.A.timeout" ] && [ "$el" -lt 25 ] && pass "G-1: killed at the unit ceiling (${el}s), marked .timeout, not retried" || fail "G-1: el=${el}s timeout-marker=$([ -e "$RT/l6/g-slow.A.timeout" ] && echo yes || echo no)"
[ "$(awk -F'\t' '$1=="g-slow.A"{print $2}' "$RT/l6/rollup.tsv")" = INFRA-ABORT ] && pass "G-1: a ceiling kill is INFRA-ABORT" || fail "G-1 verdict"
grep -q '2 x declared worst 4s' "$RT/l6/g-slow.A.log" && pass "G-1: the kill note names its basis" || fail "G-1 basis note missing"

echo "── E: the grow lane ─────────────────────────────────────────────────────────────"
# Three grow-lane units (content carries the grow token) each 4 s; --grow-cap 1 must serialise them
# while an N1 unit runs alongside unthrottled. Launch/done epochs come from progress.tsv.
for n in 1 2 3; do
    { printf '#!/bin/sh\n# a synthetic grow drill: the token below puts it in the grow lane\n: grow_to_3\nsleep 4\n'; verdict "e-grow$n" GREEN 0 0 0 0 0 0; printf 'exit 0\n'; } > "$RT/drills/e-grow$n.sh"
done
{ printf '#!/bin/sh\nsleep 4\n'; verdict e-n1 GREEN 0 0 0 0 0 0; printf 'exit 0\n'; } > "$RT/drills/e-n1.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --no-lpt --grow-cap 1 -j 4 --logdir "$RT/l7" e-grow1 e-grow2 e-grow3 e-n1 >"$RT/l7.out" 2>&1
overlap=$(awk -F'\t' '$1=="launch"{l[$2]=$5} $1=="done"{d[$2]=$5}
    END { o=0; for (a in l) for (b in l) if (a<b && a ~ /^e-grow/ && b ~ /^e-grow/) { if (l[a] < d[b] && l[b] < d[a]) o++ } print o }' "$RT/l7/progress.tsv")
[ "$overlap" = 0 ] && pass "E-1: --grow-cap 1 never runs two grow-lane units at once (progress.tsv)" || fail "E-1: $overlap overlapping grow pairs"
n1l=$(awk -F'\t' '$1=="launch" && $2=="e-n1"{print $5}' "$RT/l7/progress.tsv"); g1d=$(awk -F'\t' '$1=="done" && $2=="e-grow1"{print $5}' "$RT/l7/progress.tsv")
[ -n "$n1l" ] && [ -n "$g1d" ] && [ "$n1l" -lt "$g1d" ] && pass "E-1: the N1 unit is not held by the grow lane" || fail "E-1: n1 launch=$n1l grow1 done=$g1d"
grep -q '^REGIME	default	1	0$' "$RT/l7/rollup.tsv" && pass "REGIME row records the cap" || fail "REGIME row: $(grep REGIME "$RT/l7/rollup.tsv")"
# Negative control: uncapped (--live-grow) they overlap.
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --no-lpt --live-grow -j 4 --logdir "$RT/l8" e-grow1 e-grow2 e-grow3 >/dev/null 2>&1
overlap=$(awk -F'\t' '$1=="launch"{l[$2]=$5} $1=="done"{d[$2]=$5}
    END { o=0; for (a in l) for (b in l) if (a<b) { if (l[a] < d[b] && l[b] < d[a]) o++ } print o }' "$RT/l8/progress.tsv")
[ "$overlap" -gt 0 ] && pass "E-1 control: --live-grow runs them concurrently ($overlap overlapping pairs)" || fail "E-1 control: no overlap even uncapped — the positive case proved nothing"
grep -q '^REGIME	live-grow	0	' "$RT/l8/rollup.tsv" && pass "REGIME row records live-grow" || fail "REGIME live-grow row missing"
# E-2: a unit that declares its grows and writes the marker is released from the lane before it exits.
{
    printf '#!/bin/sh\n# arms: A\n# fixture: A=N3-live\n# grows: A=1\n# worst: A=600\n# forgoes: A=-\n: grow_to_3\ncase "${ARM:?}" in\n'
    printf '  A) echo grown >> "$SIM_GROW_DONE_FILE"; sleep 6; '; verdict e-rel GREEN 0 0 0 0 0 0; armline; printf ' exit 0 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/e-rel.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --no-lpt --grow-cap 1 -j 4 --logdir "$RT/l9" e-rel.A e-grow1 >/dev/null 2>&1
l2=$(awk -F'\t' '$1=="launch" && $2=="e-grow1"{print $5}' "$RT/l9/progress.tsv"); d1=$(awk -F'\t' '$1=="done" && $2=="e-rel.A"{print $5}' "$RT/l9/progress.tsv")
[ -n "$l2" ] && [ -n "$d1" ] && [ "$l2" -lt "$d1" ] && pass "E-2: a grow-done marker releases the lane slot before the unit exits" || fail "E-2: grow1 launch=$l2 rel done=$d1"

# E-3 (external review F4): the REAL grow fixture (drills/lib/cluster.sh) against a stub `simcluster` that
# writes the per-grow marker exactly as cmd_grow does (one line per successful grow whenever
# SIM_GROW_DONE_FILE is set). Attempt 1 grows brk2 and fails brk3; the fixture nukes and retries; attempt 2
# grows brk2 and PAUSES in brk3. While it is paused the sidecar must hold ZERO lines — the fixture owns the
# markers and writes them only after its post-check — so the lane still counts the unit as growing (with
# per-grow writes the count was already 2 ≥ declared 2 here and the slot was released mid-retry). After
# success the sidecar holds exactly the declared two lines; a fixture that fails outright writes none.
# origin: simcluster-speed external review F4
E3=$RT/e3; mkdir -p "$E3"
cat > "$E3/sim-stub" <<'EOF'
#!/bin/sh
# mirrors cmd_grow's marker protocol: a completed grow appends `<epoch> <joiner>` when the sidecar is set
case "$1" in
  up) n=$(cat "$E3_DIR/attempt" 2>/dev/null || echo 0); echo $((n+1)) > "$E3_DIR/attempt" ;;
  init|nuke) exit 0 ;;
  grow)
    n=$(cat "$E3_DIR/attempt")
    if [ "$2" = brk3 ] && [ "$n" = 1 ]; then exit 1; fi
    if [ "$2" = brk3 ]; then
      touch "$E3_DIR/second-brk3-running"
      while [ ! -e "$E3_DIR/release-brk3" ]; do sleep 0.05; done
      touch "$E3_DIR/three-voters"
    fi
    [ -n "${SIM_GROW_DONE_FILE:-}" ] && printf '%s %s\n' "$(date +%s)" "$2" >> "$SIM_GROW_DONE_FILE"
    exit 0 ;;
  *) exit 9 ;;
esac
EOF
chmod +x "$E3/sim-stub"
cat > "$E3/driver.sh" <<EOF
#!/bin/sh
# runs the real grow_to_3 with the stubs a drill would have around it
E3_DIR=$E3; export E3_DIR
SIM=$E3/sim-stub; export SIM
SIM_GROW_DONE_FILE=$E3/unit.grow-done; export SIM_GROW_DONE_FILE
. $SIM_ROOT/drills/lib/cluster.sh
log() { printf '%s\n' "\$*"; }; warn() { log "\$@"; }; err() { log "\$@"; }; ok() { log "\$@"; }
_three_voters() { test -f "$E3/three-voters"; }
poll_until() { shift 3; [ "\${1:-}" = -- ] && shift; "\$@"; }
grow_to_3 3 1; echo "grow_to_3 rc=\$?" > "$E3/rc"
EOF
: > "$E3/unit.grow-done"
sh "$E3/driver.sh" > "$E3/driver.out" 2>&1 &
e3pid=$!
e3_deadline=$(( $(date +%s) + 20 ))
while [ ! -e "$E3/second-brk3-running" ] && [ "$(date +%s)" -lt "$e3_deadline" ]; do sleep 0.1; done
if [ -e "$E3/second-brk3-running" ]; then
    mid=$(grep -c . "$E3/unit.grow-done" 2>/dev/null); case "$mid" in ''|*[!0-9]*) mid=0 ;; esac
    [ "$mid" = 0 ] && pass "E-3: while attempt 2's brk3 is still growing the sidecar holds 0 lines (per-grow writes would have made it 2 ≥ declared 2 and released the slot)" || fail "E-3: sidecar holds $mid line(s) mid-retry — the lane would release this unit while it is still growing"
else
    fail "E-3: the fixture never reached attempt 2's brk3 (driver output: $(tail -3 "$E3/driver.out"))"
fi
touch "$E3/release-brk3"
wait "$e3pid" 2>/dev/null || true
grep -q 'grow_to_3 rc=0' "$E3/rc" 2>/dev/null && pass "E-3: the retried grow succeeded" || fail "E-3: grow_to_3 did not succeed: $(cat "$E3/rc" 2>/dev/null; tail -3 "$E3/driver.out")"
[ "$(grep -c . "$E3/unit.grow-done")" = 2 ] && grep -q ' brk2$' "$E3/unit.grow-done" && grep -q ' brk3$' "$E3/unit.grow-done" && pass "E-3: after success the sidecar holds exactly the declared two lines (brk2, brk3), written by the fixture" || fail "E-3: sidecar after success: $(cat "$E3/unit.grow-done")"
grep -q 'GROW-ATTEMPTS: 2' "$E3/driver.out" && pass "E-3: the retry is still first-class evidence (GROW-ATTEMPTS: 2)" || fail "E-3: GROW-ATTEMPTS trailer missing"
# E-3b: a fixture that fails both attempts writes nothing — the slot is held until the unit exits.
E3B=$RT/e3b; mkdir -p "$E3B"
cat > "$E3B/sim-stub" <<'EOF'
#!/bin/sh
case "$1" in
  up|init|nuke) exit 0 ;;
  grow) [ "$2" = brk2 ] && { [ -n "${SIM_GROW_DONE_FILE:-}" ] && printf '%s %s\n' "$(date +%s)" "$2" >> "$SIM_GROW_DONE_FILE"; exit 0; }; exit 1 ;;
  *) exit 9 ;;
esac
EOF
chmod +x "$E3B/sim-stub"
sed -e "s#$E3/sim-stub#$E3B/sim-stub#; s#$E3/unit.grow-done#$E3B/unit.grow-done#; s#$E3/three-voters#$E3B/three-voters#; s#$E3/rc#$E3B/rc#" "$E3/driver.sh" > "$E3B/driver.sh"
: > "$E3B/unit.grow-done"
sh "$E3B/driver.sh" > "$E3B/driver.out" 2>&1
grep -q 'grow_to_3 rc=1' "$E3B/rc" && [ "$(grep -c . "$E3B/unit.grow-done")" = 0 ] && pass "E-3b: a grow that fails both attempts leaves the sidecar empty (brk2 succeeded twice; no line) — the slot is held to exit" || fail "E-3b: rc=$(cat "$E3B/rc") sidecar=$(cat "$E3B/unit.grow-done")"
# E-3c: grow_to_2 writes its single line only after BOTH false-green guards; a guard failure writes none.
E3C=$RT/e3c; mkdir -p "$E3C"
cat > "$E3C/driver.sh" <<EOF
#!/bin/sh
SIM=$E3B/sim-stub; export SIM
SIM_GROW_DONE_FILE=$E3C/unit.grow-done; export SIM_GROW_DONE_FILE
. $SIM_ROOT/drills/lib/cluster.sh
log() { printf '%s\n' "\$*"; }; warn() { log "\$@"; }; err() { log "\$@"; }; ok() { log "\$@"; }
poll_until() { shift 3; [ "\${1:-}" = -- ] && shift; "\$@"; }
_two_voters() { return 0; }
_js_meta_size_2() { test -f "$E3C/meta-formed"; }
sim_leader() { echo brk1; }
grow_to_2 0 1; echo "grow_to_2 rc=\$?" > "$E3C/rc"
EOF
: > "$E3C/unit.grow-done"
sh "$E3C/driver.sh" > "$E3C/driver.out" 2>&1
grep -q 'grow_to_2 rc=1' "$E3C/rc" && [ "$(grep -c . "$E3C/unit.grow-done")" = 0 ] && pass "E-3c: grow_to_2 with the JS-meta guard failing writes no marker (brk2 itself grew)" || fail "E-3c: rc=$(cat "$E3C/rc") sidecar=$(cat "$E3C/unit.grow-done")"
touch "$E3C/meta-formed"; : > "$E3C/unit.grow-done"
sh "$E3C/driver.sh" > "$E3C/driver.out" 2>&1
grep -q 'grow_to_2 rc=0' "$E3C/rc" && [ "$(grep -c . "$E3C/unit.grow-done")" = 1 ] && grep -q ' brk2$' "$E3C/unit.grow-done" && pass "E-3c: grow_to_2 with both guards passing writes exactly its one line" || fail "E-3c: rc=$(cat "$E3C/rc") sidecar=$(cat "$E3C/unit.grow-done")"

echo "── H: round-2 runner edges (R3-F4/F5/F6/F10/F11/F15) ───────────────────────────"
# H-1 (R3-F4): the grow lane's marker read must not spray shell errors — the l9 sweep above kept a unit
# in its grow phase for several scheduler ticks with an EMPTY marker file, which is the shape that produced
# 2535 `integer expression expected` lines in one -j12 sweep.
if grep -q 'integer expression expected' "$RT/l7.out"; then fail "H-1: the runner's own output carries 'integer expression expected' (grow_active's count read is broken)"; else pass "H-1: no 'integer expression expected' in a grow-lane sweep's output"; fi
# H-2 (R3-F5): `worst: 0` must not become `timeout 0` (no limit); it reads as "not declared" → global ceiling.
{
    printf '#!/bin/sh\n# arms: A\n# fixture: A=N1\n# grows: A=0\n# worst: A=0\n# forgoes: A=-\ncase "${ARM:?}" in\n'
    printf '  A) '; verdict h-w0 GREEN 0 0 0 0 0 0; armline; printf ' exit 0 ;;\n'
    printf '  *) exit 2 ;;\nesac\n'
} > "$RT/drills/h-w0.sh"
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --drill-timeout 600 --logdir "$RT/lh2" h-w0.A >/dev/null 2>&1
[ "$(cat "$RT/lh2/h-w0.A.tmo" 2>/dev/null)" = 600 ] && pass "H-2: worst=0 falls back to the global ceiling (600), never timeout 0" || fail "H-2: tmo=$(cat "$RT/lh2/h-w0.A.tmo" 2>/dev/null)"
# H-3 (R3-F6): an attribution re-run of an ARM unit keeps the DRILL-ARM cross-check — a deterministic
# contract violation is a REGRESSION, not LOAD-SENSITIVE. f-ca.A always prints `DRILL-ARM arm=Z`.
printf 'f-ca.A\tGREEN\t0\t-\t-\tnone\n' >> "$RT/expected-verdicts.tsv"
bash "$RT/run-drills.sh" --skip-preflight --no-retry -j 2 --logdir "$RT/lh3" f-ca.A p-plain >/dev/null 2>&1
attr=$(awk -F'\t' '$1=="ATTRIBUTION" && $2=="f-ca.A"{print $3}' "$RT/lh3/rollup.tsv")
[ "$attr" = REGRESSION ] && pass "H-3: the attribution re-run re-applies the DRILL-ARM cross-check → REGRESSION" || fail "H-3: attribution label '$attr' (want REGRESSION): $(grep ATTRIBUTION "$RT/lh3/rollup.tsv")"
# H-4 (R3-F15): a unit named twice on the command line runs once.
out=$(bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --logdir "$RT/lh4" f-split.A f-split.A 2>&1)
[ "$(awk -F'\t' '$1=="launch" && $2=="f-split.A"' "$RT/lh4/progress.tsv" | wc -l | tr -d ' ')" = 1 ] && pass "H-4: a duplicated unit on argv is launched once" || fail "H-4: $(grep -c launch "$RT/lh4/progress.tsv") launches"
# H-5 (R3-F15): a one-arm manifest still gets its ARMS row.
grep -q '^ARMS	g-slow	' "$RT/l6/rollup.tsv" && pass "H-5: a single-arm manifested drill has an ARMS row" || fail "H-5: no ARMS row for the single-arm drill g-slow"
# H-6 (R3-F11): replaying an archive that predates the split — only `<drill>.log` exists — classifies the
# whole-drill log once as a LEGACY-UNIT, never two CONTRACT-ERROR arms.
mkdir -p "$RT/lh6"
printf 'DRILL-VERDICT verdict=INCOMPLETE rc=4 assert_fail=0 setup_red=0 product_red=0 not_covered=2 nc_gap=2 nc_guard=0 pass=9 -- f-split\n' > "$RT/lh6/f-split.log"
printf '4\n' > "$RT/lh6/f-split.rc"; printf '120\n' > "$RT/lh6/f-split.secs"
out=$(bash "$RT/run-drills.sh" --replay --logdir "$RT/lh6" f-split 2>&1)
if [ "$(awk -F'\t' '$1=="f-split"{print $2}' "$RT/lh6/rollup.tsv")" = INCOMPLETE ] && ! grep -q '^f-split\.' "$RT/lh6/rollup.tsv" && printf '%s' "$out" | grep -q 'LEGACY-UNIT'; then
    pass "H-6: --replay of a pre-split archive reads the whole-drill log once as a LEGACY-UNIT"
else
    fail "H-6: $(grep -E '^f-split' "$RT/lh6/rollup.tsv"; printf '%s' "$out" | grep -c LEGACY-UNIT) legacy mention(s)"
fi
grep -q '^ARMS	f-split	' "$RT/lh6/rollup.tsv" && fail "H-6: a LEGACY-UNIT must not be joined as ARMS" || pass "H-6: no ARMS join for a LEGACY-UNIT"
# H-7 (R3-F11): a unit whose log is simply absent is INFRA-ABORT, not a CONTRACT-ERROR about a verdict line.
mkdir -p "$RT/lh7"; : > "$RT/lh7/p-plain.log"
bash "$RT/run-drills.sh" --replay --logdir "$RT/lh7" p-plain f-split.A >/dev/null 2>&1
[ "$(awk -F'\t' '$1=="f-split.A"{print $2}' "$RT/lh7/rollup.tsv")" = INFRA-ABORT ] && pass "H-7: a missing unit log classifies INFRA-ABORT" || fail "H-7: $(grep '^f-split.A' "$RT/lh7/rollup.tsv")"

echo "── external review F3 / F5: replay reads the archive, it does not re-describe it ────────────"
# H-8 (F3): a MIXED archive — one arm's post-split log plus the pre-split whole-drill log, the sibling arms
# absent. Legacy eligibility is per DRILL: any arm log means "post-split", so the absent arms are missing
# arms (INFRA-ABORT, arms_missing, the drill blocks), never the old parent log standing in for them.
mkdir -p "$RT/lh8"
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=3 -- f-split\nDRILL-ARM arm=A of=f-split\n' > "$RT/lh8/f-split.A.log"
printf '0\n' > "$RT/lh8/f-split.A.rc"; printf '30\n' > "$RT/lh8/f-split.A.secs"
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=9 -- f-split\n' > "$RT/lh8/f-split.log"
printf '0\n' > "$RT/lh8/f-split.rc"; printf '120\n' > "$RT/lh8/f-split.secs"
out=$(bash "$RT/run-drills.sh" --replay --logdir "$RT/lh8" f-split 2>&1); rc=$?
if printf '%s' "$out" | grep -q 'LEGACY-UNIT' || grep -q '^f-split	' "$RT/lh8/rollup.tsv"; then
    fail "H-8: a mixed archive fell back to the pre-split parent log (LEGACY-UNIT) although f-split.A.log exists"
else
    pass "H-8: a mixed archive is never LEGACY-UNIT (one arm log present ⇒ the archive is post-split)"
fi
arms=$(awk -F'\t' '$1=="ARMS" && $2=="f-split"' "$RT/lh8/rollup.tsv")
[ "$(printf '%s' "$arms" | cut -f4)" = 2 ] && pass "H-8: the two absent arms count as arms_missing=2" || fail "H-8: ARMS row: $arms"
[ "$(awk -F'\t' '$1=="f-split.D"{print $2}' "$RT/lh8/rollup.tsv")" = INFRA-ABORT ] && [ "$(awk -F'\t' '$1=="f-split.F"{print $2}' "$RT/lh8/rollup.tsv")" = INFRA-ABORT ] && pass "H-8: the absent arms are INFRA-ABORT units" || fail "H-8: $(grep -E '^f-split\.(D|F)' "$RT/lh8/rollup.tsv")"
[ "$rc" != 0 ] && ! printf '%s' "$out" | grep -q 'ALL GREEN' && pass "H-8: the replay exits non-zero (rc=$rc), not ALL GREEN" || fail "H-8: rc=$rc; $(printf '%s' "$out" | grep -c 'ALL GREEN') ALL GREEN line(s)"
# H-8b: selecting the ABSENT arm explicitly on the same mixed archive is still a missing arm, not legacy.
bash "$RT/run-drills.sh" --replay --logdir "$RT/lh8" f-split.D >"$RT/lh8b.out" 2>&1
[ "$(awk -F'\t' '$1=="f-split.D"{print $2}' "$RT/lh8/rollup.tsv")" = INFRA-ABORT ] && ! grep -q 'LEGACY-UNIT' "$RT/lh8b.out" && pass "H-8b: an explicit single-arm selection of the absent arm is INFRA-ABORT, not LEGACY-UNIT" || fail "H-8b: $(grep -E '^f-split' "$RT/lh8/rollup.tsv"; grep -c LEGACY-UNIT "$RT/lh8b.out")"
# H-8c: the pure pre-split archive still falls back (H-6's case) — also when ONE arm is named explicitly.
bash "$RT/run-drills.sh" --replay --logdir "$RT/lh6" f-split.A >"$RT/lh8c.out" 2>&1
[ "$(awk -F'\t' '$1=="f-split"{print $2}' "$RT/lh6/rollup.tsv")" = INCOMPLETE ] && grep -q 'LEGACY-UNIT' "$RT/lh8c.out" && pass "H-8c: a pure pre-split archive is LEGACY-UNIT even under an explicit single-arm selection" || fail "H-8c: $(grep -E '^f-split' "$RT/lh6/rollup.tsv"; grep -c LEGACY-UNIT "$RT/lh8c.out")"

# H-9 (F5): the REGIME row is the ARCHIVE's regime. A live sweep writes regime.tsv once; replay copies it
# and never rewrites it; an archive without one reads `unknown - -`, and the regime flags are refused
# under --replay (they describe a run, and replay runs nothing).
[ -f "$RT/l8/regime.tsv" ] && grep -q '^REGIME	live-grow	0	0$' "$RT/l8/regime.tsv" && pass "H-9: a live sweep writes regime.tsv (live-grow 0 0)" || fail "H-9: regime.tsv after the live-grow sweep: $(cat "$RT/l8/regime.tsv" 2>/dev/null)"
cp "$RT/l8/regime.tsv" "$RT/lh9.regime.before"
bash "$RT/run-drills.sh" --replay --logdir "$RT/l8" e-grow1 e-grow2 e-grow3 >/dev/null 2>&1
grep -q '^REGIME	live-grow	0	0$' "$RT/l8/rollup.tsv" && pass "H-9: a plain --replay of a live-grow archive keeps REGIME live-grow (not this invocation's default 5)" || fail "H-9: REGIME after replay: $(grep REGIME "$RT/l8/rollup.tsv")"
cmp -s "$RT/lh9.regime.before" "$RT/l8/regime.tsv" && pass "H-9: replay leaves regime.tsv byte-identical" || fail "H-9: replay rewrote regime.tsv"
bash "$RT/run-drills.sh" --replay --logdir "$RT/l7" e-grow1 e-grow2 e-grow3 e-n1 >/dev/null 2>&1
grep -q '^REGIME	default	1	0$' "$RT/l7/rollup.tsv" && pass "H-9: a replay of a --grow-cap 1 archive keeps the non-default cap" || fail "H-9: REGIME after replay of l7: $(grep REGIME "$RT/l7/rollup.tsv")"
# a non-default stagger survives too (an N1 unit is never staggered, so this sweep costs nothing)
bash "$RT/run-drills.sh" --skip-preflight --no-retry --no-attribute --grow-stagger 7 --logdir "$RT/lh9s" p-plain >/dev/null 2>&1
bash "$RT/run-drills.sh" --replay --logdir "$RT/lh9s" p-plain >/dev/null 2>&1
grep -q '^REGIME	default	5	7$' "$RT/lh9s/rollup.tsv" && pass "H-9: a replay keeps a non-default stagger (7)" || fail "H-9: REGIME after replay of lh9s: $(grep REGIME "$RT/lh9s/rollup.tsv")"
out=$(bash "$RT/run-drills.sh" --replay --live-grow --logdir "$RT/l7" e-grow1 2>&1); rc=$?
[ "$rc" = 2 ] && printf '%s' "$out" | grep -q 'cannot be combined with --replay' && grep -q '^REGIME	default	1	0$' "$RT/l7/rollup.tsv" && pass "H-9: --live-grow with --replay is refused (rc=2) and the archive's REGIME row stands" || fail "H-9: rc=$rc out=$(printf '%s' "$out" | head -2)"
# an archive that predates regime.tsv (lh6, hand-made above) reads unknown, never a default
grep -q '^REGIME	unknown	-	-$' "$RT/lh6/rollup.tsv" && pass "H-9: an archive without regime.tsv reports REGIME unknown - -" || fail "H-9: REGIME for the pre-metadata archive: $(grep REGIME "$RT/lh6/rollup.tsv")"

# A malformed raw receipt is unknown, never evidence for a known or uncapped regime.
# origin: simcluster-speed external review round 3 R3-F2
for shape in empty truncated empty-cap negative-cap nonnumeric-stagger extra-field duplicate; do
    rd="$RT/regime-$shape"; mkdir -p "$rd"
    cp "$RT/lh9s/p-plain.log" "$rd/p-plain.log"
    cp "$RT/lh9s/p-plain.rc" "$rd/p-plain.rc"
    case "$shape" in
        empty) : > "$rd/regime.tsv" ;;
        truncated) printf 'REGIME\tlive-grow\n' > "$rd/regime.tsv" ;;
        empty-cap) printf 'REGIME\tdefault\t\t0\n' > "$rd/regime.tsv" ;;
        negative-cap) printf 'REGIME\tdefault\t-1\t0\n' > "$rd/regime.tsv" ;;
        nonnumeric-stagger) printf 'REGIME\tdefault\t5\tbroken\n' > "$rd/regime.tsv" ;;
        extra-field) printf 'REGIME\tdefault\t5\t0\textra\n' > "$rd/regime.tsv" ;;
        duplicate) printf 'REGIME\tlive-grow\t0\t0\nREGIME\tdefault\t5\t0\n' > "$rd/regime.tsv" ;;
    esac
    cp "$rd/regime.tsv" "$rd/regime.before"
    out=$(bash "$RT/run-drills.sh" --replay --logdir "$rd" p-plain 2>&1); rc=$?
    if [ "$rc" = 0 ] && grep -q '^REGIME	unknown	-	-$' "$rd/rollup.tsv" &&
       printf '%s' "$out" | grep -q 'missing or invalid regime.tsv' && cmp -s "$rd/regime.before" "$rd/regime.tsv"; then
        pass "REGIME $shape: unknown, diagnosed, original metadata unchanged"
    else
        fail "REGIME $shape: rc=$rc row=$(grep '^REGIME' "$rd/rollup.tsv")"
    fi
done

echo "────────────────────────────────────────────────────────────────────────────────"
[ "$FAILS" -eq 0 ] && echo "arm-aggregation-test: ALL PASS" || { echo "arm-aggregation-test: $FAILS FAILED" >&2; exit 1; }
