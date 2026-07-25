#!/bin/sh
# Independent external-review regressions for the simcluster acceleration increment.
# These cases intentionally exercise guarantees not covered by the increment's own green gates.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

echo "── nested poll modes preserve the outer frame ─────────────────────────────────"
(
    # A virtual integer-second clock makes the sampling-grid assertion exact and instant.
    _er_clock=0
    _er_sleeps=
    date() {
        if [ "${1:-}" = +%s ]; then printf '%s\n' "$_er_clock"
        else command date "$@"
        fi
    }
    sleep() {
        _er_sleeps="${_er_sleeps}${1},"
        _er_clock=$((_er_clock + $1))
    }
    . "$HERE/../lib/log.sh"

    _er_outer_calls=0
    _er_inner() { return 0; }
    _er_outer() {
        _er_outer_calls=$((_er_outer_calls+1))
        # This inner fast poll must not change the outer fixed poll's mode.
        poll_until 1 1 "inner fast poll" -- _er_inner >/dev/null 2>&1
        return 1
    }
    poll_until_fixed 4 3 "outer fixed poll" -- _er_outer >/dev/null 2>&1 || true
    if [ "$_er_sleeps" = "3,3," ] && [ "$_er_outer_calls" = 3 ]; then
        exit 0
    fi
    printf 'outer fixed grid was corrupted: sleeps=%s calls=%s (want 3,3, / 3)\n' \
        "$_er_sleeps" "$_er_outer_calls" >&2
    exit 1
) && pass "an inner fast poll cannot flip an outer fixed poll" \
  || fail "an inner fast poll flips the outer fixed poll (mode is not stored in the frame)"

echo "── verdict-table validator rejects ambiguous bands/table rows ─────────────────"
RT=$(mktemp -d)
trap 'rm -rf "$RT"' EXIT
mkdir -p "$RT/drills"
: >"$RT/drills/90-d1.sh"
: >"$RT/drills/91-d2.sh"

seed() {
    cat >"$RT/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
90-d1	GREEN	0	-	-	90-d1
91-d2	INCOMPLETE	1	ASSERT-FAIL@#10@sig:known	#10	91-d2
EOF
    cat >"$RT/expected-verdicts-log.md" <<'EOF'
## 90-d1

green.

## 91-d2

  sig:known := exact-known-failure
EOF
    cat >"$RT/gotchas.md" <<'EOF'
### #10 an open defect

Status: OPEN.
EOF
}

run_validator() {
    TETHER_VERDICTS_TSV="$RT/expected-verdicts.tsv" \
    TETHER_VERDICTS_LOG="$RT/expected-verdicts-log.md" \
    TETHER_LEDGER="$RT/gotchas.md" \
    TETHER_DRILLDIR="$RT/drills" \
        sh "$HERE/validate-verdicts.sh" 2>&1
}

expect_reject() {
    _er_label=$1
    _er_tag=$2
    shift 2
    seed
    "$@"
    _er_out=$(run_validator)
    _er_rc=$?
    if [ "$_er_rc" -ne 0 ] && printf '%s\n' "$_er_out" | grep -q "$_er_tag"; then
        pass "$_er_label"
    else
        fail "$_er_label (validator rc=$_er_rc; wanted $_er_tag)"
    fi
}

expect_reject "a band cannot be ownerless" BAND-NO-OWNER \
    sed -i 's/\t#10\t91-d2$/\t-\t91-d2/' "$RT/expected-verdicts.tsv"
expect_reject "a band must name a defect that exists in the ledger" BAND-UNKNOWN-DEFECT \
    sed -i 's/ASSERT-FAIL@#10@sig:known/ASSERT-FAIL@#777@sig:known/' "$RT/expected-verdicts.tsv"
expect_reject "duplicate drill rows are forbidden" DUPLICATE-DRILL \
    sh -c 'tail -n 1 "$1" >>"$1"' sh "$RT/expected-verdicts.tsv"
expect_reject "a prose mention is not a signature definition" BAND-SIG-UNDEFINED \
    sed -i 's/sig:known := exact-known-failure/prose merely mentions sig:known without defining it/' "$RT/expected-verdicts-log.md"

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then
    echo "simcluster-accel external review: ALL PASS"
    exit 0
fi
echo "simcluster-accel external review: $FAILS FAILED"
exit 1
