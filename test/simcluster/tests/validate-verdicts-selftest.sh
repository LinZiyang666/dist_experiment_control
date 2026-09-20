#!/bin/sh
# validate-verdicts-selftest.sh — NON-VACUITY harness for tests/validate-verdicts.sh.
#
# WHY THIS EXISTS. validate-verdicts.sh is the gate that keeps the machine-readable expectation table
# machine-readable. But a gate that always says OK is worse than no gate — and this increment already
# shipped one born-vacuous validator loop (the `printf '%s' | while read` band check that never executed
# while reporting OK, caught only by a mutation of the DATA). Internal review MA11 then found the reverse
# gap: every RULE in validate-verdicts.sh could be deleted and `run-all.sh` stayed green, because nothing
# fed it a table that SHOULD fail. This harness does: it starts from a known-good sandbox and applies one
# violation at a time, asserting each is rejected with its own tag. Delete a rule, its case here flips
# from rejected to accepted, and this test fails.
#
# The validator reads its inputs from TETHER_VERDICTS_TSV / _LOG / TETHER_LEDGER / TETHER_DRILLDIR when
# set, so the whole world under test is a throwaway sandbox and the repo is never touched.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
VALIDATOR="$HERE/validate-verdicts.sh"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

RT=$(mktemp -d); trap 'rm -rf "$RT"' EXIT
mkdir -p "$RT/drills"
export TETHER_VERDICTS_TSV="$RT/expected-verdicts.tsv"
export TETHER_VERDICTS_LOG="$RT/expected-verdicts-log.md"
export TETHER_LEDGER="$RT/gotchas.md"
export TETHER_DRILLDIR="$RT/drills"

# A minimal VALID world. Drills are numeric-prefixed so they match the validator's `[0-9]*.sh` glob (the
# real drills are 00-skeleton.sh etc.). One band pins an OPEN defect with a DEFINED signature.
# 95-split is an ARM-SPLIT drill (`# arms: A B`, simcluster-speed plan §5.5): its parent row is derived
# from the two child rows (join INCOMPLETE, nc_gap 1+2, bands `-`), and B carries an arm-suffixed band.
seed() {
    rm -f "$RT/drills"/*.sh
    : > "$RT/drills/90-d1.sh"; : > "$RT/drills/91-d2.sh"
    printf '#!/bin/sh\n# arms: A B\n# fixture: A=N1 B=N1\n# grows: A=0 B=0\n# worst: A=60 B=60\n# forgoes: A=- B=-\ncase "${ARM:?}" in A) ;; B) ;; *) setup_fail x ;; esac\n' > "$RT/drills/95-split.sh"
    cat > "$TETHER_VERDICTS_TSV" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
90-d1	GREEN	0	-	-	90-d1
91-d2	INCOMPLETE	1	ASSERT-FAIL@#10@sig:known	#10	91-d2
95-split	INCOMPLETE	3	-	#10 parent debt	95-split
95-split.A	INCOMPLETE	1	-	-	95-split
95-split.B	INCOMPLETE	2	INCOMPLETE@#10@sig:b-thing-B	#10	95-split
EOF
    cat > "$TETHER_VERDICTS_LOG" <<'EOF'
## 90-d1
## 91-d2
  sig:known := some-error-pattern
## 95-split
  sig:b-thing-B := arm B pattern
## 95-split.A
EOF
    cat > "$TETHER_LEDGER" <<'EOF'
### #10 an open defect（OPEN；见 #99 已修复 的同族根因）
still open. FIXED elsewhere is not this entry.
### #99 a closed one（FIXED）
the closure word is on the HEADING, the only place either ledger gate reads (lib/ledger.sh).
EOF
}
run_validator() { sh "$VALIDATOR" 2>&1; }

seed
if run_validator | grep -q 'validate-verdicts: OK'; then pass "the seeded valid table passes"
else fail "the seeded valid table does NOT pass — the harness itself is broken"; run_validator | sed 's/^/      /'; fi

# mut <label> <expected-tag> <cmd...> : seed, apply the mutation, assert rejection carrying the tag.
mut() {
    _lbl=$1; _tag=$2; shift 2
    seed; "$@"
    out=$(run_validator)
    if printf '%s' "$out" | grep -q "$_tag" && ! printf '%s' "$out" | grep -q 'validate-verdicts: OK'; then
        pass "$_lbl → rejected ($_tag)"
    else
        fail "$_lbl → NOT rejected (wanted $_tag); validator said: $(printf '%s' "$out" | tail -1)"
    fi
}

mut "band missing signature"        BAND-NO-SIG        sed -i 's|ASSERT-FAIL@#10@sig:known|ASSERT-FAIL@#10|' "$TETHER_VERDICTS_TSV"
mut "band naming no defect"         BAND-NO-DEFECT     sed -i 's|ASSERT-FAIL@#10@sig:known|ASSERT-FAIL@nope@sig:known|' "$TETHER_VERDICTS_TSV"
mut "band bad verdict enum"         BAND-ENUM          sed -i 's|ASSERT-FAIL@#10@sig:known|FLAKY@#10@sig:known|' "$TETHER_VERDICTS_TSV"
mut "band signature undefined"      BAND-SIG-UNDEFINED sed -i 's|sig:known|sig:undefined-slug|' "$TETHER_VERDICTS_TSV"
mut "band on a CLOSED defect"       BAND-ON-CLOSED     sed -i 's|ASSERT-FAIL@#10@sig:known|ASSERT-FAIL@#99@sig:known|' "$TETHER_VERDICTS_TSV"
mut "unknown verdict enum"          BAD-ENUM           sed -i 's|^90-d1\tGREEN|90-d1\tMOSTLY-OK|' "$TETHER_VERDICTS_TSV"
mut "GREEN row with nc_gap>0"       GREEN-NCGAP        sed -i 's|^90-d1\tGREEN\t0|90-d1\tGREEN\t2|' "$TETHER_VERDICTS_TSV"
mut "INCOMPLETE pinned at 0 gaps"   INCOMPLETE-ZERO    sed -i 's|^91-d2\tINCOMPLETE\t1|91-d2\tINCOMPLETE\t0|' "$TETHER_VERDICTS_TSV"
mut "non-numeric nc_gap"            BAD-NCGAP          sed -i 's|^91-d2\tINCOMPLETE\t1|91-d2\tINCOMPLETE\tx|' "$TETHER_VERDICTS_TSV"
mut "wrong field count (5 cols)"    ROW-FIELDS         sed -i 's|^90-d1\tGREEN\t0\t-\t-\t90-d1|90-d1\tGREEN\t0\t-\t90-d1|' "$TETHER_VERDICTS_TSV"
mut "dangling note-ref"             NOTE-REF-MISSING   sed -i 's|\t90-d1$|\tno-such-section|' "$TETHER_VERDICTS_TSV"
mut "drill on disk not in table"    DRILL-UNLISTED     sh -c ': > "'"$RT"'/drills/92-d3.sh"'
mut "table row with no drill"       ROW-ORPHAN         sh -c 'printf "93-d9\tGREEN\t0\t-\t-\t93-d9\n" >> "'"$TETHER_VERDICTS_TSV"'"'
# MI7: the closed-defect / band-sig checks must NOT fail open when the ledger is missing.
mut "missing ledger is fail-CLOSED" 'missing gotcha ledger' rm -f "$TETHER_LEDGER"

# ── arm-split child rows (simcluster-speed plan §6.3 V-1…V-8) ────────────────────────────────────────
mut "V-1 parent nc_gap != Σ children"        PARENT-NCGAP        sed -i 's|^95-split\tINCOMPLETE\t3|95-split\tINCOMPLETE\t2|' "$TETHER_VERDICTS_TSV"
mut "V-2 parent carries a band"              PARENT-BANDS        sed -i 's|^95-split\tINCOMPLETE\t3\t-|95-split\tINCOMPLETE\t3\tINCOMPLETE@#10@sig:known|' "$TETHER_VERDICTS_TSV"
mut "V-3 manifest arm with no child row"     ARM-CHILD-COUNT     sed -i '/^95-split\.A\t/d' "$TETHER_VERDICTS_TSV"
mut "V-3b two rows for one arm"              ARM-CHILD-COUNT     sh -c 'printf "95-split.A\tINCOMPLETE\t1\t-\t-\t95-split\n" >> "'"$TETHER_VERDICTS_TSV"'"'
mut "V-4 orphan child (arm not in manifest)" CHILD-ARM-UNKNOWN   sh -c 'printf "95-split.Q\tGREEN\t0\t-\t-\t95-split\n" >> "'"$TETHER_VERDICTS_TSV"'"'
mut "V-4b child of a drill with no manifest" CHILD-NO-MANIFEST   sh -c 'printf "90-d1.A\tGREEN\t0\t-\t-\t90-d1\n" >> "'"$TETHER_VERDICTS_TSV"'"'
mut "V-4c child of a drill not on disk"      CHILD-ORPHAN        sh -c 'printf "96-nope.A\tGREEN\t0\t-\t-\t95-split\n" >> "'"$TETHER_VERDICTS_TSV"'"'
mut "V-5 child owner outside the parent's"   CHILD-OWNER         sed -i 's|^95-split\.B\tINCOMPLETE\t2\tINCOMPLETE@#10@sig:b-thing-B\t#10|95-split.B\tINCOMPLETE\t2\tINCOMPLETE@#10@sig:b-thing-B\t#10 #11|' "$TETHER_VERDICTS_TSV"
mut "V-6 child nc_gap '-' with no section"   CHILD-NCGAP-DASH-NO-SECTION sh -c 'sed -i "s|^95-split\.B\tINCOMPLETE\t2|95-split.B\tINCOMPLETE\t-|; s|^95-split\tINCOMPLETE\t3|95-split\tINCOMPLETE\t-|" "'"$TETHER_VERDICTS_TSV"'"'
mut "V-6b child '-' but parent still numeric" PARENT-NCGAP       sed -i 's|^95-split\.A\tINCOMPLETE\t1|95-split.A\tINCOMPLETE\t-|' "$TETHER_VERDICTS_TSV"
mut "V-7 signature ERE ends with \$"         BAND-SIG-ANCHORED   sed -i 's|sig:b-thing-B := arm B pattern|sig:b-thing-B := arm B pattern$|' "$TETHER_VERDICTS_LOG"
mut "V-7b signature ERE contains @"          BAND-SIG-AT         sed -i 's|sig:b-thing-B := arm B pattern|sig:b-thing-B := arm@B pattern|' "$TETHER_VERDICTS_LOG"
mut "V-8 child band slug without arm suffix" CHILD-BAND-SLUG     sh -c 'sed -i "s|sig:b-thing-B|sig:b-thing|" "'"$TETHER_VERDICTS_TSV"'" "'"$TETHER_VERDICTS_LOG"'"'
mut "V-8b one slug shared by two rows"       BAND-SLUG-SHARED    sed -i 's|^90-d1\tGREEN\t0\t-\t-|90-d1\tGREEN\t0\tASSERT-FAIL@#10@sig:known\t#10|' "$TETHER_VERDICTS_TSV"
mut "V-9 parent expected != children join"   PARENT-JOIN         sed -i 's|^95-split\tINCOMPLETE\t3|95-split\tGREEN\t3|' "$TETHER_VERDICTS_TSV"
# Control: the '-' child WITH its own section is accepted when the parent is '-' as well.
seed; sed -i 's|^95-split\.A\tINCOMPLETE\t1|95-split.A\tINCOMPLETE\t-|; s|^95-split\tINCOMPLETE\t3|95-split\tINCOMPLETE\t-|' "$TETHER_VERDICTS_TSV"
if run_validator | grep -q 'validate-verdicts: OK'; then pass "control: a '-' child with a '## 95-split.A' section and a '-' parent passes"
else fail "control: the documented '-' child form is rejected: $(run_validator | tail -2 | tr '\n' '|')"; fi

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then echo "validate-verdicts-selftest: ALL PASS"; exit 0; else echo "validate-verdicts-selftest: $FAILS FAILED"; exit 1; fi
