#!/bin/sh
# Independent adversarial cases for the developer's response to the first acceleration review.
# These are intentionally RED while the response still leaves the reviewed boundary open.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIMROOT="$(cd "$HERE/.." && pwd)"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

RT=$(mktemp -d)
trap 'rm -rf "$RT"' EXIT

runner_world() {
    _w=$1
    mkdir -p "$_w/drills"
    ln -sf "$SIMROOT/run-drills.sh" "$_w/run-drills.sh"
    cat >"$_w/simcluster" <<'EOF'
#!/bin/sh
case "$1" in
  check-image) exit 0 ;;
  drill)
    shift
    d=$1
    exec sh "$(dirname "$0")/drills/$d.sh"
    ;;
esac
exit 9
EOF
    chmod +x "$_w/simcluster"
}

echo "── a #67 band must distinguish the cause, not only the assertion title ─────────"
W="$RT/band"; runner_world "$W"; mkdir -p "$W/log"
: >"$W/drills/74-rebalance-on-return.sh"
cat >"$W/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
74-rebalance-on-return	INCOMPLETE	-	ASSERT-FAIL@#67@sig:b-negctrl-create	#67	74-rebalance-on-return
EOF
cat >"$W/expected-verdicts-log.md" <<'EOF'
## 74-rebalance-on-return
  sig:b-negctrl-create := B-negctrl-create ordinary expose reg create command succeeded
EOF
cat >"$W/log/74-rebalance-on-return.log" <<'EOF'
[simcluster] 74: negative-control expose reg create rc=64: permission denied (not JetStream #67)
[err ] FAIL  B-negctrl-create ordinary expose reg create command succeeded (rc=0) (want exit 0, got 1)
DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- 74
EOF
printf '1\n' >"$W/log/74-rebalance-on-return.rc"
printf '1\n' >"$W/log/74-rebalance-on-return.secs"
bash "$W/run-drills.sh" --replay --logdir "$W/log" 74-rebalance-on-return >/dev/null 2>&1 || true
bm=$(awk -F'\t' '$1=="74-rebalance-on-return"{print $15}' "$W/log/rollup.tsv")
case "$bm" in
    MATCH-BAND*) fail "#67 band absorbed a permission failure because the fixed assertion title is identical" ;;
    DEVIATION) pass "#67 band rejects a different cause at the same assertion" ;;
    *) fail "unexpected band classification [$bm]" ;;
esac

echo "── image precheck must not be forgeable from public workspace state ────────────"
I="$RT/image"; mkdir -p "$I/drills" "$I/vendor"
cp "$SIMROOT/simcluster" "$I/simcluster"
cp -R "$SIMROOT/lib" "$I/lib"
printf 'not-present-in-any-image\n' >"$I/vendor/tether"
chmod +x "$I/vendor/tether"
cat >"$I/drills/x.sh" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$I/drills/x.sh"
sha=$(sha256sum "$I/vendor/tether" | awk '{print $1}')
if SIM_KEEP=1 "$I/simcluster" drill x --image-prechecked "$sha" >/dev/null 2>&1; then
    fail "a caller forged --image-prechecked from sha256sum vendor/tether and skipped every image check"
else
    pass "an explicit caller cannot forge the one-per-sweep image attestation"
fi

echo "── validator signature slugs must share a literal safe grammar with runtime ────"
V="$RT/validator"; mkdir -p "$V/drills"
: >"$V/drills/90-d.sh"
cat >"$V/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
90-d	INCOMPLETE	1	ASSERT-FAIL@#10@sig:x/y	#10	90-d
EOF
cat >"$V/expected-verdicts-log.md" <<'EOF'
## 90-d
  sig:x/y := exact-failure
EOF
cat >"$V/gotchas.md" <<'EOF'
### #10 open
Status: OPEN.
EOF
vo=$(TETHER_VERDICTS_TSV="$V/expected-verdicts.tsv" \
     TETHER_VERDICTS_LOG="$V/expected-verdicts-log.md" \
     TETHER_LEDGER="$V/gotchas.md" TETHER_DRILLDIR="$V/drills" \
     sh "$HERE/validate-verdicts.sh" 2>&1)
vrc=$?
if [ "$vrc" -ne 0 ]; then
    pass "validator rejects a slug that breaks runtime's sed resolver"
else
    fail "validator accepted sig:x/y although runtime _sig_regex cannot resolve that sed expression"
fi

echo "── a non-existent logdir alias must not clean its existing parent ───────────────"
L="$RT/logdir"; runner_world "$L"; mkdir -p "$L/victim"
cat >"$L/drills/00-green.sh" <<'EOF'
#!/bin/sh
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- green\n' >&2
exit 0
EOF
cat >"$L/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
00-green	GREEN	0	-	-	00-green
EOF
printf '## 00-green\n' >"$L/expected-verdicts-log.md"
printf 'must-survive\n' >"$L/victim/keep.log"
bash "$L/run-drills.sh" --skip-preflight --no-retry --no-attribute \
    --logdir "$L/victim/new/.." 00-green >/dev/null 2>&1 || true
if [ -f "$L/victim/keep.log" ]; then
    pass "logdir canonicalization protects an existing parent from alias cleanup"
else
    fail "--logdir victim/new/.. bypassed canonicalization and deleted victim/keep.log"
fi

echo "── TERM must leave a partial run without RUN-COMPLETE ──────────────────────────"
S="$RT/signal"; runner_world "$S"
cat >"$S/drills/00-slow.sh" <<'EOF'
#!/bin/sh
/bin/sleep 5
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- slow\n' >&2
exit 0
EOF
cat >"$S/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
00-slow	GREEN	0	-	-	00-slow
EOF
printf '## 00-slow\n' >"$S/expected-verdicts-log.md"
bash "$S/run-drills.sh" --skip-preflight --no-retry --no-attribute \
    --logdir "$S/log" 00-slow >/dev/null 2>&1 &
spid=$!
/bin/sleep 1
kill -TERM "$spid" 2>/dev/null || true
wait "$spid" 2>/dev/null || true
if [ -f "$S/log/progress.tsv" ] && grep -q '^RUN-COMPLETE' "$S/log/progress.tsv"; then
    fail "TERM trap returned to the runner and forged RUN-COMPLETE for an interrupted sweep"
else
    pass "TERM exits without a completion sentinel"
fi

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then
    echo "simcluster-accel external re-review: ALL PASS"
    exit 0
fi
echo "simcluster-accel external re-review: $FAILS FAILED"
exit 1
