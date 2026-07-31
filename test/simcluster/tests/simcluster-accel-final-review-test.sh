#!/bin/sh
# Final-round adversarial cases. These target boundaries not covered by the two prior review gates.
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
    exec sh "$(dirname "$0")/drills/$1.sh"
    ;;
esac
exit 9
EOF
    chmod +x "$_w/simcluster"
}

echo "── a stale cause in the look-behind window must not authorize a new failure ────"
B="$RT/band"; runner_world "$B"; mkdir -p "$B/log"
: >"$B/drills/74-rebalance-on-return.sh"
cat >"$B/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
74-rebalance-on-return	INCOMPLETE	-	ASSERT-FAIL@#67@sig:b-negctrl-create	#67	74-rebalance-on-return
EOF
cat >"$B/expected-verdicts-log.md" <<'EOF'
## 74-rebalance-on-return
  sig:b-negctrl-create := negative-control expose reg create rc=70
EOF
cat >"$B/log/74-rebalance-on-return.log" <<'EOF'
[simcluster] 74: negative-control expose reg create rc=70
[simcluster] 74: cleanup completed
[ ok ] PASS  an unrelated operation completed after the old rc=70 diagnostic
[err ] FAIL  B-negctrl-create ordinary expose reg create command succeeded (rc=0) (want exit 0, got 1)
DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- 74
EOF
printf '1\n' >"$B/log/74-rebalance-on-return.rc"
printf '1\n' >"$B/log/74-rebalance-on-return.secs"
bash "$B/run-drills.sh" --replay --logdir "$B/log" 74-rebalance-on-return >/dev/null 2>&1 || true
bc=$(awk -F'\t' '$1=="74-rebalance-on-return"{print $15}' "$B/log/rollup.tsv")
case "$bc" in
    DEVIATION) pass "only the diagnostic adjacent to the first failure can authorize its band" ;;
    MATCH-BAND*) fail "an obsolete rc=70 diagnostic inside the ten-line window laundered a later permission failure" ;;
    *) fail "unexpected stale-cause classification [$bc]" ;;
esac

echo "── INT/TERM/HUP must terminate the drill process tree, not only its shell ──────"
for sspec in INT:130 TERM:143 HUP:129; do
    ssig=${sspec%%:*}; swant=${sspec#*:}
    S="$RT/signal-$ssig"; runner_world "$S"
    cat >"$S/drills/00-slow.sh" <<'EOF'
#!/bin/sh
/bin/sleep 3
printf 'orphan-survived\n' >"$SURVIVOR_MARKER"
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- slow\n' >&2
EOF
    cat >"$S/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
00-slow	GREEN	0	-	-	00-slow
EOF
    printf '## 00-slow\n' >"$S/expected-verdicts-log.md"
    # `cmd &` in a non-interactive POSIX shell starts with SIGINT ignored, which an executed bash cannot
    # trap; use timeout to keep the runner a foreground child and deliver each signal with normal
    # disposition. --preserve-status exposes the runner's own signal-derived exit.
    SURVIVOR_MARKER="$S/orphan.marker" timeout --preserve-status -k 5 -s "$ssig" 1 \
        bash "$S/run-drills.sh" --skip-preflight --no-retry --no-attribute \
        --logdir "$S/log" 00-slow >/dev/null 2>&1
    src=$?
    /bin/sleep 2
    if [ "$src" = "$swant" ] && [ ! -e "$S/orphan.marker" ] &&
       ! grep -q '^RUN-COMPLETE' "$S/log/progress.tsv" 2>/dev/null; then
        pass "$ssig returns $swant, leaves a partial archive, and reaps drill descendants"
    else
        [ -e "$S/orphan.marker" ] && sorphan=yes || sorphan=no
        grep -q '^RUN-COMPLETE' "$S/log/progress.tsv" 2>/dev/null && scomplete=yes || scomplete=no
        fail "$ssig cleanup returned $src (orphan=$sorphan complete=$scomplete)"
    fi
done

echo "── replay must not grant destructive ownership to an arbitrary directory ──────"
L="$RT/logdir"; runner_world "$L"; mkdir -p "$L/victim"
cat >"$L/drills/00-green.sh" <<'EOF'
#!/bin/sh
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- green\n' >&2
EOF
cat >"$L/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
00-green	GREEN	0	-	-	00-green
EOF
printf '## 00-green\n' >"$L/expected-verdicts-log.md"
printf 'must-survive\n' >"$L/victim/keep.log"
bash "$L/run-drills.sh" --replay --logdir "$L/victim" 00-green >/dev/null 2>&1 || true
bash "$L/run-drills.sh" --skip-preflight --no-retry --no-attribute \
    --logdir "$L/victim" 00-green >/dev/null 2>&1 || true
if [ -f "$L/victim/keep.log" ]; then
    pass "replay did not bless an unrelated directory for a later destructive cleanup"
else
    fail "replay wrote .simdrills-owned into an arbitrary directory; the next live run deleted keep.log"
fi

echo "── telemetry must survive an interval with launches but zero completions ───────"
T="$RT/telemetry"; runner_world "$T"; mkdir -p "$T/fakebin"
cat >"$T/drills/00-slow.sh" <<'EOF'
#!/bin/sh
/bin/sleep 2
printf 'DRILL-VERDICT verdict=GREEN rc=0 assert_fail=0 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- slow\n' >&2
EOF
cat >"$T/expected-verdicts.tsv" <<'EOF'
# drill	expected	expected_nc_gap	bands	owner	note-ref
00-slow	GREEN	0	-	-	00-slow
EOF
printf '## 00-slow\n' >"$T/expected-verdicts-log.md"
cat >"$T/fakebin/sleep" <<'EOF'
#!/bin/sh
case "$1" in 60) exec /bin/sleep 0.1 ;; *) exec /bin/sleep "$@" ;; esac
EOF
chmod +x "$T/fakebin/sleep"
PATH="$T/fakebin:$PATH" bash "$T/run-drills.sh" --skip-preflight --no-retry --no-attribute \
    --logdir "$T/log" 00-slow >/dev/null 2>&1 || true
if [ "$(wc -l <"$T/log/host-telemetry.tsv" 2>/dev/null || echo 0)" -ge 2 ] &&
   awk -F'\t' '$3==1{ok=1} END{exit !ok}' "$T/log/host-telemetry.tsv"; then
    pass "the sampler records an in-flight drill even when completed count is zero"
else
    fail "grep -c emitted '0' before '|| echo 0'; arithmetic killed the sampler while one drill was running"
fi

echo "── persisted argv must redact boundary-rich secret values before formatting ──"
# shellcheck source=../lib/assert.sh
. "$SIMROOT/lib/assert.sh"
secret_space="alpha beta"
secret_quote="quo'ted"
secret_line='line1
line2'
argv=$(_as_format_argv cmd --pin "$secret_space" --token="$secret_quote" \
    "tether-invite:v1?pin=$secret_line&sid=lab" PASSWORD="$secret_line" public)
if printf '%s' "$argv" | grep -F -e "$secret_space" -e "$secret_quote" -e line1 -e line2 >/dev/null; then
    fail "argv formatting leaked a spaced, quoted, multiline, assignment, or URI secret"
elif [ "$(printf '%s' "$argv" | grep -o '<REDACTED' | wc -l | tr -d ' ')" -ge 4 ] &&
     printf '%s' "$argv" | grep -q public; then
    pass "argv is boundary-preserving and masks flag/equal/URI/assignment secret forms"
else
    fail "argv redaction removed public context or missed a supported secret form"
fi

echo "── cmd_up temp state and provisioning descendants must be signal-clean ────────"
U="$RT/up"; mkdir -p "$U/tmp"
cp "$SIMROOT/simcluster" "$U/simcluster"
cp -R "$SIMROOT/lib" "$U/lib"
cat >"$U/fake-docker" <<'EOF'
#!/bin/sh
case "$1" in
  network)
    [ "$2" = inspect ] && exit 1
    exit 0
    ;;
  inspect) exit 1 ;;
  run|start) exit 0 ;;
  exec)
    shift
    ctr=$1
    shift
    case " $* " in
      *" systemctl is-system-running "*) printf 'starting\n'; exit 0 ;;
      *" /opt/sim/provision-node.sh "*)
        if [ "${FAKE_PROVISION_SLOW:-0}" = 1 ]; then
            /bin/sleep 3
            printf 'orphan-provisioner\n' >"$UP_SURVIVOR_MARKER"
        fi
        [ "${FAKE_PROVISION_FAIL:-0}" = 1 ] && exit 7
        exit 0
        ;;
      *) exit 0 ;;
    esac
    ;;
esac
exit 0
EOF
chmod +x "$U/fake-docker" "$U/simcluster"

if TMPDIR="$U/tmp" DOCKER="$U/fake-docker" SIM_ALLOW_FAKE_DNS=1 "$U/simcluster" up \
    --brokers 1 --agents 0 --ctl 0 >/dev/null 2>&1 &&
   [ -z "$(find "$U/tmp" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    pass "cmd_up success removes its unpredictable temp directory under a custom TMPDIR"
else
    fail "cmd_up success failed or leaked temp state under a custom TMPDIR"
fi

if TMPDIR="$U/tmp" DOCKER="$U/fake-docker" SIM_ALLOW_FAKE_DNS=1 FAKE_PROVISION_FAIL=1 "$U/simcluster" up \
    --brokers 1 --agents 0 --ctl 0 >/dev/null 2>&1; then
    fail "cmd_up accepted a failed background provisioner"
elif [ -z "$(find "$U/tmp" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    pass "cmd_up failure is non-zero and removes its temp directory"
else
    fail "cmd_up failure leaked temp state"
fi

for uspec in INT:130 TERM:143; do
    usig=${uspec%%:*}; uwant=${uspec#*:}
    umarker="$U/orphan-$usig.marker"
    TMPDIR="$U/tmp" DOCKER="$U/fake-docker" SIM_ALLOW_FAKE_DNS=1 FAKE_PROVISION_SLOW=1 \
        UP_SURVIVOR_MARKER="$umarker" timeout --preserve-status -k 5 -s "$usig" 1 \
        "$U/simcluster" up --brokers 1 --agents 0 --ctl 0 >/dev/null 2>&1
    urc=$?
    /bin/sleep 2
    if [ "$urc" = "$uwant" ] && [ ! -e "$umarker" ] &&
       [ -z "$(find "$U/tmp" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        pass "cmd_up $usig returns $uwant, reaps provisioners, and removes temp state"
    else
        fail "cmd_up $usig cleanup failed (rc=$urc orphan=$([ -e "$umarker" ] && echo yes || echo no))"
    fi
done

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then
    echo "simcluster-accel final external review: ALL PASS"
    exit 0
fi
echo "simcluster-accel final external review: $FAILS FAILED"
exit 1
