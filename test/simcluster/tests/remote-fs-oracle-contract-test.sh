#!/bin/sh
# Pins the three-layer attribution contract in drill 62. This is deliberately
# hermetic: it checks the exact assertion/signature and the request-start
# discriminator without Docker or a live cluster.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DRILL="$ROOT/test/simcluster/drills/62-remote-fs-safe.sh"
VERDICTS="$ROOT/test/simcluster/expected-verdicts.tsv"
GOTCHAS="$ROOT/docs/deploy-tier-gotchas.md"
FAIL=0

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAIL=$((FAIL+1)); }

row=$(awk -F '\t' '$1 == "62-remote-fs-safe" { print; n++ } END { if (n != 1) exit 1 }' "$VERDICTS") || row=
section=$(sed -n '/^### #81 —/,/^### #82 —/p' "$GOTCHAS")
bands=$(printf '%s\n' "$row" | awk -F '\t' '{print $4}')
case "$row" in
    *'ASSERT-FAIL@#81'*|*'sig:1s-no-terminal-state'*) row_unbanded=false ;;
    *) row_unbanded=true ;;
esac
case "$section" in *'✅ FIXED'*) fixed=true ;; *) fixed=false ;; esac
case "$section" in *'#81 临时 band 已删除'*) removal=true ;; *) removal=false ;; esac
if [ -n "$row" ] && [ "$bands" = "-" ] &&
   [ "$row_unbanded" = true ] && [ "$fixed" = true ] && [ "$removal" = true ]; then
    pass "closed #81 has no band, so every new transport/delivery/product red is unmasked"
else
    fail "closed #81 is still banded or its FIXED/band-removal contract is missing"
fi

start_re='msg="agent: exec".*pid='
if printf '%s\n' 'time=x level=INFO msg="agent: exec" pid=P1 argv=[/mnt/hung/probe]' | grep -qE "$start_re" &&
   ! printf '%s\n' 'time=x level=WARN msg="agent: exec spawn bounded-start failed" pid=P0 argv0=/mnt/hung/probe' | grep -qE "$start_re"; then
    pass "delivery discriminator accepts request-start and rejects late watchdog warning"
else
    fail "delivery discriminator aliases request-start with watchdog warning"
fi

if grep -qF '[ -f "$_1S_DIR/first.terminal" ] && [ -f "$_1S_DIR/first.delivered" ]' "$DRILL" &&
   grep -qF '[ -f "$_1S_DIR/second.terminal" ] && [ -f "$_1S_DIR/second.delivered" ]' "$DRILL"; then
    pass "product oracle is gated by transport AND delivery for both commands"
else
    fail "product oracle can run without both upstream layers"
fi

trap_line=$(grep -n '^drill_install_traps _cleanup_all$' "$DRILL" | cut -d: -f1)
cleanup_line=$(grep -n '^_cleanup_1s()' "$DRILL" | cut -d: -f1)
case "$trap_line:$cleanup_line" in
    *[!0-9:]*|:|*:|:*) fail "cleanup/trap definitions could not be located" ;;
    *)
        if [ "$cleanup_line" -lt "$trap_line" ]; then
            pass "temporary-evidence cleanup exists before signal traps become active"
        else
            fail "an early signal can call _cleanup_1s before it is defined"
        fi
        ;;
esac

if [ "$FAIL" -eq 0 ]; then
    echo "remote-fs oracle contract: ALL PASS"
    exit 0
fi
echo "remote-fs oracle contract: $FAIL FAILED"
exit 1
