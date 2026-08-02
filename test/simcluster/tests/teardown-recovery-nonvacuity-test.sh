#!/bin/sh
# Independent structural non-vacuity checks for the recovery and canary drills.
set -u
HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
R98="$HERE/drills/98-stuck-redial-recovery.sh"
R31="$HERE/drills/31-node-upgrade-fleet.sh"
fail=0

bad() { echo "FAIL: $*" >&2; fail=$((fail + 1)); }

hb0=$(grep -n '^HB0=' "$R98" | head -1 | cut -d: -f1)
impact=$(grep -n '^[[:space:]]*poll_until .*agt1 connection leaves' "$R98" | head -1 | cut -d: -f1)
recovery=$(grep -n '^[[:space:]]*poll_until .*agt1 heartbeat advances' "$R98" | head -1 | cut -d: -f1)
[ -n "$hb0" ] && [ -n "$impact" ] && [ -n "$recovery" ] || bad "cannot locate drill 98 heartbeat/impact/recovery anchors"
if [ -n "$hb0" ] && [ -n "$impact" ] && [ "$hb0" -lt "$impact" ]; then
    post_impact=$(sed -n "$((impact + 1)),$((recovery - 1))p" "$R98")
    if ! printf '%s\n' "$post_impact" | grep -q '^HB0='; then
        bad "drill 98 never refreshes HB0 after the fault impact; a heartbeat written before injection can satisfy RECOVERY"
    fi
    if ! printf '%s\n' "$post_impact" | grep -Fq '[ -n "$HB0" ] || die'; then
        bad "drill 98 accepts an empty post-impact heartbeat watermark; a transient ctl/jq failure makes RECOVERY vacuous"
    fi
fi

n_budget_polls=$(grep -c 'poll_until "\$RECOVERY_BUDGET"' "$R98")
[ "$n_budget_polls" -le 1 ] || bad "drill 98 starts $n_budget_polls independent full recovery budgets; impact + recovery can consume twice the published window"

if grep -q "grep -qE \"agt2 (skipped|failed)\"" "$R31"; then
    bad "drill 31's untouched oracle ignores a successful agt2 staged line; it can pass after dispatching the held-back node"
fi

[ "$fail" -eq 0 ] || exit 1
echo "teardown-recovery-nonvacuity-test: PASS"
