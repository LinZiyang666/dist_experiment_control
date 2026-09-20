#!/bin/sh
# Independent structural non-vacuity checks for the recovery and canary drills.
set -u
HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
R98="$HERE/drills/98-stuck-redial-recovery.sh"
R31="$HERE/drills/31-node-upgrade-fleet.sh"
fail=0

bad() { echo "FAIL: $*" >&2; fail=$((fail + 1)); }

# Drill 98's recovery claim, structurally (external review F5.1 → simcluster-speed X14). The invariant
# is "a heartbeat that crossed the live link can never satisfy RECOVERY". It was first encoded as
# "refresh the watermark AFTER the impact poll"; with the client-side liveness probe (H) the agent
# re-registers BEFORE the server-side impact is observable, so a post-impact watermark would be read
# after recovery and the claim would be always-true. The invariant now has a different shape, and this
# gate pins THAT shape:
#   (1) the watermark HB_INJ is captured in the parent shell AFTER the injection assert (the DROP is
#       armed and self-proven before it is read), never before;
#   (2) an empty watermark dies (fail-closed) rather than making the comparison vacuous;
#   (3) the RECOVERY poll's predicate is the CONJUNCTION helper (heartbeat past HB_INJ AND /connz on a
#       survivor), not either half alone;
#   (4) the conjunction helper really ANDs the two halves.
inject=$(grep -n '^[[:space:]]*assert_ok "inject: silent-DROP' "$R98" | head -1 | cut -d: -f1)
wm=$(grep -n '^HB_INJ=' "$R98" | head -1 | cut -d: -f1)
recovery=$(grep -n '^[[:space:]]*poll_until .*agt1 re-registers on a survivor' "$R98" | head -1 | cut -d: -f1)
[ -n "$inject" ] && [ -n "$wm" ] && [ -n "$recovery" ] || bad "cannot locate drill 98 injection/watermark/recovery anchors"
if [ -n "$inject" ] && [ -n "$wm" ] && [ -n "$recovery" ]; then
    [ "$inject" -lt "$wm" ] || bad "drill 98 captures the heartbeat watermark BEFORE the injection; a heartbeat written through the healthy link can satisfy RECOVERY"
    [ "$wm" -lt "$recovery" ] || bad "drill 98 reads the watermark after the RECOVERY poll it is compared in"
    between=$(sed -n "$((wm + 1)),$((recovery - 1))p" "$R98")
    printf '%s\n' "$between" | grep -Fq '[ -n "$HB_INJ" ] || die' \
        || bad "drill 98 accepts an empty at-injection heartbeat watermark; a transient ctl/jq failure makes RECOVERY vacuous"
    sed -n "${recovery}p" "$R98" | grep -q -- '-- _recovered_on_survivor' \
        || bad "drill 98's RECOVERY poll does not use the conjunction helper (_recovered_on_survivor); one half alone has a false positive"
    grep -q '^_recovered_on_survivor() { _hb_advanced && _registered_on_another_voter; }' "$R98" \
        || bad "drill 98's _recovered_on_survivor is not the AND of the heartbeat and /connz halves"
    grep -q '^_hb_advanced() {.*"$HB_INJ"' "$R98" \
        || bad "drill 98's _hb_advanced does not compare against the at-injection watermark HB_INJ"
    # (5) the /connz half must exclude the CUT broker — under a silent DROP the cut broker keeps agt1
    #     `present` until its own ping kill, so without the exclusion the conjunction is true at once
    #     (internal review round 1 R4-F5).
    sed -n '/^_registered_on_another_voter() {/,/^}/p' "$R98" | grep -Fq '[ "$_b" = "$CUT_BROKER" ] && continue' \
        || bad "drill 98's _registered_on_another_voter does not skip CUT_BROKER; the cut broker's own /connz satisfies the /connz half"
    # (6) the live connection must still be on the cut broker when the watermark is read (R1-F3), and
    #     that self-proof must sit between the injection and the watermark.
    holds=$(grep -n '^[[:space:]]*assert_ok "inject self-proof C:.*STILL holds agt1' "$R98" | head -1 | cut -d: -f1)
    [ -n "$holds" ] && [ "$holds" -gt "$inject" ] && [ "$holds" -lt "$wm" ] \
        || bad "drill 98 does not prove the cut broker still holds agt1's connection between the injection and the watermark (R1-F3 vacuity window)"
    grep -q '^_cut_still_holds_agt1() { \[ "$(_connz_state "$CUT_BROKER")" = present \]; }' "$R98" \
        || bad "drill 98's _cut_still_holds_agt1 is not the fail-closed /connz==present check"
fi

# ONE shared budget (F5.3): both polls draw on _budget_left from the single DEADLINE; a second
# `poll_until "$RECOVERY_BUDGET"` (or any literal budget) on either would start a fresh window. The old
# check counted `poll_until "$RECOVERY_BUDGET"` occurrences and became vacuous (0 matches) the day both
# polls moved to _budget_left (internal review round 1 R3-4) — pin the positive shape instead.
n_budget_polls=$(grep -c 'poll_until "\$(_budget_left)"' "$R98")
[ "$n_budget_polls" -eq 2 ] || bad "drill 98 must draw BOTH its RECOVERY and SERVER-SIDE polls from the shared _budget_left (found $n_budget_polls)"
grep -q 'poll_until "\$RECOVERY_BUDGET"' "$R98" && bad "drill 98 starts an independent full recovery budget; impact + recovery can consume twice the published window"
grep -q '^[[:space:]]*assert_ok "SERVER-SIDE the cut broker' "$R98" \
    || bad "drill 98 lost the SERVER-SIDE evidence assertion (the server half of the ping contract is then unobserved)"
# The FUNCTION BODY alone must reference DEADLINE. The first version also printed the range from the
# `DEADLINE=` assignment onward — which, because _budget_left is defined ABOVE that line, ran to EOF and
# always contained the assignment itself, so the grep was unconditionally true and plan §8.6's M9 "red"
# never happened (round-2 review R3-F1 / R4-2-F5). The assignment's existence is pinned separately below.
sed -n '/^_budget_left() {/,/^}/p' "$R98" | grep -q 'DEADLINE' \
    || bad "drill 98's _budget_left does not derive from the single DEADLINE"
grep -q '^DEADLINE=' "$R98" || bad "drill 98 no longer sets the single absolute DEADLINE"

if grep -q "grep -qE \"agt2 (skipped|failed)\"" "$R31"; then
    bad "drill 31's untouched oracle ignores a successful agt2 staged line; it can pass after dispatching the held-back node"
fi

[ "$fail" -eq 0 ] || exit 1
echo "teardown-recovery-nonvacuity-test: PASS"
