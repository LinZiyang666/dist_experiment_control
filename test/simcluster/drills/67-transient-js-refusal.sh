#!/bin/sh
# 67-transient-js-refusal.sh — gotcha #67, now a GREEN REGRESSION (was PRODUCT-RED before G67).
#
# THE DEFECT WAS NEVER "it refused". During a quorum loss an R=2 JetStream asset genuinely cannot be
# written, so refusing is correct. The defect was what the operator was TOLD: the refusal carried no
# hint that the condition was transient and retryable — on a path where tether already owns exactly
# that vocabulary ("transfer … rejected …; retry shortly", internal/broker/transfer.go) — and in its
# worst face it asserted a PERMANENT capability absence the broker had never reported, with advice
# (raise max_payload) unrelated to the failure.
#
#   face A — the broker gave the best-effort sizing probe and the load-bearing CreateObjectStore ONE
#     shared 5s deadline and a SINGLE attempt, so a stalled probe silently ate the budget and the
#     create was handed an already-expired context (measured: -4.25ms). Operator saw
#     `code=bucket_create_failed create_bucket: context deadline exceeded` — the same signature that
#     broke drills/42-rejoin-returning repeat run 3, and one that also fired with NO injection at all
#     on the first tier-B push after a grow.
#   face B — `caps, _ := probeCaps(...)` DISCARDED the probe error, so a zero-value CapsResp made "the
#     probe failed" indistinguishable from "the broker has no JetStream", and the CLI manufactured the
#     permanent claim from that zero value. Observed once by manual probe; this drill's clean-stop
#     injection does not reproduce it, and it has no drill-level oracle.
#
# G67 (docs/reviews/g67-plan.md) split the two deadlines, added a bounded CLASSIFIED retry of the
# create leg, and split the terminal code in two: `jetstream_not_ready` (TRANSIENT, exit 75, carries a
# retry instruction) vs `bucket_create_failed` (PERMANENT, wording byte-unchanged, deliberately
# contains no retry vocabulary). What this drill now asserts is that the refusal is HONEST.
#
# WHY A DEDICATED DRILL: the demonstration needs N=2, where taking one peer down actually loses JS
# quorum. 96 is the transfer drill but N=3 (one peer down keeps quorum, so nothing reproduces); 42 is
# N=2 but GREEN and must stay a clean R16 regression gate.
#
# THE INJECTION IS ENVIRONMENTAL (Mandate ③): stopping/starting the peer's nats-server is the sim
# supplying a machine-level outage — exactly what a rolling broker restart does to a live N=2 cluster.
# Nothing here compensates for tether or completes an operation on its behalf; the drill only observes
# what tether tells the operator, and whether it was true.
#
# NON-VACUITY (both halves are required, plan-§9-style):
#   before  — a tier-B push must SUCCEED on the healthy N=2, else "it failed while the peer was down"
#             proves nothing;
#   after   — the SAME push must SUCCEED again once the peer is back, with NO operator action, which is
#             what makes the refusal provably TRANSIENT rather than a real capability absence. Without
#             this half a genuinely broken cluster would look identical to the defect.
# Env: SIM, HERE, INSTANCE.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/logs.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

# 12 MB > transferTierAMaxBytes (8 MiB, internal/broker/transfer.go:52) => tier B is forced.
_g67_push() { "$SIM" ctl -- push /tmp/g67.bin "agt1:/tmp/$1" 2>&1; }
_g67_meta_formed() {
    "$SIM" exec brk1 -- sh -c 'curl -s --max-time 5 "localhost:8223/jsz?meta=1"' 2>/dev/null \
        | jq -e '.meta_cluster.cluster_size==2' >/dev/null 2>&1
}
# A refusal is attributable to #67 ONLY if it names a REGISTERED tier-B / JetStream face. This gate is
# deliberately narrow and was learned the hard way: an earlier oracle red on any retry-hint-less refusal
# and promptly fired on `cannot reach broker … i/o timeout`, a CONNECTION-level failure with nothing to
# do with tier-B. That RED was discarded, not banked.
# Assertions must go through FUNCTIONS, not `sh -c "... \$_G67_OUT ..."`: the child shell does not
# inherit the variable, so such a check silently tests the EMPTY STRING — which is how the first
# post-fix run produced two false FAILs and one vacuous PASS. (Caught on the deploy tier; kept as a
# comment because the mistake is easy to reintroduce.)
_g67_out_has()   { printf '%s' "$_G67_OUT" | grep -qiE "$1"; }
_g67_out_lacks() { ! printf '%s' "$_G67_OUT" | grep -qiE "$1"; }
_g67_tierb_face() {
    printf '%s' "$1" | grep -qiE 'jetstream_not_ready|tier_b_store_too_small|broker_restarting|jetstream_unavailable|broker has no JetStream|bump nats max_payload|bucket_create_failed|create_bucket|tier B prepare'
}

drill_begin "#67 transient JS stall reported as PERMANENT capability absence (tier-B push, zero retry)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 2 brokers + 1 agent + 1 ctl"   "$SIM" up --brokers 2 --agents 1 --ctl 1
assert_ok "init brk1 (N=1)"                  "$SIM" init brk1
assert_ok "grow brk2 (N=2 clustered JS)"     "$SIM" grow brk2
assert_ok "session + ctl login"              "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                  "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "JS meta FORMED at N=2 (cluster_size==2)" poll_until 60 3 "the 2-node JS meta forms" -- _g67_meta_formed
assert_ok "a 12 MB payload exists on ctl1 (> the 8 MiB tier-A ceiling => tier B is forced)" \
    "$SIM" exec ctl1 -- sh -c 'head -c 12000000 /dev/urandom > /tmp/g67.bin; test -s /tmp/g67.bin'

# ── NON-VACUITY HALF 1: tier-B genuinely works here, BEFORE any injection ──────────────────────────
# A failure of THIS control is itself #67 (face A) and is MEASURED, not absorbed. The broker-side bucket
# provisioning is a single 5s attempt with no retry (internal/broker/transfer.go:560), so a multi-second
# JS stall surfaces as `bucket_create_failed: create_bucket: context deadline exceeded`. A probe measured
# that create at ~57ms in a healthy window, so the budget has ~100x margin — a failure here is a genuine
# transient stall being made fatal, not a chronically tight timeout.
#
# MEASURED, NOT ABSORBED (Mandate ②). This DOES retry the control push once — and the retry is the whole
# point of the measurement, never a workaround: a retry that SUCCEEDS is proof the first refusal was
# transient, and it is recorded as a product_red naming #67 with "no injection was needed". The sim is
# not doing tether's job here; it is documenting the job tether refused to do. Without the retry the run
# would just die as a generic assert_fail and the finding would be lost.
_G67_PRE=$("$SIM" ctl -- push /tmp/g67.bin agt1:/tmp/g67-before.bin 2>&1); _G67_PRE_RC=$?
_G67_PRE2_RC=0
if [ "$_G67_PRE_RC" != 0 ]; then
    log "#67 CONTROL(before) FAILED rc=$_G67_PRE_RC on a HEALTHY N=2 with NO injection: $(printf '%s' "$_G67_PRE" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-300)"
    _G67_PRE2=$("$SIM" ctl -- push /tmp/g67.bin agt1:/tmp/g67-before.bin 2>&1); _G67_PRE2_RC=$?
    if printf '%s' "$_G67_PRE" | grep -q 'code=jetstream_not_ready' && [ "$_G67_PRE2_RC" = 0 ]; then
        # POST-G67 this is the CONTRACT, not a defect: the refusal named itself transient and told the
        # operator to retry shortly, and retrying worked. Recorded, not red — but see the residual
        # registered in the ledger: "grow, then push" can still need one retry under load.
        # Internal review (adopted): logging this made the branch UNCOUNTABLE — the DRILL-VERDICT line
        # became byte-identical to a first-try success, so the very detector that found #67 (drill 42
        # repeat run 3 dying on this baseline) was unreachable while the CAUSE — ledger sub-face 4,
        # `cluster add` reporting success before the JS meta can place assets — is still OPEN.
        # not_covered keeps it countable without pretending the contract was violated.
        not_covered "#67 sub-face 4 (grow reports success before the JS meta can place assets)" \
            "the FIRST tier-B push after a plain grow was refused as TRANSIENT and the documented retry then succeeded, so the G67 contract HELD — G69 has since shipped the grow-side fix (the join terminal gate now withholds SERVING until the meta has placed events at the target replica factor), so this branch firing means the fix did NOT close this window on this host — countable so that cannot become invisible" gap
    elif _g67_tierb_face "$_G67_PRE" && [ "$_G67_PRE2_RC" = 0 ]; then
        product_red "#67 STILL reproduces with NO INJECTION AT ALL (the G67 fix did not close this case) — the FIRST tier-B push after a plain \`cluster add\` grow hard-failed on a HEALTHY N=2: [$(printf '%s' "$_G67_PRE" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-200)]. A plain immediate RETRY then succeeded, which is exactly the retry tether declines to do itself (single 5s attempt, internal/broker/transfer.go:560) and gives the operator no hint to try. This is the same signature that broke drills/42-rejoin-returning repeat run 3"
    fi
fi
assert_ok "CONTROL(before) ESTABLISHED: tier-B provably works on this healthy N=2 — first attempt, or a plain retry after a #67 refusal (that retry is recorded as a product_red above, never silently absorbed). Without this, a refusal below proves nothing" \
    sh -c "[ '$_G67_PRE_RC' = 0 ] || [ '$_G67_PRE2_RC' = 0 ]"

# ── G69 POSITIVE oracle for #67 sub-face 4 ────────────────────────────────────────────────────────
# The sub-face-4 gap below fires ONLY when the first post-grow push fails and the retry succeeds — a
# precondition an unloaded host cannot produce (measured: unloaded = 1.66s, zero retries). So its
# ABSENCE proves nothing, and the internal review (G-3) was right that citing it as acceptance evidence
# was citing a coincidence. This assertion is the positive half: G69 makes `cluster add` withhold
# terminal SERVING until the JS meta has placed the events stream at the target replica factor, and it
# records `WITHOUT proving JetStream placement` in the op timeline when it has to degrade instead. On a
# healthy grow that entry must be ABSENT — which is checkable on every run, loaded or not.
_g67_join_op_proved_placement() {
    "$SIM" exec brk1 -- sh -c 'runuser -u tether -- tether cluster ops ls --json 2>/dev/null' 2>/dev/null \
        | grep -q . || return 1
    ! "$SIM" exec brk1 -- sh -c 'runuser -u tether -- tether cluster ops ls --json 2>/dev/null' 2>/dev/null \
        | grep -q 'WITHOUT proving JetStream placement'
}
if "$SIM" exec brk1 -- sh -c 'runuser -u tether -- tether cluster ops ls --json >/dev/null 2>&1'; then
    assert_ok "G69 (#67 sub-face 4): the grow reached terminal SERVING having PROVEN JetStream placement — no 'WITHOUT proving' degrade entry in any op timeline" \
        _g67_join_op_proved_placement
else
    not_covered "G69 positive oracle (#67 sub-face 4)" \
        "\`cluster ops ls --json\` is not readable on brk1 in this fixture, so the op timeline could not be inspected; the degrade-entry assertion could not run" gap
fi

# ── INJECT: take the PEER's nats-server DOWN CLEANLY (quorum lost, reversible, no data touched) ────
# WHY A CLEAN STOP AND NOT SIGSTOP. Both lose JS quorum, but SIGSTOP leaves a HUNG TCP peer that poisons
# brk1's own route I/O — measured: the ctl could not even reach brk1 (`cannot reach broker … i/o timeout`,
# rc=69), a CONNECTION-level failure that is NOT #67 and must never be laundered into it. A clean stop
# gives the isolation this drill needs: brk1 stays fully responsive on the control plane, only the JS
# meta-group loses quorum. It is also the more realistic event — this is exactly what a rolling broker
# restart / upgrade does to a live N=2 cluster.
assert_ok "INJECT: stop brk2's nats-server cleanly (JS quorum is lost; brk1 stays reachable; nothing is destroyed)" \
    "$SIM" exec brk2 -- sh -c 'systemctl stop nats-server'

_G67_OUT=$(_g67_push g67-frozen.bin); _G67_RC=$?
log "#67 push-while-stalled rc=$_G67_RC output: $(printf '%s' "$_G67_OUT" | tr '\n' ' ' | cut -c1-400)"

# ── RESTORE before judging, so a judgement failure can never leave the cluster degraded ───────────
assert_ok "RESTORE: start brk2's nats-server again" "$SIM" exec brk2 -- sh -c 'systemctl start nats-server'
assert_ok "the 2-node JS meta re-forms once the peer is back" poll_until 120 3 "meta back at cluster_size==2" -- _g67_meta_formed

# ── NON-VACUITY HALF 2 (captured BEFORE the judgement, because the judgement depends on it) ────────
# The refusal above is only attributable to #67 if the IDENTICAL push works again with no operator
# action. If it does not, the cluster was genuinely broken and nothing here is judgeable.
# EVIDENCE CAPTURE (deploy-tier lesson): the first concurrent run failed HERE and logged nothing, so
# there was no way to tell a still-degraded cluster from a slow one. Always record the last attempt.
_G67_AFTER=0
_g67_after_try=0
while [ "$_g67_after_try" -lt 12 ]; do
    _g67_after_try=$((_g67_after_try+1))
    _G67_AFTER_OUT=$("$SIM" ctl -- push /tmp/g67.bin agt1:/tmp/g67-after.bin 2>&1) && { _G67_AFTER=1; break; }
    sleep 5
done
log "#67 CONTROL(after) recovered=$_G67_AFTER after $_g67_after_try attempt(s); last output: $(printf '%s' "${_G67_AFTER_OUT:-}" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-240)"
assert_ok "CONTROL(after): the SAME tier-B push SUCCEEDS once the peer is back, with NO operator action — this is what makes the refusal above provably transient (a real capability absence would persist)" \
    sh -c "[ '$_G67_AFTER' = 1 ]"

# ── JUDGE (post-G67: this is now a GREEN REGRESSION, not a defect pin) ────────────────────────────
# WHAT CHANGED. Before G67 this arm recorded a product_red: a transient loss of JS quorum produced a
# refusal whose text gave the operator no hint that it was transient, and in its worst face asserted a
# PERMANENT capability absence the broker never reported. The fix (docs/reviews/g67-plan.md) splits
# the sizing and create deadlines, retries a CLASSIFIED transient failure a bounded number of times,
# and reports it under its own code. Refusing while quorum is genuinely lost is still correct — what
# must now hold is that the refusal is HONEST.
#
# NON-VACUITY. A text assertion alone would pass if the retry loop were deleted and only the wording
# kept — the single most likely dishonest version of this fix. The load-bearing tooth is therefore the
# broker's own journal line proving the retry ACTUALLY RAN.
_g67_retry_logged() {
    sim_broker_slog brk1 400 2>/dev/null \
        | grep -qE 'tier-B bucket provisioning retried'
}

if [ "$_G67_RC" = 0 ]; then
    not_covered "#67 regression (transient JS-quorum loss must be reported honestly)" \
        "the push SUCCEEDED while the peer was down, so no refusal was produced and the wording could not be judged. NOT evidence the fix works — re-run; if this persists the injection is no longer removing JS quorum" gap
elif [ "$_G67_AFTER" != 1 ]; then
    not_covered "#67 regression" \
        "the push failed while the peer was down and the identical push did NOT recover afterwards either, so the cluster did not demonstrably return to health and nothing here is attributable" gap
elif ! _g67_tierb_face "$_G67_OUT"; then
    not_covered "#67 regression" \
        "the refusal does not name any registered tier-B/JetStream face (raw text logged above) — e.g. a connection-level 'cannot reach broker' is a different failure and judging it here would over-claim" gap
else
    # (a) the code must be the TRANSIENT one, not the permanent terminal it used to be
    assert_ok "#67 the refusal carries the TRANSIENT code jetstream_not_ready (pre-G67 this was the permanent bucket_create_failed)" \
        _g67_out_has 'code=jetstream_not_ready'
    # (b) the operator must be told it is worth retrying — this is the whole user-visible point
    assert_ok "#67 the refusal TELLS THE OPERATOR it is transient and worth retrying" \
        _g67_out_has 'retry|transient|temporar'
    # (c) it must NOT resurrect the invented permanent claim or the unrelated advice
    assert_ok "#67 the refusal does NOT assert a permanent capability absence, and does NOT offer the unrelated max_payload advice" \
        _g67_out_lacks 'broker has no JetStream|bump nats max_payload'
    # (d) NON-VACUITY: the bounded retry actually ran. Deleting the retry loop but keeping the wording
    #     reddens THIS assertion and nothing else.
    if _g67_retry_logged; then
        _as_pass "#67 NON-VACUITY: the broker's own journal proves the bounded retry ran (not just re-worded)"
    else
        not_covered "#67 non-vacuity tooth" \
            "the refusal was worded correctly but no 'tier-B bucket provisioning retried' line was readable in brk1's broker slog, so this run cannot distinguish a real bounded retry from wording alone. NOTE the tooth deliberately does NOT accept the 'gave up' line: that one is emitted even for a PERMANENT single-attempt refusal and would pass with the retry loop deleted" gap
    fi
fi

# ── #67 FACE B: DECLARED COVERAGE HOLE (not a failure — a hole this drill owes) ────────────────────
# face A is now a GREEN regression above. face B — the CLI inventing a permanent capability claim out
# of a zero-value CapsResp after a swallowed probe error — is NOT exercised by this drill. It was
# observed exactly once, by a manual probe using SIGSTOP to freeze the peer; that injection was
# RETIRED because a hung TCP peer poisons brk1's own route I/O and produces connection-level failures
# that are a different defect (attributing them to #67 would over-claim). The clean-stop injection
# this drill uses does not reproduce face B.
#
# Recorded as a first-class gap rather than left implicit: without it this drill reports a clean GREEN
# while #67 as a whole is NOT closed, which is exactly the false-completeness shape the ledger
# crosscheck exists to catch.
not_covered "#67 face B (swallowed caps-probe error -> invented permanent capability claim)" \
    "the CLI-side fix shipped and is pinned hermetically (TestChooseTierNeverInventsACapabilityClaim, TestTierAInlineCeilingNeverWidenedByAMissingMeasurement, TestCapsProbeWarningsAreHonest), but there is NO deploy-tier oracle for it: the only injection known to reproduce it is SIGSTOP on the peer's nats-server, which was retired for producing connection-level failures that are NOT #67. A dedicated injection that fails the caps RPC WITHOUT poisoning the broker's own I/O is owed" gap

assert_ok "the recovery-run file really landed on agt1 (round-trip, not a queued no-op)" \
    "$SIM" exec agt1 -- test -s /tmp/g67-after.bin

drill_end
