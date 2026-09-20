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

# The injected push is TIMED as well as read (round-2 review R1-F3): its judgement below is on the
# duration first and the wording second, like the post-recovery face — a post-fix-worded refusal that sat
# the whole 7-minute default budget (`refused 1 attempt(s) over 7m0s`) reads like a bounded refusal and
# would otherwise pass every wording judge.
_g67_inj_t0=$(date +%s)
_G67_OUT=$(_g67_push g67-frozen.bin); _G67_RC=$?
_G67_INJ_S=$(( $(date +%s) - _g67_inj_t0 ))
log "#67 push-while-stalled rc=$_G67_RC after ${_G67_INJ_S}s output: $(printf '%s' "$_G67_OUT" | tr '\n' ' ' | cut -c1-400)"

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
_G67_AFTER_FIRST_OUT=""; _G67_AFTER_FIRST_S=0
# #84 post-recovery diagnostics (image #8 receipt, 2026-09-19): the FIRST CONTROL(after) push still sat the whole
# --timeout 120s on the Put leg while the ctl's STREAM.INFO watchdog saw a stream leader the whole time, so
# neither of its two halves fired; the hermetic 2-node replay of the same injection recovers in 5 s and cannot
# show what the deploy tier does here. Sample the bucket stream's cluster block from nats-server's OWN /jsz
# on both brokers every 5 s while the attempts run — leader, per-replica current/offline/active/lag, message
# counts — into a host-side file that is replayed below. OBSERVATION ONLY: no assert site, cannot change a
# verdict; a curl/jq hiccup is recorded as `error`, never laundered into a reading.
_g67_jsz() {
    _jz=$("$SIM" exec "$1" -- curl -sf --max-time 2 'http://127.0.0.1:8223/jsz?acc=%24G&streams=true' 2>/dev/null) || { printf 'error'; return; }
    printf '%s' "$_jz" | jq -c --arg s "OBJ_xfer-$SID" \
        '[.account_details[]?.stream_detail[]? | select(.name==$s)
          | {leader: .cluster.leader, replicas: [(.cluster.replicas // [])[] | {name, current, offline, active, lag}],
             msgs: .state.messages, first: .state.first_seq, last: .state.last_seq}] | .[0] // "no-stream"' 2>/dev/null \
        || printf 'error'
}
# The ctl's own client connection as nats-server sees it (name `tether-cli:<sid>`): which broker holds
# it, and its in/out message and pending-byte counters — a Put whose chunks never reach the stream reads
# very differently when the counters show them leaving the client than when they show nothing sent.
_g67_ctlconn() {
    _cz=$("$SIM" exec "$1" -- curl -sf --max-time 2 'http://127.0.0.1:8223/connz' 2>/dev/null) || { printf 'error'; return; }
    printf '%s' "$_cz" | jq -c --arg n "tether-cli:$SID" \
        '[.connections[]? | select(.name==$n) | {cid, in_msgs, out_msgs, in_bytes, out_bytes, pending_bytes, subscriptions}] | if length==0 then "none" else . end' 2>/dev/null \
        || printf 'error'
}
_G67_JSZ_LOG=/tmp/g67-jsz.$$.log
_G67_AFTER_T0=$(date +%s)
: > "$_G67_JSZ_LOG"
(
    # Detached (see drill 30's scene watcher for why). Bounded by the LOOP below, not by a sample count: the
    # first version stopped after 60 samples (5 min) while 12 attempts × (120 s + 5 s) can run 25 min, so a
    # third-attempt stall at t+6 min had no /jsz row over it (round-2 review R3-F14). The kill after the
    # loop ends it; the drill's EXIT trap reaps it on an early die/setup_fail; a hard ceiling of 400
    # samples (≈33 min, past the longest possible loop) keeps it from outliving a killed drill shell.
    _n=0
    while [ "$_n" -lt 400 ]; do
        _n=$((_n + 1))
        printf '%s brk1=%s brk2=%s ctl@brk1=%s ctl@brk2=%s\n' "t+$(( $(date +%s) - _G67_AFTER_T0 ))s" \
            "$(_g67_jsz brk1)" "$(_g67_jsz brk2)" "$(_g67_ctlconn brk1)" "$(_g67_ctlconn brk2)" >> "$_G67_JSZ_LOG" 2>/dev/null
        sleep 5
    done
) </dev/null >/dev/null 2>&1 &
_G67_JSZ_PID=$!
_g67_reap_sampler() { kill "${_G67_JSZ_PID:-}" 2>/dev/null; rm -f "${_G67_JSZ_LOG:-/nonexistent}" 2>/dev/null; true; }
drill_install_traps _g67_reap_sampler
while [ "$_g67_after_try" -lt 12 ]; do
    _g67_after_try=$((_g67_after_try+1))
    _g67_t0=$(date +%s)
    # plan X27: the CONTROL pushes use the operator's own `--timeout 120s`; only the push-while-stalled above
    # keeps the CLI default (it is the one sample of the default-timeout path, bounded by the unit's worst).
    # Attempt 1 is TIMED whatever its outcome (round-2 review R1-F4): a first attempt that stalls 110 s and
    # then lands is the same unbounded stall with a luckier ending, and the first version recorded its
    # duration only on the failure path — so it was neither judged nor replayed.
    if _G67_AFTER_OUT=$("$SIM" ctl -- push /tmp/g67.bin agt1:/tmp/g67-after.bin --timeout 120s 2>&1); then _g67_ok=1; else _g67_ok=0; fi
    [ "$_g67_after_try" = 1 ] && { _G67_AFTER_FIRST_OUT=$_G67_AFTER_OUT; _G67_AFTER_FIRST_S=$(( $(date +%s) - _g67_t0 )); }
    [ "$_g67_ok" = 1 ] && { _G67_AFTER=1; break; }
    # Every FAILED attempt is recorded with its own elapsed time, not only the last output: on 2026-09-19
    # (image #2, solo) attempt 1 sat for ≈425 s — the whole size-derived Put budget — AFTER the JS meta had
    # re-formed, and attempt 2 then succeeded in 192 ms. The line above the loop only kept the winning
    # output, so what attempt 1 was told (and whether it was a hang or a refusal) was lost with it.
    log "#67 CONTROL(after) attempt $_g67_after_try FAILED after $(( $(date +%s) - _g67_t0 ))s: $(printf '%s' "$_G67_AFTER_OUT" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-600)"
    sleep 5
done
kill "$_G67_JSZ_PID" 2>/dev/null || true
# Replay the /jsz samples only when they carry information: more than one attempt, or a first attempt that
# took longer than the ctl's watchdog bound (30 s) — a one-shot success needs no forensic trail.
if [ "$_g67_after_try" -gt 1 ] || [ "${_G67_AFTER_FIRST_S:-0}" -ge 30 ]; then
    log "#84 /jsz + /connz samples during CONTROL(after) (brk1 | brk2: leader / replicas current,offline,active,lag / msgs first..last; ctl connection counters per broker):"
    while IFS= read -r _g67_l; do log "  jsz| $(printf '%s' "$_g67_l" | cut -c1-900)"; done < "$_G67_JSZ_LOG"
    # nats-server's own account of the stream/raft layer over the same window (JetStream + RAFT lines only;
    # the nats journal is not one of logs.sh's four tether streams, so reading it here is not an oracle read).
    for _g67_b in brk1 brk2; do
        log "#84 $_g67_b nats-server journal, JetStream/RAFT lines since the CONTROL(after) window began (last 40):"
        "$SIM" exec "$_g67_b" -- journalctl -u nats-server --no-pager --since "@$_G67_AFTER_T0" 2>/dev/null \
            | grep -iE 'jetstream|raft|stream|leader|catchup|snapshot|quorum|route' | tail -40 \
            | while IFS= read -r _g67_l; do log "  $_g67_b nats| $(printf '%s' "$_g67_l" | cut -c1-260)"; done
    done
fi
rm -f "$_G67_JSZ_LOG" 2>/dev/null || true
# When recovery took more than one attempt, the broker's own account of the transfer window is the
# only place the FIRST attempt's fate is still visible after the instance is nuked (tier-B slot, bucket,
# finalize, JetStream readiness). Observation only; nothing here can change a verdict.
if [ "$_g67_after_try" -gt 1 ]; then
    log "#67 CONTROL(after) needed $_g67_after_try attempts — brk1 broker slog, transfer/JetStream lines (last 40):"
    sim_broker_slog brk1 600 2>/dev/null | grep -iE 'transfer|tier-B|tier_b|bucket|finalize|jetstream|in.flight' | tail -40 \
        | while IFS= read -r _g67_l; do log "  brk1| $(printf '%s' "$_g67_l" | cut -c1-240)"; done
fi
# The success line keeps 600 chars, not 240: the ctl's own account of a slow success (which watchdog face
# fired, how many attempts) sits past 240 and was cut mid-word in the image #9 receipt (round-2 R5-F5).
log "#67 CONTROL(after) recovered=$_G67_AFTER after $_g67_after_try attempt(s), first attempt ${_G67_AFTER_FIRST_S:-?}s; last output: $(printf '%s' "${_G67_AFTER_OUT:-}" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-600)"
assert_ok "CONTROL(after): the SAME tier-B push SUCCEEDS once the peer is back, with NO operator action — this is what makes the refusal above provably transient (a real capability absence would persist)" \
    sh -c "[ '$_G67_AFTER' = 1 ]"
# #84's post-recovery face (internal review round 1 R1-F1 (4)): on 2026-09-19 the FIRST CONTROL(after) push sat
# ≈425 s on a JS meta that had ALREADY re-formed (poll met at 0 s) and failed with the same bare Put timeout,
# and the second succeeded in 192 ms. If the first post-recovery attempt times out on the Put leg it is the
# same defect seen from the other side of the outage — a stall the operator is told nothing about — not
# "evidence". Judged only when the attempt actually ran and lost the whole --timeout (>=100 s of the 120 s),
# so a fast refusal or a harness hiccup can never land here.
# The judgement is on the DURATION, not on the wording (image #8 receipt): after the #84 fix the same 121 s
# stall came back worded as `code=jetstream_not_ready … refused 1 attempt(s) over 2m0s` — honest words, but
# the bound the watchdog promises (≈30 s) was not delivered, and a text-only predicate had let that pass.
# The first attempt's words are kept in the message so the two faces (bare timeout / transient wording)
# stay distinguishable in the log.
# Both endings of a ≥100 s first attempt are judged (round-2 review R1-F4): a failure that lost the whole
# --timeout, AND a success that took ≥100 s of it — 12 MB at the 2 MiB/s admission floor is 6 s, and the
# ctl's ladder is bounded at ≈3×30 s + backoff, so a first attempt that lands after 100 s made its progress
# in a stall the watchdog did not cut.
if [ "$_g67_after_try" -gt 1 ] && [ "${_G67_AFTER_FIRST_S:-0}" -ge 100 ]; then
    _g67_first_face=$(printf '%s' "$_G67_AFTER_FIRST_OUT" | grep -oiE 'code=[a-z_]+|Put: .{0,60}' | head -1)
    product_red "#84 (post-recovery face) the FIRST tier-B push after the JS meta re-formed sat ${_G67_AFTER_FIRST_S}s — the whole --timeout — on the Put leg (first attempt said: '${_g67_first_face:-?}'), and the next attempt succeeded at once: the stall is not bounded by the ctl's watchdog (≈30 s) nor by the cluster's recovery, whatever the refusal is worded as"
elif [ "$_g67_after_try" = 1 ] && [ "$_G67_AFTER" = 1 ] && [ "${_G67_AFTER_FIRST_S:-0}" -ge 100 ]; then
    product_red "#84 (post-recovery face, slow success) the FIRST tier-B push after the JS meta re-formed SUCCEEDED but took ${_G67_AFTER_FIRST_S}s (said: '$(printf '%s' "$_G67_AFTER_FIRST_OUT" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-160)'): the ctl's watchdog promises a ≈30 s bound on a stalled Put and a 12 MB upload is seconds, so the difference was a stall that went uncut"
fi

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
elif [ "${_G67_INJ_S:-0}" -ge 150 ]; then
    # DURATION FIRST (round-2 review R1-F3): the injected push is judged on how long it sat before any
    # wording is read, exactly like the post-recovery face. The ctl's ladder on a quorum-less JetStream is
    # bounded at ≈3 × 30 s probes + 3 s + 6 s backoff (+ the size-budget floor's own margin) — well under
    # 150 s; a push that sat longer was not bounded by the watchdog, whatever it finally printed (image #8:
    # a 121 s stall came back worded `refused 1 attempt(s) over 2m0s`, honest words, no bound delivered).
    product_red "#84 tier-B Put on a JS that has lost quorum sat ${_G67_INJ_S}s (rc=$_G67_RC, said: '$(printf '%s' "$_G67_OUT" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-160)') — the ctl's watchdog+retry ladder promises a bound of ≈100 s and did not deliver it, whatever the refusal is worded as"
elif printf '%s' "$_G67_OUT" | grep -qiE 'Put: .*(nats: timeout|context deadline exceeded|no responders)'; then
    # plan X27 / internal review round 1 R1-F1: the Put-LEG stall is #67's operator-facing defect moved one
    # leg down, NOT an unregistered face. Since 0b204b5 (RESOLVE-BEFORE-CREATE) prepare resolves the bucket
    # locally and succeeds on a JS that has lost quorum, so the refusal face A (a)–(d) judges below can no
    # longer be reached from this injection; what the operator is TOLD instead is a bare `Put: nats: timeout`
    # after the whole size budget (P-b: ≈7 min for 12 MB; before P-b: 37 min) or an instant `no responders`
    # — no transient code, no "retry shortly", no bound the operator can plan around. That is exactly the
    # class the drill header defines, and booking it as a coverage gap (as the first 2026-09-19 revision did)
    # was the laundering X27 forbade: a broker where every degraded-JS push sits the full budget would have
    # matched the expectation with zero deviation.
    _g67_put_face=$(printf '%s' "$_G67_OUT" | grep -oiE 'Put: .*(nats: timeout|context deadline exceeded|no responders)' | head -1 | cut -c1-80)
    product_red "#84 tier-B Put on a JS that has lost quorum sits the FULL size budget (or fails instantly with no responders) and reports a bare '$_g67_put_face' — no transient code, no retry hint, no bound the operator can plan around: #67's operator-facing defect, moved from the prepare leg to the Put leg by 0b204b5 (prepare now resolves the bucket locally and succeeds without a JS round-trip, so the G67 refusal wording (a)–(d) is unreachable from this injection)"
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
    elif _g67_out_has 'stopped answering during the upload \([0-9]+ probes over'; then
        # #84 fix: on the Put leg the bounded mechanism is the ctl's liveness watchdog, and its evidence is
        # the probe count in the message itself ("N probes over Ms failed") — printed only when the
        # watchdog actually fired. The broker never retries this leg, so the slog tooth above cannot apply.
        _as_pass "#67/#84 NON-VACUITY: the ctl's Put-leg watchdog reports the probes it lost before cancelling (the bounded mechanism ran; a re-worded message without the watchdog carries no count)"
    elif _g67_out_has 'is not accepting the upload \(refused ([2-9]|[1-9][0-9]+) attempt\(s\) over'; then
        # #84 fix, the INSTANT face (image #7 receipt: `Put: nats: no responders` in 0.9 s — a leader
        # election in progress, which the watchdog built for the STALL face never sees). The bounded
        # mechanism here is the ctl's own Put retry (3 attempts, 3 s / 6 s apart), and its evidence is the
        # attempt count + window in the refusal text, printed only after the retries actually ran. The
        # count must be ≥ 2: `refused 1 attempt(s)` IS printed for a single attempt whose transient face
        # arrived with the budget already spent (putWithJSRetry returns the refusal with attempts=1 when
        # ctx is done), so a one-attempt refusal proves no retry ran — the first version's comment said the
        # opposite and accepted it (round-2 review R1-F3).
        _as_pass "#67/#84 NON-VACUITY: the ctl's Put-leg bounded retry reports ≥2 attempts before refusing (the bounded mechanism ran; a single-attempt refusal is not evidence of it)"
    elif _g67_out_has 'is not accepting the upload \(refused 1 attempt\(s\) over'; then
        not_covered "#67 non-vacuity tooth" \
            "the refusal is worded as the bounded-retry face but reports ONE attempt — the transient face arrived with the ctx already spent, so no retry ran and this run cannot distinguish the bounded mechanism from wording alone (its duration was ${_G67_INJ_S:-?}s, under the 150 s stall bar)" gap
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
