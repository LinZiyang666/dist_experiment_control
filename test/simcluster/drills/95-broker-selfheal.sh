#!/bin/sh
# 95-broker-selfheal — S9 / G-C. N=2: G.2 broker-restart reconciliation + the FIRST behavioural proof of
# #23 (Restart=always vs on-failure) + the DELETING-session boot resume.
# Plan: docs/reviews/s7-s9-plan.md §3.2. Expected landing: GREEN.
# Runtime ~14min. Topology: 2 brokers + 1 agent + 1 ctl (grow-family).
#
# ── WHY T1 IS THE POINT OF THIS DRILL (#23's first behavioural pin) ─────────────────────────────────
# Until now #23 had only a STATIC drift check (drill 13's doctor). The claim is:
#   "Restart=on-failure would NOT revive a clean exit, stranding the broker (inactive, not failed)
#    after a nats blip. Restart=always revives any self-exit."  (install.sh:748-753, verbatim)
# To test that behaviourally you need an exit that is CLEAN (code 0), because that is the only case where
# the two Restart= policies DIFFER:
#   * kill -9  -> unclean -> BOTH policies revive it -> proves nothing about #23.
#   * SIGTERM to MainPID -> NotifyContext cancels -> b.Run returns context.Canceled -> RunE returns nil
#     -> exit 0 (serve.go:241-247) -> ONLY `always` revives it.
# And it must bypass systemctl: when systemd itself initiates the stop, Restart= does not apply at all,
# so `systemctl stop` would destroy the very discriminator. Hence `kill -TERM $MainPID` directly.
# Verified for this build: `grep -c SuccessExitStatus scripts/install.sh` = 0, so nothing widens the
# success set beyond exit 0.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. T1 MUST NOT use systemctl stop/restart (see above) — it would silently make the arm vacuous.
#  2. T1's and T2's journal wording are each other's discriminator: `Deactivated successfully` AND NOT
#     `Main process exited, code=` (clean) vs `code=killed, status=9` (unclean). Watching only NRestarts
#     go up cannot tell the two apart, and #23's entire argument evaporates.
#  3. `ActiveState==active` IS NOT A TERMINUS. The unit is Type=simple, so it is "active" the instant exec
#     happens — long before the broker is usable. Close on `broker: ready` line COUNT increasing in
#     /var/log/tether/broker.err (R-BROKERLOG: the broker's output is NOT in journald) plus real traffic.
#  4. T3 is a HARD claim + a RECORDED mechanism, not accept-both. `inactive (dead)` / `failed` there is
#     #23's symptom and an ASSERT-FAIL (a regression of G1's Restart=always fix), never a shrug. Only
#     WHICH of the three recovery paths it takes is recorded — because the clean-exit trigger is
#     unlocated in source (README says so), and hard-asserting a specific path would invent an
#     expectation nobody can prove.
#  5. T2's agent anti-oracle needs a same-window positive control (`node ls` still ONLINE). Otherwise, if
#     T3 never runs, it leaves a vacuous PASS.
#  6. Absence assertions are fail-closed: prove rc=0 and non-empty output BEFORE concluding "not there".
#  7. StartLimitBurst=5/10s is deliberately NOT disabled (install.sh:752-753). Guard `Result` and space
#     the arms, or a later arm dies of start-limit and gets blamed on the product.
#  8. R-LIVENESS-NOT-HEALTH: `cluster status`'s exit code is HEALTH (1=DEGRADED), never liveness.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/events.sh"
. "$HERE/lib/assert.sh"

SID=lab
PIN=959595
NURL="nats://brk1:4222"
EV_NURL="$NURL"
EVCAP=/tmp/95-events.jsonl

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_berr() { dexec "$1" -- tail -n "${2:-40}" /var/log/tether/broker.err 2>/dev/null; }
_ready_count() { dexec "$1" -- sh -c 'grep -c "broker: ready" /var/log/tether/broker.err 2>/dev/null || echo 0' | tr -d '\r'; }
_mainpid() { dexec "$1" -- systemctl show -p MainPID --value tether-broker 2>/dev/null | tr -d '\r'; }
_nrestarts() { dexec "$1" -- systemctl show -p NRestarts --value tether-broker 2>/dev/null | tr -d '\r'; }
_result() { dexec "$1" -- systemctl show -p Result --value tether-broker 2>/dev/null | tr -d '\r'; }
_active() { [ "$(dexec "$1" -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')" = active ]; }
# Timestamp gate, not --after-cursor (the export cursor silently filters everything — drill 94's lesson).
_jcursor_b() { dexec "$1" -- date '+%Y-%m-%d %H:%M:%S' 2>/dev/null | tr -d '\r'; }
_jafter() { dexec "$1" -- sh -c "journalctl -u tether-broker --since='$2' --no-pager 2>/dev/null | grep -qF '$3'"; }
_acursor() { dexec "$1" -- date '+%Y-%m-%d %H:%M:%S' 2>/dev/null | tr -d '\r'; }
_agent_jafter() { dexec "$1" -- sh -c "journalctl -u tether-agent --since='$2' --no-pager 2>/dev/null | grep -qF '$3'"; }
# R-LIVENESS-NOT-HEALTH: cluster status returns a HEALTH exit code (1=DEGRADED); liveness = parseable JSON.
_broker_live() { _bt brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id != null' >/dev/null 2>&1; }
_agt1_online() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }


# ── predicates (FUNCTIONS — R-NOSHC) ────────────────────────────────────────────────────────────────
_t2_session_ok()   { "$SIM" ctl -- session ls --json 2>/dev/null | jq -e --arg s "$SID" '[.sessions[]?|select(.name==$s)]|length==1' >/dev/null 2>&1; }
_t2_history_ok()   { _th=$("$SIM" ctl -- history -n 50 2>&1); [ $? = 0 ] && [ -n "$_th" ] && printf '%s' "$_th" | grep -q SELFHEAL-SENTINEL; }
# absence, fail-closed: the reader must be proven alive first (D2b/T2h are the paired positive controls).
_t2_agent_undisturbed() { ! _agent_jafter agt1 "$CURA" 'agent: re-registered after reconnect'; }
_t3_agent_works()  { "$SIM" ctl -- exec agt1 -- echo T3E-ALIVE >/dev/null 2>&1; }
# 95-D CORRECTED PREDICATE (R6 adjudication → R13). The OLD body hard-pinned .leader_id=="brk1" — but by
# arm D, T1a (SIGTERM brk1) + T2a (SIGKILL brk1) have felled brk1's broker TWICE, and at N=2 leadership can
# legitimately settle on brk2. The old pin then FAILED for a reason UNRELATED to what arm D tests (DELETING
# boot-resume), manufacturing a FALSE not_covered (r6-findings: "95-D REFUTED — 谓词过严"). Correct
# semantics: raft has a STABLE leader (some voter leads AND that identity is unchanged across two spaced
# reads) — NOT that it is brk1. Stability distinguishes "a leader exists this instant" from "raft settled";
# .leader_id is read as a string (jq // empty), never pinned to an identity. Proven able to go RED by the
# D-NEG arm below (a no-leader input and a churning-leader input must BOTH fail) + tests/r9d-nonvacuity.
_d_leader_via()    { _bt "$1" -- tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty' 2>/dev/null; }
_d_raft_ok()       {
    _drl1=$(_d_leader_via brk1); [ -n "$_drl1" ] || return 1
    sleep 2
    _drl2=$(_d_leader_via brk1); [ -n "$_drl2" ] || return 1
    [ "$_drl1" = "$_drl2" ]
}
_d_core_ok()       { "$SIM" ctl -- node ls --json >/dev/null 2>&1; }
_d_state_is_deleting() {
    _ds=$(dexec brk1 -- sqlite3 -readonly /var/lib/tether/tether.db "select state from sessions where name='doomed'" 2>/dev/null | tr -d '\r')
    log "D3 diag: sessions.state for 'doomed' = '${_ds:-<none>}'"
    [ "$_ds" = DELETING ]
}
_d_doomed_gone()   { ! "$SIM" ctl -- session ls --json 2>/dev/null | jq -e '.sessions[]?|select(.name=="doomed")' >/dev/null 2>&1; }

_cleanup() {
    ev_stop ctl1 || true
    for _n in brk1 brk2; do
        dexec "$_n" -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl start nats-server tether-broker 2>/dev/null' >/dev/null 2>&1 || true
    done
    true
}

drill_begin "95-broker-selfheal (N=2: G.2 restart reconcile + #23 discriminating proof + DELETING resume)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

assert_setup "grow_to_2 (N=2 VOTER + JS meta formed + leader pinned brk1)" grow_to_2 1 1
assert_setup "session $SID + ctl login"                    "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                             "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml (S0-tunnel)"       agent_provision_yaml agt1 "$SID" "$NURL" open
TOK=$(expose_serve_sentinel agt1 8095) || setup_fail "could not start the sentinel http.server on agt1"
# R-PIN-HOME: without --on-broker the home is a coin flip (#29's allocate-time face) — a registered defect
# owned by drill 71 that would only make this drill flaky and false-RED the self-heal.
assert_setup "expose agt1:8095 --on-broker brk1 --name heal (home PINNED — #29 makes it a coin flip otherwise)" \
    "$SIM" ctl -- expose agt1 --local 8095 --on-broker brk1 --name heal
PUB=$("$SIM" ctl -- expose explain heal --json 2>/dev/null | jq -r '.public_port // empty')
[ -n "$PUB" ] || setup_fail "could not read the public port of expose 'heal'"
assert_setup "pre-injection data plane serves the exact sentinel" \
    poll_until 30 2 "sentinel via brk1:$PUB" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
assert_setup "seed a process + an exec row for the G.2 survival oracles" \
    "$SIM" ctl -- exec agt1 -- echo SELFHEAL-SENTINEL
assert_setup "sys.events observer up (member core-sub — permissions.go:147)" ev_sub_start ctl1 "$SID" "$EVCAP"
assert_setup "the event subscription is really established" poll_until 20 2 "sys.events sub ready" -- ev_ready ctl1 "$EVCAP"

# ── T0-drift — T1's whole inference rests on the unit's content, so ASSERT the premise ──────────────
# (T0-ctrl, a scratch systemd semantics control, was deliberately CUT: a scratch unit contains no tether,
# so the depth gate's answer — "what does this add about TETHER at the deploy tier?" — is zero. T1 proves
# always+exit0 -> revived directly on the REAL unit; `on-failure` not reviving is a counterfactual about a
# config tether does not ship. See plan §11-H.)
assert_ok "T0-drift the real unit really says Restart=always (T1's inference is only valid if its premise holds)" \
    dexec brk1 -- grep -qx 'Restart=always' /etc/systemd/system/tether-broker.service

# ══ T1 — SIGTERM CLEAN-EXIT: the #23 discriminator ══════════════════════════════════════════════════
PID0=$(_mainpid brk1); NR0=$(_nrestarts brk1); RD0=$(_ready_count brk1)
[ -n "$PID0" ] && [ "$PID0" != 0 ] || setup_fail "could not read brk1's broker MainPID"
CURB=$(_jcursor_b brk1)
assert_ok "T1a SIGTERM straight to MainPID $PID0 (BYPASSING systemctl: a systemd-initiated stop makes Restart= inapplicable and would destroy the discriminator)" \
    dexec brk1 -- kill -TERM "$PID0"
_t1_clean_wording() { _jafter brk1 "$CURB" 'Deactivated successfully'; }
_t1_not_unclean()   { ! _jafter brk1 "$CURB" 'Main process exited, code='; }
_t1_restarted()     { _n=$(_nrestarts brk1); [ -n "$_n" ] && [ "$_n" -gt "${NR0:-0}" ] 2>/dev/null; }
_t1_new_pid()       { _p=$(_mainpid brk1); [ -n "$_p" ] && [ "$_p" != 0 ] && [ "$_p" != "$PID0" ]; }
_t1_ready_grew()    { _c=$(_ready_count brk1); [ -n "$_c" ] && [ "$_c" -gt "${RD0:-0}" ] 2>/dev/null; }
_t1_no_startlimit() { [ "$(_result brk1)" != start-limit-hit ]; }
assert_ok "T1b journal says 'Deactivated successfully' — the exit was CLEAN (serve.go:241-247: SIGTERM -> NotifyContext -> context.Canceled -> RunE nil -> exit 0)" \
    poll_until 30 2 "clean-exit wording in the journal" -- _t1_clean_wording
assert_ok "T1c and NOT 'Main process exited, code=' — this is T2's exact counterpart and is what makes #23 testable at all" _t1_not_unclean
assert_ok "T1d systemd revived it (NRestarts grew past $NR0) — with SuccessExitStatus unset (grep -c = 0), exit 0 is the ONLY success, so Restart=on-failure would have left it inactive-not-failed. THIS is #23's first behavioural proof" \
    poll_until 60 2 "NRestarts grows after the clean exit" -- _t1_restarted
assert_ok "T1e a NEW MainPID replaced $PID0" poll_until 30 2 "a new MainPID appears" -- _t1_new_pid
assert_ok "T1f the broker is REALLY ready — 'broker: ready' line count grew in broker.err (NOT ActiveState==active: Type=simple is 'active' the instant exec happens, long before it can serve)" \
    poll_until 90 3 "a new 'broker: ready' line lands" -- _t1_ready_grew
assert_ok "T1g FUNCTION really recovered: the data plane serves the exact sentinel again" \
    poll_until 90 2 "sentinel after the clean-exit revival" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
assert_ok "T1h no start-limit was hit (install.sh:752-753 deliberately leaves StartLimitBurst=5/10s on — a leftover would kill a later arm and get blamed on the product)" _t1_no_startlimit

# ══ T2 — kill -9 (G.2's crash-recovery direction) ═══════════════════════════════════════════════════
poll_until 20 5 "spacing the restart arms so StartLimitBurst=5/10s cannot bite" -- false || true
PID1=$(_mainpid brk1); NR1=$(_nrestarts brk1); RD1=$(_ready_count brk1)
CURB2=$(_jcursor_b brk1); CURA=$(_acursor agt1)
assert_ok "T2a SIGKILL MainPID $PID1 (unclean — the OTHER exit semantics)" dexec brk1 -- kill -9 "$PID1"
_t2_unclean_wording() { _jafter brk1 "$CURB2" 'code=killed, status=9'; }
_t2_ready_grew()      { _c=$(_ready_count brk1); [ -n "$_c" ] && [ "$_c" -gt "${RD1:-0}" ] 2>/dev/null; }
assert_ok "T2b journal says 'code=killed, status=9' — T1 asserted this string's ABSENCE, T2 asserts its PRESENCE: together they prove we really produced two DIFFERENT exit semantics rather than one" \
    poll_until 30 2 "unclean-exit wording in the journal" -- _t2_unclean_wording
assert_ok "T2c the broker really came back (new 'broker: ready' line)" poll_until 90 3 "ready line after the crash" -- _t2_ready_grew
# G.2 survival — never "it came back up", always the state it had to keep.
assert_ok "T2d G.2: the session survived the crash" \
    poll_until 60 3 "session $SID readable after the crash" -- _t2_session_ok
assert_ok "T2e G.2: the ALLOCATED port survived and the data plane recovered (real bytes)" \
    poll_until 90 2 "sentinel after the crash" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
assert_ok "T2f G.2: the pre-crash exec row is still in history" _t2_history_ok
# The roadmap said "the agent reconnects". Source says otherwise: broker and agent are independent
# processes, so a broker crash does NOT drop the agent's NATS connection. Zero agent disturbance is
# PART of G.2 — the broker healing itself must not bounce the fleet.
assert_ok "T2g the agent was NOT disturbed at all (broker and agent are independent processes — a broker crash does not drop the agent's NATS link; the roadmap's 'the agent reconnects' is wrong here). Paired with a same-window positive control below" \
    _t2_agent_undisturbed
assert_ok "T2h POSITIVE CONTROL for T2g (without it, T2g would be a vacuous PASS if the journal reader or the agent were simply dead): agt1 is ONLINE (polled — a kill-9 of the broker can briefly STALE the heartbeat view)" \
    poll_until 60 3 "agt1 ONLINE after the crash" -- _agt1_online

# ══ T3 — restart nats-server: #23's full chain ═════════════════════════════════════════════════════
poll_until 20 5 "spacing the restart arms" -- false || true
PID2=$(_mainpid brk1); NR2=$(_nrestarts brk1); RD2=$(_ready_count brk1)
CURA2=$(_acursor agt1)
assert_ok "T3a restart nats-server underneath the broker" dexec brk1 -- systemctl restart nats-server
# HARD CLAIM: `inactive (dead)` / `failed` here IS #23's symptom and a regression of G1's fix.
_t3_alive() { _active brk1 && [ "$(_result brk1)" = success ]; }
assert_ok "T3b HARD: the broker is active AND Result=success after the nats restart. 'inactive (dead)' or 'failed' here is EXACTLY #23's symptom and a regression of G1's Restart=always fix — a release blocker, never a shrug" \
    poll_until 120 2 "broker survives / is revived across the nats restart" -- _t3_alive
assert_ok "T3c HARD: real read/write recovered (data plane)" \
    poll_until 90 2 "sentinel after the nats restart" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
assert_ok "T3d HARD: exec really works again" "$SIM" ctl -- exec agt1 -- echo T3-ALIVE
# T3d already hard-proved the agent WORKS after the nats restart (exec round-trip). Whether it logged a
# full re-register is RECORDED, not asserted: if the broker survived in place the NATS link can reconnect
# seamlessly without a full drop, so onNATSReconnect (proxy.go:486) need not fire — correct, not a defect.
if _agent_jafter agt1 "$CURA2" 'agent: re-registered after reconnect'; then
    log "T3e mechanism: the agent logged a full re-register (its NATS link was fully dropped)"
else
    log "T3e mechanism: the agent did NOT log a full re-register — the NATS link reconnected seamlessly (broker survived in place; correct, not a defect). T3d's exec round-trip is the functional proof"
fi
# RECORDED, not asserted: WHICH path it took. The clean-exit trigger is unlocated in source (README says
# so; neither documented recipe reproduces it), so hard-asserting one path would manufacture an
# expectation nobody can prove. This is NOT the measure-and-record that review rejected in drill 74: the
# CLAIM above is hard-asserted; only the mechanism is recorded.
_PID3=$(_mainpid brk1); _NR3=$(_nrestarts brk1); _RD3=$(_ready_count brk1)
if [ "$_PID3" = "$PID2" ]; then _T3_PATH=SURVIVED-IN-PLACE
elif [ "${_NR3:-0}" -gt "${NR2:-0}" ] 2>/dev/null; then _T3_PATH=REVIVED-BY-UNIT
else _T3_PATH=REVIVED-AFTER-CRASH; fi
log "T3 mechanism (RECORDED, not asserted — the clean-exit trigger is unlocated in source): path=$_T3_PATH MainPID $PID2->$_PID3 NRestarts $NR2->$_NR3 ready-lines $RD2->$_RD3"

# ══ EV — tetherd_restarted (inventory row 29) ══════════════════════════════════════════════════════
# Hangs off T1/T2 ONLY: the core-sub itself disconnects during T3's nats restart, so a miss there would
# say nothing about the product. Corroboration — never a gate.
assert_ok "EV the tetherd_restarted event was emitted and is readable by a member over real NATS (inventory row 29; core-sub per permissions.go:147). Corroboration only — hung off T1/T2 because T3's nats restart drops the subscription itself" \
    poll_until 30 2 "tetherd_restarted seen" -- ev_seen ctl1 "$EVCAP" tetherd_restarted

# ══ D — DELETING boot resume (G.2 (1)b) ════════════════════════════════════════════════════════════
# RECIPE-A: raft and JetStream are SEPARATE quorums in tether (raft on :7400 over its own TCP; JS meta
# over the nats route :6222). Stopping ONLY brk2's nats-server kills the JS meta quorum while raft stays
# healthy — so the tombstone commits through raft, phase 2 (the JS stream delete) fails, and the session
# is parked in DELETING for the boot resumer to finish.
# hermetic (test/p7/audit_e2e_test.go:323) writes session.Tombstone straight into SQLite; the deploy tier
# MUST NOT copy that — in cluster mode `sessions` is replicated state and an out-of-raft write forks
# leader/follower content (broker.go:1130-1136 spells the mechanism out).
# ── D-NEG (R13) — the tightened _d_raft_ok MUST be able to go RED, or the precondition poll below is a
#    permanently-true oracle (the OLD brk1-pin was a FALSE gap; a green means nothing unless the predicate
#    can red). Drive the REAL stability logic with a subshell-local shadowed reader (never leaks into the
#    real poll): (i) NO leader visible and (ii) a CHURNING leader (two different ids across the two spaced
#    reads) must BOTH make _d_raft_ok fail. Deterministic + hermetic — zero cluster disruption. Mirrored in
#    tests/r9d-nonvacuity.sh.
_d_neg_no_leader() { ( _d_leader_via() { printf ''; }; _d_raft_ok ); }
_d_neg_churn()     { ( _CH="$1"; _d_leader_via() { if [ -f "$_CH" ]; then rm -f "$_CH"; printf brk2; else : >"$_CH"; printf brk1; fi; }; _d_raft_ok ); }
_d_neg1_ok()       { ! _d_neg_no_leader; }
_d_neg2_ok()       { ! _d_neg_churn "/tmp/95-dneg-churn.$$"; }
assert_ok "D-neg1 the tightened _d_raft_ok returns FALSE when NO leader is visible (existence half can RED — the predicate is not permanently true)" _d_neg1_ok
assert_ok "D-neg2 the tightened _d_raft_ok returns FALSE when the leader CHURNS across the two spaced reads (stability half can RED — a mere this-instant leader is rejected, and the OLD 'must be brk1' semantics is gone)" _d_neg2_ok

assert_ok "D0 create a session to doom" "$SIM" ctl -- session create doomed --pin 969696
assert_ok "D0b switch the ctl back to $SID (R-CTX: session create ACTIVATES the new session, and a ctl left on a session we are about to destroy fails every later call with an Authorization Violation)" \
    dexec -u sim ctl1 -- env HOME=/home/sim tether login -s "$SID" --pin "$PIN" --nats-url "$NURL"
assert_ok "D1 stop ONLY brk2's nats-server (kills the JS meta quorum; brk2's tether-broker and raft stay up)" \
    dexec brk2 -- systemctl stop nats-server
# Three preconditions — without them "the session vanished after a restart" is vacuous.
# The DELETING recipe needs raft to stay healthy while ONLY JS meta is down. If stopping brk2's nats
# instead cost raft its leader (N=2 is fragile), the recipe's premise does not hold on the real stack —
# record 95-D as NOT-COVERED rather than asserting a precondition we could not establish.
if ! poll_until 30 3 "raft stays healthy (a STABLE leader — any voter, not pinned to brk1) with only JS down" -- _d_raft_ok \
   || ! poll_until 30 3 "core NATS still serves" -- _d_core_ok; then
    dexec brk2 -- systemctl start nats-server >/dev/null 2>&1 || true
    not_covered "95-D DELETING boot resume: could not decouple raft from JS at N=2" \
        "stopping brk2's nats-server to kill only the JS meta quorum also disturbed raft (N=2 raft + JS share fate here) — raft has no STABLE leader (checked without pinning brk1, the R6-corrected predicate), so the middle DELETING state cannot be established on the real stack. cluster mode forbids the hermetic recipe of writing sessions rows outside raft (broker.go:1130-1136). hermetic owner: test/p7/audit_e2e_test.go:323" gap
    drill_end; exit "$?"
fi
# R13: `session rm` takes NO --yes flag (only --ack-alerts; session.go:210) — the old `--yes` failed with
# `unknown flag: --yes` (rc=70) so the tombstone never even started and the row stayed ACTIVE, which the
# tightened _d_raft_ok above finally let us SEE (the old brk1-pin aborted the arm before D3 ever ran). We
# do NOT assert rc=0 here: with JS meta down the finalize legitimately errors while the raft tombstone
# still commits, so it is _d_state_is_deleting (a direct SQLite read) that decides the path, not this rc.
_D_RM=$("$SIM" ctl -- session rm doomed 2>&1); _D_RM_RC=$?
log "D3 session rm doomed: rc=$_D_RM_RC out=$(printf '%s' "$_D_RM" | tail -1)"
if _d_state_is_deleting; then
    _as_pass "D3 the session is parked in DELETING (the tombstone committed through raft; the JS-side finalize could not complete) — the middle state is PROVEN, so the resume oracle below cannot be vacuous"
    assert_ok "D4 restore brk2's nats-server so JS can form again" dexec brk2 -- systemctl start nats-server
    # R13: the boot resumer (reconcileHistoryStreamsOnBoot, broker.go:1023-1038) is a ONE-SHOT gated on a
    # 1-second js.AccountInfo probe at boot with NO periodic retry — if JS meta has not re-formed within
    # that 1s window when brk1 restarts, the DELETING resume is SILENTLY SKIPPED and the row stays until the
    # NEXT restart. So let JS meta actually re-form after D4 BEFORE restarting brk1 in D5 (the resumer's own
    # documented precondition: "Called once from Run after JS is confirmed available"), or D6b would be
    # testing our restart timing racing JS re-election, not the resumer. A JS-backed history read proves JS
    # meta is back (its AccountInfo boot probe is lighter, so a working stream read implies the probe passes).
    # NOT papering over: this establishes the resumer's contractual precondition; the resilience nuance
    # (one-shot, 1s probe, no retry) is reported separately as a bounded observation, not masked.
    assert_ok "D4b JS meta re-formed after restoring brk2's nats (a JS-backed history read works again) — the boot resumer's documented precondition (broker.go:1023-1038); without it D5's 1s boot probe races JS re-election and the resume is silently skipped" \
        poll_until 90 3 "JS-backed history read works again after brk2 nats restore" -- _t2_history_ok
    assert_ok "D5 restart brk1's broker — the boot resumer must finish the interrupted delete (audit.go:161 / sessions.go:161,180)" \
        dexec brk1 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl restart tether-broker'
    assert_ok "D6a the broker is live again" poll_until 120 3 "brk1 live after the resume restart" -- _broker_live
    assert_ok "D6b G.2(1)b: the DELETING session is really gone after the boot resume" \
        poll_until 90 3 "the doomed session row disappears" -- _d_doomed_gone
    assert_ok "D6c UNRELATED ROWS INTACT (the control source: the resume finished ONE session, it did not sweep the table)" _t2_session_ok
    assert_ok "D6d and the data plane still serves the exact sentinel" \
        poll_until 90 3 "sentinel after the DELETING resume" -- dp_curl_ok_body ctl1 "http://brk1:$PUB/" "$TOK"
else
    dexec brk2 -- systemctl start nats-server >/dev/null 2>&1 || true
    not_covered "95-D DELETING boot resume: could not park a session in DELETING on the real stack (session rm rc=$_D_RM_RC; the JS-meta-down window did not leave the row in DELETING)" \
        "raft and JS quorum did not decouple as modelled at N=2; cluster mode forbids the hermetic recipe of writing sessions rows outside raft (broker.go:1130-1136's fork mechanism). hermetic owner: test/p7/audit_e2e_test.go:323" gap
fi

drill_end
