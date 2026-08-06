#!/bin/sh
# 98-stuck-redial-recovery.sh — S9-family, N=3 cluster + 1 agent + ctl: the POST-FIX recovery regression
# for gotcha #72 (agent stuck-disconnect teardown used to block in nc.Close() under nats.go's nc.mu for
# ~11min with no upper bound; the fix makes teardown cancel-first + bounded: pin → arm → cancel →
# bounded finalizer → sticky poison → escalate; recovery hard bound ≈ redialAfter(20s) + closeBudget(10s)
# + poisonGrace(10s) + reconnect ≤ 60s + slack). Plan: docs/reviews/gotcha72-teardown-plan.md.
#
# WHAT THIS DRILL IS — AND HONESTLY IS NOT (expected-verdicts note, feasible-critic ruling):
#   IS : a deploy-tier RECOVERY REGRESSION on the real stack over nats:// — black-hole the agent's
#        current broker's client port (silent DROP, the partition instrument), and assert the agent's
#        heartbeat resumes via ANOTHER voter within the WRITTEN budget, classifying the recovery path.
#   NOT: a pre-fix reproduction of #72 itself. The live incident rode a WSS half-dead handshake;
#        simcluster fronts no wss:// listener today, so the ws write-path black-hole is NOT
#        constructable here — that arm is the [GAP #72] below and the ledger's flip condition.
#
# RECOVERY-PATH CLASSIFICATION (MainPID three-way, per the #72 ledger's flip requirements): the assert
# names WHICH allowed path recovered — same-PID in-process rebuild (the designed path), same-PID
# self-exec (the escalate ladder's rebuild arm), or a supervisor restart (PID changed; allowed only
# with systemd — its appearance under the designed budget is recorded, not banded away).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/logs.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
# Written recovery budget — DERIVED, not guessed (first real runs, 2026-08-02: a 90s budget passed
# once and failed twice, i.e. it was being satisfied by luck):
#
#   detection   up to 4min. tether does NOT set nats.Options.PingInterval/MaxPingsOut, so nats.go's
#               defaults apply (2min ping interval, 2 outstanding pings). Under a SILENT DROP there is
#               no RST to shortcut this — the connection is only declared dead when the pings time out.
#   redialAfter 20s, armed by DisconnectErrHandler AFTER that declaration.
#   ladder      closeBudget 10s + poisonGrace 10s (gotcha #72 fix).
#   reconnect   dial + register on a surviving voter, plus backoff.
#
# The product's published ≤60s bound covers the part AFTER nats.go declares the disconnect (usage.md
# §9.9 says so explicitly); this drill necessarily measures detection + that bound, so its budget is
# the sum. Widening it is not inventing an SLA — the 4min term is a documented library default.
RECOVERY_BUDGET=330

_cleanup() { fault_cleanup_all 2>/dev/null; true; }
drill_install_traps _cleanup

_hb_of() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.last_heartbeat_at' | head -1; }
_online() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n and .status=="ONLINE")' >/dev/null 2>&1; }
_mainpid() { "$SIM" exec agt1 -- systemctl show tether-agent -p MainPID --value 2>/dev/null; }
_exe_of_pid() { dexec agt1 -- readlink "/proc/$1/exe" 2>/dev/null; }

# heartbeat ADVANCES past a captured watermark — the one probe the #72 incident proved trustworthy
# (PID-alive and ESTABLISHED-socket both read healthy while the node was gone).
_hb_advanced() { _n=$(_hb_of agt1); [ -n "$_n" ] && [ "$_n" != "$HB0" ] && _online agt1; }
# IMPACT proof, third and final shape (first real runs, 2026-08-02). A "heartbeat stalls" probe was
# the wrong instrument twice over: it compared against a watermark captured BEFORE the connz discovery
# and the three injection asserts, so the heartbeat had already advanced past it through the still
# healthy link and the predicate could never come true; and even with a fresh watermark, the fix
# recovers fast enough on this stack that a stall window is not reliably observable. The impact that
# IS unambiguous — and comes from the same authoritative source as the baseline — is that the client
# connection LEFT the broker we cut. If the cut had missed the live edge (both earlier mistakes),
# /connz would still show agt1 on CUT_BROKER and this stays red.
# IMPACT: the connection must be OBSERVED ABSENT from the cut broker — an observation error keeps
# polling instead of passing (F5.4). On success it stamps HB_IMPACT, the post-fault watermark every
# recovery assertion below compares against (F5.1: HB0 was captured before discovery + injection, so
# a heartbeat written through the still-healthy link already satisfied the old check).
_conn_left_cut_broker() {
    case "$(_connz_state "$CUT_BROKER")" in
        absent)  return 0 ;;
        present) return 1 ;;
        *)       log "98: connz read on $CUT_BROKER FAILED — treating as not-yet-absent (never fail-open)"; return 1 ;;
    esac
}
# Recovery is a PRODUCT fact, not a transport fact (F5.2): the heartbeat must advance past the
# POST-IMPACT watermark (HB0, re-read in the PARENT shell right after the impact assert — see there),
# which only happens once the agent has re-registered and its heartbeats are being recorded again.
# connz alone would only prove that a TCP session exists.
# One ABSOLUTE deadline shared by impact + recovery (F5.3): two independent full budgets meant the
# pair could legitimately consume 2×RECOVERY_BUDGET while the drill still claimed "within the
# written budget". _budget_left prints the seconds remaining, floored at 1.
_budget_left() {
    _now=$(date +%s); _rem=$((DEADLINE - _now))
    [ "$_rem" -lt 1 ] && _rem=1
    printf '%s' "$_rem"
}
# Recovery must LAND on a different voter, not on brk1 healing (which cannot happen: the DROP stays
# armed). Reads the surviving voters' journals for this agent's registration.
# origin: drill 33's first real run — neither process's application log reaches journald, so
# `journalctl -u <unit>` (which is what `$SIM logs` reads) does not carry it. Read the files.
# h1 F3 UPDATE: the ORIGINAL note here blamed unit-level redirects for both
# processes. That was only ever true of the broker, and h1 changed even that.
# The accurate account: each process writes its own slog to its own file
# (broker.yaml `log_file:` -> broker.log; the agent binary's default ->
# ~/.tether/agent/<sid>/agent.log), and h1 flipped install.sh's broker unit to
# `StandardOutput=journal` / `StandardError=journal`, and moved the slog into a
# PROCESS-OWNED rotating file named by broker.yaml's `log_file:` (broker.log).
# So: slog -> broker.log (still a FILE, still not journald), while panics and
# stacktraces now DO reach journald. `journalctl -u tether-broker` is therefore no
# longer "empty by construction" — it is the PANIC stream, and treating it as the
# slog stream would let a stacktrace satisfy an application-line assertion.
# Use drills/lib/logs.sh, which keeps the two apart.
_brk_log_of()  { sim_broker_slog "$1" 4000; }
# h1 F3: agent.log is written by the AGENT BINARY (its default sink), not by a
# unit redirect — image/units/tether-agent.service sets no Standard*= at all.
# The distinction matters: the unit's journal therefore still receives the
# agent's pre-logger boot output, so it is not empty and must not be mistaken
# for the slog.
_agt1_log()    { sim_agent_slog_tail agt1 2000; }
# origin: first real run (2026-08-02), TWO wrong ways to find the connection — both recorded.
# (1) The drill ASSUMED agt1 stays on brk1 because agent-join dials it first. WRONG on a 3-voter
#     cluster: after the agent adopts the signed roster its dial pool is VOTER-first with an
#     intra-voter shuffle, so a reconnect lands anywhere.
# (2) "whichever broker LOGGED the register" is also wrong: register is a QUEUE-GROUP subject, so the
#     member that handles it need not be the one holding the agent's TCP connection. Cutting brk1 on
#     that basis again missed the live edge — and the IMPACT probe (internal review F98-2) refused to
#     pretend a recovery had been measured, exactly as designed.
# The AUTHORITATIVE source is nats-server's own /connz on each broker (drill 41 uses the same one):
# it lists the client connections that server actually holds, by CONNECT name.
# THREE-STATE connz read (external review F5.4): "observation failed" must never be laundered into
# "the connection is absent", or a curl/jq/monitor hiccup would satisfy the IMPACT arm fail-open.
# Prints present|absent|error; callers must branch on all three.
_connz_state() {
    _cz=$("$SIM" exec "$1" -- curl -sf --max-time 2 'http://127.0.0.1:8223/connz' 2>/dev/null) || { printf 'error'; return; }
    printf '%s' "$_cz" | jq -e '.connections' >/dev/null 2>&1 || { printf 'error'; return; }
    if printf '%s' "$_cz" | jq -e --arg n "tether-agent:$SID:agt1" '.connections[]?|select(.name==$n)' >/dev/null 2>&1; then
        printf 'present'
    else
        printf 'absent'
    fi
}
_agent_conn_on() { [ "$(_connz_state "$1")" = present ]; }
_broker_holding_agt1() {
    for _b in brk1 brk2 brk3; do
        if _agent_conn_on "$_b"; then printf '%s' "$_b"; return 0; fi
    done
    return 1
}
# Recovery must land on a voter OTHER than the one we cut (the DROP stays armed, so the cut broker
# cannot heal). Scans the survivors for a register logged AFTER the cut.
# Recovery must land on a voter OTHER than the one we cut — asked of /connz, the same authoritative
# source, so "landed elsewhere" is a fact about the live connection rather than about log ordering.
_registered_on_another_voter() {
    for _b in brk1 brk2 brk3; do
        [ "$_b" = "$CUT_BROKER" ] && continue
        if _agent_conn_on "$_b"; then
            log "98: recovery landed on $_b (cut was $CUT_BROKER)"
            return 0
        fi
    done
    return 1
}

drill_begin "98-stuck-redial-recovery (post-#72-fix bounded teardown recovery over nats://)"
"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers + 1 agent + 1 ctl"    "$SIM" up --brokers 3 --agents 1 --ctl 1
assert_ok "init brk1 + grow to 3 (HA roster)" sh -c '"$0" init brk1 && "$0" grow brk2 && "$0" grow brk3' "$SIM"
assert_ok "session lab + ctl login (owner)"   "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                   "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"

PID0=$(_mainpid)
HB0=$(_hb_of agt1)
[ -n "$PID0" ] && [ -n "$HB0" ] || die "98: baseline capture failed (pid=$PID0 hb=$HB0)"
log "98: baseline PID0=$PID0 HB0=$HB0"

# Black-hole the agent's ONLY current path to the broker it is ACTUALLY connected to (discovered, not
# assumed — see _broker_that_registered_agt1). Silent DROP = the partition instrument; the roster
# still names the other two voters, which is exactly the failover the redial watchdog exists to drive.
# PER-PEER cut (internal review F98-1): fault_partition_on is peer-agnostic and would black-hole every
# voter, leaving no failover target — the drill would then measure an unreachable cluster, not a
# bounded teardown. Cut exactly the agt1↔<connected voter> edge.
CUT_BROKER=$(_broker_holding_agt1) || die "98: /connz shows no broker holding agt1's client connection"
SURVIVOR=$(for _b in brk1 brk2 brk3; do [ "$_b" = "$CUT_BROKER" ] || { printf '%s' "$_b"; break; }; done)
log "98: /connz says agt1's client connection is held by $CUT_BROKER; cutting that edge (survivor probe: $SURVIVOR)"
assert_ok "inject: silent-DROP agt1 -> $CUT_BROKER:4222 ONLY (the other voters stay reachable)" \
          fault_partition_peer_on agt1 "$CUT_BROKER" 4222
assert_ok "inject self-proof A: agt1 -> $CUT_BROKER:4222 black-holes (rc 124, a PARTITION not an outage)" \
          fault_assert_blackholed agt1 "$CUT_BROKER" 4222
assert_ok "inject self-proof B: agt1 -> $SURVIVOR:4222 is STILL reachable (the cut is one edge, so a failover target exists)" \
          fault_assert_reachable agt1 "$SURVIVOR" 4222

# IMPACT first (internal review F98-2): prove the cut actually bit the LIVE connection before
# claiming a recovery. Without this the drill can pass vacuously on an edge the agent was not using —
# which is exactly what the first two runs did, and what this arm caught both times.
DEADLINE=$(( $(date +%s) + RECOVERY_BUDGET ))   # ONE absolute budget for impact AND recovery (F5.3)
assert_ok "IMPACT the cut hit the LIVE connection: /connz OBSERVES agt1 absent from $CUT_BROKER (it was present at baseline; an observation error does not count)" \
          poll_until "$(_budget_left)" 5 "agt1 connection leaves $CUT_BROKER" -- _conn_left_cut_broker
# REFRESH THE WATERMARK IN THE PARENT SHELL (external review F5.1). HB0 was captured before the connz
# discovery and the three injection asserts, so a heartbeat written through the still-healthy link had
# already moved past it and the recovery poll could pass without anything recovering. Re-read it now
# that the fault is proven to have bitten: heartbeats are stalled, so this is the last pre-recovery
# value. It MUST be a parent-shell assignment — poll_until runs its predicate in a subshell, so a
# refresh inside the predicate would never reach the assertions below.
HB0=$(_hb_of agt1)
[ -n "$HB0" ] || die "98: post-impact heartbeat watermark capture failed"
log "98: post-impact heartbeat watermark HB0=$HB0"

# The recovery assertion — the whole drill: heartbeat resumes THROUGH ANOTHER VOTER inside the WRITTEN
# budget. Pre-fix, the teardown could sit in nc.Close() for ~11min with systemd reading active/running.
# NB the DROP stays ARMED — recovery must come from failover to brk2/brk3, never from the cut healing.
# BUDGET NOTE (internal review F98-1): the ledger's ≤60s bound is measured from the moment nats.go
# DECLARES the disconnect (redialAfter is armed by DisconnectErrHandler). Under a silent DROP that
# declaration waits on NATS ping timeouts, so this poll budget deliberately covers detection + the
# bounded teardown; it is not a claim that ≤60s starts at injection.
assert_ok "RECOVERY heartbeat advances past the POST-IMPACT watermark within the SHARED ${RECOVERY_BUDGET}s budget (DROP still armed) — proves the product re-registered, not merely that a socket exists" \
          poll_until "$(_budget_left)" 5 "agt1 heartbeat advances past the post-impact watermark" -- _hb_advanced
assert_ok "RECOVERY landed on ANOTHER voter: /connz OBSERVES agt1 on a voter other than $CUT_BROKER (the cut stays armed, so it cannot be a heal)" \
          poll_until "$(_budget_left)" 5 "agt1 connection appears on a survivor" -- _registered_on_another_voter

# Classify WHICH allowed path recovered (three-way, never banded silently):
# origin: internal review F98-4 (S14) — the previous shape used `sh -c 'true'` / `[ -n "$PID1" ]`,
# i.e. two assertions that could not fail, inflating the PASS count while proving nothing; and it
# tried to detect the escalate self-exec via a " (deleted)" exe link, which an exec of the SAME
# on-disk path never produces. Classification is now a LOG (an honest record), and the escalate arm
# is detected the only way it is actually observable: the agent's own escalation log line.
PID1=$(_mainpid)
_escalated=no
_agt1_log | grep -q 'teardown WEDGED' && _escalated=yes
if [ "$PID1" = "$PID0" ]; then
    if [ "$_escalated" = yes ]; then
        log "98: RECOVERY-PATH same-PID SELF-EXEC (escalate ladder rebuild arm — the agent logged 'teardown WEDGED')"
    else
        log "98: RECOVERY-PATH same-PID in-process rebuild (the DESIGNED path; no escalation logged)"
    fi
else
    log "98: RECOVERY-PATH supervisor restart (PID $PID0 -> $PID1; escalated=$_escalated) — allowed under systemd, recorded not banded"
fi
# The one thing that IS an assertion here: whichever path ran, escalation must not have been needed
# on a plain single-broker cut. Escalation is the double-fault rung (poison could not reach the
# blocked layer); seeing it on this ordinary partition would mean the ladder's normal rungs failed.
assert_ok "recovery used the ladder's NORMAL rungs (no 'teardown WEDGED' escalation on a single-edge partition)" \
          sh -c '[ "$1" = no ]' _ "$_escalated"

assert_ok "heal: lift the DROP"                fault_partition_off agt1
assert_ok "steady state: agt1 stays ONLINE after heal (no flap loop)" \
          poll_until 60 5 "agt1 ONLINE post-heal" -- _online agt1

not_covered "[GAP #72] the WSS write-path black-hole arm (the live incident's actual shape: half-dead wss:// handshake under nats.go's nc.mu)" \
    "simcluster fronts no wss:// listener (no websocket block, no ws certs in the image) — the ws handshake write-path black-hole is NOT constructable on this stack today. This drill regresses the BOUNDED-TEARDOWN contract over nats:// only. Flip: give simcluster a wss:// front (listener + certs, an independent increment), add the ws black-hole arm, run it multiple rounds GREEN — the #72 ledger names this as the close condition." gap

drill_end
