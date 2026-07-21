#!/bin/sh
# 71-expose-rehome-failover.sh — S3 (N=3): the cluster-expose CRASH-STRAND / RETURN behaviour of gotcha #29
# (locked plan §2 arms A/C/D GREEN; B = a HARD failing drain-migrate gate, RED-exposing the walls; E/G/F blocked by
# the same walls, see below). gotcha #29 REFRAMED to the SOURCE-ACCURATE
# mechanism (external-review R5-M3, 2026-07-14, across 6 sim runs):
#   - homeForExpose (internal/broker/home.go:96-113) DELIVERS a NAMED home directive (BrokerAddr = the home's own
#     tunnel_addr + its cert pins) ONLY when the home is Eligible() with a non-empty CertFP; otherwise it returns
#     nil (home.go:105 logs "home not deliverable on expose; leaving un-homed"). EMPIRICALLY, for a
#     `--on-broker <non-tunnel voter>` in the sim it returns nil (the freshly-grown non-leader is NOT
#     expose-home-deliverable, even after 180s of retries — probe-onbroker + probe-drain P4, 2× strict). With an
#     un-homed directive the agent's AddProxy (tunnel_adapter.go:76-77, ""⇒fallback) dials its FIXED tunnel broker,
#     which then DENIES the REGISTER (token_unknown_or_revoked) because the committed row's home_broker != self.
#     NET OBSERVABLE: a cluster expose's data plane only comes up when its home == the agent's OWN tunnel broker.
#     This is NOT "the agent hardcodes its fixed tunnel by design" (the round-1..4 wording the source refutes — the
#     agent WOULD dial a named home if one were delivered); it is the un-homed-fallback → self!=home denial path.
#   - A CRASH (node_kill the home) STRANDS a regular expose CLUSTER-WIDE: rehome_events.go:52-53 "regular exposes
#     are NOT auto-rehomed on a crash — stranded until a drain/return". Dead on EVERY live voter (F5) with epoch
#     UNCHANGED, until the home RETURNS and the agent re-tunnels (same port / same epoch). rebuild-ON and
#     rebuild-OFF behave IDENTICALLY on a crash (both strand) — the rebuild distinction is DRAIN-only (NOT-COVERED).
#
# ── R9-D (2026-07-19): ARM B IS NO LONGER A "RED-EXPOSING WALL" — IT IS P1's DEPLOY-TIER VERIFIER ──────────────
# R8 landed two distinct changes that this drill had not caught up with, and BOTH are judged here, separately:
#
#   (a) rc SEMANTICS. `cluster drain` used to write port_allocations.home_broker through raft, tell nobody, and
#       print "drain <node> ok". It now refuses rc=0 while the data plane is behind: exit 75 (EX_TEMPFAIL) with
#       codeDataplaneNotConverged ("the control plane committed the rehome but N expose(s) have NOT confirmed
#       the new home yet"). That exit is CORRECT BEHAVIOUR, not a refusal and not a transport failure — the
#       rehome IS committed and the broker keeps re-delivering. The pre-R9-D drill classified it as
#       "refused for an UNEXPECTED reason" and went RED on the product's own fix.
#   (b) DELIVERY no longer depends on the agent reconnecting. The home directive is an ACTIVE push that the
#       broker RE-DELIVERS on a reconcile pass until the agent publishes an APPLIED ack (agent/home_push.go).
#       Before R8 the only carrier was the register reply, so a healthy, quiet agent could never learn its
#       expose had moved — the tunnel stayed pinned to the drained broker forever. That is P1.
#
# So arm B is now a POSITIVE assertion with three independent oracles, in increasing order of what they prove:
#   B-cmd     — the rc is honest (rc=0, or rc=75 with the EXACT authored dataplane_not_converged string).
#   B-migrate — the TERMINAL data-plane state: the expose is homed on a SURVIVOR voter and that broker's public
#               port returns the sentinel over the real tunnel. Never an intermediate "a directive was
#               published" artifact — a published directive that nobody applied is precisely the P1 bug.
#   B-silent  — the P1 PREMISE: it got there while agt1 NEITHER restarted NOR re-registered/rebuilt its NATS
#               session. Without this, one incidental reconnect would make the drill pass in BOTH the fixed
#               and the unfixed world (R8's hermetic test buys the same guarantee with a fake agent that is
#               structurally incapable of speaking; deploy-tier buys it by counting the agent's own journal).
#
# HOW E / G / F (the remaining drain arms) ARE HANDLED — an EXPLICIT FAILING GATE, not a self-authored NOT-COVERED
# (external-review R6-M2; round-5's owner-decisions.md D1 was a developer-authored authority the reviewer could not
# verify — removed). The rebuild-OFF drain refusal (arm E) stays a HARD assert_refuses. The walls the pre-R9-D
# header listed were:
#   (1) homeForExpose does NOT deliver a non-tunnel home (un-homed fallback, above) — expose --on-broker a-non-leader
#       from a leader-tunneled agent NEVER serves (empirically, 180s×2);
#   (2) the ONLY way to home an expose on a non-leader is to tunnel the AGENT there, and agent-tunnel-to-a-non-leader
#       is INTERMITTENT (the FIXTURE gate hard-asserts + RED-exposes this; the reviewer's own solo1b hit it as 71 RED);
#   (3) even when the fixture establishes, `cluster drain brk3` can be REFUSED by a lingering grow op
#       (NATS_ROLLED_OUT, the #31 grow-op family). R7a fixed #31's release-failure shape and R7b put the marker
#       under a lease+TTL with `cluster unlock` as the operator's out, so this is no longer the EXPECTED outcome
#       — but if it still fences the drain, arm B records it as a signature-guarded PRODUCT-RED against #31 and
#       declines to judge a migration that never ran (a command that did not execute proves nothing either way).
# G/F (which need a SUCCESSFUL drain of a NON-leader-homed expose as their precondition) remain gaps. The FIXTURE
# assertion is also HARD (RED if agt→brk3 never establishes) so a non-establishing fixture is never a silent GREEN.
# Draining the AGENT's-tunnel broker = draining the LEADER (whose failover admin-path is a separate follow-up S9).
#
# EVENT ORACLES → READABLE SUBSTITUTES + the #30 raw reader: oracles bind to expose explain --json (epoch/moved/
# rebuild), curl-through (sentinel / exit-7), and `cluster status` reachability (proves the leader SAW the crash).
# #30 ADDED the operator reader `tether admin events`, so Arm B now ALSO reads the drain's expose_rehomed sys.event
# back (on top of the data-plane migrate). broker_down_rehome_summary/rehome_stalled fire only from the crash /
# no-eligible-target fixtures G/F own (still gaps), so they are not read here.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/cluster.sh"; . "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CTL() { "$SIM" ctl -- "$@"; }
_ex()        { CTL expose explain "$1" --json 2>/dev/null; }
_ex_port()   { _ex "$1" | jq -r '.public_port // empty' 2>/dev/null; }
_ex_epoch()  { _ex "$1" | jq -r '.epoch // 0' 2>/dev/null; }
# create an expose, RETRYING until the agent's tunnel to its home broker is established (a fresh / just-restarted
# tunnel is not up instantly — agent_rejected:frpc_failed until it is). Rm any rolled-back partial before each retry.
# $1=agent $2=name $3=on-broker-home [$4=extra flag e.g. --no-rebuild]. Returns non-zero if it never establishes.
_expose_ready() {
    poll_until 200 6 "expose $2 on $1@$3 (waiting for the agent tunnel to establish)" -- \
        sh -c "\"$SIM\" ctl -- expose rm $1 --name $2 >/dev/null 2>&1; \"$SIM\" ctl -- expose $1 --local 8080 --name $2 --on-broker $3 ${4:-} 2>/dev/null"
}
# the leader marks a killed broker DOWN/unreachable — readable anti-vacuity the crash was SEEN. $1 = killed broker.
_leader_sees_down() {
    _lsd_l=$(sim_leader) || return 1
    [ "$(cluster_status_json "$_lsd_l" | jq -r --arg n "$1" '.nodes[]?|select((.node_id//.nid//.id)==$n)|.reachable' 2>/dev/null)" != true ]
}
# curl EVERY LIVE voter's :P — 0 iff ALL refuse (cluster-wide dead; F5: prove it did not rehome to ANY survivor).
_all_live_voters_refuse() {
    for _b in $(list_nodes broker); do
        node_running "$_b" || continue
        dp_curl_refused ctl1 "http://$_b:$1/" || return 1
    done
    return 0
}
_epoch_moved_unchanged() {   # $1=name $2=expected-epoch — epoch unchanged AND not moved (NO rehome happened)
    [ "$(_ex_epoch "$1")" = "$2" ] && [ "$(_ex "$1" | jq -r '.moved // false' 2>/dev/null)" = false ]
}
# ── R9-D arm-B oracles ────────────────────────────────────────────────────────────────────────────────────────
# _drain_dataplane_pending <rc> <captured-output> : the EXACT R8 rc semantics. BOTH halves are required —
#   • rc == 75 (exitcode.go:33 exitTransient; error_hints.go maps codeDataplaneNotConverged to it), and
#   • the authored sentence from ErrDataPlaneNotConverged.Error() (home_convergence.go:73).
# Matching only the string would accept any exit code (including a hypothetical rc=0 that merely PRINTED it);
# matching only rc=75 would accept every other transient (catch_up_stalled, quorum blips) as if it were this one.
_drain_dataplane_pending() {
    [ "${1:-}" = 75 ] || return 1
    printf '%s' "${2:-}" | grep -qE 'the control plane committed the rehome but .* have NOT confirmed'
}
# _agt_journal_count <agent> <since> <ere> : how many lines in the agent's OWN journal window match <ere>.
# Prints a number, or NOTHING when the journal is unreadable — callers treat empty as FAIL, never as zero.
_agt_journal_count() {
    dexec "$1" -- sh -c "journalctl -u tether-agent --since='$2' --no-pager 2>/dev/null | grep -cE '$3'" 2>/dev/null
}
_agt_mainpid() { "$SIM" exec "$1" -- systemctl show tether-agent -p MainPID --value 2>/dev/null; }
# _agt_silent_since <agent> <since> : the P1 PREMISE. Over the drain window the agent
#   (i)  DID receive + apply at least one ACTIVE home directive  ("home directives pushed" / "home applied-ack",
#        agent/home_push.go:64,95), and
#   (ii) did NOT re-establish its session even once ("re-registered after reconnect" proxy.go:503, "registered"
#        agent.go:665 — which fires on EVERY register incl. a restart's first one — or either rebuild line).
# (i) is NOT the claim; it is the ANTI-VACUITY GUARD. A missing/empty/unreadable journal would otherwise make
# (ii)'s "zero reconnects" trivially true, which is exactly the permanently-true-oracle family this suite keeps
# getting bitten by. The CLAIM the drill makes about the product is B-migrate (the terminal data plane); this
# predicate only certifies that the claim was earned WITHOUT a reconnect carrying the directive.
_agt_silent_since() {
    _ass_push=$(_agt_journal_count "$1" "$2" 'agent: home directives pushed|agent: home applied-ack')
    _ass_reg=$(_agt_journal_count "$1" "$2" 'agent: re-registered after reconnect|agent: registered|rebuilding NATS session|rebuilding session')
    log "71: agt-journal window since '$2' on $1 — active home-delivery lines=[${_ass_push:-UNREADABLE}] re-register/rebuild lines=[${_ass_reg:-UNREADABLE}]"
    [ -n "${_ass_push:-}" ] && [ "$_ass_push" -ge 1 ] 2>/dev/null || return 1
    [ -n "${_ass_reg:-}" ] && [ "$_ass_reg" -eq 0 ] 2>/dev/null
}
# R6-M2: the locked drain-migrate (arm B) — an expose migrated OFF <old-home> to a survivor voter that SERVES.
_drain_migrated() {   # $1=expose-name $2=old-home
    _dm_h=$(_ex "$1" | jq -r '.home_broker // empty' 2>/dev/null)
    [ -n "$_dm_h" ] && [ "$_dm_h" != "$2" ] || return 1
    _dm_p=$(_ex_port "$1"); [ -n "$_dm_p" ] || return 1
    dp_curl_ok_body ctl1 "http://$_dm_h:$_dm_p/" "$TOK"
}
# H9 (2026-07-18 full-suite run): a failed B-migrate used to leave NO terminal evidence, so the report could
# not separate the two very different failures _drain_migrated collapses into one `return 1`:
#   (a) the control plane never moved the expose at all — home_broker is STILL the drained brk3; or
#   (b) the control plane DID move it (home_broker changed) but the data plane never followed the move.
# Those have different owners (the drain/rehome write path vs the P1-family "raft written, data plane never
# executes" gap), and telling them apart needs exactly two facts: the TERMINAL `expose explain --json`, and
# whether the OLD home is still the one answering. Both are captured here, on the failure path only.
_b_migrate_forensics() {   # $1=expose-name $2=old-home
    log "71: B-migrate FAILED — TERMINAL expose explain $1 --json (does .home_broker still say $2? then the drain never rehomed it at all) →"
    "$SIM" ctl -- expose explain "$1" --json 2>&1 | sed 's/^/[b-migrate explain] /'
    _bmf_h=$(_ex "$1" | jq -r '.home_broker // "none"' 2>/dev/null)
    _bmf_p=$(_ex_port "$1")
    log "71: B-migrate FAILED — terminal home_broker=[${_bmf_h:-?}] public_port=[${_bmf_p:-?}] old_home=$2"
    _bmf_out=$(dexec ctl1 -- curl -sS -m 6 -o /dev/null -w 'http=%{http_code}' "http://$2:${_bmf_p:-0}/" 2>&1); _bmf_rc=$?
    log "71: B-migrate FAILED — curl against the OLD home $2:${_bmf_p:-?} → rc=$_bmf_rc out=[$_bmf_out] (rc=7 refused / rc=28 timeout = the old home stopped serving; a 200 here means the expose never left $2)"
    if [ -n "$_bmf_h" ] && [ "$_bmf_h" != "$2" ] && [ "$_bmf_h" != none ]; then
        _bmf_new=$(dexec ctl1 -- curl -sS -m 6 -o /dev/null -w 'http=%{http_code}' "http://$_bmf_h:${_bmf_p:-0}/" 2>&1); _bmf_nrc=$?
        log "71: B-migrate FAILED — the home DID change to $_bmf_h; curl against the NEW home $_bmf_h:${_bmf_p:-?} → rc=$_bmf_nrc out=[$_bmf_new] (a failure here = control plane moved it, data plane did not follow)"
    else
        log "71: B-migrate FAILED — the home did NOT change (still [${_bmf_h:-?}]): this is a control-plane non-rehome, NOT a data-plane strand"
    fi
}
# ── #30 operator event reader (admin events) ────────────────────────────────────────────────────────────
# The H.1 `events` JetStream stream (sys.events) had NO operator reader before #30: home_reassign_*/expose_rehomed/
# broker_down_rehome_summary were member-SUBSCRIBE only. `tether admin events` is that reader, over the root-only
# 0600 admin socket. Read on the leader $LDR0 (always alive — never the crash/drain victim brk3; the events stream
# is clustered so any live-quorum broker serves it). --json ⇒ ONLY machine JSON on stdout (R11), host jq parses it.
# reader ANCHOR: the operator reader reads a KNOWN-fired, non-rehome event (agent_registered for sid=lab/nid=agt1,
# broker.go:1341) — proves the reader WORKS in this drill before we pin the drain's rehome kind (non-vacuity).
_reader_reads_agent_registered() {
    _c=$(dexec -u tether "$LDR0" -- tether admin events --kind agent_registered -n 200 --json 2>/dev/null | jq '[.events[]?|select(.body.sid=="lab" and .body.nid=="agt1")]|length' 2>/dev/null)
    case "$_c" in ''|*[!0-9]*) return 1;; esac; [ "$_c" -ge 1 ]
}
# target: the Arm-B drain's expose_rehomed for wstrand OFF brk3 is READABLE (clusterdrain.go:806 emits it on a
# committed migrate; body {port,name,sid,from_broker,to_broker}). from_broker=brk3 (the drained home), name=wstrand.
_reader_reads_expose_rehomed() {
    dexec -u tether "$LDR0" -- tether admin events --kind expose_rehomed -n 200 --json 2>/dev/null \
        | jq -e '[.events[]?|select(.body.name=="wstrand" and .body.from_broker=="brk3" and .body.sid=="lab")]|length>=1' >/dev/null 2>&1
}
# SECRET-FREE: every expose_rehomed body carries ONLY {v,type,ts,port,name,sid,from_broker,to_broker} — topology
# fields, never a token/psk. jq returns true/false and never prints a body value. Requires >=1 wstrand event (non-vacuous).
_expose_rehomed_secretfree() {
    dexec -u tether "$LDR0" -- tether admin events --kind expose_rehomed -n 200 --json 2>/dev/null \
        | jq -e '([.events[]?|select(.body.name=="wstrand")]|length>=1) and all(.events[]?; ((.body|keys) - ["v","type","ts","port","name","sid","from_broker","to_broker"]|length)==0)' >/dev/null 2>&1
}

drill_begin "71-expose-rehome-failover (N=3 — cluster-expose CRASH-STRAND/RETURN #29; drain-migrate = P1/R8's deploy-tier verifier)"
"$SIM" nuke >/dev/null 2>&1 || true
# R5-M5: run grow_to_3 OUTSIDE assert_ok so its FIRST-CLASS evidence (GROW-ATTEMPTS trailer + per-attempt grow rc
# + any retry warning) is VISIBLE in the log (assert_ok captures + hides stdout/stderr on success), then assert the rc.
grow_to_3 1 1; _g3rc=$?
assert_ok "grow_to_3 + 1 agent + ctl (N=3 HA cluster; GROW-ATTEMPTS logged above)"  sh -c "exit $_g3rc"
_three_voters || die "71: grow_to_3 did NOT reach 3 VOTERs — foundational, refusing to continue"
assert_ok "session lab + ctl login (owner)"             "$SIM" session "$SID" --pin "$PIN"
# agt1 tunnels to the NON-leader brk3 (its exposes home on brk3 — the CRASH target). brk1 stays leader = admin.
assert_ok "agent-join agt1 (tunnel broker = brk3, a non-leader)"  "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent_provision_yaml agt1 (tunnel_addr=brk3)"  agent_provision_yaml agt1 "$SID" nats://brk3:4222 open
TOK=$(expose_serve_sentinel agt1 8080); [ -n "$TOK" ] || die "71: agt1 sentinel empty"
LDR0=$(sim_leader) || die "71: no leader"
log "71: leader=$LDR0 ; agt1 tunnel=brk3 (crash target)"

# ── Arm A — --on-broker <bogus> negative (refusal + no row written) ────────────
assert_refuses "A --on-broker <bogus> → on_broker_unknown" "on_broker_unknown" \
               "$SIM" ctl -- expose agt1 --local 8080 --name web-bogus --on-broker nosuchbroker
assert_ok "A2 no 'web-bogus' row after refusal (not just an error string)" \
          sh -c "! \"$SIM\" ctl -- ps -a --json 2>/dev/null | jq -e '.ports[]?|select(.name==\"web-bogus\")' >/dev/null"

# ── HARD-GATE the crash target is a LIVE NON-leader (R5-M3): brk3 must run AND not be the leader, else the crash
#    arms are invalid (killing the leader loses the admin observer; killing a dead node is vacuous). ──
assert_ok "GATE brk3 is a LIVE NON-leader (crash target valid: killing it keeps the leader $LDR0 alive as admin)"  sh -c "\"$SIM\" exec brk3 -- true >/dev/null 2>&1 && [ '$LDR0' != brk3 ]"
node_running brk3 || die "71: brk3 not running — invalid crash target"
[ "$LDR0" != brk3 ] || die "71: brk3 IS the leader — killing it would lose the admin observer; refusing"

# ── FIXTURE-ESTABLISHMENT GATE (R5-M3): try to establish agt→brk3 + a rebuild-ON expose (wstrand) AND a
#    rebuild-OFF expose (wnr), both homed on brk3, both serving. If agt→brk3 never establishes (the intermittent
#    agent-tunnel-to-non-leader gap), record NOT-COVERED-THIS-RUN + SKIP the crash arms (never run them over a
#    non-established fixture — that would record a misleading strand PASS/FAIL). ──
FIXTURE=0
if _expose_ready agt1 wstrand brk3 && _expose_ready agt1 wnr brk3 --no-rebuild; then
    PC=$(_ex_port wstrand); PD=$(_ex_port wnr)
    if [ -n "$PC" ] && [ -n "$PD" ] \
       && poll_until 40 3 "wstrand@brk3 serves" -- dp_curl_ok_body ctl1 "http://brk3:$PC/" "$TOK" \
       && poll_until 40 3 "wnr@brk3 serves"     -- dp_curl_ok_body ctl1 "http://brk3:$PD/" "$TOK"; then
        FIXTURE=1
    fi
fi
# R6-M2: a non-establishing fixture must NOT leave the drill GREEN with the locked crash core silently uncovered.
# HARD-assert the fixture established — RED (exposing the intermittent agent-tunnel-to-non-leader gap) when it did not.
assert_ok "FIXTURE agt→brk3 + a brk3-homed expose ESTABLISHED (required to test the locked crash-strand core) — RED if the intermittent agent-tunnel-to-non-leader gap prevented it (R6-M2: not a silent GREEN NOT-COVERED)"  sh -c "[ '$FIXTURE' = 1 ]"

if [ "$FIXTURE" = 1 ]; then
    EC0=$(_ex_epoch wstrand); ED0=$(_ex_epoch wnr)
    assert_ok "FIXTURE rebuild-ON wstrand ($PC) + rebuild-OFF wnr ($PD) both serve on brk3 (agt tunnel established; home data plane live pre-crash)"  sh -c "true"
    assert_ok "D-rebuild-off wnr explain shows rebuild:false (--no-rebuild PERSISTED — expose.go:217 rebuild = !RebuildOff)"  sh -c "[ \"\$(\"$SIM\" ctl -- expose explain wnr --json 2>/dev/null | jq -r '.rebuild')\" = false ]"

    # ── COMBINED CRASH (Arm C rebuild-ON crash-strand + Arm D rebuild-OFF crash, ONE injection — no 2nd crash race) ──
    assert_ok "C-kill node_kill brk3 (CRASH the home broker; quorum kept: brk1 leader + brk2)"  node_kill brk3
    assert_ok "C-detect leader $LDR0 marks brk3 DOWN/unreachable (readable proof the crash was SEEN)"  poll_until 60 3 "brk3 seen down" -- _leader_sees_down brk3
    # BOTH exposes strand cluster-wide (F5): rebuild-ON and rebuild-OFF alike (crash → no auto-rehome for either).
    assert_ok "C-strand [#29] rebuild-ON wstrand is dead on EVERY live voter's :$PC (curl exit 7 cluster-wide — NOT auto-rehomed to any survivor; F5)"  poll_until 30 3 "wstrand stranded cluster-wide" -- _all_live_voters_refuse "$PC"
    assert_ok "D-strand [#29] rebuild-OFF wnr is dead on EVERY live voter's :$PD (curl exit 7 cluster-wide — rebuild-OFF goes DOWN with its crashed home, IDENTICAL to rebuild-ON on a crash — the rebuild distinction is DRAIN-only)"  poll_until 30 3 "wnr stranded cluster-wide" -- _all_live_voters_refuse "$PD"
    assert_ok "C-strand-epoch wstrand epoch UNCHANGED ($EC0) + not moved — confirms NO rehome (a rehome would ++ epoch)"  _epoch_moved_unchanged wstrand "$EC0"
    assert_ok "D-strand-explain wnr explain is HONEST post-crash: still rebuild:false + NOT moved (no home_reassign) — rebuild-OFF never rehomes"  sh -c "[ \"\$(\"$SIM\" ctl -- expose explain wnr --json 2>/dev/null | jq -r '.rebuild')\" = false ] && [ \"\$(\"$SIM\" ctl -- expose explain wnr --json 2>/dev/null | jq -r '.moved // false')\" = false ]"

    # ── RETURN: brk3 comes back → the agent re-tunnels to its returned home → BOTH recover (same port / same epoch) ──
    assert_ok "C-return node_start brk3 (the crashed home RETURNS)"  node_start brk3
    poll_until 45 3 "brk3 back to VOTER" -- _phase_is brk3 VOTER || warn "71-C: brk3 slow back to VOTER"
    # R8-M3: capture BOTH post-return recoveries so Arm E is GATED on a genuinely-live rebuild-OFF + rebuild-ON
    # fixture (E must NOT run over a non-live fixture while its description claims both exposes are live).
    if poll_until 180 4 "wstrand@brk3 recovers" -- dp_curl_ok_body ctl1 "http://brk3:$PC/" "$TOK"; then _crec=1; else _crec=0; fi
    assert_ok "C-recover rebuild-ON wstrand recovers on the SAME brk3:$PC (agt1 re-tunnels to its returned home; SAME port)"  sh -c "[ '$_crec' = 1 ]"
    assert_ok "C-recover-epoch wstrand epoch STILL UNCHANGED ($EC0) after return — recovery was a home-RETURN, not a rehome"  sh -c "[ \"\$(\"$SIM\" ctl -- expose explain wstrand --json 2>/dev/null | jq -r '.epoch // 0')\" = '$EC0' ]"
    if poll_until 120 4 "wnr@brk3 recovers" -- dp_curl_ok_body ctl1 "http://brk3:$PD/" "$TOK"; then _drec=1; else _drec=0; fi
    assert_ok "D-recover rebuild-OFF wnr recovers on the SAME brk3:$PD after return (same-home recovery, like rebuild-ON — both strand-then-return on a crash)"  sh -c "[ '$_drec' = 1 ]"
    # ── Arm E [rebuild-OFF DRAIN REFUSAL] (R7-M2 + R8-M3) — GATED on BOTH recoveries; FAIL-CLOSED EXACT oracle ──
    #    wnr (rebuild:false) + wstrand both live on brk3 after return. `cluster drain brk3` MUST refuse with rc≠0 AND
    #    the EXACT rebuild-OFF signature "will NOT be auto-migrated" (clusterdrain.go:665). Use assert_refuses — it
    #    enforces BOTH a non-zero rc AND the signature — NOT an rc-blind substring (R8-M3: a rc=0 drain summary merely
    #    mentioning "rebuild-off" must NEVER pass). Run BEFORE removing wnr; the rebuild-ON wstrand B journey below is
    #    the positive control. If #31 (NATS_ROLLED_OUT) intercepts FIRST, the exact signature is unreachable →
    #    assert_refuses REDs ("refused, but NOT for …") — the honest exposure that the locked refusal is unreachable. ──
    if [ "$_crec" = 1 ] && [ "$_drec" = 1 ]; then
        assert_refuses "E [rebuild-OFF DRAIN REFUSAL] cluster drain brk3 (wnr rebuild-off + wstrand both live) MUST refuse with rc≠0 + the EXACT rebuild-OFF signature 'will NOT be auto-migrated' (clusterdrain.go:665, the locked control) — fail-closed exact oracle (R8-M3); RED if #31 intercepts first (exact refusal unreachable) or if it does not refuse"  "will NOT be auto-migrated"  dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 --now
        printf '%s' "$_AS_OUT" | grep -qiE 'in flight|NATS_ROLLED_OUT|membership operation' \
            && log "71: Arm E DIAGNOSTIC — the drain was intercepted by the #31 lingering in-flight op (rc=$_AS_RC, NATS_ROLLED_OUT) BEFORE reaching the rebuild-OFF check; the EXACT refusal is unreachable behind #31 (that IS the assert_refuses RED above, R8-M3), not a false pass"
        dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 --abort >/dev/null 2>&1 || true
    else
        not_covered "71 Arm E THIS RUN" "a post-return recovery FAILED (C-recover=$_crec, D-recover=$_drec; the RED assertion(s) above): the rebuild-off fixture is not both-live, so a drain would run over a non-live fixture while claiming both exposes are live. Arm E is GATED (R8-M3); the recover RED(s) above are the exposure. CLASS gap (R14 re-adjudication): this hole exists BECAUSE the #29-family recovery is not yet reliable, not a sim-timing valve — it turns GREEN when the product recovers the fixture, so it is debt owned by #29, not a re-run-and-it-lands runtime-guard." gap
    fi
    CTL expose rm agt1 --name wnr >/dev/null 2>&1   # E done — remove wnr; keep wstrand (rebuild-ON) live for the B journey

    # ── Arm B [DRAIN-MIGRATE — the P1 / R8 deploy-tier verifier] (R6-M2 + R7-M2, REWRITTEN R9-D) ────────────────
    #    GATED on the rebuild-ON wstrand recovery (R9-M3): B's fixture IS wstrand; if it did NOT recover after
    #    return (_crec=0), the drain would run over a non-live fixture and could describe a bogus migration.
    #    The drain COMMAND outcome (rc + signature) is judged SEPARATELY from the DATA-PLANE outcome, because
    #    R8 deliberately made them different questions — see the R9-D block in the header. ──
    if [ "$_crec" = 1 ]; then
        # R9-D: NO `--now`. `--now` collapses the convergence deadline to `now` (clusterstatus.go:776-779), so
        # the R8 gate is structurally unable to observe an ack and rc=75 becomes the ONLY reachable outcome —
        # the drill would be measuring its own flag rather than the product. Dropping it hands the gate its
        # designed 30s drain-notice budget, which makes the rc oracle genuinely TWO-SIDED (rc=0 is reachable).
        AGT_PID0=$(_agt_mainpid agt1)
        BSINCE=$(dexec agt1 -- date '+%Y-%m-%d %H:%M:%S' 2>/dev/null)
        [ -n "$BSINCE" ] || BSINCE="-10 min"
        log "71: Arm B pre-drain — agt1 tether-agent MainPID=[${AGT_PID0:-?}] journal window opens at [$BSINCE]"
        _dm_out=$(dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 2>&1); _dmrc=$?
        log "71: Arm B cluster drain brk3 rc=$_dmrc out=[$(printf '%s' "$_dm_out" | tr '\n' '|' | head -c 300)]"
        if [ "$_dmrc" = 0 ] || _drain_dataplane_pending "$_dmrc" "$_dm_out"; then
            if [ "$_dmrc" = 0 ]; then
                assert_ok "B-cmd [R8 rc] cluster drain brk3 returned rc=0 — under R8 that is a STRONG claim, not a bare exit: DrainNode only reaches its rc=0 tail once awaitHomeConvergence has seen every migrated expose's agent ACK the new home epoch (home_convergence.go:122-142). The pre-R8 rc=0 (raft written, nobody told) is the exact lie this arm exists to catch"  sh -c "[ '$_dmrc' = 0 ]"
            else
                assert_ok "B-cmd [R8 rc] cluster drain brk3 returned the DOCUMENTED rc=75 dataplane_not_converged with its authored signature — CORRECT behaviour, NOT a refusal and NOT a transport failure: the rehome IS committed and the broker keeps re-delivering, so the CLI declines to report success the data plane has not earned yet. Pre-R9-D this drill classified the product's own P1 fix as 'a wrong refusal' and went RED on it"  _drain_dataplane_pending "$_dmrc" "$_dm_out"
            fi
            # H9: capture the poll OUTCOME first, emit the terminal forensics on the failure path, THEN judge.
            # The forensics MUST run here and not later: the `cluster drain --abort` a few lines below mutates
            # the very state (home_broker / which broker serves) the evidence is about, so a post-abort capture
            # would describe the rollback rather than the failure. The judgment is byte-for-byte the same one
            # (RED unless the expose migrated off brk3 AND served) — assert_ok simply no longer runs the poll.
            if poll_until 240 6 "wstrand migrates off brk3 + serves on a survivor" -- _drain_migrated wstrand brk3; then _bmig=1; else _bmig=0; fi
            [ "$_bmig" = 1 ] || _b_migrate_forensics wstrand brk3
            assert_ok "B-migrate [P1 TERMINAL STATE — the core of this drill] wstrand is homed on a SURVIVOR voter AND that broker's public port returns the exact sentinel over the real tunnel, within 240s of the drain. This is the END state, never an intermediate 'a home directive was published' artifact — a directive nobody applied IS the P1 bug. RED here means the control plane moved the row and the data plane never followed; the terminal explain + old/new-home curl forensics logged above say WHICH of the two it was (H9)"  sh -c "[ '$_bmig' = 1 ]"
            AGT_PID1=$(_agt_mainpid agt1)
            assert_ok "B-noreexec agt1's tether-agent MainPID is UNCHANGED across the drain (was [${AGT_PID0:-?}], now [${AGT_PID1:-?}]) — the rehome was NOT carried by an agent restart, whose boot register would have delivered the new home the pre-R8 way"  sh -c "[ -n '${AGT_PID0:-}' ] && [ '${AGT_PID0:-x}' = '${AGT_PID1:-y}' ]"
            if [ "$_bmig" = 1 ]; then
                assert_ok "B-silent [P1 PREMISE] the migration above was delivered while agt1 stayed SILENT: its own journal for the drain window contains >=1 ACTIVE home-delivery line (home directives pushed / home applied-ack) and ZERO re-register/rebuild lines. Without this, one incidental NATS reconnect would let B-migrate pass in BOTH the fixed and the unfixed world — the reconnect, not the R8 push, would have carried the directive"  _agt_silent_since agt1 "$BSINCE"
                # #30 CLOSED: home_reassign_*/expose_rehomed sys.events had no operator reader — `admin events` is now
                # that reader. B-migrate just PROVED wstrand migrated OFF brk3 on the DATA plane, so the drain also
                # committed + emitted expose_rehomed (clusterdrain.go:806); assert the operator can now READ it back.
                assert_ok "B-event [#30] anchor: the operator reader (admin events) reads a KNOWN-fired non-rehome event (agent_registered sid=lab/nid=agt1) — proves the reader WORKS in this drill before pinning the rehome kind (non-vacuity)"  _reader_reads_agent_registered
                assert_ok "B-event [#30] admin events reads the drain's expose_rehomed for wstrand OFF brk3 (from_broker=brk3, name=wstrand, sid=lab) — the migration B-migrate just proved on the data plane ALSO surfaces as an operator-readable rehome event; #30 added this reader (was NOT-COVERED: raw sys.events had no operator reader)"  poll_until 20 2 "expose_rehomed for wstrand readable" -- _reader_reads_expose_rehomed
                assert_ok "B-event [#30] the expose_rehomed payload is SECRET-FREE (body keys ⊆ {v,type,ts,port,name,sid,from_broker,to_broker}; the reader relays only topology fields — never a token/psk)"  _expose_rehomed_secretfree
            else
                not_covered "71 B-silent [P1 premise] THIS RUN" "B-migrate is RED (the expose never reached a survivor that serves), so there is no delivery whose silence could be certified. Judging the premise over a migration that did not happen would attach a verdict to nothing. The B-migrate RED above is the exposure. CLASS gap (R14): tied to the #29-family migration not landing — debt that a fix retires, not intrinsic sim non-determinism." gap
            fi
            if [ "$_dmrc" != 0 ]; then
                # The rc=75 message DOCUMENTS a remedy: "re-run to keep waiting (the broker keeps re-delivering)".
                # Assert the remedy actually TERMINATES. NB this deliberately does NOT claim to re-prove
                # convergence (by now nothing is homed on brk3, so the re-run's migrate set is empty): what it
                # pins is that the documented operator loop ends in a completed drain instead of looping forever.
                assert_ok "B-rerun [R8 remedy] the re-run the rc=75 message tells the operator to do reaches rc=0 (the documented loop TERMINATES; it does not tell operators to retry something that can never succeed)"  dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3
                assert_ok "B-rerun-effect brk3 is actually in phase DRAINING after that rc=0 (the terminal control-plane state the exit code claims), read back from the leader's roster — not inferred from the exit code alone"  poll_until 30 3 "brk3 DRAINING" -- _phase_is brk3 DRAINING
            fi
        elif printf '%s' "$_dm_out" | grep -qiE 'in flight|NATS_ROLLED_OUT|membership operation'; then
            product_red "#31 acquire-lock window (owner: R7b lease+TTL / 'cluster unlock') — cluster drain brk3 was FENCED by a lingering in-flight membership op (rc=$_dmrc, signature NATS_ROLLED_OUT/in-flight/membership-operation). R7a fixed the release-failure shape and R7b put the marker under a 15m lease with an explicit operator clear, so this is no longer the expected steady state; reproducing it here is the registered defect surfacing, not a new one"
            not_covered "71 Arm B [P1 drain-migrate + the R8 rc semantics] THIS RUN" "the drain COMMAND never executed — it was fenced by the #31 in-flight membership op recorded PRODUCT-RED above. Nothing about the rehome data plane can be judged from a command that did not run, and inventing a migration failure for it would attribute a #31 fence to P1. The PRODUCT-RED is the exposure; re-run once membership is unfenced (tether cluster unlock) to reach the P1 verifier." gap
        else
            assert_ok "B-cmd cluster drain brk3 failed for an UNDOCUMENTED reason (rc=$_dmrc, matching NEITHER the R8 dataplane_not_converged signature NOR the #31 in-flight fence: [$(printf '%s' "$_dm_out" | tr '\n' '|' | head -c 160)]) — a wrong refusal / transport failure; the false-green guard trips rather than laundering an unknown failure into one of the two known ones"  sh -c "false"
            assert_ok "B-migrate [P1 TERMINAL STATE] UNREACHABLE — the drain failed for an undocumented reason above, so the migration never happens (RED, release-blocking)"  sh -c "false"
        fi
        dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 --abort >/dev/null 2>&1 || true
    else
        not_covered "71 Arm B [DRAIN-MIGRATE] THIS RUN" "the rebuild-ON wstrand did NOT recover after return (C-recover RED, _crec=0): B's OWN fixture is not proven live, so the drain is NOT issued (running it could describe a bogus migration/serving over a dead fixture — R9-M3). The C-recover RED is the exposure. CLASS gap (R14): the #29-family recovery not landing is a persistent product hole, not a re-runnable sim-timing valve — GREEN when the fixture recovers." gap
    fi
    CTL expose rm agt1 --name wstrand >/dev/null 2>&1
else
    not_covered "71 crash-strand core + drain arms THIS RUN" "the FIXTURE assertion above is RED (R6-M2): agt→brk3 + a brk3-homed expose did NOT establish within 200s (agent_rejected:frpc_failed — the intermittent agent-tunnel-to-non-leader gap, itself the #29-family unreliability). The crash + drain arms are not run over a non-established fixture (a misleading result); the RED FIXTURE assertion exposes the gap so the drill is NOT silently GREEN. CLASS gap (R14): #29 is an OPEN defect, so this is registered debt owned by #29 (it disappears when #29 lands), not an intrinsic sim-timing runtime-guard." gap
fi

# ── Arms G / F (stickiness, rehome_stalled) — need a SUCCESSFUL drain as precondition; blocked by the same walls ──
not_covered "71 [G/F only — Arm E is now DIRECTLY EXECUTED above (R7-M2)]: the home-return stickiness arm (G) and rehome_stalled{no_eligible_target} (F)" \
    "G (home-return stickiness) needs a drained-then-RETURNED home and F (rehome_stalled{no_eligible_target}) needs an N=1-eligible topology; neither is constructed by this drill, so both stay registered gaps rather than being folded into arm B. Arm E (rebuild-OFF drain refusal) is NOT grouped here — it is executed above with assert_refuses (rc!=0 + exact 'will NOT be auto-migrated' signature). Arm B is NO LONGER grouped here either: since R8 it is a POSITIVE assertion (P1's deploy-tier verifier), not a RED-exposed wall. COVERAGE (R8-M3 correction): the rebuild-OFF drain refusal IS hermetic-tested — test/d7/integration_test.go testD7DrainRefusesRebuildOff asserts errors.As ErrRebuildOffExposes + the enumerated refused port + the home NOT silently changed; Arm E here ADDS CLI / real-stack / #31-interaction coverage, it is NOT the only coverage (the round-7 'no _test.go references' claim was a scoping error — that grep searched only internal/, missing test/d7/). What deploy-tier drill 71 uniquely covers is the rebuild-ON drain-MIGRATE end-to-end DATA PLANE over the REAL tunnel, with the agent proven silent — D7's DrainRetireFollower no-ops migrateExposes (no exposes homed) and R8's own invariant test proves the same claim hermetically against a fake agent, so the real-stack half is drill 71's alone." gap
# #30 CLOSED: the home_reassign_*/expose_rehomed RAW sys.events no longer lack an operator reader — `tether admin
# events` added it, and Arm B above reads the drain's expose_rehomed back (anchored on agent_registered, secret-free)
# whenever the rebuild-ON migrate lands (_bmig=1). The remaining raw-event kinds (broker_down_rehome_summary /
# rehome_stalled) fire only from the crash / no-eligible-target fixtures that G/F own above (still gaps), not Arm B.
drill_end
