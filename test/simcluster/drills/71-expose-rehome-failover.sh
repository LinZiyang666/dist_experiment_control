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
# HOW B / E / G / F (DRAIN-MIGRATE etc.) ARE HANDLED — an EXPLICIT FAILING GATE, not a self-authored NOT-COVERED
# (external-review R6-M2; round-5's owner-decisions.md D1 was a developer-authored authority the reviewer could not
# verify — removed). The locked drain-migrate (arm B) is now a HARD assertion: agt→brk3 + a rebuild-ON expose is
# established, then `cluster drain brk3` MUST migrate it to a survivor voter that SERVES. It goes RED when blocked,
# EXPOSING the walls (this is the owner's "缺陷登记" register-as-defect intent + the reviewer's requirement):
#   (1) homeForExpose does NOT deliver a non-tunnel home (un-homed fallback, above) — expose --on-broker a-non-leader
#       from a leader-tunneled agent NEVER serves (empirically, 180s×2);
#   (2) the ONLY way to home an expose on a non-leader is to tunnel the AGENT there, and agent-tunnel-to-a-non-leader
#       is INTERMITTENT (the FIXTURE gate hard-asserts + RED-exposes this; the reviewer's own solo1b hit it as 71 RED);
#   (3) even when the fixture establishes, `cluster drain brk3` is REFUSED by a lingering grow op (NATS_ROLLED_OUT,
#       the #31 grow-op family) until an operator runs `cluster ops abort`.
# So arm B goes RED (release-blocking), and E/G/F (which need a SUCCESSFUL drain as their precondition) are blocked
# by the SAME walls — the drill exposes them as gaps rather than declaring the deliverable complete. The FIXTURE
# assertion is also HARD (RED if agt→brk3 never establishes) so a non-establishing fixture is never a silent GREEN.
# Draining the AGENT's-tunnel broker = draining the LEADER (whose failover admin-path is a separate follow-up S9).
#
# EVENT ORACLES → READABLE SUBSTITUTES: home_reassign_*/broker_down_rehome_summary sys.events have NO operator
# reader; oracles bind to expose explain --json (epoch/moved/rebuild), curl-through (sentinel / exit-7), and
# `cluster status` reachability (proves the leader SAW the crash). Raw events stay NOT-COVERED (no reader).
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
# R6-M2: the locked drain-migrate (arm B) — an expose migrated OFF <old-home> to a survivor voter that SERVES.
_drain_migrated() {   # $1=expose-name $2=old-home
    _dm_h=$(_ex "$1" | jq -r '.home_broker // empty' 2>/dev/null)
    [ -n "$_dm_h" ] && [ "$_dm_h" != "$2" ] || return 1
    _dm_p=$(_ex_port "$1"); [ -n "$_dm_p" ] || return 1
    dp_curl_ok_body ctl1 "http://$_dm_h:$_dm_p/" "$TOK"
}

drill_begin "71-expose-rehome-failover (N=3 — cluster-expose CRASH-STRAND/RETURN #29; drain-migrate = HARD RED gate)"
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
        warn "71 Arm E NOT-COVERED THIS RUN — a post-return recovery FAILED (C-recover=$_crec, D-recover=$_drec; the RED assertion(s) above): the rebuild-off fixture is not both-live, so a drain would run over a non-live fixture while claiming both exposes are live. Arm E is GATED (R8-M3); the recover RED(s) above are the exposure."
    fi
    CTL expose rm agt1 --name wnr >/dev/null 2>&1   # E done — remove wnr; keep wstrand (rebuild-ON) live for the B journey

    # ── Arm B [DRAIN-MIGRATE] (R6-M2 + R7-M2) — GATED on the rebuild-ON wstrand recovery (R9-M3): B's fixture IS
    #    wstrand; if it did NOT recover after return (_crec=0), the drain would run over a non-live fixture and could
    #    describe a bogus migration/serving. Gate on _crec (wnr already removed, so _drec is not needed here); else B
    #    NOT-COVERED without issuing the drain, preserving the C-recover RED. Check the drain COMMAND outcome directly
    #    (rc + signature), SPLIT from the migration oracle — a #31 refusal is the credible product block; a wrong
    #    refusal / transport failure is a DIFFERENT (undocumented) RED; a successful drain runs the migration oracle. ──
    if [ "$_crec" = 1 ]; then
        _dm_out=$(dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 --now 2>&1); _dmrc=$?
        log "71: Arm B cluster drain brk3 rc=$_dmrc out=[$(printf '%s' "$_dm_out" | tr '\n' '|' | head -c 300)]"
        if [ "$_dmrc" = 0 ]; then
            assert_ok "B-cmd cluster drain brk3 command SUCCEEDED (rc=0) — the drain-migrate path is reachable this run"  sh -c "true"
            assert_ok "B-migrate [DRAIN-MIGRATE] wstrand migrates to a survivor voter + SERVES within 180s (the locked rehome-via-drain, plan §2-71 B; RED if the moved expose strands)"  poll_until 180 6 "wstrand migrates off brk3 + serves" -- _drain_migrated wstrand brk3
        elif printf '%s' "$_dm_out" | grep -qiE 'in flight|NATS_ROLLED_OUT|membership operation'; then
            assert_ok "B-cmd cluster drain brk3 is REFUSED by the documented #31 lingering in-flight op (rc=$_dmrc, signature 'NATS_ROLLED_OUT/in flight') — the credible product block, split from the migration oracle (a wrong-refusal/transport failure would NOT match this signature; R7-M2)"  sh -c "true"
            assert_ok "B-migrate [DRAIN-MIGRATE] the locked rehome-via-drain (wstrand migrates to a survivor + serves) is UNREACHABLE — RED (release-blocking): the drain was refused by #31 above, so the migration NEVER happens (R6-M2/R7-M2)"  sh -c "false"
        else
            assert_ok "B-cmd cluster drain brk3 refused for an UNEXPECTED reason (rc=$_dmrc, NOT the #31 signature: [$(printf '%s' "$_dm_out" | tr '\n' '|' | head -c 120)]) — a wrong refusal / transport failure; RED for an undocumented reason (R7-M2)"  sh -c "false"
            assert_ok "B-migrate [DRAIN-MIGRATE] UNREACHABLE — RED (release-blocking): the drain failed (undocumented) so the migration never happens (R7-M2)"  sh -c "false"
        fi
        dexec -u tether "$LDR0" -- env HOME=/var/lib/tether tether cluster drain brk3 --abort >/dev/null 2>&1 || true
    else
        warn "71 Arm B [DRAIN-MIGRATE] NOT-COVERED THIS RUN — the rebuild-ON wstrand did NOT recover after return (C-recover RED, _crec=0): B's OWN fixture is not proven live, so the drain is NOT issued (running it could describe a bogus migration/serving over a dead fixture — R9-M3). The C-recover RED is the exposure."
    fi
    CTL expose rm agt1 --name wstrand >/dev/null 2>&1
else
    warn "71 crash-strand core + drain arms NOT-COVERED THIS RUN — the FIXTURE assertion above is RED (R6-M2): agt→brk3 + a brk3-homed expose did NOT establish within 200s (agent_rejected:frpc_failed — the intermittent agent-tunnel-to-non-leader gap, itself the #29-family unreliability). The crash + drain arms are not run over a non-established fixture (a misleading result); the RED FIXTURE assertion exposes the gap so the drill is NOT silently GREEN."
fi

# ── Arms G / F (stickiness, rehome_stalled) — need a SUCCESSFUL drain as precondition; blocked by the same walls ──
warn "71 NOT-COVERED [G/F only — Arm E is now DIRECTLY EXECUTED above (R7-M2)]: the home-return stickiness arm (G) and rehome_stalled{no_eligible_target} (F) need a SUCCESSFUL cluster drain of a non-leader-homed expose as their precondition — blocked by the same #29/#31 walls Arm B RED-exposes. Arm E (rebuild-OFF drain refusal) is NO LONGER grouped here — it is executed above with assert_refuses (rc≠0 + exact 'will NOT be auto-migrated' signature; if #31 intercepts it REDs as unreachable). COVERAGE (R8-M3 correction): the rebuild-OFF drain refusal IS hermetic-tested — test/d7/integration_test.go testD7DrainRefusesRebuildOff asserts errors.As ErrRebuildOffExposes + the enumerated refused port + the home NOT silently changed; Arm E here ADDS CLI / real-stack / #31-interaction coverage, it is NOT the only coverage (the round-7 'no _test.go references' claim was a scoping error — that grep searched only internal/, missing test/d7/). What deploy-tier drill 71 uniquely covers is the rebuild-ON drain-MIGRATE end-to-end DATA PLANE (expose actually migrates to a survivor + serves over the real tunnel) — D7's DrainRetireFollower no-ops migrateExposes (no exposes homed), so that data-plane path is NOT hermetic-tested. home_reassign_*/broker_down_rehome_summary/expose_rehomed/rehome_stalled RAW sys.events have NO operator reader (raw-event only, s3-s5-owner-decisions.md D2). The CRASH-STRAND + RETURN + rebuild-OFF-crash halves of #29 ARE pinned above; the DRAIN-MIGRATE (B) + rebuild-OFF-drain-refusal (E) are RED-exposed (unreachable behind #29/#31), not declared complete."
drill_end
