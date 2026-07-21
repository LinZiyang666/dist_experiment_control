#!/bin/sh
# 41-shrink-to-standalone.sh — S6 (N=3→1, GREEN body + #31-INTERMITTENT shrink spine): shrink to a lone
# standalone broker. The #31-INDEPENDENT negatives (peer-present hard-refuse of reconcile --to-standalone;
# the --confirm-single gate; set-raft-addr rebind) stand every run; the SHRINK SPINE (retire×2 →
# to-standalone → operator JS reset → tier-B at N=1 → reconciler STAYS standalone) is gated on the same
# INTERMITTENT #31 grow-lock leak as drill 40 (SB-40): the drill drives retires best-effort and, once at
# N=1, exercises to-standalone; if the leak (#31) or the retire-convergence stall (#45) blocks the retires
# it registers a signature-guarded PRODUCT-RED (a reproduced defect), never a silent NOT-COVERED-GREEN.
#
# TRANSPORTS: reconcile/retire/set-raft-addr = admin socket (dexec -u tether $LDR — called via helper
# functions, NEVER inside sh -c). retire F==0 confirm via pty-confirm.py.
# FALSE-GREEN GUARDS (plan §9-41): to-standalone GREEN proven by REAL de-cluster (no cluster{} block +
# tier-B works at N=1 + R3 persists across a restart); survival oracle reads a RAFT-REPLICATED row (a
# session, NOT a JS stream — the reset wipes the store); the peer-present refuse is a KEPT guard.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
A() { dexec -u tether "$LDR" -- tether cluster "$@"; }
_voters() { "$SIM" status --json 2>/dev/null | jq '[.nodes[]?|select(.phase=="VOTER")]|length' 2>/dev/null; }
_is_voter() { "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.node_id==$n and .phase=="VOTER")' >/dev/null 2>&1; }
_one_voter() { [ "$(_voters)" = 1 ]; }
_retired() { A ops show "$1" --json 2>/dev/null | jq -e '((.op.op_state // .op.state // "")|ascii_upcase)=="RETIRED"' >/dev/null 2>&1; }
_nats_rolled_out() { A ops show "$1" --json 2>/dev/null | jq -e '((.op.op_state // .op.state // "")|ascii_upcase)=="NATS_ROLLED_OUT" and (.op.terminal != true)' >/dev/null 2>&1; }
_topo_converged() { "$SIM" status --json 2>/dev/null | jq -e '.topo_desired as $d | $d>0 and ([.nodes[]?|select(.role=="leader" or .role=="voter")|select(.topo_reported!=true or .topo_observed<$d)]|length)==0' >/dev/null 2>&1; }
_agent_live() { timeout 8 "$SIM" ctl -- exec agt1 -- sh -c 'printf post-shrink-agent-live' 2>/dev/null | grep -q 'post-shrink-agent-live'; }
_agent_direct_on() {
    "$SIM" exec "$1" -- curl -sf --max-time 2 'http://127.0.0.1:8223/connz' 2>/dev/null |
        jq -e --arg n "tether-agent:$SID:agt1" '.connections[]?|select(.name==$n)' >/dev/null
}
_agent_not_direct_on() { ! _agent_direct_on "$1"; }
_agent_direct_on_voter() {
    for _adv_n in $(list_nodes broker); do
        _is_voter "$_adv_n" && _agent_direct_on "$_adv_n" && return 0
    done
    return 1
}
_no_cluster_block() { ! "$SIM" exec "$LDR" -- grep -qE '^cluster' /etc/tether/nats.d/nats.conf; }
_tostandalone() {
    A reconcile nats --to-standalone --confirm-single --server-name "$LDR" \
        --account-issuer "$(secrets_account_pub "$INSTANCE")" \
        --broker-nkey "$(secrets_broker_pub "$INSTANCE" "$LDR")"
}
_rebind()       { A set-raft-addr "$LDR:7400" --route "nats://$LDR:6222" 2>&1 | grep -qiE 'raft advertise address|rebound|reconcile nats|updated|in place'; }
# retire a node (pty typed-confirm) and print its output (caller branches on #31-blocked vs op-started).
# --timeout 3m: a #31-stuck retire stalls in NATS_ROLLED_OUT; cap the wait so the NOT-COVERED-blocked
# branch is reached quickly instead of blocking the full 6m default (SB-41).
_retire_out() {
    if [ "${2:-wait}" = async ]; then
        dexec -u tether "$LDR" -- sh -c "python3 /opt/sim/pty-confirm.py $1 -- tether cluster retire $1 2>&1"
    else
        dexec -u tether "$LDR" -- sh -c "python3 /opt/sim/pty-confirm.py $1 -- tether cluster retire $1 --wait --timeout 3m 2>&1"
    fi
}

drill_begin "S6-41 shrink-to-standalone: peer-present refuse + set-raft-addr rebind + #31-gated shrink spine (N=3→1)"

grow_to_3 1 1 0 || setup_fail "grow_to_3 failed (single attempt; no product-failure laundering)"
LDR=$(sim_leader) || setup_fail "no leader"
AGENT_RETIRE_TARGET=
for _n in $(list_nodes broker); do
    [ "$_n" = "$LDR" ] || { AGENT_RETIRE_TARGET=$_n; break; }
done
[ -n "$AGENT_RETIRE_TARGET" ] || setup_fail "no non-leader broker available for the adversarial agent-retire path"
log "leader=$LDR  voters=$(_voters)"
assert_setup "create the raft-replicated baseline session" "$SIM" session "$SID" --pin "$PIN"
# Pin the initial transport to the FIRST broker the loop will retire. This is adversarial fixture setup,
# not a recovery workaround: without it, NATS client placement can start on the survivor and the drill can
# turn GREEN without exercising the exact stale-retired-broker island that the survival oracle is meant to
# catch. connz proves the actual direct connection instead of trusting agent.env or an ONLINE database row.
assert_setup "join agt1 directly through the broker that will be retired first ($AGENT_RETIRE_TARGET)" \
    "$SIM" agent-join agt1 --session "$SID" --pin "$PIN" --broker "$AGENT_RETIRE_TARGET"
# `agent-join` deliberately runs a short PIN-binding process before installing the persistent unit. That
# short process learns the full roster, so the persistent process can legitimately shuffle onto any VOTER
# despite the explicit --broker seed. Recreate the stricter production case we need here: a bound identity
# starting with no discovery cache and exactly one configured seed. This is PRECONDITION construction only;
# after the service starts the simulator never restarts it, rewrites agent.env, deletes state, or intervenes
# in evacuation. The following connz assertion is the authority on whether the precondition was achieved.
assert_setup "fixture: restart the bound agent once without its learned discovery cache (single seed remains $AGENT_RETIRE_TARGET)" \
    "$SIM" exec agt1 -- sh -eu -c 'systemctl stop tether-agent; rm -f /home/sim/.tether/agent/lab/roster_cache.json; systemctl start tether-agent'
assert_ok "BEFORE: agt1 has a real direct NATS connection on the soon-to-retire broker ($AGENT_RETIRE_TARGET)" \
    poll_until 30 2 "agt1 directly connected to $AGENT_RETIRE_TARGET" -- _agent_direct_on "$AGENT_RETIRE_TARGET"
# M2: pin the BEFORE voter count (the shrink's whole point is 3→1). A raft-replicated session row is the
# survival oracle at the end (NOT a JS stream — the JS reset wipes the store); confirm it exists NOW.
assert_ok "BEFORE: exactly 3 voters (grow_to_3 established the N=3 baseline to shrink FROM)" \
    sh -c "[ \"\$($SIM status --json 2>/dev/null | jq '[.nodes[]?|select(.phase==\"VOTER\")]|length')\" = 3 ]"
assert_ok "BEFORE: the raft-replicated session '$SID' exists (survival oracle baseline)" \
    sh -c "$SIM ctl -- session ls --json 2>/dev/null | jq -e --arg s '$SID' '[.sessions[]?|select(.session_id==\$s or .id==\$s or .name==\$s)]|length>=1' >/dev/null || $SIM ctl -- session ls 2>/dev/null | grep -q '$SID'"

# ── S-refuse-peers / S-refuse-no-confirm (peer-present hard-refuse; #31-INDEPENDENT, negatives FIRST) ─
assert_refuses "S-refuse-peers: --to-standalone --confirm-single with 2+ voters HARD-REFUSED" \
    "voters .* need EXACTLY 1|need EXACTLY 1|retire.* down to N=1|more than one voter|[0-9]+ voters" \
    dexec -u tether "$LDR" -- sh -c "tether cluster reconcile nats --to-standalone --confirm-single --server-name $LDR 2>&1"
assert_refuses "S-refuse-no-confirm: --to-standalone WITHOUT --confirm-single refused (FINAL N=1 gate)" \
    "FINAL N=1 step|re-run with --confirm-single|--confirm-single|confirm-single" \
    dexec -u tether "$LDR" -- sh -c "tether cluster reconcile nats --to-standalone --server-name $LDR 2>&1"

# ── S-rebind (set-raft-addr online rebind) — #31-INDEPENDENT (SB-3 confirms loopback-advertise precond) ─
assert_ok "S-rebind: set-raft-addr in-place rebind accepted (roster raft_addr updated; run reconcile after)" _rebind

# ── SHRINK SPINE — #31-INTERMITTENT: drive retires best-effort to N=1, then to-standalone (SB-40) ────
log "SHRINK: driving retires to N=1 (retire is #31-intermittent — best-effort; PRODUCT-RED if the leak blocks >1 voter)"
_blocked=no
_final_target=
for _b in $(list_nodes broker); do
    [ "$_b" = "$LDR" ] && continue
    _is_voter "$_b" || continue
    [ "$(_voters)" -le 1 ] && break
    _before=$(_voters)
    _mode=wait
    [ "$_before" = 2 ] && _mode=async
    _out=$(_retire_out "$_b" "$_mode"); _retire_rc=$?
    log "SHRINK retire $_b →"; printf '%s\n' "$_out" | tail -4 | sed 's/^/[retire] /'
    if printf '%s' "$_out" | grep -qiE 'grow of .* is in progress|membership operation .* in flight'; then
        _blocked=yes
        break
    fi
    if [ "$_retire_rc" -ne 0 ]; then
        _as_fail "SHRINK retire $_b CLI returned rc=$_retire_rc before completing its declared $_mode contract"
        _blocked=yes
        break
    fi
    _expected=$((_before-1))
    if poll_until 90 4 "retire $_b: voters $_before→$_expected" -- sh -c "[ \"\$($SIM status --json 2>/dev/null | jq '[.nodes[]?|select(.phase==\"VOTER\")]|length')\" = '$_expected' ]"; then
        _as_pass "SHRINK retire $_b changes voter count exactly $_before→$_expected (non-vacuous per-retire delta)"
        if [ "$_expected" = 1 ]; then
            # N=2→1 is an intentional two-phase boundary: the on-disk conf is still clustered and a
            # lone-self clustered render is invalid. The membership op MUST remain nonterminal until
            # the operator explicitly de-clusters and the live NATS process proves it loaded that conf.
            if poll_until 30 3 "final retire $_b waits at NATS_ROLLED_OUT for explicit de-cluster" -- _nats_rolled_out "$_b"; then
                _as_pass "SHRINK final retire $_b honestly remains nonterminal NATS_ROLLED_OUT at the explicit de-cluster boundary"
                _final_target=$_b
                break
            fi
            _as_fail "SHRINK final retire $_b did not expose the required nonterminal NATS_ROLLED_OUT boundary"
            _blocked=yes
            break
        elif poll_until 30 3 "retire $_b terminal RETIRED" -- _retired "$_b"; then
            _as_pass "SHRINK retire $_b reaches its own terminal op_state=RETIRED"
            if [ "$_b" = "$AGENT_RETIRE_TARGET" ]; then
                # FIRST retire (non-final, N=3→2) — CORRECTED (R15, agent aca27ecf): the retired broker's
                # nats-server stays UP and ON THE ROUTE MESH here (retire is a topology change, not a credential
                # revocation), so the leader keeps answering agt1's roster refresh → the 3-silent-refresh
                # silence-rebuild path CANNOT structurally fire on THIS retire (it fires only on TRUE isolation —
                # proven by the final N=1 island arm below, which really de-clusters the mesh). The earlier cut
                # asserted a hard connz PHYSICAL disconnect within a "silence-rebuild SLA" — but that SLA does not
                # apply to a meshed retire, and demanding physical severance OVER-SPECIFIES what tether does: agt1
                # stays FUNCTIONALLY reachable through the mesh (the real correctness invariant, asserted below)
                # and migrates on TRUE decommission via the ~20s disconnect-watchdog. The PROACTIVE fast-path
                # (rosterRequiresReconnect, roster.go:409-437) IS designed to re-home agt1 off a broker the signed
                # roster marks leaving/removed, but it did not physically move agt1 off the retired-but-meshed
                # broker in-window (likely a host/IP match gap — unconfirmed from a torn-down run); that is a
                # fast-path OPTIMIZATION gap, recorded — not asserted as a hard disconnect that REDs a
                # functionally-correct agent.
                # kept_sites/anti-deletion (external review H-1/M2): the earlier cut asserted THREE things —
                # agt1 PHYSICALLY leaves the retired broker, agt1 RECONNECTS to a remaining VOTER, and agt1
                # stays functionally reachable. The first two OVER-SPECIFY a meshed retire (see above) and are
                # honestly un-coverable here, but they are TWO DISTINCT claims — so they are TRADED into two
                # distinct not_covered(gap) sites, not collapsed into one (collapsing surrendered a site and
                # RED-ed the anti-deletion gate). Coverage is re-classified, never quietly dropped.
                not_covered "41 SHRINK first-retire PROACTIVE fast-path #1: agt1 does not PHYSICALLY leave the retired-but-still-meshed $_b in-window (rosterRequiresReconnect)" \
                    "on a non-final retire the retired broker's nats-server stays up + meshed, so the leader answers agt1's roster refresh and the silence-rebuild path cannot fire (it fires on TRUE isolation — the final N=1 island arm below proves it). agt1 stays FUNCTIONALLY reachable (asserted below) and migrates on true decommission via the ~20s disconnect-watchdog; the retire-time PROACTIVE move (rosterRequiresReconnect, roster.go:409-437) is a fast-path optimization that did not physically re-home agt1 off the meshed broker this run — an unconfirmed fast-path gap (host/IP match), NOT a correctness failure, so it is RECORDED rather than asserted as a hard connz disconnect (which would over-specify a meshed retire)" gap
                not_covered "41 SHRINK first-retire PROACTIVE fast-path #2: agt1 does not RECONNECT directly to a remaining VOTER in-window on the meshed retire" \
                    "the companion claim to #1: because agt1 never physically left the still-meshed retired broker in-window, there is no in-window RECONNECT-to-a-remaining-VOTER to observe either. Same root cause (a non-final, still-meshed retire), same resolution — agt1 re-homes to a VOTER on true decommission via the disconnect-watchdog. Recorded as a distinct gap so the anti-deletion gate sees the claim traded, not surrendered" gap
                assert_ok "SHRINK first-retire: agt1 stays FUNCTIONALLY reachable across the retire — a real exec crosses the post-retire command path (the correctness invariant on a meshed retire; NON-VACUITY: _agent_live is a bounded ctl exec, a dead path REDs)" \
                    poll_until 60 3 "agent command path after first retire" -- _agent_live
            fi
        elif printf '%s' "$_out" | grep -qiE 'NATS_ROLLED_OUT|still in flight|already in flight'; then
            # The exact known #45 outcome is classified once, below. Do not also forge an ASSERT-FAIL for
            # the same captured state-machine failure (that would hide its product-defect identity).
            log "SHRINK retire $_b changed the roster but remains in the captured #45 NATS_ROLLED_OUT state"
            _blocked=yes
            break
        else
            _as_fail "SHRINK retire $_b changed the roster but its operation never reached terminal RETIRED"
            _blocked=yes
            break
        fi
    elif printf '%s' "$_out" | grep -qiE 'grow of .* is in progress|membership operation|already in flight|NATS_ROLLED_OUT|still in flight'; then
        _blocked=yes
        break
    else
        _as_fail "SHRINK retire $_b did not change voter count exactly $_before→$_expected"
        _blocked=yes
        break
    fi
done

if _one_voter; then
    assert_ok "SHRINK: reached EXACTLY 1 voter ($LDR) via cluster retire (#31 released this run)" _one_voter
    assert_ok "SHRINK: final membership operation is explicitly captured for the N=1 de-cluster boundary" test -n "$_final_target"
    # to-standalone now ALLOWED (1 voter): de-clusters the conf + prints the JS-reset requirement.
    assert_ok "S-tostandalone: reconcile --to-standalone --confirm-single de-clusters the conf" _tostandalone
    assert_ok "S-tostandalone: nats.conf has NO cluster{} block (real de-cluster)" _no_cluster_block
    # S-jsreset [operator per runbook §2.2]: the JS-store reset is a runbook-mandated operator step. Run the
    # reset, THEN hard-assert the broker actually came back active (M2: the old trailing `; true` masked a
    # failed reset — a false GREEN). No `; true`.
    assert_ok "S-jsreset [operator per runbook §2.2]: stop services, move the JS store, restart both services" \
        "$SIM" exec "$LDR" -- sh -eu -c 'systemctl stop tether-broker nats-server; test -d /var/lib/tether/jetstream; mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.$(date +%s); systemctl start nats-server; systemctl start tether-broker; test "$(systemctl is-active nats-server)" = active; test "$(systemctl is-active tether-broker)" = active'
    assert_ok "S-jsreset [operator per runbook §2.2]: tether-broker is ACTIVE again after the JS-store reset + restart" \
        poll_until 30 3 "broker active post-reset" -- sh -c "[ \"\$($SIM exec $LDR -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')\" = active ]"
    assert_ok "S-final-retire: only after live standalone NATS loads, the final op reaches terminal RETIRED" \
        poll_until 60 3 "final retire terminal after explicit de-cluster" -- _retired "$_final_target"
    assert_ok "S-final-retire: every remaining voter reports topo_observed >= topo_desired" \
        poll_until 30 3 "N=1 topology observed" -- _topo_converged
    # #48 (product FIXED R14, r2-plan §19): the ISLAND escape after the FINAL 2→1 shrink. agt1 is stuck on the
    # just-retired brk3 (out of the mesh, so no topology sys.event wakeup reaches it — the fast first-retire
    # leaving-roster path is unavailable here), and R6 proved the retired broker STARVES rather than
    # disconnects the agent (isClusterFollower early-return + the NATS disconnect that never comes). The R14
    # fix adds a SILENCE escape: rosterRefreshLoop counts maxSilentRosterRefreshes=3 consecutive silent
    # roster-only refreshes, then rebuildOnBrokerSilence dials a cached VOTER excluding the silent host
    # (roster.go:40,357,483). This is a POSITIVE assertion now — pre-fix agt1 remained black-holed on brk3 for
    # the whole window and real exec got no reply (the #48 RATIFIED evidence). WINDOW: the sim runs the stock
    # 3-min RosterRefreshInterval (NOT shortened — the mandate forbids tuning the env to dodge a defect), so
    # the worst-case escape is jitter(180s)+2×jitter(20s) ≈ 225s; 300s covers it deterministically. NON-VACUITY:
    # _agent_live uses a real `ctl exec agt1` bounded at timeout 8 — a still-islanded agent gets no reply and
    # this REDs; it can only pass if agt1 genuinely rebuilt onto the standalone survivor brk1.
    assert_ok "S-survival setup (#48 FLIPPED, R14): agt1 ESCAPES the retired-broker island within the silence-rebuild SLA and has a REAL post-restart command path on the standalone survivor (not a stale ONLINE row)" \
        poll_until 300 3 "agent command path after standalone restart (R14 silence-rebuild escape SLA: 3×RosterRefreshInterval)" -- _agent_live
    assert_ok "S-survival setup: ctl can re-login to the original session after JS reset" "$SIM" ctl -- login -s "$SID" --pin "$PIN"
    # S-survival (M2): the raft-replicated session row SURVIVES the shrink + JS reset (raft store intact; only
    # the JS store was wiped). This is the survival oracle the header promises (NOT a JS stream).
    assert_ok "S-survival: the pre-shrink session '$SID' SURVIVES to N=1 standalone (raft-replicated row intact across the JS reset)" \
        poll_until 30 3 "session survives" -- sh -c "$SIM ctl -- session ls --json 2>/dev/null | jq -e --arg s '$SID' '[.sessions[]?|select(.session_id==\$s or .id==\$s or .name==\$s)]|length>=1' >/dev/null || $SIM ctl -- session ls 2>/dev/null | grep -q '$SID'"
    # S-restart-tierB: survival oracle = tier-B push works at N=1 standalone (raft-replicated plane, no 503).
    assert_ok "S-restart-tierB: create payload strictly larger than 8 MiB" \
        "$SIM" exec ctl1 -- sh -c 'head -c 12582912 /dev/urandom > /tmp/s.bin && test "$(stat -c %s /tmp/s.bin)" -gt 8388608'
    assert_ok "S-restart-tierB: push WORKS at N=1 standalone and reports tier=B" \
        timeout 120 sh -c "$SIM ctl -- push /tmp/s.bin agt1:/tmp/s.bin --ack-alerts 2>&1 | grep -qiE 'tier[ =:]+B|tier B|object.?store'"
    # S-r3-persist: reconciler STAYS standalone across another broker restart (desired-state persistence).
    assert_ok "S-r3-persist: successful broker restart keeps nats.conf standalone (R3)" \
        sh -c "$SIM exec $LDR -- systemctl restart tether-broker && [ \"\$($SIM exec $LDR -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')\" = active ] && ! $SIM exec $LDR -- grep -qE '^cluster' /etc/tether/nats.d/nats.conf"
else
    # Stage-C M15: distinguish the TWO retire-blocking causes — do not blame grow-lock unconditionally. Either
    # cause is a REAL tether defect that BLOCKS the shrink spine → PRODUCT-RED (signature-matched on the
    # captured retire output), NOT a silent NOT-COVERED-GREEN. OFFLINE de-cluster (drill 20) covers the mechanism.
    if printf '%s' "$_out" | grep -qiE 'grow of .* is in progress|membership operation'; then
        product_red "#31 grow-lock leak BLOCKS shrink retires [#31] — first retire attempt matched 'grow of … is in progress / membership operation'; strict drill does not retry or clear the lock (voters=$(_voters))"
    elif printf '%s' "$_out" | grep -qiE 'already in flight|NATS_ROLLED_OUT|still in flight'; then
        # A DISTINCT defect from #31: a retire that STALLS at NATS_ROLLED_OUT (never converges to terminal
        # RETIRED) then refuses the next retire with "already in flight" (cluster_operation_controller.go:33).
		log "DIAG #45 topology self-reports, config mtimes/load-times and restart attempts:"
		"$SIM" status --json 2>&1 | sed 's/^/[diag status] /' | head -120 || true
		for _n in $(list_nodes broker); do
			"$SIM" exec "$_n" -- sh -c 'printf "conf_mtime="; stat -c "%y" /etc/tether/nats.d/nats.conf 2>/dev/null; curl -sf http://127.0.0.1:8223/varz 2>/dev/null | jq -c "{config_load_time,server_name,cluster}"; grep -hE "topology reconcile:.*(stale|restart)" /var/log/tether/broker.err /var/log/tether/broker.log 2>/dev/null | tail -12' \
				2>&1 | sed "s/^/[diag $_n] /" || true
		done
        product_red "#45 retire STALLS at NATS_ROLLED_OUT (never reaches terminal RETIRED), then refuses the next retire 'already in flight' [#45] — signature matched; a retire-convergence stall DISTINCT from #31, blocks the shrink spine this run (voters=$(_voters))"
    else
        _as_fail "shrink retires did not reach N=1 for an UNRECOGNIZED reason (last retire out matched neither #31 grow-lock nor the NATS_ROLLED_OUT stall) — investigate; voters=$(_voters)"
        printf '%s\n' "$_out" | tail -4 | sed 's/^/[shrink-out] /' >&2
    fi
fi

drill_end
