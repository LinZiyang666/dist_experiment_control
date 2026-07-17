#!/bin/sh
# 40-drain-retire.sh — S6 (N=3, explore→pin): topology shrink via drain/retire on the real admin-socket
# membership plane. The #31-INDEPENDENT arms (drain round-trip, ops observability, reconcile --plan
# zero-write, the Tier-2 / machine-confirm safety negatives) stand every run; the retire SPINE is
# #31-INTERMITTENT (SB-40, plan §12): the grow-lock leak that blocks retire is best-effort and only
# SOMETIMES leaks, so the drill BRANCHES on the actual retire outcome — refused-with-grow-in-flight (after a
# [GAP #31] clear attempt) → a signature-guarded PRODUCT-RED (the #31 leak reproduced, blocking the spine);
# op-started → the GREEN spine (retire is NON-BLOCKING; it starts an op and returns `watch: cluster ops show
# <op>`, polled to RETIRED — or an `assert_bug #45` if it stalls at NATS_ROLLED_OUT).
#
# TRANSPORTS: drain/retire/ops/reconcile = broker LOCAL admin socket (callAdmin) → `dexec -u tether $LDR`.
# retire's F==0 typed-confirm is fed by /opt/sim/pty-confirm.py. FALSE-GREEN GUARDS (plan §9-40): drain
# round-trip asserts the intermediate DRAINING was OBSERVED before --abort; the retire double-removal is
# proven by an independent CONTROL-SOURCE WRITE on the surviving cluster (raft has no operator reader).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
A()   { dexec -u tether "$LDR" -- tether cluster "$@"; }
CTL() { "$SIM" ctl -- "$@"; }
_drain_ok() { dexec -u tether "$LDR" -- tether cluster drain "$1" >/dev/null 2>&1; }
_abort_ok() { dexec -u tether "$LDR" -- tether cluster drain "$1" --abort >/dev/null 2>&1; }
# drain sets the node's ROSTER phase to DRAINING (clusterdrain.go:147) — read it from cluster status, NOT
# from `ops show` (which returns the join op with phase="", nested under .op).
_is_draining() { "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.node_id==$n and (.phase=="DRAINING" or .phase=="DRAIN"))' >/dev/null 2>&1; }
_is_voter()    { "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.node_id==$n and .phase=="VOTER" and .reachable==true)' >/dev/null 2>&1; }
# Never pin operation reads to a leader variable across the injected leadership
# transfer. The mid-retire leader can be the target itself; after it transfers
# leadership and is removed, its local admin socket/DB is necessarily stale.
_op_show_json() {
    _osj_ldr=$(sim_leader) || return 1
    dexec -u tether "$_osj_ldr" -- tether cluster ops show "$1" --json
}
# schema helpers (call A + jq DIRECTLY — never dexec inside sh -c). Diag-verified: ops ls --json =
# {schema:"cluster_ops",schema_version:1,ops:[…]}; ops show --json = {schema:"cluster_op",schema_version:1,op:{…}}.
_ops_ls_schema()   { A ops ls --json 2>/dev/null | jq -e '.schema=="cluster_ops" and .schema_version==1' >/dev/null 2>&1; }
_ops_show_schema() { _op_show_json "$1" 2>/dev/null | jq -e '.schema=="cluster_op" and .schema_version==1' >/dev/null 2>&1; }
# reconcile --manual --plan on a multi-user clustered conf REQUIRES --account-issuer + --broker-nkey (the
# auth block has >1 nkey user so it can't auto-read) — a KEPT guard, exercised as a refusal (SB-40 finding).
_recon_needs_args() { A reconcile nats --manual --plan 2>&1 | grep -qiE 'need --account-issuer|--broker-nkey'; }
_ret_has_note()     { printf '%s' "$RET_OUT" | grep -q 'NOT a credential revocation'; }
_not_in_roster() { ! "$SIM" status --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.node_id==$n)' >/dev/null 2>&1; }
# Stage-C M4: the old oracle (session ls && node ls) was TWO READS labeled a WRITE. A real WRITE = a session
# create that COMMITS through raft (only possible if the surviving config is coherent — no phantom voter).
_ctl_write_ok() { "$SIM" ctl -- session create "wtest$(date +%s)$$" --pin 246810 >/dev/null 2>&1; }
_file_fingerprint() { "$SIM" exec "$LDR" -- sh -c 'stat -c "%n:%s:%Y" /etc/tether/nats.d/nats.conf /etc/tether/nats.d/nats.conf.bak.* 2>/dev/null | sort'; }
_prepare_dryrun_joiner_secrets() {
    secrets_mint_node "$INSTANCE" dry4 || return 1
    _pds_src=$(_secrets_node_dir "$INSTANCE" dry4)
    _pds_ctr=$(ctr_name "$NL")
    dexec "$NL" -- rm -rf /tmp/dry4-secrets || return 1
    dexec "$NL" -- mkdir -p /tmp/dry4-secrets || return 1
    d cp "$_pds_src/." "$_pds_ctr:/tmp/dry4-secrets" >/dev/null || return 1
    dexec "$NL" -- sh -c 'chown -R tether:tether /tmp/dry4-secrets && chmod 700 /tmp/dry4-secrets && chmod 600 /tmp/dry4-secrets/*-key.pem /tmp/dry4-secrets/*.nk && chmod 644 /tmp/dry4-secrets/*-cert.pem /tmp/dry4-secrets/cluster-ca.pem'
}
_plan_json_ok() { [ "$PLAN_RC" = 0 ] && printf '%s' "$PLAN_JSON" | jq -e '.schema=="takeover_natsconf_plan" and .schema_version==1 and (.routes|length)==3 and (.auth_users|length)==3 and (.dry_run|length)>0' >/dev/null 2>&1; }
# Stage-C M4: roster-absence alone is satisfied at PlanClusterNodeRemove (during the transition INTO
# NATS_ROLLED_OUT, controller:758-767) BEFORE terminal RETIRED (:768-778) — a stalled retire could false-green.
# Require terminal RETIRED (op_state) AND roster-absence; a NATS_ROLLED_OUT stall times out → honest RED.
_retired()      { _op_show_json "$1" 2>/dev/null | jq -e '((.op.op_state // .op.state // "")|ascii_upcase)|test("RETIRED")' >/dev/null 2>&1; }
_retire_done()  { _retired "$1" && _not_in_roster "$1"; }
_retire_gone()  { poll_until 150 5 "$1 terminal RETIRED + gone from roster" -- _retire_done "$1"; }
_op_state_is()  { _op_show_json "$1" 2>/dev/null | jq -e --arg s "$2" '(.op.op_state // "")==$s' >/dev/null 2>&1; }
_retire_pre_remove() { _op_show_json "$1" 2>/dev/null | jq -e '(.op.op_state // "")|test("NO_NEW_HOME|REHOME_EXPOSES|STREAMS_AT_TARGET|SEED_WITHDRAWN")' >/dev/null 2>&1; }

drill_begin "S6-40 drain/retire: round-trip + ops + reconcile-plan + safety negatives + #31 retire gate (N=3)"

grow_to_3 1 1 0 || setup_fail "grow_to_3 failed (single attempt; no product-failure laundering)"
LDR=$(sim_leader) || setup_fail "no leader"
T=$(a_non_leader_voter) || setup_fail "no non-leader voter"
log "leader=$LDR  drain/retire target T=$T"
assert_setup "create session fixture" "$SIM" session "$SID" --pin "$PIN"
assert_setup "join expose-capable agent fixture" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
# DIAGNOSTIC dumps (explore→pin: capture the REAL stdout so the oracles pin the actual strings).
log "DIAG drain $T →"; A drain "$T" 2>&1 | sed 's/^/[diag drain] /'
log "DIAG ops show $T --json →"; A ops show "$T" --json 2>&1 | head -c 400 | sed 's/^/[diag opshow] /'; echo
A drain "$T" --abort >/dev/null 2>&1 || true
poll_until 30 3 "$T back to VOTER (post-diag)" -- _is_voter "$T" || warn "diag: $T not back to VOTER"

# ── D-drain round-trip (VOTER→DRAINING→VOTER); plain drain checks assertNoActiveOp, NOT growActiveJoiner ─
assert_ok "D-drain: cluster drain $T succeeds (rc=0)" _drain_ok "$T"
assert_ok "D-drain FG: intermediate DRAINING phase OBSERVED before --abort" \
    poll_until 20 2 "$T reaches DRAINING" -- _is_draining "$T"
assert_ok "D-drain: cluster drain $T --abort succeeds (rc=0)" _abort_ok "$T"
assert_ok "D-drain: $T back to VOTER after --abort" poll_until 30 3 "$T back to VOTER" -- _is_voter "$T"

# ── RED drain --retire redirect (Hidden REMOVED → steers to cluster retire) ──────────────────────────
assert_refuses "drain --retire is REMOVED → steers to cluster retire" \
    "is removed.* run .*cluster retire|cluster drain --retire.* is removed|removed" \
    dexec -u tether "$LDR" -- sh -c "tether cluster drain $T --retire 2>&1"

# ── OPS-OBS: schemas (dump the real --json first) ────────────────────────────────────────────────────
log "DIAG ops ls →"; A ops ls 2>&1 | head -c 300 | sed 's/^/[diag opsls] /'; echo
log "DIAG ops ls --json →"; A ops ls --json 2>&1 | head -c 300 | sed 's/^/[diag opslsjson] /'; echo
assert_ok "OPS-OBS: ops ls --json schema=cluster_ops v1" _ops_ls_schema
assert_ok "OPS-OBS: ops show --json schema=cluster_op v1" _ops_show_schema "$T"

# ── RECONCILE-plan: --manual --plan requires --account-issuer/--broker-nkey on a multi-user conf + zero-write ─
MD5_0=$("$SIM" exec "$LDR" -- md5sum /etc/tether/nats.d/nats.conf 2>/dev/null | awk '{print $1}')
assert_ok "RECONCILE-plan: --manual --plan on a clustered (multi-user) conf REQUIRES --account-issuer/--broker-nkey (KEPT guard)" _recon_needs_args
assert_ok "RECONCILE-plan: zero-write (nats.conf md5 unchanged after the --plan attempt)" \
    sh -c "[ \"\$($SIM exec $LDR -- md5sum /etc/tether/nats.d/nats.conf 2>/dev/null | awk '{print \$1}')\" = '$MD5_0' ]"

# Enter the real renderer with every issuer/nkey/peer input. Pin both machine output and the human footer,
# then compare the complete conf+.bak size/mtime fingerprint before/after (not only nats.conf bytes).
ACCT=$(secrets_account_pub "$INSTANCE") || setup_fail "cannot derive account issuer"
BRK=$(secrets_broker_pub "$INSTANCE" "$LDR") || setup_fail "cannot derive leader broker nkey"
set -- reconcile nats --manual --plan --json --server-name "$LDR" --route-url "nats://$LDR:6222" \
    --account-issuer "$ACCT" --broker-nkey "$BRK" --secrets-dir /etc/tether/secrets
for _peer in $(list_nodes broker); do
    [ "$_peer" = "$LDR" ] && continue
    set -- "$@" --peer "$(peer_triple "$INSTANCE" "$_peer")"
done
FP0=$(_file_fingerprint)
PLAN_JSON=$(A "$@" 2>/dev/null); PLAN_RC=$?
assert_ok "RECONCILE-plan full renderer: rc=0, schema v1, all 3 routes/auth users, dry_run result present" \
    _plan_json_ok
set -- reconcile nats --manual --plan --server-name "$LDR" --route-url "nats://$LDR:6222" \
    --account-issuer "$ACCT" --broker-nkey "$BRK" --secrets-dir /etc/tether/secrets
for _peer in $(list_nodes broker); do [ "$_peer" = "$LDR" ] && continue; set -- "$@" --peer "$(peer_triple "$INSTANCE" "$_peer")"; done
assert_ok "RECONCILE-plan human footer says NOTHING WAS WRITTEN and gives leader-last restart order" \
    sh -c "out=\$(\"$SIM\" exec '$LDR' -- runuser -u tether -- tether cluster $* 2>&1); printf '%s' \"\$out\" | grep -q 'NOTHING WAS WRITTEN' && printf '%s' \"\$out\" | grep -qi 'leader LAST'"
FP1=$(_file_fingerprint)
assert_ok "RECONCILE-plan full renderer leaves nats.conf and the entire .bak set byte/mtime-identical" \
    sh -c "[ '$FP0' = '$FP1' ]"

# cluster apply is plan-only but must refuse a non-authoritative follower view before rendering copy/paste steps.
NL=$(list_nodes broker | grep -vx "$LDR" | head -1) || setup_fail "no non-leader for apply refusal"
assert_setup "APPLY-plan fixture: write a valid desired roster on $NL" \
    "$SIM" exec "$NL" -- sh -c 'printf "nodes:\n  - node_id: brk1\n    desired: voter\n  - node_id: brk2\n    desired: voter\n  - node_id: brk3\n    desired: voter\n" > /tmp/roster.yaml'
assert_refuses "APPLY-plan on non-leader refuses a non-authoritative roster view" \
    "authoritative LEADER view|non-leader view|Re-run .*leader" \
    dexec -u tether "$NL" -- tether cluster apply -f /tmp/roster.yaml

# `cluster add --dry-run` still resolves the live leader and identity, but must not acquire the grow
# marker, create an operation, create raft/, or touch the DB/config. Use a non-member target and compare
# every local durable input around the real command. The command runs on a stand-in container, so install
# a real dry4 joiner secret tree in /tmp: reusing $NL's route cert makes the product correctly reject the
# nats://dry4 route SAN (#24) and is an invalid positive fixture.
assert_setup "ADD-dryrun fixture: install a CA-signed dry4 route certificate with SAN=DNS:dry4" _prepare_dryrun_joiner_secrets
DRY_ID=$(secrets_node_ident_pub "$INSTANCE" dry4) || setup_fail "derive dry-run node identity"
DRY_FP0=$("$SIM" exec "$NL" -- sh -c 'md5sum /var/lib/tether/tether.db /etc/tether/broker.yaml /etc/tether/nats.d/nats.conf; find /var/lib/tether/raft -maxdepth 2 -type f -printf "%p:%s:%T@\n" | sort')
assert_ok "ADD-dryrun: resolves and prints a grow plan for a not-yet-provisioned target" \
    dexec -u tether "$NL" -- env HOME=/var/lib/tether tether cluster add dry4 --dry-run \
    --raft-addr dry4:7400 --nats-route nats://dry4:6222 --tunnel-addr dry4:7000 --public-host dry4 \
    --node-ident-pub "$DRY_ID" --secrets-dir /tmp/dry4-secrets --nats-url "nats://$LDR:4222"
DRY_FP1=$("$SIM" exec "$NL" -- sh -c 'md5sum /var/lib/tether/tether.db /etc/tether/broker.yaml /etc/tether/nats.d/nats.conf; find /var/lib/tether/raft -maxdepth 2 -type f -printf "%p:%s:%T@\n" | sort')
assert_ok "ADD-dryrun: complete DB/config/raft fingerprint is unchanged" sh -c "[ '$DRY_FP0' = '$DRY_FP1' ]"

# ── OPS-CONFIRM/ABORT — genuine unreachable nonvoter, no synthetic operation-row writes ────────────
# AddNonvoter cannot wedge quorum. The absent target remains CATCHING_UP until the real adaptive deadline
# expires, then BLOCKED. confirm re-enters CATCHING_UP; abort makes it terminal and frees the target slot.
BLK=blocked4
BLK_BUNDLE=$(dexec -u tether "$NL" -- tether cluster join prepare --node-id "$BLK" --name "$BLK" \
    --raft-addr "$BLK:7400" --nats-route "nats://$BLK:6222" --tunnel-addr "$BLK:7000" \
    --public-host "$BLK" --secrets-dir /etc/tether/secrets 2>/dev/null | head -1) \
    || setup_fail "OPS fixture join prepare failed"
BLK_OUT=$(A join approve "$BLK_BUNDLE" 2>&1); BLK_RC=$?
BLK_OP=$(printf '%s' "$BLK_OUT" | sed -n 's/.*join operation \([^ ]*\) created.*/\1/p' | head -1)
[ "$BLK_RC" = 0 ] && [ -n "$BLK_OP" ] || setup_fail "OPS fixture approve did not create an operation: $BLK_OUT"
assert_ok "OPS-CONFIRM fixture: genuine unreachable nonvoter reaches BLOCKED after its catch-up deadline" \
    poll_until 175 5 "$BLK_OP BLOCKED" -- _op_state_is "$BLK_OP" BLOCKED
assert_ok "OPS-CONFIRM: confirm retries the BLOCKED join" A ops confirm "$BLK_OP"
assert_ok "OPS-CONFIRM: operation re-enters CATCHING_UP" poll_until 20 2 "$BLK_OP CATCHING_UP" -- _op_state_is "$BLK_OP" CATCHING_UP
assert_ok "OPS-ABORT: abort makes the in-flight operation terminal" A ops abort "$BLK_OP"
assert_ok "OPS-ABORT: op state is ABORTED" poll_until 20 2 "$BLK_OP ABORTED" -- _op_state_is "$BLK_OP" ABORTED
# A fresh PoP for the SAME target must now be admissible, proving the active slot was freed. Abort it
# immediately, then use the explicit raw escape to remove the staged nonvoter/roster residue.
BLK_BUNDLE2=$(dexec -u tether "$NL" -- tether cluster join prepare --node-id "$BLK" --name "$BLK" \
    --raft-addr "$BLK:7400" --nats-route "nats://$BLK:6222" --tunnel-addr "$BLK:7000" \
    --public-host "$BLK" --secrets-dir /etc/tether/secrets 2>/dev/null | head -1) \
    || setup_fail "OPS slot-free second join prepare failed"
BLK_OUT2=$(A join approve "$BLK_BUNDLE2" 2>&1); BLK_OP2=$(printf '%s' "$BLK_OUT2" | sed -n 's/.*join operation \([^ ]*\) created.*/\1/p' | head -1)
[ -n "$BLK_OP2" ] || setup_fail "OPS-ABORT did not free the same-target slot: $BLK_OUT2"
assert_ok "OPS-ABORT: same target admits a fresh operation after abort" A ops abort "$BLK_OP2"
assert_ok "OPS cleanup: raw force-remove the explicitly staged nonvoter" \
    dexec -u tether "$LDR" -- env TETHER_CONFIRM_NODE_ID="$BLK" tether cluster recovery node remove "$BLK" --manual --force --confirm-node-id "$BLK"
assert_ok "OPS cleanup: synthetic target is absent from the authoritative roster" poll_until 30 3 "$BLK absent" -- _not_in_roster "$BLK"

# ── SAFETY NEGATIVES (Tier-2 + machine-confirm; all #31-independent) ─────────────────────────────────
assert_refuses "SAFE: recovery node remove --yes rejected (NO --yes override, Tier-2)" \
    "NO --yes override|cannot run unattended" \
    dexec -u tether "$LDR" -- sh -c "tether cluster recovery node remove $T --manual --yes 2>&1"
assert_refuses "SAFE: recovery node remove WITHOUT --manual rejected (requires --manual)" \
    "requires --manual|routine path is .*cluster retire" \
    dexec -u tether "$LDR" -- sh -c "tether cluster recovery node remove $T 2>&1"
assert_refuses "SAFE: machine-confirm missing env → refused (double-factor)" \
    "type the node_id|confirm-node-id|aborted|TTY|type the" \
    dexec -u tether "$LDR" -- sh -c "TETHER_CONFIRM_NODE_ID= tether cluster recovery node remove $T --manual --confirm-node-id $T 2>&1"

# ── RETIRE SPINE — #31-INTERMITTENT: capture the actual retire outcome ONCE and branch (SB-40) ───────
log "RETIRE: attempting cluster retire $T (pty typed-confirm); #31 grow-lock leak is intermittent"
RET_OUT=$(dexec -u tether "$LDR" -- sh -c "python3 /opt/sim/pty-confirm.py $T -- tether cluster retire $T 2>&1")
log "RETIRE out →"; printf '%s\n' "$RET_OUT" | sed 's/^/[retire] /'
if printf '%s' "$RET_OUT" | grep -qiE 'grow of .* is in progress|membership operation .* in flight'; then
    # #31 LEAKED this run → retire blocked. Do not re-run cluster add or retry retire: a successful second
    # attempt would erase the first product failure and turn an intermittent defect into a false GREEN.
    log "#31 retire BLOCKED by the leaked cluster_grow_active lock [#31] (reproduced this run — intermittent leak)"
fi

if printf '%s' "$RET_OUT" | grep -qiE 'grow of .* is in progress|membership operation .* in flight'; then
    # The #31 grow-lock leak is REPRODUCED and it BLOCKS the retire spine — a real tether defect,
    # signature-matched on the captured retire output → PRODUCT-RED (not a silent NOT-COVERED-GREEN). The
    # retire spine's convergence half cannot proceed this run; OFFLINE de-cluster (drill 20) covers the
    # shrink mechanism.
    product_red "#31 grow-lock leak BLOCKS cluster retire [#31] — the first and only retire attempt matched 'grow of … is in progress / membership operation in flight'; retire spine blocked this run"
elif printf '%s' "$RET_OUT" | grep -qiE 'watch: .*cluster ops show|retir|removed'; then
    # retire STARTED (grow-lock released this run) → the GREEN spine is reachable. Poll the op to RETIRED.
    _as_pass "R-retire: cluster retire $T STARTED an op (grow-lock released this run — #31 intermittent, GREEN spine reachable)"
    # Kill the current leader only after a pre-removal cursor is durably visible. N=3 keeps quorum on the
    # other two; once a new leader is observed, immediately return the old voter so the resumed removal
    # never runs with only one future survivor. This proves operation state survives leadership change.
    OLD_LDR=$LDR
    if poll_until 45 2 "$T retire reaches a stable pre-removal cursor" -- _retire_pre_remove "$T"; then
        _as_pass "R-resume: captured a durable pre-RAFT_REMOVED retire cursor"
        node_kill "$OLD_LDR"
        if poll_until 45 2 "new leader after mid-retire kill" -- sh -c "n=\$($SIM status --json 2>/dev/null | jq -r '.leader_id // empty'); test -n \"\$n\" && test \"\$n\" != '$OLD_LDR'"; then
            LDR=$(sim_leader) || setup_fail "resolve new mid-retire leader"
            _as_pass "R-resume: leadership moved from $OLD_LDR to $LDR while the retire op remained in flight"
        else
            _as_fail "R-resume: no replacement leader after killing $OLD_LDR"
        fi
        node_start "$OLD_LDR"
        assert_setup "R-resume: old leader returns before irreversible removal" poll_until 60 3 "$OLD_LDR reachable VOTER" -- _is_voter "$OLD_LDR"
    else
        _as_fail "R-resume: retire skipped every observable pre-removal cursor; cannot safely inject leader kill"
    fi
    if _retire_gone "$T"; then
        _as_pass "R-retire: $T reaches terminal RETIRED + gone from roster (op driven to completion — grow-lock released + converged this run)"
    else
        # Stage-C M4 + #45: the retire STARTED but did NOT converge to terminal RETIRED within 150s — it
        # stalls at NATS_ROLLED_OUT (roster may be removed, but the op never reaches RETIRED). #45 is the
        # retire-convergence stall, distinct from #31 (grow-lock) and #37 (mid-retire-resume). EXPOSE it.
        OP_JSON=$(_op_show_json "$T" 2>/dev/null); OP_RC=$?
        OP_STATE=$(printf '%s' "$OP_JSON" | jq -r '.op.op_state // .op.state // empty' 2>/dev/null)
        OP_ERR=$(printf '%s' "$OP_JSON" | jq -r '.op.last_error // empty' 2>/dev/null)
        if [ "$OP_RC" = 0 ] && printf '%s %s' "$OP_STATE" "$OP_ERR" | grep -qiE 'DRAIN_REQUESTED|NO_NEW_HOME|REHOME_EXPOSES|STREAMS_AT_TARGET|SEED_WITHDRAWN|LEADER_TRANSFERRED|RAFT_REMOVED|NATS_ROLLED_OUT|BLOCKED|already in flight|still in flight'; then
            product_red "#45 retire convergence defect: the captured ops-show document remained state=$OP_STATE last_error=$OP_ERR after 150s; terminal RETIRED was not reached"
        else
            _as_fail "retire failed to converge but captured ops-show does not match #45 (rc=$OP_RC state=${OP_STATE:-missing} last_error=${OP_ERR:-missing})"
        fi
    fi
    assert_ok "R-retire PRIMARY oracle: control-source WRITE succeeds on the surviving cluster (raft coherent)" \
        poll_until 30 3 "ctl write ok post-retire" -- _ctl_write_ok
    # R-hint: the retire NOTE 'NOT a credential revocation' was captured in $RET_OUT (prints to STDERR,
    # cluster_retire.go:127-129). Assert from the captured output — no side-effecting second retire.
    assert_ok "R-hint: retire NOTE 'NOT a credential revocation' was printed (from captured retire output)" _ret_has_note
else
    _as_fail "RETIRE: unrecognized outcome (neither #31-blocked nor op-started) — investigate the RETIRE out dump"
fi

drill_end
