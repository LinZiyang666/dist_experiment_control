#!/bin/sh
# 74-rebalance-on-return.sh — S4 (N=3, G7a m11 sim leg): proxy __proxy__ home rebalance. Distribution oracle = a
# FAIL-CLOSED per-broker home COUNT from ONE validated `proxy status --json` snapshot (R5-M2 + R6-M1: rc/JSON/exactly
# 3 DISTINCT nids/real-voter homes). Builds the LOCKED one-per-voter (1/1/1) baseline via `cluster rebalance proxy`,
# proves ALL THREE homed exits FLOW bytes before skew (SETUP-ss-brkN), skews via a broker kill+return, checks
# default-off stickiness, manual `cluster rebalance proxy --dry-run`/real → even (command rc part of the oracle),
# HARD-drives the SS DATA PLANE through the rebalance-MOVED exit (B-dp), runs a NON-VACUOUS co-homed ordinary-expose
# NEGATIVE CONTROL (B-negctrl: created+validly-homed+serving before AND after a rc-checked rebalance), and the
# TETHER_AUTO_REBALANCE=on auto EFFECT on return. Closes m11 sim leg.
#
# external-review R6-M1 (round-5's measure-and-record accept-both REVERTED as a unilateral acceptance change):
# B-dp (moved-exit data-plane closure) and C-auto (auto-rebalance-on-return EFFECT) are LOCKED acceptance criteria
# and are HARD assertions — the drill goes RED (release-blocking) if the moved-exit data plane STRANDS or the auto
# path does NOT fire, EXPOSING the #33-family moved-exit stranding + the auto-rebalance gap. The no-reader carve-out
# (s3-s5-owner-decisions.md D2) covers ONLY the raw proxy_auto_rebalanced EVENT, NOT the data plane or the EFFECT.
# The false-success paths R6-M1 found are closed: snapshot requires 3 DISTINCT nids; dry-run/real rebalance require
# the command rc=0; the negative control is non-vacuous (no 'single' empty-explain compare).
#
# FALSE-GREEN GUARDS:
#  - distribution from a FAIL-CLOSED validated snapshot (invalid → spread 99 / unique DIST-INVALID sentinel; 3
#    DISTINCT nids required), not a status word and not empty-list-counts-as-zero.
#  - manual rebalance (dry-run AND real) requires the COMMAND rc=0 — a failed command can never pass on an unchanged
#    or naturally-converged distribution (R6-M1).
#  - moved-exit data plane (B-dp) + auto EFFECT (C-auto) are HARD — RED when they fail, never a warn/NOT-COVERED
#    (only the raw sys.events EVENT is NOT-COVERED, no operator reader).
#  - the negative control is created + validly homed on a real broker + serving before AND after (no 'single' fallback).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"; . "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/proxy.sh"; . "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
trap 'ss_down ctl1 2>/dev/null; for b in brk1 brk2 brk3; do ingress_down $b 443 2>/dev/null; done' EXIT INT TERM
CTL() { "$SIM" ctl -- "$@"; }
_setup_ingress_all() { ingress_trust_inject ctl1 || return 1; for b in brk1 brk2 brk3; do secrets_mint_ingress "$INSTANCE" "$b" && ingress_up "$b" 443 ingress "/sub/=127.0.0.1:8090" || return 1; done; }
_sub_token() { CTL proxy sub create --name "$1" 2>&1 | grep -oE '/sub/[A-Za-z0-9._-]+' | head -1 | sed 's#/sub/##'; }
_rebal() { "$SIM" exec "$(sim_leader)" -- runuser -u tether -- tether cluster rebalance proxy "$@" 2>&1; }
_elig_voters() { _rebal --dry-run | grep -oE '[0-9]+ eligible voter' | grep -oE '^[0-9]+' | head -1; }
_ge3_eligible() { _ev=$(_elig_voters); [ -n "$_ev" ] && [ "$_ev" -ge 3 ]; }
_construct_111() { _rebal >/dev/null 2>&1 || return 1; _spread_zero; }   # R7-M1: require the rebalance command rc=0 (not just the resulting spread) — a balanced snapshot must not be attributed to a FAILED command (lingering ops already refuse cluster commands)
# SS leg through the exit CURRENTLY homed on <broker>: re-fetch /sub (via brk1 ingress, always alive), start a
# VERIFIED-READY ss-local pinned to that exit, curl the RFC1918 sink. 0 iff bytes flow. Re-fetches each tick so a
# rebalance-MOVED exit that renders late is handled (same pattern as drill 73's _ss_leg_try).
_ss_via_home() {   # $1=broker whose homed exit to egress through, $2=local socks port
    _sv_a=$(CTL proxy status --json 2>/dev/null | jq -r --arg b "$1" '.nodes[]?|select(.home_broker==$b)|.nid' 2>/dev/null | head -1)
    [ -n "$_sv_a" ] || return 1
    dexec ctl1 -- pkill -f "ss-local .*-l $2 " >/dev/null 2>&1 || true
    ss_sub_fetch ctl1 "https://brk1/sub/$TOK_A" "/tmp/74-leg-$2.${INSTANCE:-x}.yaml" "$CA" || return 1
    ss_up ctl1 "/tmp/74-leg-$2.${INSTANCE:-x}.yaml" "$_sv_a" "$2" 2>/dev/null || return 1
    ss_curl_ok ctl1 "$2" "http://$SINK_IP:9090/" "$SINK_TOK"
}
# R7-M1: SS leg pinned to a SPECIFIC exit nid (not "whatever is homed on broker X") — used to baseline the EXACT
# exits' data plane immediately before the manual rebalance, so a later B-dp failure is attributable to THIS move
# (the moved exit is proven to have flowed right before it moved, per-nid not per-broker).
_ss_via_agent() {   # $1=exit nid to egress through, $2=local socks port
    dexec ctl1 -- pkill -f "ss-local .*-l $2 " >/dev/null 2>&1 || true
    ss_sub_fetch ctl1 "https://brk1/sub/$TOK_A" "/tmp/74-leg-$2.${INSTANCE:-x}.yaml" "$CA" || return 1
    ss_up ctl1 "/tmp/74-leg-$2.${INSTANCE:-x}.yaml" "$1" "$2" 2>/dev/null || return 1
    ss_curl_ok ctl1 "$2" "http://$SINK_IP:9090/" "$SINK_TOK"
}
_proxy_ready() { _n=$(CTL proxy status --json 2>/dev/null | jq '[.nodes[]?|select(.ready==true)]|length' 2>/dev/null); [ "${_n:-0}" -ge "$1" ]; }
EXPECT_EXITS="${EXPECT_EXITS:-3}"   # 3 agent exits over 3 voters (the locked one-per-voter fixture)
# FAIL-CLOSED snapshot (external-review R5-M2): capture ONE `proxy status --json` document, then VALIDATE it —
# command rc==0, valid JSON, EXACTLY EXPECT_EXITS agent rows, and EVERY home_broker in {brk1,brk2,brk3} (a real
# voter — an un-homed 'none'/'' or garbage is INVALID for a balance/spread computation). On ANY failure print a
# UNIQUE invalid sentinel (embedding a nanosecond stamp) + return non-zero, so spread/stability/dry-run comparisons
# FAIL CLOSED: an empty / malformed / partially-un-homed status can NEVER certify 'balanced' / 'stable' /
# 'zero-change' (the old _snap_homes returned empty on failure and _spread counted 0/0/0 → spread 0 → false GREEN).
_snap_homes() {
    _sh_j=$(CTL proxy status --json 2>/dev/null) || { printf 'SNAP-INVALID-cmd-%s\n' "$(date +%s%N 2>/dev/null)"; return 1; }
    printf '%s' "$_sh_j" | jq -e . >/dev/null 2>&1 || { printf 'SNAP-INVALID-json-%s\n' "$(date +%s%N 2>/dev/null)"; return 1; }
    _sh_rows=$(printf '%s' "$_sh_j" | jq -r '[.nodes[]?] | length' 2>/dev/null)
    [ "$_sh_rows" = "$EXPECT_EXITS" ] || { printf 'SNAP-INVALID-rows%s-%s\n' "${_sh_rows:-x}" "$(date +%s%N 2>/dev/null)"; return 1; }
    # R6-M1: require EXPECT_EXITS DISTINCT agent nids — 3 rows for the SAME nid (duplicate_nids_snapshot_accepted)
    # must NOT satisfy the count; a per-broker home count is only meaningful over distinct exits.
    _sh_uniq=$(printf '%s' "$_sh_j" | jq -r '.nodes[]?|.nid // "?"' 2>/dev/null | sort -u | grep -c .)
    [ "$_sh_uniq" = "$EXPECT_EXITS" ] || { printf 'SNAP-INVALID-nids%s-%s\n' "${_sh_uniq:-x}" "$(date +%s%N 2>/dev/null)"; return 1; }
    _sh_homes=$(printf '%s' "$_sh_j" | jq -r '.nodes[]?|.home_broker // "none"' 2>/dev/null)
    if printf '%s\n' "$_sh_homes" | grep -qvE '^brk[123]$'; then printf 'SNAP-INVALID-home-%s\n' "$(date +%s%N 2>/dev/null)"; return 1; fi
    printf '%s\n' "$_sh_homes"
}
# R9-M1/M2: a VALIDATED `nid=home` snapshot for pre-flow enumeration + before/after attribution. Mirrors _snap_homes'
# FAIL-CLOSED checks (cmd-rc / valid-JSON / exactly EXPECT_EXITS rows / EXPECT_EXITS DISTINCT nids / every home a real
# voter) but emits `nid=home` lines. Returns NON-ZERO + empty output on ANY empty/partial/malformed status, so a
# zero-iteration enumeration can NEVER leave a pre-flow gate flag =1 (the R9-M1 fail-OPEN hole) and a before/after
# diff can never rest on a transient partial view.
_snap_nidhome() {
    _snh_j=$(CTL proxy status --json 2>/dev/null) || return 1
    printf '%s' "$_snh_j" | jq -e . >/dev/null 2>&1 || return 1
    _snh_rows=$(printf '%s' "$_snh_j" | jq -r '[.nodes[]?]|length' 2>/dev/null)
    [ "$_snh_rows" = "$EXPECT_EXITS" ] || return 1
    _snh_uniq=$(printf '%s' "$_snh_j" | jq -r '.nodes[]?|.nid // "?"' 2>/dev/null | sort -u | grep -c .)
    [ "$_snh_uniq" = "$EXPECT_EXITS" ] || return 1
    printf '%s' "$_snh_j" | jq -r '.nodes[]?|.home_broker // "none"' 2>/dev/null | grep -qvE '^brk[123]$' && return 1
    printf '%s' "$_snh_j" | jq -r '.nodes[]?|"\(.nid)=\(.home_broker)"' 2>/dev/null
}
_count_in() { printf '%s\n' "$1" | grep -c "^$2$"; }   # $1=captured newline-list of homes, $2=broker
# single-broker count routed through the VALIDATED snapshot (R5-M2): -1 on an invalid snapshot so _ktgt_empty /
# _ktgt_loaded FAIL CLOSED (a failed status can never look like "rehomed away" or "loaded").
_count_on() { _co_snap=$(_snap_homes) || { echo -1; return 0; }; _count_in "$_co_snap" "$1"; }
# spread = max(home count) - min(home count) across the 3 brokers, from ONE VALIDATED snapshot. Invalid → 99
# (so _spread_le1 FAILS closed; a "balanced" claim can never rest on a failed/empty status).
_spread() {
    _sp_snap=$(_snap_homes) || { echo 99; return 0; }
    _mx=0; _mn=99
    for b in brk1 brk2 brk3; do _c=$(_count_in "$_sp_snap" "$b"); [ "$_c" -gt "$_mx" ] && _mx=$_c; [ "$_c" -lt "$_mn" ] && _mn=$_c; done
    echo $((_mx - _mn))
}
# _dist prints the per-broker counts from ONE validated snapshot; on invalid it prints the UNIQUE sentinel so two
# invalid snapshots NEVER compare byte-identical (→ _dist_stable / _rebalance_dryrun can't false-green "stable"/"zero").
_dist() { _di_snap=$(_snap_homes) || { printf 'DIST-INVALID-%s\n' "$_di_snap"; return 0; }; for b in brk1 brk2 brk3; do printf '%s=%s ' "$b" "$(_count_in "$_di_snap" "$b")"; done; echo; }
_spread_ge1() { [ "$(_spread)" -ge 1 ] && [ "$(_spread)" != 99 ]; }   # skew present (and NOT the invalid-99 sentinel)
_spread_le1() { [ "$(_spread)" -le 1 ]; }
_spread_zero(){ [ "$(_spread)" -eq 0 ]; }   # one-per-voter (max-min==0) — the locked 1/1/1 baseline
_ktgt_empty()  { [ "$(_count_on "$KTGT")" -eq 0 ]; }   # KTGT carries NO __proxy__ home (fail-closed: -1 on invalid ≠ 0)
_ktgt_loaded() { [ "$(_count_on "$KTGT")" -gt 0 ]; }   # KTGT carries >=1 __proxy__ home
# M6: KTGT is proxy-ELIGIBLE iff a rebalance --dry-run (a ZERO-mutation preview) PLANS a move ONTO it (the planner
# only targets deliverable + reachable voters, proxy_rebalance.go:196). Proving KTGT is eligible makes the
# default-off "stays empty" NON-vacuous: auto-rebalance COULD fire here if it were on, yet it does not.
_dryrun_targets_ktgt() { "$SIM" exec "$(sim_leader)" -- runuser -u tether -- tether cluster rebalance proxy --dry-run 2>/dev/null | grep -q -- "-> $KTGT "; }
# KTGT stays EMPTY across a QUIET window (6×5s = 30s) — default-off = STABLE no auto-migration, not merely "not yet".
_ktgt_stable_empty() { _sn=0; while [ "$_sn" -lt 6 ]; do [ "$(_count_on "$KTGT")" -eq 0 ] || return 1; sleep 5; _sn=$((_sn+1)); done; }
# _skew kills the home-HEAVIEST non-leader broker and waits for its exits to REHOME AWAY (its home count→0).
# STRICT causal oracle: NOT "spread>=1" (an uneven initial load-spread already satisfies that regardless of the
# kill), but "the killed broker actually lost its exits to survivors" — the real effect of the kill.
_skew() { node_kill "$KTGT"; poll_until 40 3 "$KTGT exits rehome AWAY (its home count→0)" -- _ktgt_empty; }
_rebalance_dryrun() {
    _b=$(_dist)
    _dro=$("$SIM" exec "$(sim_leader)" -- runuser -u tether -- tether cluster rebalance proxy --dry-run 2>&1); _drc=$?
    _a=$(_dist)
    log "74: B-dry rc=$_drc before=[$_b] after=[$_a] (rc MUST be 0 AND counts byte-identical — R6-M1)"
    # R6-M1: a FAILED dry-run command + an unchanged distribution must NOT pass (dryrun_command_failed_but_oracle_rc=0).
    [ "$_drc" = 0 ] && [ "$_a" = "$_b" ]   # command SUCCEEDED (rc 0) AND zero change
}
# distribution is STABLE when two reads 3s apart match — the post-return proxy reconcile must quiesce before we
# can attribute any dry-run delta to the dry-run itself (rather than to still-settling rehome).
_dist_stable() { _st=$(_dist); sleep 3; [ "$(_dist)" = "$_st" ]; }
# RE-RUN rebalance each tick: a just-restarted voter is raft-VOTER but may not be proxy-home-ELIGIBLE yet (it
# must re-register cert/public-host/health), and the rebalance planner SKIPS non-eligible homes
# (proxy_rebalance_test.go:167). So retry until the returned voter becomes eligible and the spread evens, or
# time out — a timeout would PIN a real "rebalance never re-homes onto a returned VOTER" gap (proxy-eligibility
# lags raft-VOTER indefinitely). Decides timing-recovery (GREEN) vs a structural gap (gotcha).
_rebalance_tick() {
    # R6-M1: require the manual rebalance command to SUCCEED (rc=0) — else a natural convergence would be
    # mis-attributed to a FAILED manual rebalance. A failed command → return 1 → the poll keeps retrying / times out.
    "$SIM" exec "$(sim_leader)" -- runuser -u tether -- tether cluster rebalance proxy >/dev/null 2>&1 || return 1
    _spread_le1
}
_rebalance_real() { poll_until 180 6 "distribution evens (spread<=1) once the returned voter is proxy-eligible" -- _rebalance_tick; }
# RETURN the killed broker: restart its container → it rejoins as a VOTER. Exits do NOT auto-move back to it
# (no auto-rebalance by default) — so the cluster is back to 3 voters but the distribution stays SKEWED (KTGT=0
# homes) until a manual `cluster rebalance proxy`. This is what makes the spread<=1 target reachable (rebalance
# can only spread across LIVE voters; without the return, KTGT stays dead at 0 homes and spread can never drop).
_return() { node_start "$KTGT"; poll_until 90 3 "$KTGT rejoins as VOTER (cluster back to 3)" -- _three_voters; }
# Arm C (Stage-C pin-3): TETHER_AUTO_REBALANCE=on auto path — a returned voter's exits auto-even WITHOUT the
# manual `cluster rebalance proxy` verb. Set the env via a broker-unit drop-in + restart (the real deploy shape).
_set_auto_rebalance() {
    for _b in $(list_nodes broker); do
        node_running "$_b" || continue
        dexec "$_b" -- sh -c 'mkdir -p /etc/systemd/system/tether-broker.service.d && printf "[Service]\nEnvironment=TETHER_AUTO_REBALANCE=on\n" > /etc/systemd/system/tether-broker.service.d/autorebal.conf && systemctl daemon-reload && systemctl restart tether-broker' >/dev/null 2>&1
    done
    poll_until 60 3 "cluster back to 3 VOTER after the env-restart" -- _three_voters
}
_env_on_all() { for _b in $(list_nodes broker); do node_running "$_b" || continue; dexec "$_b" -- systemctl show tether-broker -p Environment 2>/dev/null | grep -q 'TETHER_AUTO_REBALANCE=on' || return 1; done; }
_auto_tick() { _spread_le1; }   # NO manual verb — observe whether the auto path evens it out on its own
# NEGATIVE-CONTROL helpers (R6-M1): the ordinary expose 'reg' must be created + validly homed on a REAL broker +
# SERVING — no empty-explain 'single' fallback letting two missing results compare equal. Called as FUNCTIONS
# (not sh -c) so the command-sub inherits these + the globals NEG_TOK/EXH0/_regrc.
_reg_home()   { "$SIM" ctl -- expose explain reg --json 2>/dev/null | jq -r '.home_broker // empty' 2>/dev/null; }
_reg_port()   { "$SIM" ctl -- expose explain reg --json 2>/dev/null | jq -r '.public_port // empty' 2>/dev/null; }
_reg_serves() { _rp=$(_reg_port); _rh=$(_reg_home); [ -n "$_rp" ] && [ -n "$_rh" ] && dp_curl_ok_body ctl1 "http://$_rh:$_rp/" "$NEG_TOK"; }
# R6 FIX: the expose data plane takes a few seconds to come up after create — POLL for reg to become validly
# homed + serving (was a single immediate check that raced the data plane).
_reg_ready()    { _rh=$(_reg_home); printf '%s' "$_rh" | grep -qE '^brk[123]$' && _reg_serves; }
_negctrl_post() { [ "$(_reg_home)" = "${EXH0:-}" ] && [ -n "${EXH0:-}" ] && _reg_serves; }

drill_begin "74-rebalance-on-return (N=3 proxy home rebalance — G7a m11 sim leg)"
"$SIM" nuke >/dev/null 2>&1 || true
# R5-M5: run grow_to_3 OUTSIDE assert_ok so GROW-ATTEMPTS + per-attempt grow rc are VISIBLE (assert_ok hides them on success).
grow_to_3 3 1; _g3rc=$?
assert_ok "grow_to_3 + 3 agents + ctl (N=3 HA cluster; GROW-ATTEMPTS logged above)"  sh -c "exit $_g3rc"
_three_voters || die "74: grow_to_3 did NOT reach 3 VOTERs — foundational, refusing to continue"
assert_ok "session lab + ctl login (owner)"  "$SIM" session "$SID" --pin "$PIN"
for a in agt1 agt2 agt3; do
    assert_ok "agent-join $a"  "$SIM" agent-join "$a" --session "$SID" --pin "$PIN"
    assert_ok "agent_provision_yaml $a proxyprivate"  agent_provision_yaml "$a" "$SID" nats://brk1:4222 proxyprivate
done
assert_ok "SETUP ingress on all 3 brokers + trust ctl1 (for /sub cross-container SS legs)"  _setup_ingress_all
assert_ok "proxy on --yes (owner)"  sh -c "\"$SIM\" ctl -- proxy on --yes 2>&1 | grep -qi 'proxy ON'"
assert_ok "3 agent exits reach proxy-ready"  poll_until 40 3 "3 exits ready" -- _proxy_ready 3
SINK_TOK=$(bridge_serve_sentinel ctl1 9090); SINK_IP=$(node_bridge_ip ctl1)
TOK_A=$(_sub_token alice); [ -n "$TOK_A" ] || die "74: could not mint a sub token"; log "74: alice sub minted (len=${#TOK_A})"

# ── LOCKED 1/1/1 BASELINE (external-review R5-M2): the plan requires one exit PER VOTER (max-min==0) before the
#    destructive arms. The initial reconcile is NON-deterministic (drill 73's finding — it can pile exits on the
#    leader; the live run printed brk1=0 brk2=1 brk3=2), so CONSTRUCT 1/1/1 with `cluster rebalance proxy` once all
#    3 voters are proxy-ELIGIBLE, then ASSERT spread==0. A hard-gate: if 1/1/1 is not reachable the drill DIES (a
#    real one-per-voter setup/product gap), never runs the destructive arms over a skewed/invalid baseline. ──
# external-review R5-M2 (observe + expose): the NATURAL initial reconcile does NOT guarantee one-per-voter —
# OBSERVE + LOG it before constructing 1/1/1, so the record shows tether's own distribution (typically piled onto
# the leader / fewer-homes voters, spread>0), not a swept-under CONSTRUCTED baseline. This is the finding surfacing,
# not hidden: the plan's one-per-voter is a SETUP we build via rebalance (like drill 73 constructs its spread).
_nat=$(_dist); _natsp=$(_spread)
log "74: NATURAL initial __proxy__ distribution (before any rebalance) = [$_nat] spread=$_natsp $([ "$_natsp" = 0 ] && echo '(happened to be one-per-voter this run)' || echo '(NOT one-per-voter — tether initial reconcile piles; the locked 1/1/1 is CONSTRUCTED below, R5-M2)')"
poll_until 240 5 ">=3 proxy-eligible voters (all 3 recovered post-grow)" -- _ge3_eligible || die "74: fewer than 3 proxy-eligible voters after 240s — cannot construct the locked one-per-voter baseline (a MEASURED setup gap)"
assert_ok "SETUP-111 construct one-per-voter via cluster rebalance proxy (spread==0 — the locked 1/1/1 baseline; the non-deterministic initial reconcile can pile exits, so this is CONSTRUCTED not assumed)"  poll_until 120 5 "spread==0 (1/1/1)" -- _construct_111
_three_voters || die "74: not 3 voters after 1/1/1 construct"
log "74: 1/1/1 baseline distribution: $(_dist)"
# R6 finding: the proxy distribution DRIFTS back to piling on the tunnel broker — non-tunnel voters' (brk2/brk3)
# proxy-home-eligibility is UNSTABLE (gain it → get an exit → lose it → the exit re-piles on brk1). RE-CONSTRUCT
# 1/1/1 RIGHT before the destructive skew so KTGT is picked from a fresh balanced distribution; RED if it cannot be
# re-balanced (the #34 eligibility instability is total this run).
if poll_until 120 5 "re-1/1/1 before skew" -- _construct_111; then RECON=1; else RECON=0; fi
assert_ok "SKEW-reconstruct re-build 1/1/1 (spread==0, rc-checked rebalance) right before the destructive skew — RED if the proxy distribution can't be re-balanced (the #34 non-tunnel-voter proxy-eligibility instability). GATES the destructive arms (R7-M1: a failed foundation must NOT flow into misleading destructive PASS lines)"  sh -c "[ '$RECON' = 1 ]"
# R8-M1: the locked SKEW baseline is "one-per-voter PLUS every exit flowing" — reconstruction re-establishes the
# 1/1/1 DISTRIBUTION but can REMAP which exit is on each voter, so re-PROVE every exit's data plane flows ADJACENT to
# the skew (per-nid, via _ss_via_agent; the plan §3-74 ① three-legs-before-skew requirement, now a GATE). A failed
# pre-flow must NOT feed the destructive claims (round-8: a failed SETUP-ss leg still fed the SKEW/RETURN/A/B/C arms).
# R9-M1: enumerate over a VALIDATED snapshot (`_snap_nidhome`) — a fail-OPEN raw `jq '.nodes[].nid'` loop leaves
# RECON_FLOW=1 on an empty/partial status (zero iterations). Fail-CLOSED: an invalid snapshot → RECON_FLOW=0.
RECON_FLOW=1
_rnids=$([ "$RECON" = 1 ] && _snap_nidhome | cut -d= -f1)
if [ "$RECON" = 1 ] && [ -n "$_rnids" ]; then
    _rsport=1099
    for _rnid in $_rnids; do
        if poll_until 120 4 "SS via exit $_rnid flows (post-reconstruct)" -- _ss_via_agent "$_rnid" "$_rsport"; then _rf=1; else _rf=0; RECON_FLOW=0; fi
        assert_ok "SKEW-flow exit $_rnid flows bytes over the RECONSTRUCTED 1/1/1 baseline ADJACENT to the skew (the locked 'one-per-voter + every exit flowing' SKEW baseline over a VALIDATED snapshot, plan §3-74 ①; per-nid via _ss_via_agent — GATES the destructive arms, R8-M1/R9-M1)"  sh -c "[ '$_rf' = 1 ]"
        ss_down ctl1 2>/dev/null || true
        _rsport=$((_rsport+1))
    done
elif [ "$RECON" = 1 ]; then
    RECON_FLOW=0   # RECON passed but the status snapshot is empty/partial/malformed → fail-CLOSED RED (R9-M1)
    assert_ok "SKEW-flow-snapshot the SKEW baseline needs a VALIDATED proxy-status snapshot (exactly $EXPECT_EXITS distinct nids, all homes real voters) — RED + fail-closed when the status is empty/partial/malformed (R9-M1: a zero-iteration enumeration must NOT leave RECON_FLOW=1)"  sh -c "false"
else
    RECON_FLOW=0   # RECON already RED above (SKEW-reconstruct) — no extra assertion, avoid double-counting
fi
if [ "$RECON" != 1 ] || [ "$RECON_FLOW" != 1 ]; then
    log "74: post-reconstruct distribution: $(_dist)"
    warn "74 destructive arms (SKEW/RETURN/A/B/C) NOT-COVERED THIS RUN — the locked SKEW baseline (1/1/1 + every exit flowing) did NOT establish (RECON=$RECON, RECON_FLOW=$RECON_FLOW; the SKEW-reconstruct/SKEW-flow RED above). Per R7-M1/R8-M1 the destructive kill/rebalance arms are SKIPPED over an invalid/non-flowing baseline — NO misleading PASS lines over a failed foundation. The RED(s) above are the exposed #34 instability."
    ss_down ctl1 2>/dev/null || true
    drill_end; exit $?
fi
# ── Arm SKEW — kill the home-HEAVIEST non-leader broker (quorum kept 2/3) → its exits rehome AWAY ──
# pick the heaviest so the kill demonstrably moves exits (killing a 0-home broker would make rehome vacuous).
KTGT=""; _kmx=-1
for b in $(list_nodes broker); do
    [ "$b" = "$(sim_leader)" ] && continue
    [ "$b" = brk1 ] && continue   # R6 FIX: brk1 = the agents' TUNNEL broker (they provision nats://brk1); killing
                                  # it severs every agent's NATS control connection + breaks the whole proxy plane.
    _kc=$(_count_on "$b"); [ "${_kc:-0}" -gt "$_kmx" ] && { _kmx=$_kc; KTGT=$b; }
done
[ -n "$KTGT" ] || KTGT=brk2
[ "$KTGT" != brk1 ] || die "74: KTGT resolved to brk1 (the agents' tunnel broker) — refusing to kill it"
log "74: KTGT=$KTGT (home-heaviest non-leader, $_kmx homes); initial dist $(_dist)"
# self-review finding-10: die-gate the SKEW-precond — if the non-deterministic initial reconcile piled all exits on
# the leader, every non-leader broker has 0 homes → _kmx=0 → the _skew kill would run over a 0-home KTGT and its
# _ktgt_empty (home count→0) rehome-away oracle is trivially satisfied from the start (a vacuous destructive arm).
# assert_ok only records+continues, so hard-gate here: refuse the destructive kill if KTGT carries no home.
assert_ok "SKEW-precond KTGT=$KTGT started with ≥1 __proxy__ home (else the _ktgt_empty rehome-away oracle is vacuously satisfied; Stage-C pin-6 + self-review finding-10)"  sh -c "[ '${_kmx:-0}' -gt 0 ]"
[ "${_kmx:-0}" -gt 0 ] || die "74: SKEW-precond FAILED — KTGT=$KTGT carries 0 __proxy__ homes (initial reconcile piled all exits on the leader); refusing the destructive kill over a vacuous _ktgt_empty oracle (self-review finding-10)"
assert_ok "SKEW kill $KTGT → its exits rehome AWAY to survivors ($KTGT home count → 0)"  _skew
log "74: post-kill distribution: $(_dist)"

# ── Arm RETURN — restart $KTGT → rejoins as VOTER (cluster back to 3); exits do NOT auto-move back ──
assert_ok "RETURN restart $KTGT → rejoins as VOTER (cluster back to 3 voters)"  _return
log "74: post-return distribution: $(_dist)  (still skewed — $KTGT rejoined but carries 0 homes)"

# ── Arm A — default-off (external-review M6): the returned KTGT is proven proxy-ELIGIBLE (a dry-run PLANS a move
#    onto it), yet it STAYS at 0 homes across a full quiet window — proving default-off genuinely does NOT
#    auto-migrate, not merely "hasn't yet". The old assertion fired the INSTANT raft-VOTER was reached — before
#    KTGT was even proxy-eligible — so even a delayed default-on auto-rebalance would have passed it vacuously. ──
# eligibility recovery of a just-returned voter takes up to ~120s (cert/public-host re-register + §17 reachable
# observe) — the same window Arm B's _rebalance_real (poll_until 120) waits — so this poll must OUTLAST it (150s).
assert_ok "A-elig returned $KTGT becomes proxy-ELIGIBLE within 240s (rebalance --dry-run PLANS a move onto it → auto-rebalance COULD fire here if it were on; the dry-run mutates nothing). RED if the returned voter's proxy-eligibility recovery does NOT complete in a GENEROUS window — a real returned-voter-eligibility gap, not a too-short poll (R6)"  poll_until 240 5 "dry-run targets $KTGT" -- _dryrun_targets_ktgt
assert_ok "A default-off: $KTGT stays at 0 homes across a 30s quiet window (STABLE no auto-move-back without TETHER_AUTO_REBALANCE — not merely 'not yet'; M6)"  _ktgt_stable_empty

# ── Arm B — manual cluster rebalance proxy (dry-run zero-change + real evens + exits move BACK onto KTGT) ──
# settle first: the post-return rehome reconcile must quiesce, else a natural drift would masquerade as a
# dry-run side effect (and hide a genuine dry-run defect if there is one).
assert_ok "B-settle post-return proxy distribution stops drifting before measuring dry-run"  poll_until 45 3 "distribution stable" -- _dist_stable
BEFORE=$(_dist)
# R9-M1: prove EVERY exit (per-nid, over a VALIDATED snapshot) flows IMMEDIATELY before the move, and place the
# COMPLETE B injection (dry-run + real rebalance + effect/no-none + move-attribution + B-dp + negative control) BEHIND
# that aggregate gate — round-8's B_PREFLOW gated ONLY B-dp while B-dry/B-real still mutated the distribution over an
# invalid baseline. Fail-CLOSED enumeration via _snap_nidhome: an empty/partial status leaves B_PREFLOW=0, never 1.
B_PREFLOW=1; _preb_port=1096; NEG_OK=0
_pnids=$(_snap_nidhome | cut -d= -f1)
if [ -n "$_pnids" ]; then
    for _pnid in $_pnids; do
        if poll_until 90 4 "SS via exit $_pnid flows pre-B" -- _ss_via_agent "$_pnid" "$_preb_port"; then _pf=1; else _pf=0; B_PREFLOW=0; fi
        assert_ok "B-flow-pre exit $_pnid flows bytes before the manual rebalance (per-nid SEQUENTIAL probe over a VALIDATED snapshot, ~90s each; the fresh adjacent snapshot re-validates attribution right before B-real; whichever exit B moves onto $KTGT is proven live — GATES the WHOLE B injection, R8-M1/R9-M1/R10-M1)"  sh -c "[ '$_pf' = 1 ]"
        ss_down ctl1 2>/dev/null || true
        _preb_port=$((_preb_port+1))
    done
else
    B_PREFLOW=0
    assert_ok "B-flow-pre-snapshot the B pre-flow baseline needs a VALIDATED proxy-status snapshot (exactly $EXPECT_EXITS distinct nids, all homes real voters) — RED + fail-closed when the status is empty/partial/malformed (R9-M1: a zero-iteration enumeration must NOT leave B_PREFLOW=1)"  sh -c "false"
fi
if [ "$B_PREFLOW" != 1 ]; then
    warn "74 B injection (dry-run / real rebalance / B-move / B-dp / negative control) NOT-COVERED THIS RUN — the pre-flow baseline (VALIDATED snapshot + EVERY exit flowing) did NOT establish (B_PREFLOW=0; the B-flow-pre RED(s) above). The manual rebalance is NOT run over an invalid data-plane baseline, so it cannot mutate the distribution / contaminate the ordinary-expose or Arm-C fixtures (R9-M1). The pre-flow RED is the exposure; NEG_OK stays 0."
else
    # R10-M1: the FRESH adjacent attribution snapshot must be an EXECUTABLE gate. Capture _snap_nidhome WITHOUT a
    # pipeline — a `_snap_nidhome | tr` pipeline returns tr's rc (always 0), MASKING a failed helper (empty output).
    # Check BOTH its rc AND non-empty content, THEN transform. A failed fresh snapshot SKIPS the WHOLE B injection
    # (dry-run/real included), preserving a RED — round-9 ran B-dry/B-real over an empty attribution baseline.
    if _bh=$(_snap_nidhome) && [ -n "$_bh" ]; then
        BEFORE_HOMES=$(printf '%s' "$_bh" | tr '\n' ' ')
        assert_ok "B-snapshot the fresh attribution snapshot adjacent to the injection is VALIDATED (rc=0 + non-empty exact nid=home, captured WITHOUT a rc-masking pipeline) — R10-M1"  sh -c "true"
        log "74: B pre-rebalance per-agent homes (fresh, VALIDATED, adjacent to injection) = [$BEFORE_HOMES]"
        assert_ok "B-dry cluster rebalance proxy --dry-run: preview + ZERO change (home counts byte-identical)"  _rebalance_dryrun
        assert_ok "B-real cluster rebalance proxy: evens the distribution (spread<=1)"  _rebalance_real
        assert_ok "B-real-effect $KTGT now carries >=1 home again (rebalance moved exits BACK onto the returned voter)"  _ktgt_loaded
        assert_ok "B-real-nonone NO exit left un-homed (home=none) after rebalance — spread<=1 must not hide a silently un-homed agent; Stage-C cluster-3"  sh -c "! \"$SIM\" ctl -- proxy status --json 2>/dev/null | jq -e '.nodes[]?|select((.home_broker//\"none\")==\"none\")' >/dev/null 2>&1"
        # R8-M1: identify the EXACT moved exit + GATE B-dp on a VALID attributable move (non-empty nid; old home != KTGT;
        # new home == KTGT). B-dp drives the data plane through THAT recorded nid via _ss_via_agent — NOT _ss_via_home
        # (which re-picks whatever is first on KTGT each poll, letting a different healthy exit mask the recorded strand).
        DP_A=$(CTL proxy status --json 2>/dev/null | jq -r --arg b "$KTGT" '.nodes[]?|select(.home_broker==$b)|.nid' 2>/dev/null | head -1)
        DP_BEFORE=$(printf '%s' "$BEFORE_HOMES" | tr ' ' '\n' | grep "^${DP_A}=" | cut -d= -f2)
        DP_NOW=$(CTL proxy status --json 2>/dev/null | jq -r --arg n "$DP_A" '.nodes[]?|select(.nid==$n)|.home_broker // empty' 2>/dev/null | head -1)
        if [ -n "$DP_A" ] && [ -n "$DP_BEFORE" ] && [ "$DP_BEFORE" != "$KTGT" ] && [ "$DP_NOW" = "$KTGT" ]; then B_MOVE=1; else B_MOVE=0; fi
        log "74: B-dp move check — exit=$DP_A oldhome=[${DP_BEFORE:-?}] newhome=[${DP_NOW:-?}] KTGT=$KTGT → valid-attributable-move=$B_MOVE"
        assert_ok "B-move a REAL exit MOVED onto $KTGT by THIS manual rebalance (exact nid $DP_A non-empty, old home ${DP_BEFORE:-?} != $KTGT, new home == $KTGT) — the attribution GATE for B-dp (R8-M1)"  sh -c "[ '$B_MOVE' = 1 ]"
        if [ "$B_MOVE" = 1 ]; then
            assert_ok "B-dp [DATA-PLANE, HARD] an SS leg MUST flow bytes through the RECORDED moved exit $DP_A (was on $DP_BEFORE, proven flowing pre-B, now on $KTGT) within 240s — tests THAT exact nid via _ss_via_agent, not whatever is first on $KTGT (R8-M1). RED if it STRANDS = the #33-family moved-exit stranding EXPOSED as release-blocking (R6-M1)"  poll_until 240 5 "recorded moved exit $DP_A SS flows" -- _ss_via_agent "$DP_A" 1093
            ss_down ctl1 2>/dev/null || true
        else
            warn "74 B-dp NOT-COVERED THIS RUN — no VALID attributable move (B-move RED above: exit=$DP_A old=[${DP_BEFORE:-?}] new=[${DP_NOW:-?}] KTGT=$KTGT). Driving the data plane without a proven move would let a different healthy exit mask a strand of the recorded moved exit (R8-M1). The B-move RED is the exposure."
        fi
        # NEGATIVE CONTROL (R6-M1 non-vacuous + R9-M1 rc-gated): a co-homed ORDINARY expose must NOT be moved by a
        # SUCCESSFUL __proxy__-only rebalance. Created + validly homed on a real broker + SERVING before AND after; the
        # post-control + NEG_OK for Arm C run ONLY when the negative-control rebalance itself SUCCEEDED (_nrc==0).
        NEG_TOK=$(expose_serve_sentinel agt1 8081); [ -n "$NEG_TOK" ] || die "74: negctrl sentinel empty on agt1"
        "$SIM" ctl -- expose rm agt1 --name reg >/dev/null 2>&1
        "$SIM" ctl -- expose agt1 --local 8081 --name reg >/dev/null 2>&1; _regrc=$?
        log "74: negative-control expose reg create rc=$_regrc"
        assert_ok "B-negctrl-create ordinary expose reg create command succeeded (rc=0)"  sh -c "[ '$_regrc' = 0 ]"
        if poll_until 30 3 "reg homed+serving" -- _reg_ready; then _regpre=1; else _regpre=0; fi
        assert_ok "B-negctrl-pre reg becomes VALIDLY homed on a real broker + SERVING before the rebalance (POLLED — the data plane takes a few seconds; a NON-VACUOUS negative control, R6-M1: not an empty-explain 'single' compare) — GATES the negative-control transaction (R8-M1)"  sh -c "[ '$_regpre' = 1 ]"
        EXH0=$(_reg_home)
        if [ "$_regrc" = 0 ] && [ "$_regpre" = 1 ] && printf '%s' "$EXH0" | grep -qE '^brk[123]$'; then
            log "74: negative-control expose reg validly homed on $EXH0 + serving (pre-control ESTABLISHED)"
            _rebal >/dev/null 2>&1; _nrc=$?
            assert_ok "B-negctrl-rc the rebalance command SUCCEEDED (rc=0) so 'reg unchanged' is a REAL negative control, not a no-op (R6-M1)"  sh -c "[ '$_nrc' = 0 ]"
            # R9-M1: gate the post-control AND NEG_OK (for Arm C) on _nrc==0 — a FAILED negative-control rebalance means an
            # "unchanged + serving" PASS is NOT evidence that a SUCCESSFUL __proxy__ rebalance IGNORED the ordinary expose.
            if [ "$_nrc" = 0 ]; then
                NEG_OK=1
                assert_ok "B-negctrl the co-homed ORDINARY expose reg is NOT moved by the SUCCESSFUL __proxy__-only rebalance — home STILL $EXH0 AND still SERVING (plan inv-5; RED if it moved or stopped serving)"  poll_until 15 3 "reg still homed $EXH0 + serving" -- _negctrl_post
            else
                NEG_OK=0
                warn "74 B-negctrl + C-negctrl NOT-COVERED THIS RUN — the negative-control rebalance FAILED (rc=$_nrc; the B-negctrl-rc RED above). An 'unchanged + serving' post-control would NOT evidence a SUCCESSFUL __proxy__ rebalance ignoring the ordinary expose (R9-M1). The rebalance-rc RED is the exposure."
            fi
        else
            NEG_OK=0
            warn "74 B-negctrl + C-negctrl NOT-COVERED THIS RUN — the ordinary-expose PRE-control did NOT establish (create rc=$_regrc, pre-serving=$_regpre, home=[$EXH0]; the B-negctrl-create/pre RED above). A partial/non-serving row must NOT feed the post-control (R8-M1). The pre-control RED is the exposure."
        fi
    else
        NEG_OK=0
        assert_ok "B-snapshot the FRESH attribution snapshot adjacent to the injection is VALIDATED (rc=0 + non-empty exact nid=home) — RED + fail-closed skip of the WHOLE B injection when the status is empty/partial/malformed (R10-M1: a _snap_nidhome|tr pipeline previously masked the helper rc; captured WITHOUT a pipeline now)"  sh -c "false"
        warn "74 B injection (dry-run / real / B-move / B-dp / negative control) NOT-COVERED THIS RUN — the FRESH adjacent VALIDATED snapshot failed (empty/partial status; the B-snapshot RED above). B-dry/B-real are NOT run over an empty attribution baseline (R10-M1); NEG_OK stays 0."
    fi
fi
# R7-M1: reg (created only inside a valid B injection with NEG_OK=1) is kept alive across Arm C for the __proxy__-only
# negative control; removed after Arm C. C-negctrl runs only if NEG_OK=1.
log "74: post-B distribution: $(_dist) ; NEG_OK=$NEG_OK (reg kept for the Arm-C negative control iff NEG_OK=1)"

# ── Arm C — TETHER_AUTO_REBALANCE=on auto path (Stage-C pin-3): returned voter auto-evens WITHOUT the manual verb ──
# R8-M2: env + SKEW-precond + pre-flow + skew + return are GATES. A failed edge must NOT let _auto_tick (=_spread_le1)
# certify an already-even distribution as an "auto effect". C_EDGE tracks a VALID skew+return edge — only then may
# C-auto/C-dp run; a failed prerequisite leaves C-auto/C-dp NOT-COVERED (the prerequisite RED preserved).
C_EDGE=1
if _set_auto_rebalance; then _cenv=1; else _cenv=0; C_EDGE=0; fi
assert_ok "C-setup TETHER_AUTO_REBALANCE=on on all broker units (drop-in + daemon-reload + restart; rejoin 3-VOTER) — GATES Arm C (R8-M2)"  sh -c "[ '$_cenv' = 1 ]"
if _env_on_all; then _cverify=1; else _cverify=0; C_EDGE=0; fi
assert_ok "C-verify env live on every broker (systemctl show -p Environment) — GATES Arm C (R8-M2)"  sh -c "[ '$_cverify' = 1 ]"
# pick the home-heaviest non-leader non-tunnel broker; REQUIRE it carries >0 homes (C SKEW-precond, R8-M2 — the
# round-8 retry hit KTGT=brk2 with 0 homes, so _skew's _ktgt_empty rehome-away oracle was satisfied from the start).
KTGT=""; _kmx=-1; for b in $(list_nodes broker); do [ "$b" = "$(sim_leader)" ] && continue; [ "$b" = brk1 ] && continue; _kc=$(_count_on "$b"); [ "${_kc:-0}" -gt "$_kmx" ] && { _kmx=$_kc; KTGT=$b; }; done
[ -n "$KTGT" ] || KTGT=brk2
[ "$KTGT" != brk1 ] || die "74: Arm-C KTGT resolved to brk1 (the agents' tunnel broker) — refusing to kill it"
log "74: Arm-C KTGT=$KTGT ($_kmx homes)"
if [ "${_kmx:-0}" -gt 0 ]; then _cprec=1; else _cprec=0; C_EDGE=0; fi
assert_ok "C-SKEW-precond KTGT=$KTGT carries >=1 __proxy__ home BEFORE the C kill (else _skew's _ktgt_empty rehome-away oracle is vacuously satisfied from the start — R8-M2, the retry hit KTGT with 0 homes)"  sh -c "[ '$_cprec' = 1 ]"
# R9-M2: after the env restart (C-setup restarts EVERY broker), the earlier all-exit baseline is INVALIDATED, and
# auto-rebalance may return a DIFFERENT nid than the one homed on KTGT pre-kill. So prove EVERY expected exit's SS
# leg (per-nid, over a VALIDATED snapshot) BEFORE the kill — then whichever nid auto-moves onto KTGT is guaranteed to
# have been proven flowing POST-restart (round-8 proved only the first-on-KTGT nid, then post-proved a different one).
# Capture the pre-skew nid→home from the SAME validated snapshot for the auto-move before/after diff.
# R10-M1: capture the snapshot WITHOUT a rc-masking pipeline, then derive both the space-joined homes and the nid
# list from the ALREADY-VALIDATED string (a `_snap_nidhome | tr` pipeline would return tr's rc, hiding a failed helper).
_cbh=$(_snap_nidhome) || _cbh=""
C_BEFORE_HOMES=$([ -n "$_cbh" ] && printf '%s' "$_cbh" | tr '\n' ' ')
C_PRE_NIDS=$([ -n "$_cbh" ] && printf '%s' "$_cbh" | cut -d= -f1 | grep .)
log "74: Arm-C pre-skew homes (VALIDATED)=[$C_BEFORE_HOMES]"
if [ "$C_EDGE" = 1 ] && [ -n "$C_PRE_NIDS" ]; then
    _cpre=1; _cpport=1102
    for _cnid in $C_PRE_NIDS; do
        if poll_until 90 4 "C pre-kill SS flows via $_cnid" -- _ss_via_agent "$_cnid" "$_cpport"; then _cf=1; else _cf=0; _cpre=0; fi
        assert_ok "C-ss-pre exit $_cnid flows bytes over the POST-RESTART baseline before the C kill (per-nid SEQUENTIAL probe, ALL exits over a VALIDATED snapshot, ~90s each — R9-M2; not just the first on KTGT, since auto may return a different nid; the C-skew-adjacent reselect re-validates a non-vacuous kill boundary right before _skew) — GATES the auto edge"  sh -c "[ '$_cf' = 1 ]"
        ss_down ctl1 2>/dev/null || true
        _cpport=$((_cpport+1))
    done
else
    _cpre=0
    [ "$C_EDGE" = 1 ] && assert_ok "C-ss-pre-snapshot the post-restart baseline needs a VALIDATED proxy-status snapshot (exactly $EXPECT_EXITS distinct nids, all homes real voters) — RED + fail-closed when the status is empty/partial/malformed (R9-M2)"  sh -c "false"
fi
[ "$_cpre" = 1 ] || C_EDGE=0
if [ "$C_EDGE" = 1 ]; then
    # R10-M1: the ~270s all-nid pre-flow loop above drifts the distribution, so the KTGT chosen BEFORE it may now carry
    # 0 homes (a vacuous kill whose _ktgt_empty is true from the start). RESELECT + revalidate KTGT from ONE FRESH
    # validated snapshot ADJACENT to the kill: pick the current heaviest non-leader non-tunnel broker with >0 homes
    # (every exit was pre-proven flowing above, so any homed target is valid). If none carries a home → edge NOT-COVERED.
    if _cadj=$(_snap_nidhome) && [ -n "$_cadj" ]; then
        _cadj_homes=$(printf '%s' "$_cadj" | cut -d= -f2); _ktgt2=""; _kmx2=-1
        for b in $(list_nodes broker); do
            [ "$b" = "$(sim_leader)" ] && continue; [ "$b" = brk1 ] && continue
            _kc=$(_count_in "$_cadj_homes" "$b"); [ "${_kc:-0}" -gt "$_kmx2" ] && { _kmx2=$_kc; _ktgt2=$b; }
        done
    else _cadj_homes=""; _ktgt2=""; _kmx2=-1; fi
    if [ -n "$_ktgt2" ] && [ "$_ktgt2" != brk1 ] && [ "${_kmx2:-0}" -gt 0 ]; then KTGT=$_ktgt2; _cadjok=1; else _cadjok=0; C_EDGE=0; fi
    log "74: C-skew-adjacent reselected KTGT=$KTGT (${_kmx2} homes) from ONE fresh validated snapshot right before the kill"
    assert_ok "C-skew-adjacent a non-leader non-tunnel broker STILL carries >=1 __proxy__ home on a VALIDATED snapshot ADJACENT to the kill (KTGT reselected=$KTGT) — R10-M1: the ~270s all-nid pre-flow loop drifts the distribution, so re-validate a NON-VACUOUS kill boundary right before _skew (else _skew kills a 0-home broker and _ktgt_empty is true from the start)"  sh -c "[ '$_cadjok' = 1 ]"
fi
if [ "$C_EDGE" = 1 ]; then
    if _skew; then _cskew=1; else _cskew=0; C_EDGE=0; fi   # kill KTGT → its homes rehome AWAY (count→0)
    assert_ok "C-skew kill $KTGT → its exits rehome AWAY ($KTGT count→0) — GATES the auto edge (R8-M2)"  sh -c "[ '$_cskew' = 1 ]"
fi
if [ "$C_EDGE" = 1 ]; then
    if _return; then _cret=1; else _cret=0; C_EDGE=0; fi
    assert_ok "C-return restart $KTGT → rejoins VOTER — GATES the auto edge (R8-M2)"  sh -c "[ '$_cret' = 1 ]"
    # KTGT must STILL be at 0 homes immediately before the auto window (no manual rebalance ran) — else "auto evened
    # it" would be certified over an already-non-skewed distribution (R8-M2).
    if _ktgt_empty; then _cstill0=1; else _cstill0=0; C_EDGE=0; fi
    assert_ok "C-still-skewed $KTGT is STILL at 0 homes immediately before the auto window (the skew edge held in; no manual verb ran) — GATES the auto EFFECT (R8-M2)"  sh -c "[ '$_cstill0' = 1 ]"
fi
# C-auto / C-dp run ONLY over a VALID skew+return edge (C_EDGE=1). Over an INVALID edge, _auto_tick (=_spread_le1)
# could certify an already-even distribution as an "auto effect" WITHOUT any automatic move — so leave them
# NOT-COVERED (the prerequisite RED preserved) rather than emit a false auto-effect PASS (R8-M2).
# Timing (external-review R4-M5): the auto trigger = ~30s return dwell + autoRebalanceQuietWindow 60s +
# eligibility-recovery (~60-90s typical), so poll the full locked >=180s window before reporting a non-fire.
# R6-M1 HARD: the AUTO-EFFECT is a LOCKED acceptance criterion — RED (release-blocking) if the auto path does NOT
# fire over a valid edge, EXPOSING the auto-rebalance-on-return gap (the no-reader carve-out covers ONLY the raw
# proxy_auto_rebalanced EVENT, NOT the EFFECT). ROOT (proxy_auto_rebalance.go:57-58): the fire-gates are
# `downNow empty ∧ NO in-flight op ∧ !force-single ∧ no recent proxy rehome`; the non-fire here is LIKELY the #31
# lingering in-flight op closing the 'no in-flight op' gate (same op that blocks drain/upgrade). The diagnostic below
# logs the leader's cluster-ops so a non-terminal op present ⇒ auto correctly DEFERS (root=#31), not a broken arm
# (its flap/gate/cooldown logic is hermetic-pinned in g7_auto_rebalance_test).
if [ "$C_EDGE" = 1 ]; then
    _ci_ops=$("$SIM" exec "$(sim_leader)" -- runuser -u tether -- env HOME=/var/lib/tether tether cluster ops ls 2>&1 | tr '\n' '|' | head -c 320)
    log "74: C-auto FIRE-GATE DIAGNOSTIC — leader cluster ops ls = [$_ci_ops] (a non-terminal/in-flight op ⇒ the 'no in-flight op' fire-gate is closed ⇒ #31 DEFERS the auto fire; runtime evidence for the C-auto root cause)"
    # R8-M2/R9-M2: snapshot the pre-auto distribution from a VALIDATED snapshot so the auto-moved exit is derived from
    # a validated before/after diff (not a transient partial view).
    _cba=$(_snap_nidhome) || _cba=""; C_BEFORE_AUTO=$([ -n "$_cba" ] && printf '%s' "$_cba" | tr '\n' ' ')   # R10-M1: no rc-masking pipeline
    if poll_until 180 5 "auto-even spread<=1 (locked >=180s)" -- _auto_tick; then C_AUTO=1; else C_AUTO=0; fi
    assert_ok "C-auto [AUTO-EFFECT, HARD] distribution AUTO-evens (spread<=1) within the locked 180s window WITHOUT the manual verb OVER A VALID skew+return edge — TETHER_AUTO_REBALANCE=on MUST auto-rebalance-on-return (locked plan §3-74 Arm C). RED if it does NOT fire = the auto-rebalance gap EXPOSED as release-blocking; likely root = the #31 lingering in-flight op closing the 'no in-flight op' fire-gate (proxy_auto_rebalance.go:57), the same op that blocks drain/upgrade (R6-M1)"  sh -c "[ '$C_AUTO' = 1 ]"
    log "74: Arm-C post-auto distribution: $(_dist)"
    if [ "$C_AUTO" = 1 ]; then
        # R8-M2: derive the EXACT auto-moved exit — a nid NOW on KTGT whose home in the pre-auto snapshot was != KTGT
        # — and drive the data plane through THAT recorded nid via _ss_via_agent (not _ss_via_home, the identity hole).
        C_DP_A=$(CTL proxy status --json 2>/dev/null | jq -r --arg b "$KTGT" '.nodes[]?|select(.home_broker==$b)|.nid' 2>/dev/null | head -1)
        C_DP_BEFORE=$(printf '%s' "$C_BEFORE_AUTO" | tr ' ' '\n' | grep "^${C_DP_A}=" | cut -d= -f2)
        # R9-M2: also require the auto-moved exit to be one of the nids PROVEN flowing pre-kill (C_PRE_NIDS) — the
        # all-exit C-ss-pre baseline guarantees this, but assert it so a post-proved-but-never-pre-proved exit cannot
        # slip through (round-9: pre-proved agt2, auto returned agt3, gate=1 accepted an unproven exit).
        if [ -n "$C_DP_A" ] && [ -n "$C_DP_BEFORE" ] && [ "$C_DP_BEFORE" != "$KTGT" ] && printf '%s' "$C_PRE_NIDS" | grep -qx "$C_DP_A"; then _cmove=1; else _cmove=0; fi
        log "74: C-dp auto-move check — exit=$C_DP_A pre-auto-home=[${C_DP_BEFORE:-?}] KTGT=$KTGT pre-proven=$(printf '%s' "$C_PRE_NIDS" | grep -qx "$C_DP_A" && echo yes || echo no) → valid-auto-move=$_cmove"
        assert_ok "C-move a REAL exit AUTO-moved onto $KTGT (nid $C_DP_A non-empty, pre-auto home ${C_DP_BEFORE:-?} != $KTGT, AND $C_DP_A was proven flowing pre-kill) — the attribution gate for C-dp (R8-M2/R9-M2)"  sh -c "[ '$_cmove' = 1 ]"
        if [ "$_cmove" = 1 ]; then
            assert_ok "C-dp [DATA-PLANE] SS leg flows bytes through the RECORDED auto-moved exit $C_DP_A (pre-auto home $C_DP_BEFORE, proven flowing pre-kill, now on $KTGT) after the auto rebalance — tests THAT exact nid via _ss_via_agent (R8-M2/R9-M2)"  poll_until 120 5 "auto-moved exit $C_DP_A SS flows" -- _ss_via_agent "$C_DP_A" 1105
            ss_down ctl1 2>/dev/null || true
        else
            warn "74 C-dp NOT-COVERED THIS RUN — no attributable auto move (C-move RED: exit=$C_DP_A pre-auto-home=[${C_DP_BEFORE:-?}] KTGT=$KTGT). Driving the data plane without a proven auto-move would let a different exit mask a strand (R8-M2)."
        fi
    else
        warn "74 C-dp NOT-COVERED THIS RUN — the auto path did NOT fire (C-auto RED above, likely the #31 fire-gate), so there is no AUTO-moved exit to close the data plane THROUGH. Flips to GREEN the day auto-rebalance-on-return fires (R7-M1)."
    fi
else
    warn "74 C-auto + C-dp NOT-COVERED THIS RUN — the Arm-C skew/return EDGE is INVALID (a C-setup/C-verify/C-SKEW-precond/C-ss-pre/C-skew/C-return/C-still-skewed RED above). Certifying an auto effect over an invalid edge would let _auto_tick (=_spread_le1) pass on an already-even distribution WITHOUT any automatic move (R8-M2). The prerequisite RED(s) are the exposure; C-auto/C-dp flip GREEN the day a valid edge AND a firing auto path both hold."
fi
# R7-M1 + R8-M1: the __proxy__-only NEGATIVE CONTROL across the WHOLE Arm C return transaction (kill+return+auto
# window) — reg (homed on tunnel broker brk1, never KTGT) must NOT be moved by ANY part of Arm C whether or not the
# auto path fired. NOT gated on C_AUTO (a control that runs only when the thing it controls fires is not a control),
# but gated on NEG_OK (no established pre-control ⇒ nothing to negatively-control — already warned at B-negctrl).
if [ "$NEG_OK" = 1 ]; then
    assert_ok "C-negctrl the co-homed ORDINARY expose reg is NOT moved by the Arm-C return transaction — home STILL $EXH0 AND still SERVING — the __proxy__-only negative control ACROSS THE AUTOMATIC return transaction (R7-M1 unconditional-on-C_AUTO; R8-M1 gated-on-established-pre-control; plan inv-5)"  poll_until 15 3 "reg still homed $EXH0 + serving after Arm C" -- _negctrl_post
fi
"$SIM" ctl -- expose rm agt1 --name reg >/dev/null 2>&1   # reg served across the Arm B + Arm C negative controls; remove now

warn "74 NOT-COVERED [ONLY the raw proxy_auto_rebalanced EVENT — sys.events has NO operator reader]: the plan's exact-one proxy_auto_rebalanced count==1 anti-flap EVENT assertion is UN-PINNABLE — sys.events has NO operator read command (admin = audit/evict/nodes/sessions only), so 'the event fired exactly once' cannot be READ (a TECHNICALLY-UNOBSERVABLE / review-accepted limitation — NOT an owner scope decision; s3-s5-owner-decisions.md D2, covers ONLY raw event assertions). The auto EFFECT itself (distribution auto-evens) is NOT carved out — it is the HARD C-auto assertion above (RED if it does not fire). R6-M1: the moved-exit data-plane closure (B-dp) and the auto EFFECT (C-auto) are LOCKED acceptance criteria and HARD assertions here — the drill goes RED (release-blocking) when the #33-family moved-exit stranding manifests or the auto path does not fire; that RED is the honest exposure, NOT force-greened. There is NO data-plane / auto-effect measure-and-record carve-out (round-5's was a unilateral acceptance change, reverted)."
drill_end
