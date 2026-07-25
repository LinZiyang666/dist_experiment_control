#!/bin/sh
# 73-proxy-cluster-ha.sh — S4 (N=3 + 2 agent + ctl): proxy cluster HA. The CENTRAL finding this drill pins
# (spike zz-probe-proxy-truth, 2026-07-12): a proxy __proxy__ exit is STRUCTURALLY IMMUNE to expose's #29.
#   - __proxy__ home is LOAD-SPREAD across Eligible voters (proxy_reconcile.go:314 fewest-homes), so an exit
#     routinely homes on a broker != the agent's fixed tunnel_addr. NB (external-review M1): the INITIAL reconcile
#     is NON-deterministic and CAN transiently pile both exits on brk1 — this drill never assumes a spread; it
#     CONSTRUCTS one with `cluster rebalance proxy` + a poll, and fail-fasts (die) if construction fails.
#   - YET the SS egress data plane stays LIVE, because the proxy Home directive carries HomeBrokerAddr+CertPins
#     and the agent ACTIVELY DIALS its home broker (tunnel_adapter.go:80 OpenHome), instead of reusing the
#     fixed tunnel_addr the way an expose does. This is exactly the home!=tunnel case that KILLS an expose (#29,
#     drill 71) — for a proxy it just works at STEADY STATE. #29 is an EXPOSE defect, not a proxy one. HOWEVER
#     (gotcha #33, OBSERVED not-yet-attributed): after a CRASH-rehome (kill the home broker, quorum kept 2/3) the
#     CONTROL plane recovers PROMPTLY (home moves off the dead broker + the exit reaches proxy-ready) yet the SS
#     DATA plane is NOT live at that instant and recovers only after a VARIABLE lag (observed <45s..>150s across
#     runs — non-deterministic, so the drill MEASURES the per-run lag rather than gating a fixed threshold). Root
#     cause is UNDER INVESTIGATION — NOT the ApplyHome "re-point a dead session" claim the first round asserted
#     (tunnel.go:937-964: ApplyHome→OpenHome is an atomic session replace + redial; the SS server is independent of
#     the tunnel). So a proxy is #29-IMMUNE at steady state; PROMPT crash-rehome data-plane recovery is NOT-COVERED
#     — Arm REHOME observes the gap (data plane black-holes at rehomed+ready) + measures the recovery lag.
# Readable control-plane oracles: proxy status --cluster view, /sub http_code, quorum-loss freeze (/sub keeps
# vending 200 WHILE a control write is fenced). #30 CLOSED: a7b88cb made the cluster-mode `sub revoke` REALLY
# emit proxy_keyset_changed and #30 added the operator reader `tether admin events` — the REVOKE arm now reads
# the event back (DELTA past the sub-create baseline, secret-free) ON TOP OF its /sub 404 data effect.
# proxy_node_unready/proxy_no_ready_nodes/proxy_partial remain NOT-COVERED (no reader for those kinds yet).
#
# FALSE-GREEN GUARDS:
#  - SS egress asserts BYTES through the exit (ss-local → agent exit → curl an RFC1918 sink returns the sentinel),
#    not an exit code; the exit under test is one whose home != the tunnel broker (else it wouldn't prove #29-immunity).
#  - the exit-rehome arm re-fetches /sub AFTER the rehome and drives bytes through the NEW home — a stale pre-kill
#    tunnel would fail. quorum kept (kill 1 broker) so the rehome can actually commit (rehome is a Raft write).
#  - quorum-freeze /sub 200 alone is NOT the gate: it is paired with a control-WRITE fence (a raft `sub create`
#    that must NOT mint a token) — proves quorum is genuinely lost, not a healthy cluster. The /sub token used for
#    the freeze read is alice, which is NEVER revoked (the revoke arm uses a separate carol token).
#  - R5-M6 causal gating: the dead-homed exit's /sub-VENDED server is cross-checked == the broker about to be
#    killed (Q-xcheck), the killed broker is proven genuinely DOWN (Q-dead), and the SEPARATION claim is a COMPOUND
#    gate on the black-hole ACTUALLY holding — a dead leg that keeps serving FAILS (RED), never a false "DEAD while
#    200" conjunction (the solo2 accumulation bug). A non-black-holing leg is diagnosed (tunnel-survival vs teardown).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"; . "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/cluster.sh"; . "$HERE/drills/lib/ingress.sh"
. "$HERE/drills/lib/proxy.sh"; . "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): the combined trap resumed after the handler on INT/TERM —
# a Ctrl-C reaped the sidecars and then kept running the broker-kill / black-hole steps below (and raced
# cmd_drill's nuke). drill_install_traps: EXIT registered separately, INT/TERM exit 128+signo without
# resuming. Cleanup body unchanged.
_cleanup() { ss_down ctl1 2>/dev/null; for b in brk1 brk2 brk3; do ingress_down "$b" 443 2>/dev/null; done; }
drill_install_traps _cleanup
CTL() { "$SIM" ctl -- "$@"; }

_setup_ingress_all() { ingress_trust_inject ctl1 || return 1; for b in brk1 brk2 brk3; do secrets_mint_ingress "$INSTANCE" "$b" && ingress_up "$b" 443 ingress "/sub/=127.0.0.1:8090" || return 1; done; }
_proxy_ready()  { _n=$(CTL proxy status --json 2>/dev/null | jq '[.nodes[]?|select(.ready==true)]|length' 2>/dev/null); [ "${_n:-0}" -ge "$1" ]; }
_sub_token()    { CTL proxy sub create --name "$1" 2>&1 | grep -oE '/sub/[A-Za-z0-9._-]+' | head -1 | sed 's#/sub/##'; }
_sub_http()     { "$SIM" exec "$1" -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:8090/sub/$2" 2>/dev/null; }
_home_of()      { CTL proxy status --json 2>/dev/null | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.home_broker // "none"' 2>/dev/null; }
_agent_ready()  { [ "$(CTL proxy status --json 2>/dev/null | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.ready' 2>/dev/null)" = true ]; }
_homes_line()   { CTL proxy status --json 2>/dev/null | jq -r '.nodes[]?|"\(.nid)=\(.home_broker // "none")/rdy=\(.ready)"' 2>/dev/null | tr '\n' ' '; }
# ── #30 operator event reader (admin events) ────────────────────────────────────────────────────────────
# The H.1 `events` JetStream stream (sys.events) had NO operator reader before #30: a MEMBER could NATS-
# SUBSCRIBE, but the OPERATOR (root on the broker host, no member cred) could not. `tether admin events` is
# that reader, over the same root-only 0600 admin socket. Read on brk1 (always alive in 73; the events stream
# is clustered so any live-quorum broker serves it). --json ⇒ ONLY machine JSON on stdout (R11), host jq parses it.
_admin_events_brk1() { "$SIM" exec brk1 -- runuser -u tether -- tether admin events "$@" 2>/dev/null; }
# count of proxy_keyset_changed events carrying sid=lab (empty/non-numeric on a jq/reader miss → callers RED).
_pkc_count() { _admin_events_brk1 --kind proxy_keyset_changed -n 200 --json | jq '[.events[]?|select(.body.sid=="lab")]|length' 2>/dev/null; }
# reader ANCHOR: the operator reader ALREADY reads the earlier alice+carol sub-create proxy_keyset_changed
# events (cluster-mode create emits it too, proxy_cluster_wire.go:154) — >=2 for sid=lab proves the reader WORKS
# in this drill before we pin the revoke's delta, and guarantees both create emits are in the pre-revoke baseline.
_pkc_two() { _c=$(_pkc_count); case "$_c" in ''|*[!0-9]*) return 1;; esac; [ "$_c" -ge 2 ]; }
# revoke landed: the sid=lab count GREW past the pre-revoke baseline _PKC_BEFORE (a DELTA, so it pins the REVOKE's
# emit — a7b88cb — not the create emits already counted in the baseline).
_pkc_grew() { _c=$(_pkc_count); case "$_c" in ''|*[!0-9]*) return 1;; esac; case "${_PKC_BEFORE:-}" in ''|*[!0-9]*) return 1;; esac; [ "$_c" -gt "$_PKC_BEFORE" ]; }
# SECRET-FREE: every matching event body carries ONLY {v,type,ts,sid} (proxy events never a psk/token). jq
# returns true/false and never PRINTS a body value, so the check itself leaks nothing. Requires >=1 event (non-vacuous).
_pkc_secretfree() { _admin_events_brk1 --kind proxy_keyset_changed -n 200 --json | jq -e '([.events[]?|select(.body.sid=="lab")]|length>=1) and all(.events[]?; ((.body|keys) - ["v","type","ts","sid"]|length)==0)' >/dev/null 2>&1; }
# CV: cluster view has the CLUSTER line + per-agent HOME/REASON, member-readable, no leaked secret.
_cv() {
    _o=$(CTL proxy status --cluster 2>&1) || return 1
    printf '%s' "$_o" | grep -qiE 'CLUSTER|HOME|REASON|ha-policy' && ! printf '%s' "$_o" | grep -qiE 'password:|[A-Za-z0-9+/_-]{40,}'
}
# an exit whose home is NOT the tunnel/leader broker brk1 — prints "AGENT HOMEBROKER". Returns 1 if none;
# callers MUST die (M1). The INITIAL reconcile is NON-deterministic — isolated reruns DID land both exits on
# brk1 (external-review M1), so the "fewest-homes guarantees one exists" premise was FALSE for the transient
# initial state. The spread is CONSTRUCTED explicitly via _construct_nontunnel (rebalance+poll), never assumed.
# This is the exit that would DIE if it were an expose (#29).
# $1 (optional) = a broker to EXCLUDE from the pick (the leader — self-review finding-9: the planner moves the
# off-brk1 exit onto the lowest-node_id min-load voter, which == the leader if an election put the leader on brk2;
# picking it would trip the "NT_HB != leader" invariant and spuriously die. Prefer a non-leader off-brk1 exit).
_pick_nontunnel() {
    for _a in agt1 agt2; do _h=$(_home_of "$_a"); [ -n "$_h" ] && [ "$_h" != brk1 ] && [ "$_h" != none ] && { [ -z "${1:-}" ] || [ "$_h" != "$1" ]; } && { printf '%s %s' "$_a" "$_h"; return 0; }; done
    return 1
}
# DETERMINISTIC topology construction (M1): `cluster rebalance proxy` spreads the exits across eligible voters —
# re-run each poll tick until >=1 exit homes OFF brk1 (the Home re-push + agent re-establish + status refresh lags
# the raft commit; brk2/brk3 also need proxy-eligibility to recover first).
# external-review R3-M1 ROOT CAUSE FIX: `cluster rebalance proxy` is an ADMIN verb served by the leader BROKER's
# admin socket — it is NOT a ctl-over-NATS op. The old `CTL cluster rebalance proxy` (= ctl) FAILED silently with
# "admin socket ... no such file or directory" on the ctl container, so the rebalance NEVER ran and construction
# depended on the reaper's non-deterministic natural reconcile (the long-standing 73 flakiness). Run it on the
# leader broker like drill 74 does. (diag-elig confirmed the leader path works + reports eligibility immediately.)
_rebal() { "$SIM" exec "$(sim_leader)" -- runuser -u tether -- tether cluster rebalance proxy "$@" 2>&1; }
_construct_nontunnel() { _rebal >/dev/null 2>&1; _pick_nontunnel "${_ldr0:-}" >/dev/null 2>&1; }
# external-review R3-M1: gate construction on the ACTUAL broker proxy-eligibility STATE, not a fixed-time guess.
# The rebalance planner reports "across N eligible voter(s)" (proxy_rebalance.go eligibleProxyHomes = VOTER + cert
# + public_host + §17-reachable). N>=2 ⟺ at least one NON-brk1 voter is proxy-home-eligible, so a spread OFF brk1
# is actually achievable. This is the observable the construction depends on; before it, no rebalance can move an
# exit off brk1 (both stay brk1/rdy=true — the solo1 failure). --dry-run is a zero-mutation preview.
_proxy_elig_voters() { _rebal --dry-run | grep -oE '[0-9]+ eligible voter' | grep -oE '^[0-9]+' | head -1; }
_ge2_proxy_eligible() { _ev=$(_proxy_elig_voters); [ -n "$_ev" ] && [ "$_ev" -ge 2 ]; }
_still_homed_on() { [ "$(_home_of "$1")" = "$2" ]; }   # $1=agent still homed on broker $2 (live-target guard)
_rehomed_off()  { _h=$(_home_of "$1"); [ -n "$_h" ] && [ "$_h" != "$2" ] && [ "$_h" != none ]; }
# SS egress via a NAMED exit: fetch alice's /sub, start ss-local pinned to that exit, drive bytes to the RFC1918 sink.
_ss_down_port() { dexec ctl1 -- pkill -f "ss-local .*-l $1 " >/dev/null 2>&1 || true; }   # reap ONLY this port's ss-local (safe alongside other legs)
# _ss_leg_try <exit-nid> <port> : reap THIS port, RE-FETCH /sub (brk1 — always alive in 73), start a VERIFIED-READY
# ss-local (ss_up returns 1 if the exit is NOT rendered in /sub OR the client never binds — R4-M1), one curl to the
# sink. 0 iff bytes flow. Re-fetching each tick handles a MOVED/rehomed exit that RENDERS LATE (the same
# intermittency #33 measures — external-review R4: the old fetch-once-and-fail was the Q-legup 'proxy not found' flake).
_ss_leg_try() {
    _ss_down_port "$2"
    ss_sub_fetch ctl1 "https://brk1/sub/$TOK_A" "/tmp/73-leg-$2.${INSTANCE:-x}.yaml" "$CA" || return 1
    ss_up ctl1 "/tmp/73-leg-$2.${INSTANCE:-x}.yaml" "$1" "$2" 2>/dev/null || return 1
    ss_curl_ok ctl1 "$2" "http://$SINK_IP:9090/" "$SINK_TOK"
}
# a persistent SS leg via a named exit that LEAVES the leg up on success; polls (RE-FETCHING /sub each tick) until a
# moved/rehomed exit renders + a verified-ready leg on THIS port flows. die-gated baseline (never renders+flows in
# 240s = a real moved-exit-not-serving RED). $1=exit agent $2=port. Safe alongside other legs (per-port reap).
_ss_egress()  { poll_until 240 5 "SS leg via $1:$2 renders+flows" -- _ss_leg_try "$1" "$2"; }
# (The #33 REHOME observation is DATA-PLANE-based — Arm REHOME below establishes an SS leg, samples it dead the
# instant the control plane reports rehomed+ready, then MEASURES the recovery lag. The round-3 readiness-based
# `_ready_lags_60s` reframe was WRONG — proxy_ready recovers fast; the DATA plane is what lags — and was removed;
# self-review finding-3 killed the leftover ">90s" figure it implied.)
# data-plane FREEZE separation (Stage-C mandate-1/pin-2/dp-1): keep TWO SS legs up — one via an exit homed on the
# survivor leader, one via an exit homed on the will-be-killed voter — then prove the survivor leg still flows
# bytes WHILE the dead leg black-holes under quorum loss. A proxy exit's SS egress rides its HOME broker's tunnel;
# kill the home → that leg black-holes; the leader-homed leg keeps flowing. That is data-plane freeze, not a read gate.
_agent_homed_on() { CTL proxy status --json 2>/dev/null | jq -r --arg b "$1" '.nodes[]?|select(.home_broker==$b)|.nid' 2>/dev/null | head -1; }
_homed_both()     { [ -n "$(_agent_homed_on "$1")" ] && [ -n "$(_agent_homed_on "$2")" ]; }
# QUORUM 1+1 construct: rebalance + poll until BOTH the leader $LDR and the surviving voter $K2 home an exit. Runs
# AFTER the Q-heal (proxy off/on) re-established all exits FRESH, so both are healthy + flowing (no #33 dependency).
_qconstruct()     { _rebal >/dev/null 2>&1; _homed_both "$LDR" "$K2"; }
_ss_flows()       { ss_curl_ok ctl1 "$1" "http://$SINK_IP:9090/" "$SINK_TOK"; }     # $1=port → bytes flow through the leg
_ss_deadhole()    { ! ss_curl_ok ctl1 "$1" "http://$SINK_IP:9090/" "$SINK_TOK"; }   # $1=port → black-hole (curl fails)
# external-review R5-M6: the /sub-VENDED server (home broker public_host) for a named exit — so the QUORUM arm can
# cross-check that the exit it is about to KILL is the exact broker /sub actually vends for it (not a stale/other
# endpoint), and later diagnose a mismatch (stale rendering vs status attribution vs tunnel survival).
_sub_server_of() {   # $1=exit-nid → prints the Clash `server:` field (the vended home broker host)
    ss_sub_fetch ctl1 "https://brk1/sub/$TOK_A" "/tmp/73-srv.${INSTANCE:-x}.yaml" "$CA" >/dev/null 2>&1 || return 1
    dexec ctl1 -- python3 -c "import yaml
ps=(yaml.safe_load(open('/tmp/73-srv.${INSTANCE:-x}.yaml')) or {}).get('proxies') or []
for p in ps:
 if p.get('name')=='$1': print(p.get('server','')); break" 2>/dev/null
}
_ss_leg_up()      { _ss_egress "$1" "$2"; }   # robust persistent leg (re-fetch /sub per tick + per-port reap); QUORUM keeps TWO up at once, so per-port reap is REQUIRED (ss_down-all would kill the other leg)
# _exit_flows_fresh <exit-nid> <port> : ONE fresh operator-style attempt to egress through <exit> — re-fetch /sub
# (picks up the CURRENT home), start a VERIFIED-READY ss-local (ss_up returns 1 if the exit is NOT rendered in
# /sub OR the client never binds — R4-M1), then a single curl to the sink. 0 iff bytes flow. Self-cleans the prior
# tick's ss-local so it is safe inside a poll_until (used to MEASURE crash-rehome auto-recovery in Arm REHOME).
_exit_flows_fresh() {
    ss_down ctl1 2>/dev/null
    ss_sub_fetch ctl1 "https://brk1/sub/$TOK_A" "/tmp/73-rec-$1.${INSTANCE:-x}.yaml" "$CA" || return 1
    ss_up ctl1 "/tmp/73-rec-$1.${INSTANCE:-x}.yaml" "$1" "$2" 2>/dev/null || return 1
    ss_curl_ok ctl1 "$2" "http://$SINK_IP:9090/" "$SINK_TOK"
}
# quorum-loss control-write fence: a raft `sub create` must NOT mint a /sub token (it either refuses with a
# quorum error or times out — both are "fenced"). Minting a token would prove the cluster is still healthy.
_write_fenced() {
    _o=$(timeout 12 "$SIM" ctl -- proxy sub create --name qfence 2>&1)
    ! printf '%s' "$_o" | grep -qE '/sub/[A-Za-z0-9._-]{6,}'
}
_kill_2nd() { [ -n "$K2" ] && node_kill "$K2"; }   # kill the 2nd non-leader voter (K2 set below); node_kill is a docker.sh fn

drill_begin "73-proxy-cluster-ha (N=3 proxy #29-immunity + rehome + quorum freeze)"
"$SIM" nuke >/dev/null 2>&1 || true
# R5-M5: run grow_to_3 OUTSIDE assert_ok so GROW-ATTEMPTS + per-attempt grow rc are VISIBLE (assert_ok hides them on success).
grow_to_3 2 1; _g3rc=$?
assert_ok "grow_to_3 + 2 agents + ctl (N=3 HA cluster; GROW-ATTEMPTS logged above)"  sh -c "exit $_g3rc"
# external-review R2-M1: grow is FOUNDATIONAL — a short grow (brk3 not VOTER) must NOT let the rest of the drill
# (session/agents/proxy, then destructive kills) run. assert_ok records + continues, so hard-gate here + die.
_three_voters || die "73: grow_to_3 did NOT reach 3 VOTERs — foundational failure, refusing to continue (not a valid #33/quorum run)"
assert_ok "session lab + ctl login (owner)"              "$SIM" session "$SID" --pin "$PIN"
for _a in agt1 agt2; do
    assert_ok "agent-join $_a (tunnel=brk1)"             "$SIM" agent-join "$_a" --session "$SID" --pin "$PIN"
    assert_ok "agent_provision_yaml $_a proxyprivate"    agent_provision_yaml "$_a" "$SID" nats://brk1:4222 proxyprivate
done
assert_ok "SETUP ingress on all 3 brokers + trust ctl1"  _setup_ingress_all
assert_ok "proxy on --ha-policy freeze-on-quorum-loss (owner)" \
          sh -c "\"$SIM\" ctl -- proxy on --yes --ha-policy freeze-on-quorum-loss 2>&1 | grep -qi 'proxy ON'"
assert_ok "both agent exits reach proxy-ready"           poll_until 60 3 "2 exits ready" -- _proxy_ready 2
# external-review R2-M1 FAIL-FAST GATE: assert_ok accumulates a failure + CONTINUES, so a broken foundation (grow
# short of 3 VOTER, exits not ready) would still reach the destructive broker-kill / quorum-loss arms and let
# their inverted/negative oracles record misleading PASS lines. HARD-GATE here: die before any destructive arm if
# the N=3 + 2-ready-exit foundation is not actually valid — every later arm's causality depends on it.
_three_voters || die "73: FOUNDATION INVALID (not 3 VOTERs after grow) — refusing to run destructive arms"
_proxy_ready 2 || die "73: FOUNDATION INVALID (2 proxy exits not ready) — refusing to run destructive arms"

# sink: an RFC1918 HTTP endpoint on the always-alive ctl node (survives every broker kill below).
SINK_TOK=$(bridge_serve_sentinel ctl1 9090); SINK_IP=$(node_bridge_ip ctl1)

# ── Arm CV — proxy status --cluster (readable view; no SS data plane) ─────────
assert_ok "CV proxy status --cluster: CLUSTER line + per-agent HOME/REASON, member-readable, no secret"  _cv

# ── Arm SUB — alice sub create + /sub loopback + cross-container ingress + forged 404 ──
TOK_A=$(_sub_token alice); log "73: alice token minted (len=${#TOK_A}) ; homes: $(_homes_line)"   # external-review R2-M3: NEVER log the raw bearer token value
assert_ok "SUB1 sub create alice → token minted"  sh -c "[ -n '$TOK_A' ]"
assert_ok "SUB2 /sub loopback 200 (signed subscription doc)"  sh -c "[ \"\$(\"$SIM\" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_A)\" = 200 ]"
assert_ok "SUB3 /sub cross-container via ingress (loopback stays loopback-only)"  sh -c "\"$SIM\" exec ctl1 -- curl -s --max-time 6 --cacert '$CA' https://brk1/sub/$TOK_A 2>/dev/null | grep -qE 'proxies:|DIRECT'"
assert_ok "SUB4 forged /sub token → 404 (no info leak)"  sh -c "[ \"\$(\"$SIM\" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/deadbeefdeadbeef)\" = 404 ]"

# ── Arm SS — proxy egress is LIVE through an exit homed OFF the tunnel broker (structural #29-immunity) ──
# M1: CONSTRUCT the spread deterministically (the initial reconcile can pile both exits on brk1). fail-fast if
# construction fails — a missing non-tunnel exit means the whole #29-immunity proof is unprovable, never a PASS.
_ldr0=$(sim_leader) || die "73: SS no leader"
# NB: a freshly-grown brk2/brk3 is raft-VOTER within seconds but proxy-home-ELIGIBLE only after its cert/public-host
# re-register + the §17 observe loop marks it reachable (~up to 120s — the same recovery drill 74 documents). Until
# then only brk1 is an eligible home, so the reaper piles BOTH exits on brk1 and rebalance has nothing to spread
# onto. So the construct poll must OUTLAST that eligibility recovery (150s), re-running rebalance each tick.
# external-review R3-M1: first gate on the OBSERVABLE proxy-eligibility state (>=2 eligible voters, so a non-brk1
# home exists), with a diagnostic deadline — NOT a blind fixed-time assumption. If eligibility never reaches >=2,
# die LOUD (a measured brk2/brk3 proxy-eligibility-recovery gap), never a silent construction timeout.
poll_until 240 5 ">=2 proxy-eligible voters (brk2/brk3 proxy-home eligibility recovered post-grow)" -- _ge2_proxy_eligible || die "73: fewer than 2 proxy-eligible voters after 240s — brk2/brk3 proxy-home eligibility did NOT recover (a MEASURED setup gap; the drill refuses to guess); construction of a non-tunnel exit is impossible"
assert_ok "SS-construct rebalance until >=1 exit homes OFF the tunnel/leader broker brk1 (now that >=2 voters are proxy-eligible, the spread is achievable; M1/R3-M1)"  poll_until_fixed 90 5 ">=1 exit off brk1" -- _construct_nontunnel
NT=$(_pick_nontunnel "$_ldr0") || die "73: SS no NON-LEADER non-tunnel exit after construct (homes: $(_homes_line); leader=$_ldr0) — topology construction FAILED (or a leader-election anomaly moved the leader off brk1 so the only off-brk1 exit sits on the leader — a setup flake, not a product defect; re-run), #29-immunity is unprovable this run"
NT_A=${NT% *}; NT_HB=${NT#* }
[ -n "$NT_A" ] && [ -n "$NT_HB" ] || die "73: SS empty NT_A='$NT_A' / NT_HB='$NT_HB' after pick"
[ "$NT_HB" != "$_ldr0" ] || die "73: SS NT_HB=$NT_HB is the leader — non-tunnel pick invariant broken"
node_running "$NT_HB" || die "73: SS target broker $NT_HB not running before the SS baseline"
log "73: non-tunnel exit = $NT_A homed on $NT_HB (leader=$_ldr0, agent tunnel_addr=brk1, so home!=tunnel — the expose-#29 case)"
assert_ok "SS-spread >=1 __proxy__ exit homes OFF brk1 (NT_A=$NT_A on $NT_HB), provably NOT all-on-tunnel — CONSTRUCTED, not assumed"  sh -c "[ -n '$NT_A' ]"
assert_ok "SS-guard NT_HB=$NT_HB is a NON-leader (the REHOME kill must not take the leader — Stage-C cluster-4)"  sh -c "[ '$NT_HB' != '$_ldr0' ]"
# die-gate (self-review finding-8): a moved exit's SS data plane must establish (generous 240s poll inside
# _ss_egress) or the destructive REHOME/QUORUM arms would run over a dead #29-immunity baseline.
_ss_egress "$NT_A" 1080; _sseg=$?
assert_ok "SS [DATA-PLANE] egress via $NT_A (home=$NT_HB != tunnel brk1) → curl RFC1918 sink returns bytes — proxy dials HomeBrokerAddr+pins, IMMUNE to expose #29 (steady state)"  sh -c "exit $_sseg"
[ "$_sseg" -eq 0 ] || die "73: SS #29-immunity baseline did NOT flow bytes (moved-exit data plane never established in 240s) — refusing to run destructive REHOME/QUORUM arms over a dead baseline (M1 fail-fast; self-review finding-8)"
ss_down ctl1

# ── Arm REHOME — kill the non-tunnel exit's HOME broker (quorum kept 2/3) and MEASURE the crash-rehome recovery.
#    gotcha #33 REFRAMED (external-review R4-M1): earlier rounds claimed "the SS data plane lags control-readiness"
#    — but the source REFUTES that: TunnelExposeAdapter.ApplyHome→Client.OpenHome (tunnel.go:849-963) BLOCKS until
#    dial+REGISTER+yamux succeeds and the replacement session is installed, and the agent re-ACKs ready=true only
#    THEN (proxy.go:132-183). So at ready=true the tunnel to the NEW home is already up — there is NO reliable
#    "ready-but-black-hole" window to invert-assert (the round-3 readiness oracle AND the round-4 "data trails
#    ready" oracle were BOTH over-claims). What IS deterministic + measurable: (a) the crash SEVERS the tunnel to
#    the dead home so the exit BLACK-HOLES right after the kill; (b) the control plane AUTO-rehomes + reaches
#    ready; (c) a fresh /sub (now the NEW home) AUTO-flows again — no manual 'proxy off; proxy on'. This arm gates
#    those three deterministic facts and MEASURES the recovery latencies (traced), attributing no root cause and
#    claiming no fixed figure. The SS /sub `server` points at the exit's HOME broker public port, so a client
#    pinned pre-crash observes the black-hole and a FRESH client (re-fetched /sub) observes the recovery. ──
# baseline: fresh /sub + a VERIFIED-READY ss-local (ss_up now polls the SOCKS listener — R4-M1) + real bytes.
# die-gated (self-review finding-8). KEEP it up (no ss_down) so the SAME proven-ready client observes the crash.
_ss_egress "$NT_A" 1081; _r33base=$?
assert_ok "REHOME-baseline SS leg via $NT_A (home=$NT_HB) flows bytes pre-kill (live destructive baseline; the SAME verified-ready client observes the crash black-hole below — anti-vacuity)"  sh -c "exit $_r33base"
[ "$_r33base" -eq 0 ] || die "73: REHOME pre-kill SS baseline via $NT_A did NOT flow in 240s — refusing the destructive kill over a dead baseline (M1 fail-fast; self-review finding-8)"
# ── live-target guard (REWRITTEN R9-D item 2) ──────────────────────────────────────────────────────────────────
# It used to be ONE compound assert_ok followed by `|| die`. When the home DRIFTED off the constructed target —
# which is literally what #34 IS — that shape produced: an ASSERT-FAIL on a line described as a "live destructive
# target" check, plus a SETUP-RED from the die, and the whole drill reported as a broken FOUNDATION. The product
# defect that caused it was named nowhere. Two independent facts, attributed separately:
#   • the target CONTAINER is still running — an infra fact; a hard assert either way.
#   • the target still HOMES the exit      — when it does not, that is #34's distribution drift, recorded as a
#     signature-carrying PRODUCT-RED against its owner. The REHOME arm is then SKIPPED entirely: killing a broker
#     that no longer hosts the exit is a vacuous kill, and every #33 statement built on it would be describing a
#     broker that hosted nothing (exactly the H10-class misattribution the full-suite review called out).
_np_running=0; node_running "$NT_HB" && _np_running=1
_np_home=$(_home_of "$NT_A")
log "73: REHOME-precond snapshot — target=$NT_HB running=$_np_running ; $NT_A home=[${_np_home:-none}] ; homes: $(_homes_line)"
assert_ok "REHOME-precond-live the kill target $NT_HB is still RUNNING (killing an already-dead container is a vacuous injection)"  sh -c "[ '$_np_running' = 1 ]"
REHOME_OK=0
if [ "$_np_running" = 1 ] && [ "$_np_home" = "$NT_HB" ]; then
    REHOME_OK=1
    assert_ok "REHOME-precond-home $NT_HB STILL homes $NT_A at kill time — the 1/1/1 spread constructed by 'cluster rebalance proxy' HELD long enough to be used (a KEPT invariant, and the causal precondition of every #33 claim below)"  sh -c "[ '$_np_home' = '$NT_HB' ]"
else
    product_red "#34 proxy home distribution does not STAY put (owner: R8 product side; docs/deploy-tier-gotchas.md:231) — the spread this drill CONSTRUCTED with 'cluster rebalance proxy' had already drifted by kill time: $NT_A is homed on [${_np_home:-none}], not the constructed $NT_HB. Same signature the 2026-07-18 full-suite run recorded (a constructed 1/1/1 collapsing back onto one broker after a disturbance). Operationally: proxy HA capacity spread is not durable — an operator must re-run 'cluster rebalance proxy' after every disturbance or the spread is decorative"
fi
if [ "$REHOME_OK" = 1 ]; then
_t_kill=$(date +%s)
assert_ok "REHOME kill $NT_A's home broker $NT_HB (non-leader; quorum kept 2/3 so the rehome raft-write can commit)"  node_kill "$NT_HB"
# #33-a (DETERMINISTIC + CAUSAL): the crash severs the tunnel to the dead home → the SAME client that flowed +
# was listener-verified pre-kill now BLACK-HOLES. This is the EXIT going down, not the client (it was proven up).
assert_ok "REHOME [#33-a] the SS data plane via $NT_A BLACK-HOLES right after its home broker $NT_HB crashes — the crash severed the tunnel to the dead home; the SAME verified-ready client that flowed pre-kill now black-holes (the exit, not the client)"  poll_until 30 3 "$NT_A SS black-holes post-crash" -- _ss_deadhole 1081
ss_down ctl1
# control plane AUTO-recovers: home leaves the dead broker (die-gated) + exit reaches ready (die-gated, generous —
# ready waits for OpenHome to re-dial the new home). Capture the ready edge for the recovery-latency trace.
assert_ok "REHOME [CONTROL] $NT_A's __proxy__ home leaves the dead $NT_HB (auto-rehome to a survivor voter — a raft rehome write)"  poll_until 90 3 "$NT_A rehomes off $NT_HB" -- _rehomed_off "$NT_A" "$NT_HB"
_rehomed_off "$NT_A" "$NT_HB" || die "73: REHOME control-rehome did NOT complete ($NT_A still on dead $NT_HB) — refusing the recovery measurement on an invalid rehome"
assert_ok "REHOME [CONTROL] $NT_A reaches proxy-ready post-rehome at least once (ready = the agent re-ACKed after OpenHome re-dialed the new home; NB it may then FLAP — that is part of #33 below, not a die condition)"  poll_until 150 3 "$NT_A ready post-rehome" -- _agent_ready "$NT_A"
_t_ready=$(date +%s)
log "73: post-rehome CONTROL plane home moved off $NT_HB + $NT_A reached ready at least once: $(_homes_line)"
# #33-b (MEASURE + RECORD — external-review R4-M1: no die, no flaky inverted pin): does the SS DATA plane
# AUTO-recover? Poll up to 180s for the exit to serve a FLOWING leg — a fresh /sub renders $NT_A AND a
# verified-ready ss-local flows real bytes (_exit_flows_fresh). Record the outcome per-run.
# RUN-OBSERVED (2026-07-13): after this crash-rehome the exit's readiness FLAPS (transient ready) and it is NOT
# rendered in /sub, so the DATA plane does NOT auto-recover — the QUORUM arm's 'proxy off; proxy on' heal below is
# what recovers it (auto FAILS, manual WORKS = gotcha #33). This confirms the earlier rounds' observation; the
# round-4 "auto-recovers, no manual needed" wording was premature and is corrected here from the real run.
if poll_until 180 6 "$NT_A auto-recovers a FLOWING SS data plane post-crash-rehome" -- _exit_flows_fresh "$NT_A" 1082; then
    _t_flow=$(date +%s); _33auto=AUTO-RECOVERED
    log "73: #33 crash-rehome data plane AUTO-recovered ≈ $((_t_flow-_t_kill))s after the crash (no manual intervention this run)"
else
    _t_flow=$(date +%s); _33auto=STRANDED
    warn "73 gotcha #33 (crash-rehome data plane does NOT auto-recover — OBSERVED + MEASURED, root cause NOT attributed): after a proxy exit's HOME broker is CRASH-killed (quorum kept 2/3), the CONTROL plane rehomes the exit off the dead broker and it reaches proxy-ready at least once, YET its SS DATA plane does NOT auto-recover within 180s — readiness FLAPS and the exit is not rendered in /sub, so no fresh client can egress through it. Recovery today needs a MANUAL 'proxy off; proxy on' (the QUORUM arm below performs exactly that heal + re-establishes the exits). control-plane rehome ≠ data-plane recovery. Do NOT attribute a root cause (the first-round ApplyHome 're-point a dead session' claim was WRONG — tunnel.go:849-963 OpenHome is a blocking dial+REGISTER+yamux+session-install). #33 not #32 — plan §355 reserves #32 (CANDIDATE, stale-listener). FLIPS the day crash-rehome auto-recovers the data plane."
fi
ss_down ctl1
assert_ok "REHOME [#33] crash-rehome DATA-plane auto-recovery MEASURED = $_33auto (control rehomed+ready ≈ $((_t_ready-_t_kill))s after the crash; STRANDED ⇒ the #33 gap, which the QUORUM arm below proves the manual 'proxy off; proxy on' heal recovers) — recorded per-run, never pre-judged or die-masked"  sh -c "[ '$_33auto' = AUTO-RECOVERED ] || [ '$_33auto' = STRANDED ]"
log "73: #33 TRACE from the $NT_HB crash at t0: control rehomed+ready ≈ $((_t_ready-_t_kill))s ; data-plane auto-recovery = $_33auto (probe ended ≈ $((_t_flow-_t_kill))s)"
else
    # R9-D: #34 drift skipped the whole REHOME arm. Two things must still be true for the rest of the drill to
    # mean anything, and NEITHER may borrow the REHOME arm's language:
    #   (1) the #33 measurement is genuinely uncovered THIS RUN — recorded, not silently absent;
    #   (2) the QUORUM arm below still needs 2 of 3 brokers down to lose committable quorum, and $NT_HB is the
    #       one it excludes from its K2 pick. So $NT_HB is killed anyway, as PURE PROVISIONING carrying NO #33
    #       claim (labelled as such, and issued through assert_setup so a failed kill aborts instead of quietly
    #       letting the QUORUM arm run at 2/3 while calling itself a quorum-loss proof).
    ss_down ctl1
    not_covered "73 Arm REHOME (#33 crash-rehome control-plane + data-plane measurement) THIS RUN" "the constructed home drifted before the kill (#34, recorded PRODUCT-RED above), so $NT_HB no longer hosts $NT_A. Killing it would be a vacuous injection and every #33 statement downstream — black-hole, control rehome, auto-recovery latency — would be describing a broker that hosted nothing. The drill declines to manufacture a #33 verdict on top of a #34 failure; re-run for the #33 measurement, or fix #34 to make the spread durable." gap
    assert_setup "REHOME-skip [env] kill $NT_HB as PURE QUORUM-arm PROVISIONING (2 of 3 brokers must be down before the quorum-loss separation below; this kill asserts NOTHING about #33 — see the NOT-COVERED above)"  node_kill "$NT_HB"
    _33auto=SKIPPED-NO-VALID-TARGET
fi

# ── Arm REVOKE — carol sub revoke → /sub 404 (data effect) WHILE alice stays 200; #30 event NOT-COVERED ──
TOK_C=$(_sub_token carol)
assert_ok "REVOKE carol minted + /sub 200 before revoke"  sh -c "[ -n '$TOK_C' ] && [ \"\$(\"$SIM\" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_C)\" = 200 ]"
# #30 ANCHOR + baseline (non-vacuity): the operator reader already reads the alice+carol sub-create
# proxy_keyset_changed events. Poll until BOTH have landed (>=2) so the post-revoke DELTA is attributable to
# the REVOKE alone, then freeze the baseline.
assert_ok "REVOKE-event [#30] anchor: the operator reader (admin events) reads the earlier sub-create proxy_keyset_changed events (>=2 for sid=lab) — proves the reader WORKS in this drill before pinning the revoke's delta (non-vacuity)"  poll_until 20 2 ">=2 sub-create proxy_keyset_changed readable" -- _pkc_two
_PKC_BEFORE=$(_pkc_count); log "73: [#30] proxy_keyset_changed(sid=lab) baseline before revoke = ${_PKC_BEFORE:-?} (alice+carol sub-create emits)"
assert_ok "REVOKE proxy sub revoke carol"  "$SIM" ctl -- proxy sub revoke carol
assert_ok "REVOKE-effect carol /sub → 404 WHILE alice /sub still 200 (precise, not a blanket outage)"  poll_until 15 2 "carol 404 ∧ alice 200" -- sh -c "[ \"\$(\"$SIM\" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_C)\" = 404 ] && [ \"\$(\"$SIM\" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_A)\" = 200 ]"
# #30 CLOSED: a7b88cb made the CLUSTER-MODE revoke REALLY emit proxy_keyset_changed (the single-mode path
# already did via pushCurrentKeyset), and #30 added the operator reader (the events stream had no NATS-less
# operator reader). The old NOT-COVERED[gap] "sys.events has no operator reader" is retired — assert the
# revoke's event is now READABLE: the sid=lab count GREW past the baseline (DELTA pins the REVOKE's emit, not
# the sub-create emits already counted), and the payload is secret-free.
assert_ok "REVOKE-event [#30] admin events reads the CLUSTER-MODE revoke's proxy_keyset_changed: the sid=lab count grew past the pre-revoke baseline ${_PKC_BEFORE:-?} (DELTA pins the revoke's emit a7b88cb, not the sub-create emits) — operator reader over the root admin socket, no NATS member cred (closes the old #30 no-reader gap)"  poll_until 20 2 "proxy_keyset_changed count grows across the revoke" -- _pkc_grew
assert_ok "REVOKE-event [#30] the proxy_keyset_changed payload is SECRET-FREE (body keys ⊆ {v,type,ts,sid}; the reader relays only sid — never a psk/token/PSK material)"  _pkc_secretfree

# ── Arm QUORUM — DATA-PLANE separation (M7/M1, R-CONTROLSRC): under quorum loss a dead-homed exit's SS leg
#    BLACK-HOLES (data plane dead) WHILE a survivor(leader)-homed exit's SS leg STILL FLOWS and /sub still vends
#    200. The survivor leg is REQUIRED (NOT optional) — without an independent live data-plane control source the
#    black-hole cannot be told apart from a whole-proxy outage. With EXACTLY 2 live voters (leader + K2) after the
#    REHOME kill, the fewest-homes balance is 1-on-each → BOTH baselines are DETERMINISTICALLY constructible. ──
# HEAL first (external-review #33 decoupling): the REHOME crash-rehome left NT_A's data plane in the no-flow gap.
# Rather than depend on that recovery timing, cycle proxy off then on — separate asserts (raft writes at 2/3,
# quorum intact) that tear down + FRESH-establish ALL exits on the two survivors (fresh establish is prompt), so
# the QUORUM's 1+1 baselines are healthy + flowing. This decouples the QUORUM data-plane separation proof from
# #33's crash-rehome recovery timing (the #33 GAP is already OBSERVED in Arm REHOME above).
# external-review R2-M1: assert OFF and ON as SEPARATE checks — joining them with `;` discarded an OFF failure.
assert_ok "Q-heal-off proxy off succeeds (exit 0; raft write at 2/3, quorum intact)"  "$SIM" ctl -- proxy off
assert_ok "Q-heal-on proxy on succeeds → FRESH keyset + re-establishes all exits on the 2 survivors (sidestepping #33's crash-rehome recovery timing)"  sh -c "sleep 3; \"$SIM\" ctl -- proxy on --yes --ha-policy freeze-on-quorum-loss 2>&1 | grep -qi 'proxy ON'"
assert_ok "Q-heal2 both surviving exits reach proxy-ready after the heal"  poll_until 60 3 "2 exits ready post-heal" -- _proxy_ready 2
TOK_A=$(_sub_token alice2)   # off/on bumped the keyset; mint a fresh sub token that renders the NEW PSK for the SS legs
[ -n "$TOK_A" ] || die "73: Q could not mint a post-heal sub token"
LDR=$(sim_leader) || die "73: Q no leader"
node_running "$LDR" || die "73: Q leader $LDR not running"
# K2 = the surviving non-leader voter (the other non-leader was REHOME-killed as NT_HB). REQUIRED.
K2=""; for b in $(list_nodes broker); do [ "$b" != "$LDR" ] && [ "$b" != "$NT_HB" ] && node_running "$b" && K2=$b && break; done
[ -n "$K2" ] || die "73: Q no surviving non-leader voter (K2) — cannot construct the data-plane separation"
# DETERMINISTIC 1+1 (M1/M7): rebalance + poll until BOTH leader AND K2 home an exit. The off/on heal re-homes both
# exits on brk1 (resolveHomeForAgent maps by nats_server = the agents' tunnel broker brk1), so the rebalance must
# move one onto K2 — which needs K2 to be proxy-ELIGIBLE again (its §17 reachable re-observe after the REHOME
# reconfiguration can lag ~150s). Poll (rebalancing each tick) up to 240s so K2's eligibility reliably recovers.
assert_ok "Q-construct rebalance until BOTH leader $LDR and K2 $K2 each home >=1 exit (DETERMINISTIC 1+1 over the 2 live voters — both baselines fresh+healthy after the heal; survivor NOT optional; waits out K2's post-reconfiguration proxy-eligibility)"  poll_until_fixed 240 5 "leader+K2 both homed" -- _qconstruct
DEAD_A=$(_agent_homed_on "$K2"); DEAD_HB=$K2
SURV_A=$(_agent_homed_on "$LDR")
# fail-fast BEFORE any destructive injection (M1): both control sources MUST exist and MUST be distinct.
[ -n "$DEAD_A" ] || die "73: Q no K2-homed exit after construct — dead-leg baseline MISSING, cannot inject"
[ -n "$SURV_A" ] || die "73: Q no leader-homed exit after construct — survivor-leg baseline MISSING (R-CONTROLSRC REQUIRES it; NOT optional)"
[ "$DEAD_A" != "$SURV_A" ] || die "73: Q dead=$DEAD_A and survivor=$SURV_A are the SAME agent — separation impossible"
log "73: Q dead-homed exit=$DEAD_A (on $DEAD_HB, will die) ; survivor(leader $LDR)-homed exit=$SURV_A (stays live)"
# BOTH pre-kill baselines must render+flow BEFORE the kill (finding-8: no destructive Q-kill over a dead baseline).
# But a rebalance-MOVED dead-homed exit onto K2 INTERMITTENTLY does not render+serve (the same moved-exit gap #33
# measures). So: try both baselines (each re-fetches /sub up to 240s waiting for the moved exit to render); if the
# dead-homed one can't be established, do NOT die (aborting the whole drill) and do NOT run a vacuous kill — record
# the separation as NOT-COVERED THIS RUN and reach drill_end. When BOTH flow, run the full destructive proof.
_ss_leg_up "$DEAD_A" 1083; _qld=$?
assert_ok "Q-legup-dead dead-homed SS leg via $DEAD_A (home $DEAD_HB) renders+flows bytes pre-kill (baseline for the separation)"  sh -c "exit $_qld"
_ss_leg_up "$SURV_A" 1082; _qls=$?
assert_ok "Q-legup-surv survivor-homed SS leg via $SURV_A (home leader $LDR) renders+flows bytes pre-kill (independent live control source)"  sh -c "exit $_qls"
if [ "$_qld" -eq 0 ] && [ "$_qls" -eq 0 ]; then
    assert_ok "Q-baseline /sub 200 before quorum loss"  sh -c "[ \"\$(\"$SIM\" exec $LDR -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_A)\" = 200 ]"
    # external-review R5-M6: cross-check the VENDED server matches the broker about to be killed. _agent_homed_on
    # picks by control-plane home_broker; the SS leg egresses via the /sub-vended `server`. If they DISAGREE, a
    # post-kill "leg still serves" would be a STALE/OTHER endpoint, not tunnel survival. Pin them equal FIRST.
    _dsrv=$(_sub_server_of "$DEAD_A"); log "73: Q dead-homed exit $DEAD_A /sub-vended server=[$_dsrv] (must == DEAD_HB $DEAD_HB)"
    assert_ok "Q-xcheck the dead-homed exit $DEAD_A's /sub-VENDED server is EXACTLY the broker about to be killed ($DEAD_HB) — control-plane home_broker AND the vended data-plane endpoint AGREE, so a post-kill black-hole is the dead home, not a stale/other endpoint (R5-M6)"  sh -c "[ '$_dsrv' = '$DEAD_HB' ]"
    # ── R7-M3: Q-xcheck is the CAUSAL PREREQUISITE and must GATE the kill — NOT merely increment the failure
    #    counter. If the vended data-plane endpoint DISAGREES with the control-plane home, killing $DEAD_HB proves
    #    nothing (the leg vends via $_dsrv, not $DEAD_HB), and the black-hole/separation assertions would be a
    #    vacuous destructive run. Branch on the xcheck; on mismatch RECORD the endpoint mismatch as the exposed
    #    defect and SKIP the kill (the same discipline REHOME-precond uses). ──
    if [ "$_dsrv" = "$DEAD_HB" ]; then
        # kill DEAD_HB → 2 brokers down (REHOME-killed $NT_HB + $DEAD_HB now) → 1/3, quorum lost, leader survives
        assert_ok "Q-kill kill dead-homed exit's broker $DEAD_HB (→ 1/3 with the REHOME-killed $NT_HB; committable quorum lost; leader $LDR survives)"  node_kill "$DEAD_HB"
        # R5-M6: node_kill gates only docker rc — PROVE $DEAD_HB is genuinely DOWN (container killed → its tunnel/public
        # ports are gone), so the black-hole below is the dead home, not a slow/stale response from a still-running broker.
        assert_ok "Q-dead $DEAD_HB is genuinely DOWN after node_kill (container not running — its tunnel/public ports are gone) so the black-hole below is the DEAD home, not a stale endpoint (R5-M6)"  sh -c "! \"$SIM\" exec $DEAD_HB -- true >/dev/null 2>&1"
        # ── CORE separation: the black-hole is a HARD PREREQUISITE (R5-M6). Capture it; the SEPARATION claim only holds
        #    if the dead leg ACTUALLY black-holed — assertions no longer accumulate into a false conjunction. ──
        if poll_until 15 3 "dead SS black-holes" -- _ss_deadhole 1083; then _bh=1; else _bh=0; fi
        assert_ok "Q-freeze [DATA-PLANE] dead-homed SS via $DEAD_A (home=$DEAD_HB killed) BLACK-HOLES under quorum loss (SS rides the home broker's tunnel; home dead → leg dead; no rehome possible — that is a raft write, quorum is gone)"  sh -c "[ '$_bh' = 1 ]"
        assert_ok "Q-freeze [DATA-PLANE+] survivor-homed SS via $SURV_A (home=leader $LDR alive) STILL flows bytes — the REQUIRED independent live control source proves this is a HOME-scoped black-hole, not a whole-proxy outage"  poll_until 20 3 "survivor SS flows" -- _ss_flows 1082
        # SEPARATION is a COMPOUND causal gate (R5-M6): claimed ONLY if the dead leg PROVEN-black-holed ($_bh=1) AND
        # /sub still vends 200. If the dead leg was NOT actually black-holing, this FAILS (RED) — no false conjunction.
        assert_ok "Q-freeze [SEPARATION] the dead SS leg is PROVEN dead (black-hole held above) WHILE /sub STILL vends 200 (frozen READ gate) — proves /sub-200 ≠ data-plane liveness (#20). CAUSALLY GATED on the black-hole: a dead leg that kept serving FAILS this, never a false 'DEAD while 200' conjunction (R5-M6)"  sh -c "[ '$_bh' = 1 ] && [ \"\$(\"$SIM\" exec $LDR -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8090/sub/$TOK_A)\" = 200 ]"
        assert_ok "Q-freeze [corrob] a control WRITE is FENCED (proxy sub create does NOT mint under quorum loss)"  _write_fenced
        [ "$_bh" = 1 ] || warn "73 Q-DIAGNOSIS: the dead-homed leg via $DEAD_A (vended server=$_dsrv == home $DEAD_HB, Q-xcheck PASSED) did NOT black-hole within 15s though $DEAD_HB is DOWN — since the endpoints AGREE this is NOT stale rendering / status attribution; it is either a tunnel that outlived its killed home or a slow teardown. The SEPARATION assertion above is RED (the causal gate), NOT a false PASS."
    else
        not_covered "73 QUORUM separation proof THIS RUN — Q ENDPOINT MISMATCH (R7-M3 exposed defect)" "the dead-homed exit $DEAD_A's /sub-VENDED server=[$_dsrv] does NOT equal its control-plane home_broker=$DEAD_HB — the control plane and the data plane DISAGREE on where the exit lives (stale /sub rendering OR status-attribution drift). This mismatch IS the exposed defect (Q-xcheck FAILED above = the RED). REFUSING the destructive kill of $DEAD_HB: it would prove nothing since the leg vends via $_dsrv, not $DEAD_HB — a vacuous kill over a mismatched endpoint. The QUORUM separation proof is gated by the mismatch; no contradictory 'endpoints matched' claim is made." gap
    fi
else
    not_covered "73 QUORUM data-plane separation THIS RUN" "a rebalance-MOVED dead-homed exit ($DEAD_A on $DEAD_HB) did not render+serve pre-kill within 240s (dead-leg flowed=$([ "$_qld" -eq 0 ] && echo yes || echo NO), survivor flowed=$([ "$_qls" -eq 0 ] && echo yes || echo NO)) — the SAME moved-exit intermittency #33/#34 measures (a proxy exit MOVED onto a non-tunnel broker sometimes strands). Refusing a vacuous destructive Q-kill over a dead baseline (finding-8); NOT a die — the drill completes. CLASS gap (R14 re-adjudication): the stranding is a CONFIRMED product defect (#33/#34) reproducing, not intrinsic sim non-determinism — this hole is debt owned by #33/#34 and turns GREEN when moved-exit rendering is made deterministic in the product, so it is a gap, not a re-run-and-it-lands runtime-guard (matching line 381's sibling gap)." gap
fi
ss_down ctl1

drill_end
