#!/bin/sh
# 78-proxy-dial-backoff.sh — deploy-tier regression for gotcha #78: a node whose proxy tunnel dial can
# never establish must BACK OFF (not dial every 5s forever), the broker's `read REGISTER` WARN must be
# damped (not one line per junk connection), an operator's explicit proxy off/on must bypass any backoff
# window instantly, and `proxy.participate: false` must take the node out of the egress pool entirely
# (allocation freed, zero dials) and back in when flipped.
#
# WHAT THIS IS — AND HONESTLY IS NOT (the 98-header IS/IS-NOT convention):
#   IS : the "#78 driver" on the real stack — an agent that CANNOT establish the tunnel, driven by the
#        broker's real 5s heartbeat repair loop, on real systemd + real iptables.
#   NOT: a byte-exact reproduction of the live WSL shape (TCP establishes, bytes die mid-handshake —
#        needs a middlebox this sim does not front). fault_reject_on (fast refuse) reproduces the
#        DRIVER — dial-never-establishes, retried forever — which is what #78's product fixes gate on.
#        The broker-WARN arm (C) uses establish-then-close junk connections, which IS byte-exact for
#        the read-REGISTER EOF class.
#
# ATTEMPT ORACLE — the netfilter packet counter, NOT product logs: the #78 fix deliberately stops the
# product from restating every attempt (agent WARNs first-of-run then Debug; broker WARNs damped), so
# counting log lines under-counts BY DESIGN. The REJECT rule's packet counter sees one SYN per
# connect() regardless of what anyone logs — a product-independent cadence witness.
#
# origin: docs/deploy-tier-gotchas.md #78 (plan: docs/reviews/g75-g78-deploy-defaults-plan.md D8)
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/fault.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
TUNNEL_PORT=7000

_cleanup() { fault_cleanup_all 2>/dev/null; true; }
drill_install_traps _cleanup

# review F5: the drill MUST carry a verdict frame — drill_begin/drill_end emit
# the DRILL-VERDICT line that expected-verdicts.tsv compares against. Without
# it the run just exits rc=0 and cannot be judged.
drill_begin "78-proxy-dial-backoff (#78: proxy first-dial backoff + WARN damping + participate opt-out)"

CTL() { "$SIM" ctl -- "$@"; }
_proxy_ready_n()  { _n=$(CTL proxy status --json 2>/dev/null | jq '[.nodes[]?|select(.ready==true)]|length' 2>/dev/null); [ "${_n:-0}" -ge "$1" ]; }
_proxy_opted_out(){ CTL proxy status --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1" and .opted_out==true)' >/dev/null 2>&1; }
# review F5 (problem B): a CURRENT-SHELL predicate — assert_ok inherits it, so
# NO `sh -c` (a fresh shell loses the function and empty command substitutions
# make the oracle vacuously pass — the same B1/M3 scope defect). Reads status
# ONCE and fails closed on an empty/invalid read or an ambiguous row set: agt1
# must have exactly one row with public_port 0 (freed), or be absent entirely
# (opted-out nodes need not appear). Either way "no live public port".
_agt1_proxy_port_freed() {
    _st=$(CTL proxy status --json 2>/dev/null) || return 1
    [ -n "$_st" ] || return 1
    printf '%s' "$_st" | jq -e . >/dev/null 2>&1 || return 1   # valid JSON
    _n=$(printf '%s' "$_st" | jq '[.nodes[]?|select(.nid=="agt1")]|length' 2>/dev/null)
    case "$_n" in
        0) return 0 ;;                       # row absent → no public port
        1) ;;                                # exactly one row → check its port
        *) return 1 ;;                       # ambiguous → fail closed
    esac
    _port=$(printf '%s' "$_st" | jq -r '.nodes[]?|select(.nid=="agt1")|.public_port // 0' 2>/dev/null)
    [ "$_port" = 0 ]
}

# ── topology: single broker + one agent + ctl (the racknerd shape) ─────────────────────────────
assert_ok "up 1 broker + 1 agent + 1 ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (N=1: auth_callout + JS bootstrap)" "$SIM" init brk1
assert_ok "session $SID + ctl login (owner)" "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1 (onboards + tunnel to brk1)" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"

# ── Arm A: dial backoff under a never-establishing tunnel ──────────────────────────────────────
# Inject BEFORE proxy on so the very first establish attempt already fails — the live shape (the WSL
# node could never establish, from the first directive on).
assert_ok "A0 inject: agt1 -> :$TUNNEL_PORT REJECTs (dial can never establish)" \
    fault_reject_on agt1 "$TUNNEL_PORT"
assert_ok "A1 proxy on (owner, confirmed)" sh -c "\"$SIM\" ctl -- proxy on --yes 2>&1 | grep -qi 'proxy ON'"

# 195s observation window in three 65s buckets. The backoff is proven TWO ways that do NOT depend on
# the fault's fail-SPEED:
#   (1) TOTAL vs the no-backoff baseline: the broker's 5s heartbeat repair pushes ~195/5 = ~39 times
#       over the window, so the OLD fixed-5s behavior would dial ~39 times. Backoff must cut this by
#       at least half (<= 20). This holds whether the dial fails fast (REJECT, ~0s) or slow (a real
#       half-open timeout, ~5s).
#   (2) DECREASING TREND across buckets (bucket-3 < bucket-1): the schedule deepens, so later windows
#       hold strictly fewer attempts. This is the shape the old fixed 5s could never produce
#       (it puts a flat ~13 in EVERY bucket).
# We deliberately do NOT assert an absolute bucket-3 count: a fast REJECT (~0s per attempt) packs more
# attempts into each window than a slow timeout, so an absolute threshold would be fault-speed-coupled.
_pk0=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk0=${_pk0:-0}
sleep 65; _pk1=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk1=${_pk1:-0}
sleep 65; _pk2=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk2=${_pk2:-0}
sleep 65; _pk3=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk3=${_pk3:-0}
_d1=$((_pk1 - _pk0)); _d2=$((_pk2 - _pk1)); _d3=$((_pk3 - _pk2)); _dtot=$((_pk3 - _pk0))
log "78 A: dial attempts per 65s bucket: $_d1 / $_d2 / $_d3 (total $_dtot; no-backoff baseline ~39)"
# assert_ok runs these as FUNCTIONS inheriting this shell (NOT sh -c — a fresh shell would lose the
# $_d* globals to empty and make every arithmetic test a false-GREEN; the B1/M3 lesson).
_bucket1_dialed()       { [ "$_d1" -ge 2 ]; }
_total_under_baseline() { [ "$_dtot" -le 20 ]; }
_trend_decreasing()     { [ "$_d3" -lt "$_d1" ]; }
# Anti-vacuous FIRST: if nothing ever dialed, the injection missed the path and every bound below
# would pass on air.
assert_ok "A2 anti-vacuous: the agent really was dialing (bucket-1 attempts >= 2)" \
    _bucket1_dialed
assert_ok "A3 backoff cut the total well below the no-backoff baseline (total <= 20 vs ~39 at a fixed 5s)" \
    _total_under_baseline
assert_ok "A4 backoff trend: bucket-3 < bucket-1 (strictly fewer attempts as the schedule deepens)" \
    _trend_decreasing
# While held back, the node must stay honestly unready (never fake-ready over a dead tunnel).
assert_ok "A5 suppressed node stays unready in proxy status" \
    sh -c "! \"$SIM\" ctl -- proxy status --json 2>/dev/null | jq -e '.nodes[]?|select(.nid==\"agt1\" and .ready==true)' >/dev/null"

# ── Arm B: operator bypass — off/on mints a new epoch and must not wait out any window ─────────
assert_ok "B0 heal the tunnel path" fault_partition_off agt1
assert_ok "B1 proxy off (owner)" sh -c "\"$SIM\" ctl -- proxy off 2>&1 | grep -qi 'proxy OFF'"
assert_ok "B2 proxy on again (owner, confirmed)" sh -c "\"$SIM\" ctl -- proxy on --yes 2>&1 | grep -qi 'proxy ON'"
# 60s, not 10s: generous under parallel drills, still 5x tighter than the 5min cap the old failure
# would have to sit out — full discrimination retained.
assert_ok "B3 the new epoch establishes within 60s (no backoff hangover from arm A)" \
    poll_until 60 3 "agt1 proxy ready after off/on" -- _proxy_ready_n 1

# ── Arm C: broker read-REGISTER WARN damping — NOT-COVERED (constructability limit, honestly) ──
# The `tunnel server: read REGISTER` WARN site lives AFTER the TLS handshake (the control listener is
# tls.NewListener, tunnel.go:285). To reach it a connection must COMPLETE TLS against the broker's
# tunnel cert, THEN fail the REGISTER read (EOF / read-deadline) — the true half-open shape the live
# WSL incident had. A raw connect-then-close from the sim (no TLS, no CA) fails at the TLS layer and
# hits the `accept` WARN site instead — it never reaches read-REGISTER (empirically: WARN delta 0 on a
# 60-conn raw storm). Forging a TLS-completing-then-EOF client without the tether binary is the same
# middlebox the drill header's [NOT] clause already calls unconstructable here. So this face is
# left to the hermetic tests (internal/tunnel/register_log_damping_test.go: per-class damping under an
# interleaved storm, no-recovery-on-unauthorized-line, per-Cap reaffirmation) — NOT faked with a
# raw-TCP storm that would either false-GREEN or false-RED. (Mandate: expose, don't compensate.)
not_covered "78 C broker read-REGISTER WARN damping (#78 face ②)" \
    "the WARN site is post-TLS; a raw junk-TCP storm fails at the TLS handshake and hits the accept site, not read-REGISTER (measured WARN delta 0). The half-open TLS-completed-then-EOF shape needs a real TLS middlebox this sim does not front (same limit as the drill header's WSS [NOT] clause). Damping is hermetically covered by internal/tunnel/register_log_damping_test.go." gap

# ── Arm D: proxy.participate=false — out of the pool, allocation freed, zero dials; flip back ──
assert_ok "D0 provision agt1 with proxy.participate=false (real agent.yaml + flagless restart, ONLINE again)" \
    agent_provision_yaml agt1 "$SID" "nats://brk1:4222" proxyoptout
# Re-arm the counter: an opted-out node must produce ZERO dial attempts even though the session
# proxy switch is still ON and the broker's repair loop is running.
assert_ok "D1 re-arm the dial counter" fault_reject_on agt1 "$TUNNEL_PORT"
_pk_d0=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk_d0=${_pk_d0:-0}
sleep 60
_pk_d1=$(fault_reject_pkts agt1 "$TUNNEL_PORT"); _pk_d1=${_pk_d1:-0}
assert_ok "D2 opted-out node makes ZERO dial attempts across 60s of live repair loop" \
    sh -c "[ '$((_pk_d1 - _pk_d0))' -eq 0 ]"
assert_ok "D3 proxy status marks agt1 opted-out (won't, not can't)" _proxy_opted_out
assert_ok "D4 the __proxy__ allocation is freed (agt1 status row carries no public port, read from real product state)" \
    _agt1_proxy_port_freed
# Recovery: flip back — a broken recovery path would make opt-out a silent one-way switch.
assert_ok "D5 heal the tunnel path for the recovery leg" fault_partition_off agt1
assert_ok "D6 provision agt1 back with proxy.participate=true (ONLINE again)" \
    agent_provision_yaml agt1 "$SID" "nats://brk1:4222" proxyoptin
assert_ok "D7 the node re-enters the pool and serves again within 90s" \
    poll_until 90 3 "agt1 proxy ready after opt back in" -- _proxy_ready_n 1

# review F5: emit the DRILL-VERDICT frame — with arm C's not_covered gap this
# resolves to INCOMPLETE / nc_gap=1, matching expected-verdicts.tsv.
drill_end
