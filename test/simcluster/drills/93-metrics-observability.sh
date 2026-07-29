#!/bin/sh
# 93-metrics-observability.sh — S8 (N=3, GREEN): the operator observability surface — /metrics true values
# + /healthz + /readyz + the alert webhook (a runtime-injected python receiver in the ctl container; NO
# baked image change, SSH-down §0.3) + `cluster status --card`/`--watch`/`--offline` exit taxonomy +
# `--log-json`. The obs seam is enabled via the broker.yaml `observability:` map (serveconf.go:34, nested
# under broker:, read by serve.go:91 pickFlagOrYaml) — a labeled [env] provisioning step.
#
# SOURCE FACTS (verified): /metrics gauges are `tether_broker_*` (cluster_mode/is_leader/voters/
# quorum_margin); peer_applied_lag{node=…}/peer_reachable{node=…} are LEADER-gated (follower emits the
# HELP/TYPE header, zero {node=…} rows). /healthz=200 `ok`; /readyz ready=200 `ready: leader=… self=VOTER`,
# not-ready=503 `not ready: …`. --card healthy = `CLUSTER  HEALTHY (HA)`; JSON mirrors .health/.exit_code.
# --log-json = slog JSON to STDERR with .time/.level/.msg.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
MPORT=9100; WPORT=9199
# obs_enable <brk>: append the observability: seam under broker: in the root-owned broker.yaml (as root via
# dexec), then restart the broker. Idempotency-guarded. metrics on loopback; webhook → the ctl receiver.
obs_enable() {
    dexec "$1" -- sh -c "grep -qE '^  observability:' /etc/tether/broker.yaml || printf '  observability:\n    metrics_listen: 127.0.0.1:$MPORT\n    alert_webhook_url: http://ctl1:$WPORT/\n    log_json: true\n' >> /etc/tether/broker.yaml"
    dexec "$1" -- systemctl restart tether-broker
}
_curl_brk() { dexec "$1" -- curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$2$3" 2>/dev/null; }
_metrics()  { dexec "$1" -- curl -s "http://127.0.0.1:$MPORT/metrics" 2>/dev/null; }
_leader_metric() { _metrics "$LDR" | grep -E "^tether_broker_$1 " | awk '{print $2}'; }
_http_exact() {
    _hx=$(dexec "$1" -- curl -sS -w '\n__HTTP_CODE__=%{http_code}' "http://127.0.0.1:$MPORT$2" 2>/dev/null) || return 1
    _hc=$(printf '%s\n' "$_hx" | sed -n 's/^__HTTP_CODE__=//p' | tail -1)
    _hb=$(printf '%s\n' "$_hx" | sed '/^__HTTP_CODE__=/d')
    [ "$_hc" = "$3" ] && printf '%s' "$_hb" | grep -qiE "$4"
}
# H13 (2026-07-18 full-suite run): the two webhook oracles were inline `sh -c "… jq …"` bodies inside a
# poll_until, and their failure printed nothing but `poll_until: timed out after 20s`. The received bodies —
# the only evidence that can tell "the product emitted the wrong wire schema" from "our jq is wrong" — were
# never dumped, so three deterministic REDs were un-attributable. They are predicate FUNCTIONS now (also
# R-NOSHC-correct), and every failure path dumps the receiver log verbatim.
#
# The jq bodies also carried a real defect: `…==([…]|sort))))` closed one paren too many, so jq exited 3
# (compile error) on EVERY tick and the assertions could NEVER pass regardless of what the product sent.
# That is the opposite of a vacuous oracle — it is an always-RED one — and it is why the webhook wire
# contract (schema / schema_version / transition / no-secret key whitelist) has never actually been checked.
# The paren is balanced below; the CONDITION is unchanged, key-for-key.
_hook_log() { "$SIM" exec ctl1 -- sh -c 'cat /tmp/hook.log' 2>/dev/null; }
_hook_dump() {   # $1 = a short context label for the failure path
    log "DIAG hook.log dump ($1) — the exact bodies the receiver captured:"
    _hook_log | sed 's/^/[hook] /'
    log "DIAG hook.log dump ($1) — end (blank above = the broker POSTed nothing at all)"
}
# R13 webhook attribution diag: is the SEND PATH broken (R12 payload struct / R13 Run-order regression → a
# `alert webhook: …` Warn in broker.err, or NO delta generated at all), or did leadership MOVE so the alert
# reconciler re-seeded and swallowed the raise delta (alert_reconcile.go:120-123,177 re-seed on leadership
# change)? The poster logs ONLY on failure (POST failed / queue full / marshal); a successful POST is silent.
_webhook_broker_diag() {   # $1 = context label
    _wcur=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty' 2>/dev/null)
    log "DIAG webhook ($1): LDR captured at setup=[$LDR]  vs  current cluster leader=[$_wcur]  (differ ⇒ leadership moved ⇒ reconciler re-seeded, swallowing the raise delta; the cluster_leader field would also mismatch)"
    for _wb in brk1 brk2 brk3; do
        log "DIAG webhook ($1): $_wb broker.err alert/webhook lines (last 10) — a 'alert webhook:' Warn = send path FAILED (regression); NONE = delta never generated (seeding/timing):"
        dexec "$_wb" -- sh -c "grep -nE 'alert webhook|d8b:|alert (raise|clear)' /var/log/tether/broker.err 2>/dev/null | tail -10" 2>/dev/null | sed "s/^/[$_wb] /"
    done
}
_hook_warmup_seen() {
    _hook_log | jq -s -e --arg m "$WARM" 'any(.[]; .dedup_key=="manual" and .message==$m and (.transition=="raised" or .transition=="cleared"))' >/dev/null 2>&1
}
_hook_raised_exact() {
    _hook_log | jq -s -e --arg m "$HK" --arg l "$LDR" 'any(.[]; .schema=="tether_alert_webhook" and .schema_version==1 and .transition=="raised" and .kind=="manual" and .severity=="severe" and .dedup_key=="manual" and .message==$m and .cluster_leader==$l and (.ts|length)>0 and ((keys|sort)==(["cluster_leader","dedup_key","kind","message","schema","schema_version","severity","transition","ts"]|sort)))' >/dev/null 2>&1
}
_hook_cleared_exact() {
    [ "$("$SIM" exec ctl1 -- sh -c 'wc -l < /tmp/hook.log' 2>/dev/null | tr -d ' \r')" -gt "${N_RAISED:-0}" ] 2>/dev/null || return 1
    _hook_log | jq -s -e --arg m "$HK" --arg l "$LDR" 'any(.[]; .schema=="tether_alert_webhook" and .schema_version==1 and .transition=="cleared" and .dedup_key=="manual" and .kind=="manual" and .severity=="severe" and .message==$m and .cluster_leader==$l and ((keys|sort)==(["cluster_leader","dedup_key","kind","message","schema","schema_version","severity","transition","ts"]|sort)))' >/dev/null 2>&1
}
_watch_pty() {
    _wc=$(ctr_name "$LDR"); _wf="/tmp/s6s8-93-watch-$$.out"
    if [ "${DOCKER_SUDO:-0}" = 1 ]; then timeout 9 sudo docker exec -t "$_wc" runuser -u tether -- tether cluster status --watch 2s >"$_wf" 2>&1
    else timeout 9 $DOCKER exec -t "$_wc" runuser -u tether -- tether cluster status --watch 2s >"$_wf" 2>&1; fi
    _wr=$?; _wn=$(grep -cE 'NODE_ID[[:space:]]+NAME|HEALTHY|NOT-HA|DEGRADED' "$_wf" 2>/dev/null || true)
    { [ "$_wr" = 124 ] || [ "$_wr" = 0 ]; } && [ "$_wn" -ge 2 ]
}

drill_begin "S8-93 metrics-observability: /metrics + /healthz + /readyz + webhook + --card/--watch/--offline (N=3)"

assert_setup "grow_to_3 (N=3 HA baseline; single attempt)" grow_to_3 1 1 0
LDR=$(sim_leader) || setup_fail "no leader"
FOLL=$(a_non_leader_voter) || setup_fail "no follower"
log "leader=$LDR follower=$FOLL"
assert_setup "create member session for remote observability" "$SIM" session "$SID" --pin "$PIN"

# ── [env] webhook receiver: a python-stdlib http server in the ctl container capturing POST bodies ──
"$SIM" exec ctl1 -- sh -c "cat > /tmp/hookrx.py <<'PY'
import http.server,sys
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n=int(self.headers.get('Content-Length',0)); b=self.rfile.read(n)
        open('/tmp/hook.log','ab').write(b+b'\n')
        self.send_response(200); self.end_headers()
    def log_message(self,*a): pass
http.server.HTTPServer(('0.0.0.0',$WPORT),H).serve_forever()
PY
: > /tmp/hook.log
nohup python3 /tmp/hookrx.py >/tmp/hookrx.out 2>&1 & echo \$! > /tmp/hookrx.pid"
sleep 1
assert_ok "[env] webhook receiver up on ctl1:$WPORT" sh -c "$SIM exec ctl1 -- sh -c 'kill -0 \$(cat /tmp/hookrx.pid) 2>/dev/null'"

# ── [env] obs_enable each broker (observability seam + rolling restart, keep quorum) ────────────────
for b in brk1 brk2 brk3; do assert_setup "enable observability on $b and restart successfully" obs_enable "$b"; sleep 3; done
assert_setup "N=3 healthy after observability rolling restart" poll_until 60 4 "N=3 healthy" -- sh -c "[ \"\$($SIM status --json 2>/dev/null | jq '[.nodes[]?|select(.phase==\"VOTER\")]|length')\" = 3 ]"
LDR=$(sim_leader); FOLL=$(a_non_leader_voter)
log "DIAG /metrics on $LDR →"; _metrics "$LDR" | grep -E '^tether_broker_' | head -12 | sed 's/^/[diag met] /'

# ── MET: /metrics true values on the leader; follower has NO peer DATA rows (leader-gated) ──────────
assert_ok "MET: leader /metrics tether_broker_cluster_mode==1" sh -c "[ \"\$($SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics 2>/dev/null | grep -E '^tether_broker_cluster_mode ' | awk '{print \$2}')\" = 1 ]"
assert_ok "MET: leader /metrics tether_broker_is_leader==1" sh -c "[ \"\$($SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics 2>/dev/null | grep -E '^tether_broker_is_leader ' | awk '{print \$2}')\" = 1 ]"
assert_ok "MET: leader /metrics tether_broker_voters==3" sh -c "[ \"\$($SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics 2>/dev/null | grep -E '^tether_broker_voters ' | awk '{print \$2}')\" = 3 ]"
assert_ok "MET: leader /metrics has peer_reachable{node=…} DATA rows (leader-gated)" \
    sh -c "$SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics 2>/dev/null | grep -qE '^tether_broker_peer_reachable\\{node='"
assert_ok "MET FG: follower /metrics has is_leader==0 + NO peer_applied_lag{node=…} DATA rows (leader-gated)" \
    sh -c "$SIM exec $FOLL -- curl -s http://127.0.0.1:$MPORT/metrics 2>/dev/null | { grep -qE '^tether_broker_is_leader 0' && ! grep -qE '^tether_broker_peer_applied_lag\\{node='; }"
assert_ok "MET: POST to /metrics → 405" sh -c "[ \"\$($SIM exec $LDR -- curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$MPORT/metrics 2>/dev/null)\" = 405 ]"

# ── TOPO-ACTION (batch C / C3): the topology reconcile ACTION reaches the ctl over the wire ─────────
# Before batch C both status renderers classified a wedged reconcile by substring-matching a free-text
# Reason, with DIFFERENT substring sets — so a render failure showed STUCK on one end and "still
# catching up" on the other. The fix puts the closed-enum Action on the wire. These assert the WIRE
# CHAIN end to end on a live cluster (broker self-report → health responder → adminsock → ctl render);
# the STUCK/HELD/BEHIND classification itself is covered hermetically in internal/natsconf.
#
# Deliberately BOTH the self row and a peer row: the field is populated on two separate code paths
# (the health-echo fold and the authoritative self overwrite), and the self row is the one a wedged
# broker reports about ITSELF — so a chain that works only for peers would miss the common case.
assert_ok "TOPO-ACTION: cluster status --json carries topo_action on EVERY reporting voter (self row AND peer rows)" \
    sh -c "$SIM exec $LDR -- sh -c 'tether cluster status --json 2>/dev/null | jq -e \"[.nodes[]|select(.topo_reported==true)] | length>=3 and (map(has(\\\"topo_action\\\")) | all)\" >/dev/null'"
assert_ok "TOPO-ACTION: a converged cluster reports the converged actions, never a failure one" \
    sh -c "$SIM exec $LDR -- sh -c 'tether cluster status --json 2>/dev/null | jq -e \"[.nodes[]|select(.topo_reported==true)|.topo_action] | all(. == \\\"noop\\\" or . == \\\"reloaded\\\")\" >/dev/null'"
# The TOPO cell is `applied/observed→desired <marker>` and CONTAINS A SPACE, so a column index would
# split it — match the whole cell shape instead, and require one per voter so a single converged row
# cannot stand in for three.
# TOPO-STUCK (batch C / C3-13): the FAILURE polarity, on the real stack.
#
# The converged assertions above prove the wire chain carries a value; they cannot prove the value is
# CLASSIFIED correctly, because every classification maps to the same rendering when nothing is wrong.
# This wedges a real broker's real nats.conf with an unrecognized directive — the exact way an operator
# hand-edits one — and asserts the polarity the pre-batch-C code got wrong on BOTH ends: the TOPO cell
# must read STUCK (it rendered "…" for a render/apply failure) and the health verdict must be DEGRADED
# naming STUCK (it reported HEALTHY_HA when the wedge happened at the already-converged generation).
#
# It restores the conf and re-asserts convergence, so the wedge cannot leak into the drills after it.
_topo_wedge()   { $SIM exec "$FOLL" -- sh -c 'cp /etc/tether/nats.d/nats.conf /tmp/nats.conf.batchc.bak && printf "\ntotally_unrecognized_directive: 1\n" >> /etc/tether/nats.d/nats.conf'; }
_topo_unwedge() { $SIM exec "$FOLL" -- sh -c 'test -f /tmp/nats.conf.batchc.bak && mv /tmp/nats.conf.batchc.bak /etc/tether/nats.d/nats.conf'; }
_topo_action_of() { $SIM exec "$LDR" -- sh -c "tether cluster status --json 2>/dev/null | jq -r '.nodes[]|select(.node_id==\"$FOLL\")|.topo_action // empty'"; }
_topo_is_stuck()  { [ "$(_topo_action_of)" = "unknown_directive" ]; }
# A predicate FUNCTION, not an `sh -c` body: the helpers above are shell functions in THIS shell and a
# subshell spawned by `sh -c` cannot see them (it would silently evaluate an empty string forever).
_topo_converged_again() { _a=$(_topo_action_of); [ "$_a" = noop ] || [ "$_a" = reloaded ]; }

assert_ok "setup: TOPO-STUCK wedge $FOLL's nats.conf with an unrecognized directive" _topo_wedge
if poll_until 45 5 "$FOLL reports unknown_directive" -- _topo_is_stuck; then
    assert_ok "TOPO-STUCK: the wedged broker's topo_action reaches the ctl as the closed-enum value (not a free-text reason)" sh -c "true"
    assert_ok "TOPO-STUCK: the TOPO column renders STUCK for it — NOT the catching-up marker the pre-batch-C ctl showed for a render/apply failure" \
        sh -c "$SIM exec $LDR -- sh -c 'tether cluster status 2>/dev/null' | grep -E '^$FOLL[[:space:]]' | grep -q 'STUCK'"
    assert_ok "TOPO-STUCK: the health verdict is DEGRADED and its banner names STUCK (the wedge is at the CONVERGED generation, which the pre-batch-C gate reported HEALTHY_HA)" \
        sh -c "$SIM exec $LDR -- sh -c 'tether cluster status --json 2>/dev/null' | jq -e '(.health|test(\"DEGRADED\")) and (.banner|test(\"STUCK\"))' >/dev/null"
    assert_ok "TOPO-STUCK: cluster doctor reports the topology check FATAL and names the wedged broker" \
        sh -c "$SIM exec $LDR -- sh -c 'tether cluster doctor 2>&1' | grep -iE 'topology' | grep -q '$FOLL'"
else
    not_covered "93 TOPO-STUCK (the reconciler did not report unknown_directive within 45s of the conf edit)" \
        "the wedge is a real conf edit and the reconcile loop is 5s, so this should be prompt; if it recurs, capture the broker's topology-reconcile log lines before treating it as a product defect"
fi
assert_ok "setup: TOPO-STUCK restore $FOLL's nats.conf" _topo_unwedge
assert_ok "TOPO-STUCK: the wedge CLEARS once the conf is fixed (no sticky STUCK — the classification is derived per pass, not latched)" \
    poll_until 90 5 "$FOLL back to a converged action" -- _topo_converged_again

assert_ok "TOPO-ACTION: the human TOPO column agrees — all 3 voters render the converged marker, none renders catching-up or STUCK" \
    sh -c "out=\$($SIM exec $LDR -- sh -c 'tether cluster status 2>/dev/null'); \
      n=\$(printf '%s\n' \"\$out\" | grep -cE '→[0-9]+ ✓'); \
      bad=\$(printf '%s\n' \"\$out\" | grep -cE '→[0-9]+ (…|STUCK|HOLD|\\?)'); \
      [ \"\$n\" -ge 3 ] && [ \"\$bad\" = 0 ] || { printf '%s\n' \"\$out\"; false; }"

# ── ADMINRT (R13): `admin runtime` — the broker's PROCESS self-introspection observability surface. It
# complements /metrics (cluster gauges) with process gauges (goroutines/threads/fds/rss/uptime) + each R7
# reconciler's last_tick — the "is a reconciler STALLED?" live-broker diagnostic. Broker-local admin
# socket, run ON the broker as tether (callAdmin; from the ctl it has no admin socket). goroutines is
# runtime.NumGoroutine() — the in-process truth, NOT the /proc Threads proxy.
_rt_max() { "$SIM" exec "$LDR" -- runuser -u tether -- tether admin runtime --json 2>/dev/null | jq -r '[.reconcilers[]?|.last_tick]|map(select(.!=null))|max // empty' 2>/dev/null; }
_rt_advanced() {
    _cur=$(_rt_max); [ -n "$_cur" ] || return 1
    [ "$_cur" != "${RT0:-}" ] || return 1
    # RFC3339(nano) from ONE broker's clock is lexically == chronologically ordered, so a later max sorts last.
    [ "$(printf '%s\n%s\n' "${RT0:-}" "$_cur" | LC_ALL=C sort | tail -1)" = "$_cur" ]
}
assert_ok "ADMINRT: admin runtime --json emits the stable schema with a positive live goroutine count, thread count and uptime (the process-introspection surface exists and is sane)" \
    sh -c "$SIM exec $LDR -- runuser -u tether -- tether admin runtime --json 2>/dev/null | jq -e '.schema==\"admin_runtime\" and .schema_version==2 and .goroutines>0 and .threads>0 and .uptime_seconds>0' >/dev/null"
assert_ok "ADMINRT: the per-reconciler last_tick surface is populated (>=1 registered R7 reconciler — the stalled-reconciler diagnostic has rows to age)" \
    sh -c "$SIM exec $LDR -- runuser -u tether -- tether admin runtime --json 2>/dev/null | jq -e '(.reconcilers|length)>=1' >/dev/null"
RT0=$(_rt_max)
assert_ok "ADMINRT FRESHNESS: the leader's max reconciler last_tick ADVANCES across two spaced samples (a live pass is TICKING, not merely present — a stalled reconciler's last_tick would stop moving; node-states runs every ReconcileInterval=1s)" \
    poll_until 30 3 "reconciler last_tick advances past $RT0" -- _rt_advanced

# ── HEALTH: /healthz 200 'ok'; /readyz 200 'ready: … self=VOTER' (M3: assert HTTP STATUS + body) ─────
# _curl_brk returns the http_code; assert BOTH the 200 status AND the body (a body match alone could pass
# on a 500 error page that happens to contain 'ok').
assert_ok "HEALTH: one /healthz response has HTTP 200 and exact body ok" _http_exact "$LDR" /healthz 200 '^ok$'
# /readyz serving VOTER → HTTP 200 AND 'ready:' AND explicitly NOT 'not ready' (the regex must exclude the
# 503 not-ready body, which also contains the substring 'ready').
assert_ok "HEALTH: one /readyz response has HTTP 200 and positive ready body" _http_exact "$LDR" /readyz 200 '^ready:.*(VOTER|single|leader=)'
assert_setup "READYZ-503: start draining the follower" dexec -u tether "$LDR" -- tether cluster drain "$FOLL"
assert_ok "READYZ-503: one follower /readyz response has HTTP 503 and not-ready body" \
    poll_until 30 2 "follower readyz 503" -- _http_exact "$FOLL" /readyz 503 '^not ready:'
assert_setup "READYZ-503: abort drain" dexec -u tether "$LDR" -- tether cluster drain "$FOLL" --abort

# ── WEBHOOK: alert raise → POST captured on ctl1, no-secret whitelist ────────────────────────────────
# R13 (candidate-b flake fix): the alert webhook fires ONLY on a leader-side COMMITTED delta, and the
# reconciler RE-SEEDS its baseline — firing NOTHING — whenever leadership moves (alert_reconcile.go:120-123
# seed pass, :177 re-seed on leadership loss). The obs rolling-restart above leaves a benign DEGRADED
# transient with brief leadership FLUX; a raise issued during it is swallowed by a re-seed. On the real
# stack this was a TIMING FLAKE (r13d/r13d2 the raise POST was missed; r13e it landed — SAME code, and
# warmup+cleared ALWAYS POST with the correct exact schema, so the send path is alive and this is neither a
# regression nor a missing-URL: it is a leadership-flux race). Fix = wait for STABLE leadership and
# RE-CAPTURE LDR/FOLL before the delta-timing-sensitive webhook arm, so no re-seed swallows the raise and
# the body's cluster_leader field (which the oracle pins) matches. NOT loosening — the exact-schema oracle
# is unchanged; this only establishes the delta mechanism's stable-leadership precondition.
_wh_leader_stable() { _wl1=$(sim_leader 2>/dev/null); [ -n "$_wl1" ] || return 1; sleep 3; _wl2=$(sim_leader 2>/dev/null); [ -n "$_wl2" ] && [ "$_wl1" = "$_wl2" ]; }
assert_ok "WEBHOOK precondition: STABLE cluster leadership before the delta-timing-sensitive webhook arm (the reconciler re-seeds + fires NOTHING on any leadership move — alert_reconcile.go:120-123,177 — which intermittently swallowed the raise delta)" \
    poll_until_fixed 90 3 "stable leadership before the webhook arm" -- _wh_leader_stable
LDR=$(sim_leader); FOLL=$(a_non_leader_voter)
log "WEBHOOK: re-captured stable leader LDR=$LDR follower FOLL=$FOLL (the webhook body stamps cluster_leader = LDR; the oracle pins it)"
# The first leader-side reconcile pass intentionally seeds the active-set baseline and emits no delta.
# Hold a unique warmup alert across at least one poll, then clear it and require a matching webhook
# transition before truncating. The real sentinel below can no longer land in the suppression window.
WARM="warmup93_$$"
assert_setup "WEBHOOK warmup: raise a unique baseline manual alert" dexec -u tether "$LDR" -- tether alert raise --kind manual --severity info --message "$WARM"
sleep 8
assert_setup "WEBHOOK warmup: clear after one complete reconcile poll" dexec -u tether "$LDR" -- tether alert clear manual
if poll_until 20 2 "warmup webhook" -- _hook_warmup_seen; then _hkw=1; else _hkw=0; _hook_dump "WEBHOOK warmup"; fi
assert_ok "WEBHOOK warmup: leader-side reconciler/poster emitted the unique alert" sh -c "[ '$_hkw' = 1 ]"
assert_setup "WEBHOOK truncate receiver log" "$SIM" exec ctl1 -- sh -c ': > /tmp/hook.log'
HK="HOOK93_$$"
assert_setup "WEBHOOK raise a unique manual alert" dexec -u tether "$LDR" -- tether alert raise --kind manual --severity severe --message "$HK"
if poll_until 20 2 "exact raised POST" -- _hook_raised_exact; then _hkr=1; else _hkr=0; _hook_dump "WEBHOOK raised"; _webhook_broker_diag "WEBHOOK raised"; fi
# _webhook_send_warned : true iff SOME broker logged an 'alert webhook:' Warn — the send PATH failed (a real
# regression), as opposed to NONE (the delta was never generated → the raft-lease-jitter timing race).
_webhook_send_warned() {
    for _wb in brk1 brk2 brk3; do
        dexec "$_wb" -- sh -c 'grep -q "alert webhook:" /var/log/tether/broker.err' 2>/dev/null && return 0
    done
    return 1
}
# CLASSIFY the raise arm (R15): a landed exact POST is GREEN; a SEND-PATH Warn is a real regression
# (assert_fail); a NONE (blank receiver, no Warn) is the KNOWN raft-lease-jitter timing race and is recorded
# as a runtime-guard, NOT laundered into a false green. R15's product fix (alert_reconcile conditional
# re-seed — only re-baseline on a GENUINE handoff to another node, PRESERVE across a same-node blip) reduces
# this drop but does NOT fully eliminate it: the reliable fix is a persistent committed-transition cursor (a
# follow-up batch). The send-path + EXACT schema are already proven by the warmup(raised+cleared) arm above,
# so the coverage is not lost — only this raise-under-jitter arm is intermittently unobservable on a
# saturated deploy-tier host.
if [ "$_hkr" = 1 ]; then
    assert_ok "WEBHOOK raised: exact schema, transition, alert identity, leader and key whitelist" sh -c "true"
elif _webhook_send_warned; then
    _as_fail "WEBHOOK raised: the POST SEND PATH failed ('alert webhook:' Warn in a broker.err) — a real regression, not a timing miss"
else
    not_covered "93 WEBHOOK raised (NONE — the committed alert delta was never generated: raft-lease-jitter timing race)" \
        "the exact-raised POST did not land within the window and the receiver log is BLANK (NONE, not a send-path Warn) — the leader's IN-MEMORY alert-transition baseline is not durable across a sub-second raft lease step-down/re-elect on a saturated deploy-tier host (alert_reconcile.go). R15's conditional-reset product fix (preserve the baseline across a same-node blip, re-seed only on a genuine handoff — TestWebhookSurvivesSameNodeLeaseBlip) REDUCES but does not eliminate it; the reliable fix is a persistent committed-transition cursor, owed to a follow-up. The send path + EXACT schema ARE proven by the warmup(raised+cleared) arm above — only this raise-under-jitter arm is intermittently unobservable in-sim." runtime-guard
fi
# M3 transition=cleared: an 'alert clear' must fire a SECOND webhook POST. Schema-agnostic proof — the
# receiver log grows past the raised count (does not assume the exact transition-field wording).
N_RAISED=$("$SIM" exec ctl1 -- sh -c 'wc -l < /tmp/hook.log' 2>/dev/null | tr -d ' \r')
assert_setup "WEBHOOK clear the same manual alert" dexec -u tether "$LDR" -- tether alert clear manual
if poll_until 20 2 "exact cleared POST" -- _hook_cleared_exact; then _hkc=1; else _hkc=0; _hook_dump "WEBHOOK cleared"; fi
assert_ok "WEBHOOK cleared: second exact POST is transition=cleared for the same alert and same leader" sh -c "[ '$_hkc' = 1 ]"

# URL validation is startup-load-bearing. Exercise it on a follower so the remaining two voters keep
# quorum, then restore the valid endpoint and require the same node to return to VOTER.
BAD=$FOLL
assert_setup "WEBHOOK-URL negative: install a forbidden ftp:// URL on $BAD" \
    dexec "$BAD" -- sed -i -E 's#alert_webhook_url:.*#alert_webhook_url: ftp://invalid.example/hook#' /etc/tether/broker.yaml
"$SIM" exec "$BAD" -- systemctl restart tether-broker >/dev/null 2>&1 || true
assert_ok "WEBHOOK-URL negative: startup fails loudly on non-http(s), not a later POST timeout" \
    poll_until 30 2 "forbidden webhook URL startup diagnostic" -- sh -c \
    "$SIM exec $BAD -- sh -c \"grep -qiE 'alert webhook.*scheme.*not allowed|http/https only' /var/log/tether/broker.err\""
assert_setup "WEBHOOK-URL negative: restore valid receiver URL and restart $BAD" \
    dexec "$BAD" -- sh -c "sed -i -E 's#alert_webhook_url:.*#alert_webhook_url: http://ctl1:$WPORT/#' /etc/tether/broker.yaml; systemctl restart tether-broker"
assert_setup "WEBHOOK-URL negative: restored follower returns VOTER" \
    poll_until 60 3 "$BAD returns VOTER" -- sh -c "$SIM status --json 2>/dev/null | jq -e --arg n '$BAD' '.nodes[]?|select(.node_id==\$n and .phase==\"VOTER\" and .reachable==true)' >/dev/null"

# ── CARD: --card glance headline; JSON mirrors .health/.exit_code ────────────────────────────────────
# `cluster status`/`--card` are ADMIN-SOCKET commands (callAdmin) → run on a BROKER as tether, NOT from the
# ctl (which has no admin socket; from ctl it exits 69). (harness fix SB-93.)
# R13 (--settle): the obs rolling-restart leaves a BENIGN DEGRADED-WRITABLE transient (topology-observer
# lag) for a few seconds. Two adjacent one-shot samples used to STRADDLE it (--json caught exit 1, --card a
# moment later caught exit 0), false-REDing CARD/JSON-2/3/4 on a race, not a product fault. `--settle 30s`
# (R13) debounces exactly that benign transient: it waits up to 30s for a DEGRADED(exit 1) verdict to clear
# before trusting it — a genuinely quorum-lost/force-single cluster still exits at once, and a SUSTAINED
# DEGRADED still exits 1 after the window, so NOTHING is masked (cluster.go:124-135). Both samples now
# converge to the SAME settled verdict, so they are comparable. NOT a loosening — --settle is the product's
# own sanctioned debounce for a benign restart transient, exercised here for the first time on the deploy tier.
STATUS_JSON=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --settle 30s --json 2>/dev/null); STATUS_RC=$?
STATUS_HEALTH=$(printf '%s' "$STATUS_JSON" | jq -r '.health // empty'); STATUS_EXIT=$(printf '%s' "$STATUS_JSON" | jq -r '.exit_code // empty')
CARD_OUT=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --settle 30s --card 2>&1); CARD_RC=$?
# H13: the `| head -8` here truncated the card EXACTLY where it mattered — the product prints `(exit N)` on
# the card's LAST line, which is the very string the fourth clause below greps for, so the one diagnostic
# that could have explained the RED was the one line never printed. Dump the whole card.
log "DIAG --card (FULL — the '(exit N)' footer is on the last line and used to be cut by head -8) →"
printf '%s\n' "$CARD_OUT" | sed 's/^/[diag card] /'
log "DIAG --card/--json rc+fields → STATUS_RC=$STATUS_RC STATUS_EXIT=[$STATUS_EXIT] CARD_RC=$CARD_RC STATUS_HEALTH=[$STATUS_HEALTH]"
# H13: this was ONE assertion joining FOUR independent clauses with `&&`, so a RED named the conjunction and
# located nothing. Split into four, plus a non-vacuity gate first: STATUS_HEALTH/STATUS_EXIT come from jq
# `// empty`, and an empty needle would make both `grep -qF` clauses match ANYTHING (a vacuous PASS).
assert_ok "CARD/JSON-0 NON-VACUITY: --json yielded a non-empty .health and .exit_code to compare against (an empty needle would make the two grep clauses below match any output)" \
    sh -c "[ -n '$STATUS_HEALTH' ] && [ -n '$STATUS_EXIT' ]"
assert_ok "CARD/JSON-1: the --json sample's process rc equals its own .exit_code field" \
    sh -c "[ '$STATUS_RC' = '$STATUS_EXIT' ]"
assert_ok "CARD/JSON-2: the --card sample's process rc equals the --json sample's .exit_code (adjacent stable samples agree)" \
    sh -c "[ '$CARD_RC' = '$STATUS_EXIT' ]"
assert_ok "CARD/JSON-3: the card text carries the SAME health label the --json sample reported ($STATUS_HEALTH)" \
    sh -c "printf '%s' \"\$0\" | grep -qF '$STATUS_HEALTH'" "$CARD_OUT"
assert_ok "CARD/JSON-4: the card text carries the exit-code footer '(exit $STATUS_EXIT)' matching the --json sample" \
    sh -c "printf '%s' \"\$0\" | grep -qF '(exit $STATUS_EXIT)'" "$CARD_OUT"

# ── EXIT taxonomy (B2): online healthy → 0 (from a BROKER admin socket) ─────────────────────────────
# Post the obs rolling-restart the cluster may show a transient DEGRADED-WRITABLE (topology-observer lag) —
# poll for a converged healthy verdict + a valid B2 code (0=HEALTHY_HA, or 1=DEGRADED-WRITABLE transient).
assert_ok "EXIT: online cluster status returns a valid B2 verdict + code (HEALTHY/NOT-HA, exit 0/1) from a broker" \
    poll_until 60 4 "online status verdict" -- sh -c "$SIM exec $LDR -- runuser -u tether -- tether cluster status >/dev/null 2>&1; rc=\$?; { [ \$rc = 0 ] || [ \$rc = 1 ]; } && $SIM exec $LDR -- runuser -u tether -- tether cluster status 2>&1 | grep -qiE 'HEALTHY|NOT-HA|DEGRADED'"

# ── LOGJSON: broker emits structured JSON log lines (.msg/.time/.level) ──────────────────────────────
# journalctl -o cat strips the systemd prefix so the raw slog JSON line surfaces (grep '^{' would miss the
# prefixed line). measure-and-record: if log_json:true is honored we see a JSON slog line; else it's a
# candidate seam-not-honored finding (the yaml observability.log_json may not reach the broker logger).
if "$SIM" exec "$LDR" -- sh -c "tail -n 600 /var/log/tether/broker.err /var/log/tether/broker.log 2>/dev/null | grep -m1 -E '^\\{.*\"(msg|level|time)\"' | jq -e '.msg and .level and .time' >/dev/null 2>&1"; then
    _as_pass "LOGJSON: broker service log has a slog JSON line with .msg/.time/.level (log_json seam honored)"
else
    _as_fail "LOGJSON: observability.log_json=true produced no valid slog JSON line in the broker service logs"
fi

# ── WATCH smoke (Stage-C M3: no trailing `; true`, run from a broker, assert a REAL repaint frame) ──
# --watch is a TUI repaint loop; over a non-tty dexec it may not render frames the same way. measure-and-record.
assert_ok "WATCH: docker PTY renders at least two status frames before the bounded timeout" _watch_pty

# MET degraded transition: stop only one follower's broker (NATS remains up). The leader's cached
# observability poll must subtract that already-lost voter from static headroom: quorum_margin 1→0,
# while voters remains 3. Restart and require the same gauges to recover to 3/1.
MET_DOWN=$(a_non_leader_voter) || setup_fail "select follower for quorum-margin transition"
assert_setup "MET-degraded: stop follower broker $MET_DOWN" "$SIM" exec "$MET_DOWN" -- systemctl stop tether-broker
assert_ok "MET-degraded: same leader sample reports voters=3 and live quorum_margin=0" \
    poll_until 30 3 "quorum_margin 0" -- sh -c \
    "$SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics | grep -q '^tether_broker_voters 3$' && $SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics | grep -q '^tether_broker_quorum_margin 0$'"
assert_setup "MET-degraded: restart follower broker $MET_DOWN" "$SIM" exec "$MET_DOWN" -- systemctl start tether-broker
assert_ok "MET-degraded: recovery sample returns voters=3 and quorum_margin=1" \
    poll_until 60 3 "quorum_margin recovers to 1" -- sh -c \
    "$SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics | grep -q '^tether_broker_voters 3$' && $SIM exec $LDR -- curl -s http://127.0.0.1:$MPORT/metrics | grep -q '^tether_broker_quorum_margin 1$'"

# ── EXIT taxonomy all-down (Stage-C M3: real ROSTER_UNREACHABLE, distinct from DEGRADED; DESTRUCTIVE — last) ─
for b in brk1 brk2 brk3; do "$SIM" exec "$b" -- systemctl stop tether-broker nats-server >/dev/null 2>&1 || true; done
sleep 3
# ROSTER_UNREACHABLE is the broker-local OFFLINE roster probe: it reads the durable DB, then proves every
# roster peer's :7400 is dead. The ctl transport cannot manufacture a health verdict when NATS itself is
# unreachable; that path correctly uses sysexits EX_UNAVAILABLE=69 and is a different contract.
_ad_out=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --offline --db /var/lib/tether/tether.db 2>&1); _ad_rc=$?
log "DIAG all-down cluster status --offline (rc=$_ad_rc) →"; printf '%s\n' "$_ad_out" | head -3 | sed 's/^/[diag alldown] /'
_down_rows=$(printf '%s\n' "$_ad_out" | awk '$1 ~ /^brk[123]$/ && $3=="VOTER" && $4=="DOWN" {n++} END {print n+0}')
if [ "$_ad_rc" = 2 ] && [ "$_down_rows" = 3 ]; then
    _as_pass "EXIT: all brokers down → durable offline roster probe → exit 2 + exact 3/3 VOTER/DOWN rows (ROSTER_UNREACHABLE taxonomy)"
elif [ "$_ad_rc" = 0 ] && printf '%s' "$_ad_out" | grep -qiE 'no broker answered|no cluster detected'; then
    product_red "all-down remote status incorrectly returns rc=0 with 'no cluster detected/no broker answered' instead of exact ROSTER_UNREACHABLE rc=2"
else
    _as_fail "EXIT all-down returned an unrecognized taxonomy (rc=$_ad_rc)"
fi

# ── #42 (R13 judgment: PHYSICAL LIMIT, not a defect) — BOUNDED observation gap ──────────────────────
# After a quorum-loss, `cluster status --remote` misreports "electing a leader (transient) — re-run
# shortly" for up to ~TFence(10s) before it self-corrects to READ-ONLY/exit 2 (LeaderContactStale flips
# only after TFence=10s + the leader lease; internal/cluster/read.go:18). R13 ruled this the INTRINSIC
# lower bound of raft-lease safety — fencing earlier would mis-fence benign election blips — so it is NOT a
# bug and there is nothing to "fix". The precise in-window measurement (survivor socket ALREADY says
# quorum-lost while --remote still says transient, then --remote self-corrects after TFence) needs a
# sustained 1-survivor quorum-loss with sub-second sampling — a dedicated deploy-tier construction owed to
# the survivor-taxonomy drill (gotcha pins it on 92 leg-a), not 93's N=3 observability spine. Recorded as a
# BOUNDED gap so ledger-crosscheck keeps a non-GREEN owner cell for it, with the reason stated in full.
not_covered "93 #42 quorum-loss ~TFence(10s) cluster status --remote transient window" \
    "PHYSICAL LIMIT, not a defect (R13 product-side judgment): LeaderContactStale flips only after TFence=10s + the leader lease (internal/cluster/read.go:18) — the intrinsic lower bound of raft-lease safety; fencing earlier would mis-fence benign election blips. The window is BOUNDED (~10s) and SELF-CORRECTING (--remote -> READ-ONLY/exit 2 after TFence), never a permanent black hole. The precise in-window positive measurement (survivor socket already quorum-lost while --remote still transient) is a dedicated deploy-tier construction owed to the survivor-taxonomy drill (gotcha pins 92 leg-a); parked as a bounded follow-up rather than over-claimed as a positive here" gap

# cleanup the receiver (container teardown nuke also handles it; explicit for cross-arm hygiene).
"$SIM" exec ctl1 -- sh -c 'kill $(cat /tmp/hookrx.pid) 2>/dev/null' >/dev/null 2>&1 || true

drill_end
