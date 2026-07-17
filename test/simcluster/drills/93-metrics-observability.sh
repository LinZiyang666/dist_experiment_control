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
# The first leader-side reconcile pass intentionally seeds the active-set baseline and emits no delta.
# Hold a unique warmup alert across at least one poll, then clear it and require a matching webhook
# transition before truncating. The real sentinel below can no longer land in the suppression window.
WARM="warmup93_$$"
assert_setup "WEBHOOK warmup: raise a unique baseline manual alert" dexec -u tether "$LDR" -- tether alert raise --kind manual --severity info --message "$WARM"
sleep 8
assert_setup "WEBHOOK warmup: clear after one complete reconcile poll" dexec -u tether "$LDR" -- tether alert clear manual
assert_ok "WEBHOOK warmup: leader-side reconciler/poster emitted the unique alert" \
    poll_until 20 2 "warmup webhook" -- sh -c "$SIM exec ctl1 -- sh -c 'cat /tmp/hook.log' | jq -s -e --arg m '$WARM' 'any(.[]; .dedup_key==\"manual\" and .message==\$m and (.transition==\"raised\" or .transition==\"cleared\"))' >/dev/null"
assert_setup "WEBHOOK truncate receiver log" "$SIM" exec ctl1 -- sh -c ': > /tmp/hook.log'
HK="HOOK93_$$"
assert_setup "WEBHOOK raise a unique manual alert" dexec -u tether "$LDR" -- tether alert raise --kind manual --severity severe --message "$HK"
assert_ok "WEBHOOK raised: exact schema, transition, alert identity, leader and key whitelist" \
    poll_until 20 2 "exact raised POST" -- sh -c "$SIM exec ctl1 -- sh -c 'cat /tmp/hook.log' | jq -s -e --arg m '$HK' --arg l '$LDR' 'any(.[]; .schema==\"tether_alert_webhook\" and .schema_version==1 and .transition==\"raised\" and .kind==\"manual\" and .severity==\"severe\" and .dedup_key==\"manual\" and .message==\$m and .cluster_leader==\$l and (.ts|length)>0 and ((keys|sort)==([\"cluster_leader\",\"dedup_key\",\"kind\",\"message\",\"schema\",\"schema_version\",\"severity\",\"transition\",\"ts\"]|sort))))' >/dev/null"
# M3 transition=cleared: an 'alert clear' must fire a SECOND webhook POST. Schema-agnostic proof — the
# receiver log grows past the raised count (does not assume the exact transition-field wording).
N_RAISED=$("$SIM" exec ctl1 -- sh -c 'wc -l < /tmp/hook.log' 2>/dev/null | tr -d ' \r')
assert_setup "WEBHOOK clear the same manual alert" dexec -u tether "$LDR" -- tether alert clear manual
assert_ok "WEBHOOK cleared: second exact POST is transition=cleared for the same alert and same leader" \
    poll_until 20 2 "exact cleared POST" -- sh -c "[ \"\$($SIM exec ctl1 -- sh -c 'wc -l < /tmp/hook.log' 2>/dev/null | tr -d ' \r')\" -gt '${N_RAISED:-0}' ] && $SIM exec ctl1 -- sh -c 'cat /tmp/hook.log' | jq -s -e --arg m '$HK' --arg l '$LDR' 'any(.[]; .schema==\"tether_alert_webhook\" and .schema_version==1 and .transition==\"cleared\" and .dedup_key==\"manual\" and .kind==\"manual\" and .severity==\"severe\" and .message==\$m and .cluster_leader==\$l and ((keys|sort)==([\"cluster_leader\",\"dedup_key\",\"kind\",\"message\",\"schema\",\"schema_version\",\"severity\",\"transition\",\"ts\"]|sort))))' >/dev/null"

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
STATUS_JSON=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --json 2>/dev/null); STATUS_RC=$?
STATUS_HEALTH=$(printf '%s' "$STATUS_JSON" | jq -r '.health // empty'); STATUS_EXIT=$(printf '%s' "$STATUS_JSON" | jq -r '.exit_code // empty')
CARD_OUT=$("$SIM" exec "$LDR" -- runuser -u tether -- tether cluster status --card 2>&1); CARD_RC=$?
log "DIAG --card →"; printf '%s\n' "$CARD_OUT" | sed 's/^/[diag card] /' | head -8
assert_ok "CARD/JSON: adjacent stable samples agree on the same health label and exit code" \
    sh -c "[ '$STATUS_RC' = '$STATUS_EXIT' ] && [ '$CARD_RC' = '$STATUS_EXIT' ] && printf '%s' \"\$0\" | grep -qF '$STATUS_HEALTH' && printf '%s' \"\$0\" | grep -qF '(exit $STATUS_EXIT)'" "$CARD_OUT"

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

# cleanup the receiver (container teardown nuke also handles it; explicit for cross-arm hygiene).
"$SIM" exec ctl1 -- sh -c 'kill $(cat /tmp/hookrx.pid) 2>/dev/null' >/dev/null 2>&1 || true

drill_end
