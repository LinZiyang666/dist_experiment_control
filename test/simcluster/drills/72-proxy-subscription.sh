#!/bin/sh
# 72-proxy-subscription.sh — S4 GREEN (N=1 cluster + 2 agent + ctl): the proxy subscription journey on the real
# out-of-process auth_callout stack. owner-only + member-readable no-secret → sub create (URL once) → /sub Clash
# body (loopback verify + cross-container via S0-ingress) + forged-token 404 → real Shadowsocks dual-leg (agt1
# allow_private POSITIVE + agt2 default-deny NEGATIVE + wrong-PSK AEAD) → sub revoke (an ESTABLISHED in-flight alice
# stream is FORCE-CLOSED while bob's held stream survives + a NEW conn through the revoked PSK black-holes, recover)
# → proxy off (exit torn down: a held/new SS conn black-holes + the __proxy__ public port is RECLAIMED + /sub 0
# nodes). R5-M1 (2026-07-14): in-flight force-close is BYTE-OBSERVED (a held-open THREADED-slow-sink SS stream
# whose received-byte count is proven STRICTLY GROWING pre-revoke — REV-hold-base — so a stalled/never-connected
# curl cannot false-green it; alice force-closes early WHILE bob keeps transferring), and __proxy__ reclaim checks
# the OS listener (OFF-port-reclaim) AND the port_allocations SOURCE (OFF-alloc-reclaim) AND safe same-port reuse
# (OFF-reuse). Each FAILS (exposes the gap) if tether does not force-close a transferring stream / leaks the port.
# Consumes S0-隧道 (SS egress rides the tunnel) + S0-ingress (/sub cross-container). U1-pinned: init'd N=1 cluster.
#
# FALSE-GREEN GUARDS (plan §10-72):
#  - /sub cross-container goes through the ingress front (product loopback:8090 stays loopback-only; no dev-no-auth).
#  - SS positive + negative legs asserted in the SAME run ("all pass" and "all fail" are both false-greens).
#  - wrong-PSK produces NO blocked-dest log (distinguishes AEAD trial-decrypt failure from dest-policy block).
#  - forged /sub token and a revoked token BOTH 404 (byte-identical → no existence oracle).
#  - revoke closes on "new conn refused + bob still relays + alice2 recovers", never on "everything stopped".
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/proxy.sh"; . "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): a POSIX INT/TERM handler RETURNS to the next command, so
# the old combined trap tore the SS/ingress sidecars down and then RESUMED the drill's revoke/token steps.
# drill_install_traps registers EXIT on its own and exits 128+signo on INT/TERM. Cleanup body unchanged.
_cleanup() { ss_down ctl1 2>/dev/null; ingress_down brk1 443 2>/dev/null; }
drill_install_traps _cleanup
CTL() { "$SIM" ctl -- "$@"; }

# ── drill-local predicates (assert_ok runs these in a command-sub that INHERITS functions; NOT `sh -c`) ──
_setup_ingress() {
    secrets_mint_ingress "$INSTANCE" brk1 && ingress_up brk1 443 ingress "/sub/=127.0.0.1:8090" \
        && ingress_trust_inject ctl1
}
_member_login()  { CTLH mem login -s "$SID" --pin "$PIN" --broker nats://brk1:4222 >/dev/null 2>&1; }
_g3_no_yes()     { "$SIM" ctl -- proxy on 2>&1 | grep -qiE 'aborted|--yes|confirm|refus'; }
_proxy_on_owner(){ "$SIM" ctl -- proxy on --yes 2>&1 | grep -qi 'proxy ON'; }
# proxy exit readiness is async: after `proxy on`, each agent must receive the directive + start its SS
# server before it shows ready:true (and appears as an ss proxy in /sub). A too-fast /sub fetch sees
# proxies:[] (DIRECT fallback). Poll until >= N agents are ready.
_proxy_ready()   { _n=$("$SIM" ctl -- proxy status --json 2>/dev/null | jq '[.nodes[]?|select(.ready==true)]|length' 2>/dev/null); [ "${_n:-0}" -ge "$1" ]; }
_member_status_nosecret() {
    _o=$(CTLH mem proxy status 2>&1) || return 1
    printf '%s' "$_o" | grep -q 'PROXY: ON' || return 1
    # no LEAKED secret: the literal "<token>" placeholder in "subscription URL prefix: https://brk1/sub/<token>"
    # is NOT a leak (redactSubs strips real PSKs/tokens). Assert no `password:` field and no real long secret
    # value (≥40 chars) — the placeholder is only 7 chars, so it won't trip this.
    ! printf '%s' "$_o" | grep -qiE 'password:|[A-Za-z0-9+/_-]{40,}'
}
_two_ss_nodes()  { [ "$(ss_parse_count ctl1 /tmp/72-sub-alice.yaml)" = 2 ]; }
_sub_body_signed() {   # loopback body has agt1+agt2 ss proxies w/ real exit host:port
    _b=$(_sub_loopback "$1") || return 1
    printf '%s' "$_b" | grep -q 'type: ss' && printf '%s' "$_b" | grep -q 'name: agt1' && printf '%s' "$_b" | grep -q 'name: agt2'
}
_sub_ingress_same() {  # cross-container ingress body carries the same 2 ss proxies (loopback stays loopback-only)
    _b=$(dexec ctl1 -- curl -s --max-time 6 --cacert "$CA" "https://brk1/sub/$1" 2>/dev/null) || return 1
    printf '%s' "$_b" | grep -q 'name: agt1' && printf '%s' "$_b" | grep -q 'name: agt2'
}
_sub_loopback() { "$SIM" exec brk1 -- curl -s --max-time 6 "http://127.0.0.1:8090/sub/$1" 2>/dev/null; }
_forged_404() { [ "$("$SIM" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 'http://127.0.0.1:8090/sub/deadbeefdeadbeefdeadbeef' 2>/dev/null)" = 404 ]; }
_revoked_404() { [ "$("$SIM" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:8090/sub/$1" 2>/dev/null)" = 404 ]; }

# SS legs
_ss_pos() {   # agt1 (allow_private) exit → curl RFC1918 sink → real bytes
    ss_up ctl1 /tmp/72-sub-alice.yaml agt1 1080 || return 1
    poll_until 12 2 "SS via agt1 → sink" -- ss_curl_ok ctl1 1080 "http://$SINK_IP:9090/" "$SINK_TOK"
}
_ss_neg_privdest() {   # agt2 (default deny) exit → sink blocked; authoritative = agt2 journald blocked-dest in window
    _t0=$(dexec agt2 -- date -u '+%Y-%m-%d %H:%M:%S' 2>/dev/null)
    ss_up ctl1 /tmp/72-sub-alice.yaml agt2 1081 || return 1
    ss_curl ctl1 1081 "http://$SINK_IP:9090/" >/dev/null 2>&1   # expected to FAIL (blocked)
    poll_until 10 2 "agt2 blocked-dest log" -- _agt2_blocked "$_t0"
}
# h1 F3: the agent's slog left journald for its own capped file.
_agt2_blocked() { sim_agent_slog_grep agt2 'block.*(non-public|private|destination)|destination.*not.*allow'; }
_aead_wrongpsk() {   # AEAD trial-decrypt failure via a DATA-PLANE effect, not a journal line
    # DISCRIMINATOR (Stage-C harness-safety-2, revised after live probe): target agt1 (ALLOW_PRIVATE) and assert
    # wrong-PSK yields NO SINK BYTES. Since agt1 NEVER dest-blocks (SS-pos proved it flows with the CORRECT PSK to
    # the very same sink), the ONLY possible reason for no bytes on the wrong PSK is the chacha20-poly1305 AEAD
    # trial-decrypt failure. (The original "no blocked-dest log" on agt1 was vacuous; and on agt2 the AEAD-fail-vs-
    # dest-block JOURNAL distinction is not reliably separable — agt2 logs a block-ish line for both. The data-plane
    # effect "correct PSK flows, wrong PSK does NOT — on an exit that never dest-blocks" is the sound discriminator.)
    ss_down ctl1 2>/dev/null
    dexec ctl1 -- sh -c "nohup ss-local -s brk1 -p $EXIT1_PORT -k WRONGPSKWRONGPSKWRONG -m chacha20-ietf-poly1305 -b 127.0.0.1 -l 1082 -u >/tmp/ss-wrong.log 2>&1 & echo ok" >/dev/null 2>&1
    sleep 3
    ! dexec ctl1 -- curl -s --max-time 8 --socks5-hostname "127.0.0.1:1082" "http://$SINK_IP:9090/" 2>/dev/null | grep -qF "$SINK_TOK"
}
# loopback-only negative (Stage-C dp-3): the product /sub listener binds 127.0.0.1:8090; a cross-container DIRECT
# dial to brk1:8090 (bypassing the ingress TLS front) must be refused — proving loopback-only, not dev-no-auth.
_loopback_only_neg() { [ "$(dexec ctl1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://brk1:8090/sub/$TOKa" 2>/dev/null)" = 000 ]; }

# sub lifecycle predicates
_sub_token()    { "$SIM" ctl -- proxy sub create --name "$1" 2>&1 | grep -oE '/sub/[A-Za-z0-9._-]+' | head -1 | sed 's#/sub/##'; }
_sub_httpcode() { "$SIM" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:8090/sub/$1" 2>/dev/null; }
_sub_is200()    { [ "$(_sub_httpcode "$1")" = 200 ]; }
_dup_alice() {   # explore→pin: duplicate name refused AND the original token still resolves 200 (no silent rotate)
    _o=$("$SIM" ctl -- proxy sub create --name alice 2>&1)
    printf '%s' "$_o" | grep -qiE 'name_taken|already exists|duplicate|in use|taken' && [ "$(_sub_httpcode "$TOKa")" = 200 ]
}
_rev_baseline() { [ "$(_sub_httpcode "$TOKa")" = 200 ] && [ "$(_sub_httpcode "$TOKb")" = 200 ]; }
_rev_split_p()  { [ "$(_sub_httpcode "$TOKa")" = 404 ] && [ "$(_sub_httpcode "$TOKb")" = 200 ]; }
_rev_split()    { poll_until 15 2 "alice→404 while bob→200 (revoke propagates via reaper)" -- _rev_split_p; }
_rev_recover()  { TOKa2=$(_sub_token alice2); [ -n "$TOKa2" ] || return 1; poll_until 10 2 "alice2 /sub 200" -- _sub_is200 "$TOKa2"; }
_off_semantics(){
    "$SIM" ctl -- proxy status 2>&1 | grep -qiE 'PROXY: OFF|OFF' || return 1
    # HTTP-200 SUCCESS GATE (external-review M3): a live token must still RESOLVE 200 with a DIRECT-fallback body
    # (subhttp.go:106-132 — token valid + session ACTIVE + vendable → renderClash; 0 ss nodes when proxy_enabled=0).
    # A 404/empty fetch must NEVER vacuously satisfy `! grep type: ss`. Assert 200 + non-empty FIRST, then 0 ss.
    _oft="${TOKa2:-$TOKa}"
    [ "$(_sub_httpcode "$_oft")" = 200 ] || return 1
    _b=$(_sub_loopback "$_oft" 2>/dev/null)
    [ -n "$_b" ] || return 1
    ! printf '%s' "$_b" | grep -q 'type: ss'
}
# M3 revoke/off DATA-PLANE helpers — each sub has its OWN PSK (activeProxyKeys → ProxyKey{SubID,Secret:PSK});
# revoke drops that PSK from the agent keyset (revokeSubAndBump+pushCurrentKeyset, agent keeps its running SS
# server) → that sub's in-flight SS is cut while OTHER subs keep flowing. All legs egress via agt1 (allow_private).
_ss_black()     { ! ss_curl_ok ctl1 "$1" "http://$SINK_IP:9090/" "$SINK_TOK"; }   # SS leg no longer reaches the sink
_rev_ss_setup() {
    ss_up ctl1 /tmp/72-sub-alice.yaml agt1 1080 || return 1                        # alice PSK leg (re-up)
    ss_sub_fetch ctl1 "https://brk1/sub/$TOKb" /tmp/72-sub-bob.yaml "$CA" || return 1
    ss_up ctl1 /tmp/72-sub-bob.yaml agt1 1085                                       # bob PSK leg (independent control)
}
_rev_ss_recover() {
    ss_sub_fetch ctl1 "https://brk1/sub/$TOKa2" /tmp/72-sub-alice2.yaml "$CA" || return 1
    ss_up ctl1 /tmp/72-sub-alice2.yaml agt1 1086 || return 1                        # alice2 new PSK leg
    poll_until 15 2 "alice2 SS flows" -- ss_curl_ok ctl1 1086 "http://$SINK_IP:9090/" "$SINK_TOK"
}
# ── HELD-OPEN in-flight force-close (external-review R4-M3 + R5-M1) — a SLOW streaming sink + a backgrounded curl
#    that HOLDS an SS connection open, so `sub revoke` is tested for FORCE-CLOSING an ALREADY-ESTABLISHED, BYTE-
#    OBSERVED stream (not just marker-absence). R5-M1 hardening: (1) ThreadingHTTPServer (was single-threaded
#    HTTPServer — alice held the SOLE handler ~120s and bob merely waited in the accept backlog, so it was never
#    proven concurrently in-flight); (2) each held curl STREAMS ITS RECEIVED BYTES to a file so the baseline
#    asserts a STRICTLY GROWING byte count (established + actively transferring), not just "no exit marker" — a
#    curl stalled before connecting / a missing sink / no curl process can no longer satisfy the in-flight
#    predicate; (3) the marker records curl's EXIT CODE, so a force-close is alice EXITING EARLY (well before the
#    ~120s natural end) with its bytes FROZEN, WHILE bob's byte count KEEPS GROWING. ──
_slow_sink_up() {   # $1=node $2=port — stream 1 byte / 0.4s for ~120s per connection (THREADED: alice+bob concurrent)
    dexec "$1" -- sh -c "cat > /tmp/slowsink.py <<'PYEOF'
import http.server, time
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header('Content-Type','text/plain'); self.end_headers()
        for _ in range(300):
            try: self.wfile.write(b'x'); self.wfile.flush()
            except Exception: return
            time.sleep(0.4)
    def log_message(self,*a): pass
http.server.ThreadingHTTPServer(('0.0.0.0', $2), H).serve_forever()
PYEOF
nohup python3 /tmp/slowsink.py >/tmp/slowsink-$2.log 2>&1 & echo ok" >/dev/null 2>&1
}
# start a HELD-OPEN SS stream on ctl1 via <socks-port> to the SLOW sink, STREAMING received bytes to <bytes-file>
# (--no-buffer so the byte count grows as data arrives); write curl's EXIT CODE to <marker> when it exits (a
# FORCE-CLOSE makes it exit EARLY; a natural end exits after ~120s). $1=socks-port $2=marker-file $3=bytes-file.
_ss_hold_open() {
    dexec ctl1 -- sh -c "rm -f $2 $3; nohup sh -c 'curl -s --no-buffer --max-time 135 --socks5-hostname 127.0.0.1:$1 http://$SINK_IP:9091/ >$3 2>/dev/null; echo \$? > $2' >/dev/null 2>&1 & echo ok" >/dev/null 2>&1
}
_hold_bytes()  { dexec ctl1 -- sh -c "wc -c < '$1' 2>/dev/null || echo 0" | tr -d ' \n'; }   # current bytes received
_hold_growing() { _hg0=$(_hold_bytes "$1"); sleep 3; _hg1=$(_hold_bytes "$1"); [ "${_hg1:-0}" -gt "${_hg0:-0}" ]; }   # byte count STRICTLY grows over 3s = established + actively streaming
_hold_exited() { dexec ctl1 -- test -f "$1"; }         # 0 iff the held stream's curl has EXITED (force-closed or ended)
_hold_alive()  { ! dexec ctl1 -- test -f "$1"; }       # 0 iff still streaming (marker absent = curl not exited)
# BOTH held streams established + FLOWING (byte counts growing) AND neither exited — the byte-observed baseline.
_both_held_flowing() { _hold_alive /tmp/72-hold-alice.rc && _hold_alive /tmp/72-hold-bob.rc \
    && _hold_growing /tmp/72-hold-alice.bytes && _hold_growing /tmp/72-hold-bob.bytes; }
# bob STILL streaming AFTER the revoke: not exited AND its byte count still GROWING (survives the per-sub cut).
_bob_still_flowing() { _hold_alive /tmp/72-hold-bob.rc && _hold_growing /tmp/72-hold-bob.bytes; }
# ── OFF PORT/LISTENER + ALLOCATION reclaim (external-review R4-M3 + R5-M1) — after `proxy off` the __proxy__
#    exit's public port must be RELEASED both as an OS listener AND as a port_allocations row, and the port must be
#    SAFELY REUSABLE. If any leaks the assertion FAILS = the gap is EXPOSED. ──
_exit_port_listening() { dexec brk1 -- sh -c "ss -Htln 2>/dev/null | grep -qE '[:.]$1 '"; }   # $1 = broker exit port
_exit_port_gone()      { ! _exit_port_listening "$1"; }
# authoritative allocation SOURCE (R5-M1): the __proxy__ exit's row in port_allocations, not just the OS listener.
_proxy_alloc_rows() { "$SIM" exec brk1 -- runuser -u tether -- sqlite3 /var/lib/tether/tether.db "SELECT COUNT(*) FROM port_allocations WHERE name='__proxy__' AND state='ALLOCATED'" 2>/dev/null | tr -d ' \n'; }
_proxy_alloc_ge1()  { _n=$(_proxy_alloc_rows); [ -n "$_n" ] && [ "$_n" -ge 1 ] 2>/dev/null; }
_proxy_alloc_zero() { [ "$(_proxy_alloc_rows)" = 0 ]; }
# controlled SAME-PORT reuse (R5-M1): after off + reclaim, a fresh regular expose can claim the exact reclaimed
# port (no `port_taken`) and SERVE — proving the allocation was genuinely released, not just the listener closed.
_reuse_ok() {   # $1 = the reclaimed exit port
    RTOK=$(expose_serve_sentinel agt1 8090) || return 1
    "$SIM" ctl -- expose rm agt1 --name reuse >/dev/null 2>&1
    "$SIM" ctl -- expose agt1 --local 8090 --name reuse --remote-port "$1" >/dev/null 2>&1 || return 1
    poll_until 20 2 "reclaimed port $1 reused + serves" -- dp_curl_ok_body ctl1 "http://brk1:$1/" "$RTOK"
}

drill_begin "72-proxy-subscription (N=1 cluster proxy + sub + SS dual-leg + revoke)"
"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 2 agents + 1 ctl"          "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_ok "init brk1 (N=1 cluster)"                  "$SIM" init brk1
assert_ok "session lab + ctl login (owner)"          "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent-join agt2"                          "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
assert_ok "agent_provision_yaml agt1 proxyprivate (POSITIVE SS leg: allow_private_destinations)" \
          agent_provision_yaml agt1 "$SID" nats://brk1:4222 proxyprivate
assert_ok "agent_provision_yaml agt2 open (NEGATIVE leg: default private-dest deny)" \
          agent_provision_yaml agt2 "$SID" nats://brk1:4222 open
assert_ok "SETUP ingress (mint leaf + same-netns TLS front /sub + trust inject on ctl1)"  _setup_ingress
SINK_TOK=$(bridge_serve_sentinel agt2 9090); SINK_IP=$(node_bridge_ip agt2)
log "72: RFC1918 sink = $SINK_IP:9090 tok=$SINK_TOK"

# ── Arm O — owner-only + member-readable no-secret + G3 confirm gate ──────────
assert_ok "O-G3 proxy on WITHOUT --yes (non-PTY) → aborted (confirmProxyOn gate)"  _g3_no_yes
assert_ok "O1 proxy on --yes (owner) → PROXY ON"                                    _proxy_on_owner
assert_ok "O1b both agents reach proxy-ready (SS server up; /sub populates only when ready)" \
          poll_until 30 3 "2 agents proxy-ready" -- _proxy_ready 2
assert_ok "O0 member login (2nd identity joins session lab via pin)"               _member_login
assert_refuses "O2 member proxy on → not_owner (owner-only)" "not_owner|owner-only|not the owner" \
               CTLH mem proxy on --yes
assert_ok "O3 member proxy status readable + no PSK/token leak"                     _member_status_nosecret

# ── Arm C — sub create + duplicate-name conflict (explore→pin) ────────────────
TOKa=$(_sub_token alice); log "72: alice token minted (len=${#TOKa})"   # external-review M3: NEVER log the token value
assert_ok "C1 sub create alice → token minted (URL printed once)"  sh -c "[ -n '$TOKa' ]"
# duplicate same-name: explore→pin (refuse sub_name_taken AND first token still 200 = no silent rotate)
assert_ok "C2 duplicate sub create alice → conflict pinned + first token still valid"  _dup_alice

# ── Arm SUB — /sub body (loopback verify + cross-container ingress) + forged 404 ──
assert_ok "SUB1 /sub loopback body: 2 ss proxies (agt1+agt2) w/ real exit host:port"  _sub_body_signed "$TOKa"
assert_ok "SUB2 /sub cross-container via ingress = same 2 ss proxies (loopback stays loopback-only)"  _sub_ingress_same "$TOKa"
assert_ok "SUB3 forged /sub token → 404 (no info leak; tether's only outward public HTTP door)"  _forged_404

# ── Arm SS — real Shadowsocks dual-leg (OQ-1) ────────────────────────────────
assert_ok "SS0 fetch /sub to ctl1 via ingress (for ss-local parse)" \
          ss_sub_fetch ctl1 "https://brk1/sub/$TOKa" /tmp/72-sub-alice.yaml "$CA"
# external-review M3: the /sub Clash YAML carries real Shadowsocks passwords and the status JSON can echo tokens —
# NEVER dump either to the persistent runner log. Record only a structural summary (proxy count, ready count).
log "72-DBG /sub body: $(dexec ctl1 -- sh -c "grep -c 'type: ss' /tmp/72-sub-alice.yaml" 2>/dev/null) ss proxies (content redacted)"
log "72-DBG proxy status: $("$SIM" ctl -- proxy status --json 2>/dev/null | jq -c '{enabled, nodes: [.nodes[]?|{nid, ready, home_broker}]}' 2>/dev/null) (secrets redacted)"
assert_ok "SS0b /sub has exactly 2 ss proxies (agt1+agt2)"  _two_ss_nodes
EXIT1_PORT=$(dexec ctl1 -- python3 -c "import yaml
for p in yaml.safe_load(open('/tmp/72-sub-alice.yaml'))['proxies']:
 if p['name']=='agt1': print(p['port'])" 2>/dev/null); log "72: agt1 exit port=$EXIT1_PORT"
EXIT2_PORT=$(dexec ctl1 -- python3 -c "import yaml
for p in yaml.safe_load(open('/tmp/72-sub-alice.yaml'))['proxies']:
 if p['name']=='agt2': print(p['port'])" 2>/dev/null); log "72: agt2 exit port=$EXIT2_PORT"
assert_ok "SUB2-neg cross-container DIRECT to brk1:8090 (bypass ingress) → connection refused 000 (product /sub is loopback-only, NOT dev-no-auth; Stage-C dp-3)"  _loopback_only_neg
assert_ok "SS-pos agt1 (allow_private) exit → curl RFC1918 sink returns bytes (POSITIVE leg)"  _ss_pos
assert_ok "SS-neg-privdest agt2 (default deny) exit → sink blocked + journald blocked-dest (NEGATIVE leg)"  _ss_neg_privdest
assert_ok "SS-aead wrong-PSK to the agt1 (allow_private) exit → NO sink bytes, WHILE the correct PSK (SS-pos) DOES flow to the same sink — no bytes on an exit that never dest-blocks = pure AEAD trial-decrypt failure (Stage-C harness-safety-2)"  _aead_wrongpsk

# ── Arm REV — sub revoke alice: DATA-PLANE cut (external-review M3) + /sub control triple ──
# bob = an independent second sub (control source) with its OWN PSK. Both alice+bob SS legs (via agt1) flow
# before revoke; revoke drops alice's PSK from the agent keyset → alice's leg is CUT while bob's leg keeps
# flowing → alice2's new PSK recovers the data plane. NOT just the /sub HTTP layer (the old vacuous coverage).
TOKb=$(_sub_token bob); log "72: bob token minted (len=${#TOKb})"   # external-review M3: NEVER log the token value
ss_down ctl1 2>/dev/null
assert_ok "REV-baseline alice+bob both /sub 200 (prove both live before revoke)"  _rev_baseline
assert_ok "REV-ss0 bring up alice(1080)+bob(1085) SS legs via agt1 (each with its own PSK)"  _rev_ss_setup
assert_ok "REV-ss-base-a alice SS leg (PSK_alice) flows bytes to the sink (pre-revoke LIVE baseline, REQUIRED)"  poll_until 12 2 "alice SS flows" -- ss_curl_ok ctl1 1080 "http://$SINK_IP:9090/" "$SINK_TOK"
assert_ok "REV-ss-base-b bob SS leg (PSK_bob) flows bytes to the sink (pre-revoke LIVE baseline, independent control, REQUIRED)"  poll_until 12 2 "bob SS flows" -- ss_curl_ok ctl1 1085 "http://$SINK_IP:9090/" "$SINK_TOK"
# HELD-OPEN in-flight streams (external-review R4-M3): open a PERSISTENT SS byte stream via alice(1080) + bob(1085)
# to the SLOW sink, prove BOTH are streaming, then the revoke must FORCE-CLOSE alice's in-flight stream WHILE bob's
# survives. If tether does NOT force-close the established stream this FAILS = the gap is EXPOSED (not force-greened).
_slow_sink_up agt2 9091; sleep 1
_ss_hold_open 1080 /tmp/72-hold-alice.rc /tmp/72-hold-alice.bytes
_ss_hold_open 1085 /tmp/72-hold-bob.rc /tmp/72-hold-bob.bytes
sleep 6   # let both connections establish + start streaming through the exit
# R5-M1: BYTE-OBSERVED baseline — BOTH held streams must have a STRICTLY GROWING received-byte count AND not have
# exited. A stalled/never-connected curl / missing sink / no curl process shows NO byte growth → this FAILS (so a
# force-close can never be claimed on a stream that never established or transferred a byte).
assert_ok "REV-hold-base BOTH held-open SS streams are ESTABLISHED + ACTIVELY TRANSFERRING bytes pre-revoke (received-byte count STRICTLY GROWING on each; not merely 'no exit marker' — R5-M1)"  _both_held_flowing
_ba0=$(_hold_bytes /tmp/72-hold-alice.bytes); log "72: pre-revoke alice held-stream bytes=$_ba0 (>0 + growing = truly in-flight)"
assert_ok "REV1 proxy sub revoke alice"  "$SIM" ctl -- proxy sub revoke alice
assert_ok "REV-hold-alice [IN-FLIGHT FORCE-CLOSE] alice's ESTABLISHED byte stream is FORCE-CLOSED within ~30s of the revoke — its curl EXITS EARLY (marker written; NOT the ~120s natural end) with its bytes FROZEN. Revoke cuts an in-flight, byte-transferring stream, not merely new connections"  poll_until 30 2 "alice held stream force-closed" -- _hold_exited /tmp/72-hold-alice.rc
_ba1=$(_hold_bytes /tmp/72-hold-alice.bytes); log "72: post-revoke alice held-stream bytes FROZEN at=$_ba1 (curl exited early = force-closed)"
assert_ok "REV-hold-bob [IN-FLIGHT SURVIVES] bob's HELD-OPEN stream KEEPS TRANSFERRING (not exited AND its byte count still GROWING after the revoke) — the force-close is PER-SUB, not a blanket cut"  _bob_still_flowing
assert_ok "REV2 [SEMANTIC] alice /sub → 404 (revoked) WHILE bob /sub → 200 (control unaffected)"  _rev_split
assert_ok "REV2-dp [DATA-PLANE] alice SS leg BLACK-HOLES after revoke (PSK_alice dropped from the agent keyset; new curl through 1080 no longer reaches the sink)"  poll_until 20 2 "alice SS black-holes" -- _ss_black 1080
assert_ok "REV2-dp+ [DATA-PLANE] bob SS leg (PSK_bob still live) STILL flows bytes — revoke is per-sub, NOT a blanket outage"  poll_until 12 2 "bob SS still flows" -- ss_curl_ok ctl1 1085 "http://$SINK_IP:9090/" "$SINK_TOK"
TOKa2=$(_sub_token alice2)   # MAIN shell — an assert_ok subshell would lose this global (Stage-C harness-safety-1)
assert_ok "REV3 recover: sub create alice2 → new token minted"  sh -c "[ -n '$TOKa2' ]"
assert_ok "REV3b alice2 /sub → 200 (recovered token resolves; revoke did not break the proxy)"  poll_until 10 2 "alice2 /sub 200" -- _sub_is200 "$TOKa2"
assert_ok "REV3-dp [DATA-PLANE] alice2 SS leg (new PSK_alice2) flows bytes to the sink — the data plane RECOVERED, not merely a /sub 200"  _rev_ss_recover

# ── Arm OFF — proxy off (full stop + DATA-PLANE cut + /sub 0 nodes, HTTP-200 gated) ──
# PORT/LISTENER + ALLOCATION reclaim (external-review R4-M3 + R5-M1): the __proxy__ exit's public port must be
# RELEASED as an OS listener AND as a port_allocations row, and be SAFELY reusable, after `proxy off`.
assert_ok "OFF-port-pre brk1 LISTENs on the __proxy__ exit port $EXIT1_PORT (pre-off baseline — the exit's public listener is up)"  _exit_port_listening "$EXIT1_PORT"
assert_ok "OFF-alloc-pre the __proxy__ exit has >=1 ALLOCATED row in port_allocations (authoritative allocation-source baseline, not just the listener — R5-M1)"  _proxy_alloc_ge1
assert_ok "OFF1 proxy off"  "$SIM" ctl -- proxy off
assert_ok "OFF2-dp [DATA-PLANE] the alice2 SS leg (1086) BLACK-HOLES after proxy off — the exit is torn down (SS server stopped + tunnel dropped), not just a /sub cosmetic change"  poll_until 30 2 "SS leg dead after off" -- _ss_black 1086
assert_ok "OFF-port-reclaim [PORT RECLAIM] brk1 RELEASES the __proxy__ exit port $EXIT1_PORT after proxy off (the public LISTENER is GONE — ss -ltn no longer shows it). FAILS if tether leaks the listener."  poll_until 25 2 "exit port reclaimed" -- _exit_port_gone "$EXIT1_PORT"
assert_ok "OFF-alloc-reclaim [ALLOCATION RECLAIM] the __proxy__ exit's port_allocations rows drop to 0 after proxy off (the authoritative allocation SOURCE is released, not merely the OS listener — R5-M1)"  poll_until 25 2 "__proxy__ alloc rows 0" -- _proxy_alloc_zero
assert_ok "OFF-reuse [SAFE REUSE] a fresh regular expose can claim the exact reclaimed port $EXIT1_PORT (no port_taken) and SERVE a sentinel through it — proves the allocation was genuinely released + the port is safely reusable, not a leaked half-state (R5-M1)"  _reuse_ok "$EXIT1_PORT"
assert_ok "OFF3 [SEMANTIC] proxy status OFF + /sub still RESOLVES 200 with 0 ss nodes (DIRECT fallback; HTTP-200 gated, NOT a vacuous 404/empty)"  _off_semantics

log "72 R5-M1 NOW-COVERED: (1) BYTE-OBSERVED in-flight force-close — alice's ESTABLISHED, actively-transferring stream (strictly-growing byte count pre-revoke, REV-hold-base) is FORCE-CLOSED (early exit + frozen bytes) WHILE bob KEEPS transferring (REV-hold-bob, still growing) — over a THREADED slow sink so both are genuinely concurrent; (2) __proxy__ reclaim after proxy off = OS listener GONE (OFF-port-reclaim) + port_allocations rows→0 (OFF-alloc-reclaim, authoritative source) + SAFE same-port reuse (OFF-reuse serves through the reclaimed port). Each FAILS (exposes the gap) if tether does not force-close a byte-transferring stream / does not release the allocation / leaks the port."
drill_end
