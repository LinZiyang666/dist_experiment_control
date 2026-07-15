# drills/lib/proxy.sh — Shadowsocks subscription client helper for drill 72's real-egress legs (OQ-1).
# Depends on lib/docker.sh (dexec) + lib/log.sh + the drill's $SIM.
#
# The `/sub/<token>` body is Clash YAML (application/yaml — NOT SIP008 JSON), so python3-yaml (baked)
# extracts each proxy's server/port/cipher/password; `ss-local` (shadowsocks-libev, baked) then relays a
# local SOCKS5 to that agent exit. Everything runs INSIDE the ctl/agent container (R-NO-HOST-LEAK); the
# drill traps `pkill -f ss-local`. Stock ss-local interops with tether's SS server (chacha20-ietf-poly1305
# + evpBytesToKey(MD5) + HKDF-SHA1 "ss-subkey"); the Clash `password` is the base64 PSK literal.
#
# NB: the exact Clash proxy field names (server/port/cipher/password) are confirmed against a live /sub body
# in the Stage-B spike; ss_parse_count is the anchor that FAILS LOUD if the schema differs.

# ss_sub_fetch <node> <sub-url> <out-file> [cacert] : fetch the /sub body into <out-file> on <node>.
ss_sub_fetch() {
    _sf_node=$1; _sf_url=$2; _sf_out=$3; _sf_ca=${4:-}
    if [ -n "$_sf_ca" ]; then
        dexec "$_sf_node" -- sh -c "curl -s --max-time 6 --cacert '$_sf_ca' '$_sf_url' > '$_sf_out'"
    else
        dexec "$_sf_node" -- sh -c "curl -s --max-time 6 '$_sf_url' > '$_sf_out'"
    fi
}

# ss_parse_count <node> <sub-file> : print the number of `ss`-type proxies in a Clash YAML /sub body.
# (0 = the #31 candidate signature "all exits ready yet /sub renders 0 nodes" — see plan §5.)
ss_parse_count() {
    dexec "$1" -- python3 -c "import yaml,sys;d=yaml.safe_load(open('$2')) or {};print(sum(1 for p in (d.get('proxies') or []) if p.get('type')=='ss'))" 2>/dev/null
}

# ss_up <node> <sub-file> <proxy-name> <local-socks-port> : start ss-local on <node> bound to
# 127.0.0.1:<local-socks-port>, relaying to the Clash proxy named <proxy-name> (== the agent nid). The exit
# server/port point at the broker frp public port (SS traffic does NOT go through the ingress TLS front, it
# rides the broker's public band → yamux → agent SS server). Backgrounded in <node>; trap-reaped by ss_down.
ss_up() {
    _su_node=$1; _su_body=$2; _su_name=$3; _su_lport=$4
    _su_spec=$(dexec "$_su_node" -- python3 -c "import yaml,sys
ps=(yaml.safe_load(open('$_su_body')) or {}).get('proxies') or []
m=[p for p in ps if p.get('name')=='$_su_name']
if not m: sys.exit(1)
p=m[0]; print(p['server'],p['port'],p['cipher'],p['password'])" 2>/dev/null) \
        || { warn "ss_up: proxy '$_su_name' not found in $_su_body on $_su_node"; return 1; }
    # shellcheck disable=SC2086
    set -- $_su_spec
    dexec "$_su_node" -- sh -c "nohup ss-local -s '$1' -p '$2' -k '$4' -m '$3' -b 127.0.0.1 -l '$_su_lport' -u >/tmp/sslocal-$_su_lport.log 2>&1 & echo ok" >/dev/null 2>&1 \
        || { warn "ss_up: ss-local failed on $_su_node:$_su_lport"; return 1; }
    # external-review R4-M1: a bare `& echo ok` returns rc=0 even for a NONEXISTENT executable / an instant
    # crash / a bad spec — which would let a #33 black-hole oracle bless a FAILED LOCAL CLIENT as a product gap.
    # VERIFY the local client is actually READY: poll until its SOCKS listener is bound on 127.0.0.1:<lport> AND
    # an ss-local process is alive. Return 1 (a HARNESS/setup failure — callers die-gate it, NEVER count it as a
    # product black-hole) if it never comes up. This makes "curl through this client failed" mean the EXIT is
    # down, not the client. (Separates local-client-startup from product data-plane per R4-M1.)
    poll_until 10 1 "ss-local $_su_node:$_su_lport SOCKS listener ready" -- ss_local_ready "$_su_node" "$_su_lport" \
        || { warn "ss_up: ss-local on $_su_node:$_su_lport never bound its SOCKS listener (child crashed / bad spec) — a HARNESS failure, NOT a product black-hole (see /tmp/sslocal-$_su_lport.log)"; return 1; }
}

# ss_local_ready <node> <port> : predicate — the ss-local SOCKS listener is actually bound (LISTEN) on
# 127.0.0.1:<port> AND an ss-local process is alive. Proves the LOCAL client is ready so a subsequent curl
# failure through it is a PRODUCT black-hole (exit down), not a failed client launch (external-review R4-M1).
ss_local_ready() {
    dexec "$1" -- sh -c "ss -ltn 2>/dev/null | grep -qE '127\.0\.0\.1:$2 ' && pgrep -f 'ss-local.*-l $2 ' >/dev/null 2>&1"
}

# ss_down <node> : reap all ss-local on <node> (trap cleanup; cmd_nuke also reaps the container).
ss_down() { dexec "$1" -- pkill -f ss-local >/dev/null 2>&1 || true; }

# ss_curl <node> <local-socks-port> <target-url> : curl <target-url> through the local ss SOCKS5 (proves
# the SS egress leg carries real bytes). Non-zero if the relay fails / destination is blocked.
ss_curl() { dexec "$1" -- curl -s --max-time 8 --socks5-hostname "127.0.0.1:$2" "$3"; }

# ss_curl_ok <node> <local-socks-port> <target-url> <token> : predicate — the SOCKS5 relay returns a body
# containing <token> (positive egress data-plane oracle).
ss_curl_ok() { dexec "$1" -- curl -s --max-time 8 --socks5-hostname "127.0.0.1:$2" "$3" 2>/dev/null | grep -q "$4"; }
