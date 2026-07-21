# drills/lib/dataplane.sh — G-A shared data-plane fixtures + curl oracles (70/71/72/73/74/30).
# Depends on lib/docker.sh (dexec) + lib/log.sh (warn) + the drill's $SIM.
#
# R-DATAPLANE (#20): the point of these helpers is to close every expose/rehome/failover oracle on REAL
# tunneled bytes (an exact sentinel body / connection-refused), never on a status field. R-NO-HOST-LEAK:
# the sentinel http.server runs INSIDE the agent container, never on the host weilandserver.

# expose_serve_sentinel <agt> [port=8080] : on <agt>, serve a per-run-unique sentinel string over
# `python3 -m http.server` bound to 127.0.0.1:<port>. `expose`ing this port and curling the public port
# cross-container then returns the EXACT token (end-to-end reverse-tunnel proof). Prints the token on stdout
# (warns to stderr). Readiness is proven by the drill's first data-plane curl (polled), not here.
# R9-D: BOUNDED RETRY + a VISIBLE reason. This used to be a single `docker exec … >/dev/null 2>&1` whose
# failure was reported as the bare line "could not start http.server" — the daemon's actual complaint was
# discarded. Measured 2026-07-19: it failed on 2 of 3 parallel (-j4) runs of drill 71 and aborted the drill
# at SETUP-RED before ANY product assertion ran, with nothing in the log to say why. That is a sim-fixture
# fragility, not a tether defect, so making it robust is provisioning's job (Mandate ③) — it works around
# no product behaviour: the sentinel is just an HTTP server the drill tunnels to. The retry is bounded and
# every failed attempt's stderr is surfaced, so a PERSISTENT failure still aborts, loudly and attributably.
expose_serve_sentinel() {
    _ess_agt=$1; _ess_port=${2:-8080}
    # SANITISE the drill name before it goes into the token. `$_AS_DRILL` is FREE TEXT — the human drill title
    # passed to drill_begin — and the token is interpolated into a single-quoted `sh -c "… printf '%s' '$tok' …"`
    # payload. A title containing an apostrophe TERMINATES that quote and a following `(` then makes the remote
    # shell die with `sh: 1: Syntax error: ")" unexpected`; parentheses alone are harmless inside the quotes,
    # which is why titles have carried them for months without incident. Measured 2026-07-19: editing drill 71's
    # title to "…= P1/R8's deploy-tier verifier)" broke its sentinel on every run and aborted the drill at
    # SETUP-RED before a single product assertion — a drill's PROSE silently disabling its data plane. Reduce to
    # [A-Za-z0-9_-] so no title can ever do that again (the token only has to be unique + greppable; dp_curl_ok_body
    # matches it with grep -F).
    _ess_name=$(printf '%s' "${_AS_DRILL:-drill}" | tr -c 'A-Za-z0-9_-' '-' | cut -c1-40)
    _ess_tok="SENTINEL-${_ess_name}-$$-${_ess_agt}-${_ess_port}"
    _ess_try=1
    while [ "$_ess_try" -le 3 ]; do
        _ess_err=$(dexec "$_ess_agt" -- sh -c "mkdir -p /srv/sentinel-$_ess_port && printf '%s' '$_ess_tok' > /srv/sentinel-$_ess_port/index.html && cd /srv/sentinel-$_ess_port && nohup python3 -m http.server $_ess_port --bind 127.0.0.1 >/tmp/httpd-$_ess_port.log 2>&1 & echo started" 2>&1)
        if [ $? -eq 0 ]; then printf '%s' "$_ess_tok"; return 0; fi
        warn "expose_serve_sentinel: attempt $_ess_try/3 could not start http.server on $_ess_agt:$_ess_port — [$(printf '%s' "$_ess_err" | tr '\n' '|' | head -c 200)]"
        _ess_try=$((_ess_try+1)); sleep 3
    done
    warn "expose_serve_sentinel: FAILED after 3 attempts on $_ess_agt:$_ess_port (a persistent sim-fixture failure, surfaced with the daemon's own error above)"
    return 1
}

# dp_curl_body <from-node> <url> : curl <url> from <from-node>'s container, print the body. Non-zero on
# connect failure. Use with `grep -q "$TOKEN"` for the "returns the exact sentinel" data-plane oracle.
dp_curl_body() { dexec "$1" -- curl -s --max-time 6 "$2"; }

# dp_curl_ok_body <from-node> <url> <token> : predicate — curl <url> returns a body CONTAINING <token>.
# grep -F: the token can contain regex metacharacters (drill descriptions bleed into _AS_DRILL), so match literally.
dp_curl_ok_body() { dexec "$1" -- curl -s --max-time 6 "$2" 2>/dev/null | grep -qF "$3"; }

# bridge_serve_sentinel <node> <port> : like expose_serve_sentinel but binds 0.0.0.0, so it is reachable from
# OTHER containers via <node>'s bridge IP — the RFC1918 target for drill 72's SS egress legs. Prints the token.
bridge_serve_sentinel() {
    _bss_node=$1; _bss_port=$2
    _bss_tok="SINK-$$-${_bss_node}-${_bss_port}"
    dexec "$_bss_node" -- sh -c "mkdir -p /srv/sink-$_bss_port && printf '%s' '$_bss_tok' > /srv/sink-$_bss_port/index.html && cd /srv/sink-$_bss_port && nohup python3 -m http.server $_bss_port --bind 0.0.0.0 >/tmp/sink-$_bss_port.log 2>&1 & echo ok" >/dev/null 2>&1 \
        || { warn "bridge_serve_sentinel: could not start sink on $_bss_node:$_bss_port"; return 1; }
    printf '%s' "$_bss_tok"
}

# node_bridge_ip <node> : print <node>'s docker bridge IP (RFC1918 — the SS egress target address).
node_bridge_ip() { d inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(ctr_name "$1")" 2>/dev/null; }

# dp_curl_refused <from-node> <url> : succeeds iff the curl gets connection-REFUSED (curl exit 7) — the
# honest "stream cut / port closed" oracle. NOT `! curl -sf`: a half-open listener returning 4xx/5xx would
# satisfy `! curl -sf` and false-green a still-live public port (R1). exit 7 = "failed to connect".
dp_curl_refused() {
    dexec "$1" -- curl -s -o /dev/null --max-time 6 "$2" >/dev/null 2>&1
    [ "$?" -eq 7 ]
}
