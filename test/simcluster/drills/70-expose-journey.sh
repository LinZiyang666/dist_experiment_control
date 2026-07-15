#!/bin/sh
# 70-expose-journey.sh — S3 GREEN (N=1 cluster + 1 agent + ctl): the full `expose` data-plane journey on the
# REAL reverse tunnel. agent serves a sentinel on loopback → expose → curl the PUBLIC port cross-container
# returns the EXACT sentinel (end-to-end tunnel proof, R-DATAPLANE) → port/name negatives → expose rm cuts the
# stream + frees the port → agent restart → state.json token auto-reconnect (P6). Consumes S0-隧道 (landed S2).
#
# EXPLORE→PIN (live spike 2026-07-12 overrode three plan assumptions that were drawn from usage.md text):
#  - `init` makes brk1 an N=1 CLUSTER (clusterMode=true), so ps -a HAS a HOME column (=brk1), NOT absent (J2).
#  - `--on-broker brk1` SUCCEEDS (brk1 is an eligible VOTER of the N=1 cluster), NOT on_broker_single_mode (J8).
#  - `expose explain --json` has NO epoch field, so the "same expose reconnected" oracle uses .moved==false +
#    port unchanged, not epoch==E0 (J10). All pinned to LIVE truth (Mandate: expose reality, don't fit the doc).
#
# FALSE-GREEN GUARDS (plan §10-70):
#  - every positive closes on curl of the EXACT sentinel body (grep -q token), never a TCP connect / status.
#  - stream-cut closes on curl exit 7 (dp_curl_refused), NOT `! curl -sf` (a half-open 4xx/5xx would fool it).
#  - J10 does STOP→refused→START (a bare restart may never drop → false "reconnect") + asserts port unchanged
#    ∧ moved==false (a re-registered NEW expose would curl-OK too, but on a fresh port).
#  - public port always from `expose explain --json | jq .public_port`, never a grep of a merged 2>&1 line.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/dataplane.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790

_eport() { "$SIM" ctl -- expose explain "$1" --json 2>/dev/null | jq -r '.public_port // empty' 2>/dev/null; }

drill_begin "70-expose-journey (N=1 cluster expose data-plane journey + P6 reconnect)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 1 agent + 1 ctl"          "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (N=1 cluster)"                 "$SIM" init brk1
assert_ok "session lab + ctl login (owner)"         "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                         "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent_provision_yaml agt1 (agent.yaml tunnel_addr → expose data plane live; S0-隧道)" \
          agent_provision_yaml agt1 "$SID" nats://brk1:4222 open
TOK=$(expose_serve_sentinel agt1 8080)
[ -n "$TOK" ] || die "70: sentinel token empty (expose_serve_sentinel failed / _AS_DRILL unset)"
log "70: sentinel=$TOK"

# ── J1 expose --local + DATA-PLANE IRON PROOF ────────────────────────────────
assert_ok "J1a expose agt1 --local 8080 --name web"  "$SIM" ctl -- expose agt1 --local 8080 --name web
P=$(_eport web); log "70: web public port=$P"
[ -n "$P" ] || die "J1: could not resolve web public port from explain --json"
assert_ok "J1b [DATA-PLANE IRON PROOF] curl brk1:$P returns the EXACT sentinel over the reverse tunnel" \
          poll_until 20 2 "curl brk1:$P body=$TOK" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"

# ── J2 ps -a PORTS (pinned: N=1 cluster HAS a HOME column = brk1) ─────────────
assert_ok "J2 ps -a PORTS: web row w/ PUBLIC :$P + HOME brk1 (N=1 cluster home — pinned to live, not absent)" \
          sh -c "\"$SIM\" ctl -- ps -a 2>/dev/null | sed -n '/^PORTS/,\$p' | grep -Eq 'web[[:space:]]+agt1[[:space:]]+:8080[[:space:]]+:$P[[:space:]]+ALLOCATED[[:space:]]+brk1'"
assert_ok "J2json ps -a --json .ports[web]: port==$P AND home_broker==brk1" \
          sh -c "\"$SIM\" ctl -- ps -a --json 2>/dev/null | jq -e '.ports[]|select(.name==\"web\")|(.port==$P and .home_broker==\"brk1\")' >/dev/null"

# ── J3 expose explain (rebuild policy + deferred B5 footer) ───────────────────
assert_ok "J3 explain web: rebuild on + B5 'not yet recorded' footer present (values deferred, not asserted)" \
          sh -c "o=\$(\"$SIM\" ctl -- expose explain web 2>&1); printf '%s' \"\$o\" | grep -qE 'rebuild:.*on' && printf '%s' \"\$o\" | grep -qiE 'not yet recorded|planned, B5'"
assert_ok "J3json explain --json: rebuild==true ∧ moved==false ∧ home_broker==brk1" \
          sh -c "\"$SIM\" ctl -- expose explain web --json 2>/dev/null | jq -e '.rebuild==true and .moved==false and .home_broker==\"brk1\"' >/dev/null"

# ── J4 --remote-port (explicit in-band) + data-plane proof ───────────────────
assert_ok "J4a --remote-port 14500 (in-band, free)"  "$SIM" ctl -- expose agt1 --local 8080 --name web-rp --remote-port 14500
assert_ok "J4b curl brk1:14500 returns the sentinel (explicit port carries real bytes)" \
          poll_until 15 2 "curl 14500" -- dp_curl_ok_body ctl1 "http://brk1:14500/" "$TOK"

# ── J5/J6/J7 negatives (exact code sigs, from live spike) ────────────────────
assert_refuses "J5 port_taken (--remote-port on an already-allocated port)" "port_taken" \
               "$SIM" ctl -- expose agt1 --local 8080 --name web-pt --remote-port "$P"
assert_refuses "J6 port_out_of_band (--remote-port 15500 outside 14000-14999)" "port_out_of_band" \
               "$SIM" ctl -- expose agt1 --local 8080 --name web-ob2 --remote-port 15500
assert_refuses "J7 name_taken (duplicate --name web)" "name_taken" \
               "$SIM" ctl -- expose agt1 --local 8080 --name web

# ── J8 --on-broker (pinned: N=1 cluster self-pin SUCCEEDS, not single_mode) ───
assert_ok "J8 --on-broker brk1 (self = eligible VOTER in N=1 cluster) SUCCEEDS — pinned to live truth" \
          "$SIM" ctl -- expose agt1 --local 8080 --name web-ob --on-broker brk1

# ── J9 expose rm → stream cut + immediate port reuse (5 elements) ────────────
assert_ok "J9-baseline curl web:$P alive BEFORE rm (prove the data plane is live first)" \
          dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"
assert_ok "J9a expose rm agt1 --name web"  "$SIM" ctl -- expose rm agt1 --name web
assert_ok "J9b [SEMANTIC] curl brk1:$P now connection-REFUSED (exit 7 — NOT !curl-sf; R1 half-open guard)" \
          poll_until 15 2 "brk1:$P refused" -- dp_curl_refused ctl1 "http://brk1:$P/"
assert_ok "J9c port $P immediately REUSABLE (--remote-port $P re-expose)" \
          "$SIM" ctl -- expose agt1 --local 8080 --name web-reuse --remote-port "$P"
assert_ok "J9d curl brk1:$P = sentinel after reuse (real bytes, not expose exit-0 — bound≠forwarding)" \
          poll_until 15 2 "reuse curl" -- dp_curl_ok_body ctl1 "http://brk1:$P/" "$TOK"

# ── J10 agent restart → state.json token reconnect (P6, 5 elements) ──────────
RP=$(_eport web-reuse); log "70: web-reuse port=$RP"
assert_ok "J10-baseline curl web-reuse:$RP alive + record port"  dp_curl_ok_body ctl1 "http://brk1:$RP/" "$TOK"
assert_ok "J10a systemctl STOP tether-agent (precise: agent unit, NOT broker)"  \
          sh -c "\"$SIM\" exec agt1 -- systemctl stop tether-agent; true"
assert_ok "J10b [SEMANTIC] curl brk1:$RP REFUSED while agent down (proves the tunnel is agent-driven)" \
          poll_until 15 2 "down refused" -- dp_curl_refused ctl1 "http://brk1:$RP/"
assert_ok "J10c systemctl START tether-agent"  \
          sh -c "\"$SIM\" exec agt1 -- systemctl start tether-agent; true"
assert_ok "J10d [P6] curl brk1:$RP = SAME sentinel after reconnect (state.json token; port $RP unchanged)" \
          poll_until 30 3 "reconnect curl" -- dp_curl_ok_body ctl1 "http://brk1:$RP/" "$TOK"
assert_ok "J10e explain moved==false ∧ public_port==$RP (reconnected SAME expose, not re-created; epoch unobservable)" \
          sh -c "\"$SIM\" ctl -- expose explain web-reuse --json 2>/dev/null | jq -e '.moved==false and .public_port==$RP' >/dev/null"

drill_end
