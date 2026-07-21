#!/bin/sh
# 82-agent-onboarding-invite.sh — S2 GREEN (N=2 cluster + FRESH agent): the real C2 onboarding journey.
# leader `cluster seeds publish --sid` mints an agent-join invite → a FRESH agent container `tether agent
# join <invite> --start` → ONLINE, on the real out-of-process nats-server auth_callout first-connect stack.
# + well-known manifest (loopback + cross-container via S0-ingress) + C1 grow convergence + config
# refresh/doctor + trust-anchor negatives + user-service spike. Consumes S0-ingress. See s2-plan.md §3.3.
#
# FALSE-GREEN GUARDS:
#  (a) invite "minted" only from an echo → grep the REAL tether-invite:v1? token line + feed it to `agent join`.
#  (b) agent "ONLINE" only from a stale row → agt1 is a FRESH container, never ONLINE, so its first ONLINE
#      can only be this join.
#  (c) manifest "verifies" only because curl got 200 on a 503/empty body → assert the body is signed JSON
#      (jq -e '.roster and .seeds'), never `not available` 503.
#  (d) cross-container https "reachable" only from a plaintext fallback / rebound loopback → the positive is
#      the agent's REAL Go-x509 fetch (config refresh); the product loopback listener stays loopback-only.
#  (e) forged invite "refused" only from a missing --nid / env wall → each negative sig pins the exact text,
#      and --nid is always supplied.
#  (f) refresh "converged" only from exit 0 with a stale roster_gen → C1 asserts roster_gen strictly grew.
#  (g) doctor "all green" only from treating a FATAL as WARN → the FATAL variant (T2) asserts exit 77.
#  (h) "no residue" loudly claimed while agent.yaml WAS written → T2 pins the EXACT residue reality.
#  (i) manifest leg "serve-ready" only because curl always returns non-000 → the #27-CLOSED bound check is
#      gated by a control curl to a genuinely-unbound port (must be 000), so the bound assertion cannot pass
#      vacuously. #27 is now serve-ready by DEFAULT after init (no operator manifest_listen step).
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/ingress.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt

CTL()   { "$SIM" ctl -- "$@"; }
AJOIN() { "$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim tether "$@"; }
BROK()  { "$SIM" exec brk1 -- runuser -u tether -- tether "$@"; }
# R10: reap the same-netns ingress sidecars on ANY exit BEFORE cmd_nuke removes their netns-owner broker.
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): a POSIX INT/TERM handler RETURNS to the next command, so
# the combined trap reaped the sidecars and then RESUMED the join/forgery steps below. drill_install_traps
# registers EXIT on its own and exits 128+signo on INT/TERM. Cleanup body unchanged.
_cleanup() { ingress_down brk1 443 2>/dev/null; ingress_down brk1 8444 2>/dev/null; }
drill_install_traps _cleanup

_agt1_online() { CTL node ls 2>/dev/null | grep -qE "^agt1[[:space:]]+.*ONLINE"; }
# manifest listener probe: no listener bound → curl http_code 000 (couldn't connect); a bound listener is
# 200 or 503. This cleanly distinguishes "no listener" from "listener returns an HTTP error".
_manifest_refused() {
    _code=$("$SIM" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:7480/.well-known/tether/cluster.json 2>/dev/null)
    [ "$_code" = "000" ]
}
# #27 CLOSED (R12): after cluster init the listener is bound BY DEFAULT ⇒ curl answers (code != 000).
_manifest_bound() { ! _manifest_refused; }
# non-恒真 control: a port nothing listens on MUST still yield curl 000 — proves the 000-detection actually
# discriminates a refused connection, so _manifest_bound's pass is not a vacuous "curl always returns non-000".
_unbound_port_refused() {
    _c=$("$SIM" exec brk1 -- curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:39999/.well-known/tether/cluster.json 2>/dev/null)
    [ "$_c" = "000" ]
}
# drill-local predicates that call the ingress.sh / CTL / AJOIN functions (assert_ok runs these in a
# command-sub subshell that INHERITS the functions — unlike `sh -c "…func…"`).
_j4_refresh() { AJOIN agent config refresh --once --session "$SID" 2>&1 | grep -q 'roster refreshed'; }
# R1 (CRITICAL fix): the TLS negatives MUST exercise the PRODUCT agent Go-x509 fetcher (config refresh →
# clusterroster.FetchManifest, system-roots, no InsecureSkipVerify — fetch.go:36-52), NOT curl/OpenSSL. Each
# builds a scratch agent.yaml (account_pub pinned + bootstrap_url → the bad front) and drives config refresh;
# the sig pins the exact Go x509 message. A curl-only oracle could not catch an InsecureSkipVerify regression.
_mk_yaml() {  # _mk_yaml <node> <home> <nid> <bootstrap>
    "$SIM" exec "$1" -- runuser -u sim -- sh -c "mkdir -p $2/agent/$SID && printf 'broker_url: nats://brk1:4222\naccount_pub: $REAL\nbootstrap_url: $4\nsession: $SID\nnid: $3\n' > $2/agent/$SID/agent.yaml"
}
_ineg_san() {
    # wrong-SAN (cn=notabroker) HTTPS front on :8444 (same netns, instance-CA-signed); CA IS trusted on agt1.
    secrets_mint_ingress "$INSTANCE" brk1 ingress-wrongsan notabroker && ingress_up brk1 8444 ingress-wrongsan || return 1
    _mk_yaml agt1 /home/sim/.tether-negsan agt1 "https://brk1:8444/.well-known/tether/cluster.json"
    "$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim TETHER_HOME=/home/sim/.tether-negsan \
        tether agent config refresh --once --session "$SID" 2>&1 | grep -qE 'x509: certificate is valid for .*not brk1'
}
_ineg_ca() {
    # agt2 has the instance CA NOT injected → the product fetcher rejects the (valid-SAN) front as untrusted.
    _mk_yaml agt2 /home/sim/.tether agt2 "https://brk1/.well-known/tether/cluster.json"
    "$SIM" exec agt2 -- runuser -u sim -- env HOME=/home/sim \
        tether agent config refresh --once --session "$SID" 2>&1 | grep -qE 'x509: certificate signed by unknown authority'
}
# R7 (T2 doctor): pin the MANIFEST-forgery FATAL (exit 77 + the exact string), not just exit≠0 (which the
# missing agent.nk identity-FATAL guarantees regardless). R8 (J5 doctor): pin the verify string + no FATAL.
_t2_residue() {
    "$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim TETHER_HOME=/home/sim/.tether-forge2 tether agent join "$FORGED" --nid agt1 --pin "$PIN" >/tmp/forge2.log 2>&1 || true
    "$SIM" exec agt1 -- test -f "/home/sim/.tether-forge2/agent/$SID/agent.yaml" || return 1   # (a) yaml residue
    ! "$SIM" exec agt1 -- test -e "/home/sim/.tether-forge2/agent/$SID/roster_cache.json" || return 1  # (b) cache absent (AdoptDecision rejected)
    _dout=$("$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim TETHER_HOME=/home/sim/.tether-forge2 tether agent doctor --session "$SID" 2>&1); _drc=$?
    [ "$_drc" -eq 77 ] && printf '%s' "$_dout" | grep -q 'roster does NOT verify against the pinned account'  # (c) manifest-MITM FATAL
}
_j5_doctor() {
    _out=$(AJOIN agent doctor --session "$SID" 2>&1); _rc=$?
    [ "$_rc" -eq 0 ] && printf '%s' "$_out" | grep -q 'well-known manifest verifies against the pin' && ! printf '%s' "$_out" | grep -q FATAL
}
_u2() {
    AJOIN agent --install-user-service --session "$SID" --nid agt1 2>&1 | grep -qE "wrote.*tether-agent@$SID" \
        && "$SIM" exec agt1 -- test -f "/home/sim/.config/systemd/user/tether-agent@$SID.service"
}
_u4() {
    AJOIN agent --uninstall --session "$SID" --nid agt1 >/dev/null 2>&1
    ! "$SIM" exec agt1 -- test -f "/home/sim/.config/systemd/user/tether-agent@$SID.service"
}
_m1_ok() { "$SIM" exec brk1 -- curl -s --max-time 5 http://127.0.0.1:7480/.well-known/tether/cluster.json 2>/dev/null | jq -e '.roster and .seeds' >/dev/null 2>&1; }
_m2_ok() { "$SIM" exec agt1 -- curl -s --max-time 6 --cacert "$CA" https://brk1/.well-known/tether/cluster.json 2>/dev/null | jq -e '.roster and .seeds' >/dev/null 2>&1; }

drill_begin "82-agent-onboarding-invite (N=2 + fresh agent: C2 onboarding + S0-ingress)"

"$SIM" nuke >/dev/null 2>&1 || true
# agt1 = the fresh agent that joins via invite; agt2 = a fresh container kept CA-UN-injected (I-NEG-CA negative).
assert_ok "up 2 brokers + 2 fresh agents + 1 ctl"  "$SIM" up --brokers 2 --agents 2 --ctl 1
assert_ok "init brk1 (standalone → N=1 cluster)"   "$SIM" init brk1
# R15: leader-health floor so the broker is provably ALIVE — a crashed broker would ALSO refuse curl (000),
# so SETUP-27's BOUND check (and the control port's 000) are attributable to the listener state, not a dead
# broker. This is the health precondition under which the #27-CLOSED default-bind is asserted.
assert_ok "N=1 leader floor (leader=brk1) — broker alive, so the #27 bound/refused checks reflect the manifest listener, not a dead broker" \
          sh -c "\"$SIM\" exec brk1 -- sh -c 'tether cluster status --json' | jq -e '.leader_id==\"brk1\"' >/dev/null"
# ── #27 CLOSED (R12): C2 well-known discovery is now serve-ready after cluster init BY DEFAULT ─────────
# resolveManifestListen defaults manifest_listen to 127.0.0.1:7480 in cluster mode, so a bare `cluster init`
# (the sim's broker.yaml seam writes the cluster: block but NOT manifest_listen — mirroring the real seam)
# binds the listener with NO operator step. The old inverted "listener NOT bound" gap + its ingress_enable_
# manifest workaround are RETIRED; the listener being bound after init is now a plain GREEN regression.
assert_ok "SETUP-27-control a genuinely-unbound port yields curl 000 (proves the connection-refused oracle discriminates — the bound check below is not a vacuous 'curl always returns non-000')" \
          _unbound_port_refused
assert_ok "SETUP-27 [#27 CLOSED] after cluster init the well-known manifest listener is BOUND by default (curl to 127.0.0.1:7480 answers, NOT connection-refused) — no operator manifest_listen step required" \
          poll_until 30 2 "manifest listener bound after init" -- _manifest_bound
# S0-ingress bring-up: mint leaf, start same-netns TLS front, inject CA into agt1 trust store
# (the loopback manifest listener the front routes to is already bound by the #27 default above).
assert_ok "SETUP-ingress mint ingress leaf for brk1"    secrets_mint_ingress "$INSTANCE" brk1
assert_ok "SETUP-ingress start same-netns HTTPS front on brk1:443" ingress_up brk1
assert_ok "SETUP-ingress inject instance CA into agt1 trust store" ingress_trust_inject agt1
assert_ok "session lab + ctl login (owner)"        "$SIM" session "$SID" --pin "$PIN"
# R1: capture the instance account pub EARLY — the I-NEG TLS negatives (below) need it to pin their scratch
# agent.yaml before the T arm that also uses it.
REAL=$(secrets_account_pub "$INSTANCE")
log "82: instance account pub captured for I-NEG/T arms"

# ── Arm P — leader mints the agent-join invite (first publish = product semantic) ────────────────────
# R2: assert the REAL empty-seed signature (no `|| true`) so a pre-seeded/regressed state RED-s.
assert_ok      "P0 no seed bundle before first publish (first publish is manual — product semantic)" \
               sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether cluster seeds show 2>&1 | grep -qiE 'not published|no signed seed|endpoints:[[:space:]]*\$' && ! \"$SIM\" exec brk1 -- runuser -u tether -- tether cluster seeds show 2>&1 | grep -qE 'nats://|wss://'"
EXP=$(BROK cluster seeds publish --bootstrap https://brk1/.well-known/tether/cluster.json --endpoint nats://brk1:4222 --sid "$SID" 2>&1); echo "  publish: $EXP"
INV=$(printf '%s' "$EXP" | grep -oE 'tether-invite:v1[^[:space:]]*' | head -1)
assert_ok      "P1 seeds publish minted an agent-join invite (tether-invite:v1?...&sid=lab)" \
               sh -c "printf '%s' '$INV' | grep -qE 'tether-invite:v1.*sid=$SID'"

# ── Arm M — well-known manifest two legs (loopback verify-sig + cross-container via S0-ingress) ───────
# poll (not a single curl): the broker throttles manifest re-signs to `manifestRecheckInterval = 30s`
# (cluster_manifest.go:22) — a request within 30s of the SETUP-time (pre-seeds) build serves the STALE
# no-seeds cache; only after 30s does a request re-sign with the published seed bundle. So poll >30s.
# (This 30s freshness lag is a documented cache design, not a defect — noted, not a gotcha.) jq keys
# verified live: {schema_version, roster, seeds}.
assert_ok      "M1 broker-local loopback manifest verifies (signed JSON with roster+seeds, not 503)" \
               poll_until 45 3 "loopback manifest signed (past 30s resign throttle)" -- _m1_ok
assert_ok      "M2 cross-container https via S0-ingress + cert-verify (same signed JSON; loopback stays loopback-only)" \
               poll_until 45 3 "ingress manifest signed" -- _m2_ok

# ── Arm J — join journey (fresh agent → ONLINE + on-disk truth + refresh + doctor + TLS pos/neg) ─────
assert_ok "J1 fresh agent join --start → ONLINE (real auth_callout first-connect provision)" \
          sh -c "\"$SIM\" exec agt1 -- sh -c \"nohup runuser -u sim -- env HOME=/home/sim tether agent join '$INV' --nid agt1 --pin $PIN --start >/tmp/join.log 2>&1 & echo ok\" >/dev/null 2>&1; true"
assert_ok "J1-online agt1 reaches ONLINE (first-ever ONLINE = real registration)" \
          poll_until 40 3 "agt1 ONLINE after join" -- _agt1_online
assert_ok "J2 agent.yaml at real path, 0600, correct fields" \
          sh -c "\"$SIM\" exec agt1 -- sh -c 'test \$(stat -c %a /home/sim/.tether/agent/$SID/agent.yaml) = 600 && grep -q \"broker_url: nats://brk1:4222\" /home/sim/.tether/agent/$SID/agent.yaml && grep -q \"account_pub:\" /home/sim/.tether/agent/$SID/agent.yaml && grep -q \"bootstrap_url: https://brk1\" /home/sim/.tether/agent/$SID/agent.yaml && grep -q \"session: $SID\" /home/sim/.tether/agent/$SID/agent.yaml'"
assert_ok "J3 roster cache pre-warmed (S0-ingress manifest fetch + AdoptDecision succeeded)" \
          sh -c "\"$SIM\" exec agt1 -- test -f /home/sim/.tether/agent/$SID/roster_cache.json && ! grep -q 'pre-warm skipped' /tmp/join.log"
assert_ok "J4 config refresh --once = hard TLS positive oracle (real Go-x509 fetch through trusted front)" \
          _j4_refresh
# capture roster_gen G1 for C1 baseline
G1=$("$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim tether agent config refresh --once --session "$SID" 2>&1 | grep -oE 'roster_gen=[0-9]+' | grep -oE '[0-9]+' | head -1)
log "82: baseline roster_gen G1=${G1:-<none>}"
assert_ok "J5 agent doctor: exit 0 AND 'manifest verifies against the pin' AND no FATAL (R8: not bare exit-0)" \
          _j5_doctor

# ── I-NEG — TLS negatives (wrong-SAN cert; untrusted CA) ──────────────────────────────────────────────
assert_ok "I-NEG-SAN wrong-SAN cert refused (CA trusted, hostname mismatch)"       _ineg_san
assert_ok "I-NEG-CA untrusted CA refused (agt2: fresh container, CA NOT injected)" _ineg_ca

# ── Arm C1 — grow convergence (roster_gen strictly grows; NOT agent_roster_stale — 6-min grace) ──────
assert_ok "C1-grow grow brk2 (N=2)"                "$SIM" grow brk2
# R3: poll past the 30s manifest re-sign throttle (as M1/M2 do) — a single-shot refresh can read the stale
# pre-grow gen if it lands <30s after the last manifest touch → spurious false-RED. Gen is monotone.
assert_ok "C1 agent roster converges: roster_gen strictly grew after grow (polled past 30s throttle)" \
          poll_until 45 3 "roster_gen grew past $G1" -- sh -c "G2=\$(\"$SIM\" exec agt1 -- runuser -u sim -- env HOME=/home/sim tether agent config refresh --once --session $SID 2>&1 | grep -oE 'roster_gen=[0-9]+' | grep -oE '[0-9]+' | head -1); [ -n \"\$G2\" ] && [ -n \"$G1\" ] && [ \"\$G2\" -gt \"$G1\" ]"

# ── Arm T — trust-anchor negatives (isolated TETHER_HOME; never pollute J's good agent.yaml) ─────────
# ($REAL captured in setup.) Mint a valid-but-foreign account pub for the forged invite.
DECOY=$("$SIM" exec agt1 -- sh -c 'nk -gen account | nk -inkey /dev/stdin -pubout' 2>/dev/null | head -1)
log "82: DECOY account pub minted (forged invite)"
FORGED=$(printf '%s' "$INV" | sed "s/pin=[A-Za-z0-9]*/pin=$DECOY/")
assert_refuses "T1 forged pin + --expect-account-pub → refused BEFORE any write (no residue)" \
               "does not match the invite" \
               "$SIM" exec agt1 -- runuser -u sim -- env HOME=/home/sim TETHER_HOME=/home/sim/.tether-forge tether agent join "$FORGED" --nid agt1 --expect-account-pub "$REAL" --pin "$PIN"
assert_ok      "T1-noresidue no agent.yaml written under the forge home" \
               sh -c "! \"$SIM\" exec agt1 -- test -e /home/sim/.tether-forge/agent/$SID/agent.yaml"
# R7: T2 pins the MANIFEST-forgery FATAL (exit 77 + exact string), not just exit≠0.
assert_ok      "T2 forged pin + inline seed, no --expect → agent.yaml RESIDUE + cache absent + doctor exit77+manifest-FATAL (DOC-7)" \
               _t2_residue
# R14: `env -u TETHER_DEV_NO_AUTH` so T3/T4/T5 are hermetic (T5's https-refusal is gated on it being unset).
assert_refuses "T3 tampered: unknown param → parse refused before any write" \
               "unknown param" \
               "$SIM" exec agt1 -- runuser -u sim -- env -u TETHER_DEV_NO_AUTH HOME=/home/sim TETHER_HOME=/home/sim/.tether-t3 tether agent join "tether-invite:v1?pin=$REAL&url=https://brk1/.well-known/tether/cluster.json&sid=$SID&seed=nats://brk1:4222&bogus=1" --nid agt1 --pin "$PIN"
assert_refuses "T4 tampered: wrong scheme/version → parse refused" \
               "expected scheme|version" \
               "$SIM" exec agt1 -- runuser -u sim -- env -u TETHER_DEV_NO_AUTH HOME=/home/sim TETHER_HOME=/home/sim/.tether-t4 tether agent join "tether-invite:v2?pin=$REAL&url=https://brk1/.well-known/tether/cluster.json&sid=$SID" --nid agt1 --pin "$PIN"
assert_refuses "T5 tampered: non-https bootstrap → parse refused (TETHER_DEV_NO_AUTH explicitly unset)" \
               "must be https" \
               "$SIM" exec agt1 -- runuser -u sim -- env -u TETHER_DEV_NO_AUTH HOME=/home/sim TETHER_HOME=/home/sim/.tether-t5 tether agent join "tether-invite:v1?pin=$REAL&url=http://brk1/.well-known/tether/cluster.json&sid=$SID&seed=nats://brk1:4222" --nid agt1 --pin "$PIN"

# ── Arm U — user-service spike (install ≠ start; usage §6.1; three-way verdict like 62) ───────────────
# U1: linger. If the container has no user-manager, the whole U arm falls to NOT-COVERED (measured).
if "$SIM" exec agt1 -- loginctl enable-linger sim >/dev/null 2>&1 && \
   "$SIM" exec agt1 -- sh -c "systemctl is-active user@\$(id -u sim).service" >/dev/null 2>&1; then
    assert_ok "U1 linger + user manager active"    true
    # stop the J1 foreground daemon (same nid/nkey would contend)
    "$SIM" exec agt1 -- pkill -f 'tether agent join' >/dev/null 2>&1 || true
    "$SIM" exec agt1 -- pkill -x tether >/dev/null 2>&1 || true
    poll_until 6 1 "J1 daemon stopped" -- sh -c "! \"$SIM\" exec agt1 -- pgrep -x tether >/dev/null 2>&1" || true  # R19: poll, no fixed sleep
    assert_ok "U2 --install-user-service writes unit (install ≠ start)"  _u2
    if "$SIM" exec agt1 -- sh -c "runuser -u sim -- env XDG_RUNTIME_DIR=/run/user/\$(id -u sim) systemctl --user daemon-reload && runuser -u sim -- env XDG_RUNTIME_DIR=/run/user/\$(id -u sim) systemctl --user enable --now tether-agent@$SID.service" >/dev/null 2>&1; then
        assert_ok "U3 user-unit enable --now → same agent back ONLINE" \
                  poll_until 30 2 "agt1 ONLINE via user-unit" -- _agt1_online
    else
        not_covered "U3 (in-sim)" "systemctl --user not viable in this container (measured)" gap
    fi
    assert_ok "U4 --uninstall removes the user unit"  _u4
else
    not_covered "U1-U4 (in-sim)" "container has no systemd --user manager / linger (measured — usage §6.1 path left to real machines)" gap
fi

# cleanup: kill any backgrounded ingress sidecars + join daemons (trap nuke also reaps by label)
ingress_down brk1 443 2>/dev/null || true
ingress_down brk1 8444 2>/dev/null || true
drill_end
